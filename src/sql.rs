use std::{collections::HashMap, iter};

use once_cell::sync::Lazy;
use regex::{Captures, Regex};

use crate::{
    db::DB,
    errors::{self, DBError},
    table::{ColumnDef, Table},
    values::{DBType, DBValue},
};

pub(crate) enum WhereClause {
    Eq(usize, DBValue),
    Neq(usize, DBValue),
    Lt(usize, DBValue),
    Lte(usize, DBValue),
    Gt(usize, DBValue),
    Gte(usize, DBValue),
}

impl WhereClause {
    pub fn compile(
        expression: &str,
        name_to_col: &HashMap<String, (usize, ColumnDef)>,
    ) -> errors::Result<Self> {
        // TODO: sane parser
        static RGX: Lazy<Regex> =
            Lazy::new(|| Regex::new(r"^(\w+)\s*(=|!=|<|<=|>|>=)\s*(.+)$").unwrap());
        let caps = RGX
            .captures(expression)
            .ok_or_else(|| DBError::Parse(format!("Error parsing where clause: {}", expression)))?;
        let col_name = caps.get(1).unwrap().as_str();
        let op = caps.get(2).unwrap().as_str();
        let value_str = caps.get(3).unwrap().as_str();
        let value = value_str
            .parse::<DBValue>()
            .map_err(|_| DBError::Parse(format!("Invalid value in where clause: {}", value_str)))?;
        let (col_index, column_def) = name_to_col
            .get(col_name)
            .ok_or_else(|| DBError::Execution(format!("Column {} does not exist", col_name)))?;
        if value.dtype() != column_def.dtype() {
            return Err(DBError::Execution(format!(
                "Type mismatch: {} is not of type {}",
                value_str,
                column_def.dtype()
            )));
        }
        match op {
            "=" => Ok(WhereClause::Eq(*col_index, value)),
            "!=" => Ok(WhereClause::Neq(*col_index, value)),
            "<" => Ok(WhereClause::Lt(*col_index, value)),
            "<=" => Ok(WhereClause::Lte(*col_index, value)),
            ">" => Ok(WhereClause::Gt(*col_index, value)),
            ">=" => Ok(WhereClause::Gte(*col_index, value)),
            _ => Err(DBError::Parse(format!(
                "Invalid operator in where clause: {}",
                op
            ))),
        }
    }
}

pub(crate) enum Query<'a> {
    CreateTable {
        db: &'a mut DB,
        table_name: String,
        column_decls: Vec<ColumnDef>,
    },
    DropTable {
        db: &'a mut DB,
        table_name: String,
    },
    Insert {
        db: &'a DB,
        table_name: String,
        values: Vec<DBValue>,
    },
    Select {
        db: &'a DB,
        table_name: String,
        where_clause: Option<WhereClause>,
        columns: Vec<(String, usize)>,
    },
    Update {
        db: &'a DB,
        table_name: String,
        where_clause: Option<WhereClause>,
        update_clauses: Vec<(usize, DBValue)>,
    },
    Delete {
        db: &'a DB,
        table_name: String,
        where_clause: Option<WhereClause>,
    },
}

impl<'a> Query<'a> {
    fn parse_where_clause(
        caps: &Captures,
        column_mapping: &HashMap<String, (usize, ColumnDef)>,
    ) -> errors::Result<Option<WhereClause>> {
        caps.name("where_clause")
            .map(|w| w.as_str())
            .map(|w| WhereClause::compile(w, column_mapping))
            .transpose()
    }

    fn get_column_mapping(table: &Table) -> HashMap<String, (usize, ColumnDef)> {
        table
            .columns()
            .iter()
            .enumerate()
            .map(|(i, col)| (col.name().to_owned(), (i, col.clone())))
            .collect()
    }

    pub fn compile_create_table(db: &'a mut DB, query: &str) -> errors::Result<Self> {
        static RGX: Lazy<Regex> = Lazy::new(|| {
            Regex::new(r"^create table\s+(?P<table>\w+)\s+\(\s*(?P<decls>(?:\w+\s+\w+,\s*)*)(?P<last_decl>\w+\s+\w+\s*)\)$").unwrap()
        });
        let caps = RGX
            .captures(query)
            .ok_or_else(|| DBError::Parse("Error parsing create table statement".to_owned()))?;

        let table_name = caps.name("table").unwrap().as_str().to_owned();
        let column_decls = caps
            .name("decls")
            .unwrap()
            .as_str()
            .split(',')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .chain(iter::once(caps.name("last_decl").unwrap().as_str()))
            .map(|s| {
                let (name, dtype_str) = s.split_once(char::is_whitespace).unwrap();
                dtype_str
                    .parse::<DBType>()
                    .map(|dtype| ColumnDef::new(name.to_owned(), dtype))
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self::CreateTable {
            db,
            table_name,
            column_decls,
        })
    }

    pub fn compile_drop_table(db: &'a mut DB, query: &str) -> errors::Result<Self> {
        static RGX: Lazy<Regex> = Lazy::new(|| Regex::new(r"^drop table\s+(?P<table>\w+)$").unwrap());
        let caps = RGX
            .captures(query)
            .ok_or_else(|| DBError::Parse("Error parsing drop table statement".to_owned()))?;

        let table_name = caps.name("table").unwrap().as_str().to_owned();

        Ok(Self::DropTable { db, table_name })
    }

    pub fn compile_insert(db: &'a DB, query: &str) -> errors::Result<Self> {
        static RGX: Lazy<Regex> = Lazy::new(|| {
            Regex::new(r"^insert into\s+(?P<table>\w+)\s+values\s+\((?P<vals>(?:.+?,\s*)*)(?P<last_val>.+?)\)$").unwrap()
        });
        let caps = RGX
            .captures(query)
            .ok_or_else(|| DBError::Parse("Error parsing insert statement".to_owned()))?;

        let table_name = caps.name("table").unwrap().as_str().to_owned();

        let values = caps
            .name("vals")
            .unwrap()
            .as_str()
            .split(',')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .chain(iter::once(caps.name("last_val").unwrap().as_str()))
            .map(|s| s.parse::<DBValue>())
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self::Insert {
            db,
            table_name,
            values,
        })
    }

    pub fn compile_select(db: &'a DB, query: &str) -> errors::Result<Self> {
        static RGX: Lazy<Regex> = Lazy::new(|| {
            Regex::new(r"^select\s+(\*|(?P<cols>(?:\w+,\s*)*)(?P<last_col>\w+))\s+from\s+(?P<table>\w+)(?:\s+where\s+(?P<where_clause>.+))?$").unwrap()
        });
        let caps = RGX
            .captures(query)
            .ok_or_else(|| DBError::Parse("Error parsing select statement".to_owned()))?;

        let table_name = caps.name("table").unwrap().as_str().to_owned();
        let table = db.get_table(&table_name)?;
        let column_mapping = Query::get_column_mapping(table);

        let column_names = if caps.name("last_col").is_some() {
            caps.name("cols")
                .unwrap()
                .as_str()
                .split(',')
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())
                .chain(iter::once(caps.name("last_col").unwrap().as_str()))
                .collect::<Vec<_>>()
        } else {
            table
                .columns()
                .iter()
                .map(|col| col.name())
                .collect::<Vec<_>>()
        };
        let columns = column_names
            .into_iter()
            .map(|name| {
                column_mapping
                    .get(name)
                    .map(|(index, _)| (name.to_owned(), *index))
                    .ok_or_else(|| DBError::Execution(format!("Column {} does not exist", name,)))
            })
            .collect::<Result<Vec<_>, _>>()?;

        let where_clause = Query::parse_where_clause(&caps, &column_mapping)?;

        Ok(Self::Select {
            db,
            table_name,
            where_clause,
            columns,
        })
    }

    pub fn compile_update(db: &'a DB, query: &str) -> errors::Result<Self> {
        static RGX: Lazy<Regex> = Lazy::new(|| {
            Regex::new(r"^update\s+(?P<table>\w+)\s+set\s+(?P<updates>(?:\w+\s*=\s*.+?,\s*)*)(?P<last_update>(?:\w+\s*=\s*.+?))(?:\s+where\s+(?P<where_clause>.+))?$").unwrap()
        });
        let caps = RGX
            .captures(query)
            .ok_or_else(|| DBError::Parse("Error parsing update statement".to_owned()))?;

        let table_name = caps.name("table").unwrap().as_str().to_owned();
        let table = db.get_table(&table_name)?;
        let column_mapping = Query::get_column_mapping(table);

        let update_clauses = caps
            .name("updates")
            .unwrap()
            .as_str()
            .split(',')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .chain(iter::once(caps.name("last_update").unwrap().as_str()))
            .map(|s| s.split_once('=').unwrap())
            .map(|(col_name, value_str)| {
                value_str
                    .trim()
                    .parse::<DBValue>()
                    .map(|value| (col_name.trim(), value))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let update_clauses = update_clauses
            .into_iter()
            .map(|(col_name, value)| {
                column_mapping
                    .get(col_name)
                    .ok_or_else(|| {
                        DBError::Execution(format!("Column {} does not exist", col_name))
                    })
                    .and_then(|(column_index, column)| {
                        if value.dtype() == column.dtype() {
                            Ok((*column_index, value))
                        } else {
                            Err(DBError::Execution(format!(
                                "Type mismatch: {} is not of type {}",
                                value,
                                column.dtype()
                            )))
                        }
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;

        let where_clause = Query::parse_where_clause(&caps, &column_mapping)?;

        Ok(Self::Update {
            db,
            table_name,
            where_clause,
            update_clauses,
        })
    }

    pub fn compile_delete(db: &'a DB, query: &str) -> errors::Result<Self> {
        static RGX: Lazy<Regex> = Lazy::new(|| {
            Regex::new(r"^delete from\s+(?P<table>\w+)(?:\s+where\s+(?P<where_clause>.+))?$").unwrap()
        });
        let caps = RGX
            .captures(query)
            .ok_or_else(|| DBError::Parse("Error parsing delete statement".to_owned()))?;

        let table_name = caps.name("table").unwrap().as_str().to_owned();
        let table = db.get_table(&table_name)?;

        let column_mapping = Query::get_column_mapping(table);
        let where_clause = Query::parse_where_clause(&caps, &column_mapping)?;

        Ok(Self::Delete {
            db,
            table_name,
            where_clause,
        })
    }

    pub fn compile(db: &'a mut DB, query: &str) -> errors::Result<Self> {
        // TODO: sane parser
        if query.starts_with("create table") {
            Ok(Self::compile_create_table(db, query)?)
        } else if query.starts_with("drop table") {
            Ok(Self::compile_drop_table(db, query)?)
        } else if query.starts_with("insert into") {
            Ok(Self::compile_insert(db, query)?)
        } else if query.starts_with("select") {
            Ok(Self::compile_select(db, query)?)
        } else if query.starts_with("update") {
            Ok(Self::compile_update(db, query)?)
        } else if query.starts_with("delete from") {
            Ok(Self::compile_delete(db, query)?)
        } else {
            Err(DBError::Parse(format!("Unknown operation: {}", query)))
        }
    }
}
