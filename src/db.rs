use std::{
    collections::HashMap,
    fs::File,
    io::{self, BufReader, BufWriter},
    path::PathBuf,
};

use crate::{
    errors::{self, DBError},
    operations::{
        execute_create_table, execute_delete, execute_drop_table, execute_insert, execute_select,
        execute_update,
    },
    serializer::{BinaryReader, BinaryWriter},
    sql::Query,
    table::Table,
    tuple::Tuple,
};

pub struct DB {
    pub(crate) storage_dir: PathBuf,
    pub(crate) tables: HashMap<String, Table>,
    pub(crate) next_table_id: u32,
}

impl DB {
    pub fn init(storage_dir: PathBuf) -> io::Result<Self> {
        if storage_dir.exists() {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "Storage directory already exists",
            ));
        }
        std::fs::create_dir_all(&storage_dir)?;
        let res = Self {
            storage_dir,
            tables: HashMap::new(),
            next_table_id: 0,
        };
        res.save_metadata()?;
        Ok(res)
    }

    pub fn load(storage_dir: PathBuf) -> io::Result<Self> {
        if !storage_dir.exists() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "Storage directory does not exist",
            ));
        }
        let file = File::open(storage_dir.join("metadata"))?;
        let mut reader = BinaryReader::new(BufReader::new(file));
        let next_table_id = reader.read_u32()?;
        let table_count = reader.read_u32()? as usize;
        let mut tables = HashMap::with_capacity(table_count);
        for _ in 0..table_count {
            let table = Table::from_reader(&mut reader)?;
            tables.insert(table.name().to_owned(), table);
        }
        Ok(Self {
            storage_dir,
            tables,
            next_table_id,
        })
    }

    pub(crate) fn save_metadata(&self) -> io::Result<()> {
        let file = File::create(self.storage_dir.join("metadata"))?;
        let mut writer = BinaryWriter::new(BufWriter::new(file));
        writer.write_u32(self.next_table_id)?;
        writer.write_u32(self.tables.len() as u32)?;
        for table in self.tables.values() {
            table.write_metadata(&mut writer)?;
        }
        Ok(())
    }

    pub(crate) fn get_table(&self, name: &str) -> errors::Result<&Table> {
        self.tables
            .get(name)
            .ok_or(DBError::Execution(format!("Table {} does not exist", name)))
    }

    /// Returns either an iterator over the results (with column names) or a number of rows affected.
    pub fn execute(
        &mut self,
        query_string: &str,
    ) -> errors::Result<(
        Option<(Vec<String>, impl Iterator<Item = errors::Result<Tuple>>)>,
        Option<usize>,
    )> {
        let query = Query::compile(self, query_string)?;
        match query {
            Query::CreateTable {
                db,
                table_name,
                column_decls: columns,
            } => {
                execute_create_table(db, table_name.clone(), columns.clone()).map(|_| (None, None))
            }
            Query::DropTable { db, table_name } => {
                execute_drop_table(db, table_name).map(|_| (None, None))
            }
            Query::Insert {
                db,
                table_name,
                values,
            } => execute_insert(db, table_name, values).map(|count| (None, Some(count))),
            Query::Select {
                db,
                table_name,
                where_clause,
                columns,
            } => execute_select(db, table_name, where_clause, columns).map(|res| (Some(res), None)),
            Query::Update {
                db,
                table_name,
                where_clause,
                update_clauses,
            } => execute_update(db, table_name, update_clauses, where_clause)
                .map(|affected| (None, Some(affected))),
            Query::Delete {
                db,
                table_name,
                where_clause,
            } => {
                execute_delete(db, table_name, where_clause).map(|affected| (None, Some(affected)))
            }
        }
    }
}
