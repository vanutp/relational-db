use crate::{
    DB,
    errors::{self, DBError},
    page_table::{PageTable, TableIterator},
    sql::WhereClause,
    table::{ColumnDef, Table},
    tuple::Tuple,
    values::DBValue,
};

fn tuple_matches(tuple: &Tuple, where_clause: &WhereClause) -> bool {
    match where_clause {
        WhereClause::Eq(col_index, value) => &tuple.values[*col_index] == value,
        WhereClause::Neq(col_index, value) => &tuple.values[*col_index] != value,
        WhereClause::Lt(col_index, value) => &tuple.values[*col_index] < value,
        WhereClause::Lte(col_index, value) => &tuple.values[*col_index] <= value,
        WhereClause::Gt(col_index, value) => &tuple.values[*col_index] > value,
        WhereClause::Gte(col_index, value) => &tuple.values[*col_index] >= value,
    }
}

pub(crate) fn execute_create_table(
    db: &mut DB,
    table_name: String,
    columns: Vec<ColumnDef>,
) -> errors::Result<()> {
    if db.tables.contains_key(&table_name) {
        return Err(DBError::Execution(format!(
            "Table {} already exists",
            table_name
        )));
    }
    let table = Table::new(db.next_table_id, table_name.clone(), columns);
    let table_file_path = table.get_table_file_path(&db.storage_dir);
    PageTable::init(&table, &table_file_path)?;
    db.tables.insert(table_name, table);
    db.next_table_id += 1;
    db.save_metadata()?;
    Ok(())
}

pub(crate) fn execute_drop_table(db: &mut DB, table_name: String) -> errors::Result<()> {
    db.get_table(&table_name)?
        .get_page_table(&db.storage_dir)?
        .delete()?;
    db.tables.remove(&table_name).unwrap();
    db.save_metadata()?;
    Ok(())
}

pub(crate) fn execute_insert(
    db: &DB,
    table_name: String,
    values: Vec<DBValue>,
) -> errors::Result<usize> {
    let table = db.get_table(&table_name)?;
    if values.len() != table.columns().len() {
        return Err(DBError::Execution(format!(
            "Insert values count ({}) does not match table columns count ({})",
            values.len(),
            table.columns().len()
        )));
    }
    for (i, value) in values.iter().enumerate() {
        if value.dtype() != table.columns()[i].dtype() {
            return Err(DBError::Execution(format!(
                "Value type mismatch for column {}: expected {:?}, got {:?}",
                table.columns()[i].name(),
                table.columns()[i].dtype(),
                value.dtype()
            )));
        }
    }

    let mut page_table = table.get_page_table(&db.storage_dir)?;
    let tuple = Tuple::new(values);
    page_table.insert_tuple(&tuple)?;
    Ok(1)
}

pub(crate) fn execute_select(
    db: &DB,
    table_name: String,
    where_clause: Option<WhereClause>,
    columns: Vec<(String, usize)>,
) -> errors::Result<(Vec<String>, SelectIterator)> {
    let page_table = db.get_table(&table_name)?.get_page_table(&db.storage_dir)?;
    let column_names = columns.iter().map(|(name, _)| name.clone()).collect();
    let iterator = SelectIterator {
        table_iter: page_table.into_iter(),
        where_clause,
        columns,
    };
    Ok((column_names, iterator))
}

pub(crate) struct SelectIterator<'a> {
    table_iter: TableIterator<'a, PageTable<'a>>,
    where_clause: Option<WhereClause>,
    columns: Vec<(String, usize)>,
}

impl Iterator for SelectIterator<'_> {
    type Item = errors::Result<Tuple>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.table_iter.next() {
                Some(Ok((_, _, tuple))) => {
                    let matches = self
                        .where_clause
                        .as_ref()
                        .map(|clause| tuple_matches(&tuple, clause))
                        .unwrap_or(true);
                    if matches {
                        let filtered_values: Vec<DBValue> = self
                            .columns
                            .iter()
                            .map(|&(_, index)| tuple.values[index].clone())
                            .collect();
                        return Some(Ok(Tuple::new(filtered_values)));
                    }
                }
                Some(Err(err)) => return Some(Err(err)),
                None => return None,
            }
        }
    }
}

pub(crate) fn execute_update(
    db: &DB,
    table_name: String,
    update_clauses: Vec<(usize, DBValue)>,
    where_clause: Option<WhereClause>,
) -> errors::Result<usize> {
    let mut page_table = db.get_table(&table_name)?.get_page_table(&db.storage_dir)?;

    let mut update_queue = vec![];
    for tuple in page_table.iter() {
        let (page_id, offset, mut tup_data) = tuple?;
        if let Some(ref clause) = where_clause {
            if !tuple_matches(&tup_data, clause) {
                continue;
            }
        }
        for (col_index, value) in &update_clauses {
            tup_data.values[*col_index] = value.clone();
        }
        update_queue.push((page_id, offset, tup_data));
    }
    let update_count = update_queue.len();
    for (page_id, offset, tuple) in update_queue {
        page_table.overwrite_tuple(page_id, offset, &tuple)?;
    }

    Ok(update_count)
}

pub(crate) fn execute_delete(
    db: &DB,
    table_name: String,
    where_clause: Option<WhereClause>,
) -> errors::Result<usize> {
    let mut page_table = db.get_table(&table_name)?.get_page_table(&db.storage_dir)?;

    let mut delete_queue = vec![];
    for tuple in page_table.iter() {
        let (page_id, offset, tup_data) = tuple?;
        if let Some(ref clause) = where_clause {
            if !tuple_matches(&tup_data, clause) {
                continue;
            }
        }
        delete_queue.push((page_id, offset));
    }
    let delete_count = delete_queue.len();
    for (page_id, offset) in delete_queue {
        page_table.delete_tuple(page_id, offset)?;
    }

    Ok(delete_count)
}
