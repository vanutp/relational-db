use std::{
    borrow::Borrow,
    fs::{self, File},
    io::{self, BufWriter, Cursor, Read, Seek, SeekFrom},
    path::{Path, PathBuf},
};

use crate::{
    errors::{self, DBError},
    page::{PAGE_SIZE, Page, PageIterator},
    serializer::{BinaryReader, BinaryWriter},
    table::Table,
    tuple::Tuple,
};

pub(crate) struct PageTable<'a> {
    table: &'a Table,
    file_path: PathBuf,
    page_count: u32,
}

impl<'a> PageTable<'a> {
    pub fn load(table: &'a Table, file_path: PathBuf) -> io::Result<Self> {
        let metadata = fs::metadata(&file_path)?;
        if metadata.len() % PAGE_SIZE as u64 != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Page file size is not a multiple of page size",
            ));
        }
        let page_count = (metadata.len() / PAGE_SIZE as u64) as u32;
        Ok(Self {
            table,
            file_path,
            page_count,
        })
    }

    pub fn delete(&mut self) -> errors::Result<()> {
        fs::remove_file(&self.file_path)?;
        self.page_count = 0;
        Ok(())
    }

    pub fn init(table: &'a Table, file_path: &Path) -> errors::Result<Self> {
        if fs::exists(file_path)? {
            return Err(DBError::Integrity(format!(
                "Table {} is already initialized",
                table.name(),
            )));
        }
        let file = File::create_new(file_path)?;
        let empty_page = Page::new(table, 0);
        let mut writer = BinaryWriter::new(BufWriter::new(file));
        empty_page.write(&mut writer)?;
        Ok(Self {
            table,
            file_path: file_path.to_owned(),
            page_count: 1,
        })
    }

    fn get_page(&self, page_id: u32) -> errors::Result<Page<'a>> {
        let page_count = self.page_count;
        if page_id >= page_count {
            return Err(DBError::Integrity(format!(
                "Attempted to load invalid page {}, only {} pages exist",
                page_id, page_count,
            )));
        }
        let mut file = File::open(&self.file_path)?;
        file.seek(SeekFrom::Start(PAGE_SIZE as u64 * page_id as u64))?;
        let mut buf = vec![0; PAGE_SIZE];
        file.read_exact(&mut buf)?;
        let mut reader = BinaryReader::new(Cursor::new(&buf));
        Ok(Page::read(self.table, &mut reader)?)
    }

    fn save_page(&mut self, page: &Page) -> errors::Result<()> {
        let mut file = File::options().write(true).open(&self.file_path)?;
        file.seek(SeekFrom::Start(PAGE_SIZE as u64 * page.id() as u64))?;
        let mut writer = BinaryWriter::new(BufWriter::new(file));
        page.write(&mut writer)?;
        drop(writer);
        if page.id() >= self.page_count {
            self.page_count = page.id() + 1;
        }
        Ok(())
    }

    pub fn iter(&self) -> TableIterator<'a, &PageTable<'a>> {
        self.into_iter()
    }

    pub fn insert_tuple(&mut self, tuple: &Tuple) -> errors::Result<(u32, u16)> {
        let page_count = self.page_count;
        let mut page = self.get_page(page_count - 1)?;
        if !page.can_fit_tuple(tuple) {
            page = Page::new(self.table, self.page_count);
        }
        let res = page.insert_tuple(tuple).map(|offset| (page.id(), offset))?;
        self.save_page(&page)?;
        Ok(res)
    }

    pub fn overwrite_tuple(
        &mut self,
        page_id: u32,
        offset: u16,
        tuple: &Tuple,
    ) -> errors::Result<(u32, u16)> {
        let mut page = self.get_page(page_id)?;
        let mut res = (page_id, offset);
        if !page.overwrite_tuple(offset, tuple)? {
            page.mark_tuple_dead(offset)?;
            if page.can_fit_tuple(tuple) {
                res.1 = page.insert_tuple(tuple)?;
            } else {
                res = self.insert_tuple(tuple)?;
            }
        }
        // TODO: transactions
        self.save_page(&page)?;
        Ok(res)
    }

    pub fn delete_tuple(&mut self, page_id: u32, offset: u16) -> errors::Result<()> {
        let mut page = self.get_page(page_id)?;
        page.mark_tuple_dead(offset)?;
        self.save_page(&page)?;
        Ok(())
    }
}

impl<'a> IntoIterator for PageTable<'a> {
    type Item = errors::Result<(u32, u16, Tuple)>;
    type IntoIter = TableIterator<'a, PageTable<'a>>;

    fn into_iter(self) -> Self::IntoIter {
        TableIterator::new(self)
    }
}

impl<'a, 'b> IntoIterator for &'b PageTable<'a> {
    type Item = errors::Result<(u32, u16, Tuple)>;
    type IntoIter = TableIterator<'a, &'b PageTable<'a>>;

    fn into_iter(self) -> Self::IntoIter {
        TableIterator::new(self)
    }
}

pub(crate) struct TableIterator<'a, PT> {
    page_table: PT,
    page_id: u32,
    page_iterator: Option<PageIterator<Page<'a>>>,
    errored: bool,
}

impl<'a, PT: Borrow<PageTable<'a>>> TableIterator<'a, PT> {
    pub fn new(page_table: PT) -> Self {
        Self {
            page_table,
            page_id: 0,
            page_iterator: None,
            errored: false,
        }
    }
}

impl<'a, PT: Borrow<PageTable<'a>>> Iterator for TableIterator<'a, PT> {
    type Item = errors::Result<(u32, u16, Tuple)>;

    fn next(&mut self) -> Option<Self::Item> {
        let page_table = self.page_table.borrow();

        if self.errored {
            return None;
        }
        loop {
            match self.page_iterator.as_mut().and_then(|it| it.next()) {
                Some(Ok((offset, tuple))) => return Some(Ok((self.page_id, offset, tuple))),
                Some(Err(err)) => {
                    self.errored = true;
                    return Some(Err(err));
                }
                None => {
                    // If that's the first iteration, page_id is 0 but no page is loaded yet,
                    // so no need to increment it.
                    if self.page_iterator.is_some() {
                        self.page_id += 1;
                    }
                    if self.page_id >= page_table.page_count {
                        return None;
                    }
                    match page_table.get_page(self.page_id) {
                        Ok(page) => {
                            self.page_iterator = Some(page.into_iter());
                        }
                        Err(err) => {
                            self.errored = true;
                            return Some(Err(err));
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use rand::seq::IteratorRandom;
    use temp_dir::TempDir;

    use crate::{
        page::test::create_random_tuple, table::test::sample_table, tuple::Tuple, values::DBValue,
    };

    use super::PageTable;

    fn validate_tuples(page_table: &PageTable, tuples_map: &HashMap<i32, (u32, u16, Tuple)>) {
        let mut total_found = 0;
        for tup_data in page_table.iter() {
            let tup_data = tup_data.unwrap();
            let DBValue::Int(id) = tup_data.2.values[0].clone() else {
                panic!("Expected first value to be an Int");
            };
            let expected_tuple = &tuples_map[&id];
            assert_eq!(tup_data, expected_tuple.clone());
            total_found += 1;
        }
        assert_eq!(
            total_found,
            tuples_map.len(),
            "The number of tuples found does not match the expected count"
        );
    }

    #[test]
    fn stress_test() {
        let storage_dir = TempDir::new().unwrap();
        let table = sample_table();
        let table_file_path = table.get_table_file_path(storage_dir.path());
        let mut page_table = PageTable::init(&table, &table_file_path).unwrap();
        let mut tuples_map = HashMap::new();
        let mut id = 0;

        for i in 0..50_000 {
            if i % 10000 == 0 {
                println!("Iteration: {}", i);
            }
            if rand::random_range(0..5) == 0 {
                // delete a random tuple
                if tuples_map.is_empty() {
                    continue;
                }
                let id = *tuples_map.keys().choose(&mut rand::rng()).unwrap();
                for tuple in page_table.iter() {
                    let (page_id, offset, tuple) = tuple.unwrap();
                    if tuple.values[0] == DBValue::Int(id) {
                        page_table.delete_tuple(page_id, offset).unwrap();
                        break;
                    }
                }
                tuples_map.remove(&id);
            } else {
                let tuple = create_random_tuple(id);
                let (page_id, offset) = page_table.insert_tuple(&tuple).unwrap();
                tuples_map.insert(id, (page_id, offset, tuple));
                id += 1;
            }
            if i % 1000 == 0 {
                validate_tuples(&page_table, &tuples_map);
            }
        }

        assert!(page_table.page_count > 10);
    }
}
