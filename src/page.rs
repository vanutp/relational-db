use std::{
    borrow::Borrow,
    io::{self, Cursor, Read, Write},
};

use crate::{
    errors::{self, DBError},
    serializer::{BinaryReader, BinaryWriter},
    table::Table,
    tuple::Tuple,
};

pub(crate) const PAGE_SIZE: usize = 8192;
pub(crate) const PAGE_HEADER_SIZE: usize = 4 + 2 * 2;
pub(crate) const PAGE_DATA_SIZE: usize = PAGE_SIZE - PAGE_HEADER_SIZE;

#[derive(Debug)]
struct TupleHeader {
    alive: bool,
    len: usize,
}

impl TupleHeader {
    fn size() -> usize {
        1 + 2
    }

    fn from_reader(reader: &mut BinaryReader<impl Read>) -> io::Result<Self> {
        let alive = reader.read_bool()?;
        let len = reader.read_u16()? as usize;
        Ok(Self { alive, len })
    }

    fn write(&self, writer: &mut BinaryWriter<impl Write>) -> io::Result<()> {
        writer.write_bool(self.alive)?;
        writer.write_u16(self.len as u16)?;
        Ok(())
    }
}

#[derive(PartialEq, Debug)]
pub(crate) struct Page<'a> {
    table: &'a Table,
    id: u32,
    free_space_end: usize, // u16, starting from data section
    dead_space: usize,     // u16
    data: [u8; PAGE_DATA_SIZE],
}

impl<'a> Page<'a> {
    pub fn new(table: &'a Table, id: u32) -> Self {
        Self {
            table,
            id,
            free_space_end: 0,
            dead_space: 0,
            data: [0; PAGE_DATA_SIZE],
        }
    }

    pub fn read(table: &'a Table, reader: &mut BinaryReader<impl Read>) -> io::Result<Self> {
        let id = reader.read_u32()?;
        let free_space_end = reader.read_u16()? as usize;
        let dead_space = reader.read_u16()? as usize;
        let mut data = [0; PAGE_DATA_SIZE];
        reader.read_exact(&mut data)?;
        Ok(Self {
            table,
            id,
            free_space_end,
            dead_space,
            data,
        })
    }

    pub fn write(&self, writer: &mut BinaryWriter<impl Write>) -> io::Result<()> {
        writer.write_u32(self.id)?;
        writer.write_u16(self.free_space_end as u16)?;
        writer.write_u16(self.dead_space as u16)?;
        writer.write_all(&self.data)?;
        Ok(())
    }

    pub fn id(&self) -> u32 {
        self.id
    }

    pub fn free_space(&self) -> usize {
        PAGE_DATA_SIZE - self.free_space_end
    }

    pub fn can_fit_tuple(&self, tuple: &Tuple) -> bool {
        self.free_space() >= TupleHeader::size() + tuple.size()
    }

    pub fn insert_tuple(&mut self, tuple: &Tuple) -> errors::Result<u16> {
        if !self.can_fit_tuple(tuple) {
            return Err(DBError::Integrity(format!(
                "Not enough space to add tuple: {} bytes needed, {} bytes available",
                TupleHeader::size() + tuple.size(),
                self.free_space(),
            )));
        }

        let header = TupleHeader {
            alive: true,
            len: tuple.size(),
        };
        let header_cursor = Cursor::new(&mut self.data[self.free_space_end..]);
        header.write(&mut BinaryWriter::new(header_cursor))?;

        let data_cursor = Cursor::new(&mut self.data[self.free_space_end + TupleHeader::size()..]);
        tuple.write(self.table, &mut BinaryWriter::new(data_cursor))?;

        let tuple_offset = self.free_space_end as u16;
        self.free_space_end += TupleHeader::size() + header.len;
        Ok(tuple_offset)
    }

    pub fn mark_tuple_dead(&mut self, tuple_offset: u16) -> errors::Result<()> {
        if tuple_offset >= PAGE_DATA_SIZE as u16 {
            return Err(DBError::Integrity(format!(
                "Tuple offset out of bounds: {}",
                tuple_offset
            )));
        }

        let cursor = Cursor::new(&self.data[tuple_offset as usize..]);
        let mut header = TupleHeader::from_reader(&mut BinaryReader::new(cursor))?;

        header.alive = false;
        self.dead_space += TupleHeader::size() + header.len;

        let cursor = Cursor::new(&mut self.data[tuple_offset as usize..]);
        header.write(&mut BinaryWriter::new(cursor))?;

        Ok(())
    }

    pub fn overwrite_tuple(&mut self, tuple_offset: u16, tuple: &Tuple) -> errors::Result<bool> {
        if tuple_offset >= PAGE_DATA_SIZE as u16 {
            return Err(DBError::Integrity(format!(
                "Tuple offset out of bounds: {}",
                tuple_offset
            )));
        }
        let header_cursor = Cursor::new(&mut self.data[tuple_offset as usize..]);
        let header = TupleHeader::from_reader(&mut BinaryReader::new(header_cursor))?;
        if !header.alive {
            return Err(DBError::Integrity(format!(
                "Tuple is dead: {}",
                tuple_offset
            )));
        }
        if tuple.size() > header.len {
            return Ok(false);
        }

        let data_cursor =
            Cursor::new(&mut self.data[tuple_offset as usize + TupleHeader::size()..]);
        tuple.write(self.table, &mut BinaryWriter::new(data_cursor))?;

        Ok(true)
    }

    pub fn iter(&self) -> PageIterator<&Self> {
        self.into_iter()
    }
}

impl<'a> IntoIterator for Page<'a> {
    type Item = errors::Result<(u16, Tuple)>;
    type IntoIter = PageIterator<Page<'a>>;

    fn into_iter(self) -> Self::IntoIter {
        PageIterator::new(self)
    }
}

impl<'a, 'b> IntoIterator for &'b Page<'a> {
    type Item = errors::Result<(u16, Tuple)>;
    type IntoIter = PageIterator<&'b Page<'a>>;

    fn into_iter(self) -> Self::IntoIter {
        PageIterator::new(self)
    }
}

#[derive(Debug)]
pub(crate) struct PageIterator<P> {
    page: P,
    offset: usize,
}

impl<'a, P: Borrow<Page<'a>>> PageIterator<P> {
    pub fn new(page: P) -> Self {
        Self { page, offset: 0 }
    }
}

impl<'a, P: Borrow<Page<'a>>> Iterator for PageIterator<P> {
    type Item = errors::Result<(u16, Tuple)>;

    fn next(&mut self) -> Option<Self::Item> {
        let page = self.page.borrow();

        if self.offset >= page.free_space_end {
            return None;
        }
        let mut cursor = Cursor::new(&page.data[self.offset..]);
        let header = TupleHeader::from_reader(&mut BinaryReader::new(cursor.clone())).expect("Failed to read tuple header");
        if !header.alive {
            self.offset += TupleHeader::size() + header.len;
            return self.next();
        }
        cursor.set_position(TupleHeader::size() as u64);
        let tuple_offset = self.offset as u16;
        self.offset += TupleHeader::size() + header.len;
        Some(Tuple::read(page.table, &mut BinaryReader::new(cursor)).map(|t| (tuple_offset, t)))
    }
}

#[cfg(test)]
pub(crate) mod test {
    use std::{
        collections::HashMap,
        fs::File,
        io::{BufReader, BufWriter, Cursor, Seek, SeekFrom},
    };

    use rand::seq::IteratorRandom;
    use temp_dir::TempDir;

    use crate::{
        errors::DBError,
        page::PAGE_SIZE,
        serializer::{BinaryReader, BinaryWriter},
        table::test::sample_table,
        tuple::Tuple,
        utils::test::random_string,
        values::DBValue,
    };

    use super::{Page, TupleHeader};

    pub fn create_tuple(id: i32, name: &str, height: f64, is_fox: bool) -> Tuple {
        Tuple::new(vec![
            DBValue::Int(id),
            DBValue::String(name.to_string()),
            DBValue::Double(height),
            DBValue::Bool(is_fox),
        ])
    }

    pub fn create_random_tuple(id: i32) -> Tuple {
        let name = random_string();
        let height = rand::random::<f64>();
        let is_fox = rand::random::<bool>();
        create_tuple(id, &name, height, is_fox)
    }

    fn validate_tuples(page: &Page, tuples_map: &HashMap<i32, (u16, Tuple)>) {
        let mut total_found = 0;
        for tuple in page.iter() {
            let (_, tuple) = tuple.unwrap();
            let DBValue::Int(id) = tuple.values[0].clone() else {
                panic!("Expected first value to be an Int");
            };
            let expected_tuple = &tuples_map[&id];
            assert_eq!(tuple, expected_tuple.1.clone());
            total_found += 1;
        }
        assert_eq!(
            total_found,
            tuples_map.len(),
            "The number of tuples found does not match the expected count"
        );
    }

    #[test]
    fn test_page() {
        let table = sample_table();

        let mut page = Page::new(&table, 0);
        let mut tuples_map = HashMap::new();

        let tuple = create_tuple(1, "test_test_test", 1.874, true);
        let offset = page.insert_tuple(&tuple).unwrap();
        tuples_map.insert(1, (offset, tuple));
        let tuple = create_tuple(2, "test_test_test2", 2., true);
        let offset = page.insert_tuple(&tuple).unwrap();
        tuples_map.insert(2, (offset, tuple));
        let tuple = create_tuple(3, "test_test3", 3., false);
        let offset = page.insert_tuple(&tuple).unwrap();
        tuples_map.insert(3, (offset, tuple));
        let tuple = create_tuple(4, "test4", 4., true);
        let offset = page.insert_tuple(&tuple).unwrap();
        tuples_map.insert(4, (offset, tuple));

        validate_tuples(&page, &tuples_map);

        // test overwrite
        tuples_map.get_mut(&1).unwrap().1.values[1] = DBValue::String("smol".to_string());
        assert!(
            page.overwrite_tuple(tuples_map[&1].0, &tuples_map[&1].1.clone())
                .unwrap()
        );
        validate_tuples(&page, &tuples_map);
        // can return to same size
        tuples_map.get_mut(&1).unwrap().1.values[1] = DBValue::String("test_test_test".to_string());
        assert!(
            page.overwrite_tuple(tuples_map[&1].0, &tuples_map[&1].1.clone())
                .unwrap()
        );
        validate_tuples(&page, &tuples_map);
        // cannot grow more than the original size
        tuples_map.get_mut(&1).unwrap().1.values[1] =
            DBValue::String("very_very_very_long".to_string());
        assert!(
            !page
                .overwrite_tuple(tuples_map[&1].0, &tuples_map[&1].1.clone())
                .unwrap()
        );
        tuples_map.get_mut(&1).unwrap().1.values[1] = DBValue::String("test_test_test".to_string());

        // test mark dead
        page.mark_tuple_dead(tuples_map[&3].0).unwrap();
        tuples_map.remove(&3);
        validate_tuples(&page, &tuples_map);

        // test relocate
        tuples_map.get_mut(&1).unwrap().1.values[1] =
            DBValue::String("very_very_very_long".to_string());
        assert!(
            !page
                .overwrite_tuple(tuples_map[&1].0, &tuples_map[&1].1.clone())
                .unwrap()
        );
        page.mark_tuple_dead(tuples_map[&1].0).unwrap();
        tuples_map.get_mut(&1).unwrap().0 = page.insert_tuple(&tuples_map[&1].1).unwrap();
        validate_tuples(&page, &tuples_map);

        // test read/write
        let mut new_data = vec![];
        let mut writer = BinaryWriter::new(Cursor::new(&mut new_data));
        page.write(&mut writer).unwrap();
        assert!(new_data.len() <= PAGE_SIZE);

        let mut reader = BinaryReader::new(Cursor::new(new_data));
        let read_page = Page::read(&table, &mut reader).unwrap();
        assert_eq!(read_page, page);
    }

    #[test]
    fn stress_test() {
        let table = sample_table();
        let mut page = Page::new(&table, 0);
        let mut tuples_map = HashMap::new();
        let mut i = 0;

        loop {
            if rand::random_range(0..5) == 0 {
                // delete a random tuple
                if tuples_map.is_empty() {
                    continue;
                }
                let id = *tuples_map.keys().choose(&mut rand::rng()).unwrap();
                for tuple in page.iter() {
                    let (offset, tuple) = tuple.unwrap();
                    if tuple.values[0] == DBValue::Int(id) {
                        page.mark_tuple_dead(offset).unwrap();
                        break;
                    }
                }
                tuples_map.remove(&id);
            } else {
                let tuple = create_random_tuple(i);

                if page.free_space() < TupleHeader::size() + tuple.size() {
                    // page is full, validate that we can't add more tuples
                    let DBError::Integrity(_) = page.insert_tuple(&tuple).unwrap_err() else {
                        panic!("Expected IntegrityError");
                    };
                    break;
                }

                let offset = page.insert_tuple(&tuple).unwrap();
                tuples_map.insert(i, (offset, tuple));
                i += 1;
            }
            validate_tuples(&page, &tuples_map);
        }
    }

    #[test]
    fn test_write_to_file() {
        let table = sample_table();
        let storage_dir = TempDir::new().unwrap();
        let mut page = Page::new(&table, 0);
        let tuple = create_tuple(1, "vanutp", 1.0, true);
        page.insert_tuple(&tuple).unwrap();
        let file_path = storage_dir.path().join("test");
        let mut file = File::create_new(&file_path).unwrap();
        file.seek(SeekFrom::Start(PAGE_SIZE as u64 * page.id() as u64))
            .unwrap();
        let mut writer = BinaryWriter::new(BufWriter::new(file));
        page.write(&mut writer).unwrap();

        drop(writer);

        let file = File::open(&file_path).unwrap();
        let mut reader = BinaryReader::new(BufReader::new(file));
        let read_page = Page::read(&table, &mut reader).unwrap();
        assert_eq!(read_page.id(), page.id());
        assert_eq!(read_page.free_space_end, page.free_space_end);
        assert_eq!(read_page.dead_space, page.dead_space);
        assert_eq!(read_page.data, page.data);
    }
}
