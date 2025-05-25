use std::{
    io::{self, Read, Write},
    path::{Path, PathBuf},
};

use crate::{
    page_table::PageTable,
    serializer::{BinaryReader, BinaryWriter},
    values::DBType,
};

#[derive(PartialEq, Debug, Clone)]
pub(crate) struct ColumnDef {
    name: String,
    dtype: DBType,
}

impl ColumnDef {
    pub fn new(name: String, dtype: DBType) -> Self {
        Self { name, dtype }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn dtype(&self) -> DBType {
        self.dtype
    }

    fn from_reader(reader: &mut BinaryReader<impl Read>) -> io::Result<Self> {
        let name = reader.read_string()?;
        let dtype = match reader.read_u8()? {
            0 => DBType::Bool,
            1 => DBType::Int,
            2 => DBType::Double,
            3 => DBType::String,
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Invalid column type",
                ));
            }
        };
        Ok(Self { name, dtype })
    }

    fn write(&self, writer: &mut BinaryWriter<impl Write>) -> io::Result<()> {
        writer.write_string(&self.name)?;
        let dtype = match &self.dtype {
            DBType::Bool => 0,
            DBType::Int => 1,
            DBType::Double => 2,
            DBType::String => 3,
        };
        writer.write_u8(dtype)?;
        Ok(())
    }
}

#[derive(PartialEq, Debug)]
pub(crate) struct Table {
    id: u32,
    name: String,
    columns: Vec<ColumnDef>,
}

impl Table {
    pub fn new(id: u32, name: String, columns: Vec<ColumnDef>) -> Self {
        Self { id, name, columns }
    }

    pub fn from_reader(reader: &mut BinaryReader<impl Read>) -> io::Result<Self> {
        let id = reader.read_u32()?;
        let name = reader.read_string()?;
        let column_count = reader.read_u32()? as usize;
        let mut columns = Vec::with_capacity(column_count);
        for _ in 0..column_count {
            columns.push(ColumnDef::from_reader(reader)?);
        }
        Ok(Self { id, name, columns })
    }

    pub fn write_metadata(&self, writer: &mut BinaryWriter<impl Write>) -> io::Result<()> {
        writer.write_u32(self.id)?;
        writer.write_string(&self.name)?;
        writer.write_u32(self.columns.len() as u32)?;
        for column in &self.columns {
            column.write(writer)?;
        }
        Ok(())
    }

    pub fn get_table_file_path(&self, storage_dir: &Path) -> PathBuf {
        storage_dir.join(format!("{}.tbl", self.id))
    }

    pub fn get_page_table(&self, storage_dir: &Path) -> io::Result<PageTable> {
        PageTable::load(self, self.get_table_file_path(storage_dir))
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn columns(&self) -> &Vec<ColumnDef> {
        &self.columns
    }
}

#[cfg(test)]
pub(crate) mod test {
    use crate::values::DBType;

    use super::{ColumnDef, Table};

    pub fn sample_table() -> Table {
        Table::new(
            1,
            "test".to_string(),
            vec![
                ColumnDef::new("id".to_string(), DBType::Int),
                ColumnDef::new("name".to_string(), DBType::String),
                ColumnDef::new("height".to_string(), DBType::Double),
                ColumnDef::new("is_fox".to_string(), DBType::Bool),
            ],
        )
    }
}
