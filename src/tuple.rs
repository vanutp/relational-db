use std::io::{Read, Write};

use crate::{
    errors::{self, DBError},
    serializer::{BinaryReader, BinaryWriter},
    table::Table,
    values::DBValue,
};

#[derive(PartialEq, Debug, Clone)]
pub struct Tuple {
    pub(crate) values: Vec<DBValue>,
}

impl Tuple {
    pub fn new(values: Vec<DBValue>) -> Self {
        Tuple { values }
    }

    pub fn values(&self) -> &[DBValue] {
        &self.values
    }

    pub fn into_values(self) -> Vec<DBValue> {
        self.values
    }

    pub(crate) fn read(table: &Table, reader: &mut BinaryReader<impl Read>) -> errors::Result<Self> {
        let mut values = Vec::with_capacity(table.columns().len());
        for column in table.columns() {
            let value = DBValue::from_reader(reader, column.dtype())?;
            values.push(value);
        }
        Ok(Tuple { values })
    }

    pub(crate) fn write(
        &self,
        table: &Table,
        writer: &mut BinaryWriter<impl Write>,
    ) -> errors::Result<()> {
        if self.values.len() != table.columns().len() {
            return Err(DBError::Execution(format!(
                "Tuple write error: tuple length does not match table column count: {} != {}",
                self.values.len(),
                table.columns().len()
            )));
        }
        for (i, column) in table.columns().iter().enumerate() {
            if self.values[i].dtype() != column.dtype() {
                return Err(DBError::Execution(format!(
                    "Tuple write error: tuple value type does not match table column type: {} != {}",
                    self.values[i].dtype(),
                    column.dtype(),
                )));
            }
            self.values[i].write(writer)?;
        }
        Ok(())
    }

    pub fn size(&self) -> usize {
        self.values.iter().map(|x| x.len()).sum()
    }
}

#[cfg(test)]
mod test {
    use std::io::Cursor;

    use crate::{
        serializer::{BinaryReader, BinaryWriter},
        table::test::sample_table,
        values::DBValue,
    };

    use super::Tuple;

    #[test]
    fn test_tuple() {
        let table = sample_table();
        let data = vec![
            0, 0, 0, 1, // id
            0, 0, 0, 4, // name length
            b't', b'e', b's', b't', // name
            63, 253, 251, 231, 108, 139, 67, 150, // height (1.874)
            1,   // active
        ];
        let reader = &mut BinaryReader::new(Cursor::new(data.clone()));
        let tuple = Tuple::read(&table, reader).unwrap();
        assert_eq!(tuple.values.len(), 4);
        assert_eq!(tuple.values[0], DBValue::Int(1));
        assert_eq!(tuple.values[1], DBValue::String("test".to_string()));
        assert_eq!(tuple.values[2], DBValue::Double(1.874));
        assert_eq!(tuple.values[3], DBValue::Bool(true));

        let mut new_data = vec![];
        let mut writer = BinaryWriter::new(Cursor::new(&mut new_data));
        tuple.write(&table, &mut writer).unwrap();
        assert_eq!(new_data, data);
    }

    // #[test]
    // fn test_bad_tuple() {
    //     todo!();
    // }
}
