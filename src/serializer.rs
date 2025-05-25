use std::{
    io::{self, Read, Write},
    slice,
};

pub struct BinaryReader<R: Read> {
    reader: R,
}

impl<R: Read> BinaryReader<R> {
    pub fn new(reader: R) -> Self {
        Self { reader }
    }

    pub fn read_u8(&mut self) -> io::Result<u8> {
        let mut data = 0_u8;
        self.reader.read_exact(slice::from_mut(&mut data))?;
        Ok(data)
    }

    pub fn read_u16(&mut self) -> io::Result<u16> {
        let mut buffer = [0; 2];
        self.reader.read_exact(&mut buffer)?;
        Ok(u16::from_be_bytes(buffer))
    }

    pub fn read_u32(&mut self) -> io::Result<u32> {
        let mut buffer = [0; 4];
        self.reader.read_exact(&mut buffer)?;
        Ok(u32::from_be_bytes(buffer))
    }

    pub fn read_string(&mut self) -> io::Result<String> {
        let len = self.read_u32()? as usize;
        let mut buffer = vec![0; len];
        self.reader.read_exact(&mut buffer)?;
        String::from_utf8(buffer)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Invalid UTF-8"))
    }

    pub fn read_f64(&mut self) -> io::Result<f64> {
        let mut buffer = [0; 8];
        self.reader.read_exact(&mut buffer)?;
        Ok(f64::from_be_bytes(buffer))
    }

    pub fn read_bool(&mut self) -> io::Result<bool> {
        let byte = self.read_u8()?;
        Ok(byte != 0)
    }

    pub fn read_i32(&mut self) -> io::Result<i32> {
        let mut buffer = [0; 4];
        self.reader.read_exact(&mut buffer)?;
        Ok(i32::from_be_bytes(buffer))
    }
}

impl<R: Read> Read for BinaryReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.reader.read(buf)
    }
}

pub struct BinaryWriter<W: Write> {
    writer: W,
}

impl<W: Write> BinaryWriter<W> {
    pub fn new(writer: W) -> Self {
        Self { writer }
    }

    pub fn write_u8(&mut self, value: u8) -> io::Result<()> {
        self.writer.write_all(slice::from_ref(&value))
    }

    pub fn write_u16(&mut self, value: u16) -> io::Result<()> {
        self.writer.write_all(&value.to_be_bytes())
    }

    pub fn write_u32(&mut self, value: u32) -> io::Result<()> {
        self.writer.write_all(&value.to_be_bytes())
    }

    pub fn write_string(&mut self, value: &str) -> io::Result<()> {
        let bytes = value.as_bytes();
        self.write_u32(bytes.len() as u32)?;
        self.writer.write_all(bytes)
    }

    pub fn write_f64(&mut self, value: f64) -> io::Result<()> {
        self.writer.write_all(&value.to_be_bytes())
    }

    pub fn write_bool(&mut self, value: bool) -> io::Result<()> {
        let byte = if value { 1 } else { 0 };
        self.write_u8(byte)
    }

    pub fn write_i32(&mut self, value: i32) -> io::Result<()> {
        self.writer.write_all(&value.to_be_bytes())
    }
}

impl<W: Write> Write for BinaryWriter<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.writer.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}
