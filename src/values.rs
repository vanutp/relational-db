use std::{
    fmt::Display,
    io::{self, Read, Write},
    str::FromStr,
};

use crate::{
    errors,
    serializer::{BinaryReader, BinaryWriter},
    utils::escape,
};

#[derive(PartialEq, Debug, Clone, Copy)]
pub enum DBType {
    Bool,
    Int,
    Double,
    String,
}

impl Display for DBType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DBType::Bool => write!(f, "bool"),
            DBType::Int => write!(f, "int"),
            DBType::Double => write!(f, "double precision"),
            DBType::String => write!(f, "text"),
        }
    }
}

impl FromStr for DBType {
    type Err = errors::DBError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "bool" => Ok(DBType::Bool),
            "int" => Ok(DBType::Int),
            "double" | "double precision" => Ok(DBType::Double),
            "text" | "string" => Ok(DBType::String),
            _ => Err(errors::DBError::Parse(format!("Invalid type: {}", s))),
        }
    }
}

#[derive(PartialEq, Debug, Clone)]
pub enum DBValue {
    Bool(bool),
    Int(i32),
    Double(f64),
    String(String),
}

impl Display for DBValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DBValue::Bool(v) => write!(f, "{}", v),
            DBValue::Int(v) => write!(f, "{}", v),
            DBValue::Double(v) => write!(f, "{}", v),
            DBValue::String(v) => write!(f, "'{}'", escape(v)),
        }
    }
}

impl DBValue {
    pub fn from_reader(reader: &mut BinaryReader<impl Read>, dtype: DBType) -> io::Result<Self> {
        match dtype {
            DBType::Bool => Ok(DBValue::Bool(reader.read_bool()?)),
            DBType::Int => Ok(DBValue::Int(reader.read_i32()?)),
            DBType::Double => Ok(DBValue::Double(reader.read_f64()?)),
            DBType::String => Ok(DBValue::String(reader.read_string()?)),
        }
    }

    pub fn write(&self, writer: &mut BinaryWriter<impl Write>) -> io::Result<()> {
        match self {
            DBValue::Bool(v) => writer.write_bool(*v),
            DBValue::Int(v) => writer.write_i32(*v),
            DBValue::Double(v) => writer.write_f64(*v),
            DBValue::String(v) => writer.write_string(v),
        }
    }

    pub fn dtype(&self) -> DBType {
        match self {
            DBValue::Bool(_) => DBType::Bool,
            DBValue::Int(_) => DBType::Int,
            DBValue::Double(_) => DBType::Double,
            DBValue::String(_) => DBType::String,
        }
    }

    pub fn len(&self) -> usize {
        match self {
            DBValue::Bool(_) => 1,
            DBValue::Int(_) => 4,
            DBValue::Double(_) => 8,
            DBValue::String(s) => 4 + s.len(),
        }
    }
}

impl FromStr for DBValue {
    type Err = errors::DBError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Ok(v) = s.parse::<bool>() {
            return Ok(DBValue::Bool(v));
        }
        if let Ok(v) = s.parse::<i32>() {
            return Ok(DBValue::Int(v));
        }
        if let Ok(v) = s.parse::<f64>() {
            return Ok(DBValue::Double(v));
        }
        // TODO: handle '
        if s.starts_with('\'') && s.ends_with('\'') {
            let value = s[1..s.len() - 1].to_string();
            return Ok(DBValue::String(value));
        }
        Err(errors::DBError::Parse(format!("Invalid DBValue: {}", s)))
    }
}

impl PartialOrd for DBValue {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match (self, other) {
            (DBValue::Int(a), DBValue::Int(b)) => a.partial_cmp(b),
            (DBValue::Double(a), DBValue::Double(b)) => a.partial_cmp(b),
            _ => None,
        }
    }
}
