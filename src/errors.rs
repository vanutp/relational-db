use std::{fmt, io};

#[derive(Debug)]
pub enum DBError {
    Parse(String),
    Execution(String),
    Integrity(String),
    IO(io::Error),
}

impl fmt::Display for DBError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            DBError::Parse(msg) => write!(f, "Failed to parse the query: {}", msg),
            DBError::Execution(msg) => write!(f, "Failed to execute the query: {}", msg),
            DBError::Integrity(msg) => write!(f, "Integrity error: {}", msg),
            DBError::IO(err) => write!(f, "IO Error: {}", err),
        }
    }
}

impl From<io::Error> for DBError {
    fn from(err: io::Error) -> DBError {
        DBError::IO(err)
    }
}

pub type Result<T> = std::result::Result<T, DBError>;
