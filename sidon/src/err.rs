//! One error type. Storage failures get reported to two audiences -- an operator reading
//! a log and a guest kernel reading an NBD error code -- and the distinction that matters
//! for the second one is whether the data is *wrong* or merely *unavailable*.

use std::fmt;

#[derive(Debug)]
pub enum Error {
    /// The local filesystem said no. Recoverable in the sense that the map is intact.
    Io(String),
    /// A stored value did not match its checksum, or a record's framing was impossible.
    /// This is the one that must never be answered with data.
    Corrupt(String),
    /// Hydra or Daruk could not be reached, or refused a statement.
    Meta(String),
    /// The caller asked for something the current state forbids: a stale epoch, a write
    /// to an immutable vdisk, a read past the end.
    Refused(String),
}

impl Error {
    pub fn io(m: impl Into<String>) -> Self {
        Error::Io(m.into())
    }
    pub fn corrupt(m: impl Into<String>) -> Self {
        Error::Corrupt(m.into())
    }
    pub fn meta(m: impl Into<String>) -> Self {
        Error::Meta(m.into())
    }
    pub fn refused(m: impl Into<String>) -> Self {
        Error::Refused(m.into())
    }

    /// The errno an NBD client is told. EIO for anything that damaged or hid data, EPERM
    /// for a refusal, ENOSPC passed through so a guest can distinguish a full pool from a
    /// broken one.
    pub fn errno(&self) -> u32 {
        match self {
            Error::Io(m) if m.contains("No space left") => 28, // ENOSPC
            Error::Io(_) => 5,                                 // EIO
            Error::Corrupt(_) => 5,                            // EIO, never a value
            Error::Meta(_) => 5,
            Error::Refused(_) => 1, // EPERM
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(m) => write!(f, "io: {m}"),
            Error::Corrupt(m) => write!(f, "corrupt: {m}"),
            Error::Meta(m) => write!(f, "metadata: {m}"),
            Error::Refused(m) => write!(f, "refused: {m}"),
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::Io(e.to_string())
    }
}

pub type Result<T> = std::result::Result<T, Error>;
