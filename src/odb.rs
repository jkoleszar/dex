use std::fmt::{Debug, Display, Formatter};
use std::path::Path;

use ring::digest::{digest, SHA512_256};
use rusqlite::Connection;
use thiserror::Error;

#[derive(Copy, Clone, Debug)]
pub enum ObjectId {
    SHA512_256([u8;32])
}
impl Display for ObjectId {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        let (label, hash) = match &self {
            ObjectId::SHA512_256(h) => ("SHA512_256", h),
        };
        write!(f, "{label}:")?;
        for byte in hash {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

#[derive(Error, Debug)]
pub enum Error {
    #[error("database error")]
    DbError(#[from] rusqlite::Error),
}

pub struct ObjectDb {
    conn: Connection,
}

impl ObjectDb {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
        let conn = Connection::open(path)?;
        conn.execute(
            "CREATE TABLE IF NOT EXISTS chunks (
                id   TEXT PRIMARY KEY,
                data BLOB
            )",
            (), // empty list of parameters.
        )?;
        Ok(ObjectDb { conn })
    }

    pub fn insert_chunk(&self, data: &[u8]) -> Result<ObjectId, Error> {
        let hash = digest(&SHA512_256, data);
        let id = format!("{:x?}", hash);
        self.conn.execute(
            "INSERT OR IGNORE INTO chunks (id, data) VALUES (?1, ?2)",
            (&id, data),
        )?;
        Ok(ObjectId::SHA512_256(hash.as_ref().try_into().unwrap()))
    }
}
