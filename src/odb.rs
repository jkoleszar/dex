use std::fmt::{Display, Formatter};
use std::path::Path;

use ring::digest::{digest, SHA512_256};
use rusqlite::Connection;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("database error")]
    DbError(#[from] rusqlite::Error),
}

pub struct Key(String);
impl Display for Key {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        self.0.fmt(f)
    }
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

    pub fn insert_chunk(&self, data: &[u8]) -> Result<Key, Error> {
        let hash = digest(&SHA512_256, data);
        let id = format!("{:x?}", hash);
        self.conn.execute(
            "INSERT OR IGNORE INTO chunks (id, data) VALUES (?1, ?2)",
            (&id, data),
        )?;
        Ok(Key(id))
    }
}
