use std::fmt::{Debug, Display, Formatter};

use hex;
use ring::digest::{digest, SHA512_256};
use rusqlite::{Connection, OptionalExtension};
use thiserror::Error;

#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq)]
pub enum ObjectId {
    SHA512_256([u8; 32]),
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
impl ObjectId {
    pub fn parse(s: &str) -> Result<ObjectId, hex::FromHexError> {
        let mut encoded = [0; 32];
        hex::decode_to_slice(s, &mut encoded)?;
        Ok(ObjectId::SHA512_256(encoded))
    }
    pub fn to_string(&self) -> String {
        format!("{self}")
    }
}

#[derive(Error, Debug)]
pub enum Error {
    #[error("database error")]
    DbError(#[from] rusqlite::Error),

    #[error("object {0} not found")]
    Missing(ObjectId),
}

pub struct ObjectDb<'a> {
    conn: &'a mut Connection,
}

impl<'a> ObjectDb<'a> {
    pub fn new(conn: &'a mut Connection) -> Self {
        ObjectDb { conn }
    }

    pub fn create(&self) -> Result<(), Error> {
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS objects (
                id   TEXT PRIMARY KEY,
                data BLOB
            )",
            (), // empty list of parameters.
        )?;
        Ok(())
    }
    pub fn insert_object(&self, data: &[u8]) -> Result<ObjectId, Error> {
        let hash = digest(&SHA512_256, data);
        let id = format!("{hash:x?}");
        self.conn.execute(
            "INSERT OR IGNORE INTO objects (id, data) VALUES (?1, ?2)",
            (&id, data),
        )?;
        Ok(ObjectId::SHA512_256(hash.as_ref().try_into().unwrap()))
    }

    pub fn get_object_encoded(&self, key: &ObjectId) -> Result<Vec<u8>, Error> {
        self.conn
            .query_row(
                "SELECT data FROM objects WHERE id=(?)",
                [key.to_string()],
                |row| row.get(0),
            )
            .optional()?
            .ok_or(Error::Missing(*key))
    }
}
