use std::fmt::{Debug, Display, Formatter};
use std::mem::MaybeUninit;
use std::num::TryFromIntError;

use capnp::message::{ReaderSegments, TypedReader};
use capnp::Word;
use hex;
use ring::digest::{digest, SHA512_256};
use rusqlite::{Connection, OptionalExtension};
use thiserror::Error;
use unwrap_infallible::UnwrapInfallible;

use crate::capnp::LazyTypedReader;
use crate::proto::odb_capnp::{object, object_id, stat};

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
}

/// Write an ObjectId into a capnp message.
pub trait ObjectIdIntoCapnp {
    fn from_oid(self, oid: &ObjectId);
}
impl<'a> ObjectIdIntoCapnp for object_id::Builder<'a> {
    fn from_oid(self, oid: &ObjectId) {
        match oid {
            ObjectId::SHA512_256(hash) => self.init_id(32).copy_from_slice(&hash[..]),
        };
    }
}

/// Read an ObjectId from a capnp message.
impl<'a> TryFrom<object_id::Reader<'a>> for ObjectId {
    type Error = Error;
    fn try_from(message: object_id::Reader<'a>) -> Result<ObjectId, Error> {
        let unchecked = message.get_id()?;

        // Convert a TryFromSliceError into a more specific serialization error.
        let checked: Result<[u8; 32], _> = unchecked.try_into();
        if let Ok(hash) = checked {
            Ok(ObjectId::SHA512_256(hash))
        } else {
            Err(capnp::Error::failed("missing or corrupt object id".to_string()).into())
        }
    }
}

#[derive(Error, Debug)]
pub enum Error {
    #[error("database error: {0}")]
    DbError(#[from] rusqlite::Error),

    #[error("object {0} not found")]
    Missing(ObjectId),

    #[error("serialization error: {0}")]
    SerializationError(#[from] capnp::Error),
}

pub struct ObjectDb<'a> {
    conn: &'a mut Connection,
}

pub type ObjectReader = LazyTypedReader<object::Owned>;

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

    pub fn insert_object<S: ReaderSegments, O: Into<TypedReader<S, object::Owned>>>(
        &self,
        object: O,
    ) -> Result<ObjectId, Error> {
        let object = object.into();
        let serialized = object.into_inner().canonicalize()?;
        let data = Word::words_to_bytes(&serialized);
        let hash = digest(&SHA512_256, data);
        let id = format!("{hash:x?}");
        self.conn.execute(
            "INSERT OR IGNORE INTO objects (id, data) VALUES (?1, ?2)",
            (&id, data),
        )?;
        Ok(ObjectId::SHA512_256(hash.as_ref().try_into().unwrap()))
    }

    pub fn get_object(&self, key: &ObjectId) -> Result<ObjectReader, Error> {
        let data = self
            .conn
            .query_row(
                "SELECT data FROM objects WHERE id=(?)",
                [key.to_string()],
                |row| row.get(0),
            )
            .optional()?
            .ok_or(Error::Missing(*key))?;

        // Attach our known type to it.
        Ok(LazyTypedReader::<object::Owned>::new(data))
    }

    // TODO: might be nice to have an RAII Transaction wrapper, but at that
    // point, it's probably worth bringing in something third party like
    // diesel.
    pub fn begin(&self) -> Result<(), Error> {
        self.conn.execute("BEGIN", ())?;
        Ok(())
    }

    pub fn commit(&self) -> Result<(), Error> {
        self.conn.execute("COMMIT", ())?;
        Ok(())
    }
}

// Popuplate a stat64 struct from a capnp Entry
#[derive(Debug, Error)]
pub enum StatFromCapnpError {
    #[error("{source} while decoding {context}")]
    OutOfRange {
        source: TryFromIntError,
        context: String,
    },
    #[error("schema violation: {0}")]
    NotInSchema(#[from] capnp::NotInSchema),
}

pub struct StatWithUnknownInode(libc::stat64);

impl StatWithUnknownInode {
    pub fn finalize(mut self, inode: u64) -> libc::stat64 {
        self.0.st_ino = inode;
        self.0
    }
}

#[cfg(target_os = "linux")]
const MINORBITS: usize = 20;

#[cfg(target_os = "linux")]
pub fn mkdev(major: u64, minor: u64) -> libc::dev_t {
    (major << MINORBITS) | minor
}

impl<'a> TryFrom<stat::Reader<'a>> for StatWithUnknownInode {
    type Error = StatFromCapnpError;

    fn try_from(entry: stat::Reader<'a>) -> Result<StatWithUnknownInode, StatFromCapnpError> {
        // SAFETY: Unfortunately, we can't use a struct initializer here because of
        // some private padding fields. Applications are expected to zero this
        // structure according to the ABI.
        let mut stat = unsafe { MaybeUninit::<libc::stat64>::zeroed().assume_init() };
        stat.st_dev = 0;
        stat.st_nlink = std::cmp::min(1, entry.get_st_nlink());

        stat.st_mode = entry.get_st_mode()
            | match entry.get_kind()? {
                stat::Kind::Block => libc::S_IFBLK,
                stat::Kind::Char => libc::S_IFCHR,
                stat::Kind::Fifo => libc::S_IFIFO,
                stat::Kind::Regular => libc::S_IFREG,
                stat::Kind::Dir => libc::S_IFDIR,
                stat::Kind::Symlink => libc::S_IFLNK,
            };
        stat.st_uid = entry.get_st_uid();
        stat.st_gid = entry.get_st_gid();
        stat.st_rdev = mkdev(
            entry.get_device_major().into(),
            entry.get_device_minor().into(),
        );
        stat.st_size =
            entry
                .get_st_size()
                .try_into()
                .map_err(|e| StatFromCapnpError::OutOfRange {
                    source: e,
                    context: "st_size".to_string(),
                })?;
        stat.st_blksize = 4096;
        stat.st_blocks = (stat.st_size + 511) / 512;
        stat.st_mtime =
            entry
                .get_st_mtime()
                .try_into()
                .map_err(|e| StatFromCapnpError::OutOfRange {
                    source: e,
                    context: "st_mtime".to_string(),
                })?;
        stat.st_mtime_nsec = entry.get_st_mtime_nsec().try_into().unwrap_infallible();
        stat.st_ctime =
            entry
                .get_st_ctime()
                .try_into()
                .map_err(|e| StatFromCapnpError::OutOfRange {
                    source: e,
                    context: "st_ctime".to_string(),
                })?;
        stat.st_ctime_nsec = entry.get_st_ctime_nsec().try_into().unwrap_infallible();
        stat.st_atime = stat.st_mtime;
        stat.st_atime_nsec = stat.st_mtime_nsec;

        // The following fields remain unset:
        // stat.st_ino
        // stat.st_nlink
        Ok(StatWithUnknownInode(stat))
    }
}
