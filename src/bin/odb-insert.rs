use std::collections::HashMap;
use std::ffi::{OsStr, OsString};
use std::fs;
use std::fs::File;
use std::io::Read;
use std::path::{Component, Path, PathBuf};

use anyhow::{bail, Result};
use capnp::message::TypedBuilder;
use clap::Parser;
use rusqlite::Connection;
use tar::{Archive, EntryType, Header};

use dex::odb::{ObjectDb, ObjectId};
use dex::proto::odb_capnp::{object, tree};

/// Import files into the object store
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Database file
    #[arg(long, required = true)]
    db: String,

    /// File to import
    #[arg(short, long, required = true)]
    file: String,

    /// Extract TAR file
    #[arg(short = 'x')]
    extract: bool,
}

struct MapEntry(HashMap<OsString, MapEntry>);

fn insert_directory(
    db: &ObjectDb,
    map: &MapEntry,
    path: &OsStr,
    headers: &HashMap<OsString, tar::Header>,
    files: &HashMap<OsString, Option<ObjectId>>,
) -> Result<ObjectId> {
    println!("{path:?}");
    let entries: Result<Vec<(&OsStr, Option<ObjectId>, &Header)>> = map
        .0
        .iter()
        .map(
            |(filename, child)| -> Result<(&OsStr, Option<ObjectId>, &Header)> {
                let reconstructed_path = PathBuf::from(path).join(filename);
                let reconstructed_path_str = reconstructed_path.as_path().as_os_str();

                let header = headers.get(reconstructed_path_str).unwrap();
                let oid = match header.entry_type() {
                    EntryType::Directory => Some(insert_directory(
                        db,
                        child,
                        reconstructed_path_str,
                        headers,
                        files,
                    )?),
                    _ => *files.get(reconstructed_path_str).unwrap(),
                };
                Ok((filename, oid, headers.get(reconstructed_path_str).unwrap()))
            },
        )
        .collect();
    let mut entries = entries?;

    // Sort by name.
    entries.sort_unstable_by(|a, b| a.0.cmp(b.0));

    // Construct object
    let mut object = TypedBuilder::<object::Owned>::new_default();
    let mut tree = object
        .init_root()
        .init_tree()
        .init_entries(entries.len() as u32);
    for (i, (path, oid, header)) in entries.iter().enumerate() {
        let mut entry = tree.reborrow().get(i.try_into()?);
        if let Some(filename) = path.to_str() {
            entry
                .reborrow()
                .init_name(filename.len().try_into()?)
                .push_str(filename);
        } else {
            bail!("Path {path:?} is not valid UTF-8");
        }

        if let Some(ObjectId::SHA512_256(hash)) = oid {
            entry
                .reborrow()
                .init_oid()
                .init_id(32)
                .copy_from_slice(&hash[..])
        } else {
            bail!("Could not find entry OID for {path:?}");
        }

        match header.entry_type() {
            EntryType::Directory => tree
                .reborrow()
                .get(i.try_into()?)
                .set_kind(tree::entry::Kind::Dir),
            EntryType::Regular => tree
                .reborrow()
                .get(i.try_into()?)
                .set_kind(tree::entry::Kind::File),
            EntryType::Symlink => tree
                .reborrow()
                .get(i.try_into()?)
                .set_kind(tree::entry::Kind::Symlink),
            _ => bail!(
                "Unsupported entry type {:?} for {path:?}",
                header.entry_type()
            ),
        };
    }

    // Write the object to the database.
    db.insert_object(object).map_err(Into::into)
}

fn insert_archive<P: AsRef<Path>>(db: &ObjectDb, file: P) -> Result<ObjectId> {
    let mut archive = Archive::new(File::open(&file)?);
    let mut by_dir = MapEntry(HashMap::new());
    let mut headers: HashMap<OsString, tar::Header> = HashMap::new();
    let mut files: HashMap<OsString, Option<ObjectId>> = HashMap::new();
    for entry in archive.entries()? {
        let mut entry = entry?;
        let path = entry.header().path()?;

        // Normalize paths
        let norm = PathBuf::from_iter(PathBuf::from("/").join(path).components());

        // Collapse paths into a recursive map. We will use this later to
        // reconstruct the directory structure.
        headers.insert(norm.clone().into(), entry.header().clone());
        let mut map = &mut by_dir;
        for path in norm.components() {
            let path = match path {
                Component::RootDir => continue,
                Component::Normal(p) => p,
                _ => bail!("Unexpected path fragment in {norm:?}"),
            };
            if !map.0.contains_key(path) {
                map.0.insert(path.clone().into(), MapEntry(HashMap::new()));
            }
            map = map.0.get_mut(path).unwrap();
        }

        // Insert ordinary files into the database
        let len = entry.header().size()?;
        let oid = match entry.header().entry_type() {
            EntryType::Regular | EntryType::Symlink => {
                println!("Inserting file {norm:?}");
                Some(insert_file(db, &mut entry, len)?)
            }
            _ => None,
        };
        files.insert(norm.clone().into(), oid);
    }
    insert_directory(db, &by_dir, &OsString::from("/"), &headers, &files)
}

fn insert_file<P: Read>(db: &ObjectDb, file: &mut P, len: u64) -> Result<ObjectId> {
    // Read the file from disk. For now we'll just read this into one big
    // blob.
    let mut object = TypedBuilder::<object::Owned>::new_default();
    let blob = object.init_root().init_blob(len as u32);
    file.read_exact(blob)?;
    db.insert_object(object).map_err(Into::into)
}

fn main() -> Result<()> {
    let args = Args::parse();

    let mut conn = Connection::open(&args.db)?;
    let db = ObjectDb::new(&mut conn);
    db.create()?;

    db.begin()?;
    let oid = if args.extract {
        insert_archive(&db, &args.file)?
    } else {
        let mut file = fs::File::open(&args.file)?;
        let metadata = fs::metadata(&args.file)?;
        insert_file(&db, &mut file, metadata.len())?
    };
    db.commit()?;

    println!("{oid}");
    Ok(())
}
