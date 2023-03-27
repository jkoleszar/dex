use std::collections::HashMap;
use std::ffi::{OsStr, OsString};
use std::fs;
use std::fs::File;
use std::io::Read;
use std::path::{Component, Path, PathBuf};

use anyhow::{bail, Context, Result};
use capnp::message::TypedBuilder;
use clap::Parser;
use rusqlite::Connection;
use tar::{Archive, EntryType, Header};

use dex::odb::{ObjectDb, ObjectId, ObjectIdIntoCapnp};
use dex::proto::odb_capnp::{object, stat};

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

fn set_stat(mut stat: stat::Builder, header: &tar::Header) -> Result<()> {
    stat.set_kind(match header.entry_type() {
        EntryType::Block => stat::Kind::Block,
        EntryType::Char => stat::Kind::Char,
        EntryType::Fifo => stat::Kind::Fifo,
        EntryType::Regular => stat::Kind::Regular,
        EntryType::Directory => stat::Kind::Dir,
        EntryType::Symlink => stat::Kind::Symlink,
        EntryType::Link => todo!(),
        EntryType::GNUSparse => todo!(),
        _ => unimplemented!(),
    });
    stat.set_st_mode(header.mode()?);

    // TODO: count hard links for real.
    stat.set_st_nlink(1);

    stat.set_st_uid(header.uid()?.try_into().context("uid".to_string())?);
    stat.set_st_gid(header.gid()?.try_into().context("gid".to_string())?);
    stat.set_st_size(header.size()?);
    let mtime = header.mtime()?;
    stat.set_st_mtime(mtime);
    stat.set_st_ctime(
        header
            .as_gnu()
            .and_then(|gnu| gnu.ctime().ok())
            .unwrap_or(mtime),
    );
    match header.entry_type() {
        EntryType::Block | EntryType::Char => {
            if let Some(major) = header.device_major()? {
                stat.set_device_major(major);
            }
            if let Some(minor) = header.device_minor()? {
                stat.set_device_major(minor);
            }
        }
        _ => (),
    }
    Ok(())
}

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

        if let Some(oid) = oid {
            entry.reborrow().init_oid().from_oid(oid)
        } else {
            bail!("Could not find entry OID for {path:?}");
        }

        set_stat(entry.reborrow().init_stat(), header)?;
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

    // Insert the tree into the database
    let root_path = OsString::from("/");
    let tree_oid = insert_directory(db, &by_dir, &root_path, &headers, &files)?;

    // Need to create a supplemental "TreeRoot" object containing the tree and
    // its stat data.
    if let object::Tree(tree) = db.get_object(&tree_oid)?.reader().get()?.which()? {
        let mut object = TypedBuilder::<object::Owned>::new_default();
        let mut root = object.init_root().init_tree_root();
        root.set_tree(tree?)?;
        set_stat(root.init_stat(), headers.get(&root_path).unwrap())?;
        db.insert_object(object).map_err(Into::into)
    } else {
        unreachable!()
    }
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
