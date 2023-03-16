use std::fs;
use std::io::Read;

use anyhow::Result;
use capnp::Word;
use clap::Parser;
use rusqlite::Connection;

use dex::odb::ObjectDb;
use dex::proto::odb_capnp::object;

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
}

fn main() -> Result<()> {
    let args = Args::parse();

    // Read the file from disk. For now we'll just read this into one big
    // blob.
    let metadata = fs::metadata(&args.file)?;
    let mut message = ::capnp::message::Builder::new_default();
    let object = message.init_root::<object::Builder>();
    let blob = object.init_blob(metadata.len() as u32);
    let mut file = fs::File::open(&args.file)?;
    file.read(blob)?;
    let serialized = message.into_reader().canonicalize()?;

    // Write the blob to the database.
    let mut conn = Connection::open(&args.db)?;
    let db = ObjectDb::new(&mut conn);
    db.create()?;
    let oid = db.insert_object(Word::words_to_bytes(&serialized))?;

    println!("{oid}");
    Ok(())
}
