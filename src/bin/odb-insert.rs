use std::fs;
use std::io::Read;

use anyhow::Result;
use capnp::Word;
use clap::Parser;
use ring::digest::{digest, SHA512_256};
use rusqlite::Connection;

use dex::proto::odb_capnp::chunk;

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
    // chunk.
    let metadata = fs::metadata(&args.file)?;
    let mut message = ::capnp::message::Builder::new_default();
    let chunk = message.init_root::<chunk::Builder>();
    let data = chunk.init_data(metadata.len() as u32);
    let mut file = fs::File::open(&args.file)?;
    file.read(data)?;
    let serialized = message.into_reader().canonicalize()?;

    let hash = format!(
        "{:x?}",
        digest(&SHA512_256, Word::words_to_bytes(&serialized))
    );

    // Write the chunk to the database.
    let db = Connection::open(&args.db)?;
    db.execute(
        "CREATE TABLE IF NOT EXISTS chunks (
            id   TEXT PRIMARY KEY,
            data BLOB
        )",
        (), // empty list of parameters.
    )?;
    db.execute(
        "INSERT OR IGNORE INTO chunks (id, data) VALUES (?1, ?2)",
        (&hash, Word::words_to_bytes(&serialized)),
    )?;

    println!("{hash}");
    Ok(())
}
