use std::fs;
use std::io::Read;

use anyhow::Result;
use capnp::Word;
use clap::Parser;

use dex::odb::ObjectDb;
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

    // Write the chunk to the database.
    let db = ObjectDb::new(&args.db)?;
    let hash = db.insert_chunk(Word::words_to_bytes(&serialized))?;
    println!("{hash}");
    Ok(())
}
