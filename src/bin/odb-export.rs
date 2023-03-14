use std::io;
use std::io::Write;

use anyhow::Result;
use clap::Parser;

use dex::odb::{ObjectDb, ObjectId};

/// Import files into the object store
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Database file
    #[arg(long, required = true)]
    db: String,

    /// Object to export
    oid: String,
}

fn main() -> Result<()> {
    let args = Args::parse();

    let db = ObjectDb::new(&args.db)?;
    let oid = ObjectId::parse(&args.oid)?;
    let chunk = db.get_chunk_encoded(&oid)?;
    io::stdout().write_all(&chunk)?;
    Ok(())
}
