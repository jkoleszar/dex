use anyhow::Result;
use clap::Parser;
use tokio_rusqlite::Connection;

use dex::odb::{ObjectDb, ObjectId, ObjectIdIntoCapnp};
use dex::odb_rpc::{Exporter, ImportToStdout};
use dex::proto::odb_capnp::{export, import};

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

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let args = Args::parse();
    let oid = ObjectId::parse(&args.oid)?;

    tokio::task::LocalSet::new()
        .run_until(async move {
            let conn = Connection::open(&args.db).await?;
            conn.call(|conn| ObjectDb::new(conn).create()).await?;
            let exporter: export::Client = capnp_rpc::new_client(Exporter::new(conn));
            let importer: import::Client = capnp_rpc::new_client(ImportToStdout);

            let mut want_req = exporter.want_request();
            want_req.get().init_id().from_oid(&oid);
            want_req.send().promise.await?;

            let mut begin_req = exporter.begin_request();
            begin_req.get().set_import(importer);
            begin_req.send().promise.await?;

            Ok(())
        })
        .await
}
