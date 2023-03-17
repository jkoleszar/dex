use std::net::ToSocketAddrs;

use anyhow::Result;
use capnp_rpc::{rpc_twoparty_capnp, twoparty, RpcSystem};
use clap::Parser;
use futures::AsyncReadExt;
use tokio_rusqlite::Connection;

use dex::odb::ObjectDb;
use dex::odb_rpc::ExportFactory;
use dex::proto::odb_capnp::export_factory;

/// Import files into the object store
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Database file
    #[arg(long, required = true)]
    db: String,

    /// HOST:PORT to listen on
    listen_address: String,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let args = Args::parse();
    let addr = args
        .listen_address
        .to_socket_addrs()?
        .next()
        .expect("could not parse address to listen on");

    tokio::task::LocalSet::new()
        .run_until(async move {
            let conn = Connection::open(&args.db).await?;
            conn.call(|conn| ObjectDb::new(conn).create()).await?;
            let listener = tokio::net::TcpListener::bind(&addr).await?;
            let factory: export_factory::Client = capnp_rpc::new_client(ExportFactory::new(conn));

            loop {
                let (stream, _) = listener.accept().await?;
                stream.set_nodelay(true)?;
                let (reader, writer) =
                    tokio_util::compat::TokioAsyncReadCompatExt::compat(stream).split();
                let network = twoparty::VatNetwork::new(
                    reader,
                    writer,
                    rpc_twoparty_capnp::Side::Server,
                    Default::default(),
                );
                let rpc_system = RpcSystem::new(Box::new(network), Some(factory.clone().client));

                tokio::task::spawn_local(rpc_system);
            }
        })
        .await
}
