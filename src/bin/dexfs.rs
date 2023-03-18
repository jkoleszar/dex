use std::net::ToSocketAddrs;
use std::path::Path;

use anyhow::Result;
use capnp_rpc::{rpc_twoparty_capnp, twoparty, RpcSystem};
use clap::Parser;
use futures::AsyncReadExt;

use dex::odb::ObjectId;
use dex::proto::odb_capnp::export_factory;

const FUSE_TASK_COUNT: usize = 1;

/// FUSE mount a remote dex object
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Root object
    #[arg(long, required = true)]
    root: String,

    /// Mount dexfs at the given path
    mount_point: String,

    /// HOST:PORT to connect to
    remote: String,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    env_logger::init();

    let args = Args::parse();
    let addr = args
        .remote
        .to_socket_addrs()?
        .next()
        .expect("could not parse remote address");
    let root = ObjectId::parse(&args.root)?;

    tokio::task::LocalSet::new()
        .run_until(async move {
            // Connect to the export service
            let stream = tokio::net::TcpStream::connect(&addr).await?;
            stream.set_nodelay(true)?;
            let (reader, writer) =
                tokio_util::compat::TokioAsyncReadCompatExt::compat(stream).split();
            let rpc_network = Box::new(twoparty::VatNetwork::new(
                reader,
                writer,
                rpc_twoparty_capnp::Side::Client,
                Default::default(),
            ));
            let mut rpc_system = RpcSystem::new(rpc_network, None);
            let export: export_factory::Client =
                rpc_system.bootstrap(rpc_twoparty_capnp::Side::Server);

            // Run FUSE
            let fuse_handles =
                dex::fuse::run_fuse(Path::new(&args.mount_point), FUSE_TASK_COUNT, export, root)?;
            for task in fuse_handles.into_iter() {
                task.await??;
            }
            Ok(())
        })
        .await
}
