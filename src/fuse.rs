use std::ffi::CStr;
use std::io;
use std::panic;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use capnp_rpc::rpc_twoparty_capnp;
use fuse_backend_rs::abi::fuse_abi::{CreateIn, OpenOptions, SetattrValid};
use fuse_backend_rs::api::filesystem::{
    AsyncFileSystem, AsyncZeroCopyReader, AsyncZeroCopyWriter, Context, Entry, FileSystem,
};
use fuse_backend_rs::transport::{FuseChannel, FuseSession};
use log;
use thiserror::Error;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::capnp::{duplex_stream_client, duplex_stream_server};
use crate::odb::ObjectId;
use crate::proto::odb_capnp::export_factory;

#[derive(Error, Debug)]
pub enum Error {
    #[error("fuse error")]
    FuseError(#[from] fuse_backend_rs::transport::Error),
}

/// Unsafe wrapper to force a type to be Sync
///
/// This is only needed because fuse-backend-rs has a seemingly misplaced
/// trait bound requiring FileSystem be Sync. The crate compiles fine when
/// that bound is removed, suggesting if it's needed at all it's by some
/// other dependency. Provided we understand their data model correctly,
/// it appears that the FileSystem is only called from the Server running
/// on the thread which calls {async_,}handle_message. It's desirable that
/// there be multiple FUSE workers which logically share a filesystem, but
/// in our case we would like there to be some thread-local !Sync data in
/// addition to the shared state.
///
/// SAFETY: Data wrapped by this type must not be shared across threads. Deref
/// this immediately to minimize the chance of misuse.
///
/// TODO: resolve this with upstream.
struct ThreadLocal<T>(T);
unsafe impl<T> Sync for ThreadLocal<T> {}

struct DexFS {
    _export: ThreadLocal<export_factory::Client>,
}

impl DexFS {
    fn new(export: export_factory::Client, _root: ObjectId) -> DexFS {
        DexFS {_export: ThreadLocal(export)}
    }
}

type FuseServer = fuse_backend_rs::api::server::Server<DexFS>;

impl FileSystem for DexFS {
    type Inode = u64;
    type Handle = u64;
}

#[async_trait]
impl AsyncFileSystem for DexFS {
    async fn async_lookup(
        &self,
        _ctx: &Context,
        _parent: <Self as FileSystem>::Inode,
        _name: &CStr,
    ) -> io::Result<Entry> {
        unimplemented!()
    }

    async fn async_getattr(
        &self,
        _ctx: &Context,
        _inode: <Self as FileSystem>::Inode,
        _handle: Option<<Self as FileSystem>::Handle>,
    ) -> io::Result<(libc::stat64, Duration)> {
        unimplemented!()
    }

    async fn async_setattr(
        &self,
        _ctx: &Context,
        _inode: <Self as FileSystem>::Inode,
        _attr: libc::stat64,
        _handle: Option<<Self as FileSystem>::Handle>,
        _valid: SetattrValid,
    ) -> io::Result<(libc::stat64, Duration)> {
        unimplemented!()
    }

    async fn async_open(
        &self,
        _ctx: &Context,
        _inode: <Self as FileSystem>::Inode,
        _flags: u32,
        _fuse_flags: u32,
    ) -> io::Result<(Option<<Self as FileSystem>::Handle>, OpenOptions)> {
        unimplemented!()
    }

    async fn async_create(
        &self,
        _ctx: &Context,
        _parent: <Self as FileSystem>::Inode,
        _name: &CStr,
        _args: CreateIn,
    ) -> io::Result<(Entry, Option<<Self as FileSystem>::Handle>, OpenOptions)> {
        unimplemented!()
    }

    #[allow(clippy::too_many_arguments)]
    async fn async_read(
        &self,
        _ctx: &Context,
        _inode: <Self as FileSystem>::Inode,
        _handle: <Self as FileSystem>::Handle,
        _w: &mut (dyn AsyncZeroCopyWriter + Send),
        _size: u32,
        _offset: u64,
        _lock_owner: Option<u64>,
        _flags: u32,
    ) -> io::Result<usize> {
        unimplemented!()
    }

    #[allow(clippy::too_many_arguments)]
    async fn async_write(
        &self,
        _ctx: &Context,
        _inode: <Self as FileSystem>::Inode,
        _handle: <Self as FileSystem>::Handle,
        _r: &mut (dyn AsyncZeroCopyReader + Send),
        _size: u32,
        _offset: u64,
        _lock_owner: Option<u64>,
        _delayed_write: bool,
        _flags: u32,
        _fuse_flags: u32,
    ) -> io::Result<usize> {
        unimplemented!()
    }

    async fn async_fsync(
        &self,
        _ctx: &Context,
        _inode: <Self as FileSystem>::Inode,
        _datasync: bool,
        _handle: <Self as FileSystem>::Handle,
    ) -> io::Result<()> {
        unimplemented!()
    }

    async fn async_fallocate(
        &self,
        _ctx: &Context,
        _inode: <Self as FileSystem>::Inode,
        _handle: <Self as FileSystem>::Handle,
        _mode: u32,
        _offset: u64,
        _length: u64,
    ) -> io::Result<()> {
        unimplemented!()
    }

    async fn async_fsyncdir(
        &self,
        ctx: &Context,
        inode: <Self as FileSystem>::Inode,
        datasync: bool,
        handle: <Self as FileSystem>::Handle,
    ) -> io::Result<()> {
        self.async_fsync(ctx, inode, datasync, handle).await
    }
}

async fn service_kernel(server: FuseServer, mut channel: FuseChannel) -> Result<(), Error> {
    while let Some((reader, writer)) = channel.get_request()? {
        // SAFETY: The fuse-backend-rs async io framework borrows underlying
        // buffers from Reader and Writer, so we must ensure they are valid
        // until the Future object returned has completed.
        if let Err(e) = unsafe {
            server
                .async_handle_message(reader, writer.into(), None, None)
                .await
        } {
            match e {
                fuse_backend_rs::Error::EncodeMessage(_) => {
                    // Kernel has shut down this session (EBADF).
                    break;
                }
                _ => {
                    log::error!("Handling fuse message failed");
                    continue;
                }
            }
        }
    }
    Ok(())
}

fn fuse_thread(
    fuse_channel: FuseChannel,
    export: export_factory::Client,
    root: ObjectId,
) -> JoinHandle<Result<(), Error>> {
    static THREAD_ID: AtomicUsize = AtomicUsize::new(0);
    let tid = THREAD_ID.fetch_add(1, Ordering::SeqCst);

    // capnp rpc clients may not be shared across threads, so create a
    // pipe between this thread and the fuse thread so that it can
    // issue RPCs.
    let (s1, s2) = tokio::io::duplex(4096);
    tokio::task::spawn_local(duplex_stream_server(s1, export.client));

    // The fuse service task is !Send, so may not be rescheduled
    // across threads, making a simple spawn() impossible. Instead,
    // spawn a thread with an independent runtime and start the
    // task within that context.
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let thread = std::thread::Builder::new()
        .name(format!("dexfs-fuse-{tid}"))
        .spawn(move || -> Result<(), Error> {
            let tid = std::thread::current().name().unwrap().to_string();
            log::debug!("thread {tid} started");

            // Create client side of RPC pipe
            let export = duplex_stream_client(s2).bootstrap(rpc_twoparty_capnp::Side::Server);

            // Create FUSE server.
            let fuse_server = FuseServer::new(DexFS::new(export, root));

            let local = tokio::task::LocalSet::new();
            local.spawn_local(service_kernel(fuse_server, fuse_channel));
            runtime.block_on(local);
            log::debug!("thread {tid} done");
            Ok(())
        })
        .unwrap();
    tokio::task::spawn_blocking(|| {
        thread
            .join()
            .map_err(|reason| panic::resume_unwind(reason))
            .unwrap()
    })
}

pub async fn run_fuse(
    mount_point: &Path,
    task_count: usize,
    export: export_factory::Client,
    root: ObjectId,
    until: CancellationToken,
) -> Result<(), Error> {
    let mut tasks: Vec<JoinHandle<Result<(), Error>>> = Vec::new();

    // Create FUSE session. Dropping this handle will unmount the filesystem.
    let mut fuse_session = FuseSession::new(
        mount_point,
        "dexfs",
        "",   /* subtype? */
        true, /* readonly */
    )?;
    fuse_session.mount()?;
    log::debug!("fuse session started.");

    // Tasks to service the kernel
    for _ in 0..task_count {
        let channel = fuse_session.new_channel()?;
        tasks.push(fuse_thread(channel, export.clone(), root));
    }

    // Wait on all tasks. If one returns an Error, the remaining
    // ones are detatched. The FuseSession will be dropped, which
    // will cause them to exit.
    for task in tasks.into_iter() {
        tokio::select! {
            r = task => match r {
                Ok(result) => result?,
                Err(join_error) => {
                    if let Ok(reason) = join_error.try_into_panic() {
                        // Resume the panic on the main task
                        panic::resume_unwind(reason);
                    }
                }
            },
            _ = until.cancelled() => break,
        }
    }
    Ok(())
}
