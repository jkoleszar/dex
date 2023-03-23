use std::ffi::CStr;
use std::io;
use std::panic;
use std::path::Path;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use async_trait::async_trait;
use fuse_backend_rs::abi::fuse_abi::{CreateIn, OpenOptions, SetattrValid};
use fuse_backend_rs::api::filesystem::{
    AsyncFileSystem, AsyncZeroCopyReader, AsyncZeroCopyWriter, Context, Entry, FileSystem,
};
use fuse_backend_rs::transport::{FuseChannel, FuseSession};
use log;
use thiserror::Error;
use tokio::sync::mpsc;
use tokio::sync::Mutex as AsyncMutex;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::odb::ObjectId;
use crate::odb_readthrough;
use crate::odb_readthrough::LazyObject;

#[derive(Error, Debug)]
pub enum Error {
    #[error("fuse error")]
    FuseError(#[from] fuse_backend_rs::transport::Error),

    #[error("database error")]
    DbError(#[from] rusqlite::Error),
}

#[derive(Error, Debug)]
pub enum InodeError {
    #[error("entry missing from inode map")]
    Invalid(u64),
}

impl From<InodeError> for io::Error {
    fn from(value: InodeError) -> io::Error {
        match value {
            InodeError::Invalid(_) => enoent(),
        }
    }
}

/// An InodeEntry that may be shared between threads.
struct InodeEntry {
    object: LazyObject,
}

type SharedInodeEntry = Arc<AsyncMutex<InodeEntry>>;

/// DexFS state shared across all workers
struct SharedState {
    /// Connection to object database
    _odb: mpsc::Sender<odb_readthrough::Request>,

    /// Indexed by inode
    inodes: RwLock<Vec<Option<SharedInodeEntry>>>,
}

impl SharedState {
    fn new(odb: mpsc::Sender<odb_readthrough::Request>, root: ObjectId) -> Arc<SharedState> {
        let mut state = SharedState {
            _odb: odb.clone(),
            inodes: Vec::new().into(),
        };

        // inode 0 is unused
        state.alloc_inode();

        // inode 1 is the root object
        let ino = state.alloc_inode();
        let entry = Arc::new(AsyncMutex::new(InodeEntry {
            object: LazyObject::new(root, odb.clone()),
        }));
        state.inodes.write().unwrap()[ino] = Some(entry);

        Arc::new(state)
    }

    fn alloc_inode(&mut self) -> usize {
        let mut vec = self.inodes.write().unwrap();
        let ino = vec.len();
        vec.push(None);
        ino
    }

    fn inode_index(inode: u64) -> Result<usize, InodeError> {
        inode.try_into().map_err(|_| InodeError::Invalid(inode))
    }

    async fn get_inode(&self, inode: u64) -> Result<SharedInodeEntry, io::Error> {
        let shared_entry = self
            .inodes
            .read()
            .unwrap() // assume the lock was not poisoned.
            .get(Self::inode_index(inode)?)
            .ok_or(InodeError::Invalid(inode))? // inode was never allocated
            .as_ref()
            .ok_or(InodeError::Invalid(inode))? // inode was deleted
            .clone();

        let mut entry = shared_entry.lock().await;

        // Check that the entry is valid.
        match entry.object.get().await {
            LazyObject::Error(e) => {
                log::error!("get_inode: {e}");
                return Err(eio());
            }
            LazyObject::Missing(oid) => {
                log::error!("get_inode: inode {inode} refers to missing oid {oid}");
                return Err(enoent());
            }
            LazyObject::Ok(o) => o,
            LazyObject::Future(_, _) | LazyObject::Pending(_) => unreachable!(),
        };

        // TODO: It seems like we can't return a tokio MutexGuard like you can
        // an ordinary sync mutex? The caller will have to re-lock.
        drop(entry);
        Ok(shared_entry)
    }
}

struct DexFS {
    /// Shared state
    shared: Arc<SharedState>,
}

impl DexFS {
    fn new(shared: Arc<SharedState>) -> DexFS {
        DexFS { shared }
    }
}

type FuseServer = fuse_backend_rs::api::server::Server<DexFS>;

impl FileSystem for DexFS {
    type Inode = u64;
    type Handle = u64;
}

fn eio() -> io::Error {
    io::Error::from_raw_os_error(libc::EIO)
}

fn enoent() -> io::Error {
    io::Error::from_raw_os_error(libc::ENOENT)
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
        inode: <Self as FileSystem>::Inode,
        _handle: Option<<Self as FileSystem>::Handle>,
    ) -> io::Result<(libc::stat64, Duration)> {
        let shared_entry = self.shared.get_inode(inode).await?;
        let _entry = shared_entry.lock().await;
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

pub async fn run_fuse(
    mount_point: &Path,
    task_count: usize,
    odb: mpsc::Sender<odb_readthrough::Request>,
    root: ObjectId,
    until: CancellationToken,
) -> Result<(), Error> {
    let mut tasks: Vec<JoinHandle<Result<(), Error>>> = Vec::new();

    // Initialize state shared across all workers.
    let fs_shared_state = SharedState::new(odb, root);

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
    for tid in 0..task_count {
        let channel = fuse_session.new_channel()?;
        //tasks.push(fuse_thread(channel, Arc::clone(&fs_shared_state)));
        let shared = Arc::clone(&fs_shared_state);

        // TODO: It seems like this should be spawn() not spawn_local() as
        // we want the task to run across different workers, but
        // async_handle_message takes a Option<&dyn MetricsHook> parameter
        // which is not Send.
        tasks.push(tokio::task::spawn_local(async move {
            log::debug!("dexfs-fuse-{tid} task started");
            let server = FuseServer::new(DexFS::new(shared));
            let r = service_kernel(server, channel).await;
            log::debug!("dexfs-fuse-{tid} task exited: {r:?}");
            r
        }));
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
