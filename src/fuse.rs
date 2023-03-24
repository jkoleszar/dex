use std::ffi::CStr;
use std::io;
use std::panic;
use std::path::Path;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use anyhow::{anyhow, Context as AnyhowContext};
use async_trait::async_trait;
use fuse_backend_rs::abi::fuse_abi::{CreateIn, OpenOptions, SetattrValid};
use fuse_backend_rs::api::filesystem::{
    AsyncFileSystem, AsyncZeroCopyReader, AsyncZeroCopyWriter, Context, Entry, FileSystem,
};
use fuse_backend_rs::transport::{FuseChannel, FuseSession};
use libc::{EIO, ENOENT};
use log;
use thiserror::Error;
use tokio::sync::mpsc;
use tokio::sync::Mutex as AsyncMutex;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::odb::{ObjectId, Stat};
use crate::odb_readthrough;
use crate::odb_readthrough::LazyObject;
use crate::proto::odb_capnp::{object, tree};

#[derive(Error, Debug)]
pub enum Error {
    #[error("fuse error")]
    FuseError(#[from] fuse_backend_rs::transport::Error),

    #[error("database error")]
    DbError(#[from] rusqlite::Error),
}

#[derive(Error, Debug)]
pub enum InodeError {
    #[error("entry {0} missing from inode map")]
    Invalid(u64),
}

/// An InodeEntry that may be shared between threads.
struct InodeEntry {
    oid: ObjectId,
    object: LazyObject,
    parent_inode: u64,
    stat: Option<Stat>,
}

// Populate an InodeEntry from a capnp message.
impl<'a> TryFrom<tree::entry::Reader<'a>> for InodeEntry {
    type Error = anyhow::Error;

    fn try_from(entry: tree::entry::Reader<'a>) -> Result<InodeEntry, anyhow::Error> {
        let oid: ObjectId = entry.get_oid()?.try_into()?;
        Ok(InodeEntry {
            oid: oid.clone(),
            object: LazyObject::new(oid),
            parent_inode: 0,
            stat: Some(entry.clone().try_into()?),
        })
    }
}

type SharedInodeEntry = Arc<AsyncMutex<InodeEntry>>;

/// DexFS state shared across all workers
struct SharedState {
    /// Connection to object database
    odb: mpsc::Sender<odb_readthrough::Request>,

    /// Indexed by inode
    inodes: RwLock<Vec<Option<SharedInodeEntry>>>,
}

impl SharedState {
    fn new(odb: mpsc::Sender<odb_readthrough::Request>, root: ObjectId) -> Arc<SharedState> {
        let state = SharedState {
            odb: odb.clone(),
            inodes: Vec::new().into(),
        };

        // inode 0 is unused
        state.inodes.write().unwrap().push(None);

        // inode 1 is the root object
        let entry = InodeEntry {
            oid: root.clone(),
            object: LazyObject::new(root),
            parent_inode: 0,
            stat: Some(Stat::new_root()),
        };
        Self::alloc_inode(&state.inodes, entry);

        Arc::new(state)
    }

    fn alloc_inode(inodes: &RwLock<Vec<Option<SharedInodeEntry>>>, entry: InodeEntry) -> usize {
        let mut vec = inodes.write().unwrap();
        let ino = vec.len();
        vec.push(Some(Arc::new(AsyncMutex::new(entry))));
        ino
    }

    fn inode_index(inode: u64) -> Result<usize, anyhow::Error> {
        inode
            .try_into()
            .map_err(|_| InodeError::Invalid(inode).into())
    }

    fn populate_tree<'a>(
        &self,
        tree: tree::Reader<'a>,
        parent: u64,
    ) -> Result<Vec<usize>, anyhow::Error> {
        let inodes = &self.inodes;
        let entries: Result<Vec<usize>, anyhow::Error> = tree
            .get_entries()?
            .iter()
            .map(|entry| {
                let mut new_entry: InodeEntry = entry.try_into()?;
                new_entry.parent_inode = parent;
                Ok(Self::alloc_inode(inodes, new_entry))
            })
            .collect();
        Ok(entries?)
    }

    async fn get_inode(&self, inode: u64) -> Result<SharedInodeEntry, anyhow::Error> {
        let shared_entry = self
            .inodes
            .read()
            .unwrap() // assume the lock was not poisoned.
            .get(Self::inode_index(inode)?)
            .ok_or(InodeError::Invalid(inode))
            .context("inode was never allocated")?
            .as_ref()
            .ok_or(InodeError::Invalid(inode))
            .context("inode was deleted")?
            .clone();

        let mut entry = shared_entry.lock().await;

        // Check that the entry is valid.
        let initially_ok = matches!(entry.object, LazyObject::Ok(_));
        let object = match entry.object.get(&self.odb).await {
            LazyObject::Error(e) => {
                // TODO: LazyObject::Error currently has a restricted lifetime. This
                // may go away if that module ends up switching to use anyhow.
                Err(anyhow!("LazyObject::Error: {e:?}").context(format!("get_inode({inode})")))
            }
            LazyObject::Missing(oid) => Err(FuseError::new(ENOENT))
                .context(format!("get_inode({inode}) refers to missing oid {oid}")),
            LazyObject::Ok(o) => Ok(o),
            LazyObject::Future(_) | LazyObject::Pending(_) => unreachable!(),
        }?;

        // Populate our child inodes if necessary.
        if !initially_ok {
            if let object::Tree(tree) = object
                .reader()
                .get()
                .map_err(|e| {
                    anyhow::Error::from(e)
                        .context(format!("corrupt object at ino {inode} oid {}", entry.oid))
                })?
                .which()
                .map_err(|e| {
                    anyhow::Error::from(e).context(format!(
                        "unknown object type at ino {inode} oid {}",
                        entry.oid
                    ))
                })?
            {
                tree.map_err(anyhow::Error::from)
                    .and_then(|tree| Ok(self.populate_tree(tree, entry.parent_inode)?))
                    .map_err(|e| {
                        anyhow::Error::from(e)
                            .context(format!("corrupt tree at ino {inode} oid {}", entry.oid))
                    })?;
            }
        }

        // TODO: It seems like we can't return a tokio MutexGuard like you can
        // an ordinary sync mutex? The caller will have to re-lock.
        drop(entry);
        Ok(shared_entry)
    }

    async fn stat(&self, inode: u64) -> anyhow::Result<libc::stat64> {
        let shared_child = self.get_inode(inode).await?;
        let child = shared_child.lock().await;
        if let Some(stat) = child.stat {
            return Ok(stat.clone().into());
        }
        // Stat has not been populated.

        Err(anyhow!("inode {inode} has unset stat structure."))
    }
}

const TTL: Duration = Duration::from_secs(1);

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

/// An error that will be returned to the FileSystem/end user.
///
/// This lets us distinguish between an unexpected io::Error that was
/// generated somewhere in our processing from an io::Error that we
/// intend to be returned as-is to the FUSE layer.
#[derive(Debug, Error)]
#[error(transparent)]
struct FuseError(io::Error);
impl FuseError {
    fn new(code: i32) -> Self {
        FuseError(io::Error::from_raw_os_error(code))
    }
}
impl From<FuseError> for io::Error {
    fn from(value: FuseError) -> io::Error {
        value.0
    }
}
impl From<InodeError> for FuseError {
    fn from(value: InodeError) -> FuseError {
        match value {
            InodeError::Invalid(_) => FuseError::new(ENOENT),
        }
    }
}
impl From<anyhow::Error> for FuseError {
    fn from(value: anyhow::Error) -> FuseError {
        for e in value.chain() {
            // Walk the error chain to see if any of them specified FuseError,
            // and if so, copy it. The underlying io::Error is not copyable,
            // but FuseError can only be created via raw os errors, so this is
            // safe.
            if let Some(err) = e.downcast_ref::<FuseError>() {
                log::debug!("{value:?}");
                return FuseError::new(err.0.raw_os_error().unwrap());
            }
        }
        log::error!("{value:?}");
        FuseError::new(EIO)
    }
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
        let result = self
            .shared
            .stat(inode)
            .await
            .map(|stat| (stat, TTL))
            .map_err(FuseError::from);
        log::trace!("async_getattr: {:?}", result);
        Ok(result?)
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
