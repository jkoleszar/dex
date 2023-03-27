use std::ffi::CStr;
use std::io;
use std::panic;
use std::path::Path;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use anyhow::{anyhow, bail, Context as AnyhowContext, Result};
use async_trait::async_trait;
use fuse_backend_rs::abi::fuse_abi::{CreateIn, OpenOptions, SetattrValid};
use fuse_backend_rs::api::filesystem::{
    AsyncFileSystem, AsyncZeroCopyReader, AsyncZeroCopyWriter, Context, DirEntry, Entry,
    FileSystem, FsOptions,
};
use fuse_backend_rs::transport::{FuseChannel, FuseSession};
use libc::{EIO, ENOENT};
use log;
use thiserror::Error;
use tokio::sync::mpsc;
use tokio::sync::Mutex as AsyncMutex;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::odb::{ObjectId, ObjectReader, StatWithUnknownInode};
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
    object: AsyncMutex<LazyObject>,
    parent_inode: u64,
    stat: libc::stat64,
    filename: String,
    children: Vec<usize>,
}

type SharedInodeEntry = Arc<InodeEntry>;

struct InodeTable(RwLock<Vec<Option<SharedInodeEntry>>>);
impl InodeTable {
    fn new() -> Self {
        InodeTable(Vec::new().into())
    }

    fn index(inode: u64) -> Result<usize, anyhow::Error> {
        inode
            .try_into()
            .map_err(|_| InodeError::Invalid(inode).into())
    }

    fn get(&self, inode: u64) -> Result<SharedInodeEntry> {
        Ok(self
            .0
            .read()
            .unwrap() // assume the lock was not poisoned.
            .get(Self::index(inode)?)
            .ok_or(InodeError::Invalid(inode))
            .context("inode was never allocated")?
            .as_ref()
            .ok_or(InodeError::Invalid(inode))
            .context("inode was deleted")?
            .clone())
    }

    fn alloc(&self) -> usize {
        let mut vec = self.0.write().unwrap();
        let ino = vec.len();
        vec.push(None);
        ino
    }

    fn replace(&self, index: usize, entry: InodeEntry) {
        let mut vec = self.0.write().unwrap();
        vec[index] = Some(Arc::new(entry));
    }
}

type OdbSender = mpsc::Sender<odb_readthrough::Request>;

/// DexFS state shared across all workers
struct SharedState {
    /// Connection to object database
    odb: OdbSender,

    /// Indexed by inode
    inodes: InodeTable,
}

impl SharedState {
    fn new(odb: OdbSender) -> Arc<SharedState> {
        let state = SharedState {
            odb: odb.clone(),
            inodes: InodeTable::new(),
        };

        Arc::new(state)
    }

    async fn fetch_object<'a>(
        object: &'a mut LazyObject,
        odb: &OdbSender,
    ) -> Result<&'a ObjectReader> {
        match object.get(odb).await {
            LazyObject::Error(e) => {
                // TODO: LazyObject::Error currently has a restricted lifetime. This
                // may go away if that module ends up switching to use anyhow.
                Err(anyhow!("LazyObject::Error: {e:?}"))
            }
            LazyObject::Missing(oid) => {
                Err(FuseError::new(ENOENT)).context(format!("missing oid {oid}"))
            }
            LazyObject::Ok(o) => Ok(o),
            LazyObject::Future(_) | LazyObject::Pending(_) => unreachable!(),
        }
    }

    async fn populate_root(&self, root: ObjectId) -> Result<()> {
        let mut lazy_object = LazyObject::new(root);
        let object = Self::fetch_object(&mut lazy_object, &self.odb)
            .await
            .with_context(|| format!("populate_root({root})"))?;
        let reader = object.reader();
        let (tree, stat) = if let object::TreeRoot(root) = reader.get()?.which()? {
            let stat = StatWithUnknownInode::try_from(root.clone()?.get_stat()?)?.finalize(1);
            (root?.get_tree()?, stat)
        } else {
            bail!("oid {root} is not a TreeRoot");
        };

        // inode 0 is unused
        self.inodes.alloc();

        // inode 1 is the root object
        self.inodes.alloc();
        let children = self.populate_tree(tree, 1)?;
        self.inodes.replace(
            1,
            InodeEntry {
                oid: root.clone(),
                object: lazy_object.into(),
                parent_inode: 0,
                stat: stat,
                filename: ".".to_string(),
                children,
            },
        );

        Ok(())
    }

    fn populate_tree<'a>(
        &self,
        tree: tree::Reader<'a>,
        parent: u64,
    ) -> Result<Vec<usize>, anyhow::Error> {
        let entries: Result<Vec<usize>, anyhow::Error> = tree
            .get_entries()?
            .iter()
            .map(|entry| {
                let idx = self.inodes.alloc();

                // Finalize entry based on our now-known inode number
                // TODO: decide how to handle hard links
                let oid: ObjectId = entry.get_oid()?.try_into()?;

                if !entry.has_stat() {
                    return Err(anyhow!("entry {oid} missing stat data"));
                }
                let stat = StatWithUnknownInode::try_from(entry.get_stat()?)?.finalize(idx as u64);

                self.inodes.replace(
                    idx,
                    InodeEntry {
                        oid: oid.clone(),
                        object: LazyObject::new(oid).into(),
                        parent_inode: parent,
                        stat,
                        filename: entry.get_name()?.to_string(),
                        children: Vec::new(),
                    },
                );
                Ok(idx)
            })
            .collect();
        Ok(entries?)
    }

    async fn get_inode(&self, inode: u64) -> Result<SharedInodeEntry, anyhow::Error> {
        use std::ops::Deref;
        let entry = self.inodes.get(inode)?;

        let mut object_lock = entry.object.lock().await;

        // Check that the entry is valid.
        let initially_ok = matches!(object_lock.deref(), LazyObject::Ok(_));
        let object = match object_lock.get(&self.odb).await {
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
            let reader = object.reader();

            // Populate tree entries.
            if let Some(tree) = match reader.get()?.which()? {
                object::TreeRoot(root) => Some(root?.get_tree()),
                object::Tree(tree) => Some(tree),
                _ => None,
            } {
                tree.map_err(anyhow::Error::from)
                    .and_then(|tree| Ok(self.populate_tree(tree, entry.parent_inode)?))
                    .map_err(|e| {
                        anyhow::Error::from(e)
                            .context(format!("corrupt tree at ino {inode} oid {}", entry.oid))
                    })?;
            }
        }

        drop(object_lock);
        Ok(entry)
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

fn mode_to_dirtype(st_mode: u32) -> u32 {
    // IFTODT (((mode) & 0170000) >> 12)
    ((st_mode) & 0o170000) >> 12
}

impl FileSystem for DexFS {
    type Inode = u64;
    type Handle = u64;

    fn init(&self, mut capable: FsOptions) -> Result<FsOptions, io::Error> {
        // Request READDIRPLUS only.
        capable |= FsOptions::DO_READDIRPLUS;
        capable &= !FsOptions::READDIRPLUS_AUTO;
        Ok(capable)
    }

    fn readdirplus(
        &self,
        _ctx: &Context,
        inode: Self::Inode,
        _handle: Self::Handle,
        _size: u32,
        offset: u64,
        add_entry: &mut dyn FnMut(DirEntry<'_>, Entry) -> Result<usize, io::Error>,
    ) -> Result<(), io::Error> {
        let inode_entry = self.shared.inodes.get(inode).map_err(FuseError::from)?;
        for (i, child_inode) in inode_entry
            .children
            .iter()
            .enumerate()
            .skip(offset.try_into().unwrap())
        {
            let child = self
                .shared
                .inodes
                .get(*child_inode as u64)
                .map_err(FuseError::from)?;
            // NB: The 'offset' to return here is opaque to the kernel. It acts as
            // a continuation cookie to get the *next* element in the sequence.
            // Since we have all the entries in a static list, we can just use the
            // list index directly, making sure to preincrement, or we'll end up
            // in an infinite loop.
            let dirent = DirEntry {
                ino: *child_inode as u64,
                offset: (i + 1) as u64,
                name: child.filename.as_bytes(),
                type_: mode_to_dirtype(child.stat.st_mode),
            };
            let ent = Entry {
                inode: *child_inode as u64,
                generation: 0,
                attr: child.stat,
                attr_flags: 0,
                attr_timeout: TTL,
                entry_timeout: TTL,
            };
            log::trace!(
                "adding child {} ({i} of {}) offset {offset}",
                child.filename,
                inode_entry.children.len()
            );
            add_entry(dirent, ent)?;
        }
        Ok(())
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
        let entry = self
            .shared
            .get_inode(inode)
            .await
            .map_err(FuseError::from)?;
        let result = (entry.stat.clone(), TTL);
        log::trace!("async_getattr: {:?}", result);
        Ok(result)
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
    odb: OdbSender,
    root: ObjectId,
    until: CancellationToken,
) -> Result<(), anyhow::Error> {
    let mut tasks: Vec<JoinHandle<Result<(), Error>>> = Vec::new();

    // Initialize state shared across all workers.
    let fs_shared_state = SharedState::new(odb);
    fs_shared_state.populate_root(root).await?;

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
