use std::marker::PhantomData;
use std::sync::Arc;

use capnp::capability::Client;
use capnp::message::{Reader, ReaderOptions, ReaderSegments, TypedReader};
use capnp::traits::Owned;
use capnp_rpc::RpcSystem;
use capnp_rpc::{rpc_twoparty_capnp, twoparty};
use futures::AsyncReadExt;

/// A ReaderSegments implementation with owned storage.
///
/// This allows creation of a message Reader that owns its storage, which
/// is useful when the data is not created by a Builder and the creator
/// wants to hand ownership of the data to its caller.
pub struct VecSegments {
    buf: Vec<u8>,
}

impl VecSegments {
    /// Creates a new VecSegments, taking ownership of the buffer provided.
    pub fn new(buf: Vec<u8>) -> Self {
        VecSegments { buf }
    }
}

impl ReaderSegments for VecSegments {
    fn get_segment(&self, idx: u32) -> Option<&[u8]> {
        if idx == 0 {
            Some(self.buf.as_slice())
        } else {
            None
        }
    }
}

/// A ReaderSegments implementation with shared owned storage.
///
/// This allows creation of a message Reader that owns its storage and
/// whose backing buffer can be shared across threads.
#[derive(Clone)]
pub struct SharedVecSegments {
    buf: Arc<Vec<u8>>,
}

impl SharedVecSegments {
    /// Creates a new VecSegments, taking ownership of the buffer provided.
    pub fn new(buf: Vec<u8>) -> Self {
        SharedVecSegments { buf: Arc::new(buf) }
    }
}

impl ReaderSegments for SharedVecSegments {
    fn get_segment(&self, idx: u32) -> Option<&[u8]> {
        if idx == 0 {
            Some(self.buf.as_slice())
        } else {
            None
        }
    }
}

/// A lazily constructed TypedReader factory
///
/// This is useful in conjuction with a shared owned storage backing buffer
/// because messages can not be shared across threads but their backing
/// storage can.
pub struct LazyTypedReader<T: Owned> {
    buf: SharedVecSegments,
    _marker: PhantomData<T>,
}

impl<T: Owned> LazyTypedReader<T> {
    pub fn new(buf: Vec<u8>) -> Self {
        LazyTypedReader {
            buf: SharedVecSegments::new(buf),
            _marker: PhantomData,
        }
    }

    pub fn reader(&self) -> TypedReader<SharedVecSegments, T> {
        Reader::<SharedVecSegments>::new(self.buf.clone(), ReaderOptions::default()).into()
    }
}

/// Instantiates a two party RpcSystem over a stream. This allows clients
/// to run in a separate thread from their Server or the connected RpcSystem.
pub fn duplex_stream_server(
    stream: tokio::io::DuplexStream,
    cap: Client,
) -> RpcSystem<twoparty::VatId> {
    let (reader, writer) = tokio_util::compat::TokioAsyncReadCompatExt::compat(stream).split();
    let network = twoparty::VatNetwork::new(
        reader,
        writer,
        rpc_twoparty_capnp::Side::Server,
        Default::default(),
    );
    RpcSystem::new(Box::new(network), Some(cap))
}

/// Instantiates a two party RpcSystem over a stream. This allows clients
/// to run in a separate thread from their Server or the connected RpcSystem.
pub fn duplex_stream_client(stream: tokio::io::DuplexStream) -> RpcSystem<twoparty::VatId> {
    let (reader, writer) = tokio_util::compat::TokioAsyncReadCompatExt::compat(stream).split();
    let network = twoparty::VatNetwork::new(
        reader,
        writer,
        rpc_twoparty_capnp::Side::Client,
        Default::default(),
    );
    RpcSystem::new(Box::new(network), None)
}
