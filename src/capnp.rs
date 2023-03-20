use capnp::capability::Client;
use capnp::message::ReaderSegments;
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
