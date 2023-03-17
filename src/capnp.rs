use capnp::message::ReaderSegments;

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
