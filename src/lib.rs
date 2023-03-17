pub mod capnp;
pub mod odb;
pub mod odb_rpc;

pub mod proto {
    pub mod odb_capnp {
        include!(concat!(env!("OUT_DIR"), "/odb_capnp.rs"));
    }
}
