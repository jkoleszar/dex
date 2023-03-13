pub mod odb;

pub mod proto {
    pub mod odb_capnp {
        include!(concat!(env!("OUT_DIR"), "/odb_capnp.rs"));
    }
}
