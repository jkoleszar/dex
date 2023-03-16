use std::collections::HashSet;
use std::io::BufWriter;

use capnp::capability::Promise;
use capnp::message::{Reader, ReaderOptions, SegmentArray, TypedReader};
use capnp_rpc::pry;
use futures::future;

use crate::odb::{ObjectDb, ObjectId};
use crate::proto::odb_capnp::{export, import, object};

pub struct ImportToStdout;

type RpcError = ::capnp::Error;

impl From<crate::odb::Error> for RpcError {
    fn from(value: crate::odb::Error) -> RpcError {
        RpcError::failed(format!("odb: {value}"))
    }
}

impl import::Server for ImportToStdout {
    fn send_object(
        &mut self,
        params: import::SendObjectParams,
        mut _results: import::SendObjectResults,
    ) -> Promise<(), RpcError> {
        let params = pry!(params.get());
        let object = pry!(params.get_object());
        let mut stdout = BufWriter::new(std::io::stdout());
        let mut out = ::capnp::message::Builder::new_default();
        pry!(out.set_root_canonical(object));
        pry!(capnp::serialize::write_message(&mut stdout, &out));
        Promise::ok(())
    }
}

fn send_one_object(
    oid: ObjectId,
    db: tokio_rusqlite::Connection,
    remote: import::Client,
) -> Promise<(), RpcError> {
    Promise::from_future(async move {
        let encoded = db
            .call(move |conn| ObjectDb::new(conn).get_object_encoded(&oid))
            .await?;
        let segments = &[encoded.as_slice()];
        let segment_array = SegmentArray::new(segments);
        let object = TypedReader::<_, object::Owned>::new(Reader::new(
            segment_array,
            ReaderOptions::default(),
        ));
        let mut request = remote.send_object_request();
        request.get().set_object(object.get()?)?;
        request.send().promise.await?;
        Ok(())
    })
}

fn send_objects<I: Iterator<Item = ObjectId>>(
    oids: I,
    db: tokio_rusqlite::Connection,
    remote: import::Client,
) -> Vec<Promise<(), RpcError>> {
    oids.map(|oid| send_one_object(oid, db.clone(), remote.clone()))
        .collect()
}

pub struct Exporter {
    db: tokio_rusqlite::Connection,
    want: HashSet<ObjectId>,
    have: HashSet<ObjectId>,
}

impl Exporter {
    pub fn new(db: tokio_rusqlite::Connection) -> Self {
        Exporter {
            db,
            want: HashSet::new(),
            have: HashSet::new(),
        }
    }
}

impl export::Server for Exporter {
    fn want(
        &mut self,
        params: export::WantParams,
        mut _results: export::WantResults,
    ) -> Promise<(), RpcError> {
        let params = pry!(params.get());
        let id_param = pry!(pry!(params.get_id()).get_id());
        if let Ok(oid) = id_param.try_into() {
            let oid = ObjectId::SHA512_256(oid);
            self.want.insert(oid);
            Promise::ok(())
        } else {
            Promise::err(RpcError::failed("invalid object id".to_string()))
        }
    }

    fn have(
        &mut self,
        params: export::HaveParams,
        mut _results: export::HaveResults,
    ) -> Promise<(), RpcError> {
        let params = pry!(params.get());
        let id_param = pry!(pry!(params.get_id()).get_id());
        if let Ok(oid) = id_param.try_into() {
            let oid = ObjectId::SHA512_256(oid);
            self.have.insert(oid);
            Promise::ok(())
        } else {
            Promise::err(RpcError::failed("invalid object id".to_string()))
        }
    }

    fn begin(
        &mut self,
        params: export::BeginParams,
        mut _results: export::BeginResults,
    ) -> Promise<(), RpcError> {
        let params = pry!(params.get());
        let import = pry!(params.get_import());
        let requests = send_objects(self.want.iter().cloned(), self.db.clone(), import);
        Promise::from_future(async move {
            future::try_join_all(requests.into_iter()).await?;
            Ok(())
        })
    }
}
