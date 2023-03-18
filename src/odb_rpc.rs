use std::cell::RefCell;
use std::collections::HashSet;
use std::io::BufWriter;
use std::rc::Rc;

use capnp::capability::Promise;
use capnp_rpc::pry;
use futures::future;

use crate::odb::{ObjectDb, ObjectId};
use crate::proto::odb_capnp::{export, export_factory, import};

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
        let object = db
            .call(move |conn| ObjectDb::new(conn).get_object(&oid))
            .await?;
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

#[derive(Clone)]
pub struct Export {
    db: tokio_rusqlite::Connection,
    want: Rc<RefCell<HashSet<ObjectId>>>,
    have: Rc<RefCell<HashSet<ObjectId>>>,
}

impl Export {
    pub fn new(db: tokio_rusqlite::Connection) -> Self {
        Export {
            db,
            want: Rc::new(RefCell::new(HashSet::new())),
            have: Rc::new(RefCell::new(HashSet::new())),
        }
    }
}

impl export::Server for Export {
    fn want(
        &mut self,
        params: export::WantParams,
        mut results: export::WantResults,
    ) -> Promise<(), RpcError> {
        let params = pry!(params.get());
        let oid = pry!(pry!(params.get_id()).try_into());
        results.get().set_self(capnp_rpc::new_client(self.clone()));
        self.want.borrow_mut().insert(oid);
        Promise::ok(())
    }

    fn have(
        &mut self,
        params: export::HaveParams,
        mut results: export::HaveResults,
    ) -> Promise<(), RpcError> {
        let params = pry!(params.get());
        let oid = pry!(pry!(params.get_id()).try_into());
        results.get().set_self(capnp_rpc::new_client(self.clone()));
        self.have.borrow_mut().insert(oid);
        Promise::ok(())
    }

    fn begin(
        &mut self,
        params: export::BeginParams,
        mut _results: export::BeginResults,
    ) -> Promise<(), RpcError> {
        let params = pry!(params.get());
        let import = pry!(params.get_import());
        let requests = send_objects(self.want.borrow().iter().cloned(), self.db.clone(), import);
        Promise::from_future(async move {
            future::try_join_all(requests.into_iter()).await?;
            Ok(())
        })
    }
}

pub struct ExportFactory {
    db: tokio_rusqlite::Connection,
}

impl ExportFactory {
    pub fn new(db: tokio_rusqlite::Connection) -> Self {
        ExportFactory { db }
    }
}

impl export_factory::Server for ExportFactory {
    fn new(
        &mut self,
        _params: export_factory::NewParams,
        mut results: export_factory::NewResults,
    ) -> Promise<(), RpcError> {
        let client: export::Client = capnp_rpc::new_client(Export::new(self.db.clone()));
        results.get().set_export(client);
        Promise::ok(())
    }
}
