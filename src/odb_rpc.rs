use std::cell::RefCell;
use std::collections::HashSet;
use std::io::BufWriter;
use std::rc::Rc;

use capnp::capability::{Promise, RemotePromise};
use capnp_rpc::pry;
use tokio::sync::oneshot;

use crate::odb::{ObjectDb, ObjectId};
use crate::proto::odb_capnp::{export, export_factory, import};

type RpcError = ::capnp::Error;

impl From<crate::odb::Error> for RpcError {
    fn from(value: crate::odb::Error) -> RpcError {
        RpcError::failed(format!("odb: {value}"))
    }
}

pub struct ImportToStdout;

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

    fn done(
        &mut self,
        _params: import::DoneParams,
        mut _results: import::DoneResults,
    ) -> Promise<(), RpcError> {
        Promise::ok(())
    }
}

struct OneshotImportState {
    db: Option<tokio_rusqlite::Connection>,
    oids: Rc<RefCell<Option<Vec<ObjectId>>>>,
    completion: Option<oneshot::Sender<Vec<ObjectId>>>,
}

/// An Import server that receives objects, writes them to the database,
/// and notifies of completion to a oneshot channel.
#[derive(Clone)]
pub struct OneshotImport {
    state: Rc<RefCell<OneshotImportState>>,
}

impl OneshotImport {
    pub fn new(
        db: tokio_rusqlite::Connection,
        completion: oneshot::Sender<Vec<ObjectId>>,
    ) -> OneshotImport {
        Self {
            state: Rc::new(RefCell::new(OneshotImportState {
                db: Some(db),
                oids: Rc::new(RefCell::new(Some(Vec::new()))),
                completion: Some(completion),
            })),
        }
    }
}

impl import::Server for OneshotImport {
    fn send_object(
        &mut self,
        params: import::SendObjectParams,
        mut results: import::SendObjectResults,
    ) -> Promise<(), RpcError> {
        log::debug!("oneshot import send_object");
        if let Some(db) = self.state.borrow().db.as_ref() {
            let params = pry!(params.get());
            let object = pry!(params.get_object());

            // Provide a hook for pipelining requests back to this
            // server.
            results.get().set_self(capnp_rpc::new_client(self.clone()));

            // Must copy the sub-message out of its container in order to write
            // it to the database.
            // TODO: remove this copy.
            // TODO: why doesn't set_root_canonical work?
            let mut out = ::capnp::message::Builder::new_default();
            pry!(out.set_root(object));

            let db = db.clone();
            let rc = Rc::clone(&self.state.borrow().oids);
            Promise::from_future(async move {
                let oid = db
                    .call(|conn| ObjectDb::new(conn).insert_object(out))
                    .await?;
                log::debug!("received oid {} from remote", &oid);
                // Safe to unwrap oids because it shares the same lifetime as db.
                rc.borrow_mut().as_mut().unwrap().push(oid);
                Ok(())
            })
        } else {
            Promise::err(RpcError::failed("attempted to reuse import".to_string()))
        }
    }

    fn done(
        &mut self,
        _params: import::DoneParams,
        mut _results: import::DoneResults,
    ) -> Promise<(), RpcError> {
        log::debug!("oneshot import done");
        let mut state = self.state.borrow_mut();
        if let Some(tx) = state.completion.take() {
            // Drop reference to the database.
            state.db.take();

            // Safe to unwrap the result because we either take() all or none.
            pry!(tx
                .send(state.oids.take().unwrap())
                .map_err(|_| RpcError::failed("receiver hung up".to_string())));
            Promise::ok(())
        } else {
            Promise::err(RpcError::failed("done called more than once".to_string()))
        }
    }
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
        log::debug!("want {oid}");
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
        log::debug!("have {oid}");
        self.have.borrow_mut().insert(oid);
        Promise::ok(())
    }

    fn begin(
        &mut self,
        params: export::BeginParams,
        mut _results: export::BeginResults,
    ) -> Promise<(), RpcError> {
        use crate::proto::odb_capnp::import::send_object_results;
        let params = pry!(params.get());
        let remote = pry!(params.get_import());
        let db = self.db.clone();
        let oids = self.want.borrow().clone();
        log::debug!("begin ({} objects)", oids.len());
        if !oids.is_empty() {
            Promise::from_future(async move {
                let mut pipeline: Option<RemotePromise<send_object_results::Owned>> = None;
                for oid in oids.into_iter() {
                    log::debug!("fetching {oid} from database");
                    let object = db
                        .call(move |conn| ObjectDb::new(conn).get_object(&oid))
                        .await?;
                    log::debug!("sending to remote");
                    let mut request = if let Some(prev) = pipeline {
                        prev.pipeline.get_self().send_object_request()
                    } else {
                        remote.send_object_request()
                    };
                    request.get().set_object(object.reader().get()?)?;
                    pipeline = Some(request.send())
                }
                let mut done = pipeline.unwrap().pipeline.get_self().done_request();
                done.get().set_self(remote);

                log::debug!("waiting for objects to be sent");
                done.send().promise.await?;
                log::debug!("done.");
                Ok(())
            })
        } else {
            Promise::err(RpcError::failed("no objects requested".to_string()))
        }
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
        log::debug!("starting new export");
        Promise::ok(())
    }
}
