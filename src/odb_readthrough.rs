use thiserror::Error;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_rusqlite::Connection;

use crate::odb;
use crate::odb::{ObjectDb, ObjectId, ObjectIdIntoCapnp, ObjectReader};
use crate::odb_rpc::OneshotImport;
use crate::proto::odb_capnp::export_factory;

#[derive(Error, Debug)]
pub enum Error {
    #[error("odb error: {0}")]
    OdbError(#[from] odb::Error),

    #[error("rpc error: {0}")]
    RpcError(#[from] capnp::Error),

    #[error("database error: {0}")]
    DbError(#[from] rusqlite::Error),

    #[error("remote hung up without completing")]
    RemoteError,
}

pub type Response = Result<ObjectReader, Error>;

pub struct Request {
    oid: ObjectId,
    callback: oneshot::Sender<Response>,
}

pub struct Readthrough {
    pub db: Connection,
    pub export: export_factory::Client,
}

fn pipeline<T>(
    v: &mut Vec<capnp::capability::Promise<(), capnp::Error>>,
    p: capnp::capability::RemotePromise<T>,
) -> T::Pipeline
where
    T: capnp::traits::Pipelined + capnp::traits::Owned + 'static,
{
    let pipe = p.pipeline;
    v.push(capnp::capability::Promise::from_future(async move {
        p.promise.await?;
        Ok(())
    }));
    pipe
}

impl Readthrough {
    async fn lookup(db: &Connection, oid: &ObjectId) -> Response {
        let oid = *oid;
        db.call(move |conn| ObjectDb::new(conn).get_object(&oid))
            .await
            .map_err(|e| e.into())
    }

    async fn fetch(
        db: &Connection,
        export: &export_factory::Client,
        oid: &ObjectId,
    ) -> Result<(), Error> {
        log::debug!("fetch oid {oid}");
        // Create importer to receive the new objects
        let (tx, rx) = oneshot::channel();

        // Execute RPC to request the object.
        let mut export = export
            .new_request()
            .send()
            .promise
            .await?
            .get()?
            .get_export()?;

        let mut promises = Vec::new();
        let mut want = export.want_request();
        want.get().init_id().from_oid(oid);
        export = pipeline(&mut promises, want.send()).get_self();
        let mut begin = export.begin_request();
        let importer = OneshotImport::new(db.clone(), tx);
        begin.get().set_import(capnp_rpc::new_client(importer));
        pipeline(&mut promises, begin.send());

        // Wait for the import to complete.
        // TODO: implement timeout.
        log::debug!("waiting for transfer");
        for p in promises {
            p.await?;
        }
        let res = if rx.await.is_err() {
            Err(Error::RemoteError)
        } else {
            Ok(())
        };

        log::debug!("received oid {oid}");
        res
    }

    pub fn serve(&self) -> (mpsc::Sender<Request>, JoinHandle<()>) {
        let (tx, mut rx) = mpsc::channel::<Request>(1);
        let db = self.db.clone();
        let export = self.export.clone();
        let handle = tokio::task::spawn_local(async move {
            while let Some(request) = rx.recv().await {
                let result = match Readthrough::lookup(&db, &request.oid).await {
                    Err(Error::OdbError(odb::Error::Missing(_))) => {
                        log::debug!("requesting oid {} from remote", &request.oid);
                        let f = Readthrough::fetch(&db, &export, &request.oid).await;
                        if f.is_ok() {
                            Readthrough::lookup(&db, &request.oid).await
                        } else {
                            Err(f.err().unwrap())
                        }
                    }
                    r => r,
                };

                request
                    .callback
                    .send(result)
                    .map_err(|_| ())
                    .expect("callback channel dropped");
            }
        });
        (tx, handle)
    }
}

/// A handle to a remote object that is fetched the first time it is accessed.
pub enum LazyObject {
    Future(ObjectId, mpsc::Sender<Request>),
    Pending(oneshot::Receiver<Response>),
    Missing(ObjectId),
    Error(Error),
    Ok(ObjectReader),
}

impl LazyObject {
    pub fn new(oid: ObjectId, cache: mpsc::Sender<Request>) -> Self {
        LazyObject::Future(oid, cache)
    }

    pub async fn get(&mut self) -> &Self {
        match self {
            LazyObject::Future(oid, cache) => {
                log::debug!("requesting oid {} from cache", &oid);
                let (tx, rx) = oneshot::channel();
                if cache
                    .send(Request {
                        oid: *oid,
                        callback: tx,
                    })
                    .await
                    .is_err()
                {
                    *self = LazyObject::Error(Error::RemoteError);
                    return self;
                }

                log::debug!("waiting on cache");
                *self = match rx.await {
                    Ok(Ok(object)) => LazyObject::Ok(object),
                    Ok(Err(Error::OdbError(odb::Error::Missing(oid)))) => LazyObject::Missing(oid),
                    Ok(Err(e)) => LazyObject::Error(e),

                    // The cache task failed to send a callback
                    Err(_) => LazyObject::Error(Error::RemoteError),
                };
                self
            }

            _ => self,
        }
    }
}
