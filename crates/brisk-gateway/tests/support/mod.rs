//! Shared helpers for the integration tests: run [`serve`] on an ephemeral
//! port with a caller-supplied service and a manual shutdown trigger.

#![allow(dead_code)]

use std::convert::Infallible;
use std::error::Error as StdError;
use std::future::{Ready, ready};
use std::io;
use std::net::SocketAddr;

use brisk_gateway::server::{ServerConfig, bind, serve};
use bytes::Bytes;
use http::{Request, Response};
use http_body::Body;
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::service::Service;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio_rustls::TlsAcceptor;

/// A server started by [`start`].
pub(crate) struct TestServer {
    pub(crate) addr: SocketAddr,
    shutdown: Option<oneshot::Sender<()>>,
    pub(crate) task: JoinHandle<io::Result<()>>,
}

impl TestServer {
    /// `http://127.0.0.1:<port>`.
    pub(crate) fn http_base(&self) -> String {
        format!("http://{}", self.addr)
    }

    /// Completes the `shutdown` future passed to `serve`.
    pub(crate) fn trigger_shutdown(&mut self) {
        let tx = self.shutdown.take().expect("shutdown already triggered");
        tx.send(()).expect("serve task already finished");
    }
}

/// Binds `127.0.0.1:0` and spawns `serve` with the given parameters.
pub(crate) fn start<S, B>(config: ServerConfig, tls: Option<TlsAcceptor>, service: S) -> TestServer
where
    S: Service<Request<Incoming>, Response = Response<B>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Into<Box<dyn StdError + Send + Sync>>,
    B: Body + Send + 'static,
    B::Data: Send,
    B::Error: Into<Box<dyn StdError + Send + Sync>>,
{
    let listener = bind("127.0.0.1:0".parse().unwrap(), &config).unwrap();
    let addr = listener.local_addr().unwrap();
    let (tx, rx) = oneshot::channel::<()>();
    let task = tokio::spawn(serve(listener, tls, config, service, async move {
        // A dropped sender also means "stop", so tests cannot leak servers.
        let _ = rx.await;
    }));
    TestServer {
        addr,
        shutdown: Some(tx),
        task,
    }
}

/// Builds a `200 OK` response with a fixed body, as a future that can be
/// returned directly from a `service_fn` closure.
pub(crate) fn text(body: &'static str) -> Ready<Result<Response<Full<Bytes>>, Infallible>> {
    ready(Ok(Response::new(Full::new(Bytes::from_static(
        body.as_bytes(),
    )))))
}
