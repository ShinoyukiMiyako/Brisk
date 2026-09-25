//! floor-B: the blind forwarder with its upstream connections driven by the
//! forwarding task itself (02, E11 and C20).
//!
//! floor-A sends upstream through reqwest, whose hyper-util legacy client
//! spawns every upstream connection as a task of its own: each response frame
//! is read by that task, handed over the body channel and read again by the
//! task that serves the inbound connection. floor-B removes that hop. It keeps
//! [`hyper::client::conn::http1`] connections in its own pool and polls the
//! upstream [`Connection`] from the task that forwards the request: in the
//! handler, alternately with the response future, until the response head
//! arrives, then in [`SameTaskBody::poll_frame`]. Comparing the two floors
//! therefore prices the task hop and reqwest's own per-request layers, and
//! nothing else.
//!
//! # Forwarding semantics
//!
//! Those of [`crate::forward`]: the base URL follows the rules of
//! [`Forwarder::new`](crate::forward::Forwarder::new); the method, path and
//! query are kept, and a request-target that URL parsing would rewrite gets
//! `400 Bad Request`; hop-by-hop headers and `Host` are removed, a
//! `TE: trailers` is kept and split HTTP/2 `Cookie` fields are joined; the
//! inbound body is the upstream request body as it is; response headers are
//! forwarded minus hop-by-hop headers; an upstream failure before the response
//! head becomes `502 Bad Gateway`, one after it aborts the inbound response.
//!
//! What reqwest and the legacy client add on top is added here too, in the
//! same order, so the upstream receives the same request bytes from both
//! floors: `Accept: */*` when the request has none (reqwest's default header),
//! then `Host` from the base URL, with the port only when it is not the
//! scheme's default (the legacy client's `set_host`), and the origin-form
//! request-target (its `origin_form`).
//!
//! # HTTP/1.1 connection parameters
//!
//! [`http1::Builder`] is used exactly as [`http1::Builder::new`] returns it,
//! because that is what the upstream client of floor-A uses (reqwest 0.13.5
//! and hyper-util 0.1.20 as locked in `Cargo.lock`, built by
//! [`brisk_gateway::upstream::build_client`]):
//!
//! - hyper-util's `legacy::Builder::new` starts its HTTP/1 builder from
//!   `http1::Builder::new()` and changes it only through its own `http1_*`
//!   and `http09_responses` setters. Its `timer` reaches only the HTTP/2
//!   builder; `pool_timer`, `pool_idle_timeout` and `pool_max_idle_per_host`
//!   configure the pool, not the connection.
//! - reqwest's `ClientBuilder::build` (`src/async_impl/client.rs`) calls those
//!   setters only for options enabled on the `ClientBuilder`, and
//!   `build_client` enables none of them (`http1_only` selects the protocol
//!   and the TLS ALPN, not a builder option). Option by option:
//!   - `http09_responses`: reqwest sets it only after
//!     `ClientBuilder::http09_responses`; hyper default `false`.
//!   - `title_case_headers`: only after `http1_title_case_headers`; hyper
//!     default `false` (header names are written in lower case).
//!   - `allow_obsolete_multiline_headers_in_responses`,
//!     `ignore_invalid_headers_in_responses` and
//!     `allow_spaces_after_header_name_in_responses` (the response parser's
//!     leniency): only after the reqwest methods of the same name with an
//!     `http1_` prefix; hyper default `false` for each, a strict parser.
//!   - `max_headers`: only after `http1_max_headers`; hyper default of 100
//!     response headers.
//!   - `preserve_header_case`, `writev`, `read_buf_exact_size` and
//!     `max_buf_size`: hyper-util has setters for them, which reqwest never
//!     calls. So header case is not preserved; the write strategy follows the
//!     transport's `is_write_vectored`, which is true for a TCP stream and for
//!     a tokio-rustls stream in both floors (queued `writev`); the read buffer
//!     is adaptive from 8 KiB; and the buffer limit is hyper's default of
//!     8 KiB + 100 * 4 KiB = 417,792 bytes.
//!
//! The connection itself is set up as reqwest sets up its own: `TCP_NODELAY`
//! on the socket, and the TCP connect plus TLS handshake bounded by the
//! `connect_timeout` of [`UpstreamClientConfig::default`].
//!
//! # Pool
//!
//! Idle connections sit in a last-in-first-out `Vec` behind a
//! [`std::sync::Mutex`], without an upper bound, like reqwest's default
//! `pool_max_idle_per_host`: a bound such as 1024 would make the share of
//! fresh connections differ between the floors at S1's peak concurrency.
//! Checkout drops connections whose [`SendRequest`] is no longer ready and
//! opens a new connection when none is left. A request that a reused
//! connection hands back unsent (it was found closed before the request was
//! written) is retried on the next connection, as the legacy client's default
//! `retry_canceled_requests` does; a failure on a fresh connection is final.
//!
//! A connection returns to the pool once its response body has been read to
//! the end (trailers included) and its [`SendRequest`] is ready again; a body
//! dropped before its end closes the connection. Readiness normally comes in
//! the same poll as the end of the body, and the connection is pooled before
//! that end is passed on. When the upstream answered before reading the whole
//! request body, readiness waits for the rest of the upload; the end of the
//! response is then passed on at once, as floor-A does, and a spawned task
//! drives the connection until it is ready and pools it, as the legacy
//! client's connection task does. Measurements never take that path, since
//! brisk-mock reads a chat request in full before answering it. See
//! [`SameTaskBody`].
//!
//! # Deliberate differences from floor-A
//!
//! None of them acts on a pooled connection during a measurement: floor-B
//! sets no TCP keepalive and no `TCP_USER_TIMEOUT` and has no idle timeout
//! for pooled connections; it connects with [`TcpStream::connect`], so the
//! upstream host is resolved by the system resolver instead of the gateway's
//! `SafeResolver` and its addresses are tried one after another; for an
//! `https` upstream it trusts only `--upstream-ca`, not the platform roots,
//! and offers ALPN `http/1.1`; and it polls [`Connection`] without
//! `with_upgrades`, since floor strips `Upgrade` and no upgrade can happen.
//! The legacy client also races a new connection against the pool and takes
//! whichever is ready first, pooling the other; floor-B connects whenever the
//! pool is empty. That can only change how many connections a burst opens,
//! which the per-arm reuse rate of each benchmark session reports.
//!
//! # Known limitation
//!
//! Nothing polls a connection while it sits in the pool, so a close by the
//! upstream goes unnoticed until the next request on that connection (which
//! then moves to another connection if hyper hands it back unsent). floor-B
//! only serves measurements against brisk-mock, which does not close idle
//! connections.

use std::convert::Infallible;
use std::fmt;
use std::future::{Future, poll_fn};
use std::io;
use std::pin::{Pin, pin};
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::{Context, Poll};
use std::time::Duration;

use brisk_bench_core::transport::tls::ALPN_HTTP11;
use brisk_gateway::server::{ServerConfig, serve};
use brisk_gateway::upstream::UpstreamClientConfig;
use http::header::{ACCEPT, CONTENT_TYPE, HOST, TE};
use http::uri::PathAndQuery;
use http::{HeaderMap, HeaderValue, Request, Response, StatusCode, Uri, Version};
use http_body_util::{Either, Full};
use hyper::body::{Body, Bytes, Frame, Incoming, SizeHint};
use hyper::client::conn::TrySendError;
use hyper::client::conn::http1::{self, Connection, SendRequest};
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use reqwest::Url;
use rustls_pki_types::ServerName;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::rustls::ClientConfig;
use tokio_rustls::{TlsAcceptor, TlsConnector};

use crate::forward::{
    BAD_GATEWAY_BODY, BAD_TARGET_BODY, UpstreamUrlError, join_cookies, strip_hop_by_hop,
    te_accepts_trailers,
};

/// Response body of floor-B: the streamed upstream body, or a short text
/// body that floor-B generates itself.
pub type FloorBody = Either<SameTaskBody, Full<Bytes>>;

/// A rejected floor-B configuration.
#[derive(Debug, thiserror::Error)]
pub enum SameTaskConfigError {
    /// The upstream base URL is unusable; the rules are floor-A's.
    #[error(transparent)]
    Url(#[from] UpstreamUrlError),
    /// An `https` upstream was given without a CA to trust.
    #[error("floor-B needs --upstream-ca for the https upstream {0:?}")]
    MissingCa(String),
    /// The upstream host is not a valid TLS server name.
    #[error("upstream host {0:?} is not a valid TLS server name")]
    ServerName(String),
}

/// Forwards requests to one upstream base URL over pooled HTTP/1.1
/// connections that are driven by the forwarding task itself. Cloning is
/// cheap and shares the pool.
#[derive(Debug, Clone)]
pub struct SameTaskForwarder {
    inner: Arc<Inner>,
}

struct Inner {
    /// Upstream base URL without a trailing slash, as in floor-A.
    base: String,
    /// Length of the scheme and authority at the start of `base`; what
    /// follows them in a joined URL is the origin-form request-target.
    origin_len: usize,
    /// `Host` of every upstream request.
    host: HeaderValue,
    /// `host:port` for the TCP connect.
    connect_addr: String,
    /// Connector and server name for an `https` upstream.
    tls: Option<(TlsConnector, ServerName<'static>)>,
    builder: http1::Builder,
    connect_timeout: Duration,
    idle: Mutex<Vec<Upstream>>,
}

impl fmt::Debug for Inner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Inner")
            .field("base", &self.base)
            .field("tls", &self.tls.is_some())
            .field("connect_timeout", &self.connect_timeout)
            .finish_non_exhaustive()
    }
}

impl SameTaskForwarder {
    /// Creates a forwarder to `upstream` joined with each request's path and
    /// query.
    ///
    /// `upstream` must be an `http` or `https` URL with a host and without
    /// credentials, a query or a fragment, exactly as for
    /// [`Forwarder::new`](crate::forward::Forwarder::new). `tls` is the
    /// client configuration for an `https` upstream and is required for one;
    /// floor-B uses a copy whose ALPN list is `http/1.1` alone, the only
    /// protocol it speaks, whatever `tls` offers. It is unused for an `http`
    /// upstream.
    pub fn new(
        upstream: &str,
        tls: Option<Arc<ClientConfig>>,
    ) -> Result<Self, SameTaskConfigError> {
        let url = parse_base(upstream)?;
        let unsupported = |reason| UpstreamUrlError::Unsupported {
            url: upstream.to_owned(),
            reason,
        };
        let base = url.as_str().trim_end_matches('/').to_owned();
        // Without a query or fragment the serialized URL is the origin
        // followed by the path, and `base` trims the same slashes as this.
        let origin_len = base.len() - url.path().trim_end_matches('/').len();
        let host_str = url
            .host_str()
            .ok_or_else(|| unsupported("must have a host"))?;
        let port = url
            .port_or_known_default()
            .ok_or_else(|| unsupported("must have a port"))?;
        // `Url` drops a port equal to the scheme default, which is exactly
        // when the legacy client leaves it out of `Host`.
        let host = match url.port() {
            Some(port) => format!("{host_str}:{port}"),
            None => host_str.to_owned(),
        };
        let host = HeaderValue::try_from(host)
            .map_err(|_| unsupported("must have a host usable as a Host header"))?;
        let tls = if url.scheme() == "https" {
            let config = tls.ok_or_else(|| SameTaskConfigError::MissingCa(base.clone()))?;
            // IPv6 literals keep their brackets in the URL but not in SNI.
            let sni_host = host_str.trim_start_matches('[').trim_end_matches(']');
            let name = ServerName::try_from(sni_host.to_owned())
                .map_err(|_| SameTaskConfigError::ServerName(sni_host.to_owned()))?;
            // An upstream that selected h2 would get HTTP/1.1 bytes on an h2
            // session, so nothing else may be offered.
            let mut config = Arc::unwrap_or_clone(config);
            config.alpn_protocols = vec![ALPN_HTTP11.to_vec()];
            Some((TlsConnector::from(Arc::new(config)), name))
        } else {
            None
        };
        Ok(Self {
            inner: Arc::new(Inner {
                connect_addr: format!("{host_str}:{port}"),
                base,
                origin_len,
                host,
                tls,
                builder: http1::Builder::new(),
                connect_timeout: UpstreamClientConfig::default().connect_timeout,
                idle: Mutex::new(Vec::new()),
            }),
        })
    }

    /// The normalized upstream base URL requests are forwarded to.
    pub fn upstream_base(&self) -> &str {
        &self.inner.base
    }

    /// Number of idle connections in the pool.
    pub fn idle_connections(&self) -> usize {
        self.inner.lock_idle().len()
    }

    /// Forwards one request and returns the upstream response with its body
    /// still streaming.
    ///
    /// Never fails: a request-target that URL parsing would rewrite gets
    /// `400 Bad Request`, and an upstream error before the response head
    /// (connect failure, reset, invalid response) `502 Bad Gateway`, each
    /// with a short text body. An error after the head surfaces as a body
    /// error, which makes hyper abort the inbound response.
    pub async fn forward(&self, req: Request<Incoming>) -> Response<FloorBody> {
        let (parts, body) = req.into_parts();
        let Some(joined) = join_target(&self.inner.base, &parts.uri) else {
            return text_response(StatusCode::BAD_REQUEST, BAD_TARGET_BODY);
        };
        // floor-A hands reqwest the joined URL, which fails the request
        // before sending when the URL is not a valid `Uri`; the same failure
        // gets the same answer here.
        let target = Bytes::from(joined).slice(self.inner.origin_len..);
        let uri = match Uri::from_maybe_shared(target) {
            Ok(uri) => uri,
            Err(err) => {
                tracing::warn!(error = ?err, "upstream request failed");
                return text_response(StatusCode::BAD_GATEWAY, BAD_GATEWAY_BODY);
            }
        };
        let headers = self.upstream_headers(parts.headers, parts.version);

        let mut request = Request::new(body);
        *request.method_mut() = parts.method;
        *request.uri_mut() = uri;
        *request.headers_mut() = headers;

        loop {
            let (mut upstream, reused) = match self.inner.checkout() {
                Some(upstream) => (upstream, true),
                None => match self.inner.connect().await {
                    Ok(upstream) => (upstream, false),
                    Err(err) => {
                        tracing::warn!(error = ?err, "connecting upstream failed");
                        return text_response(StatusCode::BAD_GATEWAY, BAD_GATEWAY_BODY);
                    }
                },
            };
            match upstream.send(request).await {
                Ok(response) => return self.respond(response, upstream).await,
                Err(mut err) => match err.take_message() {
                    Some(unsent) if reused => {
                        tracing::debug!(error = ?err.error(), "pooled connection was closed; retrying");
                        request = unsent;
                    }
                    _ => {
                        tracing::warn!(error = ?err.into_error(), "upstream request failed");
                        return text_response(StatusCode::BAD_GATEWAY, BAD_GATEWAY_BODY);
                    }
                },
            }
        }
    }

    /// Turns inbound request headers into upstream request headers with the
    /// same operations, in the same order, as floor-A, reqwest and the legacy
    /// client apply them, so the header block is the same byte for byte
    /// (removals reorder a `HeaderMap`).
    fn upstream_headers(&self, mut headers: HeaderMap, inbound: Version) -> HeaderMap {
        let accepts_trailers = te_accepts_trailers(&headers);
        strip_hop_by_hop(&mut headers);
        headers.remove(HOST);
        if accepts_trailers {
            headers.insert(TE, HeaderValue::from_static("trailers"));
        }
        if inbound == Version::HTTP_2 {
            join_cookies(&mut headers);
        }
        headers
            .entry(ACCEPT)
            .or_insert_with(|| HeaderValue::from_static("*/*"));
        headers.insert(HOST, self.inner.host.clone());
        headers
    }

    async fn respond(
        &self,
        response: Response<Incoming>,
        upstream: Upstream,
    ) -> Response<FloorBody> {
        let (mut parts, body) = response.into_parts();
        strip_hop_by_hop(&mut parts.headers);
        // The upstream's HTTP version describes the upstream hop only; hyper
        // picks the version of the inbound connection itself.
        parts.version = Version::default();
        let mut body = SameTaskBody {
            body,
            upstream: Some(upstream),
            owner: Arc::clone(&self.inner),
            ended: false,
        };
        // hyper never polls a body that is empty from the start, so the
        // connection would be closed with it instead of pooled.
        if body.body.is_end_stream() {
            poll_fn(|cx| {
                body.finish(cx);
                Poll::Ready(())
            })
            .await;
        }
        Response::from_parts(parts, Either::Left(body))
    }
}

/// Parses and checks an `--upstream` base URL with the rules of
/// [`Forwarder::new`](crate::forward::Forwarder::new).
fn parse_base(upstream: &str) -> Result<Url, UpstreamUrlError> {
    let url = Url::parse(upstream).map_err(|source| UpstreamUrlError::Parse {
        url: upstream.to_owned(),
        source,
    })?;
    let unsupported = |reason| UpstreamUrlError::Unsupported {
        url: upstream.to_owned(),
        reason,
    };
    if !matches!(url.scheme(), "http" | "https") {
        return Err(unsupported("must use the http or https scheme"));
    }
    if !url.has_host() {
        return Err(unsupported("must have a host"));
    }
    if url.query().is_some() || url.fragment().is_some() {
        return Err(unsupported("must not have a query or fragment"));
    }
    // floor-A refuses credentials because reqwest would drop them silently;
    // floor-B would drop them too, and the two accept the same URLs.
    if !url.username().is_empty() || url.password().is_some() {
        return Err(unsupported("must not contain credentials"));
    }
    Ok(url)
}

/// Joins `base` with the request-target of `uri`, as floor-A does.
///
/// Returns `None`, after logging, when URL parsing would rewrite the joined
/// URL (dot segments, characters the URL standard percent-encodes), so it
/// cannot be forwarded unchanged.
fn join_target(base: &str, uri: &Uri) -> Option<String> {
    let target = uri.path_and_query().map_or("/", PathAndQuery::as_str);
    let mut raw = String::with_capacity(base.len() + target.len());
    raw.push_str(base);
    raw.push_str(target);
    match Url::parse(&raw) {
        Ok(url) if url.as_str() == raw => Some(raw),
        parsed => {
            tracing::warn!(
                request_target = target,
                rewritten = parsed.as_ref().ok().map(Url::as_str),
                "request-target would be rewritten; refusing"
            );
            None
        }
    }
}

impl Inner {
    fn lock_idle(&self) -> MutexGuard<'_, Vec<Upstream>> {
        self.idle
            .lock()
            .expect("idle pool mutex poisoned, although only push and pop run under it")
    }

    /// Takes the most recently pooled connection that is still ready.
    fn checkout(&self) -> Option<Upstream> {
        loop {
            let upstream = self.lock_idle().pop()?;
            if upstream.sender.is_ready() {
                return Some(upstream);
            }
            // Closed: dropped here, after the lock has been released.
        }
    }

    /// Takes back a connection whose response has ended. It is pooled at
    /// once when its sender is ready, the normal case. Otherwise the upstream
    /// answered before the whole request body was sent: a task spawned on the
    /// current Tokio runtime then drives the connection through the rest of
    /// the upload and pools it once it is ready. A finished or failed
    /// connection is dropped.
    fn release(self: &Arc<Self>, mut upstream: Upstream, cx: &mut Context<'_>) {
        match upstream.poll_idle(cx) {
            Poll::Ready(true) => self.lock_idle().push(upstream),
            Poll::Ready(false) => {}
            Poll::Pending => {
                tracing::debug!(
                    "upstream answered before the request body was sent; pooling the connection after the upload"
                );
                let owner = Arc::clone(self);
                tokio::spawn(async move {
                    if poll_fn(|cx| upstream.poll_idle(cx)).await {
                        owner.lock_idle().push(upstream);
                    }
                });
            }
        }
    }

    /// Opens a new upstream connection, within the connect timeout.
    async fn connect(&self) -> Result<Upstream, ConnectError> {
        let stream = tokio::time::timeout(self.connect_timeout, self.open_stream())
            .await
            .map_err(|_| ConnectError::Timeout(self.connect_timeout))??;
        let (sender, conn) = self
            .builder
            .handshake(TokioIo::new(stream))
            .await
            .map_err(ConnectError::Handshake)?;
        Ok(Upstream {
            sender,
            conn: Some(conn),
        })
    }

    async fn open_stream(&self) -> Result<UpstreamStream, ConnectError> {
        let tcp = TcpStream::connect(self.connect_addr.as_str())
            .await
            .map_err(ConnectError::Tcp)?;
        tcp.set_nodelay(true).map_err(ConnectError::Tcp)?;
        match &self.tls {
            None => Ok(UpstreamStream::Plain(tcp)),
            Some((connector, name)) => {
                let tls = connector
                    .connect(name.clone(), tcp)
                    .await
                    .map_err(ConnectError::Tls)?;
                Ok(UpstreamStream::Tls(Box::new(tls)))
            }
        }
    }
}

/// Why a new upstream connection could not be opened.
#[derive(Debug, thiserror::Error)]
enum ConnectError {
    #[error("TCP connect failed")]
    Tcp(#[source] io::Error),
    #[error("TLS handshake failed")]
    Tls(#[source] io::Error),
    #[error("connect timed out after {0:?}")]
    Timeout(Duration),
    #[error("HTTP/1 handshake failed")]
    Handshake(#[source] hyper::Error),
}

/// One upstream connection: the request sender and the connection state
/// machine, which this module polls itself instead of spawning it.
struct Upstream {
    sender: SendRequest<Incoming>,
    /// `None` once the connection has finished (closed or failed).
    conn: Option<Connection<TokioIo<UpstreamStream>, Incoming>>,
}

impl Upstream {
    /// Advances the connection's I/O; forgets it once it has finished.
    fn poll_conn(&mut self, cx: &mut Context<'_>) {
        if let Some(conn) = &mut self.conn
            && let Poll::Ready(result) = Pin::new(conn).poll(cx)
        {
            // hyper has already failed the request or body waiting on the
            // connection (a body only with a generic "connection error"), so
            // this records the cause, at the level the legacy client uses.
            if let Err(err) = result {
                tracing::debug!(error = ?err, "upstream connection failed");
            }
            self.conn = None;
        }
    }

    /// Drives the connection until it can take the next request: `true`
    /// then, `false` if it has finished or failed first.
    fn poll_idle(&mut self, cx: &mut Context<'_>) -> Poll<bool> {
        if self.conn.is_none() {
            return Poll::Ready(false);
        }
        let mut ready = self.sender.poll_ready(cx);
        if ready.is_pending() {
            // The dispatcher asks for the next request only when it is
            // polled after the exchange ended, which the last poll of the
            // body may have preceded; polling it also writes what is left of
            // the request body.
            self.poll_conn(cx);
            if self.conn.is_none() {
                return Poll::Ready(false);
            }
            ready = self.sender.poll_ready(cx);
        }
        // An error means the connection is closing, not that the finished
        // exchange failed.
        ready.map(|result| result.is_ok())
    }

    /// Sends `request` and drives the connection until the response head
    /// arrives or the request fails.
    #[expect(
        clippy::result_large_err,
        reason = "the error is hyper's own, and it carries the unsent request back for the retry"
    )]
    async fn send(
        &mut self,
        request: Request<Incoming>,
    ) -> Result<Response<Incoming>, TrySendError<Request<Incoming>>> {
        let mut response = pin!(self.sender.try_send_request(request));
        poll_fn(|cx| {
            // The connection first, so the request is written in this poll.
            self.poll_conn(cx);
            response.as_mut().poll(cx)
        })
        .await
    }
}

/// Transport of an upstream connection.
#[derive(Debug)]
enum UpstreamStream {
    Plain(TcpStream),
    // Boxed because a TLS stream is many times the size of a socket, and a
    // pooled connection should not carry that size when it is plain.
    Tls(Box<tokio_rustls::client::TlsStream<TcpStream>>),
}

impl AsyncRead for UpstreamStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Plain(s) => Pin::new(s).poll_read(cx, buf),
            Self::Tls(s) => Pin::new(&mut **s).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for UpstreamStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Self::Plain(s) => Pin::new(s).poll_write(cx, buf),
            Self::Tls(s) => Pin::new(&mut **s).poll_write(cx, buf),
        }
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Self::Plain(s) => Pin::new(s).poll_write_vectored(cx, bufs),
            Self::Tls(s) => Pin::new(&mut **s).poll_write_vectored(cx, bufs),
        }
    }

    // hyper picks its write strategy from this, so it must report the
    // transport's own answer, as reqwest's connection wrappers do.
    fn is_write_vectored(&self) -> bool {
        match self {
            Self::Plain(s) => s.is_write_vectored(),
            Self::Tls(s) => s.is_write_vectored(),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Plain(s) => Pin::new(s).poll_flush(cx),
            Self::Tls(s) => Pin::new(&mut **s).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Plain(s) => Pin::new(s).poll_shutdown(cx),
            Self::Tls(s) => Pin::new(&mut **s).poll_shutdown(cx),
        }
    }
}

/// The upstream response body, which also drives its upstream connection.
///
/// Each [`poll_frame`](Body::poll_frame) polls the upstream [`Connection`]
/// first, so the socket is read (and a request body still being uploaded is
/// written) in the task that forwards the response, then the upstream body.
///
/// At the end of the body the connection goes back to the pool once its
/// [`SendRequest`] is ready; dropping the body earlier closes it. hyper stops
/// polling a response body as soon as it reports
/// [`is_end_stream`](Body::is_end_stream), once its `Content-Length` has been
/// written, or once it has returned trailers, without asking for the end of
/// the stream. So the frame that completes the upstream body (the last bytes
/// of a known length, or the trailers, which come only after all data) ends
/// it too: the connection is handed back first, and the frame is returned
/// with `is_end_stream` already true. The connection is normally ready in
/// that same poll, as its dispatcher returns to idle and asks for the next
/// request while it reads the body's last bytes, and it is pooled there. It
/// is not while the request body is still being written; the end of the
/// response is passed on regardless, and the connection is pooled later, as
/// the module documentation describes.
pub struct SameTaskBody {
    body: Incoming,
    /// `None` once the connection has been handed back.
    upstream: Option<Upstream>,
    owner: Arc<Inner>,
    ended: bool,
}

impl fmt::Debug for SameTaskBody {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SameTaskBody")
            .field("ended", &self.ended)
            .field(
                "connection_open",
                &self.upstream.as_ref().is_some_and(|u| u.conn.is_some()),
            )
            .finish_non_exhaustive()
    }
}

impl SameTaskBody {
    /// Marks the body as ended and hands its connection back.
    fn finish(&mut self, cx: &mut Context<'_>) {
        self.ended = true;
        if let Some(upstream) = self.upstream.take() {
            self.owner.release(upstream, cx);
        }
    }
}

impl Body for SameTaskBody {
    type Data = Bytes;
    type Error = hyper::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, hyper::Error>>> {
        let this = self.get_mut();
        if this.ended {
            return Poll::Ready(None);
        }
        if let Some(upstream) = &mut this.upstream {
            upstream.poll_conn(cx);
        }
        let last = match Pin::new(&mut this.body).poll_frame(cx) {
            Poll::Ready(None) => None,
            Poll::Ready(Some(Ok(frame))) if frame.is_trailers() || this.body.is_end_stream() => {
                Some(frame)
            }
            other => return other,
        };
        this.finish(cx);
        Poll::Ready(last.map(Ok))
    }

    fn is_end_stream(&self) -> bool {
        self.ended
    }

    fn size_hint(&self) -> SizeHint {
        if self.ended {
            SizeHint::with_exact(0)
        } else {
            self.body.size_hint()
        }
    }
}

fn text_response(status: StatusCode, body: &'static str) -> Response<FloorBody> {
    let mut response = Response::new(Either::Right(Full::new(Bytes::from_static(
        body.as_bytes(),
    ))));
    *response.status_mut() = status;
    response.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    response
}

/// Serves `forwarder` on `listener` through the gateway's connection layer
/// until `shutdown` completes, then drains in-flight connections as
/// [`brisk_gateway::server::serve`] describes.
pub async fn run(
    listener: TcpListener,
    tls: Option<TlsAcceptor>,
    config: ServerConfig,
    forwarder: SameTaskForwarder,
    shutdown: impl Future<Output = ()>,
) -> io::Result<()> {
    let service = service_fn(move |req| {
        let forwarder = forwarder.clone();
        async move { Ok::<_, Infallible>(forwarder.forward(req).await) }
    });
    serve(listener, tls, config, service, shutdown).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::forward::Forwarder;

    fn forwarder(upstream: &str) -> Result<SameTaskForwarder, SameTaskConfigError> {
        SameTaskForwarder::new(upstream, None)
    }

    fn tls_config() -> Arc<ClientConfig> {
        let bundle =
            brisk_bench_core::transport::certs::CertBundle::generate(&["localhost".to_owned()])
                .unwrap();
        brisk_bench_core::transport::tls::client_config_from_pem(bundle.ca_cert_pem.as_bytes())
            .unwrap()
    }

    fn floor_a(upstream: &str) -> Result<Forwarder, UpstreamUrlError> {
        let client =
            brisk_gateway::upstream::build_client(&brisk_gateway::upstream::UpstreamClientConfig {
                // Same profile as the binary; no request is sent.
                allow_private: true,
                ..brisk_gateway::upstream::UpstreamClientConfig::default()
            })
            .unwrap();
        Forwarder::new(client, upstream)
    }

    #[test]
    fn host_header_keeps_only_a_non_default_port() {
        let fwd = forwarder("http://127.0.0.1:9000/prefix/").unwrap();
        assert_eq!(fwd.inner.host, "127.0.0.1:9000");
        assert_eq!(fwd.inner.connect_addr, "127.0.0.1:9000");
        assert_eq!(fwd.upstream_base(), "http://127.0.0.1:9000/prefix");
        assert_eq!(&fwd.inner.base[fwd.inner.origin_len..], "/prefix");

        let fwd = forwarder("http://mock.local:80").unwrap();
        assert_eq!(fwd.inner.host, "mock.local");
        assert_eq!(fwd.inner.connect_addr, "mock.local:80");
        assert_eq!(fwd.inner.origin_len, fwd.inner.base.len());

        let fwd = forwarder("http://[::1]:8080//").unwrap();
        assert_eq!(fwd.inner.host, "[::1]:8080");
        assert_eq!(fwd.inner.connect_addr, "[::1]:8080");
        assert_eq!(fwd.upstream_base(), "http://[::1]:8080");
        assert_eq!(fwd.inner.origin_len, fwd.inner.base.len());
    }

    #[test]
    fn https_needs_a_ca_and_takes_the_bare_host_as_server_name() {
        assert!(matches!(
            forwarder("https://127.0.0.1:19443"),
            Err(SameTaskConfigError::MissingCa(_))
        ));
        let fwd = SameTaskForwarder::new("https://[::1]:19443", Some(tls_config())).unwrap();
        let (_, name) = fwd.inner.tls.as_ref().unwrap();
        assert_eq!(name.to_str(), "::1");
        let fwd = SameTaskForwarder::new("https://localhost", Some(tls_config())).unwrap();
        assert_eq!(fwd.inner.host, "localhost");
        assert_eq!(fwd.inner.connect_addr, "localhost:443");
        assert!(fwd.inner.tls.is_some());
        // The CA is only needed for TLS.
        let fwd = SameTaskForwarder::new("http://localhost:8080", Some(tls_config())).unwrap();
        assert!(fwd.inner.tls.is_none());
    }

    #[test]
    fn https_offers_only_alpn_http11_whatever_the_config_offers() {
        for offered in [vec![b"h2".to_vec(), b"http/1.1".to_vec()], Vec::new()] {
            let mut config = Arc::unwrap_or_clone(tls_config());
            config.alpn_protocols = offered.clone();
            let fwd = SameTaskForwarder::new("https://localhost", Some(Arc::new(config))).unwrap();
            let (connector, _) = fwd.inner.tls.as_ref().unwrap();
            assert_eq!(
                connector.config().alpn_protocols,
                [b"http/1.1".to_vec()],
                "{offered:?}"
            );
        }
    }

    #[test]
    fn accepts_and_normalizes_base_urls_exactly_as_floor_a() {
        for url in [
            "http://127.0.0.1:9000",
            "http://127.0.0.1:9000/",
            "https://mock.local:8443/prefix/",
            "http://h//",
            "http://[::1]:8080/a/b",
            "127.0.0.1:9000",
            "ftp://host/",
            "http://host/?q=1",
            "http://host/#frag",
            "http://user:pw@host/",
            "http://user@host/",
            "not a url",
            "data:text/plain,x",
        ] {
            // `tls` is given so an https URL is judged on the URL alone.
            let b = SameTaskForwarder::new(url, Some(tls_config()));
            match floor_a(url) {
                Ok(a) => assert_eq!(b.unwrap().upstream_base(), a.upstream_base(), "{url}"),
                Err(_) => assert!(
                    matches!(b, Err(SameTaskConfigError::Url(_))),
                    "{url} was accepted by floor-B only"
                ),
            }
        }
    }

    #[test]
    fn joined_targets_are_refused_where_floor_a_refuses_them() {
        let base = "http://127.0.0.1:9000/prefix";
        for (target, forwarded) in [
            ("/v1/models?q=a%27b&x=%2F", true),
            ("/", true),
            ("/v1/../../admin", false),
            ("/a/%2e%2e/secret", false),
            ("/a/./b", false),
            ("/v1/models?q=it's", false),
        ] {
            let uri: Uri = target.parse().unwrap();
            let joined = join_target(base, &uri);
            assert_eq!(joined.is_some(), forwarded, "{target}");
            if let Some(joined) = joined {
                assert_eq!(joined, format!("{base}{target}"));
            }
        }
    }

    #[test]
    fn upstream_headers_match_what_floor_a_sends() {
        let fwd = forwarder("http://127.0.0.1:9000").unwrap();
        let mut inbound = HeaderMap::new();
        inbound.insert(HOST, HeaderValue::from_static("floor.example"));
        inbound.insert("connection", HeaderValue::from_static("x-hop"));
        inbound.insert("x-hop", HeaderValue::from_static("1"));
        inbound.insert(TE, HeaderValue::from_static("trailers, gzip"));
        inbound.append("cookie", HeaderValue::from_static("a=1"));
        inbound.append("cookie", HeaderValue::from_static("b=2"));
        inbound.insert("authorization", HeaderValue::from_static("Bearer k"));

        let h1 = fwd.upstream_headers(inbound.clone(), Version::HTTP_11);
        let names: Vec<&str> = h1.keys().map(http::HeaderName::as_str).collect();
        assert_eq!(names, ["authorization", "cookie", "te", "accept", "host"]);
        assert_eq!(h1[TE], "trailers");
        assert_eq!(h1[ACCEPT], "*/*");
        assert_eq!(h1[HOST], "127.0.0.1:9000");
        assert_eq!(h1.get_all("cookie").iter().count(), 2);

        let h2 = fwd.upstream_headers(inbound.clone(), Version::HTTP_2);
        assert_eq!(
            h2.get_all("cookie").iter().collect::<Vec<_>>(),
            ["a=1; b=2"]
        );

        inbound.insert(ACCEPT, HeaderValue::from_static("text/event-stream"));
        let kept = fwd.upstream_headers(inbound, Version::HTTP_11);
        assert_eq!(kept[ACCEPT], "text/event-stream");
    }

    #[test]
    fn text_responses_carry_status_and_content_type() {
        let response = text_response(StatusCode::BAD_GATEWAY, BAD_GATEWAY_BODY);
        assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
        assert_eq!(
            response.headers()[CONTENT_TYPE],
            "text/plain; charset=utf-8"
        );
    }
}
