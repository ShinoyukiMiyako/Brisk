//! Inbound connection layer: listener setup, the accept loop and per-connection
//! HTTP serving.
//!
//! The accept loop is hand-written instead of relying on a framework so that
//! connection limits, accept-error backoff, TLS handshake deadlines and graceful
//! shutdown stay under direct control (see the data-plane design, section on
//! the downstream side and R20).

use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use http::{Request, Response};
use http_body::Body;
use hyper::body::Incoming;
use hyper::service::Service;
use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
use hyper_util::server::conn::auto;
use hyper_util::server::graceful::{GracefulShutdown, Watcher};
use socket2::{Domain, Protocol, Socket, Type};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinSet;
use tokio_rustls::TlsAcceptor;

use crate::{BoxError, net};

/// File descriptors kept free for upstream connections when deriving the
/// default inbound connection limit from `RLIMIT_NOFILE`.
#[cfg(unix)]
const UPSTREAM_FD_RESERVE: u64 = 1024;

/// Lower bound for the derived default inbound connection limit.
#[cfg(unix)]
const MIN_MAX_CONNECTIONS: usize = 256;

/// Default inbound connection limit on Windows, which has no `RLIMIT_NOFILE`.
#[cfg(not(unix))]
const WINDOWS_MAX_CONNECTIONS: usize = 10_000;

/// Backoff after `EMFILE`/`ENFILE`: descriptors only come back when other
/// connections close, so spinning on `accept` would just burn CPU.
const RESOURCE_EXHAUSTED_BACKOFF: Duration = Duration::from_millis(20);

/// Backoff after any other accept error, which is usually a connection that was
/// reset before it could be accepted.
const TRANSIENT_BACKOFF: Duration = Duration::from_millis(1);

/// Inbound server settings.
///
/// [`Default`] yields the values from the data-plane design document. On Unix
/// the default connection limit is derived from the current `RLIMIT_NOFILE`
/// soft limit, so binaries should call [`net::raise_nofile_soft_limit`] before
/// [`ServerConfig::default`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerConfig {
    /// Maximum number of concurrently open inbound connections. When reached,
    /// the accept loop stops accepting until a connection closes.
    pub max_connections: usize,
    /// Deadline for receiving a request head. For HTTP/1 it applies to every
    /// request head, including the idle wait between keep-alive requests. For
    /// every connection it also bounds the time from accept (or TLS handshake
    /// completion) until the first request arrives, so a silent client cannot
    /// hold a connection permit during protocol detection.
    pub header_read_timeout: Duration,
    /// HTTP/2 per-stream initial flow-control window, in bytes. At most
    /// `2^31 - 1`; ignored when [`h2_adaptive_window`](Self::h2_adaptive_window)
    /// is set.
    pub h2_initial_stream_window_size: u32,
    /// HTTP/2 per-connection initial flow-control window, in bytes. At most
    /// `2^31 - 1`; ignored when [`h2_adaptive_window`](Self::h2_adaptive_window)
    /// is set.
    pub h2_initial_connection_window_size: u32,
    /// Whether HTTP/2 uses BDP-based adaptive flow-control windows. When set,
    /// hyper starts both windows at the protocol default of 65535 bytes and
    /// overrides the two `h2_initial_*_window_size` values.
    pub h2_adaptive_window: bool,
    /// HTTP/2 `SETTINGS_MAX_CONCURRENT_STREAMS`.
    pub h2_max_concurrent_streams: u32,
    /// HTTP/2 `SETTINGS_MAX_HEADER_LIST_SIZE`, in bytes.
    pub h2_max_header_list_size: u32,
    /// Interval between HTTP/2 keep-alive pings.
    pub h2_keep_alive_interval: Duration,
    /// Deadline for completing an inbound TLS handshake.
    pub tls_handshake_timeout: Duration,
    /// How long [`serve`] waits for in-flight connections after shutdown was
    /// requested before aborting them.
    pub graceful_shutdown_timeout: Duration,
    /// TCP keepalive idle time for accepted sockets.
    pub tcp_keepalive: Duration,
    /// Listen backlog passed to `listen(2)`.
    pub backlog: i32,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            max_connections: default_max_connections(),
            header_read_timeout: Duration::from_secs(30),
            h2_initial_stream_window_size: 1024 * 1024,
            h2_initial_connection_window_size: 2 * 1024 * 1024,
            h2_adaptive_window: false,
            h2_max_concurrent_streams: 256,
            h2_max_header_list_size: 64 * 1024,
            h2_keep_alive_interval: Duration::from_secs(30),
            tls_handshake_timeout: Duration::from_secs(10),
            graceful_shutdown_timeout: Duration::from_secs(30),
            tcp_keepalive: Duration::from_secs(30),
            backlog: 4096,
        }
    }
}

/// Derives the default connection limit: 80% of the `RLIMIT_NOFILE` soft
/// limit, minus a reserve for upstream sockets, but never below 256.
#[cfg(unix)]
fn default_max_connections() -> usize {
    let soft = rustix::process::getrlimit(rustix::process::Resource::Nofile).current;
    max_connections_for_nofile(soft)
}

#[cfg(not(unix))]
fn default_max_connections() -> usize {
    WINDOWS_MAX_CONNECTIONS
}

/// `soft_limit` of `None` means `RLIM_INFINITY`.
#[cfg(unix)]
fn max_connections_for_nofile(soft_limit: Option<u64>) -> usize {
    let Some(soft) = soft_limit else {
        return Semaphore::MAX_PERMITS;
    };
    let derived = (soft / 5 * 4).saturating_sub(UPSTREAM_FD_RESERVE);
    usize::try_from(derived)
        .unwrap_or(Semaphore::MAX_PERMITS)
        .clamp(MIN_MAX_CONNECTIONS, Semaphore::MAX_PERMITS)
}

/// Creates a non-blocking listener with `SO_REUSEADDR` and
/// [`ServerConfig::backlog`].
///
/// Must be called from within a Tokio runtime, because the socket is
/// registered with the runtime's reactor.
pub fn bind(addr: SocketAddr, config: &ServerConfig) -> io::Result<TcpListener> {
    let socket = Socket::new(Domain::for_address(addr), Type::STREAM, Some(Protocol::TCP))?;
    // On Windows SO_REUSEADDR lets another process steal a bound port, which
    // is the opposite of the fast-rebind semantics wanted on Unix.
    #[cfg(unix)]
    socket.set_reuse_address(true)?;
    socket.set_nonblocking(true)?;
    socket.bind(&addr.into())?;
    socket.listen(config.backlog)?;
    TcpListener::from_std(socket.into())
}

/// How the accept loop reacts to an `accept` error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AcceptErrorClass {
    /// The process or system ran out of file descriptors.
    ResourceExhausted,
    /// Any other error, typically a peer that reset before being accepted.
    Transient,
}

impl AcceptErrorClass {
    /// Classifies an error returned by `accept`.
    pub fn of(err: &io::Error) -> Self {
        match err.raw_os_error() {
            Some(code) if is_fd_exhaustion(code) => Self::ResourceExhausted,
            _ => Self::Transient,
        }
    }

    /// How long the accept loop sleeps before retrying.
    pub const fn backoff(self) -> Duration {
        match self {
            Self::ResourceExhausted => RESOURCE_EXHAUSTED_BACKOFF,
            Self::Transient => TRANSIENT_BACKOFF,
        }
    }
}

#[cfg(unix)]
fn is_fd_exhaustion(code: i32) -> bool {
    use rustix::io::Errno;
    code == Errno::MFILE.raw_os_error() || code == Errno::NFILE.raw_os_error()
}

/// `WSAEMFILE`: Winsock's "too many open sockets", the Windows counterpart of
/// `EMFILE`. Windows has no system-wide `ENFILE` equivalent.
#[cfg(windows)]
const WSAEMFILE: i32 = 10024;

#[cfg(windows)]
fn is_fd_exhaustion(code: i32) -> bool {
    code == WSAEMFILE
}

#[cfg(not(any(unix, windows)))]
fn is_fd_exhaustion(_code: i32) -> bool {
    false
}

/// Largest HTTP/2 flow-control window allowed by RFC 9113, section 6.9.1.
const MAX_H2_WINDOW_SIZE: u32 = (1 << 31) - 1;

/// Connection limits below this are logged as a warning at startup, since they
/// usually come from an unraised `RLIMIT_NOFILE` rather than a deliberate
/// choice.
const LOW_MAX_CONNECTIONS: usize = 4096;

/// Per-connection context handed to the service factory of
/// [`serve_with_conn_info`].
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct ConnInfo {
    /// Address of the remote peer.
    pub peer: SocketAddr,
    /// Local address the connection was accepted on.
    pub local: SocketAddr,
    /// TLS session details, `None` for plaintext connections.
    pub tls: Option<TlsInfo>,
}

/// Negotiated TLS parameters of an inbound connection.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct TlsInfo {
    /// Server name (SNI) sent by the client, if any.
    pub server_name: Option<String>,
    /// Negotiated ALPN protocol, if any.
    pub alpn_protocol: Option<Vec<u8>>,
}

/// Accepts and serves HTTP/1.1 and HTTP/2 connections until `shutdown`
/// completes, cloning `service` once per connection.
///
/// Equivalent to [`serve_with_conn_info`] with a factory that ignores the
/// [`ConnInfo`]; see there for the full behavior. A
/// [`hyper::service::service_fn`] over a `Clone` closure satisfies the bounds.
pub async fn serve<S, B>(
    listener: TcpListener,
    tls: Option<TlsAcceptor>,
    config: ServerConfig,
    service: S,
    shutdown: impl Future<Output = ()>,
) -> io::Result<()>
where
    S: Service<Request<Incoming>, Response = Response<B>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Into<BoxError>,
    B: Body + Send + 'static,
    B::Data: Send,
    B::Error: Into<BoxError>,
{
    serve_with_conn_info(listener, tls, config, move |_: &ConnInfo| service, shutdown).await
}

/// Accepts and serves HTTP/1.1 and HTTP/2 connections until `shutdown`
/// completes, building each connection's service from its [`ConnInfo`].
///
/// `make_service` is cloned once per connection and called once, after the TLS
/// handshake if there is one, so per-connection context (peer address, SNI,
/// ALPN) costs nothing on the request path.
///
/// - A permit from a semaphore sized [`ServerConfig::max_connections`] is
///   taken *before* each `accept`; it is released when the connection task
///   ends, so a full server leaves new connections in the kernel backlog.
/// - `accept` errors never terminate the loop; they back off according to
///   [`AcceptErrorClass`]. Only the first error of a streak is logged at warn
///   level, plus the streak length once `accept` recovers, so an error storm
///   cannot flood the log.
/// - Accepted sockets get `TCP_NODELAY` and keepalive via
///   [`net::tune_accepted`].
/// - With `tls`, the handshake runs inside the connection task and is bounded
///   by [`ServerConfig::tls_handshake_timeout`].
/// - A connection that sends no request within
///   [`ServerConfig::header_read_timeout`] (counted after the TLS handshake)
///   is closed.
/// - After `shutdown` completes the listener is closed, in-flight connections
///   are asked to finish gracefully, and they are aborted once
///   [`ServerConfig::graceful_shutdown_timeout`] elapses.
///
/// Returns [`io::ErrorKind::InvalidInput`] if `max_connections` is zero or
/// exceeds [`Semaphore::MAX_PERMITS`], or if an HTTP/2 window exceeds
/// `2^31 - 1`. Per-connection failures are logged at debug level and never
/// end the loop.
pub async fn serve_with_conn_info<F, S, B>(
    listener: TcpListener,
    tls: Option<TlsAcceptor>,
    config: ServerConfig,
    make_service: F,
    shutdown: impl Future<Output = ()>,
) -> io::Result<()>
where
    F: FnOnce(&ConnInfo) -> S + Clone + Send + 'static,
    S: Service<Request<Incoming>, Response = Response<B>> + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Into<BoxError>,
    B: Body + Send + 'static,
    B::Data: Send,
    B::Error: Into<BoxError>,
{
    validate(&config)?;
    if config.max_connections < LOW_MAX_CONNECTIONS {
        tracing::warn!(
            max_connections = config.max_connections,
            "low inbound connection limit; raise RLIMIT_NOFILE with \
             net::raise_nofile_soft_limit before ServerConfig::default()"
        );
    } else {
        tracing::info!(
            max_connections = config.max_connections,
            "inbound connection limit"
        );
    }

    let limiter = Arc::new(Semaphore::new(config.max_connections));
    let builder = Arc::new(http_builder(&config));
    let graceful = GracefulShutdown::new();
    let mut connections = JoinSet::new();
    let mut shutdown = std::pin::pin!(shutdown);

    loop {
        let accepted = tokio::select! {
            biased;
            () = &mut shutdown => break,
            Some(joined) = connections.join_next(), if !connections.is_empty() => {
                log_join_result(joined);
                continue;
            }
            accepted = accept_with_permit(&listener, &limiter) => accepted?,
        };
        let (stream, peer, permit) = accepted;

        let local = match net::tune_accepted(&stream, config.tcp_keepalive)
            .and_then(|()| stream.local_addr())
        {
            Ok(local) => local,
            Err(err) => {
                tracing::debug!(error = %err, %peer, "failed to set up accepted socket");
                continue;
            }
        };

        let conn = Connection {
            builder: Arc::clone(&builder),
            watcher: graceful.watcher(),
            first_request_timeout: config.header_read_timeout,
            _permit: permit,
        };
        let make_service = make_service.clone();
        if let Some(acceptor) = &tls {
            let acceptor = acceptor.clone();
            let handshake_timeout = config.tls_handshake_timeout;
            connections.spawn(async move {
                match tokio::time::timeout(handshake_timeout, acceptor.accept(stream)).await {
                    Ok(Ok(tls_stream)) => {
                        let session = tls_stream.get_ref().1;
                        let info = ConnInfo {
                            peer,
                            local,
                            tls: Some(TlsInfo {
                                server_name: session.server_name().map(str::to_owned),
                                alpn_protocol: session.alpn_protocol().map(<[u8]>::to_vec),
                            }),
                        };
                        let service = make_service(&info);
                        conn.run(tls_stream, service).await;
                    }
                    Ok(Err(err)) => {
                        tracing::debug!(error = %err, %peer, "TLS handshake failed");
                    }
                    Err(_) => tracing::debug!(%peer, "TLS handshake timed out"),
                }
            });
        } else {
            let info = ConnInfo {
                peer,
                local,
                tls: None,
            };
            let service = make_service(&info);
            connections.spawn(conn.run(stream, service));
        }
    }

    drop(listener);
    drain(graceful, connections, config.graceful_shutdown_timeout).await;
    Ok(())
}

/// Rejects settings that would make `serve` panic or every connection fail.
fn validate(config: &ServerConfig) -> io::Result<()> {
    if config.max_connections == 0 || config.max_connections > Semaphore::MAX_PERMITS {
        return Err(invalid_input(format!(
            "max_connections must be in 1..={}, got {}",
            Semaphore::MAX_PERMITS,
            config.max_connections
        )));
    }
    if !config.h2_adaptive_window {
        let windows = [
            (
                "h2_initial_stream_window_size",
                config.h2_initial_stream_window_size,
            ),
            (
                "h2_initial_connection_window_size",
                config.h2_initial_connection_window_size,
            ),
        ];
        for (name, value) in windows {
            if value > MAX_H2_WINDOW_SIZE {
                return Err(invalid_input(format!(
                    "{name} must be at most {MAX_H2_WINDOW_SIZE}, got {value}"
                )));
            }
        }
    }
    Ok(())
}

fn invalid_input(message: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

/// Waits for a connection permit, then accepts one connection, retrying
/// `accept` errors with backoff while keeping the permit.
async fn accept_with_permit(
    listener: &TcpListener,
    limiter: &Arc<Semaphore>,
) -> io::Result<(TcpStream, SocketAddr, OwnedSemaphorePermit)> {
    let permit = Arc::clone(limiter)
        .acquire_owned()
        .await
        .map_err(|_| io::Error::other("connection limiter closed"))?;
    let mut failures: u64 = 0;
    loop {
        match listener.accept().await {
            Ok((stream, peer)) => {
                if failures > 0 {
                    tracing::warn!(failures, "accept recovered after consecutive errors");
                }
                return Ok((stream, peer, permit));
            }
            Err(err) => {
                let class = AcceptErrorClass::of(&err);
                if failures == 0 {
                    tracing::warn!(
                        error = %err,
                        ?class,
                        "accept failed; further errors are counted until accept recovers"
                    );
                } else {
                    tracing::trace!(error = %err, ?class, "accept failed");
                }
                failures = failures.saturating_add(1);
                tokio::time::sleep(class.backoff()).await;
            }
        }
    }
}

fn http_builder(config: &ServerConfig) -> auto::Builder<TokioExecutor> {
    let mut builder = auto::Builder::new(TokioExecutor::new());
    builder
        .http1()
        .timer(TokioTimer::new())
        .keep_alive(true)
        // Explicit although it is hyper's default: only without half-close
        // does hyper watch the read side for EOF while a request is handled
        // and its response written (`mid_message_detect_eof`), which is how a
        // client disconnect drops the upstream request in time. hyper skips
        // that probe when the client pipelined more bytes after the request.
        .half_close(false)
        .header_read_timeout(config.header_read_timeout);
    builder
        .http2()
        .timer(TokioTimer::new())
        .initial_stream_window_size(config.h2_initial_stream_window_size)
        .initial_connection_window_size(config.h2_initial_connection_window_size)
        .adaptive_window(config.h2_adaptive_window)
        .max_concurrent_streams(config.h2_max_concurrent_streams)
        .max_header_list_size(config.h2_max_header_list_size)
        .keep_alive_interval(config.h2_keep_alive_interval);
    builder
}

/// Everything a connection task owns besides its service. Dropping it returns
/// the connection permit and detaches from graceful-shutdown tracking.
struct Connection {
    builder: Arc<auto::Builder<TokioExecutor>>,
    watcher: Watcher,
    first_request_timeout: Duration,
    _permit: OwnedSemaphorePermit,
}

impl Connection {
    async fn run<I, S, B>(self, io: I, service: S)
    where
        I: AsyncRead + AsyncWrite + Unpin + Send + 'static,
        S: Service<Request<Incoming>, Response = Response<B>> + Send + 'static,
        S::Future: Send + 'static,
        S::Error: Into<BoxError>,
        B: Body + Send + 'static,
        B::Data: Send,
        B::Error: Into<BoxError>,
    {
        let Self {
            builder,
            watcher,
            first_request_timeout,
            _permit,
        } = self;
        let first = Arc::new(FirstRequest::default());
        let service = SignalFirstRequest {
            inner: service,
            first: Arc::clone(&first),
        };
        let conn = watcher.watch(builder.serve_connection(TokioIo::new(io), service));
        let mut conn = std::pin::pin!(conn);

        // hyper-util's protocol detection and HTTP/2 have no deadline of their
        // own before the first request, so a silent peer would otherwise hold
        // its connection permit forever.
        let started = tokio::select! {
            biased;
            result = &mut conn => {
                log_conn_result(result);
                return;
            }
            () = first.notify.notified() => true,
            () = tokio::time::sleep(first_request_timeout) => false,
        };
        if !started {
            tracing::debug!("no request before the first-request deadline; closing");
            return;
        }
        log_conn_result(conn.await);
    }
}

fn log_conn_result(result: Result<(), BoxError>) {
    if let Err(err) = result {
        tracing::debug!(error = %err, "connection closed with error");
    }
}

/// Signals that a connection has received its first request.
#[derive(Debug, Default)]
struct FirstRequest {
    seen: AtomicBool,
    notify: Notify,
}

impl FirstRequest {
    fn mark(&self) {
        // `notify_one` stores a permit when nobody waits yet, so the signal is
        // not lost if the request arrives before `notified()` is polled.
        if !self.seen.swap(true, Ordering::Relaxed) {
            self.notify.notify_one();
        }
    }
}

/// Service wrapper that reports the first call; afterwards it costs one
/// relaxed atomic load per request.
struct SignalFirstRequest<S> {
    inner: S,
    first: Arc<FirstRequest>,
}

impl<S, R> Service<R> for SignalFirstRequest<S>
where
    S: Service<R>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    fn call(&self, req: R) -> Self::Future {
        if !self.first.seen.load(Ordering::Relaxed) {
            self.first.mark();
        }
        self.inner.call(req)
    }
}

fn log_join_result(joined: Result<(), tokio::task::JoinError>) {
    if let Err(err) = joined
        && err.is_panic()
    {
        tracing::error!(error = %err, "connection task panicked");
    }
}

/// Signals graceful shutdown to every connection and waits up to `timeout`
/// for them to finish; stragglers are aborted.
async fn drain(graceful: GracefulShutdown, mut connections: JoinSet<()>, timeout: Duration) {
    let finished = tokio::time::timeout(timeout, async {
        graceful.shutdown().await;
        while let Some(joined) = connections.join_next().await {
            log_join_result(joined);
        }
    })
    .await;
    if finished.is_err() {
        tracing::warn!(
            remaining = connections.len(),
            "graceful shutdown timed out; aborting remaining connections"
        );
        connections.shutdown().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_matches_design_values() {
        let config = ServerConfig::default();
        assert_eq!(config.header_read_timeout, Duration::from_secs(30));
        assert_eq!(config.h2_initial_stream_window_size, 1 << 20);
        assert_eq!(config.h2_initial_connection_window_size, 2 << 20);
        assert!(!config.h2_adaptive_window);
        assert_eq!(config.h2_max_concurrent_streams, 256);
        assert_eq!(config.h2_max_header_list_size, 64 << 10);
        assert_eq!(config.h2_keep_alive_interval, Duration::from_secs(30));
        assert_eq!(config.tls_handshake_timeout, Duration::from_secs(10));
        assert_eq!(config.graceful_shutdown_timeout, Duration::from_secs(30));
        assert_eq!(config.tcp_keepalive, Duration::from_secs(30));
        assert_eq!(config.backlog, 4096);
        #[cfg(unix)]
        assert!(config.max_connections >= MIN_MAX_CONNECTIONS);
        #[cfg(windows)]
        assert_eq!(config.max_connections, 10_000);
    }

    #[cfg(unix)]
    #[test]
    fn max_connections_derivation() {
        // 65536 * 0.8 = 52428 (integer), minus the 1024 upstream reserve.
        assert_eq!(max_connections_for_nofile(Some(65_536)), 51_404);
        assert_eq!(max_connections_for_nofile(Some(1_000_000)), 798_976);
        // Small limits fall back to the floor instead of going to zero.
        assert_eq!(max_connections_for_nofile(Some(1024)), MIN_MAX_CONNECTIONS);
        assert_eq!(max_connections_for_nofile(Some(0)), MIN_MAX_CONNECTIONS);
        assert_eq!(max_connections_for_nofile(None), Semaphore::MAX_PERMITS);
    }

    #[cfg(unix)]
    #[test]
    fn fd_exhaustion_is_resource_exhausted() {
        use rustix::io::Errno;
        for errno in [Errno::MFILE, Errno::NFILE] {
            let err = io::Error::from_raw_os_error(errno.raw_os_error());
            assert_eq!(
                AcceptErrorClass::of(&err),
                AcceptErrorClass::ResourceExhausted
            );
            assert_eq!(
                AcceptErrorClass::of(&err).backoff(),
                Duration::from_millis(20)
            );
        }
    }

    #[cfg(windows)]
    #[test]
    fn fd_exhaustion_is_resource_exhausted() {
        let err = io::Error::from_raw_os_error(WSAEMFILE);
        assert_eq!(
            AcceptErrorClass::of(&err),
            AcceptErrorClass::ResourceExhausted
        );
        assert_eq!(
            AcceptErrorClass::of(&err).backoff(),
            Duration::from_millis(20)
        );
    }

    #[test]
    fn other_errors_are_transient() {
        let reset = io::Error::from(io::ErrorKind::ConnectionReset);
        let aborted = io::Error::from(io::ErrorKind::ConnectionAborted);
        let custom = io::Error::other("synthetic");
        for err in [reset, aborted, custom] {
            assert_eq!(AcceptErrorClass::of(&err), AcceptErrorClass::Transient);
            assert_eq!(
                AcceptErrorClass::of(&err).backoff(),
                Duration::from_millis(1)
            );
        }
        #[cfg(unix)]
        {
            let econnaborted =
                io::Error::from_raw_os_error(rustix::io::Errno::CONNABORTED.raw_os_error());
            assert_eq!(
                AcceptErrorClass::of(&econnaborted),
                AcceptErrorClass::Transient
            );
        }
    }

    #[tokio::test]
    async fn bind_listens_on_ephemeral_port() {
        let listener = bind("127.0.0.1:0".parse().unwrap(), &ServerConfig::default()).unwrap();
        let addr = listener.local_addr().unwrap();
        assert_ne!(addr.port(), 0);
        let (client, accepted) = tokio::join!(TcpStream::connect(addr), listener.accept());
        client.unwrap();
        accepted.unwrap();
        #[cfg(unix)]
        assert!(
            socket2::SockRef::from(&listener).reuse_address().unwrap(),
            "SO_REUSEADDR must be read back as set"
        );
    }

    #[test]
    fn validate_accepts_defaults() {
        validate(&ServerConfig::default()).unwrap();
    }

    #[test]
    fn validate_rejects_bad_limits_and_windows() {
        let zero = ServerConfig {
            max_connections: 0,
            ..ServerConfig::default()
        };
        let too_many = ServerConfig {
            max_connections: Semaphore::MAX_PERMITS + 1,
            ..ServerConfig::default()
        };
        let stream_window = ServerConfig {
            h2_initial_stream_window_size: MAX_H2_WINDOW_SIZE + 1,
            ..ServerConfig::default()
        };
        let conn_window = ServerConfig {
            h2_initial_connection_window_size: u32::MAX,
            ..ServerConfig::default()
        };
        for config in [zero, too_many, stream_window, conn_window] {
            let err = validate(&config).unwrap_err();
            assert_eq!(err.kind(), io::ErrorKind::InvalidInput, "{config:?}");
        }

        // Adaptive windows replace the configured sizes, so they are not checked.
        let adaptive = ServerConfig {
            h2_adaptive_window: true,
            h2_initial_connection_window_size: u32::MAX,
            ..ServerConfig::default()
        };
        validate(&adaptive).unwrap();
        let max_window = ServerConfig {
            h2_initial_stream_window_size: MAX_H2_WINDOW_SIZE,
            h2_initial_connection_window_size: MAX_H2_WINDOW_SIZE,
            ..ServerConfig::default()
        };
        validate(&max_window).unwrap();
    }

    #[tokio::test]
    async fn first_request_signal_fires_once_even_before_waiting() {
        let first = FirstRequest::default();
        first.mark();
        first.mark();
        tokio::time::timeout(Duration::from_secs(1), first.notify.notified())
            .await
            .expect("signal raised before notified() was polled must not be lost");
        assert!(first.seen.load(Ordering::Relaxed));
    }
}
