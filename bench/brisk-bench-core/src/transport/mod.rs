//! Plaintext and TLS connections over nonblocking `mio` sockets.
//!
//! [`Conn`] is the per-connection object a shard event loop keeps in its slab.
//! It is driven entirely by readiness events:
//!
//! - on a readable event call [`Conn::read`] until it returns
//!   [`io::ErrorKind::WouldBlock`], handling each [`ReadBatch`] as it comes so
//!   every batch keeps its own kernel receive timestamp;
//! - on a writable event call [`Conn::flush`];
//! - after either, call [`Conn::sync_interest`], which re-registers only
//!   when the interest set changed;
//! - to close, call [`Conn::shutdown`] and, while it returns `Ok(false)`,
//!   call it again on later writable events.
//!
//! On Linux every receive is a `recvmsg` so that, once
//! [`Conn::enable_rx_timestamps`] has been called, the `SCM_TIMESTAMPNS`
//! control message of each receive is reported. The timestamp is
//! `CLOCK_REALTIME`; convert it with [`crate::clock::RealtimeOffset`].

pub mod certs;
pub mod tls;

use std::cell::RefCell;
use std::io::{self, BufRead as _, Write as _};
use std::net::SocketAddr;
use std::sync::Arc;

use mio::net::{TcpListener, TcpStream};
use mio::{Interest, Registry, Token};
use rustls::pki_types::ServerName;

/// Default number of bytes requested from the kernel per receive.
pub const DEFAULT_READ_SIZE: usize = 64 * 1024;

thread_local! {
    /// Ciphertext receive buffer shared by all TLS connections of a thread.
    /// Every shard is one thread, so a per-thread buffer gives full-size
    /// receives without a `read_size` buffer per connection.
    static TLS_RX: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
}

/// Outcome of a single [`Conn::read`] call, i.e. of one socket receive.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadBatch {
    /// Plaintext bytes appended to the output buffer. Can be zero for TLS
    /// records that carry no application data (handshake, session tickets).
    pub plaintext_bytes: usize,
    /// `CLOCK_REALTIME` nanoseconds at which the kernel received the data, if
    /// receive timestamps are enabled and the platform supports them. For TLS
    /// every plaintext byte of the batch gets the timestamp of this receive.
    pub kernel_rx_ns: Option<u64>,
    /// The peer closed its sending side (TCP FIN or TLS `close_notify`).
    pub eof: bool,
}

/// A TCP connection with optional TLS, for use in a `mio` event loop.
///
/// Output is buffered without bound: [`write`](Self::write) never refuses
/// data before [`shutdown`](Self::shutdown). Watch its return value or
/// [`pending_bytes`](Self::pending_bytes) for backpressure.
#[derive(Debug)]
pub struct Conn {
    stream: TcpStream,
    tls: Option<rustls::Connection>,
    /// Bytes (ciphertext for TLS) not yet accepted by the kernel.
    pending: Vec<u8>,
    pending_pos: usize,
    connecting: bool,
    read_size: usize,
    /// Interest last passed to the registry, to skip redundant reregisters.
    registered: Option<Interest>,
    /// `shutdown` was called: no more writes, `close_notify` queued for TLS.
    closing: bool,
    /// The sending side of the socket has been shut down.
    write_shut: bool,
}

impl Conn {
    fn new(stream: TcpStream, tls: Option<rustls::Connection>, connecting: bool) -> Self {
        let tls = tls.map(|mut conn| {
            // Unbounded rustls buffers keep `write` infallible for flow
            // control; the caller observes backlog through `pending_bytes`.
            conn.set_buffer_limit(None);
            conn
        });
        Self {
            stream,
            tls,
            pending: Vec::new(),
            pending_pos: 0,
            connecting,
            read_size: DEFAULT_READ_SIZE,
            registered: None,
            closing: false,
            write_shut: false,
        }
    }

    /// Wraps an accepted plaintext stream and enables `TCP_NODELAY`.
    pub fn accept_plain(stream: TcpStream) -> io::Result<Self> {
        stream.set_nodelay(true)?;
        Ok(Self::new(stream, None, false))
    }

    /// Wraps an accepted stream as a TLS server connection and enables
    /// `TCP_NODELAY`. The handshake progresses through `read` and `flush`.
    pub fn accept_tls(stream: TcpStream, config: Arc<rustls::ServerConfig>) -> io::Result<Self> {
        stream.set_nodelay(true)?;
        let conn = rustls::ServerConnection::new(config).map_err(tls_error)?;
        Ok(Self::new(stream, Some(conn.into()), false))
    }

    /// Starts a nonblocking plaintext connect.
    ///
    /// Register the connection with [`interest`](Self::interest) (which
    /// includes `WRITABLE` while connecting) and call
    /// [`flush`](Self::flush) on the first writable event; `TCP_NODELAY` is
    /// set as soon as the connection is established. Data written earlier is
    /// buffered.
    pub fn connect_plain(addr: SocketAddr) -> io::Result<Self> {
        Ok(Self::new(TcpStream::connect(addr)?, None, true))
    }

    /// Starts a nonblocking TLS connect to `addr`, verifying the server as
    /// `server_name`. See [`connect_plain`](Self::connect_plain).
    pub fn connect_tls(
        addr: SocketAddr,
        config: Arc<rustls::ClientConfig>,
        server_name: ServerName<'static>,
    ) -> io::Result<Self> {
        let conn = rustls::ClientConnection::new(config, server_name).map_err(tls_error)?;
        Ok(Self::new(
            TcpStream::connect(addr)?,
            Some(conn.into()),
            true,
        ))
    }

    /// Sets how many bytes one receive asks the kernel for. Applies to
    /// plaintext and TLS connections alike (for TLS it bounds the ciphertext
    /// of one receive).
    pub fn set_read_size(&mut self, bytes: usize) {
        self.read_size = bytes.max(1);
    }

    /// The underlying socket.
    pub fn stream(&self) -> &TcpStream {
        &self.stream
    }

    /// The TLS session, if this is a TLS connection.
    pub fn tls(&self) -> Option<&rustls::Connection> {
        self.tls.as_ref()
    }

    /// Whether this is a TLS connection.
    pub fn is_tls(&self) -> bool {
        self.tls.is_some()
    }

    /// Whether the TCP connect or the TLS handshake is still in progress.
    pub fn is_handshaking(&self) -> bool {
        self.connecting || self.tls.as_ref().is_some_and(|t| t.is_handshaking())
    }

    /// Asks the kernel to timestamp received packets (`SO_TIMESTAMPNS`).
    ///
    /// Only effective on Linux; elsewhere it succeeds without effect and
    /// [`ReadBatch::kernel_rx_ns`] stays `None`.
    pub fn enable_rx_timestamps(&self) -> io::Result<()> {
        sys::enable_rx_timestamps(&self.stream)
    }

    /// Bytes accepted by [`write`](Self::write) but not yet handed to the
    /// kernel (ciphertext for TLS).
    pub fn pending_bytes(&self) -> usize {
        self.pending.len() - self.pending_pos
    }

    /// Whether the connection needs a writable event to make progress.
    pub fn wants_write(&self) -> bool {
        self.connecting
            || self.pending_bytes() > 0
            || self.tls.as_ref().is_some_and(|t| t.wants_write())
    }

    /// The interest set to register with: always readable, plus writable
    /// while connecting or while output is pending.
    pub fn interest(&self) -> Interest {
        if self.wants_write() {
            Interest::READABLE | Interest::WRITABLE
        } else {
            Interest::READABLE
        }
    }

    /// Registers the socket under `token` with the current
    /// [`interest`](Self::interest).
    pub fn register(&mut self, registry: &Registry, token: Token) -> io::Result<()> {
        let interest = self.interest();
        registry.register(&mut self.stream, token, interest)?;
        self.registered = Some(interest);
        Ok(())
    }

    /// Re-registers if [`interest`](Self::interest) changed since the last
    /// registration; a no-op (no syscall) otherwise. Call after handling
    /// every event for this connection.
    pub fn sync_interest(&mut self, registry: &Registry, token: Token) -> io::Result<()> {
        let interest = self.interest();
        if self.registered != Some(interest) {
            registry.reregister(&mut self.stream, token, interest)?;
            self.registered = Some(interest);
        }
        Ok(())
    }

    /// Removes the socket from the registry, e.g. before dropping or pooling
    /// it under another token.
    pub fn deregister(&mut self, registry: &Registry) -> io::Result<()> {
        registry.deregister(&mut self.stream)?;
        self.registered = None;
        Ok(())
    }

    /// Completes a pending nonblocking connect. Returns whether the
    /// connection is established.
    fn poll_connect(&mut self) -> io::Result<bool> {
        if !self.connecting {
            return Ok(true);
        }
        if let Some(err) = self.stream.take_error()? {
            return Err(err);
        }
        match self.stream.peer_addr() {
            Ok(_) => {
                self.connecting = false;
                self.stream.set_nodelay(true)?;
                Ok(true)
            }
            Err(err) if is_connect_in_progress(&err) => Ok(false),
            Err(err) => Err(err),
        }
    }

    /// Performs one socket receive and appends the resulting plaintext to
    /// `out`.
    ///
    /// Returns [`io::ErrorKind::WouldBlock`] when the socket has nothing more;
    /// with `mio`'s edge-triggered readiness, call this in a loop until then.
    /// For TLS, handshake responses produced by the receive are flushed on a
    /// best-effort basis; check [`interest`](Self::interest) afterwards.
    pub fn read(&mut self, out: &mut Vec<u8>) -> io::Result<ReadBatch> {
        if !self.poll_connect()? {
            return Err(io::ErrorKind::WouldBlock.into());
        }
        if self.tls.is_some() {
            self.read_tls(out)
        } else {
            let (n, kernel_rx_ns) = sys::recv_append(&self.stream, out, self.read_size)?;
            Ok(ReadBatch {
                plaintext_bytes: n,
                kernel_rx_ns,
                eof: n == 0,
            })
        }
    }

    fn read_tls(&mut self, out: &mut Vec<u8>) -> io::Result<ReadBatch> {
        let batch = TLS_RX.with_borrow_mut(|rx| {
            rx.clear();
            let (received, kernel_rx_ns) = sys::recv_append(&self.stream, rx, self.read_size)?;
            let tls = self
                .tls
                .as_mut()
                .expect("read_tls on a plaintext connection");
            let before = out.len();
            let mut eof = received == 0;
            // rustls takes at most 4 KiB per `read_tls`, so the receive is fed
            // in slices; an empty receive is passed once to report the FIN.
            let mut input: &[u8] = rx;
            loop {
                let fed = tls.read_tls(&mut input)?;
                let state = match tls.process_new_packets() {
                    Ok(state) => state,
                    Err(err) => {
                        // rustls queued an alert describing the failure; the
                        // connection is dead either way, so a failed send is
                        // moot.
                        let _ = tls.write_tls(&mut &self.stream);
                        return Err(tls_error(err));
                    }
                };
                eof |= state.peer_has_closed();
                eof |= drain_plaintext(tls, out)?;
                // `fed == 0` with input left means close_notify was received;
                // rustls ignores whatever follows it.
                if input.is_empty() || fed == 0 {
                    break;
                }
            }
            Ok(ReadBatch {
                plaintext_bytes: out.len() - before,
                kernel_rx_ns,
                eof,
            })
        })?;
        self.push_tls_output()?;
        Ok(batch)
    }

    /// Queues `data` for sending and writes as much as the socket accepts
    /// right away; the rest stays pending until [`flush`](Self::flush).
    ///
    /// Returns `Ok(true)` when all output, this call's included, has been
    /// handed to the kernel and `Ok(false)` when some of it is still pending
    /// (the socket would block or the connect is in progress).
    ///
    /// On a plaintext connection with nothing pending the data goes straight
    /// to the `write` syscall without being copied.
    ///
    /// # Errors
    ///
    /// [`io::ErrorKind::BrokenPipe`] after [`shutdown`](Self::shutdown), and
    /// any socket error.
    pub fn write(&mut self, data: &[u8]) -> io::Result<bool> {
        if self.closing {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "write after shutdown",
            ));
        }
        if let Some(tls) = &mut self.tls {
            tls.writer().write_all(data)?;
            self.push_tls_output()?;
            return Ok(!self.wants_write());
        }
        let mut rest = data;
        if !self.connecting && self.pending_pos == self.pending.len() {
            rest = write_until_block(&self.stream, rest)?;
        }
        if !rest.is_empty() {
            self.compact_pending();
            self.pending.extend_from_slice(rest);
        }
        Ok(!self.wants_write())
    }

    /// Writes pending output. Returns `Ok(true)` when everything has been
    /// handed to the kernel and `Ok(false)` when the socket would block (or
    /// the connect is still in progress); keep `WRITABLE` interest then.
    pub fn flush(&mut self) -> io::Result<bool> {
        if !self.poll_connect()? {
            return Ok(false);
        }
        if self.tls.is_some() {
            self.push_tls_output()?;
        }
        let rest = write_until_block(&self.stream, &self.pending[self.pending_pos..])?;
        self.pending_pos = self.pending.len() - rest.len();
        if rest.is_empty() {
            self.pending.clear();
            self.pending_pos = 0;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Moves ciphertext out of rustls: straight to the socket while nothing
    /// is pending and the socket accepts it, the remainder into `pending`
    /// (keeping byte order).
    fn push_tls_output(&mut self) -> io::Result<()> {
        let Some(tls) = self.tls.as_mut() else {
            return Ok(());
        };
        if !self.connecting && self.pending_pos == self.pending.len() {
            while tls.wants_write() {
                match tls.write_tls(&mut &self.stream) {
                    Ok(0) => return Err(io::ErrorKind::WriteZero.into()),
                    Ok(_) => {}
                    Err(err) if err.kind() == io::ErrorKind::WouldBlock => break,
                    Err(err) if err.kind() == io::ErrorKind::Interrupted => {}
                    Err(err) => return Err(err),
                }
            }
        }
        if tls.wants_write() {
            if self.pending_pos > 0 && self.pending_pos * 2 >= self.pending.len() {
                self.pending.drain(..self.pending_pos);
                self.pending_pos = 0;
            }
            while tls.wants_write() {
                tls.write_tls(&mut self.pending)?;
            }
        }
        Ok(())
    }

    fn compact_pending(&mut self) {
        if self.pending_pos > 0 && self.pending_pos * 2 >= self.pending.len() {
            self.pending.drain(..self.pending_pos);
            self.pending_pos = 0;
        }
    }

    /// Closes the sending side once all pending output has been delivered.
    ///
    /// The first call stops further [`write`](Self::write)s and, for TLS,
    /// queues a `close_notify` alert. Every call flushes; only when nothing is
    /// pending any more is the socket shut down for writing (TCP FIN) and
    /// `Ok(true)` returned. `Ok(false)` means output is still pending: keep
    /// `WRITABLE` interest (see [`interest`](Self::interest)) and call
    /// `shutdown` again on the next writable event. Calls after completion
    /// return `Ok(true)` without side effects.
    pub fn shutdown(&mut self) -> io::Result<bool> {
        if self.write_shut {
            return Ok(true);
        }
        if !self.closing {
            self.closing = true;
            if let Some(tls) = &mut self.tls {
                tls.send_close_notify();
            }
        }
        if !self.flush()? {
            return Ok(false);
        }
        match self.stream.shutdown(std::net::Shutdown::Write) {
            Err(err) if err.kind() == io::ErrorKind::NotConnected => {}
            other => other?,
        }
        self.write_shut = true;
        Ok(true)
    }
}

/// Writes until done or `WouldBlock`; returns the unwritten remainder.
fn write_until_block<'a>(stream: &TcpStream, mut data: &'a [u8]) -> io::Result<&'a [u8]> {
    let mut writer = stream;
    while !data.is_empty() {
        match writer.write(data) {
            Ok(0) => return Err(io::ErrorKind::WriteZero.into()),
            Ok(n) => data = &data[n..],
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => break,
            Err(err) if err.kind() == io::ErrorKind::Interrupted => {}
            Err(err) => return Err(err),
        }
    }
    Ok(data)
}

/// Moves all decrypted plaintext out of rustls into `out`. Returns whether
/// the peer has closed (TLS `close_notify` or a bare TCP FIN).
fn drain_plaintext(tls: &mut rustls::Connection, out: &mut Vec<u8>) -> io::Result<bool> {
    let mut reader = tls.reader();
    loop {
        let chunk = match reader.fill_buf() {
            Ok([]) => return Ok(true),
            Ok(chunk) => chunk,
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => return Ok(false),
            // TCP FIN without close_notify: many peers close this way.
            Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => return Ok(true),
            Err(err) => return Err(err),
        };
        let len = chunk.len();
        out.extend_from_slice(chunk);
        reader.consume(len);
    }
}

fn tls_error(err: rustls::Error) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, err)
}

fn is_connect_in_progress(err: &io::Error) -> bool {
    #[cfg(unix)]
    if err.raw_os_error() == Some(libc::EINPROGRESS) {
        return true;
    }
    err.kind() == io::ErrorKind::NotConnected
}

/// Options for [`bind_listener`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ListenOptions {
    /// Set `SO_REUSEPORT` so several shards can each bind their own listener
    /// to the same address. Linux only.
    pub reuse_port: bool,
    /// `listen(2)` backlog.
    pub backlog: i32,
}

impl Default for ListenOptions {
    fn default() -> Self {
        Self {
            reuse_port: false,
            backlog: 4096,
        }
    }
}

/// Binds a nonblocking listener with `SO_REUSEADDR` (Unix) and, if requested,
/// `SO_REUSEPORT` (Linux; `Unsupported` elsewhere).
pub fn bind_listener(addr: SocketAddr, options: ListenOptions) -> io::Result<TcpListener> {
    use socket2::{Domain, Protocol, Socket, Type};

    let socket = Socket::new(Domain::for_address(addr), Type::STREAM, Some(Protocol::TCP))?;
    // On Windows SO_REUSEADDR lets another socket steal a bound port, which is
    // not the Unix TIME_WAIT semantics wanted here.
    #[cfg(unix)]
    socket.set_reuse_address(true)?;
    if options.reuse_port {
        #[cfg(target_os = "linux")]
        socket.set_reuse_port(true)?;
        #[cfg(not(target_os = "linux"))]
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "SO_REUSEPORT sharding is only supported on Linux",
        ));
    }
    socket.set_nonblocking(true)?;
    socket.bind(&addr.into())?;
    socket.listen(options.backlog)?;
    Ok(TcpListener::from_std(socket.into()))
}

#[cfg(target_os = "linux")]
mod sys {
    use std::io;
    use std::mem::MaybeUninit;
    use std::os::fd::AsRawFd;

    use mio::net::TcpStream;

    use crate::clock::timespec_to_ns;

    /// Room for one `SCM_TIMESTAMPNS` message; u64 elements give the
    /// alignment `cmsghdr` needs.
    type CmsgBuf = [u64; 8];

    pub(super) fn enable_rx_timestamps(stream: &TcpStream) -> io::Result<()> {
        let on: libc::c_int = 1;
        // SAFETY: valid fd, option pointer and length describe a live c_int.
        let rc = unsafe {
            libc::setsockopt(
                stream.as_raw_fd(),
                libc::SOL_SOCKET,
                libc::SO_TIMESTAMPNS,
                (&raw const on).cast::<libc::c_void>(),
                libc::socklen_t::try_from(size_of::<libc::c_int>())
                    .expect("c_int size fits socklen_t"),
            )
        };
        if rc < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    /// One `recvmsg` into `len` bytes at `ptr`, extracting `SCM_TIMESTAMPNS`.
    ///
    /// # Safety
    ///
    /// `ptr` must be valid for writes of `len` bytes.
    unsafe fn recvmsg_raw(
        fd: libc::c_int,
        ptr: *mut u8,
        len: usize,
    ) -> io::Result<(usize, Option<u64>)> {
        let mut iov = libc::iovec {
            iov_base: ptr.cast::<libc::c_void>(),
            iov_len: len,
        };
        let mut cmsg_buf: CmsgBuf = [0; 8];
        // SAFETY: msghdr is plain old data; all-zero is a valid empty header.
        let mut msg: libc::msghdr = unsafe { MaybeUninit::zeroed().assume_init() };
        msg.msg_iov = &raw mut iov;
        msg.msg_iovlen = 1;
        msg.msg_control = cmsg_buf.as_mut_ptr().cast::<libc::c_void>();
        // glibc declares msg_controllen as size_t, musl as socklen_t.
        #[allow(
            clippy::cast_possible_truncation,
            reason = "a 64-byte control buffer fits either type"
        )]
        let controllen = size_of::<CmsgBuf>() as _;
        msg.msg_controllen = controllen;
        // SAFETY: `msg` points at a live iovec covering caller-guaranteed
        // writable memory and at a live, aligned control buffer.
        let n = unsafe { libc::recvmsg(fd, &raw mut msg, 0) };
        if n < 0 {
            return Err(io::Error::last_os_error());
        }
        let n = usize::try_from(n).expect("non-negative recvmsg result fits usize");

        let mut timestamp = None;
        // SAFETY: `msg` was filled in by the kernel; the CMSG_* helpers only
        // walk within msg_control/msg_controllen.
        let mut cmsg = unsafe { libc::CMSG_FIRSTHDR(&raw const msg) };
        while !cmsg.is_null() {
            // SAFETY: non-null pointers from CMSG_FIRSTHDR/NXTHDR point at a
            // complete header inside the control buffer.
            let header = unsafe { &*cmsg };
            if header.cmsg_level == libc::SOL_SOCKET && header.cmsg_type == libc::SCM_TIMESTAMPNS {
                // SAFETY: SCM_TIMESTAMPNS carries a struct timespec; the data
                // may be unaligned, hence read_unaligned.
                let ts = unsafe {
                    libc::CMSG_DATA(cmsg)
                        .cast::<libc::timespec>()
                        .read_unaligned()
                };
                timestamp = Some(timespec_to_ns(&ts));
            }
            // SAFETY: as above.
            cmsg = unsafe { libc::CMSG_NXTHDR(&raw const msg, cmsg) };
        }
        Ok((n, timestamp))
    }

    pub(super) fn recv_append(
        stream: &TcpStream,
        out: &mut Vec<u8>,
        read_size: usize,
    ) -> io::Result<(usize, Option<u64>)> {
        out.reserve(read_size);
        let fd = stream.as_raw_fd();
        let spare = out.spare_capacity_mut();
        let len = spare.len().min(read_size);
        let ptr = spare.as_mut_ptr().cast::<u8>();
        // SAFETY: `ptr..ptr+len` is the Vec's reserved spare capacity.
        let (n, ts) = stream.try_io(|| unsafe { recvmsg_raw(fd, ptr, len) })?;
        let new_len = out.len() + n;
        // SAFETY: the kernel initialised exactly `n <= len` bytes of the spare
        // capacity, so `new_len` bytes are initialised and within capacity.
        unsafe { out.set_len(new_len) };
        Ok((n, ts))
    }
}

#[cfg(not(target_os = "linux"))]
mod sys {
    use std::io::{self, Read as _};

    use mio::net::TcpStream;

    #[expect(
        clippy::unnecessary_wraps,
        reason = "mirrors the fallible Linux implementation"
    )]
    pub(super) fn enable_rx_timestamps(_stream: &TcpStream) -> io::Result<()> {
        Ok(())
    }

    pub(super) fn recv_append(
        stream: &TcpStream,
        out: &mut Vec<u8>,
        read_size: usize,
    ) -> io::Result<(usize, Option<u64>)> {
        let start = out.len();
        out.resize(start + read_size, 0);
        let mut reader = stream;
        let result = reader.read(&mut out[start..]);
        let n = result.as_ref().map_or(0, |&n| n);
        out.truncate(start + n);
        result.map(|n| (n, None))
    }
}

#[cfg(test)]
mod tests;
