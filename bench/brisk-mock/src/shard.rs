//! One shard: an OS thread with its own `mio::Poll`, listener, connections
//! and emission schedule.
//!
//! The loop follows the shape prescribed by
//! [`brisk_bench_core::precise`]: arm the deadline timer for the earliest
//! schedule entry, poll, handle socket events, and fire every entry that has
//! come within the spin window. Firing an entry builds the event bytes first,
//! then spins to the planned time, patches `t_write` and issues exactly one
//! write, so the gap between `t_sched` and `t_write` is only the spin exit.

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use brisk_bench_core::clock::{now_ns, realtime_ns};
use brisk_bench_core::precise::DeadlineTimer;
use brisk_bench_core::transport::Conn;
use brisk_bench_core::wire::{self, BenchParams, Marker};
use mio::net::{TcpListener, TcpStream};
use mio::{Events, Poll, Token};

use crate::request::{
    ChatRequest, Head, Inspector, MAX_BODY_BYTES, MAX_HEAD_BYTES, Method, Progress, RequestError,
    RequestReader, Route,
};
use crate::response::{self, StreamTemplate, Usage};
use crate::server::check_limits;
use crate::stats::{ErrorKind, ShardCounters, Stats};

const TIMER: Token = Token(0);
const LISTENER: Token = Token(1);
/// Token of the `mio::Waker` used to stop the shard.
pub(crate) const WAKER: Token = Token(2);
/// Connection slot `n` is registered under `Token(FIRST_CONN + n)`.
const FIRST_CONN: usize = 3;

/// Receives per readable event before the connection yields to the rest of
/// the loop. Together with [`READ_BUDGET_BYTES`] and the check for due
/// emissions after every receive, bounds how long one large upload can delay
/// scheduled writes.
const READ_BUDGET: usize = 16;
/// Plaintext bytes per readable event before the connection yields.
const READ_BUDGET_BYTES: usize = 256 * 1024;
/// Input buffered while a response is in flight, beyond which the client is
/// cut off: the request reader is not advanced then, so its limits do not
/// apply, and nothing else would stop the buffer from growing.
const MAX_BUFFERED_INPUT: usize = MAX_HEAD_BYTES + MAX_BODY_BYTES;
/// Delay before accepting again after `accept` failed, e.g. with `EMFILE`.
/// The listener is edge-triggered, so without a retry queued connections
/// would wait for the next new connection.
const ACCEPT_RETRY: Duration = Duration::from_millis(10);
/// Readiness events fetched per poll.
const EVENT_CAPACITY: usize = 1024;

/// Settings shared by every shard of a server.
#[derive(Debug)]
pub(crate) struct Settings {
    /// Parameters used when a request carries no directive.
    pub(crate) defaults: BenchParams,
    /// Model name reported in responses and by `/v1/models`.
    pub(crate) model: String,
    /// TLS configuration; plaintext when `None`.
    pub(crate) tls: Option<Arc<rustls::ServerConfig>>,
}

/// One planned emission. Ordered by `(t_sched, stream, seq)`; `slot` is
/// determined by `stream` and only locates the connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct Entry {
    t_sched: u64,
    stream: u64,
    seq: u32,
    slot: usize,
}

/// Monotonic time of chunk `seq` of a stream whose first chunk is due at
/// `t_first`.
fn chunk_time(t_first: u64, interval_us: u64, seq: u32) -> u64 {
    t_first.saturating_add(
        interval_us
            .saturating_mul(1_000)
            .saturating_mul(u64::from(seq)),
    )
}

#[derive(Debug)]
struct StreamState {
    template: StreamTemplate,
    params: BenchParams,
    t_first: u64,
    usage: Option<Usage>,
}

#[derive(Debug)]
struct CompletionState {
    params: BenchParams,
    created_unix_s: u64,
    usage: Usage,
}

#[derive(Debug)]
enum ResponseKind {
    Stream(StreamState),
    Completion(CompletionState),
}

/// The response a connection is currently producing. Requests pipelined
/// behind it stay buffered until it completes.
#[derive(Debug)]
struct Response {
    /// Shard-unique id; schedule entries carry it so an entry can never fire
    /// into a later response that reuses the connection slot.
    id: u64,
    keep_alive: bool,
    kind: ResponseKind,
}

#[derive(Debug)]
struct Connection {
    io: Conn,
    rx: Vec<u8>,
    reader: RequestReader,
    response: Option<Response>,
    /// A graceful close is in progress; input is ignored.
    closing: bool,
    /// The peer closed its sending side.
    eof: bool,
}

/// Writes `data` as one write call and counts it when the socket could not
/// take all of it.
fn send(io: &mut Conn, data: &[u8], counters: &ShardCounters) -> io::Result<()> {
    if !io.write(data)? {
        counters.write_blocked();
    }
    Ok(())
}

/// Whether an I/O error just means the peer went away.
fn is_disconnect(err: &io::Error) -> bool {
    matches!(
        err.kind(),
        io::ErrorKind::ConnectionReset
            | io::ErrorKind::ConnectionAborted
            | io::ErrorKind::BrokenPipe
            | io::ErrorKind::UnexpectedEof
    )
}

/// The event loop state of one shard.
#[derive(Debug)]
pub(crate) struct Shard {
    index: usize,
    poll: Poll,
    listener: TcpListener,
    timer: DeadlineTimer,
    schedule: BinaryHeap<Reverse<Entry>>,
    conns: Vec<Option<Connection>>,
    free: Vec<usize>,
    /// Connections whose read budget ran out while data may remain; with
    /// edge-triggered readiness no new event would arrive for them.
    read_again: Vec<usize>,
    /// When to retry `accept` after it failed.
    accept_retry_at: Option<u64>,
    /// A request body is being inspected. Nested inspections, reached through
    /// emissions fired between windows, do not fire again, which bounds the
    /// recursion.
    inspecting: bool,
    next_response_id: u64,
    /// Scratch buffer for building every write.
    out: Vec<u8>,
    settings: Arc<Settings>,
    stats: Arc<Stats>,
    stop: Arc<AtomicBool>,
}

impl Shard {
    /// Prepares a shard around a poll (with its waker already registered
    /// under [`WAKER`]) and a bound listener.
    pub(crate) fn new(
        index: usize,
        poll: Poll,
        mut listener: TcpListener,
        spin_window: Duration,
        settings: Arc<Settings>,
        stats: Arc<Stats>,
        stop: Arc<AtomicBool>,
    ) -> io::Result<Self> {
        let timer = DeadlineTimer::new(spin_window)?;
        timer.register(poll.registry(), TIMER)?;
        poll.registry()
            .register(&mut listener, LISTENER, mio::Interest::READABLE)?;
        Ok(Self {
            index,
            poll,
            listener,
            timer,
            schedule: BinaryHeap::new(),
            conns: Vec::new(),
            free: Vec::new(),
            read_again: Vec::new(),
            accept_retry_at: None,
            inspecting: false,
            next_response_id: 0,
            out: Vec::with_capacity(64 * 1024),
            settings,
            stats,
            stop,
        })
    }

    fn counters(&self) -> &ShardCounters {
        self.stats.shard(self.index)
    }

    /// Runs the event loop until the stop flag is raised.
    pub(crate) fn run(mut self) -> io::Result<()> {
        let mut events = Events::with_capacity(EVENT_CAPACITY);
        let mut again = Vec::new();
        while !self.stop.load(Ordering::Acquire) {
            let next = self.schedule.peek().map(|Reverse(e)| e.t_sched);
            match next {
                Some(deadline) => self.timer.arm(deadline)?,
                None => self.timer.disarm()?,
            }
            let now = now_ns();
            let mut timeout = if self.read_again.is_empty() {
                self.timer.poll_timeout(now, next)
            } else {
                Some(Duration::ZERO)
            };
            if let Some(at) = self.accept_retry_at {
                let retry = Duration::from_nanos(at.saturating_sub(now));
                timeout = Some(timeout.map_or(retry, |t| t.min(retry)));
            }
            match self.poll.poll(&mut events, timeout) {
                Ok(()) => {}
                Err(err) if err.kind() == io::ErrorKind::Interrupted => continue,
                Err(err) => return Err(err),
            }
            for event in &events {
                match event.token() {
                    TIMER => self.timer.acknowledge(),
                    WAKER => {}
                    LISTENER => self.accept_all(),
                    Token(token) => {
                        let slot = token - FIRST_CONN;
                        if event.is_writable() {
                            self.on_writable(slot);
                        }
                        if event.is_readable() || event.is_read_closed() || event.is_error() {
                            self.on_readable(slot);
                        }
                    }
                }
                self.fire_due();
            }
            if self.accept_retry_at.is_some_and(|at| now_ns() >= at) {
                self.accept_retry_at = None;
                self.accept_all();
            }
            std::mem::swap(&mut again, &mut self.read_again);
            for slot in again.drain(..) {
                self.on_readable(slot);
                self.fire_due();
            }
            self.fire_due();
        }
        Ok(())
    }

    /// Accepts until the backlog is empty, firing due emissions after every
    /// connection so a burst of connects does not stall running streams.
    fn accept_all(&mut self) {
        loop {
            match self.listener.accept() {
                Ok((stream, _)) => {
                    self.counters().accepted();
                    if let Err(err) = self.add_connection(stream) {
                        tracing::debug!(shard = self.index, %err, "setting up connection failed");
                        self.counters().error(ErrorKind::Io);
                        self.counters().connection_closed();
                    }
                    self.fire_due();
                }
                Err(err) if err.kind() == io::ErrorKind::WouldBlock => return,
                // The connection died in the backlog; the next one may be fine.
                Err(err)
                    if matches!(
                        err.kind(),
                        io::ErrorKind::Interrupted
                            | io::ErrorKind::ConnectionAborted
                            | io::ErrorKind::ConnectionReset
                    ) => {}
                Err(err) => {
                    // Typically EMFILE. Retrying at once would fail again.
                    tracing::warn!(shard = self.index, %err, "accept failed");
                    self.counters().error(ErrorKind::Accept);
                    let delay = u64::try_from(ACCEPT_RETRY.as_nanos()).expect("fits u64");
                    self.accept_retry_at = Some(now_ns().saturating_add(delay));
                    return;
                }
            }
        }
    }

    fn add_connection(&mut self, stream: TcpStream) -> io::Result<()> {
        let io = match &self.settings.tls {
            Some(config) => Conn::accept_tls(stream, Arc::clone(config))?,
            None => Conn::accept_plain(stream)?,
        };
        let mut conn = Connection {
            io,
            rx: Vec::new(),
            reader: RequestReader::new(),
            response: None,
            closing: false,
            eof: false,
        };
        let slot = self.free.pop().unwrap_or(self.conns.len());
        conn.io
            .register(self.poll.registry(), Token(FIRST_CONN + slot))?;
        if slot == self.conns.len() {
            self.conns.push(Some(conn));
        } else {
            self.conns[slot] = Some(conn);
        }
        Ok(())
    }

    fn conn_mut(&mut self, slot: usize) -> Option<&mut Connection> {
        self.conns.get_mut(slot).and_then(Option::as_mut)
    }

    /// Drops a connection and everything scheduled for it.
    fn close(&mut self, slot: usize, error: Option<ErrorKind>) {
        let Some(mut conn) = self.conns.get_mut(slot).and_then(Option::take) else {
            return;
        };
        // Dropping the socket removes it from the poll as well; a failed
        // deregistration leaves nothing behind.
        let _ = conn.io.deregister(self.poll.registry());
        let counters = self.stats.shard(self.index);
        if let Some(response) = conn.response.take() {
            self.schedule
                .retain(|Reverse(entry)| entry.stream != response.id);
            if matches!(response.kind, ResponseKind::Stream(_)) {
                counters.stream_finished();
            }
            counters.error(ErrorKind::Aborted);
        } else if let Some(kind) = error {
            counters.error(kind);
        }
        counters.connection_closed();
        self.free.push(slot);
    }

    /// Closes after a failed socket operation, classifying the error.
    fn fail(&mut self, slot: usize, err: &io::Error) {
        let kind = if err.kind() == io::ErrorKind::InvalidData {
            Some(ErrorKind::Tls)
        } else if is_disconnect(err) {
            None
        } else {
            Some(ErrorKind::Io)
        };
        tracing::debug!(shard = self.index, slot, %err, "connection failed");
        self.close(slot, kind);
    }

    /// Re-registers interest after output may have changed.
    fn sync(&mut self, slot: usize) {
        let token = Token(FIRST_CONN + slot);
        let Some(conn) = self.conns.get_mut(slot).and_then(Option::as_mut) else {
            return;
        };
        if let Err(err) = conn.io.sync_interest(self.poll.registry(), token) {
            self.fail(slot, &err);
        }
    }

    fn on_writable(&mut self, slot: usize) {
        let Some(conn) = self.conn_mut(slot) else {
            return;
        };
        let result = if conn.closing {
            conn.io.shutdown()
        } else {
            conn.io.flush()
        };
        let closing = conn.closing;
        match result {
            Ok(true) if closing => self.close(slot, None),
            Ok(_) => self.sync(slot),
            Err(err) => self.fail(slot, &err),
        }
    }

    /// Reads what the socket has, yielding to the rest of the loop when the
    /// budget runs out or an emission comes due.
    fn on_readable(&mut self, slot: usize) {
        let Some(conn) = self.conns.get_mut(slot).and_then(Option::as_mut) else {
            return;
        };
        let mut receives = READ_BUDGET;
        let mut bytes = READ_BUDGET_BYTES;
        let mut failure = None;
        let mut overflow = false;
        loop {
            match conn.io.read(&mut conn.rx) {
                Ok(batch) if batch.eof => {
                    conn.eof = true;
                    break;
                }
                Ok(batch) => {
                    if conn.response.is_some() && conn.rx.len() > MAX_BUFFERED_INPUT {
                        overflow = true;
                        break;
                    }
                    receives -= 1;
                    bytes = bytes.saturating_sub(batch.plaintext_bytes);
                    let due = self
                        .schedule
                        .peek()
                        .is_some_and(|Reverse(top)| self.timer.is_due(now_ns(), top.t_sched));
                    if receives == 0 || bytes == 0 || due {
                        self.read_again.push(slot);
                        break;
                    }
                }
                Err(err) if err.kind() == io::ErrorKind::WouldBlock => break,
                Err(err) if err.kind() == io::ErrorKind::Interrupted => {}
                Err(err) => {
                    failure = Some(err);
                    break;
                }
            }
        }
        if let Some(err) = failure {
            self.fail(slot, &err);
            return;
        }
        if overflow {
            tracing::debug!(shard = self.index, slot, "input overflow behind a response");
            self.counters().error(ErrorKind::BodyTooLarge);
            self.close(slot, None);
            return;
        }
        if conn.closing {
            conn.rx.clear();
        }
        self.process(slot);
    }

    /// Handles buffered requests until one needs a scheduled response or the
    /// input runs out, then applies a pending end of input.
    fn process(&mut self, slot: usize) {
        loop {
            let Some(conn) = self.conn_mut(slot) else {
                return;
            };
            if conn.response.is_some() || conn.closing {
                break;
            }
            match conn.reader.advance(&mut conn.rx) {
                Ok(Progress::NeedMore) => break,
                Ok(Progress::Continue) => {
                    let result = conn.io.write(response::CONTINUE).map(|_| ());
                    if let Err(err) = result {
                        self.fail(slot, &err);
                        return;
                    }
                }
                // t0 is when the request became complete, before any of the
                // work of answering it.
                Ok(Progress::Request(head)) => self.dispatch(slot, head, now_ns()),
                Err(err) => {
                    self.reject(slot, &err);
                    return;
                }
            }
        }
        let Some(conn) = self.conn_mut(slot) else {
            return;
        };
        if conn.eof {
            // The client stopped sending. Treating that as a disconnect is what
            // lets an abandoned stream be dropped promptly; clients of this
            // mock do not half-close while awaiting a response.
            let error = (!conn.reader.is_idle(&conn.rx)).then_some(ErrorKind::BadRequest);
            if conn.closing || conn.response.is_some() || error.is_none() {
                self.close(slot, None);
            } else {
                self.close(slot, error);
            }
            return;
        }
        self.sync(slot);
    }

    /// Answers an unreadable request and closes the connection.
    fn reject(&mut self, slot: usize, err: &RequestError) {
        let (status, reason, kind) = match err {
            RequestError::HeadTooLarge => (
                431,
                "Request Header Fields Too Large",
                ErrorKind::HeadTooLarge,
            ),
            RequestError::BodyTooLarge => (413, "Content Too Large", ErrorKind::BodyTooLarge),
            RequestError::Version(_) => (505, "HTTP Version Not Supported", ErrorKind::BadRequest),
            RequestError::Head(_) | RequestError::Framing(_) => {
                (400, "Bad Request", ErrorKind::BadRequest)
            }
        };
        self.counters().error(kind);
        self.out.clear();
        response::append_error(&mut self.out, status, reason, &err.to_string(), false);
        self.reply_and_continue(slot, false);
    }

    /// Writes the complete response in `self.out`; closes afterwards unless
    /// `keep_alive`.
    fn reply_and_continue(&mut self, slot: usize, keep_alive: bool) {
        let Some(conn) = self.conns.get_mut(slot).and_then(Option::as_mut) else {
            return;
        };
        if let Err(err) = send(&mut conn.io, &self.out, self.stats.shard(self.index)) {
            self.fail(slot, &err);
            return;
        }
        if !keep_alive {
            self.begin_close(slot);
        }
    }

    fn begin_close(&mut self, slot: usize) {
        let Some(conn) = self.conn_mut(slot) else {
            return;
        };
        conn.closing = true;
        conn.rx.clear();
        match conn.io.shutdown() {
            Ok(true) => self.close(slot, None),
            Ok(false) => self.sync(slot),
            Err(err) => self.fail(slot, &err),
        }
    }

    /// Answers a complete request that arrived at `t0`.
    fn dispatch(&mut self, slot: usize, head: Head, t0: u64) {
        let keep_alive = head.keep_alive;
        let bench_endpoint = matches!(head.route, Route::Stats | Route::Reset);
        if !bench_endpoint {
            self.counters().request();
        }
        let chat = if (head.route, head.method) == (Route::ChatCompletions, Method::Post) {
            let Some(chat) = self.inspect_body(slot) else {
                return;
            };
            Some(chat)
        } else {
            None
        };
        // Emissions fired during the inspection used the scratch buffer, so
        // it is only cleared now.
        self.out.clear();
        match (head.route, head.method) {
            (Route::ChatCompletions, Method::Post) => {
                match chat.expect("chat completions are inspected") {
                    Ok(chat) => {
                        self.start_response(slot, &chat, keep_alive, t0);
                        return;
                    }
                    Err(err) => {
                        self.counters().error(ErrorKind::BadDirective);
                        response::append_error(
                            &mut self.out,
                            400,
                            "Bad Request",
                            &format!("invalid bench directive: {err}"),
                            keep_alive,
                        );
                    }
                }
            }
            (Route::Models, Method::Get) => {
                let body = response::models_body(&self.settings.model);
                response::append_json(&mut self.out, 200, "OK", &body, keep_alive);
            }
            (Route::Stats, Method::Get) => {
                let body = serde_json::to_vec(&self.stats.snapshot())
                    .expect("serializing counters cannot fail");
                response::append_json(&mut self.out, 200, "OK", &body, keep_alive);
            }
            (Route::Reset, Method::Post) => {
                self.stats.reset();
                response::append_no_content(&mut self.out, keep_alive);
            }
            (Route::Unknown, _) => {
                self.counters().error(ErrorKind::NotFound);
                response::append_error(&mut self.out, 404, "Not Found", "unknown path", keep_alive);
            }
            _ => {
                self.counters().error(ErrorKind::MethodNotAllowed);
                response::append_error(
                    &mut self.out,
                    405,
                    "Method Not Allowed",
                    "method not allowed for this path",
                    keep_alive,
                );
            }
        }
        self.reply_and_continue(slot, keep_alive);
    }

    /// Inspects the chat request body of `slot`, then discards the body so it
    /// is not held for the length of the response.
    ///
    /// The body is scanned one window at a time, firing due emissions in
    /// between so a large body does not hold up other streams. Returns `None`
    /// only if the connection is gone, and otherwise the request or why its
    /// directive is unacceptable.
    fn inspect_body(&mut self, slot: usize) -> Option<Result<ChatRequest, String>> {
        let nested = std::mem::replace(&mut self.inspecting, true);
        let mut inspector = Inspector::new();
        let inspected = loop {
            // Only streams of other connections can fire here: this one has
            // no response, hence nothing scheduled.
            let conn = self.conns.get(slot).and_then(Option::as_ref)?;
            if let Some(result) = inspector.step(conn.reader.body(&conn.rx)) {
                break result;
            }
            if !nested {
                self.fire_due();
            }
        };
        self.inspecting = nested;
        let conn = self.conns.get_mut(slot).and_then(Option::as_mut)?;
        conn.reader.finish(&mut conn.rx);
        Some(inspected.map_err(|err| err.to_string()).and_then(|chat| {
            match chat.params.as_ref().map(check_limits) {
                Some(Err(err)) => Err(err.to_string()),
                _ => Ok(chat),
            }
        }))
    }

    /// Starts a chat completion for a request complete at `t0`; the first
    /// emission is due at t0 + ttft.
    fn start_response(&mut self, slot: usize, chat: &ChatRequest, keep_alive: bool, t0: u64) {
        let params = chat.params.unwrap_or(self.settings.defaults);
        let t_first = t0.saturating_add(params.ttft_us.saturating_mul(1_000));
        let created_unix_s = realtime_ns() / 1_000_000_000;
        let id = self.next_response_id;
        self.next_response_id += 1;

        let kind = if chat.stream {
            self.out.clear();
            response::append_stream_head(&mut self.out, keep_alive);
            let conn = self.conns[slot]
                .as_mut()
                .expect("start_response on a live connection");
            if let Err(err) = send(&mut conn.io, &self.out, self.stats.shard(self.index)) {
                self.fail(slot, &err);
                return;
            }
            self.counters().stream_started();
            ResponseKind::Stream(StreamState {
                template: StreamTemplate::new(params.sid, created_unix_s, &self.settings.model),
                params,
                t_first,
                usage: chat
                    .include_usage
                    .then(|| Usage::estimate(chat.body_len, u64::from(params.chunks))),
            })
        } else {
            ResponseKind::Completion(CompletionState {
                params,
                created_unix_s,
                usage: Usage::estimate(chat.body_len, u64::from(params.resp_bytes.div_ceil(4))),
            })
        };
        let conn = self.conns[slot]
            .as_mut()
            .expect("start_response on a live connection");
        conn.response = Some(Response {
            id,
            keep_alive,
            kind,
        });
        self.schedule.push(Reverse(Entry {
            t_sched: t_first,
            stream: id,
            seq: 0,
            slot,
        }));
    }

    /// Fires every schedule entry whose time is within the spin window.
    fn fire_due(&mut self) {
        while let Some(Reverse(top)) = self.schedule.peek() {
            if !self.timer.is_due(now_ns(), top.t_sched) {
                return;
            }
            let Some(Reverse(entry)) = self.schedule.pop() else {
                return;
            };
            self.fire(entry);
        }
    }

    fn fire(&mut self, entry: Entry) {
        let Self {
            conns,
            timer,
            out,
            schedule,
            stats,
            settings,
            index,
            ..
        } = self;
        let counters = stats.shard(*index);
        let Some(conn) = conns.get_mut(entry.slot).and_then(Option::as_mut) else {
            return;
        };
        let Some(response) = conn.response.as_ref().filter(|r| r.id == entry.stream) else {
            return;
        };
        out.clear();
        let finished = match &response.kind {
            ResponseKind::Stream(stream) if entry.seq < stream.params.chunks => {
                let marker = Marker {
                    sid: stream.params.sid,
                    seq: entry.seq,
                    t_sched: entry.t_sched,
                    t_write: 0,
                };
                let at =
                    stream
                        .template
                        .append_content_event(out, &marker, stream.params.chunk_bytes);
                timer.spin_until(entry.t_sched);
                wire::patch_t_write(&mut out[at..], now_ns());
                if let Err(err) = send(&mut conn.io, out, counters) {
                    self.fail(entry.slot, &err);
                    return;
                }
                counters.chunk();
                let next = entry.seq + 1;
                if next < stream.params.chunks {
                    schedule.push(Reverse(Entry {
                        t_sched: chunk_time(stream.t_first, stream.params.interval_us, next),
                        seq: next,
                        ..entry
                    }));
                    false
                } else {
                    // The tail carries no marker, so it follows the last
                    // content event immediately as its own write.
                    out.clear();
                    stream.template.append_tail(out, stream.usage);
                    let result = send(&mut conn.io, out, counters);
                    if let Err(err) = result {
                        self.fail(entry.slot, &err);
                        return;
                    }
                    true
                }
            }
            ResponseKind::Stream(stream) => {
                stream.template.append_tail(out, stream.usage);
                timer.spin_until(entry.t_sched);
                let result = send(&mut conn.io, out, counters);
                if let Err(err) = result {
                    self.fail(entry.slot, &err);
                    return;
                }
                true
            }
            ResponseKind::Completion(completion) => {
                let marker = Marker {
                    sid: completion.params.sid,
                    seq: 0,
                    t_sched: entry.t_sched,
                    t_write: 0,
                };
                let at = response::append_completion(
                    out,
                    &marker,
                    completion.params.resp_bytes,
                    completion.created_unix_s,
                    &settings.model,
                    completion.usage,
                    response.keep_alive,
                );
                timer.spin_until(entry.t_sched);
                if let Some(at) = at {
                    wire::patch_t_write(&mut out[at..], now_ns());
                }
                let result = send(&mut conn.io, out, counters);
                if let Err(err) = result {
                    self.fail(entry.slot, &err);
                    return;
                }
                true
            }
        };
        if finished {
            self.complete(entry.slot);
        } else {
            self.sync(entry.slot);
        }
    }

    /// Ends the current response and moves on to pipelined requests, or
    /// closes the connection if the request asked for it.
    fn complete(&mut self, slot: usize) {
        let Some(conn) = self.conn_mut(slot) else {
            return;
        };
        let response = conn.response.take().expect("complete with a response");
        if matches!(response.kind, ResponseKind::Stream(_)) {
            self.counters().stream_finished();
        }
        if response.keep_alive {
            self.process(slot);
        } else {
            self.begin_close(slot);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn entries_order_by_time_then_stream_then_seq() {
        let mut heap = BinaryHeap::new();
        let entry = |t_sched, stream, seq| Entry {
            t_sched,
            stream,
            seq,
            slot: 0,
        };
        heap.push(Reverse(entry(20, 1, 0)));
        heap.push(Reverse(entry(10, 2, 5)));
        heap.push(Reverse(entry(10, 2, 4)));
        heap.push(Reverse(entry(10, 1, 9)));
        let order: Vec<_> =
            std::iter::from_fn(|| heap.pop().map(|Reverse(e)| (e.t_sched, e.stream, e.seq)))
                .collect();
        assert_eq!(order, [(10, 1, 9), (10, 2, 4), (10, 2, 5), (20, 1, 0)]);
    }

    #[test]
    fn chunk_times_follow_the_interval_and_saturate() {
        assert_eq!(chunk_time(1_000, 5, 0), 1_000);
        assert_eq!(chunk_time(1_000, 5, 3), 16_000);
        assert_eq!(chunk_time(u64::MAX - 1, 5, 3), u64::MAX);
    }
}
