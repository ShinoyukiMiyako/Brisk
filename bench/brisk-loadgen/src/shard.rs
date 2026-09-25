//! One shard: an OS thread with its own `mio` event loop, open-loop schedule,
//! HTTP/1.1 keep-alive connection pool and interval recorder.
//!
//! Timing rules:
//!
//! - Every latency starts at the request's *scheduled* time, never at the
//!   time it was actually sent, so a slow server or a late load generator
//!   cannot hide latency (no coordinated omission).
//! - `EmitLag` is the time taken immediately before the first syscall that
//!   puts the request on its way (`write` on a pooled connection, `connect`
//!   on a new one) minus the scheduled time. Connection setup is therefore
//!   part of TTFT and request latency, not of `EmitLag`.
//! - `TtftReused` repeats `Ttft`, censored bounds included, for the requests
//!   whose attempt went out on a pooled connection. A request sent on a
//!   newly opened connection, a stale-connection retry included, carries the
//!   handshake in its TTFT; it stays in `Ttft` and the diagnostics report it
//!   apart.
//! - The receive time of a batch is the kernel receive timestamp of its
//!   `recvmsg` (Linux, converted to `CLOCK_MONOTONIC`), otherwise the clock
//!   right after the receive.
//!
//! A request whose pooled (previously used) connection turns out to be
//! closed or reset within [`STALE_WINDOW_NS`] of the send, before any
//! response byte arrived, lost the keep-alive race against the server's idle
//! close. It is retried once on a fresh connection with its original
//! scheduled time and counted as the error kind `stale_retry`. A later close
//! means the server worked on the request and failed it: a plain error.
//!
//! No latency is dropped because its request never completed. A request
//! abandoned at its timeout or still open when the run ends contributes the
//! elapsed time as a lower bound of every latency that was already due:
//! TTFT or request latency from the scheduled start and, once a marker has
//! fixed the mock's chunk schedule, the latency of each overdue chunk from
//! its planned time. These censored samples go to the interval of the
//! timeout, or to the last interval for the requests open at the end, and
//! are counted in [`Counters`].
//!
//! Sends that fall behind are fired at most [`FIRE_BURST_NS`] per turn of
//! the event loop, so a shard that cannot keep up still receives responses,
//! reports ramp steps and sees the stop flag. Once stopped, a shard sends
//! nothing more (planned sends are dropped, not sent late) and gives the
//! requests in flight [`DRAIN_NS`] to finish before it censors them.
//!
//! In ramp mode a shard reports each step no later than a fixed delay after
//! the step's planned end, whatever is still open then, and it stops on its
//! own and tells the coordinator when a send falls further behind its
//! schedule than the saturation limit: the load generator itself is then
//! the bottleneck, not the target (see [`RampSpec`]).

use std::collections::BTreeMap;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::Sender;
use std::time::Duration;

use brisk_bench_core::clock::{RealtimeOffset, now_ns};
use brisk_bench_core::cpu;
use brisk_bench_core::precise::{self, DeadlineTimer};
use brisk_bench_core::result::IntervalResult;
use brisk_bench_core::stats::{self, IntervalRecorder, Metric, StatsError};
use brisk_bench_core::transport::Conn;
use hdrhistogram::Histogram;
use mio::{Events, Poll, Token};
use rustls::pki_types::ServerName;
use serde::Serialize;

use crate::diagnostics::{self, Miss};
use crate::request::RequestTemplate;
use crate::response::{Completion, Expect, ResponseParser};
use crate::schedule::{
    Arrival, FixedArrivals, PlannedInterval, PlannedLoad, RateSegment, SEC_NS, StreamArrivals,
};

/// Token of the deadline timer; connection `i` uses `Token(i + 1)`.
const TIMER: Token = Token(0);
/// Period of concurrency sampling and live-status updates.
const SAMPLE_NS: u64 = 10_000_000;
/// Error kind of a retried stale keep-alive connection.
pub(crate) const STALE_RETRY: &str = "stale_retry";
/// A pooled connection that closes within this time of the send lost the
/// keep-alive race: the server's close crossed the request on the wire, so
/// it arrives within one round trip plus event-loop latency. That is tens of
/// microseconds on loopback and well below a millisecond on a LAN, while a
/// server that actually processed the request takes longer to fail it.
pub(crate) const STALE_WINDOW_NS: u64 = 5_000_000;
/// Longest a turn of the event loop spends firing overdue sends before it
/// polls again. A shard that has fallen behind its schedule would otherwise
/// fire its backlog without end: no response would be read, so every send
/// would open a new connection and fall further behind, and neither step
/// reports nor the stop flag would be looked at. An on-time loop fires one
/// send, or a few after a late wakeup, per turn and never gets near it.
pub(crate) const FIRE_BURST_NS: u64 = 1_000_000;
/// After a stop, the requests in flight get this long to finish; the rest
/// are censored. Long enough for any response the target is still
/// producing at the rates of a benchmark, short enough that a stopped run
/// ends within seconds.
pub(crate) const DRAIN_NS: u64 = 1_000_000_000;

/// Where a shard's requests come from.
#[derive(Debug)]
pub(crate) enum Schedule {
    /// S1 streams.
    Stream(StreamArrivals),
    /// Evenly spaced requests.
    Fixed(FixedArrivals),
}

impl Schedule {
    fn next_arrival(&mut self) -> Option<Arrival> {
        match self {
            Self::Stream(s) => s.next_arrival(),
            Self::Fixed(f) => f.next_arrival(),
        }
    }
}

/// What a response is measured as, and the mock timing the requests ask
/// for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Mode {
    /// SSE stream: TTFT and per-chunk metrics from markers.
    Stream {
        /// Mock delay before the first chunk.
        ttft_ns: u64,
        /// Mock delay between chunks.
        interval_ns: u64,
    },
    /// Whole response: `RequestLatency` at the end of the body, and
    /// `MockWriteLag` and `ChunkWire` from the marker in the content.
    Whole {
        /// Mock delay before the response.
        ttft_ns: u64,
        /// The content is long enough for the mock to embed a marker, so
        /// every successful response must carry one.
        marker: bool,
    },
}

/// Opens connections to the target.
#[derive(Debug, Clone)]
pub(crate) struct Connector {
    /// Target address.
    pub(crate) addr: SocketAddr,
    /// TLS configuration and server name for `https` targets.
    pub(crate) tls: Option<(Arc<rustls::ClientConfig>, ServerName<'static>)>,
}

impl Connector {
    fn connect(&self) -> io::Result<Conn> {
        match &self.tls {
            None => Conn::connect_plain(self.addr),
            Some((config, name)) => Conn::connect_tls(self.addr, config.clone(), name.clone()),
        }
    }
}

/// Counters a shard publishes for the live status line and the stop flag
/// the coordinator sets.
#[derive(Debug, Default)]
pub(crate) struct Live {
    /// Requests scheduled and not yet finished.
    pub(crate) open: AtomicU64,
    /// Streams the schedule has open (S1).
    pub(crate) planned_open: AtomicU64,
    /// Chunks received so far.
    pub(crate) chunks: AtomicU64,
    /// Requests completed so far.
    pub(crate) requests: AtomicU64,
    /// Errors so far, retries included.
    pub(crate) errors: AtomicU64,
    /// Ends the run early: set by the coordinator, or by a ramp shard that
    /// found itself saturated.
    pub(crate) stop: AtomicBool,
}

/// Ramp-mode configuration of a shard.
#[derive(Debug)]
pub(crate) struct RampSpec {
    /// The evaluated steps (step number >= 1) in order.
    pub(crate) steps: Vec<RateSegment>,
    /// Every step is reported this long after its planned end at the
    /// latest, earlier once all its sends are made and done. What is still
    /// open then, requests in flight and planned sends not made yet, is
    /// censored: each counts as a sample of the time since its scheduled
    /// start, which this delay makes exceed the p99 limit.
    pub(crate) judge_delay_ns: u64,
    /// A send the event loop reaches this long after its scheduled time
    /// means the shard cannot keep up with the schedule: it stops and
    /// reports [`RampMessage::Saturated`].
    pub(crate) saturation_lag_ns: u64,
    /// The p99 limit: a send reached later than this is counted in
    /// [`StepReport::late_sends`], its request over the limit on the load
    /// generator's lag alone.
    pub(crate) stop_p99_ns: u64,
    /// Where step reports and saturation go.
    pub(crate) reports: Sender<RampMessage>,
}

/// What a ramp shard tells the coordinator.
#[derive(Debug)]
pub(crate) enum RampMessage {
    /// This shard's view of one step.
    Step(StepReport),
    /// The shard reached a send `lag_ns` after its scheduled time, beyond
    /// the saturation limit, and stopped. Its reports of the steps it had
    /// started follow once its requests in flight are settled.
    Saturated {
        /// Shard number.
        shard: usize,
        /// Step of the late send; 0 for the warmup.
        step: u32,
        /// How late the send was, nanoseconds.
        lag_ns: u64,
    },
}

/// One shard's view of one ramp step.
#[derive(Debug)]
pub(crate) struct StepReport {
    /// Step number.
    pub(crate) step: u32,
    /// Request latencies from the scheduled time; requests still open and
    /// planned sends not made at report time contribute a lower bound.
    pub(crate) latency: Histogram<u64>,
    /// Requests completed successfully.
    pub(crate) requests: u64,
    /// Requests failed.
    pub(crate) errors: u64,
    /// Requests still open, plus the planned sends not made, when the step
    /// was reported.
    pub(crate) censored: u64,
    /// Planned sends of the step not made when it was reported, because
    /// the shard was behind its schedule or stopped. Part of `censored`;
    /// they are dropped, never sent.
    pub(crate) unsent: u64,
    /// Largest lag of a send of the step behind its scheduled time, taken
    /// when the event loop reached the send.
    pub(crate) max_emit_lag_ns: u64,
    /// Sends of the step made more than the p99 limit after their
    /// scheduled time.
    pub(crate) late_sends: u64,
}

/// Everything a shard needs.
#[derive(Debug)]
pub(crate) struct ShardSpec {
    /// Shard number.
    pub(crate) index: usize,
    /// Common interval origin of all shards.
    pub(crate) origin_ns: u64,
    /// Scheduled end of the run.
    pub(crate) end_ns: u64,
    /// CPUs to pin the thread to.
    pub(crate) cpus: Option<Vec<usize>>,
    /// Busy-wait window before each send.
    pub(crate) spin_window: Duration,
    /// Connection factory.
    pub(crate) connector: Connector,
    /// Request bytes.
    pub(crate) template: RequestTemplate,
    /// Arrivals.
    pub(crate) schedule: Schedule,
    /// Response measurement.
    pub(crate) mode: Mode,
    /// Offered-load tracking (S1): `(load, ttft_ns, chunk_interval_ns)`.
    pub(crate) planned: Option<(PlannedLoad, u64, u64)>,
    /// Requests older than this are abandoned.
    pub(crate) request_timeout_ns: u64,
    /// Shared live counters.
    pub(crate) live: Arc<Live>,
    /// Ramp mode.
    pub(crate) ramp: Option<RampSpec>,
}

/// Transport-level counters.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub(crate) struct Counters {
    /// Requests started by the schedule.
    pub(crate) requests_scheduled: u64,
    /// Connections opened.
    pub(crate) connections_opened: u64,
    /// Requests sent on a pooled connection.
    pub(crate) reused_sends: u64,
    /// Stale pooled connections retried.
    pub(crate) stale_retries: u64,
    /// Idle pooled connections closed by the peer.
    pub(crate) idle_closed: u64,
    /// Receives that returned data.
    pub(crate) rx_batches: u64,
    /// Receives with data but without a kernel timestamp.
    pub(crate) rx_batches_without_timestamp: u64,
    /// Requests still open when the run ended (not errors).
    pub(crate) open_at_end: u64,
    /// Timed-out or open-at-end requests that contributed censored samples.
    pub(crate) censored_requests: u64,
    /// Censored latency samples (lower bounds) recorded, one per metric: the
    /// TTFT bound of a request on a pooled connection counts under `Ttft`
    /// and `TtftReused`.
    pub(crate) censored_samples: u64,
}

impl Counters {
    /// Adds another shard's counters.
    pub(crate) fn add(&mut self, other: &Self) {
        self.requests_scheduled += other.requests_scheduled;
        self.connections_opened += other.connections_opened;
        self.reused_sends += other.reused_sends;
        self.stale_retries += other.stale_retries;
        self.idle_closed += other.idle_closed;
        self.rx_batches += other.rx_batches;
        self.rx_batches_without_timestamp += other.rx_batches_without_timestamp;
        self.open_at_end += other.open_at_end;
        self.censored_requests += other.censored_requests;
        self.censored_samples += other.censored_samples;
    }
}

/// What a shard hands back when it ends.
#[derive(Debug)]
pub(crate) struct ShardOutput {
    /// Recorded intervals.
    pub(crate) intervals: Vec<IntervalResult>,
    /// Offered load per interval (S1).
    pub(crate) planned: Option<Vec<PlannedInterval>>,
    /// Transport counters.
    pub(crate) counters: Counters,
    /// Clock steps detected by the realtime offset.
    pub(crate) clock_steps: u64,
    /// Deadline misses, fresh connections and request slip per interval.
    pub(crate) diagnostics: diagnostics::Intervals,
    /// When the shard stopped.
    pub(crate) end_ns: u64,
}

/// Runs a shard on the calling thread until its end time or a stop request.
pub(crate) fn run(spec: ShardSpec) -> io::Result<ShardOutput> {
    if let Some(cpus) = &spec.cpus {
        cpu::pin_current_thread(cpus)?;
    }
    precise::set_min_timer_slack()?;
    let mut shard = Shard::new(spec)?;
    shard.run_loop()?;
    shard.finish()
}

#[derive(Debug, Clone, Copy)]
struct Request {
    sched_ns: u64,
    /// When the current attempt went out (`write` or `connect`).
    sent_ns: u64,
    sid: u64,
    chunks: u32,
    step: u32,
    retried: bool,
    /// The current attempt went out on a newly opened connection, so its
    /// latencies include the handshake.
    fresh: bool,
    /// Markers received so far.
    markers: u32,
    /// `t_sched` of the last marker received, on the mock's clock.
    last_marker_sched: u64,
}

/// Lower bounds of latencies whose request never completed, kept apart from
/// the recorder so that the samples of requests open at the end can still
/// join the last interval after the recorder has moved past it.
#[derive(Debug, Default)]
struct Censored {
    histograms: BTreeMap<(u64, Metric), Histogram<u64>>,
    saturated: BTreeMap<u64, u64>,
}

impl Censored {
    /// Records `count` samples of `value_ns` for interval `index`.
    fn record(&mut self, index: u64, metric: Metric, value_ns: u64, count: u64) {
        self.histograms
            .entry((index, metric))
            .or_insert_with(stats::new_histogram)
            .saturating_record_n(value_ns, count);
        if value_ns > stats::HISTOGRAM_HIGH_NS {
            *self.saturated.entry(index).or_default() += count;
        }
    }

    /// Adds the samples to the intervals of one shard, which the recorder
    /// numbers contiguously from 0. Samples of an index past the last
    /// interval join the last one. Without any interval the run was stopped
    /// before its origin and is invalid anyway.
    fn merge_into(self, intervals: &mut [IntervalResult]) -> Result<(), StatsError> {
        let Some(last) = intervals.last().map(|i| i.index) else {
            return Ok(());
        };
        let position = |intervals: &[IntervalResult], index: u64| {
            intervals
                .binary_search_by_key(&index.min(last), |i| i.index)
                .expect("the recorder closes every interval from 0 to the last")
        };
        for ((index, metric), histogram) in self.histograms {
            let at = position(intervals, index);
            let interval = &mut intervals[at];
            let merged = match interval.histograms.get(&metric) {
                Some(encoded) => {
                    let mut merged = stats::decode_histogram(encoded)?;
                    merged.add(&histogram).map_err(StatsError::Add)?;
                    merged
                }
                None => histogram,
            };
            interval
                .histograms
                .insert(metric, stats::encode_histogram(&merged));
        }
        for (index, n) in self.saturated {
            let at = position(intervals, index);
            intervals[at].saturated += n;
        }
        Ok(())
    }
}

#[derive(Debug)]
struct Connection {
    conn: Conn,
    /// Responses completed on this connection.
    served: u32,
    request: Option<Request>,
    parser: ResponseParser,
}

/// Connections indexed by token.
#[derive(Debug, Default)]
struct Slab {
    entries: Vec<Option<Connection>>,
    free: Vec<usize>,
}

impl Slab {
    fn insert(&mut self, conn: Connection) -> usize {
        if let Some(idx) = self.free.pop() {
            self.entries[idx] = Some(conn);
            idx
        } else {
            self.entries.push(Some(conn));
            self.entries.len() - 1
        }
    }

    fn get_mut(&mut self, idx: usize) -> Option<&mut Connection> {
        self.entries.get_mut(idx).and_then(Option::as_mut)
    }

    fn remove(&mut self, idx: usize) -> Option<Connection> {
        let conn = self.entries.get_mut(idx)?.take()?;
        self.free.push(idx);
        Some(conn)
    }

    fn iter(&self) -> impl Iterator<Item = (usize, &Connection)> {
        self.entries
            .iter()
            .enumerate()
            .filter_map(|(idx, c)| c.as_ref().map(|c| (idx, c)))
    }
}

#[derive(Debug)]
struct StepAcc {
    latency: Histogram<u64>,
    outstanding: u64,
    requests: u64,
    errors: u64,
    max_emit_lag_ns: u64,
    late_sends: u64,
}

impl StepAcc {
    fn new() -> Self {
        Self {
            latency: stats::new_histogram(),
            outstanding: 0,
            requests: 0,
            errors: 0,
            max_emit_lag_ns: 0,
            late_sends: 0,
        }
    }
}

#[derive(Debug)]
struct RampTracker {
    spec: RampSpec,
    next_report: usize,
    steps: BTreeMap<u32, StepAcc>,
}

impl RampTracker {
    /// Notes a send of `step` reached `lag_ns` after its scheduled time.
    fn note_lag(&mut self, step: u32, lag_ns: u64) -> Option<&mut StepAcc> {
        (step > 0).then(|| {
            let acc = self.steps.entry(step).or_insert_with(StepAcc::new);
            acc.max_emit_lag_ns = acc.max_emit_lag_ns.max(lag_ns);
            acc
        })
    }

    fn on_start(&mut self, step: u32, lag_ns: u64) {
        let late = lag_ns > self.spec.stop_p99_ns;
        if let Some(acc) = self.note_lag(step, lag_ns) {
            acc.outstanding += 1;
            acc.late_sends += u64::from(late);
        }
    }

    /// The deadline of the next step to report.
    fn next_deadline(&self) -> Option<u64> {
        self.spec
            .steps
            .get(self.next_report)
            .map(|seg| seg.end_ns.saturating_add(self.spec.judge_delay_ns))
    }

    /// `latency` is `None` for a failed request.
    fn on_done(&mut self, step: u32, latency: Option<u64>) {
        // Steps already reported counted their open requests as censored.
        if let Some(acc) = self.steps.get_mut(&step) {
            acc.outstanding -= 1;
            match latency {
                Some(ns) => {
                    acc.latency.saturating_record(ns);
                    acc.requests += 1;
                }
                None => acc.errors += 1,
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Retry {
    /// A stale pooled connection may be retried.
    Allowed,
    /// The failure is final (timeouts).
    Never,
}

struct Shard {
    index: usize,
    poll: Poll,
    timer: DeadlineTimer,
    offset: RealtimeOffset,
    recorder: IntervalRecorder,
    planned: Option<(PlannedLoad, u64, u64)>,
    slab: Slab,
    idle: Vec<usize>,
    template: RequestTemplate,
    connector: Connector,
    schedule: Schedule,
    next: Option<Arrival>,
    mode: Mode,
    origin_ns: u64,
    end_ns: u64,
    request_timeout_ns: u64,
    sid_base: u64,
    started: u64,
    open: u64,
    chunks_total: u64,
    requests_total: u64,
    errors_total: u64,
    rx: Vec<u8>,
    counters: Counters,
    censored: Censored,
    diagnostics: diagnostics::Recorder,
    /// When the event loop last started waiting.
    wait_started_ns: u64,
    /// When the event loop last returned from its wait.
    woke_ns: u64,
    live: Arc<Live>,
    ramp: Option<RampTracker>,
}

impl Shard {
    fn new(spec: ShardSpec) -> io::Result<Self> {
        let poll = Poll::new()?;
        let timer = DeadlineTimer::new(spec.spin_window)?;
        timer.register(poll.registry(), TIMER)?;
        let mut schedule = spec.schedule;
        let next = schedule.next_arrival();
        Ok(Self {
            index: spec.index,
            poll,
            timer,
            offset: RealtimeOffset::new(),
            recorder: IntervalRecorder::new(spec.origin_ns),
            planned: spec.planned,
            slab: Slab::default(),
            idle: Vec::new(),
            template: spec.template,
            connector: spec.connector,
            schedule,
            next,
            mode: spec.mode,
            origin_ns: spec.origin_ns,
            end_ns: spec.end_ns,
            request_timeout_ns: spec.request_timeout_ns,
            // Stream ids stay unique across shards.
            sid_base: (spec.index as u64) << 48,
            started: 0,
            open: 0,
            chunks_total: 0,
            requests_total: 0,
            errors_total: 0,
            rx: Vec::with_capacity(brisk_bench_core::transport::DEFAULT_READ_SIZE),
            counters: Counters::default(),
            censored: Censored::default(),
            diagnostics: diagnostics::Recorder::new(spec.origin_ns),
            wait_started_ns: 0,
            woke_ns: 0,
            live: spec.live,
            ramp: spec.ramp.map(|spec| RampTracker {
                spec,
                next_report: 0,
                steps: BTreeMap::new(),
            }),
        })
    }

    fn run_loop(&mut self) -> io::Result<()> {
        let mut events = Events::with_capacity(1024);
        let mut next_sample = self.origin_ns;
        let mut next_second = self.origin_ns + SEC_NS;
        // Once the stop flag is seen: when, and when the drain ends.
        let mut stopped: Option<(u64, u64)> = None;
        loop {
            let now = now_ns();
            if stopped.is_none() && self.live.stop.load(Ordering::Relaxed) {
                // The drain never outlasts the planned end, where the
                // requests still open are censored anyway.
                let drain_end = now.saturating_add(DRAIN_NS).min(self.end_ns.max(now));
                stopped = Some((now, drain_end));
            }
            let wake_by = if let Some((stop_ns, drain_end)) = stopped {
                if self.open == 0 || now >= drain_end {
                    self.end_ns = self.end_ns.min(now);
                    return self.report_stopped_steps(stop_ns, now);
                }
                drain_end
            } else {
                self.report_ramp_steps(now)?;
                if now >= self.end_ns {
                    return Ok(());
                }
                let report_by = self.ramp.as_ref().and_then(RampTracker::next_deadline);
                report_by.map_or(self.end_ns, |t| t.min(self.end_ns))
            };
            // A stopped shard sends nothing more.
            let precise = if stopped.is_none() {
                self.next.map(|a| a.sched_ns)
            } else {
                None
            };
            match precise {
                Some(deadline) => self.timer.arm(deadline)?,
                None => self.timer.disarm()?,
            }
            let coarse = next_sample.min(next_second).min(wake_by);
            let coarse_wait = Duration::from_nanos(coarse.saturating_sub(now));
            let timeout = self
                .timer
                .poll_timeout(now, precise)
                .map_or(coarse_wait, |t| t.min(coarse_wait));
            self.wait_started_ns = now_ns();
            let polled = self.poll.poll(&mut events, Some(timeout));
            self.woke_ns = now_ns();
            match polled {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => return Err(e),
            }
            for event in &events {
                if event.token() == TIMER {
                    self.timer.acknowledge();
                    continue;
                }
                let readable = event.is_readable() || event.is_read_closed() || event.is_error();
                self.on_event(event.token().0 - 1, readable, event.is_writable())?;
                // A long batch of events must not hold up a send: check the
                // schedule after every event, not only after the batch.
                self.fire_if_due()?;
            }
            self.fire_if_due()?;
            let now = now_ns();
            if now >= next_sample {
                self.sample(now);
                next_sample = advance(next_sample, SAMPLE_NS, now);
            }
            if now >= next_second {
                self.every_second(now)?;
                next_second = advance(next_second, SEC_NS, now);
            }
        }
    }

    /// Spins to the next scheduled send and fires every due arrival once
    /// the next one is within the spin window. A stopped shard fires
    /// nothing.
    fn fire_if_due(&mut self) -> io::Result<()> {
        if self.live.stop.load(Ordering::Relaxed) {
            return Ok(());
        }
        let reached = now_ns();
        if let Some(arrival) = self.next
            && self.timer.is_due(reached, arrival.sched_ns)
        {
            self.timer.spin_until(arrival.sched_ns);
            self.fire_due(reached)?;
        }
        Ok(())
    }

    /// Fires every due arrival, for at most [`FIRE_BURST_NS`] since the
    /// loop last woke; `reached` is when the loop found the first one due.
    /// In ramp mode a send reached later than the saturation limit stops
    /// the shard instead.
    fn fire_due(&mut self, reached: u64) -> io::Result<()> {
        let burst_end = self.woke_ns.saturating_add(FIRE_BURST_NS);
        while let Some(arrival) = self.next {
            let now = now_ns();
            if arrival.sched_ns > now || now >= burst_end {
                break;
            }
            let lag_ns = now - arrival.sched_ns;
            if let Some(ramp) = &mut self.ramp
                && lag_ns > ramp.spec.saturation_lag_ns
            {
                return self.saturated(arrival.step, lag_ns);
            }
            if arrival.sched_ns < reached {
                self.deadline_missed(arrival.sched_ns, reached);
            }
            self.fire(arrival, lag_ns)?;
            self.next = self.schedule.next_arrival();
        }
        Ok(())
    }

    /// Stops the shard because a send of `step` was reached `lag_ns` late,
    /// and tells the coordinator. The send is not made; the step's report
    /// counts it as unsent.
    fn saturated(&mut self, step: u32, lag_ns: u64) -> io::Result<()> {
        self.live.stop.store(true, Ordering::Relaxed);
        let ramp = self.ramp.as_mut().expect("only ramp shards saturate");
        ramp.note_lag(step, lag_ns);
        ramp.spec
            .reports
            .send(RampMessage::Saturated {
                shard: self.index,
                step,
                lag_ns,
            })
            .map_err(|_| io::Error::other("ramp coordinator is gone"))
    }

    /// Classifies a send whose deadline `sched_ns` passed before the loop
    /// reached it at `reached`: a deadline between the start and the end of
    /// the last wait means the wake-up came late, anything else that the
    /// loop was busy.
    fn deadline_missed(&mut self, sched_ns: u64, reached: u64) {
        let miss = if (self.wait_started_ns..self.woke_ns).contains(&sched_ns) {
            Miss::LateWakeup
        } else {
            Miss::BusyLoop
        };
        self.diagnostics
            .deadline_missed(reached, reached - sched_ns, miss);
    }

    /// Starts `arrival`, reached `lag_ns` after its scheduled time.
    fn fire(&mut self, arrival: Arrival, lag_ns: u64) -> io::Result<()> {
        let request = Request {
            sched_ns: arrival.sched_ns,
            sent_ns: arrival.sched_ns,
            sid: self.sid_base | self.started,
            chunks: arrival.chunks,
            step: arrival.step,
            retried: false,
            fresh: false,
            markers: 0,
            last_marker_sched: 0,
        };
        self.started += 1;
        self.open += 1;
        self.counters.requests_scheduled += 1;
        // Before dispatching: a synchronous failure reports the step as done.
        if let Some(ramp) = &mut self.ramp {
            ramp.on_start(arrival.step, lag_ns);
        }
        self.dispatch(request)?;
        if let Some((planned, ttft_ns, interval_ns)) = &mut self.planned {
            planned.start(&arrival, arrival.sched_ns + *ttft_ns, *interval_ns);
        }
        Ok(())
    }

    fn expect(&self, request: &Request) -> Expect {
        match self.mode {
            Mode::Stream { .. } => Expect::Stream {
                sid: request.sid,
                chunks: request.chunks,
            },
            Mode::Whole { marker, .. } => Expect::Whole {
                sid: request.sid,
                marker,
            },
        }
    }

    /// Sends a new request on the most recently used idle connection, or on
    /// a new one.
    fn dispatch(&mut self, mut request: Request) -> io::Result<()> {
        let Some(idx) = self.idle.pop() else {
            return self.connect_and_send(request, true);
        };
        let expect = self.expect(&request);
        let bytes = self.template.render(request.sid, request.chunks);
        let conn = self
            .slab
            .get_mut(idx)
            .expect("idle list only holds live connections");
        let t_emit = now_ns();
        let written = conn.conn.write(bytes);
        self.recorder
            .record_span(Metric::EmitLag, t_emit, request.sched_ns, t_emit);
        request.sent_ns = t_emit;
        request.fresh = false;
        if written.is_err() {
            // Nothing was in flight: the peer dropped the idle connection.
            self.close(idx);
            self.counters.stale_retries += 1;
            self.record_error(t_emit, STALE_RETRY);
            request.retried = true;
            return self.connect_and_send(request, false);
        }
        self.counters.reused_sends += 1;
        conn.parser.reset(expect);
        conn.request = Some(request);
        if let Err(e) = conn.conn.sync_interest(self.poll.registry(), token(idx)) {
            return self.fail_conn(idx, &e, Retry::Allowed);
        }
        Ok(())
    }

    /// Opens a connection and queues the request on it; the bytes go out
    /// once the connection is established.
    fn connect_and_send(&mut self, mut request: Request, record_emit: bool) -> io::Result<()> {
        let t_emit = now_ns();
        let connected = self.connector.connect();
        if record_emit {
            self.recorder
                .record_span(Metric::EmitLag, t_emit, request.sched_ns, t_emit);
        }
        request.sent_ns = t_emit;
        request.fresh = true;
        let Ok(mut conn) = connected else {
            self.record_error(t_emit, "connect");
            self.finish_request(request, None);
            return Ok(());
        };
        self.counters.connections_opened += 1;
        self.diagnostics.fresh_send(t_emit);
        conn.enable_rx_timestamps()?;
        if conn
            .write(self.template.render(request.sid, request.chunks))
            .is_err()
        {
            self.record_error(t_emit, "connect");
            self.finish_request(request, None);
            return Ok(());
        }
        let expect = self.expect(&request);
        let idx = self.slab.insert(Connection {
            conn,
            served: 0,
            request: Some(request),
            parser: ResponseParser::new(expect),
        });
        let entry = self.slab.get_mut(idx).expect("just inserted");
        entry.conn.register(self.poll.registry(), token(idx))
    }

    fn on_event(&mut self, idx: usize, readable: bool, writable: bool) -> io::Result<()> {
        let Some(entry) = self.slab.get_mut(idx) else {
            // Closed earlier in this batch of events.
            return Ok(());
        };
        if writable && let Err(e) = entry.conn.flush() {
            return self.fail_conn(idx, &e, Retry::Allowed);
        }
        if readable && !self.read_ready(idx)? {
            return Ok(());
        }
        if let Some(entry) = self.slab.get_mut(idx)
            && let Err(e) = entry.conn.sync_interest(self.poll.registry(), token(idx))
        {
            return self.fail_conn(idx, &e, Retry::Allowed);
        }
        Ok(())
    }

    /// Drains the socket. Returns whether the connection is still open.
    fn read_ready(&mut self, idx: usize) -> io::Result<bool> {
        loop {
            let Some(entry) = self.slab.get_mut(idx) else {
                return Ok(false);
            };
            self.rx.clear();
            match entry.conn.read(&mut self.rx) {
                Ok(batch) => {
                    if batch.plaintext_bytes > 0 {
                        self.counters.rx_batches += 1;
                        let t_recv = if let Some(realtime) = batch.kernel_rx_ns {
                            self.offset.to_mono(realtime)
                        } else {
                            self.counters.rx_batches_without_timestamp += 1;
                            now_ns()
                        };
                        if !self.on_data(idx, t_recv)? {
                            return Ok(false);
                        }
                    }
                    if batch.eof {
                        self.on_eof(idx)?;
                        return Ok(false);
                    }
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => return Ok(true),
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => {
                    self.fail_conn(idx, &e, Retry::Allowed)?;
                    return Ok(false);
                }
            }
        }
    }

    /// Feeds the received bytes in `self.rx` to the connection's parser.
    /// Returns whether the connection is still open.
    fn on_data(&mut self, idx: usize, t_recv: u64) -> io::Result<bool> {
        let Self {
            slab,
            recorder,
            diagnostics,
            rx,
            chunks_total,
            mode,
            ..
        } = self;
        let (streaming, ttft_ns) = match *mode {
            Mode::Stream { ttft_ns, .. } => (true, ttft_ns),
            Mode::Whole { ttft_ns, .. } => (false, ttft_ns),
        };
        let entry = slab.get_mut(idx).expect("caller checked the entry");
        let Connection {
            parser, request, ..
        } = entry;
        let Some(request) = request.as_mut() else {
            self.close(idx);
            self.record_error(t_recv, "unsolicited");
            return Ok(false);
        };
        let fed = parser.feed(rx, |marker| {
            recorder.record_span(Metric::ChunkWire, t_recv, marker.t_write, t_recv);
            recorder.record_span(Metric::MockWriteLag, t_recv, marker.t_sched, marker.t_write);
            if streaming {
                recorder.record_span(Metric::ChunkLatency, t_recv, marker.t_sched, t_recv);
                recorder.add_chunks(t_recv, 1);
                *chunks_total += 1;
                if request.markers == 0 {
                    recorder.record_span(Metric::Ttft, t_recv, request.sched_ns, t_recv);
                    if request.fresh {
                        diagnostics.fresh_ttft(t_recv, t_recv.saturating_sub(request.sched_ns));
                    } else {
                        recorder.record_span(Metric::TtftReused, t_recv, request.sched_ns, t_recv);
                    }
                }
            }
            // A fresh connection's handshake would count as slip.
            if request.markers == 0 && !request.fresh {
                diagnostics.request_slip(
                    t_recv,
                    request.sent_ns,
                    marker.t_sched.saturating_sub(ttft_ns),
                );
            }
            request.markers += 1;
            request.last_marker_sched = marker.t_sched;
        });
        match fed {
            Ok(None) => Ok(true),
            Ok(Some(completion)) => Ok(self.complete(idx, completion, t_recv)),
            Err(e) => {
                self.fail_conn_kind(idx, e.kind(), Retry::Never)?;
                Ok(false)
            }
        }
    }

    /// Accounts for a complete response. Returns whether the connection
    /// stays open (back in the idle pool).
    fn complete(&mut self, idx: usize, completion: Completion, t_recv: u64) -> bool {
        let entry = self
            .slab
            .get_mut(idx)
            .expect("completing a live connection");
        let request = entry
            .request
            .take()
            .expect("a completion belongs to a request");
        entry.served += 1;
        let reusable = completion.keep_alive && entry.conn.pending_bytes() == 0;
        if completion.status == 200 {
            if matches!(self.mode, Mode::Whole { .. }) {
                self.recorder
                    .record_span(Metric::RequestLatency, t_recv, request.sched_ns, t_recv);
            }
            self.recorder.add_request(t_recv);
            self.requests_total += 1;
            self.finish_request(request, Some(t_recv.saturating_sub(request.sched_ns)));
        } else {
            self.record_error(t_recv, status_kind(completion.status));
            self.finish_request(request, None);
        }
        if reusable {
            self.idle.push(idx);
        } else {
            self.close(idx);
        }
        reusable
    }

    fn on_eof(&mut self, idx: usize) -> io::Result<()> {
        let entry = self.slab.get_mut(idx).expect("caller checked the entry");
        if entry.request.is_none() {
            self.counters.idle_closed += 1;
            self.close(idx);
            return Ok(());
        }
        match entry.parser.finish_eof() {
            Ok(completion) => {
                self.complete(idx, completion, now_ns());
                Ok(())
            }
            Err(e) => self.fail_conn_kind(idx, e.kind(), Retry::Allowed),
        }
    }

    fn fail_conn(&mut self, idx: usize, err: &io::Error, retry: Retry) -> io::Result<()> {
        let Some(entry) = self.slab.get_mut(idx) else {
            return Ok(());
        };
        let kind = io_error_kind(err, entry.conn.is_handshaking(), entry.conn.is_tls());
        self.fail_conn_kind(idx, kind, retry)
    }

    /// Closes a failed connection and settles its request: a stale pooled
    /// connection is retried once, anything else is an error of `kind`.
    fn fail_conn_kind(&mut self, idx: usize, kind: &'static str, retry: Retry) -> io::Result<()> {
        let Some(entry) = self.close(idx) else {
            return Ok(());
        };
        let now = now_ns();
        let Some(mut request) = entry.request else {
            self.counters.idle_closed += 1;
            return Ok(());
        };
        let stale = retry == Retry::Allowed
            && entry.served > 0
            && !entry.parser.received_any()
            && !request.retried
            && now.saturating_sub(request.sent_ns) <= STALE_WINDOW_NS;
        if stale {
            self.counters.stale_retries += 1;
            self.record_error(now, STALE_RETRY);
            request.retried = true;
            return self.connect_and_send(request, false);
        }
        self.record_error(now, kind);
        self.finish_request(request, None);
        Ok(())
    }

    /// Removes a connection from the poll, the pool and the slab.
    fn close(&mut self, idx: usize) -> Option<Connection> {
        let mut entry = self.slab.remove(idx)?;
        if let Some(pos) = self.idle.iter().rposition(|&i| i == idx) {
            self.idle.remove(pos);
        }
        // Closing the socket removes it from the poll as well; deregistering
        // first keeps the registry consistent on every platform.
        let _ = entry.conn.deregister(self.poll.registry());
        Some(entry)
    }

    fn record_error(&mut self, at_ns: u64, kind: &'static str) {
        self.recorder.add_error(at_ns, kind);
        self.errors_total += 1;
    }

    /// Ends a request; `latency_ns` is `None` when it failed.
    fn finish_request(&mut self, request: Request, latency_ns: Option<u64>) {
        self.open -= 1;
        if let Some(ramp) = &mut self.ramp {
            ramp.on_done(request.step, latency_ns);
        }
    }

    fn sample(&mut self, now: u64) {
        self.recorder.observe_concurrency(now, self.open);
        let planned_open = match &mut self.planned {
            Some((planned, ..)) => planned.sample(now),
            None => 0,
        };
        self.live.open.store(self.open, Ordering::Relaxed);
        self.live
            .planned_open
            .store(planned_open, Ordering::Relaxed);
        self.live.chunks.store(self.chunks_total, Ordering::Relaxed);
        self.live
            .requests
            .store(self.requests_total, Ordering::Relaxed);
        self.live.errors.store(self.errors_total, Ordering::Relaxed);
    }

    fn every_second(&mut self, now: u64) -> io::Result<()> {
        self.offset.refresh();
        self.recorder.tick(now);
        let expired: Vec<(usize, Request)> = self
            .slab
            .iter()
            .filter_map(|(idx, c)| {
                c.request
                    .filter(|r| now.saturating_sub(r.sched_ns) > self.request_timeout_ns)
                    .map(|r| (idx, r))
            })
            .collect();
        for (idx, request) in expired {
            self.censor(&request, now, self.interval_index(now));
            self.fail_conn_kind(idx, "timeout", Retry::Never)?;
        }
        Ok(())
    }

    fn interval_index(&self, t_ns: u64) -> u64 {
        t_ns.saturating_sub(self.origin_ns) / SEC_NS
    }

    /// Records, for a request given up on at `now`, the elapsed time as a
    /// lower bound of every latency that was already due.
    ///
    /// The first response byte is due `ttft` after the scheduled start. Once
    /// a marker has arrived, the next chunks are due at the mock's own
    /// schedule, one interval after the other from the last marker's
    /// `t_sched`. Before the first marker their planned times on the mock's
    /// clock are unknown (they follow the mock's receipt of the request,
    /// which a large body delays), so only the TTFT bound is recorded.
    fn censor(&mut self, request: &Request, now: u64, index: u64) {
        let before = self.counters.censored_samples;
        let mut record = |metric: Metric, value_ns: u64, count: u64| {
            self.censored.record(index, metric, value_ns, count);
            self.counters.censored_samples += count;
        };
        match self.mode {
            Mode::Whole { ttft_ns, .. } => {
                if now >= request.sched_ns.saturating_add(ttft_ns) {
                    record(Metric::RequestLatency, now - request.sched_ns, 1);
                }
            }
            Mode::Stream {
                ttft_ns,
                interval_ns,
            } => {
                if request.markers == 0 {
                    if now >= request.sched_ns.saturating_add(ttft_ns) {
                        record(Metric::Ttft, now - request.sched_ns, 1);
                        if !request.fresh {
                            record(Metric::TtftReused, now - request.sched_ns, 1);
                        }
                    }
                } else {
                    let remaining = u64::from(request.chunks.saturating_sub(request.markers));
                    let first_due = request.last_marker_sched.saturating_add(interval_ns);
                    if remaining > 0 && now >= first_due {
                        let late = now - first_due;
                        match late.checked_div(interval_ns) {
                            // All remaining chunks were due at once.
                            None => record(Metric::ChunkLatency, late, remaining),
                            Some(periods) => {
                                for k in 0..(periods + 1).min(remaining) {
                                    record(Metric::ChunkLatency, late - k * interval_ns, 1);
                                }
                            }
                        }
                    }
                }
            }
        }
        if self.counters.censored_samples > before {
            self.counters.censored_requests += 1;
        }
    }

    /// Reports every ramp step whose time has come: its judging deadline
    /// has passed, or it has ended with all its sends made and done.
    fn report_ramp_steps(&mut self, now: u64) -> io::Result<()> {
        let Some(mut ramp) = self.ramp.take() else {
            return Ok(());
        };
        let reported = self.report_due_steps(&mut ramp, now);
        self.ramp = Some(ramp);
        reported
    }

    fn report_due_steps(&mut self, ramp: &mut RampTracker, now: u64) -> io::Result<()> {
        while let Some(seg) = ramp.spec.steps.get(ramp.next_report).copied() {
            // Arrivals come in step order, so a later one means every send
            // of this step was made.
            let all_sent = self.next.is_none_or(|a| a.step > seg.step);
            let outstanding = ramp.steps.get(&seg.step).map_or(0, |a| a.outstanding);
            let due = now >= seg.end_ns.saturating_add(ramp.spec.judge_delay_ns)
                || (now >= seg.end_ns && all_sent && outstanding == 0);
            if !due {
                break;
            }
            self.send_step_report(ramp, seg, now, now)?;
        }
        Ok(())
    }

    /// After a stop at `stop_ns`, reports as of `now` every step that had
    /// started by then and was not reported yet. Its planned sends due
    /// before the stop and not made count as unsent; the later ones were
    /// never due.
    fn report_stopped_steps(&mut self, stop_ns: u64, now: u64) -> io::Result<()> {
        let Some(mut ramp) = self.ramp.take() else {
            return Ok(());
        };
        let mut reported = Ok(());
        while let Some(seg) = ramp.spec.steps.get(ramp.next_report).copied()
            && seg.start_ns < stop_ns
        {
            reported = self.send_step_report(&mut ramp, seg, now, stop_ns);
            if reported.is_err() {
                break;
            }
        }
        self.ramp = Some(ramp);
        reported
    }

    /// Sends the report of `seg` as of `now`. Requests of the step still
    /// open are censored at `now`; so are the step's planned sends
    /// scheduled before `cutoff` and not made yet, which are dropped from
    /// the schedule (with any warmup send still pending before them).
    fn send_step_report(
        &mut self,
        ramp: &mut RampTracker,
        seg: RateSegment,
        now: u64,
        cutoff: u64,
    ) -> io::Result<()> {
        let mut acc = ramp.steps.remove(&seg.step).unwrap_or_else(StepAcc::new);
        let mut open = 0;
        for (_, c) in self.slab.iter() {
            if let Some(r) = c.request.filter(|r| r.step == seg.step) {
                acc.latency
                    .saturating_record(now.saturating_sub(r.sched_ns));
                open += 1;
            }
        }
        let mut unsent = 0;
        while let Some(arrival) = self
            .next
            .filter(|a| a.step <= seg.step && a.sched_ns < cutoff)
        {
            if arrival.step == seg.step {
                acc.latency
                    .saturating_record(now.saturating_sub(arrival.sched_ns));
                unsent += 1;
            }
            self.next = self.schedule.next_arrival();
        }
        ramp.spec
            .reports
            .send(RampMessage::Step(StepReport {
                step: seg.step,
                latency: acc.latency,
                requests: acc.requests,
                errors: acc.errors,
                censored: open + unsent,
                unsent,
                max_emit_lag_ns: acc.max_emit_lag_ns,
                late_sends: acc.late_sends,
            }))
            .map_err(|_| io::Error::other("ramp coordinator is gone"))?;
        ramp.next_report += 1;
        Ok(())
    }

    fn finish(mut self) -> io::Result<ShardOutput> {
        let end_ns = self.end_ns;
        let last_index = self.interval_index(end_ns.saturating_sub(1));
        let open: Vec<usize> = self.slab.iter().map(|(idx, _)| idx).collect();
        for idx in open {
            if let Some(Connection {
                request: Some(request),
                ..
            }) = self.close(idx)
            {
                self.counters.open_at_end += 1;
                self.censor(&request, end_ns, last_index);
            }
        }
        let mut intervals = self.recorder.finish(end_ns);
        self.censored
            .merge_into(&mut intervals)
            .map_err(io::Error::other)?;
        Ok(ShardOutput {
            intervals,
            planned: self.planned.map(|(planned, ..)| planned.finish()),
            counters: self.counters,
            clock_steps: self.offset.steps(),
            diagnostics: self.diagnostics.finish(),
            end_ns,
        })
    }
}

impl std::fmt::Debug for Shard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Shard")
            .field("index", &self.index)
            .field("origin_ns", &self.origin_ns)
            .field("end_ns", &self.end_ns)
            .field("open", &self.open)
            .finish_non_exhaustive()
    }
}

fn token(idx: usize) -> Token {
    Token(idx + 1)
}

/// The next multiple of `step` after `now`, counting from `t`.
fn advance(t: u64, step: u64, now: u64) -> u64 {
    t + (now.saturating_sub(t) / step + 1) * step
}

/// Error kind of a non-200 response.
pub(crate) fn status_kind(status: u16) -> &'static str {
    match status {
        429 => "http_429",
        503 => "http_503",
        400..=499 => "http_4xx",
        500..=599 => "http_5xx",
        _ => "http_other",
    }
}

/// Error kind of a failed socket operation.
fn io_error_kind(err: &io::Error, handshaking: bool, tls: bool) -> &'static str {
    use io::ErrorKind as K;
    match err.kind() {
        K::InvalidData if tls => "tls",
        _ if handshaking => "connect",
        K::ConnectionReset | K::ConnectionAborted | K::BrokenPipe => "reset",
        K::TimedOut => "timeout",
        K::UnexpectedEof => "eof",
        _ => "io",
    }
}

#[cfg(test)]
mod tests;
