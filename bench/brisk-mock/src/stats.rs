//! Server-wide counters exposed by `GET /__bench/stats`.
//!
//! Every shard owns one cache-line-aligned [`ShardCounters`] block and is the
//! only writer of its counter values, so the hot path never contends on a
//! shared line. The stats endpoint sums all blocks. The reset endpoint,
//! served by any shard, never writes another shard's values: it records a
//! baseline per counter that later snapshots subtract, so a reset racing
//! with an increment cannot lose it.
//!
//! Each shard also keeps a histogram of its read lag: the time from the
//! kernel receiving the last segment of a request to the shard parsing it.
//! The mock takes t0 at parse time, so this lag reaches the client's TTFT
//! without showing up in any marker; the histogram is what makes it visible.
//! It sits behind a mutex that only its shard and the rare stats or reset
//! request take, so the lock is uncontended in practice.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use brisk_bench_core::stats::{encode_histogram, new_histogram};
use hdrhistogram::Histogram;
use serde::Serialize;

/// A monotonic counter that one shard increments and any shard may read or
/// reset.
#[derive(Debug, Default)]
struct Counter {
    /// Total since start; written by the owning shard only.
    value: AtomicU64,
    /// `value` at the last reset; written by whichever shard serves it.
    baseline: AtomicU64,
}

impl Counter {
    #[inline]
    fn add(&self, n: u64) {
        self.value.fetch_add(n, Ordering::Relaxed);
    }

    /// Total since start, unaffected by resets.
    fn total(&self) -> u64 {
        self.value.load(Ordering::Relaxed)
    }

    /// Count since the last reset.
    fn get(&self) -> u64 {
        // The two loads are not one atomic snapshot; a reset landing between
        // them can make the baseline briefly exceed the value read.
        self.total()
            .saturating_sub(self.baseline.load(Ordering::Relaxed))
    }

    fn reset(&self) {
        self.baseline.store(self.total(), Ordering::Relaxed);
    }
}

/// Error categories counted separately.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorKind {
    /// Unparseable request head, bad framing or unsupported HTTP version.
    BadRequest,
    /// The request head exceeds the size or header-count limit.
    HeadTooLarge,
    /// The request body exceeds the size limit.
    BodyTooLarge,
    /// A `bench:v1;` directive is present but invalid.
    BadDirective,
    /// Unknown path.
    NotFound,
    /// Known path, wrong method.
    MethodNotAllowed,
    /// TLS handshake or record failure.
    Tls,
    /// Socket error other than a peer reset during a response.
    Io,
    /// The client went away while a response was still scheduled.
    Aborted,
    /// `accept` failed (for example `EMFILE`).
    Accept,
}

/// Counters of one shard, padded to their own cache lines.
#[derive(Debug)]
#[repr(align(128))]
pub struct ShardCounters {
    accepts: Counter,
    requests: Counter,
    closed: Counter,
    streams_started: Counter,
    streams_finished: Counter,
    chunks: Counter,
    write_blocked: Counter,
    bad_request: Counter,
    head_too_large: Counter,
    body_too_large: Counter,
    bad_directive: Counter,
    not_found: Counter,
    method_not_allowed: Counter,
    tls: Counter,
    io: Counter,
    aborted: Counter,
    accept: Counter,
    clock_steps: Counter,
    read_lag: Mutex<Histogram<u64>>,
}

impl Default for ShardCounters {
    fn default() -> Self {
        Self {
            accepts: Counter::default(),
            requests: Counter::default(),
            closed: Counter::default(),
            streams_started: Counter::default(),
            streams_finished: Counter::default(),
            chunks: Counter::default(),
            write_blocked: Counter::default(),
            bad_request: Counter::default(),
            head_too_large: Counter::default(),
            body_too_large: Counter::default(),
            bad_directive: Counter::default(),
            not_found: Counter::default(),
            method_not_allowed: Counter::default(),
            tls: Counter::default(),
            io: Counter::default(),
            aborted: Counter::default(),
            accept: Counter::default(),
            clock_steps: Counter::default(),
            read_lag: Mutex::new(new_histogram()),
        }
    }
}

impl ShardCounters {
    /// A connection was accepted.
    #[inline]
    pub fn accepted(&self) {
        self.accepts.add(1);
    }

    /// A complete API request (not a `/__bench/*` request) was received.
    #[inline]
    pub fn request(&self) {
        self.requests.add(1);
    }

    /// A connection was closed, for whatever reason.
    #[inline]
    pub fn connection_closed(&self) {
        self.closed.add(1);
    }

    /// A streaming response was started.
    #[inline]
    pub fn stream_started(&self) {
        self.streams_started.add(1);
    }

    /// A streaming response ended, completed or aborted.
    #[inline]
    pub fn stream_finished(&self) {
        self.streams_finished.add(1);
    }

    /// A content chunk was written.
    #[inline]
    pub fn chunk(&self) {
        self.chunks.add(1);
    }

    /// A write left data pending because the socket would block.
    #[inline]
    pub fn write_blocked(&self) {
        self.write_blocked.add(1);
    }

    /// A request was parsed `lag_ns` after the kernel received its last
    /// bytes.
    pub fn read_lag(&self, lag_ns: u64) {
        self.lock_read_lag().saturating_record(lag_ns);
    }

    /// The realtime-to-monotonic offset used for read lags jumped, so lags
    /// recorded around that moment are off by the jump.
    pub fn clock_step(&self) {
        self.clock_steps.add(1);
    }

    fn lock_read_lag(&self) -> MutexGuard<'_, Histogram<u64>> {
        // Every histogram update is a single bucket increment, so a panic
        // while the lock was held cannot have left it half-written.
        self.read_lag.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// An error of the given kind occurred.
    pub fn error(&self, kind: ErrorKind) {
        match kind {
            ErrorKind::BadRequest => &self.bad_request,
            ErrorKind::HeadTooLarge => &self.head_too_large,
            ErrorKind::BodyTooLarge => &self.body_too_large,
            ErrorKind::BadDirective => &self.bad_directive,
            ErrorKind::NotFound => &self.not_found,
            ErrorKind::MethodNotAllowed => &self.method_not_allowed,
            ErrorKind::Tls => &self.tls,
            ErrorKind::Io => &self.io,
            ErrorKind::Aborted => &self.aborted,
            ErrorKind::Accept => &self.accept,
        }
        .add(1);
    }

    fn all(&self) -> [&Counter; 18] {
        [
            &self.accepts,
            &self.requests,
            &self.closed,
            &self.streams_started,
            &self.streams_finished,
            &self.chunks,
            &self.write_blocked,
            &self.bad_request,
            &self.head_too_large,
            &self.body_too_large,
            &self.bad_directive,
            &self.not_found,
            &self.method_not_allowed,
            &self.tls,
            &self.io,
            &self.aborted,
            &self.accept,
            &self.clock_steps,
        ]
    }

    fn active_streams(&self) -> u64 {
        // The gauge comes from totals, which resets do not touch.
        self.streams_started
            .total()
            .saturating_sub(self.streams_finished.total())
    }
}

/// The counters of every shard of one server.
#[derive(Debug)]
pub struct Stats {
    shards: Box<[ShardCounters]>,
}

impl Stats {
    /// Creates zeroed counters for `shards` shards.
    pub fn new(shards: usize) -> Arc<Self> {
        Arc::new(Self {
            shards: (0..shards).map(|_| ShardCounters::default()).collect(),
        })
    }

    /// The counter block owned by shard `index`.
    ///
    /// # Panics
    ///
    /// Panics if `index` is not a valid shard index.
    pub fn shard(&self, index: usize) -> &ShardCounters {
        &self.shards[index]
    }

    /// Sums all shards and lists each one.
    ///
    /// # Panics
    ///
    /// Never: every read-lag histogram is created with the same bounds, so
    /// merging them cannot fail.
    pub fn snapshot(&self) -> Snapshot {
        let sum = |f: fn(&ShardCounters) -> &Counter| self.shards.iter().map(|s| f(s).get()).sum();
        let mut read_lag = new_histogram();
        for shard in &*self.shards {
            read_lag
                .add(&*shard.lock_read_lag())
                .expect("every read-lag histogram has the same bounds");
        }
        Snapshot {
            accepts: sum(|s| &s.accepts),
            requests: sum(|s| &s.requests),
            closed: sum(|s| &s.closed),
            active_streams: self.shards.iter().map(ShardCounters::active_streams).sum(),
            streams_started: sum(|s| &s.streams_started),
            chunks: sum(|s| &s.chunks),
            write_blocked: sum(|s| &s.write_blocked),
            read_lag: ReadLagSnapshot::new(&read_lag, sum(|s| &s.clock_steps)),
            shards: self
                .shards
                .iter()
                .map(|s| ShardSnapshot {
                    accepts: s.accepts.get(),
                    requests: s.requests.get(),
                    chunks: s.chunks.get(),
                    active_streams: s.active_streams(),
                    write_blocked: s.write_blocked.get(),
                    read_lag_samples: s.lock_read_lag().len(),
                })
                .collect(),
            errors: ErrorSnapshot {
                bad_request: sum(|s| &s.bad_request),
                head_too_large: sum(|s| &s.head_too_large),
                body_too_large: sum(|s| &s.body_too_large),
                bad_directive: sum(|s| &s.bad_directive),
                not_found: sum(|s| &s.not_found),
                method_not_allowed: sum(|s| &s.method_not_allowed),
                tls: sum(|s| &s.tls),
                io: sum(|s| &s.io),
                aborted: sum(|s| &s.aborted),
                accept: sum(|s| &s.accept),
            },
        }
    }

    /// Restarts every counter and read-lag histogram from zero. The
    /// `active_streams` gauge is not a counter and keeps describing live
    /// streams.
    pub fn reset(&self) {
        for shard in &*self.shards {
            for counter in shard.all() {
                counter.reset();
            }
            shard.lock_read_lag().reset();
        }
    }
}

/// JSON body of `GET /__bench/stats`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, serde::Deserialize)]
pub struct Snapshot {
    /// Accepted connections.
    pub accepts: u64,
    /// Complete API requests received (`/__bench/*` excluded).
    pub requests: u64,
    /// Closed connections.
    pub closed: u64,
    /// Streaming responses currently in progress.
    pub active_streams: u64,
    /// Streaming responses started since the last reset.
    pub streams_started: u64,
    /// Content chunks written.
    pub chunks: u64,
    /// Writes that could not be completed immediately.
    pub write_blocked: u64,
    /// Delay from the kernel receiving a request to the mock parsing it.
    pub read_lag: ReadLagSnapshot,
    /// Per-shard breakdown, in shard order. `SO_REUSEPORT` spreads
    /// connections by a hash of their addresses, so with few long-lived
    /// connections the load of the shards can differ a lot.
    pub shards: Vec<ShardSnapshot>,
    /// Error counters by category.
    pub errors: ErrorSnapshot,
}

/// Read-lag distribution inside a [`Snapshot`], in nanoseconds.
///
/// A sample is taken for every request that became complete on a receive
/// carrying a kernel timestamp (`SO_TIMESTAMPNS`, Linux only). A request
/// pipelined behind another response is not sampled: its wait is set by
/// that response, not by the shard.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, serde::Deserialize)]
pub struct ReadLagSnapshot {
    /// Number of samples.
    pub samples: u64,
    /// Median.
    pub p50_ns: u64,
    /// 99th percentile.
    pub p99_ns: u64,
    /// 99.9th percentile.
    pub p999_ns: u64,
    /// Largest sample.
    pub max_ns: u64,
    /// Jumps of the realtime clock seen while converting kernel timestamps;
    /// samples taken around a jump are off by its size.
    pub clock_steps: u64,
    /// The whole distribution, encoded by
    /// `brisk_bench_core::stats::encode_histogram`.
    pub histogram: String,
}

impl ReadLagSnapshot {
    fn new(hist: &Histogram<u64>, clock_steps: u64) -> Self {
        Self {
            samples: hist.len(),
            p50_ns: hist.value_at_quantile(0.5),
            p99_ns: hist.value_at_quantile(0.99),
            p999_ns: hist.value_at_quantile(0.999),
            max_ns: hist.max(),
            clock_steps,
            histogram: encode_histogram(hist),
        }
    }
}

/// Counters of one shard inside a [`Snapshot`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, serde::Deserialize)]
pub struct ShardSnapshot {
    /// Accepted connections.
    pub accepts: u64,
    /// Complete API requests received.
    pub requests: u64,
    /// Content chunks written.
    pub chunks: u64,
    /// Streaming responses currently in progress.
    pub active_streams: u64,
    /// Writes that could not be completed immediately.
    pub write_blocked: u64,
    /// Read-lag samples taken.
    pub read_lag_samples: u64,
}

/// Error counters inside a [`Snapshot`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, serde::Deserialize)]
#[allow(
    clippy::struct_field_names,
    reason = "field names mirror the JSON keys"
)]
pub struct ErrorSnapshot {
    /// See [`ErrorKind::BadRequest`].
    pub bad_request: u64,
    /// See [`ErrorKind::HeadTooLarge`].
    pub head_too_large: u64,
    /// See [`ErrorKind::BodyTooLarge`].
    pub body_too_large: u64,
    /// See [`ErrorKind::BadDirective`].
    pub bad_directive: u64,
    /// See [`ErrorKind::NotFound`].
    pub not_found: u64,
    /// See [`ErrorKind::MethodNotAllowed`].
    pub method_not_allowed: u64,
    /// See [`ErrorKind::Tls`].
    pub tls: u64,
    /// See [`ErrorKind::Io`].
    pub io: u64,
    /// See [`ErrorKind::Aborted`].
    pub aborted: u64,
    /// See [`ErrorKind::Accept`].
    pub accept: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_sums_shards_and_reset_keeps_active_streams() {
        let stats = Stats::new(2);
        stats.shard(0).accepted();
        stats.shard(1).accepted();
        stats.shard(0).request();
        stats.shard(1).stream_started();
        stats.shard(1).stream_started();
        stats.shard(1).stream_finished();
        stats.shard(0).error(ErrorKind::NotFound);

        let snap = stats.snapshot();
        assert_eq!(snap.accepts, 2);
        assert_eq!(snap.requests, 1);
        assert_eq!(snap.active_streams, 1);
        assert_eq!(snap.errors.not_found, 1);

        stats.reset();
        let snap = stats.snapshot();
        assert_eq!(snap.accepts, 0);
        assert_eq!(snap.streams_started, 0);
        assert_eq!(snap.errors.not_found, 0);
        assert_eq!(snap.active_streams, 1);

        stats.shard(1).stream_finished();
        stats.shard(0).accepted();
        let snap = stats.snapshot();
        assert_eq!(snap.active_streams, 0);
        assert_eq!(snap.accepts, 1);
    }

    #[test]
    fn snapshot_lists_shards_and_merges_read_lag() {
        let stats = Stats::new(2);
        stats.shard(0).accepted();
        stats.shard(1).accepted();
        stats.shard(1).accepted();
        stats.shard(1).request();
        stats.shard(1).chunk();
        stats.shard(1).stream_started();
        stats.shard(0).write_blocked();
        for lag in 1..=99 {
            stats.shard(0).read_lag(lag * 1_000);
        }
        stats.shard(1).read_lag(5_000_000);
        stats.shard(1).clock_step();

        let snap = stats.snapshot();
        assert_eq!(
            snap.shards,
            [
                ShardSnapshot {
                    accepts: 1,
                    requests: 0,
                    chunks: 0,
                    active_streams: 0,
                    write_blocked: 1,
                    read_lag_samples: 99,
                },
                ShardSnapshot {
                    accepts: 2,
                    requests: 1,
                    chunks: 1,
                    active_streams: 1,
                    write_blocked: 0,
                    read_lag_samples: 1,
                },
            ]
        );
        let lag = &snap.read_lag;
        assert_eq!(lag.samples, 100);
        assert!(lag.p50_ns.abs_diff(50_000) <= 50, "{lag:?}");
        assert!(lag.p99_ns.abs_diff(99_000) <= 100, "{lag:?}");
        assert!(lag.max_ns.abs_diff(5_000_000) <= 5_000, "{lag:?}");
        assert_eq!(lag.clock_steps, 1);
        let decoded = brisk_bench_core::stats::decode_histogram(&lag.histogram).unwrap();
        assert_eq!(decoded.len(), 100);

        stats.reset();
        let snap = stats.snapshot();
        assert_eq!(snap.read_lag.samples, 0);
        assert_eq!(snap.read_lag.clock_steps, 0);
        assert!(snap.shards.iter().all(|s| s.read_lag_samples == 0));
        assert_eq!(snap.shards[1].active_streams, 1);
    }

    #[test]
    fn reset_racing_with_increments_loses_nothing() {
        // The owner keeps counting while another thread resets; every
        // increment after the last reset must still be visible, and the
        // gauge must balance.
        let stats = Stats::new(1);
        std::thread::scope(|scope| {
            scope.spawn(|| {
                for _ in 0..10_000 {
                    stats.reset();
                }
            });
            for _ in 0..100_000 {
                stats.shard(0).stream_started();
                stats.shard(0).stream_finished();
            }
        });
        assert_eq!(stats.snapshot().active_streams, 0);
        stats.reset();
        stats.shard(0).request();
        assert_eq!(stats.snapshot().requests, 1);
    }
}
