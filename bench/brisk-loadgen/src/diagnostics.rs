//! Load-generator evidence that the core metric set has no place for.
//!
//! - **Deadline misses.** A send the event loop reaches only after its
//!   scheduled time has missed its spin window. Either the loop woke late
//!   (the timer fired late or the thread was not running) or handling other
//!   events ran past the deadline. Sends reached in time still show emit lag
//!   when the thread is preempted while spinning or sending. The split tells
//!   which remedy an emit lag failure needs.
//! - **Fresh connections.** A request sent on a newly opened connection
//!   carries the TCP (and TLS) handshake in its TTFT or request latency. A
//!   server that closes idle keep-alive connections forces more of them, so
//!   their number and TTFT show how much of an arm's tail is connection
//!   setup.
//! - **Request slip.** On a pooled connection, `t0 − t_send`, where `t0 =
//!   t_sched(seq 0) − ttft` is the moment the mock had read the whole
//!   request. It measures the inbound path to the mock: loopback plus the
//!   time the request waited behind the mock's own work. TTFT includes it,
//!   while mock write lag does not, so a saturated mock inflates TTFT with
//!   every other check passing. Both timestamps are `CLOCK_MONOTONIC` of one
//!   host, the assumption chunk wire time already makes. Through a gateway
//!   the slip includes the gateway's forwarding, so it only bounds the mock
//!   on a direct arm.
//!
//! Shards record per interval so that the warmup can be excluded like
//! everywhere else.

use std::collections::BTreeMap;

use brisk_bench_core::result::MetricSummary;
use brisk_bench_core::stats;
use hdrhistogram::Histogram;
use serde::Serialize;

use crate::schedule::SEC_NS;

/// What delayed a send past its deadline.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Miss {
    /// The event loop returned from its wait after the deadline.
    LateWakeup,
    /// The loop was awake before the deadline but handling other events.
    BusyLoop,
}

/// One interval's diagnostics of one shard, or of all shards once merged.
#[derive(Debug, Clone, Default)]
pub(crate) struct IntervalDiagnostics {
    late_wakeups: u64,
    busy_loop_misses: u64,
    max_miss_ns: u64,
    fresh_sends: u64,
    negative_slips: u64,
    request_slip: Option<Histogram<u64>>,
    fresh_ttft: Option<Histogram<u64>>,
}

impl IntervalDiagnostics {
    fn merge(&mut self, other: Self) {
        self.late_wakeups += other.late_wakeups;
        self.busy_loop_misses += other.busy_loop_misses;
        self.max_miss_ns = self.max_miss_ns.max(other.max_miss_ns);
        self.fresh_sends += other.fresh_sends;
        self.negative_slips += other.negative_slips;
        add_histogram(&mut self.request_slip, other.request_slip);
        add_histogram(&mut self.fresh_ttft, other.fresh_ttft);
    }
}

fn add_histogram(dst: &mut Option<Histogram<u64>>, src: Option<Histogram<u64>>) {
    match (dst.as_mut(), src) {
        (Some(acc), Some(h)) => acc
            .add(&h)
            .expect("diagnostic histograms share the standard bounds"),
        (None, Some(h)) => *dst = Some(h),
        (_, None) => {}
    }
}

fn record(histogram: &mut Option<Histogram<u64>>, value_ns: u64) {
    histogram
        .get_or_insert_with(stats::new_histogram)
        .saturating_record(value_ns);
}

/// Per-interval diagnostics keyed by interval index.
pub(crate) type Intervals = BTreeMap<u64, IntervalDiagnostics>;

/// A shard's diagnostics, recorded on the interval grid of its recorder.
#[derive(Debug)]
pub(crate) struct Recorder {
    origin_ns: u64,
    intervals: Intervals,
}

impl Recorder {
    /// A recorder whose interval 0 starts at `origin_ns`.
    pub(crate) fn new(origin_ns: u64) -> Self {
        Self {
            origin_ns,
            intervals: Intervals::new(),
        }
    }

    fn at(&mut self, at_ns: u64) -> &mut IntervalDiagnostics {
        let index = at_ns.saturating_sub(self.origin_ns) / SEC_NS;
        self.intervals.entry(index).or_default()
    }

    /// A send reached at `at_ns`, `miss_ns` after its deadline.
    pub(crate) fn deadline_missed(&mut self, at_ns: u64, miss_ns: u64, miss: Miss) {
        let interval = self.at(at_ns);
        match miss {
            Miss::LateWakeup => interval.late_wakeups += 1,
            Miss::BusyLoop => interval.busy_loop_misses += 1,
        }
        interval.max_miss_ns = interval.max_miss_ns.max(miss_ns);
    }

    /// A request went out on a newly opened connection.
    pub(crate) fn fresh_send(&mut self, at_ns: u64) {
        self.at(at_ns).fresh_sends += 1;
    }

    /// TTFT of a request sent on a newly opened connection.
    pub(crate) fn fresh_ttft(&mut self, at_ns: u64, ttft_ns: u64) {
        record(&mut self.at(at_ns).fresh_ttft, ttft_ns);
    }

    /// Slip of a request sent at `sent_ns` on a pooled connection that the
    /// mock had read completely at `read_ns`.
    pub(crate) fn request_slip(&mut self, at_ns: u64, sent_ns: u64, read_ns: u64) {
        let interval = self.at(at_ns);
        match read_ns.checked_sub(sent_ns) {
            Some(slip) => record(&mut interval.request_slip, slip),
            None => interval.negative_slips += 1,
        }
    }

    /// The recorded intervals.
    pub(crate) fn finish(self) -> Intervals {
        self.intervals
    }
}

/// Adds one shard's intervals to the merged ones.
pub(crate) fn merge_into(merged: &mut Intervals, shard: Intervals) {
    for (index, interval) in shard {
        merged.entry(index).or_default().merge(interval);
    }
}

/// Post-warmup diagnostics of a run.
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub(crate) struct Diagnostics {
    /// Sends the event loop woke up for only after their deadline.
    pub(crate) late_wakeups: u64,
    /// Sends whose deadline passed while the loop handled other events.
    pub(crate) busy_loop_misses: u64,
    /// Largest deadline miss, nanoseconds.
    pub(crate) max_deadline_miss_ns: u64,
    /// Requests sent on a newly opened connection, retries included.
    pub(crate) fresh_conn_sends: u64,
    /// TTFT of the requests sent on a newly opened connection.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) fresh_conn_ttft: Option<MetricSummary>,
    /// Send to the mock's receipt of the whole request, pooled connections
    /// only.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) request_slip: Option<MetricSummary>,
    /// Slips whose mock receipt preceded the send: the clocks differ.
    pub(crate) negative_slips: u64,
}

impl Diagnostics {
    /// Sums the intervals from `warmup_intervals` on.
    pub(crate) fn summarize(intervals: &Intervals, warmup_intervals: u64) -> Self {
        let mut total = IntervalDiagnostics::default();
        for interval in intervals.range(warmup_intervals..).map(|(_, i)| i) {
            total.merge(interval.clone());
        }
        Self {
            late_wakeups: total.late_wakeups,
            busy_loop_misses: total.busy_loop_misses,
            max_deadline_miss_ns: total.max_miss_ns,
            fresh_conn_sends: total.fresh_sends,
            fresh_conn_ttft: total.fresh_ttft.as_ref().map(summary),
            request_slip: total.request_slip.as_ref().map(summary),
            negative_slips: total.negative_slips,
        }
    }

    /// Sends that missed their spin window.
    pub(crate) fn deadline_misses(&self) -> u64 {
        self.late_wakeups + self.busy_loop_misses
    }
}

/// The headline numbers of a histogram, computed as the core summarises
/// the interval histograms.
fn summary(h: &Histogram<u64>) -> MetricSummary {
    MetricSummary {
        count: h.len(),
        min_ns: h.min(),
        mean_ns: h.mean(),
        p50_ns: h.value_at_quantile(0.50),
        p90_ns: h.value_at_quantile(0.90),
        p99_ns: h.value_at_quantile(0.99),
        p999_ns: h.value_at_quantile(0.999),
        max_ns: h.max(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn warmup_is_excluded_and_shards_add_up() {
        let origin = 1_000;
        let mut a = Recorder::new(origin);
        // Interval 0 is warmup.
        a.deadline_missed(origin + 10, 900_000, Miss::LateWakeup);
        a.fresh_send(origin + 10);
        a.request_slip(origin + 10, 0, 7_000_000);
        // Interval 1.
        a.deadline_missed(origin + SEC_NS, 12_000, Miss::LateWakeup);
        a.deadline_missed(origin + SEC_NS + 5, 3_000, Miss::BusyLoop);
        a.fresh_send(origin + SEC_NS);
        a.fresh_ttft(origin + SEC_NS, 250_000);
        a.request_slip(origin + SEC_NS, 100, 8_100);
        let mut b = Recorder::new(origin);
        // Interval 2, another shard.
        b.deadline_missed(origin + 2 * SEC_NS, 27_000, Miss::BusyLoop);
        b.request_slip(origin + 2 * SEC_NS, 100, 20_100);
        b.request_slip(origin + 2 * SEC_NS, 100, 50);

        let mut merged = a.finish();
        merge_into(&mut merged, b.finish());
        let d = Diagnostics::summarize(&merged, 1);
        assert_eq!((d.late_wakeups, d.busy_loop_misses), (1, 2));
        assert_eq!(d.deadline_misses(), 3);
        assert_eq!(d.max_deadline_miss_ns, 27_000);
        assert_eq!(d.fresh_conn_sends, 1);
        assert_eq!(d.negative_slips, 1);
        let slip = d.request_slip.expect("two slips after the warmup");
        assert_eq!(slip.count, 2);
        assert!((7_990..=8_010).contains(&slip.min_ns), "{slip:?}");
        assert!((19_980..=20_020).contains(&slip.max_ns), "{slip:?}");
        assert_eq!(d.fresh_conn_ttft.map(|s| s.count), Some(1));

        let all = Diagnostics::summarize(&merged, 0);
        assert_eq!(all.late_wakeups, 2);
        assert_eq!(all.max_deadline_miss_ns, 900_000);
        assert_eq!(all.request_slip.map(|s| s.count), Some(3));
        assert_eq!(Diagnostics::summarize(&merged, 3), Diagnostics::default());
    }
}
