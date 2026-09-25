//! Per-interval latency histograms and paired bootstrap comparison of runs.
//!
//! Every latency is recorded in nanoseconds into an HDR histogram covering
//! 1 ns to 120 s with 3 significant digits. An [`IntervalRecorder`] keeps one
//! live set of histograms and closes an interval (1 s by default) by encoding
//! it with the V2 format plus base64, so memory does not grow with the length
//! of a run. [`compare`] turns paired repetitions of two arms into confidence
//! intervals for quantile differences: a Student t interval over the
//! per-pair differences, and a two-level bootstrap (whole repetitions, then
//! blocks of intervals within each run) whose spread, widened for the few
//! repetitions it draws from, bounds arm A's own quantiles.

use std::collections::BTreeMap;
use std::fmt;
use std::str::FromStr;
use std::time::Duration;

use base64::Engine as _;
use hdrhistogram::Histogram;
use hdrhistogram::serialization::{Deserializer, Serializer as _, V2Serializer};
use serde::{Deserialize, Serialize};

use crate::result::{ConcurrencyStats, IntervalResult, MetricSummary, RunResult};

/// Lowest trackable value, nanoseconds.
pub const HISTOGRAM_LOW_NS: u64 = 1;
/// Highest trackable value, nanoseconds (120 s).
pub const HISTOGRAM_HIGH_NS: u64 = 120_000_000_000;
/// Significant decimal digits kept by every histogram.
pub const HISTOGRAM_SIGFIG: u8 = 3;
/// Default interval length.
pub const DEFAULT_INTERVAL: Duration = Duration::from_secs(1);
/// Default moving-block length, in intervals.
pub const DEFAULT_BLOCK_LEN: usize = 10;
/// Default number of bootstrap resamples.
pub const DEFAULT_RESAMPLES: usize = 2000;
/// Default bootstrap seed; fixed so reports are reproducible.
pub const DEFAULT_SEED: u64 = 0x6272_6973_6b5f_6d30;
/// Two-sided 95% quantile of the standard normal distribution.
const Z_975: f64 = 1.959_963_984_540_054;

/// Errors from histogram decoding and run comparison.
#[derive(Debug, thiserror::Error)]
pub enum StatsError {
    /// A histogram string is not valid base64.
    #[error("histogram is not valid base64: {0}")]
    Base64(#[from] base64::DecodeError),
    /// A histogram payload is not a valid V2 encoding.
    #[error("histogram payload is invalid: {0:?}")]
    Decode(hdrhistogram::serialization::DeserializeError),
    /// Two histograms could not be added.
    #[error("histograms are incompatible: {0:?}")]
    Add(hdrhistogram::errors::AdditionError),
    /// An arm has no recorded values for the metric after warmup.
    #[error("arm {arm} has no {metric} samples after warmup")]
    EmptyArm {
        /// `"A"` or `"B"`.
        arm: &'static str,
        /// The compared metric.
        metric: Metric,
    },
    /// One run of an arm has no recorded values for the metric after warmup,
    /// so its repetition has nothing to stand for.
    #[error("run {run} of arm {arm} has no {metric} samples after warmup")]
    EmptyRun {
        /// `"A"` or `"B"`.
        arm: &'static str,
        /// Position of the run in its arm.
        run: usize,
        /// The compared metric.
        metric: Metric,
    },
    /// The arms hold different numbers of runs, so they do not form pairs.
    #[error("arm A has {a} run(s) and arm B {b}; runs compare in pairs, one per repetition")]
    UnpairedRuns {
        /// Runs of arm A.
        a: usize,
        /// Runs of arm B.
        b: usize,
    },
    /// The two runs of a pair carry different pairing ids.
    #[error("pair {pair} matches pairing id {a:?} of arm A with {b:?} of arm B")]
    PairMismatch {
        /// Position of the pair.
        pair: usize,
        /// Pairing id of the A run.
        a: String,
        /// Pairing id of the B run.
        b: String,
    },
    /// A bootstrap resample drew only empty intervals.
    #[error("a bootstrap resample of arm {0} contained no samples")]
    EmptyResample(&'static str),
    /// A comparison option is out of range.
    #[error("invalid comparison option: {0}")]
    InvalidOption(&'static str),
}

/// The fixed set of recorded metrics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Metric {
    /// Time to first token: planned request start to first content byte.
    Ttft,
    /// [`Self::Ttft`] of the requests sent on a reused connection only:
    /// planned request start to first content byte, leaving out every
    /// request sent on a newly opened connection (retries included), whose
    /// TTFT carries the connection handshake.
    TtftReused,
    /// `t_recv − t_sched` per chunk.
    ChunkLatency,
    /// `t_recv − t_write` per chunk.
    ChunkWire,
    /// Actual emission time minus planned emission time (load generator).
    EmitLag,
    /// `t_write − t_sched` as reported by the mock.
    MockWriteLag,
    /// Latency of a non-streaming request.
    RequestLatency,
}

impl Metric {
    /// Number of metrics.
    pub const COUNT: usize = 7;
    /// All metrics in index order.
    pub const ALL: [Self; Self::COUNT] = [
        Self::Ttft,
        Self::TtftReused,
        Self::ChunkLatency,
        Self::ChunkWire,
        Self::EmitLag,
        Self::MockWriteLag,
        Self::RequestLatency,
    ];

    /// Stable `snake_case` name, as used in JSON and on the command line.
    pub fn name(self) -> &'static str {
        match self {
            Self::Ttft => "ttft",
            Self::TtftReused => "ttft_reused",
            Self::ChunkLatency => "chunk_latency",
            Self::ChunkWire => "chunk_wire",
            Self::EmitLag => "emit_lag",
            Self::MockWriteLag => "mock_write_lag",
            Self::RequestLatency => "request_latency",
        }
    }

    fn index(self) -> usize {
        self as usize
    }
}

impl fmt::Display for Metric {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// Error for an unknown metric name.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("unknown metric {0:?}")]
pub struct UnknownMetric(pub String);

impl FromStr for Metric {
    type Err = UnknownMetric;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::ALL
            .into_iter()
            .find(|m| m.name() == s)
            .ok_or_else(|| UnknownMetric(s.to_owned()))
    }
}

/// Creates an empty histogram with the standard bounds.
///
/// # Panics
///
/// Never: the bounds are compile-time constants that hdrhistogram accepts.
pub fn new_histogram() -> Histogram<u64> {
    Histogram::new_with_bounds(HISTOGRAM_LOW_NS, HISTOGRAM_HIGH_NS, HISTOGRAM_SIGFIG)
        .expect("constant histogram bounds are valid")
}

/// Encodes a histogram as base64 of its uncompressed V2 serialization.
///
/// # Panics
///
/// Only if a bucket count exceeds `i64::MAX`, which the V2 format cannot
/// represent and no benchmark can reach.
pub fn encode_histogram(hist: &Histogram<u64>) -> String {
    let mut buf = Vec::new();
    V2Serializer::new()
        .serialize(hist, &mut buf)
        .expect("V2 serialization into a Vec only fails for counts above i64::MAX");
    base64::engine::general_purpose::STANDARD.encode(buf)
}

/// Decodes a histogram produced by [`encode_histogram`].
pub fn decode_histogram(encoded: &str) -> Result<Histogram<u64>, StatsError> {
    let bytes = base64::engine::general_purpose::STANDARD.decode(encoded)?;
    Deserializer::new()
        .deserialize(&mut bytes.as_slice())
        .map_err(StatsError::Decode)
}

/// Adds `src` into `dst`.
fn add_into(dst: &mut Histogram<u64>, src: &Histogram<u64>) -> Result<(), StatsError> {
    dst.add(src).map_err(StatsError::Add)
}

#[derive(Debug, Default, Clone, Copy)]
struct ConcurrencyAcc {
    samples: u64,
    min: u64,
    max: u64,
    sum: u128,
}

impl ConcurrencyAcc {
    fn observe(&mut self, n: u64) {
        if self.samples == 0 {
            self.min = n;
            self.max = n;
        } else {
            self.min = self.min.min(n);
            self.max = self.max.max(n);
        }
        self.samples += 1;
        self.sum += u128::from(n);
    }

    #[expect(
        clippy::cast_precision_loss,
        reason = "a mean is approximate by nature"
    )]
    fn finish(self) -> ConcurrencyStats {
        ConcurrencyStats {
            samples: self.samples,
            min: self.min,
            max: self.max,
            mean: if self.samples == 0 {
                0.0
            } else {
                self.sum as f64 / self.samples as f64
            },
        }
    }
}

/// Records metrics into fixed-length intervals on the [`crate::clock::now_ns`]
/// timeline.
///
/// Every `record*` call takes the event time; interval `i` covers
/// `[origin + i·len, origin + (i+1)·len)`. Events older than the current
/// interval are attributed to the current one. All shards of a run should use
/// the same origin so their intervals line up for [`merge_shards`].
#[derive(Debug)]
pub struct IntervalRecorder {
    origin_ns: u64,
    interval_ns: u64,
    index: u64,
    histograms: [Histogram<u64>; Metric::COUNT],
    chunks: u64,
    requests: u64,
    errors: BTreeMap<&'static str, u64>,
    concurrency: ConcurrencyAcc,
    saturated: u64,
    negative: u64,
    completed: Vec<IntervalResult>,
}

impl IntervalRecorder {
    /// Creates a recorder with 1 s intervals starting at `origin_ns`.
    pub fn new(origin_ns: u64) -> Self {
        Self::with_interval(origin_ns, DEFAULT_INTERVAL)
    }

    /// Creates a recorder with a custom interval length (at least 1 ns).
    pub fn with_interval(origin_ns: u64, interval: Duration) -> Self {
        Self {
            origin_ns,
            interval_ns: u64::try_from(interval.as_nanos())
                .unwrap_or(u64::MAX)
                .max(1),
            index: 0,
            histograms: std::array::from_fn(|_| new_histogram()),
            chunks: 0,
            requests: 0,
            errors: BTreeMap::new(),
            concurrency: ConcurrencyAcc::default(),
            saturated: 0,
            negative: 0,
            completed: Vec::new(),
        }
    }

    /// Index of the interval currently being filled.
    pub fn current_index(&self) -> u64 {
        self.index
    }

    /// Intervals closed so far.
    pub fn completed(&self) -> &[IntervalResult] {
        &self.completed
    }

    fn interval_start(&self, index: u64) -> u64 {
        self.origin_ns
            .saturating_add(index.saturating_mul(self.interval_ns))
    }

    /// Closes every interval that ended before `now_ns`. Call it periodically
    /// so idle intervals are closed even without events.
    pub fn tick(&mut self, now_ns: u64) {
        let target = now_ns.saturating_sub(self.origin_ns) / self.interval_ns;
        while self.index < target {
            let end = self.interval_start(self.index + 1);
            self.close(end);
        }
    }

    fn close(&mut self, end_ns: u64) {
        let start_ns = self.interval_start(self.index);
        let mut histograms = BTreeMap::new();
        for metric in Metric::ALL {
            let hist = &mut self.histograms[metric.index()];
            if !hist.is_empty() {
                histograms.insert(metric, encode_histogram(hist));
                hist.reset();
            }
        }
        self.completed.push(IntervalResult {
            index: self.index,
            start_ns,
            duration_ns: end_ns.saturating_sub(start_ns),
            histograms,
            concurrency: std::mem::take(&mut self.concurrency).finish(),
            chunks: std::mem::take(&mut self.chunks),
            requests: std::mem::take(&mut self.requests),
            errors: std::mem::take(&mut self.errors)
                .into_iter()
                .map(|(k, v)| (k.to_owned(), v))
                .collect(),
            saturated: std::mem::take(&mut self.saturated),
            negative: std::mem::take(&mut self.negative),
        });
        self.index += 1;
    }

    /// Records a latency. Values above 120 s are clamped and counted in
    /// [`IntervalResult::saturated`].
    #[inline]
    pub fn record(&mut self, metric: Metric, at_ns: u64, value_ns: u64) {
        self.tick(at_ns);
        if value_ns > HISTOGRAM_HIGH_NS {
            self.saturated += 1;
        }
        self.histograms[metric.index()].saturating_record(value_ns);
    }

    /// Records the latency `end_ns − start_ns`.
    ///
    /// Endpoints from different clocks (e.g. a kernel receive timestamp
    /// converted from `CLOCK_REALTIME`) can be misordered by the conversion
    /// uncertainty. Such a span is recorded as 0 and counted in
    /// [`IntervalResult::negative`] instead of wrapping around.
    #[inline]
    pub fn record_span(&mut self, metric: Metric, at_ns: u64, start_ns: u64, end_ns: u64) {
        if let Some(value_ns) = end_ns.checked_sub(start_ns) {
            self.record(metric, at_ns, value_ns);
        } else {
            self.tick(at_ns);
            self.negative += 1;
            self.histograms[metric.index()].saturating_record(0);
        }
    }

    /// Counts received chunks.
    #[inline]
    pub fn add_chunks(&mut self, at_ns: u64, n: u64) {
        self.tick(at_ns);
        self.chunks += n;
    }

    /// Counts one completed request.
    #[inline]
    pub fn add_request(&mut self, at_ns: u64) {
        self.tick(at_ns);
        self.requests += 1;
    }

    /// Counts one error of the given kind.
    pub fn add_error(&mut self, at_ns: u64, kind: &'static str) {
        self.tick(at_ns);
        *self.errors.entry(kind).or_insert(0) += 1;
    }

    /// Samples the current concurrency (e.g. open streams).
    #[inline]
    pub fn observe_concurrency(&mut self, at_ns: u64, n: u64) {
        self.tick(at_ns);
        self.concurrency.observe(n);
    }

    /// Closes all intervals up to `end_ns`, including the partial last one
    /// (its `duration_ns` is shorter), and returns them.
    pub fn finish(mut self, end_ns: u64) -> Vec<IntervalResult> {
        self.tick(end_ns);
        if end_ns > self.interval_start(self.index) {
            self.close(end_ns);
        }
        self.completed
    }
}

/// Merges the interval lists of several shards by interval index.
///
/// Histograms, counters and errors are summed. Concurrency statistics are
/// summed per field, which is exact for the mean and a bound for min/max.
/// Histograms are merged one interval at a time, so at most one decoded
/// histogram per metric is alive at once.
pub fn merge_shards(shards: Vec<Vec<IntervalResult>>) -> Result<Vec<IntervalResult>, StatsError> {
    let mut by_index: BTreeMap<u64, Vec<IntervalResult>> = BTreeMap::new();
    for interval in shards.into_iter().flatten() {
        by_index.entry(interval.index).or_default().push(interval);
    }
    by_index.into_values().map(merge_interval).collect()
}

/// Merges the non-empty set of shard intervals sharing one index.
fn merge_interval(group: Vec<IntervalResult>) -> Result<IntervalResult, StatsError> {
    let mut hists: BTreeMap<Metric, Histogram<u64>> = BTreeMap::new();
    let mut merged: Option<IntervalResult> = None;
    for mut interval in group {
        for (metric, encoded) in std::mem::take(&mut interval.histograms) {
            let decoded = decode_histogram(&encoded)?;
            match hists.get_mut(&metric) {
                Some(acc) => add_into(acc, &decoded)?,
                None => {
                    hists.insert(metric, decoded);
                }
            }
        }
        match &mut merged {
            None => merged = Some(interval),
            Some(acc) => {
                acc.start_ns = acc.start_ns.min(interval.start_ns);
                acc.duration_ns = acc.duration_ns.max(interval.duration_ns);
                acc.chunks += interval.chunks;
                acc.requests += interval.requests;
                acc.saturated += interval.saturated;
                acc.negative += interval.negative;
                for (kind, n) in interval.errors {
                    *acc.errors.entry(kind).or_insert(0) += n;
                }
                let c = &mut acc.concurrency;
                c.samples = c.samples.max(interval.concurrency.samples);
                c.min += interval.concurrency.min;
                c.max += interval.concurrency.max;
                c.mean += interval.concurrency.mean;
            }
        }
    }
    let mut merged = merged.expect("groups are built from at least one interval");
    merged.histograms = hists
        .into_iter()
        .map(|(metric, hist)| (metric, encode_histogram(&hist)))
        .collect();
    Ok(merged)
}

/// Quantile over sorted `(value, count)` totals, with hdrhistogram's rule:
/// the smallest value whose cumulative count reaches `ceil(q · total)`.
/// `None` when the counts hold fewer than `total` samples (e.g. none).
fn quantile_of(values: &[u64], counts: &[u64], total: u64, q: f64) -> Option<u64> {
    let target = quantile_rank(q, total);
    let mut cumulative = 0;
    values.iter().zip(counts).find_map(|(value, count)| {
        cumulative += count;
        (cumulative >= target).then_some(*value)
    })
}

#[expect(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "mirrors hdrhistogram's value_at_quantile rank computation"
)]
fn quantile_rank(q: f64, total: u64) -> u64 {
    ((q.min(1.0) * total as f64).ceil() as u64).max(1)
}

/// Summarises every metric over the intervals with `index >= warmup`.
pub fn summarize(
    intervals: &[IntervalResult],
    warmup_intervals: u64,
) -> Result<BTreeMap<Metric, MetricSummary>, StatsError> {
    let mut totals: BTreeMap<Metric, Histogram<u64>> = BTreeMap::new();
    for interval in intervals.iter().filter(|i| i.index >= warmup_intervals) {
        for (metric, encoded) in &interval.histograms {
            let decoded = decode_histogram(encoded)?;
            match totals.get_mut(metric) {
                Some(acc) => add_into(acc, &decoded)?,
                None => {
                    totals.insert(*metric, decoded);
                }
            }
        }
    }
    Ok(totals
        .into_iter()
        .map(|(metric, h)| {
            (
                metric,
                MetricSummary {
                    count: h.len(),
                    min_ns: h.min(),
                    mean_ns: h.mean(),
                    p50_ns: h.value_at_quantile(0.50),
                    p90_ns: h.value_at_quantile(0.90),
                    p99_ns: h.value_at_quantile(0.99),
                    p999_ns: h.value_at_quantile(0.999),
                    max_ns: h.max(),
                },
            )
        })
        .collect())
}

/// Options for [`compare_with`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompareOptions {
    /// Number of bootstrap resamples.
    pub resamples: usize,
    /// Moving-block length in intervals, for the resampling within a run.
    pub block_len: usize,
    /// RNG seed of the bootstrap.
    pub seed: u64,
}

impl Default for CompareOptions {
    fn default() -> Self {
        Self {
            resamples: DEFAULT_RESAMPLES,
            block_len: DEFAULT_BLOCK_LEN,
            seed: DEFAULT_SEED,
        }
    }
}

/// Result for one quantile.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QuantileDelta {
    /// The quantile in `[0, 1]`.
    pub quantile: f64,
    /// Point estimate for arm A over all its runs, nanoseconds.
    pub a_ns: u64,
    /// Point estimate for arm B over all its runs, nanoseconds.
    pub b_ns: u64,
    /// `b_ns − a_ns`.
    pub delta_ns: i64,
    /// Lower end of the 95% interval of Δ, built as
    /// [`Comparison::delta_interval`] says.
    pub delta_ci_low_ns: f64,
    /// Upper end of the 95% interval of Δ.
    pub delta_ci_high_ns: f64,
    /// Lower end of the two-level bootstrap's 95% percentile interval of Δ.
    /// Diagnostic only: with few repetitions it is too narrow whenever the
    /// pairs differ more than their runs' own noise explains.
    pub delta_bootstrap_ci_low_ns: f64,
    /// Upper end of the bootstrap percentile interval of Δ.
    pub delta_bootstrap_ci_high_ns: f64,
    /// Lower end of the 95% interval of arm A's quantile: the bootstrap
    /// percentile interval widened around `a_ns` by
    /// [`Comparison::a_interval_scale`].
    pub a_ci_low_ns: f64,
    /// Upper end of the 95% interval of arm A's quantile.
    pub a_ci_high_ns: f64,
    /// Lower end of the unwidened bootstrap percentile interval of arm A's
    /// quantile (diagnostic).
    pub a_bootstrap_ci_low_ns: f64,
    /// Upper end of the unwidened bootstrap percentile interval of arm A.
    pub a_bootstrap_ci_high_ns: f64,
    /// Half-width of arm A's (widened) interval divided by `a_ns`.
    pub a_ci_half_width_ratio: f64,
}

/// How [`QuantileDelta::delta_ci_low_ns`] and `delta_ci_high_ns` were built.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeltaInterval {
    /// Student t interval over the per-pair Δ with `pairs − 1` degrees of
    /// freedom, centred on the pooled point estimate.
    PairedT,
    /// One repetition only: the block bootstrap percentile interval within
    /// its two runs, which leaves out the run-to-run spread.
    WithinRunBootstrap,
}

/// One quantile of a single pair of runs.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PairQuantile {
    /// The quantile in `[0, 1]`.
    pub quantile: f64,
    /// The A run's quantile, nanoseconds.
    pub a_ns: u64,
    /// The B run's quantile, nanoseconds.
    pub b_ns: u64,
    /// `b_ns − a_ns`.
    pub delta_ns: i64,
}

/// The quantiles of one repetition: `a_runs[pair]` against `b_runs[pair]`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PairDelta {
    /// Position of the pair in the compared run lists.
    pub pair: usize,
    /// The runs' [`RunResult::pair_id`], when they carry one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pair_id: Option<String>,
    /// Post-warmup intervals of the A run.
    pub a_intervals: usize,
    /// Post-warmup intervals of the B run.
    pub b_intervals: usize,
    /// Samples of the A run.
    pub a_count: u64,
    /// Samples of the B run.
    pub b_count: u64,
    /// One entry per requested quantile, in request order.
    pub quantiles: Vec<PairQuantile>,
}

/// Output of [`compare`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Comparison {
    /// The compared metric.
    pub metric: Metric,
    /// Bootstrap resamples used.
    pub resamples: usize,
    /// Block length in intervals.
    pub block_len: usize,
    /// RNG seed.
    pub seed: u64,
    /// Repetitions compared, each a pair of one A run and one B run.
    pub pairs: usize,
    /// Only one repetition was given, so every resample drew blocks within
    /// that one pair of runs: the intervals leave out the run-to-run spread
    /// of the load and understate the uncertainty of the measurement.
    pub single_repetition_fallback: bool,
    /// How the intervals of Δ were built.
    pub delta_interval: DeltaInterval,
    /// Factor by which arm A's bootstrap percentile interval is widened
    /// around its point estimate: `t(pairs − 1) / z · √(pairs / (pairs − 1))`,
    /// 1 for a single repetition.
    pub a_interval_scale: f64,
    /// Post-warmup intervals of all A runs.
    pub a_intervals: usize,
    /// Post-warmup intervals of all B runs.
    pub b_intervals: usize,
    /// Samples of all A runs.
    pub a_count: u64,
    /// Samples of all B runs.
    pub b_count: u64,
    /// One entry per requested quantile, in request order.
    pub quantiles: Vec<QuantileDelta>,
    /// Half-width of arm A's (widened) p99 95% interval relative to its p99
    /// (the M0 exit criterion requires `< 0.05`).
    pub baseline_p99_ci_half_width_ratio: f64,
    /// Each repetition's own quantiles and Δ, in pair order; their spread is
    /// the run-to-run variation the intervals account for.
    pub per_pair: Vec<PairDelta>,
}

/// One run's post-warmup intervals as sparse counts over its arm's value
/// axis.
#[derive(Debug)]
struct RunSeries {
    /// Per interval: `(index into the arm's values, count)`.
    intervals: Vec<Vec<(usize, u64)>>,
    /// Samples over all intervals.
    total: u64,
}

impl RunSeries {
    /// Adds every interval into `counts`.
    fn add_all(&self, counts: &mut [u64]) {
        for &(idx, c) in self.intervals.iter().flatten() {
            counts[idx] += c;
        }
    }

    /// Adds one circular block resample of the run, as many intervals long
    /// as the run, into `counts` and returns the samples added.
    ///
    /// Whole blocks keep the correlation between neighbouring intervals
    /// (queues and load drift over seconds), which resampling single
    /// intervals would break and so understate the variance. Blocks wrap
    /// around the end of the run so that every interval is drawn equally
    /// often; plain moving blocks under-draw the first and last
    /// `block_len − 1` intervals.
    fn add_resample(&self, rng: &mut fastrand::Rng, block_len: usize, counts: &mut [u64]) -> u64 {
        let n = self.intervals.len();
        let block = block_len.min(n);
        let mut drawn = 0;
        let mut total = 0;
        while drawn < n {
            let start = rng.usize(..n);
            let take = block.min(n - drawn);
            for offset in 0..take {
                for &(idx, c) in &self.intervals[(start + offset) % n] {
                    counts[idx] += c;
                    total += c;
                }
            }
            drawn += take;
        }
        total
    }
}

/// One arm's runs over a value axis shared by those runs.
#[derive(Debug)]
struct Arm {
    /// Sorted distinct values (bucket highest-equivalent values).
    values: Vec<u64>,
    /// The runs, in pair order; none is empty.
    runs: Vec<RunSeries>,
    /// Samples over all runs.
    total: u64,
}

impl Arm {
    /// Decodes the post-warmup histograms of `metric`; fails if the arm or
    /// any of its runs holds no sample.
    fn build(runs: &[RunResult], metric: Metric, arm: &'static str) -> Result<Self, StatsError> {
        let mut raw: Vec<Vec<Vec<(u64, u64)>>> = Vec::with_capacity(runs.len());
        for run in runs {
            let mut intervals = Vec::new();
            for interval in run
                .intervals
                .iter()
                .filter(|i| i.index >= run.warmup_intervals)
            {
                let mut entries = Vec::new();
                if let Some(encoded) = interval.histograms.get(&metric) {
                    let hist = decode_histogram(encoded)?;
                    entries.extend(
                        hist.iter_recorded()
                            .map(|v| (v.value_iterated_to(), v.count_at_value())),
                    );
                }
                intervals.push(entries);
            }
            raw.push(intervals);
        }
        let mut values: Vec<u64> = raw.iter().flatten().flatten().map(|&(v, _)| v).collect();
        values.sort_unstable();
        values.dedup();
        let runs: Vec<RunSeries> = raw
            .into_iter()
            .map(|intervals| {
                let intervals: Vec<Vec<(usize, u64)>> = intervals
                    .into_iter()
                    .map(|entries| {
                        entries
                            .into_iter()
                            // Every value is on the axis, so the search always hits.
                            .map(|(v, c)| (values.binary_search(&v).unwrap_or_else(|at| at), c))
                            .collect()
                    })
                    .collect();
                let total = intervals.iter().flatten().map(|&(_, c)| c).sum();
                RunSeries { intervals, total }
            })
            .collect();
        let total = runs.iter().map(|run| run.total).sum();
        if total == 0 {
            return Err(StatsError::EmptyArm { arm, metric });
        }
        if let Some(run) = runs.iter().position(|run| run.total == 0) {
            return Err(StatsError::EmptyRun { arm, run, metric });
        }
        Ok(Self {
            values,
            runs,
            total,
        })
    }

    /// Post-warmup intervals over all runs.
    fn intervals(&self) -> usize {
        self.runs.iter().map(|run| run.intervals.len()).sum()
    }

    /// Fills `out` with one value per quantile; `false` if there are no
    /// samples.
    fn quantiles(&self, counts: &[u64], total: u64, qs: &[f64], out: &mut Vec<u64>) -> bool {
        out.clear();
        for &q in qs {
            match quantile_of(&self.values, counts, total, q) {
                Some(v) => out.push(v),
                None => return false,
            }
        }
        true
    }

    /// Quantiles over the full data of `runs`; `None` if they hold no
    /// sample.
    fn quantiles_over<'r>(
        &self,
        runs: impl IntoIterator<Item = &'r RunSeries>,
        qs: &[f64],
    ) -> Option<Vec<u64>> {
        let mut counts = vec![0u64; self.values.len()];
        let mut total = 0;
        for run in runs {
            run.add_all(&mut counts);
            total += run.total;
        }
        let mut out = Vec::with_capacity(qs.len());
        self.quantiles(&counts, total, qs, &mut out).then_some(out)
    }
}

/// Linear-interpolated percentile of sorted data, `p` in `[0, 1]`.
#[expect(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "index arithmetic on small, non-negative values"
)]
fn percentile(sorted: &[f64], p: f64) -> f64 {
    let pos = p * (sorted.len() - 1) as f64;
    let lo = pos.floor() as usize;
    let hi = pos.ceil() as usize;
    sorted[lo] + (sorted[hi] - sorted[lo]) * (pos - lo as f64)
}

/// Two-sided 95% quantile of Student's t distribution with `df` degrees of
/// freedom (`df ≥ 1`).
///
/// Exact to six decimals: tabulated up to 30 degrees of freedom, and above
/// that the Cornish–Fisher expansion in `1 / df`, whose error there is
/// below `1e-6`.
fn student_t_975(df: usize) -> f64 {
    const TABLE: [f64; 30] = [
        12.706_205, 4.302_653, 3.182_446, 2.776_445, 2.570_582, 2.446_912, 2.364_624, 2.306_004,
        2.262_157, 2.228_139, 2.200_985, 2.178_813, 2.160_369, 2.144_787, 2.131_450, 2.119_905,
        2.109_816, 2.100_922, 2.093_024, 2.085_963, 2.079_614, 2.073_873, 2.068_658, 2.063_899,
        2.059_539, 2.055_529, 2.051_831, 2.048_407, 2.045_230, 2.042_272,
    ];
    assert!(df >= 1, "a t interval needs at least one degree of freedom");
    if let Some(&t) = TABLE.get(df - 1) {
        return t;
    }
    let z = Z_975;
    let v = as_f64(u64::try_from(df).unwrap_or(u64::MAX));
    let (z3, z5, z7, z9) = (z.powi(3), z.powi(5), z.powi(7), z.powi(9));
    z + (z3 + z) / (4.0 * v)
        + (5.0 * z5 + 16.0 * z3 + 3.0 * z) / (96.0 * v.powi(2))
        + (3.0 * z7 + 19.0 * z5 + 17.0 * z3 - 15.0 * z) / (384.0 * v.powi(3))
        + (79.0 * z9 + 776.0 * z7 + 1482.0 * z5 - 1920.0 * z3 - 945.0 * z) / (92_160.0 * v.powi(4))
}

/// Factor that widens a percentile interval drawn from `pairs` repetitions
/// to a 95% interval.
///
/// Resampling `R` repetitions with replacement estimates the run-to-run
/// variance with divisor `R` instead of `R − 1` and uses the normal quantile
/// where only `R − 1` degrees of freedom back it; at `R = 3` the percentile
/// interval is barely the range of the three runs and covers about 80%.
/// `t(R − 1) / z · √(R / (R − 1))` undoes both (2.69, 1.58 and 1.22 at 3, 5
/// and 10 repetitions), which brought arm A's coverage to 93–95% in
/// Monte Carlo runs drawn from the pilot data.
fn run_to_run_scale(pairs: usize) -> f64 {
    if pairs < 2 {
        return 1.0;
    }
    let r = as_f64(u64::try_from(pairs).unwrap_or(u64::MAX));
    student_t_975(pairs - 1) / Z_975 * (r / (r - 1.0)).sqrt()
}

/// 95% Student t interval of the mean of `values` (at least two), centred
/// on `centre`.
fn t_interval(centre: f64, values: &[f64]) -> (f64, f64) {
    let n = as_f64(u64::try_from(values.len()).unwrap_or(u64::MAX));
    let mean = values.iter().sum::<f64>() / n;
    let var = values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / (n - 1.0);
    let half = student_t_975(values.len() - 1) * (var / n).sqrt();
    (centre - half, centre + half)
}

/// `b − a` as `i64`, saturating (histogram values stay far below 2^63).
fn signed_delta(b: u64, a: u64) -> i64 {
    let d = i128::from(b) - i128::from(a);
    i64::try_from(d).unwrap_or(if d < 0 { i64::MIN } else { i64::MAX })
}

#[expect(
    clippy::cast_precision_loss,
    reason = "nanosecond latencies are far below 2^53"
)]
fn as_f64(v: u64) -> f64 {
    v as f64
}

#[expect(
    clippy::cast_precision_loss,
    reason = "nanosecond deltas are far below 2^53"
)]
fn delta_f64(v: i64) -> f64 {
    v as f64
}

/// Compares two arms with the default block length and seed.
///
/// `quantiles` are fractions (`0.99`, not `99`).
pub fn compare(
    a_runs: &[RunResult],
    b_runs: &[RunResult],
    metric: Metric,
    quantiles: &[f64],
    resamples: usize,
) -> Result<Comparison, StatsError> {
    compare_with(
        a_runs,
        b_runs,
        metric,
        quantiles,
        &CompareOptions {
            resamples,
            ..CompareOptions::default()
        },
    )
}

/// Compares two arms of paired repetitions and reports `Δ = B − A` per
/// quantile with 95% intervals.
///
/// `a_runs[i]` and `b_runs[i]` form repetition `i`: the two runs that shared
/// its schedule (same seed) and its place in the session. Point estimates
/// come from the full data of each arm, and every pair's own quantiles and
/// Δ are reported in [`Comparison::per_pair`].
///
/// The interval of Δ is a Student t interval over the per-pair Δ with
/// `pairs − 1` degrees of freedom, centred on the pooled point estimate. The
/// spread of the pairs is the run-to-run variation itself, whatever part of
/// it the two runs of a pair share, so the interval neither ignores it nor
/// counts the within-run noise twice; with 3 repetitions that makes it wide
/// (`t(2) = 4.30`), which is the honest price of 2 degrees of freedom.
///
/// Arm A's quantiles, which the baseline criterion judges, have no pairs to
/// difference, so their interval comes from a two-level bootstrap: each
/// resample draws as many repetitions as there are, with replacement and the
/// same draw for both arms, then a circular block resample of the measured
/// intervals within every drawn run, and merges the histograms per arm. The
/// percentile interval of that is too narrow for few repetitions and is
/// widened around the point estimate by [`Comparison::a_interval_scale`].
/// The bootstrap's own intervals of Δ and A stay in the output for
/// diagnosis.
///
/// With one repetition no run-to-run spread can be estimated: Δ falls back
/// to the within-run bootstrap and A is not widened, which
/// [`Comparison::single_repetition_fallback`] flags. The two runs then draw
/// their block starts independently, so load swings they share count as
/// noise on both sides of Δ.
pub fn compare_with(
    a_runs: &[RunResult],
    b_runs: &[RunResult],
    metric: Metric,
    quantiles: &[f64],
    options: &CompareOptions,
) -> Result<Comparison, StatsError> {
    check_inputs(a_runs, b_runs, quantiles, options)?;
    let a = Arm::build(a_runs, metric, "A")?;
    let b = Arm::build(b_runs, metric, "B")?;

    // p99 is always evaluated for the baseline criterion.
    let mut qs = quantiles.to_vec();
    qs.push(0.99);
    let p99_slot = qs.len() - 1;

    let a_point = a
        .quantiles_over(&a.runs, &qs)
        .ok_or(StatsError::EmptyArm { arm: "A", metric })?;
    let b_point = b
        .quantiles_over(&b.runs, &qs)
        .ok_or(StatsError::EmptyArm { arm: "B", metric })?;
    let per_pair = pair_deltas(&a, &b, a_runs, b_runs, metric, quantiles)?;

    let pairs = a.runs.len();
    let dist = bootstrap(&a, &b, &qs, options)?;
    let delta_interval = if pairs >= 2 {
        DeltaInterval::PairedT
    } else {
        DeltaInterval::WithinRunBootstrap
    };
    let a_scale = run_to_run_scale(pairs);
    let entry = |i: usize, quantile: f64| {
        let a_ns = a_point[i];
        let delta_ns = signed_delta(b_point[i], a_ns);
        let bootstrap_delta = dist.delta_percentile(i);
        let (delta_ci_low_ns, delta_ci_high_ns) = match delta_interval {
            DeltaInterval::PairedT => {
                let deltas: Vec<f64> = per_pair
                    .iter()
                    .map(|pair| delta_f64(pair.quantiles[i].delta_ns))
                    .collect();
                t_interval(delta_f64(delta_ns), &deltas)
            }
            DeltaInterval::WithinRunBootstrap => bootstrap_delta,
        };
        let a_bootstrap = dist.a_percentile(i);
        let a_ci = widen(a_bootstrap, a_ns, a_scale);
        QuantileDelta {
            quantile,
            a_ns,
            b_ns: b_point[i],
            delta_ns,
            delta_ci_low_ns,
            delta_ci_high_ns,
            delta_bootstrap_ci_low_ns: bootstrap_delta.0,
            delta_bootstrap_ci_high_ns: bootstrap_delta.1,
            a_ci_low_ns: a_ci.0,
            a_ci_high_ns: a_ci.1,
            a_bootstrap_ci_low_ns: a_bootstrap.0,
            a_bootstrap_ci_high_ns: a_bootstrap.1,
            a_ci_half_width_ratio: half_width_ratio(a_ci, a_ns),
        }
    };

    Ok(Comparison {
        metric,
        resamples: options.resamples,
        block_len: options.block_len,
        seed: options.seed,
        pairs,
        single_repetition_fallback: pairs == 1,
        delta_interval,
        a_interval_scale: a_scale,
        a_intervals: a.intervals(),
        b_intervals: b.intervals(),
        a_count: a.total,
        b_count: b.total,
        quantiles: quantiles
            .iter()
            .enumerate()
            .map(|(i, &q)| entry(i, q))
            .collect(),
        baseline_p99_ci_half_width_ratio: half_width_ratio(
            widen(dist.a_percentile(p99_slot), a_point[p99_slot], a_scale),
            a_point[p99_slot],
        ),
        per_pair,
    })
}

/// Refuses out-of-range options and run lists that do not form pairs.
fn check_inputs(
    a_runs: &[RunResult],
    b_runs: &[RunResult],
    quantiles: &[f64],
    options: &CompareOptions,
) -> Result<(), StatsError> {
    if options.resamples < 2 {
        return Err(StatsError::InvalidOption("resamples must be at least 2"));
    }
    if options.block_len == 0 {
        return Err(StatsError::InvalidOption("block_len must be positive"));
    }
    if quantiles.iter().any(|q| !(0.0..=1.0).contains(q)) {
        return Err(StatsError::InvalidOption("quantiles must be within [0, 1]"));
    }
    if a_runs.len() != b_runs.len() {
        return Err(StatsError::UnpairedRuns {
            a: a_runs.len(),
            b: b_runs.len(),
        });
    }
    if a_runs.is_empty() {
        return Err(StatsError::InvalidOption("each arm needs at least one run"));
    }
    for (pair, (a_run, b_run)) in a_runs.iter().zip(b_runs).enumerate() {
        if let (Some(a_id), Some(b_id)) = (&a_run.pair_id, &b_run.pair_id)
            && a_id != b_id
        {
            return Err(StatsError::PairMismatch {
                pair,
                a: a_id.clone(),
                b: b_id.clone(),
            });
        }
    }
    Ok(())
}

/// Sorted bootstrap values, one list per quantile slot.
#[derive(Debug)]
struct Distributions {
    /// Arm A's quantile.
    a: Vec<Vec<f64>>,
    /// `B − A`.
    delta: Vec<Vec<f64>>,
}

impl Distributions {
    /// 95% percentile interval of arm A's quantile in slot `i`.
    fn a_percentile(&self, i: usize) -> (f64, f64) {
        (percentile(&self.a[i], 0.025), percentile(&self.a[i], 0.975))
    }

    /// 95% percentile interval of Δ in slot `i`.
    fn delta_percentile(&self, i: usize) -> (f64, f64) {
        (
            percentile(&self.delta[i], 0.025),
            percentile(&self.delta[i], 0.975),
        )
    }
}

/// Widens `(lo, hi)` around `point` by `scale`; an end on the wrong side of
/// the point counts as no width.
fn widen((lo, hi): (f64, f64), point: u64, scale: f64) -> (f64, f64) {
    let point = as_f64(point);
    (
        point - scale * (point - lo).max(0.0),
        point + scale * (hi - point).max(0.0),
    )
}

/// Half-width of `(lo, hi)` relative to `point`.
fn half_width_ratio((lo, hi): (f64, f64), point: u64) -> f64 {
    (hi - lo) / 2.0 / as_f64(point.max(1))
}

/// Runs the paired two-level bootstrap over the quantiles `qs`.
fn bootstrap(
    a: &Arm,
    b: &Arm,
    qs: &[f64],
    options: &CompareOptions,
) -> Result<Distributions, StatsError> {
    let pairs = a.runs.len();
    let mut rng = fastrand::Rng::with_seed(options.seed);
    let mut counts_a = vec![0u64; a.values.len()];
    let mut counts_b = vec![0u64; b.values.len()];
    let (mut qa, mut qb) = (Vec::new(), Vec::new());
    let mut a_dist: Vec<Vec<f64>> = vec![Vec::with_capacity(options.resamples); qs.len()];
    let mut d_dist: Vec<Vec<f64>> = vec![Vec::with_capacity(options.resamples); qs.len()];
    for _ in 0..options.resamples {
        counts_a.fill(0);
        counts_b.fill(0);
        let (mut total_a, mut total_b) = (0, 0);
        // One draw of a repetition serves both arms, so a pair's shared
        // schedule and moment in the session stay on both sides of Δ.
        for _ in 0..pairs {
            let pair = rng.usize(..pairs);
            total_a += a.runs[pair].add_resample(&mut rng, options.block_len, &mut counts_a);
            total_b += b.runs[pair].add_resample(&mut rng, options.block_len, &mut counts_b);
        }
        if !a.quantiles(&counts_a, total_a, qs, &mut qa) {
            return Err(StatsError::EmptyResample("A"));
        }
        if !b.quantiles(&counts_b, total_b, qs, &mut qb) {
            return Err(StatsError::EmptyResample("B"));
        }
        for i in 0..qs.len() {
            a_dist[i].push(as_f64(qa[i]));
            d_dist[i].push(as_f64(qb[i]) - as_f64(qa[i]));
        }
    }
    for dist in a_dist.iter_mut().chain(d_dist.iter_mut()) {
        dist.sort_unstable_by(f64::total_cmp);
    }
    Ok(Distributions {
        a: a_dist,
        delta: d_dist,
    })
}

/// Every pair's own quantiles, from the full data of its two runs.
fn pair_deltas(
    a: &Arm,
    b: &Arm,
    a_runs: &[RunResult],
    b_runs: &[RunResult],
    metric: Metric,
    quantiles: &[f64],
) -> Result<Vec<PairDelta>, StatsError> {
    a.runs
        .iter()
        .zip(&b.runs)
        .zip(a_runs.iter().zip(b_runs))
        .enumerate()
        .map(|(pair, ((a_series, b_series), (a_run, b_run)))| {
            let a_q = a
                .quantiles_over([a_series], quantiles)
                .ok_or(StatsError::EmptyRun {
                    arm: "A",
                    run: pair,
                    metric,
                })?;
            let b_q = b
                .quantiles_over([b_series], quantiles)
                .ok_or(StatsError::EmptyRun {
                    arm: "B",
                    run: pair,
                    metric,
                })?;
            Ok(PairDelta {
                pair,
                pair_id: a_run.pair_id.clone().or_else(|| b_run.pair_id.clone()),
                a_intervals: a_series.intervals.len(),
                b_intervals: b_series.intervals.len(),
                a_count: a_series.total,
                b_count: b_series.total,
                quantiles: quantiles
                    .iter()
                    .zip(a_q.iter().zip(&b_q))
                    .map(|(&quantile, (&a_ns, &b_ns))| PairQuantile {
                        quantile,
                        a_ns,
                        b_ns,
                        delta_ns: signed_delta(b_ns, a_ns),
                    })
                    .collect(),
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fingerprint::Fingerprint;
    use crate::result::{SCHEMA_VERSION, Validity};

    const SEC: u64 = 1_000_000_000;

    #[test]
    fn metric_names_round_trip() {
        for m in Metric::ALL {
            assert_eq!(m.name().parse::<Metric>().unwrap(), m);
            assert_eq!(
                serde_json::to_string(&m).unwrap(),
                format!("\"{}\"", m.name())
            );
        }
        assert_eq!("ttft_reused".parse::<Metric>().unwrap(), Metric::TtftReused);
        assert!("nope".parse::<Metric>().is_err());
    }

    #[test]
    fn histogram_codec_round_trip() {
        let mut h = new_histogram();
        for v in [0, 1, 999, 1_000_000, HISTOGRAM_HIGH_NS] {
            h.record(v).unwrap();
        }
        let decoded = decode_histogram(&encode_histogram(&h)).unwrap();
        assert_eq!(decoded, h);
        assert!(decode_histogram("!!!").is_err());
        assert!(decode_histogram("AAAA").is_err());
    }

    #[test]
    fn recorder_rotates_and_counts() {
        let origin = 10 * SEC;
        let mut r = IntervalRecorder::new(origin);
        r.record(Metric::ChunkLatency, origin + 1, 500);
        r.add_chunks(origin + 2, 3);
        r.add_error(origin + 3, "reset");
        r.observe_concurrency(origin + 4, 10);
        r.observe_concurrency(origin + 5, 20);
        // Skips interval 1 entirely.
        r.record(Metric::ChunkLatency, origin + 2 * SEC + 5, 700);
        r.record(Metric::Ttft, origin + 2 * SEC + 6, HISTOGRAM_HIGH_NS + 1);
        r.record_span(Metric::ChunkWire, origin + 2 * SEC + 6, 1_000, 1_250);
        r.record_span(Metric::ChunkWire, origin + 2 * SEC + 6, 1_250, 1_000);
        r.add_request(origin + 2 * SEC + 7);
        assert_eq!(r.current_index(), 2);
        let intervals = r.finish(origin + 2 * SEC + SEC / 2);
        assert_eq!(intervals.len(), 3);

        let first = &intervals[0];
        assert_eq!(
            (first.index, first.start_ns, first.duration_ns),
            (0, origin, SEC)
        );
        assert_eq!(first.chunks, 3);
        assert_eq!(first.errors.get("reset"), Some(&1));
        assert_eq!(first.concurrency.min, 10);
        assert_eq!(first.concurrency.max, 20);
        assert!((first.concurrency.mean - 15.0).abs() < 1e-9);
        let h = decode_histogram(&first.histograms[&Metric::ChunkLatency]).unwrap();
        assert_eq!(h.len(), 1);

        assert!(intervals[1].histograms.is_empty());
        assert_eq!(intervals[1].chunks, 0);

        let last = &intervals[2];
        assert_eq!(last.duration_ns, SEC / 2);
        assert_eq!(last.requests, 1);
        assert_eq!(last.saturated, 1);
        assert_eq!(last.negative, 1);
        let wire = decode_histogram(&last.histograms[&Metric::ChunkWire]).unwrap();
        assert_eq!((wire.len(), wire.min(), wire.max()), (2, 0, 250));
        let ttft = decode_histogram(&last.histograms[&Metric::Ttft]).unwrap();
        assert!(ttft.equivalent(ttft.max(), HISTOGRAM_HIGH_NS));
    }

    #[test]
    fn merge_and_summarize() {
        let mut a = IntervalRecorder::new(0);
        let mut b = IntervalRecorder::new(0);
        for i in 0..100 {
            a.record(Metric::EmitLag, i, 1_000);
            b.record(Metric::EmitLag, i, 3_000);
        }
        a.observe_concurrency(1, 5);
        b.observe_concurrency(1, 7);
        a.record_span(Metric::ChunkWire, 2, 10, 5);
        b.record_span(Metric::ChunkWire, 2, 10, 5);
        b.record(Metric::EmitLag, SEC + 1, 9_000);
        let merged = merge_shards(vec![a.finish(2 * SEC), b.finish(2 * SEC)]).unwrap();
        assert_eq!(merged.len(), 2);
        assert_eq!(merged.iter().map(|i| i.index).collect::<Vec<_>>(), [0, 1]);
        assert!((merged[0].concurrency.mean - 12.0).abs() < 1e-9);
        assert_eq!(merged[0].negative, 2);
        let h = decode_histogram(&merged[0].histograms[&Metric::EmitLag]).unwrap();
        assert_eq!(h.len(), 200);
        let h = decode_histogram(&merged[1].histograms[&Metric::EmitLag]).unwrap();
        assert_eq!(h.len(), 1);

        let all = summarize(&merged, 0).unwrap();
        let s = &all[&Metric::EmitLag];
        assert_eq!(s.count, 201);
        // Rank ceil(0.5 * 201) = 101 is the first 3000 ns sample.
        assert!(s.p50_ns >= 3_000 && s.p50_ns < 3_004, "{s:?}");
        assert!(s.max_ns >= 9_000 && s.max_ns < 9_010);
        let post = summarize(&merged, 1).unwrap();
        assert_eq!(post[&Metric::EmitLag].count, 1);
    }

    /// A run whose post-warmup intervals hold 1000 samples each of
    /// `base + shift` plus noise that depends only on `seed` and `base`;
    /// warmup intervals hold absurd values that must be ignored.
    fn shifted_run(seed: u64, base: u64, shift: u64, intervals: u64, warmup: u64) -> RunResult {
        let mut rng = fastrand::Rng::with_seed(seed);
        let mut r = IntervalRecorder::new(0);
        for i in 0..intervals {
            for k in 0..1000 {
                let at = i * SEC + k;
                let value = if i < warmup {
                    50_000_000
                } else {
                    base + shift + rng.u64(0..base / 5) + if k % 100 == 0 { base } else { 0 }
                };
                r.record(Metric::ChunkLatency, at, value);
            }
        }
        let intervals = r.finish(intervals * SEC);
        RunResult {
            schema_version: SCHEMA_VERSION,
            tool: "test".into(),
            scenario: "s1".into(),
            label: "x".into(),
            pair_id: None,
            params: serde_json::Value::Null,
            fingerprint: Fingerprint::collect(),
            started_unix_ms: 0,
            warmup_intervals: warmup,
            summary: summarize(&intervals, warmup).unwrap(),
            intervals,
            validity: Validity::default(),
        }
    }

    fn synthetic_run(seed: u64, base: u64, intervals: u64, warmup: u64) -> RunResult {
        shifted_run(seed, base, 0, intervals, warmup)
    }

    /// One arm of runs with the given run-level shifts, seeds from
    /// `first_seed` on.
    fn arm(first_seed: u64, shifts: &[u64]) -> Vec<RunResult> {
        (first_seed..)
            .zip(shifts)
            .map(|(seed, &shift)| shifted_run(seed, 10_000, shift, 30, 5))
            .collect()
    }

    /// HDR buckets are 8 to 16 ns wide at the test values, so a quantile may
    /// land a bucket or two away from the exact value.
    const BUCKET_SLACK_NS: f64 = 32.0;

    fn covers(q: &QuantileDelta, delta_ns: f64) -> bool {
        q.delta_ci_low_ns - BUCKET_SLACK_NS <= delta_ns
            && delta_ns <= q.delta_ci_high_ns + BUCKET_SLACK_NS
    }

    fn ci_width(q: &QuantileDelta) -> f64 {
        q.delta_ci_high_ns - q.delta_ci_low_ns
    }

    const TEST_OPTIONS: CompareOptions = CompareOptions {
        resamples: 400,
        block_len: DEFAULT_BLOCK_LEN,
        seed: DEFAULT_SEED,
    };

    #[test]
    #[expect(clippy::cast_precision_loss, reason = "test deltas are small")]
    fn compare_is_deterministic_and_detects_shift() {
        let a: Vec<_> = (0..3).map(|s| synthetic_run(s, 10_000, 40, 5)).collect();
        let b: Vec<_> = (10..13).map(|s| synthetic_run(s, 11_000, 40, 5)).collect();
        let opts = CompareOptions {
            resamples: 300,
            ..CompareOptions::default()
        };
        let first = compare_with(&a, &b, Metric::ChunkLatency, &[0.5, 0.99], &opts).unwrap();
        let second = compare_with(&a, &b, Metric::ChunkLatency, &[0.5, 0.99], &opts).unwrap();
        assert_eq!(first, second);
        assert_eq!(first.pairs, 3);
        assert!(!first.single_repetition_fallback);
        assert_eq!(first.a_intervals, 105);
        assert_eq!(first.a_count, 105_000);
        assert_eq!(first.per_pair.len(), 3);
        assert_eq!(first.per_pair[2].pair, 2);
        assert_eq!(first.per_pair[2].a_intervals, 35);
        assert_eq!(first.per_pair[2].a_count, 35_000);
        assert_eq!(first.per_pair[2].quantiles.len(), 2);

        let median = &first.quantiles[0];
        assert!(median.a_ns >= 10_000 && median.a_ns < 12_000, "{median:?}");
        assert!(
            median.delta_ns > 500 && median.delta_ns < 1_500,
            "{median:?}"
        );
        assert!(median.delta_ci_low_ns <= median.delta_ns as f64);
        assert!(median.delta_ci_high_ns >= median.delta_ns as f64);
        assert!(median.delta_ci_low_ns > 0.0);
        assert!(first.baseline_p99_ci_half_width_ratio < 0.05);
        for pair in &first.per_pair {
            let d = pair.quantiles[0].delta_ns;
            assert!(d > 500 && d < 1_500, "{pair:?}");
        }

        assert_eq!(first.delta_interval, DeltaInterval::PairedT);
        assert!((first.a_interval_scale - 2.689).abs() < 1e-3, "{first:?}");
        // The t interval sits on the per-pair spread: centred on the point
        // estimate, with half-width t(2) · sd / √3.
        let deltas: Vec<f64> = first
            .per_pair
            .iter()
            .map(|p| delta_f64(p.quantiles[0].delta_ns))
            .collect();
        let mean = deltas.iter().sum::<f64>() / 3.0;
        let sd = (deltas.iter().map(|d| (d - mean).powi(2)).sum::<f64>() / 2.0).sqrt();
        let half = 4.302_653 * sd / 3f64.sqrt();
        assert!((median.delta_ci_low_ns - (median.delta_ns as f64 - half)).abs() < 1e-6);
        assert!((median.delta_ci_high_ns - (median.delta_ns as f64 + half)).abs() < 1e-6);
        // Arm A's interval is the bootstrap one, widened around the point.
        let a_ns = median.a_ns as f64;
        assert!(
            (a_ns
                - median.a_ci_low_ns
                - first.a_interval_scale * (a_ns - median.a_bootstrap_ci_low_ns).max(0.0))
            .abs()
                < 1e-6,
            "{median:?}"
        );
        assert!(median.a_ci_high_ns >= median.a_bootstrap_ci_high_ns);

        // Another bootstrap seed moves only the bootstrap intervals.
        let other_seed = compare_with(
            &a,
            &b,
            Metric::ChunkLatency,
            &[0.5, 0.99],
            &CompareOptions { seed: 1, ..opts },
        )
        .unwrap();
        let moved = &other_seed.quantiles[0];
        assert_eq!(moved.delta_ns, median.delta_ns);
        assert_eq!(other_seed.per_pair, first.per_pair);
        assert_eq!(
            (moved.delta_ci_low_ns, moved.delta_ci_high_ns),
            (median.delta_ci_low_ns, median.delta_ci_high_ns)
        );
        let bootstrap_ends = |c: &Comparison| -> Vec<f64> {
            c.quantiles
                .iter()
                .flat_map(|q| {
                    [
                        q.delta_bootstrap_ci_low_ns,
                        q.delta_bootstrap_ci_high_ns,
                        q.a_bootstrap_ci_low_ns,
                        q.a_bootstrap_ci_high_ns,
                    ]
                })
                .collect()
        };
        assert_ne!(bootstrap_ends(&other_seed), bootstrap_ends(&first));
    }

    #[test]
    fn t_quantiles_and_run_to_run_scale() {
        assert!((student_t_975(1) - 12.706_205).abs() < 1e-9);
        assert!((student_t_975(4) - 2.776_445).abs() < 1e-9);
        // The expansion takes over seamlessly past the table.
        assert!((student_t_975(31) - 2.039_513).abs() < 2e-6);
        assert!((student_t_975(40) - 2.021_075).abs() < 2e-6);
        assert!((student_t_975(100) - 1.983_972).abs() < 2e-6);
        assert!((student_t_975(1_000_000) - Z_975).abs() < 1e-5);
        assert!((run_to_run_scale(1) - 1.0).abs() < f64::EPSILON);
        for (pairs, scale) in [(3, 2.689), (5, 1.584), (10, 1.216)] {
            assert!(
                (run_to_run_scale(pairs) - scale).abs() < 1e-3,
                "{pairs}: {}",
                run_to_run_scale(pairs)
            );
        }
    }

    /// A run of 12 one-second intervals of 100 samples each, uniform over
    /// `[CAL_BASE + level, CAL_BASE + level + CAL_SPREAD)`, recorded into
    /// histograms bounded just above the values so the many runs stay cheap.
    fn level_run(rng: &mut fastrand::Rng, level: f64) -> RunResult {
        #[expect(
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss,
            reason = "levels stay within a few thousand ns of the base"
        )]
        let low = (as_f64(CAL_BASE) + level).round() as u64;
        let mut hist = Histogram::<u64>::new_with_bounds(1, 1 << 15, HISTOGRAM_SIGFIG).unwrap();
        let intervals = (0..12)
            .map(|index| {
                hist.reset();
                for _ in 0..100 {
                    hist.record(low + rng.u64(..CAL_SPREAD)).unwrap();
                }
                IntervalResult {
                    index,
                    start_ns: index * SEC,
                    duration_ns: SEC,
                    histograms: BTreeMap::from([(Metric::ChunkLatency, encode_histogram(&hist))]),
                    concurrency: ConcurrencyStats::default(),
                    chunks: 100,
                    requests: 0,
                    errors: BTreeMap::new(),
                    saturated: 0,
                    negative: 0,
                }
            })
            .collect();
        RunResult {
            schema_version: SCHEMA_VERSION,
            tool: "test".into(),
            scenario: "s1".into(),
            label: "x".into(),
            pair_id: None,
            params: serde_json::Value::Null,
            fingerprint: Fingerprint::collect(),
            started_unix_ms: 0,
            warmup_intervals: 0,
            summary: BTreeMap::new(),
            intervals,
            validity: Validity::default(),
        }
    }

    const CAL_BASE: u64 = 8_000;
    const CAL_SPREAD: u64 = 2_000;

    /// Standard normal draw (Box–Muller).
    fn normal(rng: &mut fastrand::Rng) -> f64 {
        let u = 1.0 - rng.f64();
        (-2.0 * u.ln()).sqrt() * (std::f64::consts::TAU * rng.f64()).cos()
    }

    /// Coverage of the Δ and arm A intervals over one set of A/A experiments.
    #[derive(Debug)]
    struct Calibration {
        /// Share of Δ intervals (p50 and p90) that exclude 0.
        delta_false_positive: f64,
        /// Share of arm A's p50 intervals that cover the true median.
        a_coverage: f64,
        /// The same for the unwidened bootstrap interval.
        a_bootstrap_coverage: f64,
    }

    /// Runs `experiments` A/A comparisons of `pairs` repetitions each. Every
    /// run's level is a part its pair shares plus a part of its own, both
    /// normal with sd 300 ns, against a within-run standard error of the
    /// median near 10 ns: the run-to-run spread dominates, as it does for
    /// the chunk metrics of the pilot, where pooled intervals claimed 95%
    /// and excluded 0 in 40 of 108 A/A intervals.
    #[expect(clippy::cast_precision_loss, reason = "counts are small")]
    fn calibrate(pairs: usize, experiments: u64) -> Calibration {
        // Levels are symmetric around 0, so the median of the whole
        // population is the middle of the uniform spread; HDR reports the
        // top of the bucket holding it, 8 ns wide here.
        let true_median = (CAL_BASE + CAL_SPREAD / 2) as f64;
        let mut rng = fastrand::Rng::with_seed(0x6361_6c69_6272_6174);
        let (mut excluded, mut covered, mut covered_bootstrap) = (0u32, 0u32, 0u32);
        for experiment in 0..experiments {
            let (mut a, mut b) = (Vec::new(), Vec::new());
            for _ in 0..pairs {
                let shared = 300.0 * normal(&mut rng);
                let (a_level, b_level) = (
                    shared + 300.0 * normal(&mut rng),
                    shared + 300.0 * normal(&mut rng),
                );
                a.push(level_run(&mut rng, a_level));
                b.push(level_run(&mut rng, b_level));
            }
            let options = CompareOptions {
                resamples: 200,
                block_len: 3,
                seed: experiment,
            };
            let c = compare_with(&a, &b, Metric::ChunkLatency, &[0.5, 0.9], &options).unwrap();
            for q in &c.quantiles {
                excluded += u32::from(q.delta_ci_low_ns > 0.0 || q.delta_ci_high_ns < 0.0);
            }
            let median = &c.quantiles[0];
            let slack = 8.0;
            covered += u32::from(
                median.a_ci_low_ns - slack <= true_median
                    && true_median <= median.a_ci_high_ns + slack,
            );
            covered_bootstrap += u32::from(
                median.a_bootstrap_ci_low_ns - slack <= true_median
                    && true_median <= median.a_bootstrap_ci_high_ns + slack,
            );
        }
        let n = experiments as f64;
        Calibration {
            delta_false_positive: f64::from(excluded) / (2.0 * n),
            a_coverage: f64::from(covered) / n,
            a_bootstrap_coverage: f64::from(covered_bootstrap) / n,
        }
    }

    /// The intervals must hold their nominal 95% in A/A experiments with a
    /// dominant run-to-run spread; the unwidened bootstrap interval of A,
    /// like the pooled one before it, does not.
    #[test]
    fn aa_intervals_hold_their_nominal_coverage() {
        for pairs in [3, 5] {
            let cal = calibrate(pairs, 200);
            assert!(
                (0.015..=0.09).contains(&cal.delta_false_positive),
                "{pairs} pairs: {cal:?}"
            );
            assert!(
                (0.9..=0.99).contains(&cal.a_coverage),
                "{pairs} pairs: {cal:?}"
            );
            assert!(cal.a_bootstrap_coverage < 0.9, "{pairs} pairs: {cal:?}");
        }
    }

    /// Runs that differ in their own level, independently in the two arms,
    /// must widen the interval of Δ to the run-to-run spread and still cover
    /// the true Δ; pooling their intervals would not.
    #[test]
    fn between_run_shifts_widen_the_interval_and_cover_the_true_delta() {
        const TRUE_DELTA: f64 = 1_000.0;
        let steady_a = arm(0, &[0; 5]);
        let steady_b = arm(10, &[1_000; 5]);
        // The same set of run levels in both arms, in another order, so the
        // pooled distributions differ by exactly the true Δ while every
        // pair's own Δ is off by up to 3 µs.
        let a_shifts = [0, 2_000, 4_000, 1_000, 3_000];
        let b_shifts = [3_000, 0, 1_000, 4_000, 2_000].map(|s| s + 1_000);
        let shifted_a = arm(0, &a_shifts);
        let shifted_b = arm(10, &b_shifts);

        let steady = compare_with(
            &steady_a,
            &steady_b,
            Metric::ChunkLatency,
            &[0.5, 0.9],
            &TEST_OPTIONS,
        )
        .unwrap();
        let shifted = compare_with(
            &shifted_a,
            &shifted_b,
            Metric::ChunkLatency,
            &[0.5, 0.9],
            &TEST_OPTIONS,
        )
        .unwrap();
        for (s, w) in steady.quantiles.iter().zip(&shifted.quantiles) {
            assert!(covers(s, TRUE_DELTA), "{s:?}");
            assert!(covers(w, TRUE_DELTA), "{w:?}");
            assert!(ci_width(s) < 400.0, "{s:?}");
            assert!(ci_width(w) > 10.0 * ci_width(s), "{w:?} vs {s:?}");
            assert!(ci_width(w) > 2_000.0, "{w:?}");
        }
        // The baseline interval grows with A's own run-to-run spread, past
        // the M0 limit that the steady runs meet easily.
        assert!(steady.baseline_p99_ci_half_width_ratio < 0.01, "{steady:?}");
        assert!(
            shifted.baseline_p99_ci_half_width_ratio > 0.05,
            "{shifted:?}"
        );
        // Every pair reports its own Δ: the true Δ plus its level difference.
        for (pair, (a_shift, b_shift)) in shifted.per_pair.iter().zip(a_shifts.iter().zip(b_shifts))
        {
            let expected = i64::try_from(b_shift).unwrap() - i64::try_from(*a_shift).unwrap();
            let median = &pair.quantiles[0];
            assert!(
                (median.delta_ns - expected).abs() < 150,
                "pair {}: {median:?}, expected {expected}",
                pair.pair
            );
        }
    }

    /// A pair that shares its level must keep Δ tight however far the pairs
    /// are from each other, which holds only while both arms draw the same
    /// repetitions.
    #[test]
    fn pairs_stay_together() {
        let levels = [0, 5_000, 2_000, 8_000, 1_000];
        let a = arm(0, &levels);
        let b = arm(10, &levels.map(|s| s + 1_000));
        let paired =
            compare_with(&a, &b, Metric::ChunkLatency, &[0.5, 0.9], &TEST_OPTIONS).unwrap();
        for q in &paired.quantiles {
            assert!(covers(q, 1_000.0), "{q:?}");
            assert!(ci_width(q) < 400.0, "{q:?}");
        }
        for pair in &paired.per_pair {
            let d = pair.quantiles[0].delta_ns;
            assert!((d - 1_000).abs() < 150, "{pair:?}");
        }
        // The runs themselves differ a lot: A's own interval is wide.
        assert!(
            paired.baseline_p99_ci_half_width_ratio > 0.05,
            "{}",
            paired.baseline_p99_ci_half_width_ratio
        );

        // The same runs matched to the wrong partners lose that.
        let mut rotated = b.clone();
        rotated.rotate_left(1);
        let mismatched = compare_with(
            &a,
            &rotated,
            Metric::ChunkLatency,
            &[0.5, 0.9],
            &TEST_OPTIONS,
        )
        .unwrap();
        for (p, m) in paired.quantiles.iter().zip(&mismatched.quantiles) {
            assert_eq!(p.delta_ns, m.delta_ns);
            assert!(ci_width(m) > 10.0 * ci_width(p), "{m:?} vs {p:?}");
        }
    }

    #[test]
    fn a_single_repetition_falls_back_to_within_run_blocks() {
        let a = arm(0, &[0]);
        let b = arm(10, &[1_000]);
        let single = compare_with(&a, &b, Metric::ChunkLatency, &[0.5], &TEST_OPTIONS).unwrap();
        assert!(single.single_repetition_fallback);
        assert_eq!(single.pairs, 1);
        assert_eq!(single.per_pair.len(), 1);
        assert_eq!(
            single.per_pair[0].quantiles[0].delta_ns,
            single.quantiles[0].delta_ns
        );
        assert!(covers(&single.quantiles[0], 1_000.0), "{single:?}");

        let two = compare_with(
            &arm(0, &[0, 0]),
            &arm(10, &[1_000, 1_000]),
            Metric::ChunkLatency,
            &[0.5],
            &TEST_OPTIONS,
        )
        .unwrap();
        assert!(!two.single_repetition_fallback);
        assert_eq!(two.pairs, 2);
    }

    #[test]
    fn pairing_ids_must_agree() {
        let mut a = arm(0, &[0, 0]);
        let mut b = arm(10, &[0, 0]);
        for (i, (x, y)) in a.iter_mut().zip(&mut b).enumerate() {
            x.pair_id = Some(format!("r{i}"));
            y.pair_id = Some(format!("r{i}"));
        }
        let ok = compare_with(&a, &b, Metric::ChunkLatency, &[0.5], &TEST_OPTIONS).unwrap();
        assert_eq!(ok.per_pair[1].pair_id.as_deref(), Some("r1"));

        b.swap(0, 1);
        assert!(matches!(
            compare_with(&a, &b, Metric::ChunkLatency, &[0.5], &TEST_OPTIONS),
            Err(StatsError::PairMismatch { pair: 0, .. })
        ));
        assert!(matches!(
            compare_with(&a, &b[..1], Metric::ChunkLatency, &[0.5], &TEST_OPTIONS),
            Err(StatsError::UnpairedRuns { a: 2, b: 1 })
        ));
    }

    #[test]
    fn compare_rejects_empty_and_bad_options() {
        let a = vec![synthetic_run(1, 10_000, 3, 0)];
        assert!(matches!(
            compare(&a, &a, Metric::Ttft, &[0.5], 100),
            Err(StatsError::EmptyArm { arm: "A", .. })
        ));
        assert!(matches!(
            compare(&a, &a, Metric::ChunkLatency, &[1.5], 100),
            Err(StatsError::InvalidOption(_))
        ));
        assert!(matches!(
            compare(&a, &a, Metric::ChunkLatency, &[0.5], 1),
            Err(StatsError::InvalidOption(_))
        ));
        assert!(matches!(
            compare(&[], &[], Metric::ChunkLatency, &[0.5], 100),
            Err(StatsError::InvalidOption(_))
        ));
        // A run left with nothing after its warmup cannot stand for its pair.
        let two = vec![
            synthetic_run(1, 10_000, 3, 0),
            synthetic_run(2, 10_000, 3, 3),
        ];
        assert!(matches!(
            compare(&two, &two, Metric::ChunkLatency, &[0.5], 100),
            Err(StatsError::EmptyRun {
                arm: "A",
                run: 1,
                ..
            })
        ));
    }

    #[test]
    fn runs_recorded_before_ttft_reused_still_load_and_compare() {
        // Result files written before the metric existed (M0) have no
        // `ttft_reused` key: they still load and compare on the metrics they
        // hold, and a comparison of the new metric names what is missing.
        let json = serde_json::to_string(&synthetic_run(1, 10_000, 3, 0)).unwrap();
        assert!(!json.contains("ttft_reused"));
        let old: Vec<RunResult> = vec![serde_json::from_str(&json).unwrap()];
        compare(&old, &old, Metric::ChunkLatency, &[0.5], 100).unwrap();
        let err = compare(&old, &old, Metric::TtftReused, &[0.5], 100).unwrap_err();
        assert!(
            matches!(
                err,
                StatsError::EmptyArm {
                    arm: "A",
                    metric: Metric::TtftReused
                }
            ),
            "{err}"
        );
        assert_eq!(
            err.to_string(),
            "arm A has no ttft_reused samples after warmup"
        );
    }
}
