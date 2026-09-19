//! Per-interval latency histograms and block-bootstrap comparison of runs.
//!
//! Every latency is recorded in nanoseconds into an HDR histogram covering
//! 1 ns to 120 s with 3 significant digits. An [`IntervalRecorder`] keeps one
//! live set of histograms and closes an interval (1 s by default) by encoding
//! it with the V2 format plus base64, so memory does not grow with the length
//! of a run. [`compare`] turns runs of two arms into bootstrap confidence
//! intervals for quantile differences.

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
    pub const COUNT: usize = 6;
    /// All metrics in index order.
    pub const ALL: [Self; Self::COUNT] = [
        Self::Ttft,
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
    /// Moving-block length in intervals.
    pub block_len: usize,
    /// RNG seed; arm B uses a derived seed so the arms resample independently.
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

/// Bootstrap result for one quantile.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QuantileDelta {
    /// The quantile in `[0, 1]`.
    pub quantile: f64,
    /// Point estimate for arm A, nanoseconds.
    pub a_ns: u64,
    /// Point estimate for arm B, nanoseconds.
    pub b_ns: u64,
    /// `b_ns − a_ns`.
    pub delta_ns: i64,
    /// Lower end of the 95% percentile interval of Δ.
    pub delta_ci_low_ns: f64,
    /// Upper end of the 95% percentile interval of Δ.
    pub delta_ci_high_ns: f64,
    /// Lower end of the 95% interval of arm A's quantile.
    pub a_ci_low_ns: f64,
    /// Upper end of the 95% interval of arm A's quantile.
    pub a_ci_high_ns: f64,
    /// Half-width of arm A's interval divided by `a_ns`.
    pub a_ci_half_width_ratio: f64,
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
    /// Post-warmup intervals pooled for arm A.
    pub a_intervals: usize,
    /// Post-warmup intervals pooled for arm B.
    pub b_intervals: usize,
    /// Samples pooled for arm A.
    pub a_count: u64,
    /// Samples pooled for arm B.
    pub b_count: u64,
    /// One entry per requested quantile, in request order.
    pub quantiles: Vec<QuantileDelta>,
    /// Half-width of arm A's p99 95% interval relative to its p99 (the M0
    /// exit criterion requires `< 0.05`).
    pub baseline_p99_ci_half_width_ratio: f64,
}

/// One arm's post-warmup intervals as sparse counts over a shared value axis.
struct Arm {
    /// Sorted distinct values (bucket highest-equivalent values).
    values: Vec<u64>,
    /// Per interval: `(index into values, count)`.
    series: Vec<Vec<(usize, u64)>>,
    /// Valid block starts with their lengths; blocks never span two runs.
    blocks: Vec<(usize, usize)>,
    total: u64,
}

impl Arm {
    fn build(runs: &[RunResult], metric: Metric, block_len: usize) -> Result<Self, StatsError> {
        let mut raw: Vec<Vec<(u64, u64)>> = Vec::new();
        let mut blocks = Vec::new();
        for run in runs {
            let start = raw.len();
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
                raw.push(entries);
            }
            let len = raw.len() - start;
            if len > 0 {
                let block = block_len.min(len);
                blocks.extend((start..=start + len - block).map(|s| (s, block)));
            }
        }
        let mut values: Vec<u64> = raw.iter().flatten().map(|&(v, _)| v).collect();
        values.sort_unstable();
        values.dedup();
        let series = raw
            .into_iter()
            .map(|entries| {
                entries
                    .into_iter()
                    // Every value is on the axis, so the search always hits.
                    .map(|(v, c)| (values.binary_search(&v).unwrap_or_else(|at| at), c))
                    .collect()
            })
            .collect::<Vec<Vec<_>>>();
        let total = series.iter().flatten().map(|&(_, c)| c).sum();
        Ok(Self {
            values,
            series,
            blocks,
            total,
        })
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

    /// Point estimates over all pooled intervals; `None` if the arm is empty.
    fn pooled(&self, qs: &[f64]) -> Option<Vec<u64>> {
        let mut counts = vec![0u64; self.values.len()];
        for &(idx, c) in self.series.iter().flatten() {
            counts[idx] += c;
        }
        let mut out = Vec::new();
        self.quantiles(&counts, self.total, qs, &mut out)
            .then_some(out)
    }

    /// One moving-block resample; returns `false` if it drew no samples.
    fn resample(
        &self,
        rng: &mut fastrand::Rng,
        counts: &mut [u64],
        qs: &[f64],
        out: &mut Vec<u64>,
    ) -> bool {
        counts.fill(0);
        let n = self.series.len();
        let mut drawn = 0;
        let mut total = 0;
        while drawn < n {
            let (start, len) = self.blocks[rng.usize(..self.blocks.len())];
            let take = len.min(n - drawn);
            for interval in &self.series[start..start + take] {
                for &(idx, c) in interval {
                    counts[idx] += c;
                    total += c;
                }
            }
            drawn += take;
        }
        self.quantiles(counts, total, qs, out)
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

/// Compares two arms: pools each arm's post-warmup intervals across runs,
/// runs a moving-block bootstrap on each and reports `Δ = B − A` per quantile
/// with 95% percentile intervals.
pub fn compare_with(
    a_runs: &[RunResult],
    b_runs: &[RunResult],
    metric: Metric,
    quantiles: &[f64],
    options: &CompareOptions,
) -> Result<Comparison, StatsError> {
    if options.resamples < 2 {
        return Err(StatsError::InvalidOption("resamples must be at least 2"));
    }
    if options.block_len == 0 {
        return Err(StatsError::InvalidOption("block_len must be positive"));
    }
    if quantiles.iter().any(|q| !(0.0..=1.0).contains(q)) {
        return Err(StatsError::InvalidOption("quantiles must be within [0, 1]"));
    }
    let a = Arm::build(a_runs, metric, options.block_len)?;
    let b = Arm::build(b_runs, metric, options.block_len)?;

    // p99 is always evaluated for the baseline criterion.
    let mut qs = quantiles.to_vec();
    qs.push(0.99);
    let p99_slot = qs.len() - 1;

    let a_point = a
        .pooled(&qs)
        .ok_or(StatsError::EmptyArm { arm: "A", metric })?;
    let b_point = b
        .pooled(&qs)
        .ok_or(StatsError::EmptyArm { arm: "B", metric })?;

    let mut rng_a = fastrand::Rng::with_seed(options.seed);
    let mut rng_b = fastrand::Rng::with_seed(options.seed ^ 0x9e37_79b9_7f4a_7c15);
    let mut counts_a = vec![0u64; a.values.len()];
    let mut counts_b = vec![0u64; b.values.len()];
    let (mut qa, mut qb) = (Vec::new(), Vec::new());
    let mut a_dist: Vec<Vec<f64>> = vec![Vec::with_capacity(options.resamples); qs.len()];
    let mut d_dist: Vec<Vec<f64>> = vec![Vec::with_capacity(options.resamples); qs.len()];
    for _ in 0..options.resamples {
        if !a.resample(&mut rng_a, &mut counts_a, &qs, &mut qa) {
            return Err(StatsError::EmptyResample("A"));
        }
        if !b.resample(&mut rng_b, &mut counts_b, &qs, &mut qb) {
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

    let entry = |i: usize, quantile: f64| {
        let a_lo = percentile(&a_dist[i], 0.025);
        let a_hi = percentile(&a_dist[i], 0.975);
        let a_ns = a_point[i];
        QuantileDelta {
            quantile,
            a_ns,
            b_ns: b_point[i],
            delta_ns: signed_delta(b_point[i], a_ns),
            delta_ci_low_ns: percentile(&d_dist[i], 0.025),
            delta_ci_high_ns: percentile(&d_dist[i], 0.975),
            a_ci_low_ns: a_lo,
            a_ci_high_ns: a_hi,
            a_ci_half_width_ratio: (a_hi - a_lo) / 2.0 / as_f64(a_ns.max(1)),
        }
    };

    Ok(Comparison {
        metric,
        resamples: options.resamples,
        block_len: options.block_len,
        seed: options.seed,
        a_intervals: a.series.len(),
        b_intervals: b.series.len(),
        a_count: a.total,
        b_count: b.total,
        quantiles: quantiles
            .iter()
            .enumerate()
            .map(|(i, &q)| entry(i, q))
            .collect(),
        baseline_p99_ci_half_width_ratio: entry(p99_slot, 0.99).a_ci_half_width_ratio,
    })
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

    /// A run whose interval `i` holds 1000 samples around `base`, with extra
    /// noise; warmup intervals hold absurd values that must be ignored.
    fn synthetic_run(seed: u64, base: u64, intervals: u64, warmup: u64) -> RunResult {
        let mut rng = fastrand::Rng::with_seed(seed);
        let mut r = IntervalRecorder::new(0);
        for i in 0..intervals {
            for k in 0..1000 {
                let at = i * SEC + k;
                let value = if i < warmup {
                    50_000_000
                } else {
                    base + rng.u64(0..base / 5) + if k % 100 == 0 { base } else { 0 }
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
            params: serde_json::Value::Null,
            fingerprint: Fingerprint::collect(),
            started_unix_ms: 0,
            warmup_intervals: warmup,
            summary: summarize(&intervals, warmup).unwrap(),
            intervals,
            validity: Validity::default(),
        }
    }

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
        assert_eq!(first.a_intervals, 105);
        assert_eq!(first.a_count, 105_000);

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

        let other_seed = compare_with(
            &a,
            &b,
            Metric::ChunkLatency,
            &[0.5, 0.99],
            &CompareOptions { seed: 1, ..opts },
        )
        .unwrap();
        assert_eq!(other_seed.quantiles[0].delta_ns, median.delta_ns);
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
    }
}
