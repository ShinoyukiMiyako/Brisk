//! The JSON result document every benchmark run produces.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::fingerprint::Fingerprint;
use crate::stats::{self, Metric, StatsError};

/// Version of the [`RunResult`] layout; bumped on incompatible changes.
pub const SCHEMA_VERSION: u32 = 1;

/// Errors while reading or writing result files.
#[derive(Debug, thiserror::Error)]
pub enum ResultError {
    /// File I/O failed.
    #[error("{path}: {source}")]
    Io {
        /// The file involved.
        path: PathBuf,
        /// The underlying error.
        #[source]
        source: std::io::Error,
    },
    /// The file is not a valid result document.
    #[error("{path}: {source}")]
    Json {
        /// The file involved.
        path: PathBuf,
        /// The underlying error.
        #[source]
        source: serde_json::Error,
    },
    /// The document was written by an incompatible version.
    #[error("{path}: schema version {found}, expected {SCHEMA_VERSION}")]
    Schema {
        /// The file involved.
        path: PathBuf,
        /// The version found in the file.
        found: u32,
    },
}

/// Whether a run's numbers may be used, and why not.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Validity {
    /// `false` once any validity check failed.
    pub valid: bool,
    /// Human-readable reasons for invalidity.
    pub reasons: Vec<String>,
}

impl Default for Validity {
    fn default() -> Self {
        Self {
            valid: true,
            reasons: Vec::new(),
        }
    }
}

impl Validity {
    /// Marks the run invalid and records why.
    pub fn invalidate(&mut self, reason: impl Into<String>) {
        self.valid = false;
        self.reasons.push(reason.into());
    }
}

/// Concurrency samples of one interval.
#[derive(Debug, Clone, Copy, PartialEq, Default, Serialize, Deserialize)]
pub struct ConcurrencyStats {
    /// Number of observations.
    pub samples: u64,
    /// Smallest observed value.
    pub min: u64,
    /// Largest observed value.
    pub max: u64,
    /// Mean of the observations.
    pub mean: f64,
}

/// One closed interval of a run.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IntervalResult {
    /// Interval number since the run's origin, starting at 0.
    pub index: u64,
    /// Start on the monotonic timeline, nanoseconds.
    pub start_ns: u64,
    /// Length, nanoseconds; shorter than nominal only for the last interval.
    pub duration_ns: u64,
    /// Non-empty histograms, V2-serialised and base64-encoded.
    pub histograms: BTreeMap<Metric, String>,
    /// Concurrency observations.
    pub concurrency: ConcurrencyStats,
    /// Chunks received.
    pub chunks: u64,
    /// Requests completed.
    pub requests: u64,
    /// Errors by kind.
    pub errors: BTreeMap<String, u64>,
    /// Latency values above the histogram range, recorded clamped.
    pub saturated: u64,
    /// Spans whose end preceded their start (clock conversion uncertainty),
    /// recorded as 0. See [`stats::IntervalRecorder::record_span`].
    #[serde(default)]
    pub negative: u64,
}

impl IntervalResult {
    /// Decodes the histogram of `metric`, if any value was recorded.
    pub fn histogram(
        &self,
        metric: Metric,
    ) -> Result<Option<hdrhistogram::Histogram<u64>>, StatsError> {
        self.histograms
            .get(&metric)
            .map(|encoded| stats::decode_histogram(encoded))
            .transpose()
    }

    /// Chunks per second over this interval.
    #[expect(clippy::cast_precision_loss, reason = "rates are approximate")]
    pub fn chunk_rate_per_s(&self) -> f64 {
        if self.duration_ns == 0 {
            return 0.0;
        }
        self.chunks as f64 * 1e9 / self.duration_ns as f64
    }

    /// Total errors of all kinds.
    pub fn error_count(&self) -> u64 {
        self.errors.values().sum()
    }
}

/// Headline numbers of one metric, nanoseconds.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct MetricSummary {
    /// Number of samples.
    pub count: u64,
    /// Minimum.
    pub min_ns: u64,
    /// Mean.
    pub mean_ns: f64,
    /// Median.
    pub p50_ns: u64,
    /// 90th percentile.
    pub p90_ns: u64,
    /// 99th percentile.
    pub p99_ns: u64,
    /// 99.9th percentile.
    pub p999_ns: u64,
    /// Maximum.
    pub max_ns: u64,
}

/// A complete run: identity, environment, raw intervals and summary.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunResult {
    /// Always [`SCHEMA_VERSION`] when written by this version.
    pub schema_version: u32,
    /// Producing tool, e.g. `brisk-loadgen`.
    pub tool: String,
    /// Scenario, e.g. `stream`, `nonstream`, `bigbody`.
    pub scenario: String,
    /// Arm label, e.g. `direct-P` or `floor-P`.
    pub label: String,
    /// Command-line parameters, verbatim.
    pub params: serde_json::Value,
    /// Host and build environment.
    pub fingerprint: Fingerprint,
    /// Wall-clock start, milliseconds since the UNIX epoch.
    pub started_unix_ms: u64,
    /// Leading intervals excluded from summaries and comparisons.
    pub warmup_intervals: u64,
    /// Every interval, warmup included.
    pub intervals: Vec<IntervalResult>,
    /// Post-warmup summary per metric.
    pub summary: BTreeMap<Metric, MetricSummary>,
    /// Validity verdict.
    pub validity: Validity,
}

impl RunResult {
    /// Reads a result document and checks its schema version.
    pub fn read_json(path: &Path) -> Result<Self, ResultError> {
        let bytes = std::fs::read(path).map_err(|source| ResultError::Io {
            path: path.to_path_buf(),
            source,
        })?;
        let result: Self = serde_json::from_slice(&bytes).map_err(|source| ResultError::Json {
            path: path.to_path_buf(),
            source,
        })?;
        if result.schema_version != SCHEMA_VERSION {
            return Err(ResultError::Schema {
                path: path.to_path_buf(),
                found: result.schema_version,
            });
        }
        Ok(result)
    }

    /// Writes the document as pretty-printed JSON.
    pub fn write_json(&self, path: &Path) -> Result<(), ResultError> {
        let json = serde_json::to_vec_pretty(self).map_err(|source| ResultError::Json {
            path: path.to_path_buf(),
            source,
        })?;
        std::fs::write(path, json).map_err(|source| ResultError::Io {
            path: path.to_path_buf(),
            source,
        })
    }

    /// Recomputes [`summary`](Self::summary) from the post-warmup intervals.
    pub fn resummarize(&mut self) -> Result<(), StatsError> {
        self.summary = stats::summarize(&self.intervals, self.warmup_intervals)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stats::IntervalRecorder;

    #[test]
    fn json_round_trip_through_file() {
        let mut rec = IntervalRecorder::new(0);
        rec.record(Metric::Ttft, 5, 300_000_000);
        rec.add_chunks(6, 30);
        let intervals = rec.finish(1_000_000_000);
        let mut run = RunResult {
            schema_version: SCHEMA_VERSION,
            tool: "brisk-loadgen".into(),
            scenario: "stream".into(),
            label: "direct-P".into(),
            params: serde_json::json!({"concurrency": 1000}),
            fingerprint: Fingerprint::collect(),
            started_unix_ms: 1,
            warmup_intervals: 0,
            intervals,
            summary: BTreeMap::new(),
            validity: Validity::default(),
        };
        run.resummarize().unwrap();
        run.validity.invalidate("clock step");
        assert!(!run.validity.valid);
        assert!((run.intervals[0].chunk_rate_per_s() - 30.0).abs() < 1e-9);

        let path = std::env::temp_dir().join(format!(
            "brisk-bench-core-result-{}-{}.json",
            std::process::id(),
            crate::clock::now_ns()
        ));
        run.write_json(&path).unwrap();
        let back = RunResult::read_json(&path).unwrap();
        // Floats may differ in the last ulp after a JSON round trip, so
        // compare the exact parts exactly.
        assert_eq!(back.intervals, run.intervals);
        assert_eq!(back.fingerprint, run.fingerprint);
        assert_eq!(back.validity, run.validity);
        assert_eq!(back.params, run.params);
        assert_eq!(
            back.summary[&Metric::Ttft].p99_ns,
            run.summary[&Metric::Ttft].p99_ns
        );
        assert!(back.intervals[0].histogram(Metric::Ttft).unwrap().is_some());

        let mut wrong = run.clone();
        wrong.schema_version = 99;
        wrong.write_json(&path).unwrap();
        assert!(matches!(
            RunResult::read_json(&path),
            Err(ResultError::Schema { found: 99, .. })
        ));
        std::fs::remove_file(&path).unwrap();
    }
}
