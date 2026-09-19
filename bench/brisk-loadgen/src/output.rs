//! The result file: a [`RunResult`] plus a `loadgen` object with the load
//! generator's own evidence.
//!
//! `RunResult` readers ignore unknown fields, so the extra object does not
//! affect `RunResult::read_json` or `compare`.

use std::path::{Path, PathBuf};

use anyhow::Context as _;
use brisk_bench_core::result::RunResult;
use serde::Serialize;

use crate::cli::BodySize;
use crate::run::RampOutcome;
use crate::schedule::{PlannedInterval, StreamPlan};
use crate::shard::Counters;
use crate::validity::{LoadCheck, SelfcheckVerdict};

/// Name of the extension object in the result file.
pub(crate) const EXTENSION_KEY: &str = "loadgen";

/// Load-generator evidence stored next to the [`RunResult`] fields.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct Extension {
    /// Version of this tool.
    pub(crate) version: &'static str,
    /// Target URL as given.
    pub(crate) target: String,
    /// Resolved target address.
    pub(crate) target_addr: String,
    /// Whether TLS was used.
    pub(crate) tls: bool,
    /// Shard threads.
    pub(crate) shards: usize,
    /// Request size, head included.
    pub(crate) request_bytes: usize,
    /// Request body size.
    pub(crate) body_bytes: usize,
    /// Soft `RLIMIT_NOFILE` after raising it; `None` when unlimited or not
    /// applicable.
    pub(crate) nofile_limit: Option<u64>,
    /// Run length from origin to end, seconds.
    pub(crate) duration_s: f64,
    /// Transport counters of all shards.
    pub(crate) counters: Counters,
    /// Clock steps detected.
    pub(crate) clock_steps: u64,
    /// S1 model parameters.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) stream_plan: Option<StreamPlan>,
    /// Per-second achieved versus offered load.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) load_check: Option<LoadCheck>,
    /// Offered load per interval.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) planned: Option<Vec<PlannedInterval>>,
    /// S2 ramp steps.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) ramp: Option<RampOutcome>,
    /// `selfcheck` verdict.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) selfcheck: Option<SelfcheckVerdict>,
    /// S3 body size of this run.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) body_size: Option<BodySize>,
}

/// Writes the result document.
pub(crate) fn write_result(
    path: &Path,
    run: &RunResult,
    extension: &Extension,
) -> anyhow::Result<()> {
    let mut document = serde_json::to_value(run).context("serializing the run result")?;
    document
        .as_object_mut()
        .expect("a RunResult serializes to a JSON object")
        .insert(
            EXTENSION_KEY.to_owned(),
            serde_json::to_value(extension).context("serializing the loadgen extension")?,
        );
    let json = serde_json::to_vec_pretty(&document).context("serializing the result file")?;
    std::fs::write(path, json).with_context(|| format!("writing {}", path.display()))
}

/// Fails early when the result could not be written after the run.
pub(crate) fn check_writable(path: &Path) -> anyhow::Result<()> {
    let parent = match path.parent() {
        Some(p) if !p.as_os_str().is_empty() => p,
        _ => Path::new("."),
    };
    anyhow::ensure!(
        parent.is_dir(),
        "output directory {} does not exist",
        parent.display()
    );
    anyhow::ensure!(
        !path.is_dir(),
        "output path {} is a directory",
        path.display()
    );
    Ok(())
}

/// `dir/name.json` with suffix `10m` becomes `dir/name-10m.json`.
pub(crate) fn with_suffix(path: &Path, suffix: &str) -> PathBuf {
    let stem = path
        .file_stem()
        .map_or_else(String::new, |s| s.to_string_lossy().into_owned());
    let name = match path.extension() {
        Some(ext) => format!("{stem}-{suffix}.{}", ext.to_string_lossy()),
        None => format!("{stem}-{suffix}"),
    };
    path.with_file_name(name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn suffixes_keep_directory_and_extension() {
        assert_eq!(
            with_suffix(Path::new("out/s3-direct.json"), "10m"),
            Path::new("out/s3-direct-10m.json")
        );
        assert_eq!(with_suffix(Path::new("s3"), "1m"), Path::new("s3-1m"));
    }

    #[test]
    fn writability_is_checked_up_front() {
        assert!(check_writable(Path::new("result.json")).is_ok());
        assert!(check_writable(&std::env::temp_dir().join("x.json")).is_ok());
        assert!(check_writable(Path::new("no/such/dir/x.json")).is_err());
        assert!(check_writable(&std::env::temp_dir()).is_err());
    }
}
