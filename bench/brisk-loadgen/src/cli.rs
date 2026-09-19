//! Command-line interface.
//!
//! Every run subcommand writes one result document per run (see
//! [`crate::output`]); `compare` reads such documents back.

use std::path::PathBuf;

use brisk_bench_core::cpu::parse_cpu_list;
use brisk_bench_core::stats::{self, Metric};
use brisk_bench_core::wire::MARKER_LEN_U32;
use clap::{Args, Parser, Subcommand};
use serde::Serialize;

/// Open-loop HTTP/1.1 load generator for the Brisk benchmarks.
///
/// Latencies are measured from each request's scheduled start, so a slow
/// server cannot hide its latency by slowing the load down (no coordinated
/// omission).
#[derive(Debug, Parser)]
#[command(name = "brisk-loadgen", version, about, long_about = None)]
pub(crate) struct Cli {
    /// The scenario to run.
    #[command(subcommand)]
    pub(crate) command: Command,
}

/// Subcommands.
#[derive(Debug, Subcommand)]
pub(crate) enum Command {
    /// S1: concurrent SSE streams with log-normal durations and Poisson
    /// arrivals.
    Stream(StreamCmd),
    /// S2: non-streaming requests at a fixed rate or on a stepped ramp.
    Nonstream(NonstreamCmd),
    /// S3: large request bodies with streaming responses, one run per size.
    Bigbody(BigbodyCmd),
    /// Checks the load generator against the mock at twice a target
    /// concurrency.
    Selfcheck(SelfcheckCmd),
    /// Compares two arms of result files, paired by repetition: a t
    /// interval over the per-pair deltas, and a two-level bootstrap (whole
    /// repetitions, then blocks within each run) for arm A's baseline.
    Compare(CompareCmd),
}

/// Options shared by every run subcommand.
#[derive(Debug, Clone, Args, Serialize)]
pub(crate) struct CommonArgs {
    /// Target: a base URL such as `http://127.0.0.1:19080` (the path
    /// defaults to `/v1/chat/completions`) or a full endpoint URL.
    pub(crate) url: String,
    /// CA certificate (PEM) to trust; required for an `https://` target.
    #[arg(long)]
    pub(crate) tls_ca: Option<PathBuf>,
    /// Number of shard threads, each with its own event loop and
    /// connection pool.
    #[arg(long, default_value_t = 1, value_parser = clap::value_parser!(u16).range(1..=1024))]
    pub(crate) shards: u16,
    /// CPUs for the shard threads, e.g. `3` or `2-3`. With exactly one CPU
    /// per shard each shard gets its own; otherwise all shards share the set.
    #[arg(long, value_parser = parse_cpus)]
    pub(crate) cpu_list: Option<CpuSet>,
    /// Busy-wait window before each scheduled send, microseconds.
    #[arg(long, default_value_t = 50, value_parser = clap::value_parser!(u64).range(0..=10_000))]
    pub(crate) spin_us: u64,
    /// Arm label recorded in the result, e.g. `direct-P` or `floor-P`.
    #[arg(long, default_value = "run")]
    pub(crate) label: String,
    /// Result file (JSON).
    #[arg(long)]
    pub(crate) out: PathBuf,
    /// `model` field of every request.
    #[arg(long, default_value = "brisk-bench")]
    pub(crate) model: String,
    /// Extra request header `Name: value`; repeatable.
    #[arg(long = "header", short = 'H', value_parser = parse_header)]
    pub(crate) headers: Vec<Header>,
    /// Seed for stream durations and arrivals; equal seeds give equal
    /// schedules, which pairs the arms of an A/B comparison.
    #[arg(long, default_value_t = 1)]
    pub(crate) seed: u64,
    /// Repetition this run belongs to, e.g. `s1-P-r2`, recorded in the
    /// result. Both arms of a repetition take the same id and `--seed`;
    /// `compare` pairs the runs of its two arms by it, or by their seed when
    /// no run carries one.
    #[arg(long, value_parser = parse_pair_id)]
    pub(crate) pair_id: Option<String>,
    /// Abandon a request that has not completed this long after its
    /// scheduled start, seconds.
    #[arg(long, default_value_t = 300.0, value_parser = parse_positive)]
    pub(crate) request_timeout_s: f64,
}

/// Stream shape shared by `stream` and `selfcheck`.
#[derive(Debug, Clone, Args, Serialize)]
pub(crate) struct StreamShape {
    /// Target number of concurrent streams (`selfcheck` runs at twice this).
    #[arg(long, value_parser = clap::value_parser!(u32).range(1..))]
    pub(crate) concurrency: u32,
    /// Content chunks per second within a stream.
    #[arg(long, default_value_t = 30.0, value_parser = parse_chunk_rate)]
    pub(crate) chunk_rate: f64,
    /// Median stream duration, seconds.
    #[arg(long, default_value_t = 5.0, value_parser = parse_positive)]
    pub(crate) dur_median: f64,
    /// 99th percentile of the stream duration before truncation, seconds.
    #[arg(long, default_value_t = 60.0, value_parser = parse_positive)]
    pub(crate) dur_p99: f64,
    /// Truncation point of the stream duration, seconds.
    #[arg(long, default_value_t = 120.0, value_parser = parse_positive)]
    pub(crate) dur_max: f64,
    /// Mock delay before the first chunk, microseconds.
    #[arg(long, default_value_t = 0)]
    pub(crate) ttft_us: u64,
    /// Length of every `delta.content` string, marker included.
    #[arg(long, default_value_t = 128, value_parser = clap::value_parser!(u32).range(i64::from(MARKER_LEN_U32)..))]
    pub(crate) chunk_bytes: u32,
    /// Length of the user message, bytes.
    #[arg(long, default_value_t = 1024)]
    pub(crate) prompt_bytes: usize,
    /// Ask for a usage chunk (`stream_options.include_usage`).
    #[arg(long)]
    pub(crate) include_usage: bool,
}

/// `stream` (S1).
#[derive(Debug, Clone, Args, Serialize)]
pub(crate) struct StreamCmd {
    /// Shared options.
    #[command(flatten)]
    pub(crate) common: CommonArgs,
    /// Stream shape.
    #[command(flatten)]
    pub(crate) shape: StreamShape,
    /// Warmup excluded from summaries and comparisons, seconds.
    #[arg(long, default_value_t = 150)]
    pub(crate) warmup_s: u64,
    /// Measurement window after the warmup, seconds.
    #[arg(long, default_value_t = 300, value_parser = clap::value_parser!(u64).range(1..))]
    pub(crate) measure_s: u64,
    /// Invalidate the run when the request slip p99 (send to the mock's
    /// receipt of the whole request, pooled connections) reaches this,
    /// microseconds. Only meaningful for an arm aimed straight at the mock:
    /// through a gateway the slip includes the forwarding.
    #[arg(long, value_parser = clap::value_parser!(u64).range(1..))]
    pub(crate) max_slip_us: Option<u64>,
}

/// `selfcheck`.
#[derive(Debug, Clone, Args, Serialize)]
pub(crate) struct SelfcheckCmd {
    /// Shared options.
    #[command(flatten)]
    pub(crate) common: CommonArgs,
    /// Stream shape; the run uses twice `--concurrency`.
    #[command(flatten)]
    pub(crate) shape: StreamShape,
    /// Warmup excluded from the checks, seconds.
    #[arg(long, default_value_t = 10)]
    pub(crate) warmup_s: u64,
    /// Measurement window after the warmup, seconds.
    #[arg(long, default_value_t = 30, value_parser = clap::value_parser!(u64).range(1..))]
    pub(crate) measure_s: u64,
    /// Request slip p99 limit, microseconds (see `stream --max-slip-us`).
    #[arg(long, default_value_t = DEFAULT_SELFCHECK_SLIP_US, value_parser = clap::value_parser!(u64).range(1..))]
    pub(crate) max_slip_us: u64,
}

/// Default request slip p99 limit of `selfcheck`, microseconds.
///
/// An unsaturated mock still holds an arriving request while it spins
/// towards its next chunk (a 50 µs window by default) and writes the chunks
/// due, so tens of microseconds of slip are normal at the selfcheck's load.
/// A saturated mock queues requests for milliseconds (3 ms TTFT p99 at 2000
/// streams on one mock core in the pilot) while its write lag stays within
/// bounds. The limit sits between the two, and below the tail the floor-A
/// TTFT comparison has to resolve.
pub(crate) const DEFAULT_SELFCHECK_SLIP_US: u64 = 200;

/// `nonstream` (S2).
#[derive(Debug, Clone, Args, Serialize)]
pub(crate) struct NonstreamCmd {
    /// Shared options.
    #[command(flatten)]
    pub(crate) common: CommonArgs,
    /// Fixed request rate per second.
    #[arg(long, value_parser = parse_positive, required_unless_present = "ramp_start", conflicts_with = "ramp_start")]
    pub(crate) rate: Option<f64>,
    /// Ramp mode: rate of the warmup and of the first step, per second.
    #[arg(long, value_parser = parse_positive)]
    pub(crate) ramp_start: Option<f64>,
    /// Ramp mode: rate increase per step, percent.
    #[arg(long, default_value_t = 10.0, value_parser = parse_positive)]
    pub(crate) ramp_step_pct: f64,
    /// Ramp mode: step length, seconds.
    #[arg(long, default_value_t = 60, value_parser = clap::value_parser!(u64).range(1..))]
    pub(crate) ramp_step_s: u64,
    /// Ramp mode: stop after the first step whose p99 exceeds this,
    /// milliseconds.
    #[arg(long, default_value_t = 2.0, value_parser = parse_positive)]
    pub(crate) stop_p99_ms: f64,
    /// Ramp mode: upper bound on the number of steps.
    #[arg(long, default_value_t = 50, value_parser = clap::value_parser!(u32).range(1..))]
    pub(crate) ramp_max_steps: u32,
    /// Mock delay before the response, microseconds.
    #[arg(long, default_value_t = 0)]
    pub(crate) ttft_us: u64,
    /// Content length of each response, bytes.
    #[arg(long, default_value_t = 1024)]
    pub(crate) resp_bytes: u32,
    /// Length of the user message, bytes.
    #[arg(long, default_value_t = 1024)]
    pub(crate) prompt_bytes: usize,
    /// Warmup excluded from summaries, seconds (runs at `--ramp-start` in
    /// ramp mode).
    #[arg(long, default_value_t = 30)]
    pub(crate) warmup_s: u64,
    /// Fixed-rate mode: measurement window after the warmup, seconds.
    #[arg(long, default_value_t = 300, value_parser = clap::value_parser!(u64).range(1..))]
    pub(crate) measure_s: u64,
}

/// `bigbody` (S3).
#[derive(Debug, Clone, Args, Serialize)]
pub(crate) struct BigbodyCmd {
    /// Shared options. `--out x.json` writes `x-<size>.json` per size.
    #[command(flatten)]
    pub(crate) common: CommonArgs,
    /// Request body sizes, e.g. `100k,1m,10m` (k = KiB, m = MiB). Bodies of
    /// 10 MiB and more carry 90% of their size as a base64 image.
    #[arg(long, value_delimiter = ',', required = true, value_parser = parse_body_size)]
    pub(crate) sizes: Vec<BodySize>,
    /// Request rate per second.
    #[arg(long, value_parser = parse_positive)]
    pub(crate) rate: f64,
    /// Mock delay before the first chunk, microseconds.
    #[arg(long, default_value_t = 0)]
    pub(crate) ttft_us: u64,
    /// Content chunks per response.
    #[arg(long, default_value_t = 1, value_parser = clap::value_parser!(u32).range(1..))]
    pub(crate) chunks: u32,
    /// Delay between chunks, microseconds.
    #[arg(long, default_value_t = 33_333)]
    pub(crate) interval_us: u64,
    /// Length of every `delta.content` string, marker included.
    #[arg(long, default_value_t = 128, value_parser = clap::value_parser!(u32).range(i64::from(MARKER_LEN_U32)..))]
    pub(crate) chunk_bytes: u32,
    /// Warmup per size, seconds.
    #[arg(long, default_value_t = 30)]
    pub(crate) warmup_s: u64,
    /// Measurement window per size, seconds.
    #[arg(long, default_value_t = 300, value_parser = clap::value_parser!(u64).range(1..))]
    pub(crate) measure_s: u64,
}

/// `compare`.
#[derive(Debug, Clone, Args)]
pub(crate) struct CompareCmd {
    /// Result files of arm A (the baseline).
    #[arg(long = "a", num_args = 1.., required = true)]
    pub(crate) a: Vec<PathBuf>,
    /// Result files of arm B.
    #[arg(long = "b", num_args = 1.., required = true)]
    pub(crate) b: Vec<PathBuf>,
    /// Metrics to compare, e.g. `chunk_latency,ttft`.
    #[arg(long, value_delimiter = ',', required = true)]
    pub(crate) metric: Vec<Metric>,
    /// Quantiles in percent.
    #[arg(long, value_delimiter = ',', default_values = ["50", "99", "99.9"], value_parser = parse_percent)]
    pub(crate) quantiles: Vec<f64>,
    /// Bootstrap resamples.
    #[arg(long, default_value_t = stats::DEFAULT_RESAMPLES)]
    pub(crate) resamples: usize,
    /// Moving-block length in intervals, for the resampling within each
    /// run.
    #[arg(long, default_value_t = stats::DEFAULT_BLOCK_LEN)]
    pub(crate) block_len: usize,
    /// Bootstrap seed.
    #[arg(long, default_value_t = stats::DEFAULT_SEED)]
    pub(crate) seed: u64,
    /// Metrics whose baseline criterion (arm A's p99 95% CI half-width
    /// below 5% of its p99) decides the gate; each must be compared. The
    /// other compared metrics are judged and reported but do not gate.
    /// Without the flag, the compared ones of `ttft,chunk_latency` gate, or
    /// every compared metric when neither is compared (e.g. S2's
    /// `request_latency`).
    #[arg(long, value_delimiter = ',', num_args = 1..)]
    pub(crate) gate_metrics: Option<Vec<Metric>>,
    /// Accept result files whose run was judged invalid, or whose failure
    /// share exceeds a tenth of the highest quantile's tail.
    #[arg(long)]
    pub(crate) allow_invalid: bool,
    /// Comparison output file (JSON).
    #[arg(long)]
    pub(crate) out: PathBuf,
}

/// A parsed `--cpu-list`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct CpuSet(pub(crate) Vec<usize>);

/// An extra request header.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct Header {
    /// Header name as given.
    pub(crate) name: String,
    /// Header value, surrounding whitespace removed.
    pub(crate) value: String,
}

/// One `--sizes` entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct BodySize {
    /// The entry as written, lower-cased; used to label results.
    pub(crate) label: String,
    /// Body size in bytes.
    pub(crate) bytes: usize,
}

/// Headers the load generator writes itself.
const RESERVED_HEADERS: [&str; 5] = [
    "host",
    "content-length",
    "transfer-encoding",
    "connection",
    "content-type",
];

fn parse_cpus(s: &str) -> Result<CpuSet, String> {
    parse_cpu_list(s).map(CpuSet).map_err(|e| e.to_string())
}

fn parse_positive(s: &str) -> Result<f64, String> {
    let v: f64 = s
        .trim()
        .parse()
        .map_err(|_| format!("{s:?} is not a number"))?;
    if v.is_finite() && v > 0.0 {
        Ok(v)
    } else {
        Err(format!("{s:?} must be positive and finite"))
    }
}

/// Chunk rates above 1 MHz would round the chunk interval to zero.
fn parse_chunk_rate(s: &str) -> Result<f64, String> {
    let v = parse_positive(s)?;
    if v <= 1_000_000.0 {
        Ok(v)
    } else {
        Err(format!("{s:?} exceeds 1000000 chunks per second"))
    }
}

/// Parses a percentile such as `99.9` into the fraction `0.999`.
///
/// The decimal point is moved in the text rather than dividing by 100:
/// `99.9 / 100.0` is `0.9990000000000001`, whose quantile rank
/// `ceil(q · n)` is one sample too high whenever `0.999 · n` is whole.
pub(crate) fn parse_percent(s: &str) -> Result<f64, String> {
    let text = s.trim();
    let (int, frac) = text.split_once('.').unwrap_or((text, ""));
    let is_digits = |part: &str| part.bytes().all(|b| b.is_ascii_digit());
    if int.is_empty() && frac.is_empty() || !is_digits(int) || !is_digits(frac) {
        return Err(format!("{s:?} is not a decimal number"));
    }
    let int = format!("{int:0>2}");
    let (whole, hundredths) = int.split_at(int.len() - 2);
    let fraction: f64 = format!("{whole}.{hundredths}{frac}")
        .parse()
        .map_err(|_| format!("{s:?} is not a decimal number"))?;
    if fraction > 0.0 && fraction <= 1.0 {
        Ok(fraction)
    } else {
        Err(format!("{s:?} is not a percentile in (0, 100]"))
    }
}

/// Accepts a pairing id without whitespace or control characters, so it
/// reads unambiguously in logs and file names.
pub(crate) fn parse_pair_id(s: &str) -> Result<String, String> {
    if s.is_empty() || s.chars().any(|c| c.is_whitespace() || c.is_control()) {
        Err(format!(
            "{s:?} is not a pairing id (non-empty, no whitespace)"
        ))
    } else {
        Ok(s.to_owned())
    }
}

/// Parses `Name: value`.
pub(crate) fn parse_header(s: &str) -> Result<Header, String> {
    let (name, value) = s
        .split_once(':')
        .ok_or_else(|| format!("{s:?} is not of the form `Name: value`"))?;
    let name = name.trim();
    let is_token = |b: u8| b.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&b);
    if name.is_empty() || !name.bytes().all(is_token) {
        return Err(format!("{name:?} is not a valid header name"));
    }
    if RESERVED_HEADERS
        .iter()
        .any(|reserved| name.eq_ignore_ascii_case(reserved))
    {
        return Err(format!("header {name:?} is set by the load generator"));
    }
    let value = value.trim();
    if value.bytes().any(|b| b == b'\r' || b == b'\n') {
        return Err(format!("value of {name:?} contains a line break"));
    }
    Ok(Header {
        name: name.to_owned(),
        value: value.to_owned(),
    })
}

/// Parses a size such as `100k` (KiB), `10m` (MiB) or a plain byte count.
pub(crate) fn parse_body_size(s: &str) -> Result<BodySize, String> {
    let label = s.trim().to_ascii_lowercase();
    let (digits, unit) = match label.as_bytes().last() {
        Some(b'k') => (&label[..label.len() - 1], 1024),
        Some(b'm') => (&label[..label.len() - 1], 1024 * 1024),
        _ => (label.as_str(), 1),
    };
    let n: usize = digits
        .parse()
        .map_err(|_| format!("{s:?} is not a size like 100k or 10m"))?;
    let bytes = n
        .checked_mul(unit)
        .filter(|&b| b > 0)
        .ok_or_else(|| format!("{s:?} is not a positive size"))?;
    Ok(BodySize { label, bytes })
}

#[cfg(test)]
mod tests {
    use clap::CommandFactory as _;

    use super::*;

    fn parse(args: &[&str]) -> Result<Cli, clap::Error> {
        Cli::try_parse_from(std::iter::once("brisk-loadgen").chain(args.iter().copied()))
    }

    #[test]
    fn command_definition_is_consistent() {
        Cli::command().debug_assert();
    }

    #[test]
    fn stream_defaults_follow_the_contract() {
        let cli = parse(&[
            "stream",
            "http://127.0.0.1:19100",
            "--concurrency",
            "1000",
            "--out",
            "s1.json",
        ])
        .unwrap();
        let Command::Stream(cmd) = cli.command else {
            panic!("expected stream");
        };
        assert_eq!(cmd.shape.concurrency, 1000);
        assert!((cmd.shape.chunk_rate - 30.0).abs() < f64::EPSILON);
        assert!((cmd.shape.dur_median - 5.0).abs() < f64::EPSILON);
        assert!((cmd.shape.dur_p99 - 60.0).abs() < f64::EPSILON);
        assert!((cmd.shape.dur_max - 120.0).abs() < f64::EPSILON);
        assert_eq!((cmd.warmup_s, cmd.measure_s), (150, 300));
        assert_eq!(cmd.max_slip_us, None);
        assert_eq!(cmd.common.shards, 1);
        assert_eq!(cmd.common.label, "run");
        assert_eq!(cmd.common.pair_id, None);
        assert!(cmd.common.cpu_list.is_none());
    }

    #[test]
    fn stream_rejects_small_chunks_and_zero_concurrency() {
        let base = ["stream", "http://h:1", "--out", "o.json"];
        let with = |extra: &[&str]| {
            let mut args = base.to_vec();
            args.extend_from_slice(extra);
            parse(&args)
        };
        assert!(with(&["--concurrency", "0"]).is_err());
        assert!(with(&["--concurrency", "1", "--chunk-bytes", "77"]).is_err());
        assert!(with(&["--concurrency", "1", "--chunk-bytes", "78"]).is_ok());
        assert!(with(&["--concurrency", "1", "--chunk-rate", "0"]).is_err());
        assert!(with(&["--concurrency", "1", "--cpu-list", "3-1"]).is_err());
    }

    #[test]
    fn common_options_parse() {
        let cli = parse(&[
            "selfcheck",
            "https://mock.local:19101",
            "--concurrency",
            "100",
            "--tls-ca",
            "ca.pem",
            "--shards",
            "2",
            "--cpu-list",
            "2-3",
            "-H",
            "Authorization: Bearer x",
            "--header",
            "x-trace:1",
            "--label",
            "direct-T",
            "--pair-id",
            "sc-T-r1",
            "--out",
            "sc.json",
        ])
        .unwrap();
        let Command::Selfcheck(cmd) = cli.command else {
            panic!("expected selfcheck");
        };
        assert_eq!(cmd.common.shards, 2);
        assert_eq!(cmd.common.pair_id.as_deref(), Some("sc-T-r1"));
        assert_eq!(cmd.common.cpu_list, Some(CpuSet(vec![2, 3])));
        assert_eq!(cmd.common.headers.len(), 2);
        assert_eq!(cmd.common.headers[0].name, "Authorization");
        assert_eq!(cmd.common.headers[0].value, "Bearer x");
        assert_eq!((cmd.warmup_s, cmd.measure_s), (10, 30));
        assert_eq!(cmd.max_slip_us, DEFAULT_SELFCHECK_SLIP_US);
        assert!(
            parse(&[
                "selfcheck",
                "http://h:1",
                "--concurrency",
                "1",
                "--out",
                "o",
                "--shards",
                "0"
            ])
            .is_err()
        );
    }

    #[test]
    fn nonstream_requires_exactly_one_rate_mode() {
        let fixed = parse(&["nonstream", "http://h:1", "--rate", "500", "--out", "o"]).unwrap();
        let Command::Nonstream(cmd) = fixed.command else {
            panic!("expected nonstream");
        };
        assert_eq!(cmd.rate, Some(500.0));
        assert_eq!(cmd.ramp_start, None);

        let ramp = parse(&[
            "nonstream",
            "http://h:1",
            "--ramp-start",
            "100",
            "--out",
            "o",
        ])
        .unwrap();
        let Command::Nonstream(cmd) = ramp.command else {
            panic!("expected nonstream");
        };
        assert_eq!(cmd.ramp_start, Some(100.0));
        assert!((cmd.ramp_step_pct - 10.0).abs() < f64::EPSILON);
        assert_eq!(cmd.ramp_step_s, 60);
        assert!((cmd.stop_p99_ms - 2.0).abs() < f64::EPSILON);

        assert!(parse(&["nonstream", "http://h:1", "--out", "o"]).is_err());
        assert!(
            parse(&[
                "nonstream",
                "http://h:1",
                "--rate",
                "1",
                "--ramp-start",
                "1",
                "--out",
                "o"
            ])
            .is_err()
        );
    }

    #[test]
    fn bigbody_sizes_parse() {
        let cli = parse(&[
            "bigbody",
            "http://h:1",
            "--sizes",
            "100k,1M,10m,4096",
            "--rate",
            "5",
            "--out",
            "o.json",
        ])
        .unwrap();
        let Command::Bigbody(cmd) = cli.command else {
            panic!("expected bigbody");
        };
        let sizes: Vec<_> = cmd
            .sizes
            .iter()
            .map(|s| (s.label.as_str(), s.bytes))
            .collect();
        assert_eq!(
            sizes,
            [
                ("100k", 102_400),
                ("1m", 1_048_576),
                ("10m", 10_485_760),
                ("4096", 4096)
            ]
        );
        assert!(parse_body_size("0k").is_err());
        assert!(parse_body_size("k").is_err());
        assert!(parse_body_size("1g").is_err());
    }

    #[test]
    #[expect(
        clippy::float_cmp,
        reason = "the parser must yield the exactly rounded fraction"
    )]
    fn compare_parses_percent_quantiles_and_metrics() {
        let cli = parse(&[
            "compare",
            "--a",
            "a1.json",
            "a2.json",
            "--b",
            "b1.json",
            "--metric",
            "chunk_latency,ttft",
            "--out",
            "cmp.json",
        ])
        .unwrap();
        let Command::Compare(cmd) = cli.command else {
            panic!("expected compare");
        };
        assert_eq!(cmd.a.len(), 2);
        assert_eq!(cmd.b.len(), 1);
        assert_eq!(cmd.metric, [Metric::ChunkLatency, Metric::Ttft]);
        assert_eq!(cmd.quantiles, [0.5, 0.99, 0.999]);
        assert_eq!(cmd.resamples, stats::DEFAULT_RESAMPLES);
        assert_eq!(cmd.gate_metrics, None);
        assert!(!cmd.allow_invalid);

        let explicit = parse(&[
            "compare",
            "--a",
            "a",
            "--b",
            "b",
            "--metric",
            "ttft",
            "--quantiles",
            "90,99.99",
            "--gate-metrics",
            "ttft",
            "--out",
            "c",
        ])
        .unwrap();
        let Command::Compare(cmd) = explicit.command else {
            panic!("expected compare");
        };
        assert_eq!(cmd.gate_metrics.as_deref(), Some(&[Metric::Ttft][..]));
        assert_eq!(cmd.quantiles.len(), 2);
        assert_eq!(cmd.quantiles[1], 0.9999);
        assert!(
            parse(&[
                "compare", "--a", "a", "--b", "b", "--metric", "nope", "--out", "c"
            ])
            .is_err()
        );
        assert!(parse_percent("0").is_err());
        assert!(parse_percent("100.5").is_err());
        assert!(parse_percent("-5").is_err());
        assert!(parse_percent("1e2").is_err());
        assert!(parse_percent(".").is_err());
        assert_eq!(parse_percent("99.9"), Ok(0.999));
        assert_eq!(parse_percent("99.99"), Ok(0.9999));
        assert_eq!(parse_percent("5"), Ok(0.05));
        assert_eq!(parse_percent(".5"), Ok(0.005));
        assert_eq!(parse_percent("100"), Ok(1.0));
        assert_eq!(parse_percent("050"), Ok(0.5));
    }

    #[test]
    fn pair_ids_are_validated() {
        assert_eq!(parse_pair_id("s1-P-r2"), Ok("s1-P-r2".to_owned()));
        assert!(parse_pair_id("").is_err());
        assert!(parse_pair_id("r 1").is_err());
        assert!(parse_pair_id("r1\u{7}").is_err());
        for scenario in [
            &["stream", "http://h:1", "--concurrency", "1"][..],
            &["nonstream", "http://h:1", "--rate", "1"],
            &["bigbody", "http://h:1", "--sizes", "1k", "--rate", "1"],
        ] {
            let mut args = scenario.to_vec();
            args.extend_from_slice(&["--pair-id", "s1-P-r3", "--out", "o.json"]);
            let pair_id = match parse(&args).unwrap().command {
                Command::Stream(cmd) => cmd.common.pair_id,
                Command::Nonstream(cmd) => cmd.common.pair_id,
                Command::Bigbody(cmd) => cmd.common.pair_id,
                other => panic!("unexpected {other:?}"),
            };
            assert_eq!(pair_id.as_deref(), Some("s1-P-r3"));
        }
    }

    #[test]
    fn headers_are_validated() {
        assert!(parse_header("no-colon").is_err());
        assert!(parse_header("bad name: v").is_err());
        assert!(parse_header("Content-Length: 5").is_err());
        assert!(parse_header("host: x").is_err());
        assert_eq!(
            parse_header(" X-A :  b c ").unwrap(),
            Header {
                name: "X-A".into(),
                value: "b c".into()
            }
        );
    }
}
