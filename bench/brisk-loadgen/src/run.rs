//! Scenario orchestration: shard threads, live status, ramp coordination,
//! merging and the result document.

use std::collections::BTreeMap;
use std::io;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::sync::mpsc::{self, Receiver, RecvTimeoutError};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use anyhow::{Context as _, anyhow, bail, ensure};
use brisk_bench_core::clock::{now_ns, realtime_ns};
use brisk_bench_core::cpu;
use brisk_bench_core::fingerprint::Fingerprint;
use brisk_bench_core::result::{IntervalResult, RunResult, SCHEMA_VERSION};
use brisk_bench_core::rlimit;
use brisk_bench_core::stats;
use brisk_bench_core::transport::tls;
use serde::Serialize;

use crate::cli::{
    BigbodyCmd, CommonArgs, CpuSet, NonstreamCmd, SelfcheckCmd, StreamCmd, StreamShape,
};
use crate::diagnostics::{self, Diagnostics};
use crate::output::{self, Extension};
use crate::report;
use crate::request::{BodySpec, RequestSpec, RequestTemplate};
use crate::schedule::{
    FixedArrivals, PlannedInterval, PlannedLoad, RateSegment, SEC_NS, StreamArrivals, StreamPlan,
    merge_planned, ramp_segments, share,
};
use crate::shard::{
    self, Connector, Counters, Live, Mode, RampMessage, RampSpec, Schedule, ShardSpec, StepReport,
};
use crate::target::Target;
use crate::validity::{self, Evidence};

/// Name recorded as the producing tool.
const TOOL: &str = "brisk-loadgen";
/// Time between choosing the origin and the first scheduled request, so
/// every shard thread is up and pinned before it matters.
const STARTUP_LEAD_NS: u64 = 200_000_000;
/// Descriptors kept free beyond the connections.
const FD_HEADROOM: u64 = 256;
/// Ramp steps with more failures than this share fail.
const RAMP_MAX_ERROR_SHARE: f64 = 0.01;
/// Share of a step's samples above its p99. When more of its sends than
/// this went out later than the p99 limit, each of them over the limit on
/// the load generator's lag alone, the step's p99 exceeds the limit whatever
/// the target does: the failure is the tool's.
const RAMP_TAIL_SHARE: f64 = 0.01;
/// Least time a ramp waits after a step's planned end before judging it,
/// beyond the p99 limit. A request of the step still open by then is
/// counted as a sample above the limit, and 50 ms leaves room for a late
/// wakeup of the event loop (M0's worst was 8.3 ms) without holding up the
/// verdict.
const RAMP_JUDGE_MARGIN_NS: u64 = 50_000_000;
/// A ramp shard whose sends fall this far behind their schedule is
/// saturated and ends the ramp at once. The worst single late send M0 saw
/// on the bench VM was 8.3 ms, so this is no scheduling hiccup; each send
/// that late carries 50 times the default 2 ms p99 limit from the load
/// generator alone, so the step would measure the tool rather than the
/// target; and a backlog only grows once the offered rate exceeds what the
/// shard can send, so waiting longer only postpones the verdict.
const RAMP_SATURATION_LAG_NS: u64 = 100_000_000;
/// For short steps the saturation limit is this fraction of the step
/// instead (whichever is smaller): a lag of a tenth of the step moves that
/// much of its schedule out of the step's own window.
const RAMP_SATURATION_STEP_DIVISOR: u64 = 10;
/// Requests of a ramp time out after this many times the p99 limit, and no
/// sooner than [`RAMP_MIN_REQUEST_TIMEOUT_NS`]: a request that slow has
/// long been counted against its step, and a longer timeout would only
/// keep a saturated ramp from ending.
const RAMP_TIMEOUT_P99_FACTOR: u64 = 100;
/// Shortest request timeout of a ramp.
const RAMP_MIN_REQUEST_TIMEOUT_NS: u64 = SEC_NS;
/// Longest the coordinator waits past a step's judging deadline for the
/// report of every shard. A shard reports within a turn of its event loop,
/// well under a millisecond after the deadline; half a second without a
/// report means the load generator cannot run on time.
const RAMP_REPORT_GRACE_NS: u64 = 500_000_000;
/// Scenario of a `nonstream` ramp. Its summary mixes every step's rate, so
/// it is kept apart from fixed-rate `nonstream` results.
pub(crate) const RAMP_SCENARIO: &str = "nonstream-ramp";

/// Per-run setup shared by all scenarios.
#[derive(Debug)]
struct Setup {
    target: Target,
    connector: Connector,
    shard_cpus: Vec<Option<Vec<usize>>>,
    nofile_limit: Option<u64>,
}

impl Setup {
    fn new(common: &CommonArgs, connections: u64) -> anyhow::Result<Self> {
        output::check_writable(&common.out)?;
        let target = Target::resolve(&common.url)?;
        let tls = match (target.url.tls, &common.tls_ca) {
            (true, Some(ca)) => {
                Some((tls::client_config(ca)?, tls::server_name(&target.url.host)?))
            }
            (true, None) => bail!("an https target needs --tls-ca"),
            (false, Some(_)) => bail!("--tls-ca was given for a plaintext http target"),
            (false, None) => None,
        };
        let shards = usize::from(common.shards);
        let shard_cpus = assign_cpus(common.cpu_list.as_ref(), shards);
        if let Some(cpus) = &common.cpu_list {
            cpu::pin_current_thread(&cpus.0).context("pinning the main thread to --cpu-list")?;
        }
        let nofile_limit = rlimit::raise_nofile_limit().context("raising RLIMIT_NOFILE")?;
        if let Some(limit) = nofile_limit {
            let needed = connections + FD_HEADROOM;
            ensure!(
                limit >= needed,
                "RLIMIT_NOFILE is {limit}, below the {needed} descriptors this run may need; raise the hard limit"
            );
        }
        let connector = Connector {
            addr: target.addr,
            tls,
        };
        Ok(Self {
            target,
            connector,
            shard_cpus,
            nofile_limit,
        })
    }

    fn template(&self, common: &CommonArgs, spec: Template) -> anyhow::Result<RequestTemplate> {
        Ok(RequestTemplate::build(&RequestSpec {
            authority: &self.target.url.authority,
            path: &self.target.url.path,
            headers: &common.headers,
            model: &common.model,
            stream: spec.stream,
            include_usage: spec.include_usage,
            ttft_us: spec.ttft_us,
            interval_us: spec.interval_us,
            chunk_bytes: spec.chunk_bytes,
            resp_bytes: spec.resp_bytes,
            body: spec.body,
        })?)
    }

    fn extension(
        &self,
        common: &CommonArgs,
        template: &RequestTemplate,
        outcome: &PhaseOutcome,
        warmup_intervals: u64,
    ) -> Extension {
        Extension {
            version: env!("CARGO_PKG_VERSION"),
            target: common.url.clone(),
            target_addr: self.target.addr.to_string(),
            tls: self.connector.tls.is_some(),
            shards: usize::from(common.shards),
            request_bytes: template.len(),
            body_bytes: template.body_len(),
            nofile_limit: self.nofile_limit,
            duration_s: seconds(outcome.end_ns.saturating_sub(outcome.origin_ns)),
            counters: outcome.counters,
            clock_steps: outcome.clock_steps,
            diagnostics: Diagnostics::summarize(&outcome.diagnostics, warmup_intervals),
            stream_plan: None,
            load_check: None,
            planned: None,
            ramp: None,
            selfcheck: None,
            body_size: None,
        }
    }
}

/// Request parameters that vary by scenario.
#[derive(Debug, Clone, Copy)]
struct Template {
    stream: bool,
    include_usage: bool,
    ttft_us: u64,
    interval_us: u64,
    chunk_bytes: u32,
    resp_bytes: u32,
    body: BodySpec,
}

/// Splits `--cpu-list` over the shards: one CPU each when the counts
/// match, otherwise the whole set for every shard.
fn assign_cpus(list: Option<&CpuSet>, shards: usize) -> Vec<Option<Vec<usize>>> {
    match list {
        None => vec![None; shards],
        Some(set) if set.0.len() == shards => set.0.iter().map(|&cpu| Some(vec![cpu])).collect(),
        Some(set) => vec![Some(set.0.clone()); shards],
    }
}

/// Seed of shard `index`; distinct per shard and stable across runs.
fn shard_seed(seed: u64, index: usize) -> u64 {
    seed ^ (index as u64 + 1).wrapping_mul(0x9e37_79b9_7f4a_7c15)
}

#[expect(clippy::cast_precision_loss, reason = "display and JSON only")]
fn seconds(ns: u64) -> f64 {
    ns as f64 / 1e9
}

#[expect(clippy::cast_precision_loss, reason = "display only")]
fn count(n: u64) -> f64 {
    n as f64
}

/// Converts seconds from the command line to nanoseconds.
fn secs_ns(s: u64) -> u64 {
    s.saturating_mul(SEC_NS)
}

/// Chooses the common origin and the matching wall-clock start.
fn choose_origin() -> (u64, u64) {
    let mono = now_ns();
    let wall = realtime_ns();
    let origin = mono + STARTUP_LEAD_NS;
    (origin, (wall + STARTUP_LEAD_NS) / 1_000_000)
}

/// What the live status line shows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StatusStyle {
    /// Open streams against the offered count, chunk rate.
    Streams,
    /// Requests in flight, request rate.
    Requests,
}

/// The merged result of one phase.
#[derive(Debug)]
struct PhaseOutcome {
    origin_ns: u64,
    end_ns: u64,
    started_unix_ms: u64,
    intervals: Vec<IntervalResult>,
    planned: Option<Vec<PlannedInterval>>,
    counters: Counters,
    clock_steps: u64,
    diagnostics: diagnostics::Intervals,
    ramp: Option<RampOutcome>,
}

/// Runs the shards to completion while printing a status line every second
/// and, in ramp mode, judging steps as they are reported.
fn run_phase(
    specs: Vec<ShardSpec>,
    origin_ns: u64,
    started_unix_ms: u64,
    style: StatusStyle,
    mut ramp: Option<RampCoordinator>,
) -> anyhow::Result<PhaseOutcome> {
    let end_ns = specs.iter().map(|s| s.end_ns).max().unwrap_or(origin_ns);
    let lives: Vec<Arc<Live>> = specs.iter().map(|s| s.live.clone()).collect();
    let handles = specs
        .into_iter()
        .map(|spec| {
            thread::Builder::new()
                .name(format!("shard-{}", spec.index))
                .spawn(move || shard::run(spec))
        })
        .collect::<io::Result<Vec<JoinHandle<io::Result<shard::ShardOutput>>>>>()
        .context("spawning shard threads")?;

    let mut status = Status::new(origin_ns, style);
    let mut next_status = origin_ns + SEC_NS;
    let mut stopping = false;
    while !handles.iter().all(JoinHandle::is_finished) {
        let now = now_ns();
        // A shard that ends well before the others failed; stop the rest so
        // its error surfaces now instead of after the full run.
        if !stopping && now + 100_000_000 < end_ns && handles.iter().any(JoinHandle::is_finished) {
            stop_all(&lives);
            stopping = true;
        }
        let wait = Duration::from_nanos(
            next_status
                .saturating_sub(now)
                .clamp(1_000_000, 100_000_000),
        );
        match &mut ramp {
            Some(coordinator) => {
                match coordinator.reports.recv_timeout(wait) {
                    Ok(message) => coordinator.on_message(message, &lives),
                    Err(RecvTimeoutError::Timeout) => {}
                    // Every shard has dropped its sender and is about to exit.
                    Err(RecvTimeoutError::Disconnected) => {
                        thread::sleep(Duration::from_millis(1));
                    }
                }
                coordinator.advance(&lives, Some(now_ns()));
            }
            None => thread::sleep(wait),
        }
        let now = now_ns();
        if now >= next_status {
            status.print(now, &lives);
            next_status += SEC_NS * ((now - next_status) / SEC_NS + 1);
        }
    }

    let mut intervals = Vec::with_capacity(handles.len());
    let mut planned = Vec::new();
    let mut counters = Counters::default();
    let mut clock_steps = 0;
    let mut diagnostics = diagnostics::Intervals::new();
    let mut actual_end = origin_ns;
    for (index, handle) in handles.into_iter().enumerate() {
        let output = handle
            .join()
            .map_err(|_| anyhow!("shard {index} panicked"))?
            .with_context(|| format!("shard {index} failed"))?;
        intervals.push(output.intervals);
        planned.extend(output.planned);
        counters.add(&output.counters);
        clock_steps += output.clock_steps;
        diagnostics::merge_into(&mut diagnostics, output.diagnostics);
        actual_end = actual_end.max(output.end_ns);
    }
    let ramp = ramp
        .map(|mut coordinator| {
            while let Ok(message) = coordinator.reports.try_recv() {
                coordinator.on_message(message, &lives);
            }
            coordinator.finish(&lives)
        })
        .transpose()?;
    Ok(PhaseOutcome {
        origin_ns,
        end_ns: actual_end,
        started_unix_ms,
        intervals: stats::merge_shards(intervals).context("merging shard intervals")?,
        planned: (!planned.is_empty()).then(|| merge_planned(planned)),
        counters,
        clock_steps,
        diagnostics,
        ramp,
    })
}

fn stop_all(lives: &[Arc<Live>]) {
    for live in lives {
        live.stop.store(true, Ordering::Relaxed);
    }
}

/// Live status line on stderr.
#[derive(Debug)]
struct Status {
    origin_ns: u64,
    style: StatusStyle,
    last_ns: u64,
    last_chunks: u64,
    last_requests: u64,
}

impl Status {
    fn new(origin_ns: u64, style: StatusStyle) -> Self {
        Self {
            origin_ns,
            style,
            last_ns: origin_ns,
            last_chunks: 0,
            last_requests: 0,
        }
    }

    fn print(&mut self, now: u64, lives: &[Arc<Live>]) {
        let sum = |f: fn(&Live) -> u64| lives.iter().map(|l| f(l)).sum::<u64>();
        let open = sum(|l| l.open.load(Ordering::Relaxed));
        let planned = sum(|l| l.planned_open.load(Ordering::Relaxed));
        let chunks = sum(|l| l.chunks.load(Ordering::Relaxed));
        let requests = sum(|l| l.requests.load(Ordering::Relaxed));
        let errors = sum(|l| l.errors.load(Ordering::Relaxed));
        let dt = seconds(now.saturating_sub(self.last_ns)).max(1e-9);
        let chunk_rate = count(chunks.saturating_sub(self.last_chunks)) / dt;
        let request_rate = count(requests.saturating_sub(self.last_requests)) / dt;
        let elapsed = seconds(now.saturating_sub(self.origin_ns));
        match self.style {
            StatusStyle::Streams => eprintln!(
                "[{elapsed:>6.0}s] open {open:>6} (offered {planned:>6})  chunks/s {chunk_rate:>9.0}  \
                 done/s {request_rate:>7.1}  errors {errors}"
            ),
            StatusStyle::Requests => eprintln!(
                "[{elapsed:>6.0}s] in flight {open:>6}  req/s {request_rate:>9.1}  errors {errors}"
            ),
        }
        self.last_ns = now;
        self.last_chunks = chunks;
        self.last_requests = requests;
    }
}

/// Why a ramp step passed or failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum StepVerdict {
    /// Within the p99 limit and the error limit.
    Passed,
    /// No request of the step completed.
    NoRequests,
    /// More than [`RAMP_MAX_ERROR_SHARE`] of the step's requests failed.
    Errors,
    /// The step's p99, censored samples included, exceeded the limit.
    P99Exceeded,
    /// The load generator could not deliver the step's schedule: a shard's
    /// sends fell behind by more than the saturation limit, a shard did not
    /// report the step in time, or the step's p99 exceeded the limit with
    /// more than [`RAMP_TAIL_SHARE`] of its sends made later than the limit.
    /// Says nothing about the target.
    ToolSaturated,
}

/// Result of one ramp step, all shards merged.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct RampStep {
    /// Step number.
    pub(crate) step: u32,
    /// Offered rate per second.
    pub(crate) rate: f64,
    /// Successful requests.
    pub(crate) requests: u64,
    /// Failed requests.
    pub(crate) errors: u64,
    /// Requests still open, and planned sends not made, when judged
    /// (recorded as lower bounds).
    pub(crate) censored: u64,
    /// Planned sends not made when judged, because a shard was behind its
    /// schedule or had stopped; part of `censored`.
    pub(crate) unsent: u64,
    /// Median latency, nanoseconds.
    pub(crate) p50_ns: u64,
    /// 99th percentile latency, nanoseconds.
    pub(crate) p99_ns: u64,
    /// Largest lag of a send behind its scheduled time, as the shards'
    /// event loops reached the sends, nanoseconds.
    pub(crate) max_emit_lag_ns: u64,
    /// Sends made more than the p99 limit after their scheduled time.
    pub(crate) late_sends: u64,
    /// Within the p99 limit and the error limit.
    pub(crate) passed: bool,
    /// Why the step passed or failed.
    pub(crate) verdict: StepVerdict,
}

/// Outcome of an S2 ramp.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct RampOutcome {
    /// The p99 limit, nanoseconds.
    pub(crate) stop_p99_ns: u64,
    /// Every step is judged this long after its planned end at the latest,
    /// nanoseconds.
    pub(crate) judge_delay_ns: u64,
    /// Emit lag at which a shard counts as saturated, nanoseconds.
    pub(crate) saturation_lag_ns: u64,
    /// Request timeout of the ramp, nanoseconds.
    pub(crate) request_timeout_ns: u64,
    /// Judged steps in order, up to and including the failing one.
    pub(crate) steps: Vec<RampStep>,
    /// Why the ramp ended.
    pub(crate) stopped_by: String,
    /// Verdict of the step, or of the warmup, that ended the ramp; `None`
    /// when every step passed.
    pub(crate) stop_reason: Option<StepVerdict>,
    /// Rate of the last step before the first failing one.
    pub(crate) max_sustainable_rate: Option<f64>,
}

/// Timing limits of a ramp, derived from its parameters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[expect(
    clippy::struct_field_names,
    reason = "times are u64 nanoseconds named `_ns` throughout the crate"
)]
struct RampLimits {
    /// The p99 limit of a sustainable step.
    stop_p99_ns: u64,
    /// Delay after a step's planned end by which every shard reports it:
    /// the p99 limit plus a margin of twice the limit, the margin at least
    /// [`RAMP_JUDGE_MARGIN_NS`].
    judge_delay_ns: u64,
    /// Emit lag at which a shard is saturated.
    saturation_lag_ns: u64,
    /// Request timeout: the configured one, capped as
    /// [`RAMP_TIMEOUT_P99_FACTOR`] describes.
    request_timeout_ns: u64,
}

impl RampLimits {
    fn new(stop_p99_ns: u64, step_ns: u64, configured_timeout_ns: u64) -> Self {
        let margin = stop_p99_ns.saturating_mul(2).max(RAMP_JUDGE_MARGIN_NS);
        let timeout = stop_p99_ns
            .saturating_mul(RAMP_TIMEOUT_P99_FACTOR)
            .max(RAMP_MIN_REQUEST_TIMEOUT_NS);
        Self {
            stop_p99_ns,
            judge_delay_ns: stop_p99_ns.saturating_add(margin),
            saturation_lag_ns: RAMP_SATURATION_LAG_NS.min(step_ns / RAMP_SATURATION_STEP_DIVISOR),
            request_timeout_ns: configured_timeout_ns.min(timeout),
        }
    }
}

/// A shard that could not keep to the ramp's schedule.
#[derive(Debug, Clone, PartialEq)]
struct Saturation {
    /// Step it fell behind in; 0 for the warmup.
    step: u32,
    /// How late its send was, when known.
    lag_ns: Option<u64>,
    /// What happened, for `stopped_by`.
    cause: String,
}

/// Judges the ramp steps in order from the shards' reports and stops every
/// shard at the first step that fails or in which the load generator
/// saturates.
#[derive(Debug)]
struct RampCoordinator {
    reports: Receiver<RampMessage>,
    shards: usize,
    limits: RampLimits,
    /// The evaluated steps (step >= 1) in order.
    segments: Vec<RateSegment>,
    /// Reports of the steps not judged yet: shards reported and their
    /// merged report.
    received: BTreeMap<u32, (usize, StepReport)>,
    /// Judged steps in order.
    steps: Vec<RampStep>,
    /// The earliest step a shard saturated in.
    saturation: Option<Saturation>,
    /// The verdict that ended the ramp and why, once decided.
    stop: Option<(StepVerdict, String)>,
}

impl RampCoordinator {
    fn new(
        reports: Receiver<RampMessage>,
        shards: usize,
        limits: RampLimits,
        segments: &[RateSegment],
    ) -> Self {
        Self {
            reports,
            shards,
            limits,
            segments: segments.iter().copied().filter(|s| s.step > 0).collect(),
            received: BTreeMap::new(),
            steps: Vec::new(),
            saturation: None,
            stop: None,
        }
    }

    /// Takes in a shard's message. A saturated shard stops every shard at
    /// once; the step it saturated in is judged once the steps before it
    /// are.
    fn on_message(&mut self, message: RampMessage, lives: &[Arc<Live>]) {
        match message {
            RampMessage::Step(report) => match self.received.get_mut(&report.step) {
                Some((reported, acc)) => {
                    merge(acc, &report);
                    *reported += 1;
                }
                None => {
                    self.received.insert(report.step, (1, report));
                }
            },
            RampMessage::Saturated {
                shard,
                step,
                lag_ns,
            } => {
                let cause = format!(
                    "shard {shard} reached a send {:.1} ms after its scheduled time, beyond the \
                     {:.1} ms limit: loadgen emit lag, tool saturated",
                    millis(lag_ns),
                    millis(self.limits.saturation_lag_ns)
                );
                eprintln!("ramp {}: {cause}; stopping", step_name(step));
                self.saturate(
                    Saturation {
                        step,
                        lag_ns: Some(lag_ns),
                        cause,
                    },
                    lives,
                );
            }
        }
    }

    fn saturate(&mut self, saturation: Saturation, lives: &[Arc<Live>]) {
        if self
            .saturation
            .as_ref()
            .is_none_or(|known| saturation.step < known.step)
        {
            self.saturation = Some(saturation);
        }
        stop_all(lives);
    }

    /// Judges every step that can be judged, in order. `now` is given while
    /// the shards run: a step whose reports are still missing
    /// [`RAMP_REPORT_GRACE_NS`] after its deadline then saturates the ramp,
    /// and a saturated step waits for the reports its stopped shards send.
    /// `None` once every shard has ended and all their messages are in.
    fn advance(&mut self, lives: &[Arc<Live>], now: Option<u64>) {
        while self.stop.is_none() {
            let Some(seg) = self.segments.get(self.steps.len()).copied() else {
                return;
            };
            if let Some(saturation) = self.saturation.clone()
                && saturation.step <= seg.step
            {
                if now.is_none() {
                    self.stop_saturated(seg, &saturation);
                }
                return;
            }
            let reported = self.received.get(&seg.step).map_or(0, |(n, _)| *n);
            if reported == self.shards {
                let (_, acc) = self
                    .received
                    .remove(&seg.step)
                    .expect("the step was just found");
                if !self.judge(seg, &acc) {
                    stop_all(lives);
                }
                continue;
            }
            // Stopped shards report late by design: after their drain.
            let deadline = seg
                .end_ns
                .saturating_add(self.limits.judge_delay_ns)
                .saturating_add(RAMP_REPORT_GRACE_NS);
            if self.saturation.is_some() || now.is_none_or(|now| now < deadline) {
                return;
            }
            let cause = format!(
                "{} of {} shard(s) did not report it within {:.0} ms of its deadline: loadgen \
                 stalled, tool saturated",
                self.shards - reported,
                self.shards,
                millis(RAMP_REPORT_GRACE_NS)
            );
            eprintln!("ramp step {}: {cause}; stopping", seg.step);
            self.saturate(
                Saturation {
                    step: seg.step,
                    lag_ns: None,
                    cause,
                },
                lives,
            );
        }
    }

    /// Judges a step every shard reported; returns whether it passed. A p99
    /// over the limit is the tool's when the sends the load generator made
    /// later than the limit decide it on their own (see [`RAMP_TAIL_SHARE`]).
    fn judge(&mut self, seg: RateSegment, acc: &StepReport) -> bool {
        let limit = self.limits.stop_p99_ns;
        let total = acc.requests + acc.errors;
        let p99_ns = acc.latency.value_at_quantile(0.99);
        #[expect(clippy::cast_precision_loss, reason = "request counts are small")]
        let error_share = if total == 0 {
            1.0
        } else {
            acc.errors as f64 / total as f64
        };
        let verdict = if total == 0 {
            StepVerdict::NoRequests
        } else if error_share > RAMP_MAX_ERROR_SHARE {
            StepVerdict::Errors
        } else if p99_ns <= limit {
            StepVerdict::Passed
        } else if late_share(acc) > RAMP_TAIL_SHARE {
            StepVerdict::ToolSaturated
        } else {
            StepVerdict::P99Exceeded
        };
        let passed = verdict == StepVerdict::Passed;
        eprintln!(
            "ramp step {}: {:.1} req/s  p99 {:.3} ms  errors {}/{}  censored {} (unsent {})  \
             late sends {}  max emit lag {:.3} ms  -> {}",
            seg.step,
            seg.rate,
            millis(p99_ns),
            acc.errors,
            total,
            acc.censored,
            acc.unsent,
            acc.late_sends,
            millis(acc.max_emit_lag_ns),
            if passed { "ok" } else { "stop" }
        );
        self.record(seg, acc, verdict, None);
        let step = seg.step;
        let stopped_by = match verdict {
            StepVerdict::Passed => return true,
            StepVerdict::NoRequests => format!("step {step} completed no request"),
            StepVerdict::Errors => format!(
                "step {step} failed {:.2}% of its requests",
                error_share * 100.0
            ),
            StepVerdict::P99Exceeded => format!("step {step} p99 exceeded the limit"),
            StepVerdict::ToolSaturated => format!(
                "step {step}: p99 exceeded the limit, and {:.2}% of its sends went out more than \
                 the {:.3} ms limit after their scheduled time: loadgen emit lag, tool saturated",
                late_share(acc) * 100.0,
                millis(limit)
            ),
        };
        self.stop = Some((verdict, stopped_by));
        false
    }

    /// Ends the ramp at `seg`, whose schedule the load generator could not
    /// keep, with what the shards reported of it; or at the warmup (or an
    /// already judged step) the saturation happened in.
    fn stop_saturated(&mut self, seg: RateSegment, saturation: &Saturation) {
        let verdict = StepVerdict::ToolSaturated;
        if saturation.step < seg.step {
            self.stop = Some((
                verdict,
                format!("{}: {}", step_name(saturation.step), saturation.cause),
            ));
            return;
        }
        let acc = self.received.remove(&seg.step).map_or_else(
            || StepReport {
                step: seg.step,
                latency: stats::new_histogram(),
                requests: 0,
                errors: 0,
                censored: 0,
                unsent: 0,
                max_emit_lag_ns: 0,
                late_sends: 0,
            },
            |(_, acc)| acc,
        );
        eprintln!(
            "ramp step {}: {:.1} req/s  requests {}  censored {} (unsent {})  -> stop (tool saturated)",
            seg.step, seg.rate, acc.requests, acc.censored, acc.unsent
        );
        self.record(seg, &acc, verdict, saturation.lag_ns);
        self.stop = Some((verdict, format!("step {}: {}", seg.step, saturation.cause)));
    }

    /// Adds a judged step to the outcome. `lag_ns` is the lag of a
    /// saturating send, which the step's reports need not include.
    fn record(
        &mut self,
        seg: RateSegment,
        acc: &StepReport,
        verdict: StepVerdict,
        lag_ns: Option<u64>,
    ) {
        self.steps.push(RampStep {
            step: seg.step,
            rate: seg.rate,
            requests: acc.requests,
            errors: acc.errors,
            censored: acc.censored,
            unsent: acc.unsent,
            p50_ns: acc.latency.value_at_quantile(0.5),
            p99_ns: acc.latency.value_at_quantile(0.99),
            max_emit_lag_ns: lag_ns.map_or(acc.max_emit_lag_ns, |lag| lag.max(acc.max_emit_lag_ns)),
            late_sends: acc.late_sends,
            passed: verdict == StepVerdict::Passed,
            verdict,
        });
    }

    /// The outcome, once every shard has ended and its messages are in.
    fn finish(mut self, lives: &[Arc<Live>]) -> anyhow::Result<RampOutcome> {
        self.advance(lives, None);
        let (stop_reason, stopped_by) = match self.stop.take() {
            Some((verdict, why)) => (Some(verdict), why),
            None if self.steps.len() == self.segments.len() => (
                None,
                "all steps passed (--ramp-max-steps reached)".to_owned(),
            ),
            None => bail!(
                "the ramp ended before step {} was reported by every shard",
                self.segments[self.steps.len()].step
            ),
        };
        let max_sustainable_rate = self
            .steps
            .iter()
            .take_while(|s| s.passed)
            .last()
            .map(|s| s.rate);
        Ok(RampOutcome {
            stop_p99_ns: self.limits.stop_p99_ns,
            judge_delay_ns: self.limits.judge_delay_ns,
            saturation_lag_ns: self.limits.saturation_lag_ns,
            request_timeout_ns: self.limits.request_timeout_ns,
            steps: self.steps,
            stopped_by,
            stop_reason,
            max_sustainable_rate,
        })
    }
}

/// Adds one shard's report of a step to the others'.
fn merge(acc: &mut StepReport, report: &StepReport) {
    acc.latency
        .add(&report.latency)
        .expect("step histograms share the standard bounds");
    acc.requests += report.requests;
    acc.errors += report.errors;
    acc.censored += report.censored;
    acc.unsent += report.unsent;
    acc.max_emit_lag_ns = acc.max_emit_lag_ns.max(report.max_emit_lag_ns);
    acc.late_sends += report.late_sends;
}

/// Share of a step's sends made later than the p99 limit, the unsent ones
/// included; `acc` holds at least one send.
#[expect(clippy::cast_precision_loss, reason = "request counts are small")]
fn late_share(acc: &StepReport) -> f64 {
    let sends = acc.requests + acc.errors + acc.censored;
    (acc.late_sends + acc.unsent) as f64 / sends as f64
}

/// "warmup" for step 0, "step N" otherwise.
fn step_name(step: u32) -> String {
    if step == 0 {
        "warmup".to_owned()
    } else {
        format!("step {step}")
    }
}

fn millis(ns: u64) -> f64 {
    seconds(ns) * 1e3
}

/// Assembles the [`RunResult`] of a phase.
fn build_result(
    scenario: &str,
    common: &CommonArgs,
    params: serde_json::Value,
    warmup_intervals: u64,
    outcome: &PhaseOutcome,
) -> anyhow::Result<RunResult> {
    let summary =
        stats::summarize(&outcome.intervals, warmup_intervals).context("summarizing intervals")?;
    Ok(RunResult {
        schema_version: SCHEMA_VERSION,
        tool: TOOL.to_owned(),
        scenario: scenario.to_owned(),
        label: common.label.clone(),
        pair_id: common.pair_id.clone(),
        params,
        fingerprint: Fingerprint::collect(),
        started_unix_ms: outcome.started_unix_ms,
        warmup_intervals,
        intervals: outcome.intervals.clone(),
        summary,
        validity: brisk_bench_core::result::Validity::default(),
    })
}

/// Runs S1-style streams at `concurrency`; shared by `stream` and
/// `selfcheck`.
fn run_streams(
    common: &CommonArgs,
    shape: &StreamShape,
    concurrency: u32,
    warmup_s: u64,
    measure_s: u64,
    scenario: &str,
    params: serde_json::Value,
) -> anyhow::Result<(RunResult, Extension)> {
    let shards = usize::from(common.shards);
    ensure!(
        u32::try_from(shards).is_ok_and(|s| s <= concurrency),
        "--shards {shards} exceeds the {concurrency} concurrent streams"
    );
    // Open streams fluctuate around the target; allow for twice as many.
    let setup = Setup::new(common, 2 * u64::from(concurrency))?;
    let plan = StreamPlan::new(shape, concurrency)?;
    let template = setup.template(
        common,
        Template {
            stream: true,
            include_usage: shape.include_usage,
            ttft_us: shape.ttft_us,
            interval_us: plan.interval_us,
            chunk_bytes: shape.chunk_bytes,
            resp_bytes: 0,
            body: BodySpec::Prompt(shape.prompt_bytes),
        },
    )?;
    let total_s = warmup_s + measure_s;
    let (origin_ns, started_unix_ms) = choose_origin();
    let end_ns = origin_ns + secs_ns(total_s);
    let intervals = usize::try_from(total_s).context("run too long")?;
    let specs = (0..shards)
        .map(|index| {
            Ok(ShardSpec {
                index,
                origin_ns,
                end_ns,
                cpus: setup.shard_cpus[index].clone(),
                spin_window: Duration::from_micros(common.spin_us),
                connector: setup.connector.clone(),
                template: template.clone(),
                schedule: Schedule::Stream(StreamArrivals::new(
                    plan,
                    share(concurrency, shards, index),
                    shard_seed(common.seed, index),
                    origin_ns,
                    end_ns,
                )?),
                mode: Mode::Stream {
                    ttft_ns: plan.ttft_ns,
                    interval_ns: plan.interval_us * 1_000,
                },
                planned: Some((
                    PlannedLoad::new(origin_ns, SEC_NS, intervals),
                    plan.ttft_ns,
                    plan.interval_us * 1_000,
                )),
                request_timeout_ns: timeout_ns(common),
                live: Arc::new(Live::default()),
                ramp: None,
            })
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    eprintln!(
        "{scenario}: {concurrency} streams against {} ({} shard(s)), arrivals {:.1}/s, \
         mean lifetime {:.2} s, offered {:.0} chunks/s; warmup {warmup_s} s, measure {measure_s} s",
        common.url, shards, plan.arrival_rate, plan.mean_lifetime_s, plan.offered_chunk_rate
    );
    let outcome = run_phase(
        specs,
        origin_ns,
        started_unix_ms,
        StatusStyle::Streams,
        None,
    )?;
    let mut run = build_result(scenario, common, params, warmup_s, &outcome)?;
    let planned = outcome.planned.clone().unwrap_or_default();
    let load = validity::check_load(&run.intervals, &planned, warmup_s, concurrency);
    let mut extension = setup.extension(common, &template, &outcome, warmup_s);
    run.validity = validity::evaluate(&Evidence {
        intervals: &run.intervals,
        warmup_intervals: warmup_s,
        summary: &run.summary,
        clock_steps: outcome.clock_steps,
        load: Some(&load),
        counters: &outcome.counters,
        diagnostics: &extension.diagnostics,
        markers_expected: true,
        mock_lag_limit_ns: common.mock_write_lag_limit_ns(),
    });
    extension.stream_plan = Some(plan);
    extension.load_check = Some(load);
    extension.planned = Some(planned);
    Ok((run, extension))
}

fn timeout_ns(common: &CommonArgs) -> u64 {
    brisk_bench_core::dist::seconds_to_ns(common.request_timeout_s)
}

fn params_of<T: Serialize>(cmd: &T) -> anyhow::Result<serde_json::Value> {
    serde_json::to_value(cmd).context("recording the command-line parameters")
}

fn finish_run(common: &CommonArgs, run: &RunResult, extension: &Extension) -> anyhow::Result<()> {
    output::write_result(&common.out, run, extension)?;
    report::print_run(run, extension);
    println!("result written to {}", common.out.display());
    Ok(())
}

/// `stream` (S1).
pub(crate) fn stream(cmd: &StreamCmd) -> anyhow::Result<()> {
    let (mut run, extension) = run_streams(
        &cmd.common,
        &cmd.shape,
        cmd.shape.concurrency,
        cmd.warmup_s,
        cmd.measure_s,
        "stream",
        params_of(cmd)?,
    )?;
    if let Some(limit_us) = cmd.max_slip_us {
        validity::check_slip(
            &mut run.validity,
            &extension.diagnostics,
            limit_us.saturating_mul(1_000),
        );
    }
    finish_run(&cmd.common, &run, &extension)
}

/// `selfcheck`: S1 streams at twice the target concurrency straight to the
/// mock. Fails (non-zero exit) unless every criterion holds.
pub(crate) fn selfcheck(cmd: &SelfcheckCmd) -> anyhow::Result<()> {
    let doubled = cmd
        .shape
        .concurrency
        .checked_mul(2)
        .context("--concurrency too large to double")?;
    let (run, mut extension) = run_streams(
        &cmd.common,
        &cmd.shape,
        doubled,
        cmd.warmup_s,
        cmd.measure_s,
        "selfcheck",
        params_of(cmd)?,
    )?;
    let load = extension
        .load_check
        .as_ref()
        .expect("stream runs carry a load check");
    let verdict = validity::selfcheck(&validity::SelfcheckEvidence {
        summary: &run.summary,
        load,
        diagnostics: &extension.diagnostics,
        slip_limit_ns: cmd.max_slip_us.saturating_mul(1_000),
        mock_lag_limit_ns: cmd.common.mock_write_lag_limit_ns(),
        validity: &run.validity,
    });
    extension.selfcheck = Some(verdict.clone());
    finish_run(&cmd.common, &run, &extension)?;
    report::print_selfcheck(&verdict, cmd.shape.concurrency, doubled);
    ensure!(verdict.pass, "selfcheck failed");
    Ok(())
}

/// An evenly spaced phase of `nonstream` or `bigbody`.
#[derive(Debug)]
struct FixedPhase<'a> {
    setup: &'a Setup,
    common: &'a CommonArgs,
    template: &'a RequestTemplate,
    segments: &'a [RateSegment],
    origin_ns: u64,
    end_ns: u64,
    mode: Mode,
    chunks: u32,
    request_timeout_ns: u64,
    /// Ramp reporting: where shards report and the ramp's limits.
    ramp: Option<(&'a mpsc::Sender<RampMessage>, RampLimits)>,
}

impl FixedPhase<'_> {
    fn specs(&self) -> Vec<ShardSpec> {
        let shards = usize::from(self.common.shards);
        let steps: Vec<RateSegment> = self
            .segments
            .iter()
            .copied()
            .filter(|s| s.step > 0)
            .collect();
        (0..shards)
            .map(|index| ShardSpec {
                index,
                origin_ns: self.origin_ns,
                end_ns: self.end_ns,
                cpus: self.setup.shard_cpus[index].clone(),
                spin_window: Duration::from_micros(self.common.spin_us),
                connector: self.setup.connector.clone(),
                template: self.template.clone(),
                schedule: Schedule::Fixed(FixedArrivals::new(
                    self.segments.to_vec(),
                    index,
                    shards,
                    self.chunks,
                )),
                mode: self.mode,
                planned: None,
                request_timeout_ns: self.request_timeout_ns,
                live: Arc::new(Live::default()),
                ramp: self.ramp.map(|(tx, limits)| RampSpec {
                    steps: steps.clone(),
                    judge_delay_ns: limits.judge_delay_ns,
                    saturation_lag_ns: limits.saturation_lag_ns,
                    stop_p99_ns: limits.stop_p99_ns,
                    reports: tx.clone(),
                }),
            })
            .collect()
    }
}

/// The `nonstream` schedule: its segments, its end and a description. A
/// ramp ends once its last step can be judged.
fn nonstream_schedule(
    cmd: &NonstreamCmd,
    origin_ns: u64,
    limits: RampLimits,
) -> (Vec<RateSegment>, u64, String) {
    let warmup_ns = secs_ns(cmd.warmup_s);
    match (cmd.rate, cmd.ramp_start) {
        (Some(rate), None) => {
            let end_ns = origin_ns + warmup_ns + secs_ns(cmd.measure_s);
            let segments = vec![RateSegment {
                start_ns: origin_ns,
                end_ns,
                rate,
                step: 0,
            }];
            (
                segments,
                end_ns,
                format!("{rate} req/s for {} s", cmd.measure_s),
            )
        }
        (None, Some(start)) => {
            let segments = ramp_segments(
                origin_ns,
                warmup_ns,
                start,
                cmd.ramp_step_pct,
                secs_ns(cmd.ramp_step_s),
                cmd.ramp_max_steps,
            );
            let last = segments.last().expect("at least one ramp step").end_ns;
            let end_ns = last + limits.judge_delay_ns;
            let description = format!(
                "ramp from {start} req/s, +{}% every {} s, stop at p99 > {} ms (steps judged \
                 {:.0} ms after their end, loadgen saturated at {:.0} ms emit lag, request \
                 timeout {:.1} s)",
                cmd.ramp_step_pct,
                cmd.ramp_step_s,
                cmd.stop_p99_ms,
                millis(limits.judge_delay_ns),
                millis(limits.saturation_lag_ns),
                seconds(limits.request_timeout_ns)
            );
            (segments, end_ns, description)
        }
        _ => unreachable!("clap enforces exactly one of --rate and --ramp-start"),
    }
}

/// `nonstream` (S2): fixed rate or ramp.
pub(crate) fn nonstream(cmd: &NonstreamCmd) -> anyhow::Result<()> {
    let common = &cmd.common;
    let setup = Setup::new(common, max_in_flight(cmd.rate.or(cmd.ramp_start), common))?;
    let template = setup.template(
        common,
        Template {
            stream: false,
            include_usage: false,
            ttft_us: cmd.ttft_us,
            interval_us: 0,
            chunk_bytes: brisk_bench_core::wire::MARKER_LEN_U32,
            resp_bytes: cmd.resp_bytes,
            body: BodySpec::Prompt(cmd.prompt_bytes),
        },
    )?;
    let (origin_ns, started_unix_ms) = choose_origin();
    let stop_p99_ns = brisk_bench_core::dist::seconds_to_ns(cmd.stop_p99_ms / 1e3);
    // Only a ramp uses them; a fixed rate keeps the configured timeout.
    let limits = RampLimits::new(stop_p99_ns, secs_ns(cmd.ramp_step_s), timeout_ns(common));
    let (segments, end_ns, description) = nonstream_schedule(cmd, origin_ns, limits);
    let ramp = cmd.ramp_start.map(|_| mpsc::channel());
    // The mock embeds a marker in every completion whose content has room.
    let markers_expected = cmd.resp_bytes >= brisk_bench_core::wire::MARKER_LEN_U32;
    let specs = FixedPhase {
        setup: &setup,
        common,
        template: &template,
        segments: &segments,
        origin_ns,
        end_ns,
        mode: Mode::Whole {
            ttft_ns: cmd.ttft_us.saturating_mul(1_000),
            marker: markers_expected,
        },
        chunks: 1,
        request_timeout_ns: if ramp.is_some() {
            limits.request_timeout_ns
        } else {
            timeout_ns(common)
        },
        ramp: ramp.as_ref().map(|(tx, _)| (tx, limits)),
    }
    .specs();
    let coordinator = ramp.map(|(_, rx)| RampCoordinator::new(rx, specs.len(), limits, &segments));
    eprintln!(
        "nonstream: {description} against {} ({} shard(s)); warmup {} s",
        common.url,
        specs.len(),
        cmd.warmup_s
    );
    let outcome = run_phase(
        specs,
        origin_ns,
        started_unix_ms,
        StatusStyle::Requests,
        coordinator,
    )?;
    let scenario = if cmd.ramp_start.is_some() {
        RAMP_SCENARIO
    } else {
        "nonstream"
    };
    let mut run = build_result(scenario, common, params_of(cmd)?, cmd.warmup_s, &outcome)?;
    let mut extension = setup.extension(common, &template, &outcome, cmd.warmup_s);
    run.validity = validity::evaluate(&Evidence {
        intervals: &run.intervals,
        warmup_intervals: cmd.warmup_s,
        summary: &run.summary,
        clock_steps: outcome.clock_steps,
        load: None,
        counters: &outcome.counters,
        diagnostics: &extension.diagnostics,
        markers_expected,
        mock_lag_limit_ns: common.mock_write_lag_limit_ns(),
    });
    extension.ramp.clone_from(&outcome.ramp);
    finish_run(common, &run, &extension)
}

/// Upper bound on concurrent connections of a fixed-rate run: every
/// request of one timeout window in flight at once, capped to keep the
/// descriptor check meaningful.
#[expect(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "positive, clamped to a small range"
)]
fn max_in_flight(rate: Option<f64>, common: &CommonArgs) -> u64 {
    (rate.unwrap_or(1.0) * common.request_timeout_s.min(10.0)).clamp(1.0, 65_536.0) as u64
}

/// `bigbody` (S3): one run and one result file per size.
pub(crate) fn bigbody(cmd: &BigbodyCmd) -> anyhow::Result<()> {
    let common = &cmd.common;
    let params = params_of(cmd)?;
    for size in &cmd.sizes {
        let out = output::with_suffix(&common.out, &size.label);
        let sized = CommonArgs {
            out: out.clone(),
            ..common.clone()
        };
        let setup = Setup::new(&sized, max_in_flight(Some(cmd.rate), common))?;
        let template = setup.template(
            common,
            Template {
                stream: true,
                include_usage: false,
                ttft_us: cmd.ttft_us,
                interval_us: cmd.interval_us,
                chunk_bytes: cmd.chunk_bytes,
                resp_bytes: 0,
                body: BodySpec::Exact(size.bytes),
            },
        )?;
        let (origin_ns, started_unix_ms) = choose_origin();
        let end_ns = origin_ns + secs_ns(cmd.warmup_s + cmd.measure_s);
        let segments = vec![RateSegment {
            start_ns: origin_ns,
            end_ns,
            rate: cmd.rate,
            step: 0,
        }];
        let specs = FixedPhase {
            setup: &setup,
            common,
            template: &template,
            segments: &segments,
            origin_ns,
            end_ns,
            mode: Mode::Stream {
                ttft_ns: cmd.ttft_us.saturating_mul(1_000),
                interval_ns: cmd.interval_us.saturating_mul(1_000),
            },
            chunks: cmd.chunks,
            request_timeout_ns: timeout_ns(common),
            ramp: None,
        }
        .specs();
        eprintln!(
            "bigbody {}: {} byte bodies at {} req/s against {}; warmup {} s, measure {} s",
            size.label,
            template.body_len(),
            cmd.rate,
            common.url,
            cmd.warmup_s,
            cmd.measure_s
        );
        let outcome = run_phase(
            specs,
            origin_ns,
            started_unix_ms,
            StatusStyle::Requests,
            None,
        )?;
        let scenario = format!("bigbody-{}", size.label);
        let mut run = build_result(&scenario, &sized, params.clone(), cmd.warmup_s, &outcome)?;
        let mut extension = setup.extension(&sized, &template, &outcome, cmd.warmup_s);
        run.validity = validity::evaluate(&Evidence {
            intervals: &run.intervals,
            warmup_intervals: cmd.warmup_s,
            summary: &run.summary,
            clock_steps: outcome.clock_steps,
            load: None,
            counters: &outcome.counters,
            diagnostics: &extension.diagnostics,
            markers_expected: true,
            mock_lag_limit_ns: sized.mock_write_lag_limit_ns(),
        });
        extension.body_size = Some(size.clone());
        finish_run(&sized, &run, &extension)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cpus_are_split_or_shared() {
        assert_eq!(assign_cpus(None, 2), vec![None, None]);
        let set = CpuSet(vec![2, 3]);
        assert_eq!(
            assign_cpus(Some(&set), 2),
            vec![Some(vec![2]), Some(vec![3])]
        );
        assert_eq!(assign_cpus(Some(&set), 3), vec![Some(vec![2, 3]); 3]);
        let one = CpuSet(vec![3]);
        assert_eq!(assign_cpus(Some(&one), 1), vec![Some(vec![3])]);
    }

    #[test]
    fn shard_seeds_differ() {
        assert_ne!(shard_seed(1, 0), shard_seed(1, 1));
        assert_eq!(shard_seed(1, 0), shard_seed(1, 0));
    }

    /// The p99 limit of the coordinator tests.
    const LIMIT_NS: u64 = 2_000_000;

    fn report(step: u32, values: &[u64], errors: u64) -> StepReport {
        let mut latency = stats::new_histogram();
        for &v in values {
            latency.record(v).unwrap();
        }
        StepReport {
            step,
            latency,
            requests: values.len() as u64,
            errors,
            censored: 0,
            unsent: 0,
            max_emit_lag_ns: 1_000,
            late_sends: 0,
        }
    }

    /// A partial report as a stopped shard sends it: `unsent` planned sends
    /// censored with a large bound.
    fn stopped_report(step: u32, values: &[u64], unsent: u64, max_emit_lag_ns: u64) -> StepReport {
        let mut partial = report(step, values, 0);
        partial.latency.record_n(SEC_NS, unsent).unwrap();
        partial.censored = unsent;
        partial.unsent = unsent;
        partial.max_emit_lag_ns = max_emit_lag_ns;
        partial
    }

    /// A coordinator of `shards` shards over steps of one second from 0 at
    /// 100, 150, 225, ... per second, and their live flags.
    fn coordinator(shards: usize, steps: u32) -> (RampCoordinator, Vec<Arc<Live>>) {
        let (_tx, rx) = mpsc::channel();
        let segments = ramp_segments(0, 0, 100.0, 50.0, SEC_NS, steps);
        let limits = RampLimits::new(LIMIT_NS, SEC_NS, 300 * SEC_NS);
        let lives = (0..shards).map(|_| Arc::new(Live::default())).collect();
        (RampCoordinator::new(rx, shards, limits, &segments), lives)
    }

    /// Delivers a message as the running phase does, well before any
    /// reporting deadline.
    fn deliver(coordinator: &mut RampCoordinator, message: RampMessage, lives: &[Arc<Live>]) {
        coordinator.on_message(message, lives);
        coordinator.advance(lives, Some(0));
    }

    fn step(coordinator: &mut RampCoordinator, report: StepReport, lives: &[Arc<Live>]) {
        deliver(coordinator, RampMessage::Step(report), lives);
    }

    fn all_stopped(lives: &[Arc<Live>]) -> bool {
        lives.iter().all(|l| l.stop.load(Ordering::Relaxed))
    }

    fn verdicts(outcome: &RampOutcome) -> Vec<StepVerdict> {
        outcome.steps.iter().map(|s| s.verdict).collect()
    }

    #[test]
    fn ramp_stops_at_the_first_slow_step() {
        let (mut coordinator, lives) = coordinator(2, 3);
        let fast = vec![500_000; 100];
        let slow = vec![3_000_000; 100];
        step(&mut coordinator, report(1, &fast, 0), &lives);
        assert!(coordinator.steps.is_empty(), "waits for every shard");
        step(&mut coordinator, report(1, &fast, 0), &lives);
        assert_eq!(coordinator.steps.len(), 1);
        assert!(!lives[0].stop.load(Ordering::Relaxed));
        step(&mut coordinator, report(2, &fast, 0), &lives);
        step(&mut coordinator, report(2, &slow, 0), &lives);
        assert!(all_stopped(&lives));
        step(&mut coordinator, report(3, &fast, 0), &lives);
        step(&mut coordinator, report(3, &fast, 0), &lives);
        let outcome = coordinator.finish(&lives).unwrap();
        assert_eq!(
            verdicts(&outcome),
            [StepVerdict::Passed, StepVerdict::P99Exceeded]
        );
        assert!(outcome.steps[0].passed && !outcome.steps[1].passed);
        assert_eq!(outcome.max_sustainable_rate, Some(100.0));
        assert_eq!(outcome.stop_reason, Some(StepVerdict::P99Exceeded));
        assert!(
            outcome.stopped_by.contains("step 2"),
            "{}",
            outcome.stopped_by
        );
    }

    #[test]
    fn ramp_fails_steps_with_errors() {
        let (mut coordinator, lives) = coordinator(1, 2);
        step(&mut coordinator, report(1, &[1_000; 98], 2), &lives);
        assert!(all_stopped(&lives));
        let outcome = coordinator.finish(&lives).unwrap();
        assert_eq!(verdicts(&outcome), [StepVerdict::Errors]);
        assert_eq!(outcome.max_sustainable_rate, None);
        assert_eq!(outcome.stop_reason, Some(StepVerdict::Errors));
        assert!(
            outcome.stopped_by.contains("failed"),
            "{}",
            outcome.stopped_by
        );
    }

    #[test]
    fn ramp_passing_every_step_ends_at_the_last() {
        let (mut coordinator, lives) = coordinator(1, 2);
        step(&mut coordinator, report(1, &[1_000; 100], 0), &lives);
        step(&mut coordinator, report(2, &[1_000; 100], 0), &lives);
        assert!(!all_stopped(&lives));
        let outcome = coordinator.finish(&lives).unwrap();
        assert_eq!(verdicts(&outcome), [StepVerdict::Passed; 2]);
        assert_eq!(outcome.stop_reason, None);
        assert_eq!(outcome.max_sustainable_rate, Some(150.0));
        assert!(outcome.stopped_by.contains("all steps passed"));
    }

    #[test]
    fn saturation_stops_every_shard_at_once_and_is_told_apart_from_latency() {
        let (mut coordinator, lives) = coordinator(2, 3);
        let fast = vec![500_000; 100];
        step(&mut coordinator, report(1, &fast, 0), &lives);
        step(&mut coordinator, report(1, &fast, 0), &lives);
        let saturated = RampMessage::Saturated {
            shard: 1,
            step: 2,
            lag_ns: 150_000_000,
        };
        deliver(&mut coordinator, saturated, &lives);
        // Every shard is stopped before any of them reported step 2.
        assert!(all_stopped(&lives));
        // The stopped shards report what they made of step 2; the record
        // waits for them.
        assert_eq!(coordinator.steps.len(), 1);
        step(
            &mut coordinator,
            stopped_report(2, &fast, 3, 20_000),
            &lives,
        );
        step(
            &mut coordinator,
            stopped_report(2, &fast[..60], 40, 150_000_000),
            &lives,
        );
        let outcome = coordinator.finish(&lives).unwrap();
        assert_eq!(
            verdicts(&outcome),
            [StepVerdict::Passed, StepVerdict::ToolSaturated]
        );
        let failed = &outcome.steps[1];
        assert!(!failed.passed);
        assert_eq!(
            (failed.requests, failed.unsent, failed.censored),
            (160, 43, 43)
        );
        assert_eq!(failed.max_emit_lag_ns, 150_000_000);
        assert_eq!(outcome.stop_reason, Some(StepVerdict::ToolSaturated));
        assert_eq!(outcome.max_sustainable_rate, Some(100.0));
        assert!(
            outcome.stopped_by.starts_with("step 2: shard 1 ")
                && outcome.stopped_by.contains("tool saturated"),
            "{}",
            outcome.stopped_by
        );
        assert_eq!(outcome.saturation_lag_ns, 100_000_000);
    }

    #[test]
    fn a_p99_decided_by_late_sends_is_the_tools() {
        // Steps failing their p99 alike, 3 ms at the tail of 100 sends:
        // where the load generator made more than 1% of the sends later
        // than the limit the tail is its own; at 1% the target's latency
        // had to add to it.
        let mut values = vec![500_000; 97];
        values.extend([3_000_000; 3]);
        for (late_sends, unsent, verdict) in [
            (2, 0, StepVerdict::ToolSaturated),
            (1, 1, StepVerdict::ToolSaturated),
            (1, 0, StepVerdict::P99Exceeded),
        ] {
            let (mut coordinator, lives) = coordinator(1, 2);
            let mut only = report(1, &values, 0);
            only.late_sends = late_sends;
            only.unsent = unsent;
            only.censored = unsent;
            step(&mut coordinator, only, &lives);
            assert!(all_stopped(&lives));
            let outcome = coordinator.finish(&lives).unwrap();
            assert_eq!(verdicts(&outcome), [verdict]);
            assert_eq!(outcome.steps[0].late_sends, late_sends);
            assert_eq!(outcome.stop_reason, Some(verdict));
            assert_eq!(
                outcome
                    .stopped_by
                    .contains("loadgen emit lag, tool saturated"),
                verdict == StepVerdict::ToolSaturated,
                "{}",
                outcome.stopped_by
            );
        }
    }

    #[test]
    fn saturation_in_the_warmup_leaves_no_sustainable_rate() {
        let (mut coordinator, lives) = coordinator(2, 3);
        let saturated = RampMessage::Saturated {
            shard: 0,
            step: 0,
            lag_ns: 120_000_000,
        };
        deliver(&mut coordinator, saturated, &lives);
        assert!(all_stopped(&lives));
        let outcome = coordinator.finish(&lives).unwrap();
        assert!(outcome.steps.is_empty());
        assert_eq!(outcome.stop_reason, Some(StepVerdict::ToolSaturated));
        assert_eq!(outcome.max_sustainable_rate, None);
        assert!(
            outcome.stopped_by.starts_with("warmup: "),
            "{}",
            outcome.stopped_by
        );
    }

    #[test]
    fn saturation_is_judged_after_the_steps_before_it() {
        let fast = vec![500_000; 100];
        let slow = vec![3_000_000; 100];
        for (late_report, expected) in [
            (
                &fast,
                [StepVerdict::Passed, StepVerdict::ToolSaturated].as_slice(),
            ),
            (&slow, [StepVerdict::P99Exceeded].as_slice()),
        ] {
            let (mut coordinator, lives) = coordinator(2, 3);
            step(&mut coordinator, report(1, &fast, 0), &lives);
            // Shard 1 saturates in step 2 before its step 1 report is in.
            let saturated = RampMessage::Saturated {
                shard: 1,
                step: 2,
                lag_ns: 120_000_000,
            };
            deliver(&mut coordinator, saturated, &lives);
            assert!(all_stopped(&lives));
            assert!(coordinator.steps.is_empty());
            step(&mut coordinator, report(1, late_report, 0), &lives);
            step(&mut coordinator, stopped_report(2, &fast, 5, 1_000), &lives);
            step(&mut coordinator, stopped_report(2, &fast, 9, 1_000), &lives);
            let outcome = coordinator.finish(&lives).unwrap();
            assert_eq!(verdicts(&outcome), expected);
            assert_eq!(outcome.stop_reason, expected.last().copied());
        }
    }

    #[test]
    fn a_step_reported_late_saturates_the_ramp() {
        let (mut coordinator, lives) = coordinator(2, 3);
        step(&mut coordinator, report(1, &[500_000; 100], 0), &lives);
        let limits = coordinator.limits;
        let deadline = SEC_NS + limits.judge_delay_ns + RAMP_REPORT_GRACE_NS;
        coordinator.advance(&lives, Some(deadline - 1));
        assert!(!all_stopped(&lives));
        coordinator.advance(&lives, Some(deadline));
        assert!(all_stopped(&lives));
        // The stalled shard's report, sent once it stopped, still counts.
        step(
            &mut coordinator,
            stopped_report(1, &[500_000; 50], 50, 0),
            &lives,
        );
        let outcome = coordinator.finish(&lives).unwrap();
        assert_eq!(verdicts(&outcome), [StepVerdict::ToolSaturated]);
        assert_eq!(outcome.steps[0].requests, 150);
        assert_eq!(outcome.max_sustainable_rate, None);
        assert!(
            outcome
                .stopped_by
                .contains("1 of 2 shard(s) did not report"),
            "{}",
            outcome.stopped_by
        );
    }

    #[test]
    fn a_ramp_that_ends_with_a_step_unreported_is_an_error() {
        let (mut coordinator, lives) = coordinator(1, 2);
        step(&mut coordinator, report(1, &[1_000; 100], 0), &lives);
        let err = coordinator.finish(&lives).unwrap_err();
        assert!(err.to_string().contains("step 2"), "{err}");
    }

    #[test]
    fn ramp_limits_follow_the_p99_limit_and_the_step_length() {
        let ms = 1_000_000;
        // The contract's ramp: 2 ms p99 limit, 20 s steps.
        assert_eq!(
            RampLimits::new(2 * ms, 20 * SEC_NS, 300 * SEC_NS),
            RampLimits {
                stop_p99_ns: 2 * ms,
                judge_delay_ns: 52 * ms,
                saturation_lag_ns: 100 * ms,
                request_timeout_ns: SEC_NS,
            }
        );
        assert_eq!(
            RampLimits::new(100 * ms, 20 * SEC_NS, 300 * SEC_NS),
            RampLimits {
                stop_p99_ns: 100 * ms,
                judge_delay_ns: 300 * ms,
                saturation_lag_ns: 100 * ms,
                request_timeout_ns: 10 * SEC_NS,
            }
        );
        // Short steps and a shorter configured timeout.
        assert_eq!(
            RampLimits::new(2 * ms, 300 * ms, 500 * ms),
            RampLimits {
                stop_p99_ns: 2 * ms,
                judge_delay_ns: 52 * ms,
                saturation_lag_ns: 30 * ms,
                request_timeout_ns: 500 * ms,
            }
        );
    }

    #[test]
    fn a_ramp_ends_when_its_last_step_can_be_judged() {
        use clap::Parser as _;
        let cli = crate::cli::Cli::try_parse_from([
            "brisk-loadgen",
            "nonstream",
            "http://h:1",
            "--ramp-start",
            "100",
            "--ramp-step-s",
            "2",
            "--ramp-max-steps",
            "3",
            "--warmup-s",
            "1",
            "--out",
            "o.json",
        ])
        .unwrap();
        let crate::cli::Command::Nonstream(cmd) = cli.command else {
            panic!("expected nonstream");
        };
        let limits = RampLimits::new(2_000_000, 2 * SEC_NS, 300 * SEC_NS);
        let (segments, end_ns, _) = nonstream_schedule(&cmd, 5, limits);
        assert_eq!(segments.len(), 4);
        assert_eq!(end_ns, 5 + 7 * SEC_NS + limits.judge_delay_ns);
    }
}
