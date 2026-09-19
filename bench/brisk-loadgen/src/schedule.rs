//! Open-loop schedules: when each request is due, independent of how the
//! server responds.
//!
//! - [`StreamArrivals`] (S1): the target concurrency is pre-populated at the
//!   start with streams whose durations follow the residual-life
//!   distribution, their connections spread over the first second; streams
//!   also arrive as a Poisson process from the start on.
//! - [`FixedArrivals`] (S2, S3): an evenly spaced grid at a fixed rate, or a
//!   sequence of rate segments for the S2 ramp.
//!
//! [`PlannedLoad`] tracks what the schedule implies (open streams, chunks
//! due per interval) so the validity check can compare the achieved load
//! with the offered one.

use std::cmp::Reverse;
use std::collections::BinaryHeap;

use brisk_bench_core::dist::{DistError, LogNormal, Poisson, seconds_to_ns};
use serde::Serialize;

use crate::cli::StreamShape;

/// Nanoseconds per second.
pub(crate) const SEC_NS: u64 = 1_000_000_000;
/// Window over which the pre-built streams are started.
const PREBUILD_SPREAD_NS: u64 = SEC_NS;

/// One scheduled request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Arrival {
    /// Planned start, monotonic nanoseconds.
    pub(crate) sched_ns: u64,
    /// Chunks requested (streaming responses).
    pub(crate) chunks: u32,
    /// Planned time from start to the last chunk.
    pub(crate) lifetime_ns: u64,
    /// Ramp step (0 outside ramp steps).
    pub(crate) step: u32,
}

/// Parameters of the S1 stream model derived from the command line.
#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub(crate) struct StreamPlan {
    /// Stream duration distribution.
    #[serde(skip)]
    pub(crate) dist: LogNormal,
    /// Target concurrency.
    pub(crate) concurrency: u32,
    /// Chunks per second within a stream.
    pub(crate) chunk_rate: f64,
    /// Chunk interval requested from the mock, microseconds.
    pub(crate) interval_us: u64,
    /// Mock delay before the first chunk, nanoseconds.
    pub(crate) ttft_ns: u64,
    /// Mean of the truncated duration distribution, seconds.
    pub(crate) mean_duration_s: f64,
    /// Mean planned lifetime `ttft + (chunks − 1) · interval`, seconds.
    pub(crate) mean_lifetime_s: f64,
    /// Arrival rate `concurrency / mean_lifetime_s`, per second.
    pub(crate) arrival_rate: f64,
    /// Offered chunk rate `arrival_rate · E[chunks]`, per second.
    pub(crate) offered_chunk_rate: f64,
}

impl StreamPlan {
    /// Derives the model for `concurrency` streams of the given shape.
    ///
    /// The contract turns a duration `D` into `chunks = D · chunk_rate`. The
    /// planned lifetime of such a stream is `ttft + (chunks − 1) · interval`,
    /// and by Little's law the arrival rate that keeps `concurrency` streams
    /// open is `concurrency / E[lifetime]`. With a TTFT this is below
    /// `concurrency / E[D]`, and the offered chunk rate is below
    /// `concurrency · chunk_rate`, because a stream emits nothing during its
    /// TTFT.
    pub(crate) fn new(shape: &StreamShape, concurrency: u32) -> Result<Self, DistError> {
        let dist = LogNormal::from_median_p99(shape.dur_median, shape.dur_p99, shape.dur_max)?;
        let interval_us = interval_us(shape.chunk_rate);
        let mean_duration_s = dist.mean();
        let mean_chunks = (mean_duration_s * shape.chunk_rate).max(1.0);
        let ttft_s = micros_to_s(shape.ttft_us);
        let mean_lifetime_s = ttft_s + (mean_chunks - 1.0) * micros_to_s(interval_us);
        let arrival_rate = f64::from(concurrency) / mean_lifetime_s;
        Ok(Self {
            dist,
            concurrency,
            chunk_rate: shape.chunk_rate,
            interval_us,
            ttft_ns: shape.ttft_us.saturating_mul(1_000),
            mean_duration_s,
            mean_lifetime_s,
            arrival_rate,
            offered_chunk_rate: arrival_rate * mean_chunks,
        })
    }

    /// Chunks for a stream of `duration_s` seconds: `round(D · rate)`, at
    /// least one.
    #[expect(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        reason = "clamped to [1, u32::MAX] before the cast"
    )]
    pub(crate) fn chunks_for(&self, duration_s: f64) -> u32 {
        (duration_s * self.chunk_rate)
            .round()
            .clamp(1.0, f64::from(u32::MAX)) as u32
    }

    /// Planned time from the request's start to its last chunk.
    pub(crate) fn lifetime_ns(&self, chunks: u32) -> u64 {
        self.ttft_ns + u64::from(chunks.saturating_sub(1)) * self.interval_us * 1_000
    }

    fn arrival(&self, sched_ns: u64, duration_s: f64) -> Arrival {
        let chunks = self.chunks_for(duration_s);
        Arrival {
            sched_ns,
            chunks,
            lifetime_ns: self.lifetime_ns(chunks),
            step: 0,
        }
    }
}

/// Chunk interval in whole microseconds for a chunk rate per second.
#[expect(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "the CLI bounds the rate to (0, 1e6], so the value is in [1, u64::MAX)"
)]
pub(crate) fn interval_us(chunk_rate: f64) -> u64 {
    (1e6 / chunk_rate).round().max(1.0) as u64
}

#[expect(
    clippy::cast_precision_loss,
    reason = "durations are far below 2^53 µs"
)]
fn micros_to_s(us: u64) -> f64 {
    us as f64 / 1e6
}

#[expect(clippy::cast_precision_loss, reason = "offsets are far below 2^53 ns")]
fn ns_to_s(ns: u64) -> f64 {
    ns as f64 / 1e9
}

/// Splits `total` into `parts` near-equal shares; share `index`.
pub(crate) fn share(total: u32, parts: usize, index: usize) -> u32 {
    let parts = u32::try_from(parts).expect("shard count fits u32");
    let index = u32::try_from(index).expect("shard index fits u32");
    total / parts + u32::from(index < total % parts)
}

/// The S1 arrivals of one shard.
#[derive(Debug)]
pub(crate) struct StreamArrivals {
    plan: StreamPlan,
    rng: fastrand::Rng,
    poisson: Option<Poisson>,
    origin_ns: u64,
    end_ns: u64,
    prebuilt: u32,
    prebuilt_next: u32,
    next_poisson_ns: u64,
}

impl StreamArrivals {
    /// Arrivals for a shard holding `concurrency` of the plan's streams,
    /// between `origin_ns` and `end_ns`.
    pub(crate) fn new(
        plan: StreamPlan,
        concurrency: u32,
        seed: u64,
        origin_ns: u64,
        end_ns: u64,
    ) -> Result<Self, DistError> {
        let poisson = if concurrency == 0 {
            None
        } else {
            Some(Poisson::new(f64::from(concurrency) / plan.mean_lifetime_s)?)
        };
        let mut rng = fastrand::Rng::with_seed(seed);
        let next_poisson_ns = match &poisson {
            Some(p) => origin_ns.saturating_add(p.next_interarrival_ns(&mut rng)),
            None => u64::MAX,
        };
        Ok(Self {
            plan,
            rng,
            poisson,
            origin_ns,
            end_ns,
            prebuilt: concurrency,
            prebuilt_next: 0,
            next_poisson_ns,
        })
    }

    fn next_prebuilt_ns(&self) -> Option<u64> {
        (self.prebuilt_next < self.prebuilt).then(|| {
            self.origin_ns
                + PREBUILD_SPREAD_NS * u64::from(self.prebuilt_next) / u64::from(self.prebuilt)
        })
    }

    /// The next arrival in time order, or `None` once past the end.
    ///
    /// A pre-built stream stands for one that is open at the origin with a
    /// residual life drawn from the equilibrium distribution. Spreading the
    /// starts only delays its connection, not its end: it runs for the
    /// residual life minus its start offset, and is skipped if that is
    /// already over. Together with Poisson arrivals from the origin on this
    /// keeps the expected number of open streams at the target from the end
    /// of the spread window on.
    pub(crate) fn next_arrival(&mut self) -> Option<Arrival> {
        loop {
            let arrival = match self.next_prebuilt_ns() {
                Some(t) if t <= self.next_poisson_ns => {
                    self.prebuilt_next += 1;
                    let residual = self.plan.dist.sample_residual(&mut self.rng);
                    let elapsed = ns_to_s(t - self.origin_ns);
                    if residual <= elapsed {
                        continue;
                    }
                    self.plan.arrival(t, residual - elapsed)
                }
                _ => {
                    let poisson = self.poisson?;
                    let t = self.next_poisson_ns;
                    self.next_poisson_ns =
                        t.saturating_add(poisson.next_interarrival_ns(&mut self.rng));
                    let duration = self.plan.dist.sample(&mut self.rng);
                    self.plan.arrival(t, duration)
                }
            };
            return (arrival.sched_ns < self.end_ns).then_some(arrival);
        }
    }
}

/// A constant-rate stretch of a [`FixedArrivals`] schedule.
#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub(crate) struct RateSegment {
    /// First instant of the segment.
    pub(crate) start_ns: u64,
    /// End (exclusive).
    pub(crate) end_ns: u64,
    /// Requests per second (all shards together).
    pub(crate) rate: f64,
    /// Ramp step number; 0 for the warmup or a fixed-rate run.
    pub(crate) step: u32,
}

/// Builds the S2 ramp: a warmup at `start_rate`, then `max_steps` steps of
/// `step_ns`, each `step_pct` percent faster than the previous one.
pub(crate) fn ramp_segments(
    origin_ns: u64,
    warmup_ns: u64,
    start_rate: f64,
    step_pct: f64,
    step_ns: u64,
    max_steps: u32,
) -> Vec<RateSegment> {
    let mut segments = Vec::new();
    if warmup_ns > 0 {
        segments.push(RateSegment {
            start_ns: origin_ns,
            end_ns: origin_ns + warmup_ns,
            rate: start_rate,
            step: 0,
        });
    }
    let mut start = origin_ns + warmup_ns;
    let mut rate = start_rate;
    for step in 1..=max_steps {
        segments.push(RateSegment {
            start_ns: start,
            end_ns: start + step_ns,
            rate,
            step,
        });
        start += step_ns;
        rate *= 1.0 + step_pct / 100.0;
    }
    segments
}

/// Evenly spaced arrivals over rate segments. The grid is global; shard `i`
/// of `n` takes every `n`-th point starting at the `i`-th, so all shards
/// together send exactly at the segment rate.
#[derive(Debug)]
pub(crate) struct FixedArrivals {
    segments: Vec<RateSegment>,
    segment: usize,
    k: u64,
    global: u64,
    shard: u64,
    shards: u64,
    chunks: u32,
}

impl FixedArrivals {
    /// Arrivals of shard `shard` of `shards`; every request asks for
    /// `chunks` chunks.
    pub(crate) fn new(
        segments: Vec<RateSegment>,
        shard: usize,
        shards: usize,
        chunks: u32,
    ) -> Self {
        Self {
            segments,
            segment: 0,
            k: 0,
            global: 0,
            shard: shard as u64,
            shards: shards as u64,
            chunks,
        }
    }

    /// The next arrival, or `None` after the last segment.
    pub(crate) fn next_arrival(&mut self) -> Option<Arrival> {
        loop {
            let seg = *self.segments.get(self.segment)?;
            #[expect(clippy::cast_precision_loss, reason = "k stays far below 2^53")]
            let offset = seconds_to_ns(self.k as f64 / seg.rate);
            let t = seg.start_ns.saturating_add(offset);
            if t >= seg.end_ns {
                self.segment += 1;
                self.k = 0;
                continue;
            }
            self.k += 1;
            let index = self.global;
            self.global += 1;
            if index % self.shards == self.shard {
                return Some(Arrival {
                    sched_ns: t,
                    chunks: self.chunks,
                    lifetime_ns: 0,
                    step: seg.step,
                });
            }
        }
    }
}

/// Offered load of one interval.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, serde::Deserialize)]
pub(crate) struct PlannedInterval {
    /// Interval index.
    pub(crate) index: u64,
    /// Mean number of streams the schedule has open.
    pub(crate) concurrency: f64,
    /// Chunks the schedule has due in the interval.
    pub(crate) chunks: u64,
}

/// Tracks the load the schedule offers, sampled at the same instants as the
/// achieved concurrency.
#[derive(Debug)]
pub(crate) struct PlannedLoad {
    origin_ns: u64,
    interval_ns: u64,
    open: u64,
    ends: BinaryHeap<Reverse<u64>>,
    concurrency: Vec<(f64, u64)>,
    chunks: Vec<u64>,
}

impl PlannedLoad {
    /// Covers `intervals` intervals of `interval_ns` from `origin_ns`.
    pub(crate) fn new(origin_ns: u64, interval_ns: u64, intervals: usize) -> Self {
        Self {
            origin_ns,
            interval_ns,
            open: 0,
            ends: BinaryHeap::new(),
            concurrency: vec![(0.0, 0); intervals],
            chunks: vec![0; intervals],
        }
    }

    fn index_of(&self, t_ns: u64) -> Option<usize> {
        let idx = usize::try_from(t_ns.checked_sub(self.origin_ns)? / self.interval_ns).ok()?;
        (idx < self.chunks.len()).then_some(idx)
    }

    /// Registers a started stream whose `chunks` chunks are due at
    /// `first_ns + k · step_ns`.
    pub(crate) fn start(&mut self, arrival: &Arrival, first_ns: u64, step_ns: u64) {
        self.open += 1;
        self.ends.push(Reverse(
            arrival.sched_ns.saturating_add(arrival.lifetime_ns),
        ));
        self.add_progression(first_ns, step_ns, arrival.chunks);
    }

    /// Adds the chunk instants `first_ns + k · step_ns`, `k < n`, to the
    /// per-interval counts.
    fn add_progression(&mut self, first_ns: u64, step_ns: u64, n: u32) {
        if n == 0 {
            return;
        }
        let Some(mut idx) = self.index_of(first_ns) else {
            return;
        };
        if step_ns == 0 {
            self.chunks[idx] += u64::from(n);
            return;
        }
        // Number of k >= 0 with first + k·step < bound.
        let below = |bound: u64| bound.saturating_sub(first_ns).div_ceil(step_ns);
        let n = u64::from(n);
        let mut counted = 0;
        while counted < n && idx < self.chunks.len() {
            let end = self.origin_ns + (idx as u64 + 1) * self.interval_ns;
            let upto = below(end).min(n);
            self.chunks[idx] += upto - counted;
            counted = upto;
            idx += 1;
        }
    }

    /// Samples the planned open-stream count at `now_ns` and returns it.
    pub(crate) fn sample(&mut self, now_ns: u64) -> u64 {
        while self.ends.peek().is_some_and(|Reverse(end)| *end <= now_ns) {
            self.ends.pop();
            self.open -= 1;
        }
        if let Some(idx) = self.index_of(now_ns) {
            #[expect(clippy::cast_precision_loss, reason = "stream counts are small")]
            let open = self.open as f64;
            let slot = &mut self.concurrency[idx];
            slot.0 += open;
            slot.1 += 1;
        }
        self.open
    }

    /// The per-interval offered load.
    pub(crate) fn finish(self) -> Vec<PlannedInterval> {
        self.concurrency
            .into_iter()
            .zip(self.chunks)
            .zip(0u64..)
            .map(|(((sum, samples), chunks), index)| PlannedInterval {
                index,
                concurrency: mean(sum, samples),
                chunks,
            })
            .collect()
    }
}

/// `sum / samples`, or 0 without samples.
#[expect(clippy::cast_precision_loss, reason = "sample counts are small")]
fn mean(sum: f64, samples: u64) -> f64 {
    if samples == 0 {
        0.0
    } else {
        sum / samples as f64
    }
}

/// Sums the planned load of several shards interval by interval.
pub(crate) fn merge_planned(shards: Vec<Vec<PlannedInterval>>) -> Vec<PlannedInterval> {
    let mut merged: Vec<PlannedInterval> = Vec::new();
    for shard in shards {
        for p in shard {
            let idx = usize::try_from(p.index).expect("interval index fits usize");
            if merged.len() <= idx {
                merged.extend(
                    (merged.len() as u64..=p.index).map(|index| PlannedInterval {
                        index,
                        concurrency: 0.0,
                        chunks: 0,
                    }),
                );
            }
            merged[idx].concurrency += p.concurrency;
            merged[idx].chunks += p.chunks;
        }
    }
    merged
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shape(ttft_us: u64) -> StreamShape {
        StreamShape {
            concurrency: 1000,
            chunk_rate: 30.0,
            dur_median: 5.0,
            dur_p99: 60.0,
            dur_max: 120.0,
            ttft_us,
            chunk_bytes: 128,
            prompt_bytes: 16,
            include_usage: false,
        }
    }

    #[test]
    fn plan_follows_littles_law() {
        let plan = StreamPlan::new(&shape(0), 1000).unwrap();
        assert!((8.5..=8.9).contains(&plan.mean_duration_s));
        assert_eq!(plan.interval_us, 33_333);
        // One interval shorter than the duration: the last chunk is at
        // (chunks − 1) · interval.
        assert!((plan.mean_lifetime_s - (plan.mean_duration_s - 0.033_333)).abs() < 1e-3);
        assert!((plan.arrival_rate * plan.mean_lifetime_s - 1000.0).abs() < 1e-6);
        assert!(
            (112.0..=119.0).contains(&plan.arrival_rate),
            "{}",
            plan.arrival_rate
        );

        let with_ttft = StreamPlan::new(&shape(300_000), 1000).unwrap();
        assert!((with_ttft.mean_lifetime_s - plan.mean_lifetime_s - 0.3).abs() < 1e-9);
        assert!(with_ttft.offered_chunk_rate < 1000.0 * 30.0);
        assert_eq!(with_ttft.lifetime_ns(1), 300_000_000);
        assert_eq!(with_ttft.lifetime_ns(3), 300_000_000 + 2 * 33_333_000);
    }

    #[test]
    fn chunks_round_and_never_drop_below_one() {
        let plan = StreamPlan::new(&shape(0), 1).unwrap();
        assert_eq!(plan.chunks_for(0.0), 1);
        assert_eq!(plan.chunks_for(0.01), 1);
        assert_eq!(plan.chunks_for(5.0), 150);
        assert_eq!(plan.chunks_for(5.02), 151);
        assert_eq!(plan.chunks_for(120.0), 3600);
    }

    #[test]
    fn shares_cover_the_total() {
        for (total, parts) in [(1000, 1), (1000, 3), (5, 8), (7, 7)] {
            let shares: Vec<u32> = (0..parts).map(|i| share(total, parts, i)).collect();
            assert_eq!(shares.iter().sum::<u32>(), total);
            assert!(shares.iter().max().unwrap() - shares.iter().min().unwrap() <= 1);
        }
    }

    #[test]
    fn stream_arrivals_prebuild_then_poisson() {
        let plan = StreamPlan::new(&shape(0), 200).unwrap();
        let origin = 10 * SEC_NS;
        let end = origin + 400 * SEC_NS;
        let mut arrivals = StreamArrivals::new(plan, 200, 42, origin, end).unwrap();
        let all: Vec<Arrival> = std::iter::from_fn(|| arrivals.next_arrival()).collect();
        assert!(all.windows(2).all(|w| w[0].sched_ns <= w[1].sched_ns));
        assert!(all.iter().all(|a| a.sched_ns >= origin && a.sched_ns < end));
        assert!(all.iter().all(|a| a.chunks >= 1 && a.chunks <= 3600));
        // Pre-built streams whose residual life ends within their start
        // offset are skipped; most of the 200 survive.
        let in_first_second = all.iter().filter(|a| a.sched_ns < origin + SEC_NS).count();
        assert!((150..=260).contains(&in_first_second), "{in_first_second}");
        // Poisson part after the first second: rate 200 / E[lifetime].
        let later = all.iter().filter(|a| a.sched_ns >= origin + SEC_NS).count();
        #[expect(clippy::cast_precision_loss, reason = "test counts are small")]
        let rate = later as f64 / 399.0;
        assert!(
            (rate / plan.arrival_rate - 1.0).abs() < 0.05,
            "rate {rate} vs {}",
            plan.arrival_rate
        );
        // Deterministic for a seed.
        let mut again = StreamArrivals::new(plan, 200, 42, origin, end).unwrap();
        assert_eq!(again.next_arrival(), Some(all[0]));
    }

    #[test]
    fn planned_concurrency_matches_target_in_steady_state() {
        let target = 300;
        let plan = StreamPlan::new(&shape(0), target).unwrap();
        let origin = 0;
        let secs = 600;
        let end = origin + secs * SEC_NS;
        let mut arrivals = StreamArrivals::new(plan, target, 7, origin, end).unwrap();
        let mut planned = PlannedLoad::new(origin, SEC_NS, usize::try_from(secs).unwrap());
        let mut next = arrivals.next_arrival();
        let mut t = origin;
        while t < end {
            while let Some(a) = next.filter(|a| a.sched_ns <= t) {
                planned.start(&a, a.sched_ns, plan.interval_us * 1_000);
                next = arrivals.next_arrival();
            }
            planned.sample(t);
            t += 10_000_000;
        }
        let intervals = planned.finish();
        // After a warmup longer than the longest stream the mean over the
        // rest sits at the target; the Poisson process itself fluctuates by
        // about sqrt(target) around it.
        let steady: Vec<f64> = intervals[150..].iter().map(|p| p.concurrency).collect();
        #[expect(clippy::cast_precision_loss, reason = "test counts are small")]
        let mean = steady.iter().sum::<f64>() / steady.len() as f64;
        assert!((mean / f64::from(target) - 1.0).abs() < 0.05, "mean {mean}");
        let chunks: u64 = intervals[150..].iter().map(|p| p.chunks).sum();
        #[expect(clippy::cast_precision_loss, reason = "test counts are small")]
        let chunk_rate = chunks as f64 / 450.0;
        assert!(
            (chunk_rate / plan.offered_chunk_rate - 1.0).abs() < 0.05,
            "chunk rate {chunk_rate} vs {}",
            plan.offered_chunk_rate
        );
    }

    #[test]
    fn prebuilt_streams_hold_the_target_from_the_first_second_on() {
        let short = StreamShape {
            chunk_rate: 20.0,
            dur_median: 0.5,
            dur_p99: 2.0,
            dur_max: 3.0,
            ttft_us: 20_000,
            ..shape(0)
        };
        let target = 10_000;
        let plan = StreamPlan::new(&short, target).unwrap();
        let end = 6 * SEC_NS;
        let mut arrivals = StreamArrivals::new(plan, target, 3, 0, end).unwrap();
        let mut planned = PlannedLoad::new(0, SEC_NS, 6);
        let mut next = arrivals.next_arrival();
        let mut t = 0;
        while t < end {
            while let Some(a) = next.filter(|a| a.sched_ns <= t) {
                planned.start(&a, a.sched_ns, plan.interval_us * 1_000);
                next = arrivals.next_arrival();
            }
            planned.sample(t);
            t += 10_000_000;
        }
        // No overshoot once the spread window is over; the Poisson noise
        // at 10 000 streams is about 1%.
        for p in &planned.finish()[1..] {
            let dev = p.concurrency / f64::from(target) - 1.0;
            assert!(dev.abs() < 0.04, "{p:?}");
        }
    }

    #[test]
    fn progression_counts_per_interval() {
        let mut planned = PlannedLoad::new(1_000, 100, 5);
        // Instants 1050, 1080, ..., 1050 + 9·30 = 1320.
        planned.add_progression(1_050, 30, 10);
        // Intervals: [1000,1100): 1050,1080 → 2; [1100,1200): 1110,1140,1170 → 3;
        // [1200,1300): 1200,1230,1260,1290 → 4; [1300,1400): 1320 → 1.
        let chunks: Vec<u64> = planned.finish().iter().map(|p| p.chunks).collect();
        assert_eq!(chunks, [2, 3, 4, 1, 0]);

        let mut planned = PlannedLoad::new(0, 100, 2);
        planned.add_progression(150, 100, 5);
        planned.add_progression(50, 0, 3);
        planned.add_progression(5_000, 1, 3);
        let chunks: Vec<u64> = planned.finish().iter().map(|p| p.chunks).collect();
        assert_eq!(chunks, [3, 1]);
    }

    #[test]
    fn planned_open_expires_at_lifetime_end() {
        let mut planned = PlannedLoad::new(0, 1_000, 3);
        let a = Arrival {
            sched_ns: 100,
            chunks: 2,
            lifetime_ns: 1_000,
            step: 0,
        };
        planned.start(&a, 100, 1_000);
        assert_eq!(planned.sample(500), 1);
        assert_eq!(planned.sample(1_099), 1);
        assert_eq!(planned.sample(1_100), 0);
        let merged = merge_planned(vec![
            planned.finish(),
            vec![PlannedInterval {
                index: 4,
                concurrency: 2.0,
                chunks: 3,
            }],
        ]);
        assert_eq!(merged.len(), 5);
        assert!((merged[0].concurrency - 1.0).abs() < 1e-12);
        assert!((merged[1].concurrency - 0.5).abs() < 1e-12);
        assert_eq!(merged[4].chunks, 3);
        assert_eq!(merged[0].chunks + merged[1].chunks, 2);
    }

    #[test]
    fn fixed_arrivals_interleave_shards() {
        let segments = vec![RateSegment {
            start_ns: 0,
            end_ns: SEC_NS,
            rate: 10.0,
            step: 0,
        }];
        let collect = |shard, shards| {
            let mut a = FixedArrivals::new(segments.clone(), shard, shards, 3);
            std::iter::from_fn(move || a.next_arrival())
                .map(|a| a.sched_ns)
                .collect::<Vec<_>>()
        };
        let all = collect(0, 1);
        assert_eq!(all, (0..10).map(|k| k * 100_000_000).collect::<Vec<_>>());
        let mut merged = collect(0, 3);
        merged.extend(collect(1, 3));
        merged.extend(collect(2, 3));
        merged.sort_unstable();
        assert_eq!(merged, all);
    }

    #[test]
    fn ramp_segments_grow_geometrically() {
        let segs = ramp_segments(100, 5 * SEC_NS, 100.0, 10.0, 2 * SEC_NS, 3);
        assert_eq!(segs.len(), 4);
        assert_eq!(segs[0].step, 0);
        assert_eq!(
            (segs[1].start_ns, segs[1].end_ns),
            (100 + 5 * SEC_NS, 100 + 7 * SEC_NS)
        );
        assert!((segs[1].rate - 100.0).abs() < 1e-9);
        assert!((segs[2].rate - 110.0).abs() < 1e-9);
        assert!((segs[3].rate - 121.0).abs() < 1e-9);
        let mut arrivals = FixedArrivals::new(segs, 0, 1, 1);
        let steps: Vec<u32> = std::iter::from_fn(|| arrivals.next_arrival())
            .map(|a| a.step)
            .collect();
        assert_eq!(steps.iter().filter(|&&s| s == 0).count(), 500);
        assert_eq!(steps.iter().filter(|&&s| s == 1).count(), 200);
        assert_eq!(steps.iter().filter(|&&s| s == 2).count(), 220);
        assert_eq!(steps.iter().filter(|&&s| s == 3).count(), 242);
        assert_eq!(ramp_segments(0, 0, 1.0, 10.0, 1, 2)[0].step, 1);
    }
}
