//! Nanosecond clocks shared by every benchmark process.
//!
//! On Linux every timestamp is `CLOCK_MONOTONIC`, so values taken by the mock
//! and by the load generator on the same host are directly comparable. Kernel
//! receive timestamps (`SO_TIMESTAMPNS`) are `CLOCK_REALTIME`; [`RealtimeOffset`]
//! converts them onto the monotonic timeline.
//!
//! Other platforms fall back to nanoseconds since the UNIX epoch for both
//! clocks. That keeps the arithmetic correct for functional tests but gives no
//! precision guarantee.

/// Number of mono/real/mono triples taken per offset estimate.
const ESTIMATE_SAMPLES: usize = 16;

/// Largest change between two consecutive estimates that is still treated as
/// measurement noise rather than a clock step.
pub const STEP_THRESHOLD_NS: u64 = 20_000;

/// Returns the current monotonic time in nanoseconds.
///
/// Linux: `clock_gettime(CLOCK_MONOTONIC)`; comparable across processes on the
/// same machine. Elsewhere: nanoseconds since the UNIX epoch.
#[inline]
pub fn now_ns() -> u64 {
    imp::monotonic_ns()
}

/// Returns the current wall-clock (`CLOCK_REALTIME`) time in nanoseconds since
/// the UNIX epoch.
#[inline]
pub fn realtime_ns() -> u64 {
    imp::realtime_ns()
}

/// One offset estimate together with the width of the monotonic window it was
/// bracketed by.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OffsetEstimate {
    /// `CLOCK_REALTIME − CLOCK_MONOTONIC` in nanoseconds.
    pub offset_ns: i128,
    /// Distance between the two monotonic reads that bracket the realtime
    /// read; an upper bound on the estimate's error.
    pub uncertainty_ns: u64,
}

/// Estimates `CLOCK_REALTIME − CLOCK_MONOTONIC` once.
///
/// Reads mono, real, mono [`ESTIMATE_SAMPLES`] times and keeps the triple with
/// the narrowest monotonic bracket; the offset is `real − (m1 + m2) / 2`.
pub fn estimate_offset() -> OffsetEstimate {
    let mut best = OffsetEstimate {
        offset_ns: 0,
        uncertainty_ns: u64::MAX,
    };
    for _ in 0..ESTIMATE_SAMPLES {
        let m1 = now_ns();
        let real = realtime_ns();
        let m2 = now_ns();
        let gap = m2.saturating_sub(m1);
        if gap < best.uncertainty_ns {
            let mid = i128::midpoint(i128::from(m1), i128::from(m2));
            best = OffsetEstimate {
                offset_ns: i128::from(real) - mid,
                uncertainty_ns: gap,
            };
        }
    }
    best
}

/// Converts `CLOCK_REALTIME` timestamps (such as `SO_TIMESTAMPNS`) into the
/// `CLOCK_MONOTONIC` timeline used by [`now_ns`].
///
/// Call [`refresh`](Self::refresh) about once per second. When two consecutive
/// estimates differ by more than [`STEP_THRESHOLD_NS`] the wall clock was
/// stepped or slewed hard; [`step_detected`](Self::step_detected) latches so
/// the caller can invalidate the run.
#[derive(Debug, Clone)]
pub struct RealtimeOffset {
    current: OffsetEstimate,
    step_detected: bool,
    steps: u64,
}

impl RealtimeOffset {
    /// Takes the initial estimate.
    pub fn new() -> Self {
        Self {
            current: estimate_offset(),
            step_detected: false,
            steps: 0,
        }
    }

    /// Re-estimates the offset. Returns `true` when this refresh detected a
    /// clock step.
    ///
    /// On a step the new estimate still replaces the old one: the run is
    /// already invalid, and following the real clock keeps later conversions
    /// meaningful and prevents one step from being reported on every
    /// subsequent refresh.
    pub fn refresh(&mut self) -> bool {
        self.apply(estimate_offset())
    }

    fn apply(&mut self, next: OffsetEstimate) -> bool {
        let stepped =
            next.offset_ns.abs_diff(self.current.offset_ns) > u128::from(STEP_THRESHOLD_NS);
        if stepped {
            self.step_detected = true;
            self.steps += 1;
        }
        self.current = next;
        stepped
    }

    /// Converts a realtime timestamp in nanoseconds to monotonic nanoseconds.
    ///
    /// Saturates at `0` and `u64::MAX` instead of wrapping, which only happens
    /// for timestamps that cannot belong to this boot.
    #[inline]
    pub fn to_mono(&self, realtime_ns: u64) -> u64 {
        let mono = i128::from(realtime_ns) - self.current.offset_ns;
        u64::try_from(mono.max(0)).unwrap_or(u64::MAX)
    }

    /// The offset currently used by [`to_mono`](Self::to_mono).
    pub fn offset_ns(&self) -> i128 {
        self.current.offset_ns
    }

    /// Uncertainty of the current estimate in nanoseconds.
    pub fn uncertainty_ns(&self) -> u64 {
        self.current.uncertainty_ns
    }

    /// Whether any refresh since construction detected a clock step.
    pub fn step_detected(&self) -> bool {
        self.step_detected
    }

    /// Number of refreshes that detected a step.
    pub fn steps(&self) -> u64 {
        self.steps
    }
}

impl Default for RealtimeOffset {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(target_os = "linux")]
mod imp {
    use std::mem::MaybeUninit;

    #[inline]
    fn read_clock(clock: libc::clockid_t) -> u64 {
        let mut ts = MaybeUninit::<libc::timespec>::uninit();
        // SAFETY: `ts` is a valid, writable timespec and `clock` is one of the
        // always-available clock ids, so clock_gettime cannot fail or read
        // uninitialised memory; it fully initialises `ts` on success.
        let rc = unsafe { libc::clock_gettime(clock, ts.as_mut_ptr()) };
        assert_eq!(rc, 0, "clock_gettime failed for a mandatory clock");
        // SAFETY: rc == 0 guarantees the kernel wrote the whole struct.
        let ts = unsafe { ts.assume_init() };
        timespec_to_ns(&ts)
    }

    /// Converts a kernel `timespec` to nanoseconds; negative values clamp to 0.
    #[inline]
    pub(crate) fn timespec_to_ns(ts: &libc::timespec) -> u64 {
        let secs = u64::try_from(ts.tv_sec).unwrap_or(0);
        let nanos = u64::try_from(ts.tv_nsec).unwrap_or(0);
        secs * 1_000_000_000 + nanos
    }

    #[inline]
    pub(super) fn monotonic_ns() -> u64 {
        read_clock(libc::CLOCK_MONOTONIC)
    }

    #[inline]
    pub(super) fn realtime_ns() -> u64 {
        read_clock(libc::CLOCK_REALTIME)
    }
}

#[cfg(not(target_os = "linux"))]
mod imp {
    use std::time::{SystemTime, UNIX_EPOCH};

    #[inline]
    fn epoch_ns() -> u64 {
        let since = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock is set before the UNIX epoch");
        u64::try_from(since.as_nanos()).expect("system clock is beyond year 2554")
    }

    #[inline]
    pub(super) fn monotonic_ns() -> u64 {
        epoch_ns()
    }

    #[inline]
    pub(super) fn realtime_ns() -> u64 {
        epoch_ns()
    }
}

#[cfg(target_os = "linux")]
pub(crate) use imp::timespec_to_ns;

#[cfg(test)]
mod tests {
    use super::*;

    // Only CLOCK_MONOTONIC promises this; the epoch fallback may be adjusted.
    #[cfg(target_os = "linux")]
    #[test]
    fn now_is_non_decreasing() {
        let mut prev = now_ns();
        for _ in 0..10_000 {
            let next = now_ns();
            assert!(next >= prev, "{next} < {prev}");
            prev = next;
        }
    }

    #[test]
    fn estimate_is_tight() {
        let est = estimate_offset();
        // 16 back-to-back clock reads always have one bracket far below 1 ms,
        // even on a loaded VM.
        assert!(est.uncertainty_ns < 1_000_000, "{est:?}");
    }

    #[test]
    fn to_mono_maps_realtime_now_to_monotonic_now() {
        let offset = RealtimeOffset::new();
        let mono_before = now_ns();
        let converted = offset.to_mono(realtime_ns());
        let mono_after = now_ns();
        let slack = 1_000_000;
        assert!(
            converted + slack >= mono_before && converted <= mono_after + slack,
            "converted={converted} window=[{mono_before}, {mono_after}]"
        );
    }

    fn estimate(offset_ns: i128) -> OffsetEstimate {
        OffsetEstimate {
            offset_ns,
            uncertainty_ns: 100,
        }
    }

    #[test]
    fn small_drift_is_not_a_step_and_large_jump_latches() {
        let mut offset = RealtimeOffset {
            current: estimate(1_000_000),
            step_detected: false,
            steps: 0,
        };
        assert!(!offset.apply(estimate(1_000_000 + 19_999)));
        assert!(!offset.step_detected());
        assert_eq!(offset.offset_ns(), 1_019_999);

        assert!(offset.apply(estimate(1_019_999 - 20_001)));
        assert!(offset.step_detected());
        assert_eq!(offset.offset_ns(), 999_998);

        // The flag latches; the next quiet refresh does not count again.
        assert!(!offset.apply(estimate(999_998)));
        assert!(offset.step_detected());
        assert_eq!(offset.steps(), 1);
    }

    #[test]
    fn to_mono_saturates() {
        let offset = RealtimeOffset {
            current: estimate(1_000),
            step_detected: false,
            steps: 0,
        };
        assert_eq!(offset.to_mono(10), 0);
        assert_eq!(offset.to_mono(5_000), 4_000);
    }
}
