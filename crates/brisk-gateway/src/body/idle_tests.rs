//! Tests of [`IdleClock`] on tokio's paused clock. They read the clock to
//! place bytes and measure expiry, so they live outside `idle.rs`, which the
//! R9 check of `scripts/check-hotpath.sh` covers line by line.

use std::task::Waker;

use super::*;

const IDLE: Duration = Duration::from_millis(400);
const PERIOD: Duration = Duration::from_millis(100);
const STEP: Duration = Duration::from_millis(1);

fn timing(first_byte_deadline: Option<Instant>) -> BodyTiming {
    BodyTiming {
        // Whatever deadline `forward` left behind; the clock must replace it.
        timer: Box::pin(tokio::time::sleep(Duration::from_secs(3600))),
        first_byte_deadline,
        idle: IDLE,
    }
}

fn poll(clock: &mut IdleClock) -> Poll<Expired> {
    clock.poll_expired(&mut Context::from_waker(Waker::noop()))
}

/// Advances the paused clock in 1 ms steps, delivering bytes at the given
/// offsets from the start, until the clock expires or `limit` passes.
async fn run(
    clock: &mut IdleClock,
    bytes_at: &[Duration],
    limit: Duration,
) -> Option<(Expired, Duration)> {
    let start = Instant::now();
    let mut pending = bytes_at.iter().copied().peekable();
    loop {
        let elapsed = start.elapsed();
        while pending.next_if(|at| *at <= elapsed).is_some() {
            clock.on_bytes();
        }
        if let Poll::Ready(expired) = poll(clock) {
            return Some((expired, elapsed));
        }
        if elapsed >= limit {
            return None;
        }
        tokio::time::advance(STEP).await;
    }
}

fn assert_idle_window(expired: Option<(Expired, Duration)>, last_byte: Duration) {
    let (kind, at) = expired.expect("the clock should expire");
    assert_eq!(kind, Expired::Idle(IDLE));
    let silence = at
        .checked_sub(last_byte)
        .expect("the clock expired before the last byte");
    assert!(
        silence >= IDLE && silence < IDLE + PERIOD,
        "idle timeout after {silence:?} of silence, expected within [{IDLE:?}, {:?})",
        IDLE + PERIOD
    );
}

#[tokio::test(start_paused = true)]
async fn first_byte_deadline_fires() {
    let deadline = Instant::now() + Duration::from_millis(250);
    let mut clock = IdleClock::new(timing(Some(deadline)));
    let (kind, at) = run(&mut clock, &[], Duration::from_secs(5))
        .await
        .expect("the first-byte deadline should fire");
    assert_eq!(kind, Expired::FirstByte);
    assert_eq!(at, Duration::from_millis(250));
}

#[tokio::test(start_paused = true)]
async fn first_byte_deadline_in_the_past_fires_at_once() {
    let deadline = Instant::now();
    tokio::time::advance(Duration::from_millis(10)).await;
    let mut clock = IdleClock::new(timing(Some(deadline)));
    assert_eq!(poll(&mut clock), Poll::Ready(Expired::FirstByte));
}

#[tokio::test(start_paused = true)]
async fn first_byte_switches_to_idle_mode() {
    let deadline = Instant::now() + Duration::from_millis(250);
    let mut clock = IdleClock::new(timing(Some(deadline)));
    let first = Duration::from_millis(200);
    let expired = run(&mut clock, &[first], Duration::from_secs(5)).await;
    // Measured from the first byte, not from the first-byte deadline.
    assert_idle_window(expired, first);
}

#[tokio::test(start_paused = true)]
async fn committed_with_bytes_starts_in_idle_mode() {
    let mut clock = IdleClock::new(timing(None));
    let expired = run(&mut clock, &[], Duration::from_secs(5)).await;
    assert_idle_window(expired, Duration::ZERO);
}

#[tokio::test(start_paused = true)]
async fn steady_bytes_never_expire() {
    let mut clock = IdleClock::new(timing(None));
    let bytes: Vec<Duration> = (1..=50).map(|i| Duration::from_millis(i * 90)).collect();
    let expired = run(&mut clock, &bytes, Duration::from_millis(4500)).await;
    assert_eq!(expired, None);
}

#[tokio::test(start_paused = true)]
async fn idle_window_holds_for_every_byte_phase() {
    // The last byte lands at every millisecond offset within a period, so
    // both ends of the `[idle, 1.25 * idle)` window are exercised.
    for phase in 0..100 {
        let mut clock = IdleClock::new(timing(None));
        let last = Duration::from_millis(1000 + phase);
        let bytes = [Duration::from_millis(300), Duration::from_millis(700), last];
        let expired = run(&mut clock, &bytes, Duration::from_secs(5)).await;
        assert_idle_window(expired, last);
    }
}

#[tokio::test(start_paused = true)]
async fn starved_periods_are_not_counted_as_silence() {
    let mut clock = IdleClock::new(timing(None));
    assert!(poll(&mut clock).is_pending());
    // The upstream delivered bytes, but the body was not polled again for
    // far longer than the idle timeout (client backpressure).
    clock.on_bytes();
    tokio::time::advance(Duration::from_secs(10)).await;
    assert!(poll(&mut clock).is_pending());
    // Counting restarts at the resumption, as if a byte had arrived then.
    let expired = run(&mut clock, &[], Duration::from_secs(5)).await;
    assert_idle_window(expired, Duration::ZERO);
}

#[tokio::test(start_paused = true)]
async fn zero_idle_expires_immediately() {
    let mut clock = IdleClock::new(BodyTiming {
        idle: Duration::ZERO,
        ..timing(None)
    });
    assert_eq!(poll(&mut clock), Poll::Ready(Expired::Idle(Duration::ZERO)));
}
