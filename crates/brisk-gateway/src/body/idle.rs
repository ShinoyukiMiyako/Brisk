//! First-byte and idle detection for a committed body on the request's only
//! timer (D15, D31).
//!
//! Resetting a timer locks a shard of tokio's timer wheel, so the per-chunk
//! path never touches it: [`IdleClock::on_bytes`] only sets a flag. The timer
//! fires once per period (`idle / 4`); a period in which no byte arrived is
//! silent, and four silent periods in a row end the stream. The last byte
//! therefore lies at most one period before the start of the silent run, and
//! the timeout fires within `[idle, 1.25 * idle)` of it; exactly
//! `1.25 * idle` only for a byte that arrives at the very instant a period
//! begins, such as the instant of commit on a paused test clock.

use std::future::Future;
use std::task::{Context, Poll};
use std::time::Duration;

use tokio::time::Instant;

use super::BodyTiming;

/// Silent periods in a row that make up the idle timeout.
const SILENT_PERIODS: u32 = 4;

/// Why the clock ended the stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Expired {
    /// No body byte arrived before the first-byte deadline.
    FirstByte,
    /// No body byte arrived for the carried idle timeout.
    Idle(Duration),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    /// Committed before any body byte; the timer holds the first-byte deadline.
    FirstByte,
    /// Bytes have arrived; the timer fires at the end of each period.
    Idle,
}

/// Watches a committed body for the first-byte deadline and, after the first
/// byte, for idle periods.
#[derive(Debug)]
pub(crate) struct IdleClock {
    timing: BodyTiming,
    mode: Mode,
    period: Duration,
    /// A byte arrived since the timer last fired.
    chunk_seen: bool,
    /// Periods in a row that ended without a byte.
    silent_ticks: u32,
}

impl IdleClock {
    /// Takes over `timing.timer` and resets it; allocates nothing.
    pub(crate) fn new(timing: BodyTiming) -> Self {
        let period = timing.idle / SILENT_PERIODS;
        let mut clock = Self {
            timing,
            mode: Mode::FirstByte,
            period,
            chunk_seen: false,
            silent_ticks: 0,
        };
        match clock.timing.first_byte_deadline {
            Some(deadline) => clock.timing.timer.as_mut().reset(deadline),
            None => clock.start_idle(),
        }
        clock
    }

    /// Marks that bytes arrived; no clock read except on the first call.
    pub(crate) fn on_bytes(&mut self) {
        match self.mode {
            Mode::Idle => self.chunk_seen = true,
            Mode::FirstByte => self.start_idle(),
        }
    }

    /// Polls the timer; registers only this stream's waker when it is pending.
    pub(crate) fn poll_expired(&mut self, cx: &mut Context<'_>) -> Poll<Expired> {
        if self.timing.timer.as_mut().poll(cx).is_pending() {
            return Poll::Pending;
        }
        if self.mode == Mode::FirstByte {
            return Poll::Ready(Expired::FirstByte);
        }
        if self.chunk_seen {
            self.chunk_seen = false;
            self.silent_ticks = 0;
        } else {
            self.silent_ticks += 1;
            if self.silent_ticks >= SILENT_PERIODS {
                return Poll::Ready(Expired::Idle(self.timing.idle));
            }
        }
        let next = self.timing.timer.deadline() + self.period;
        self.timing.timer.as_mut().reset(next);
        if self.timing.timer.as_mut().poll(cx).is_pending() {
            return Poll::Pending;
        }
        // The next period had already ended: the task was not polled for at
        // least a whole period, which happens when the client applies
        // backpressure and the upstream is not read at all. Those periods say
        // nothing about the upstream, so they are skipped, not counted as
        // silent. Rare by construction, so reading the clock here is cheap.
        let now = Instant::now(); // hotpath-allow: only after a whole period passed unpolled
        self.timing.timer.as_mut().reset(now + self.period);
        match self.timing.timer.as_mut().poll(cx) {
            Poll::Pending => Poll::Pending,
            // Only a zero `idle` leaves the new deadline already elapsed; with
            // no allowed silence the stream is idle at once.
            Poll::Ready(()) => Poll::Ready(Expired::Idle(self.timing.idle)),
        }
    }

    /// Switches to idle mode with a fresh period starting now.
    fn start_idle(&mut self) {
        self.mode = Mode::Idle;
        self.chunk_seen = false;
        self.silent_ticks = 0;
        let now = Instant::now(); // hotpath-allow: once per body, at commit or on its first byte
        self.timing.timer.as_mut().reset(now + self.period);
    }
}

#[cfg(test)]
#[path = "idle_tests.rs"]
mod tests;
