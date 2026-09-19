//! Waiting for absolute deadlines with microsecond precision without starving
//! socket I/O.
//!
//! A shard event loop drives a [`DeadlineTimer`] like this:
//!
//! ```no_run
//! # use std::time::Duration;
//! # use brisk_bench_core::{clock::now_ns, precise::{DeadlineTimer, DEFAULT_SPIN_WINDOW}};
//! # fn main() -> std::io::Result<()> {
//! const TIMER: mio::Token = mio::Token(0);
//! let mut poll = mio::Poll::new()?;
//! let mut events = mio::Events::with_capacity(1024);
//! let mut timer = DeadlineTimer::new(DEFAULT_SPIN_WINDOW)?;
//! timer.register(poll.registry(), TIMER)?;
//! let mut next_deadline = now_ns() + 1_000_000;
//! loop {
//!     timer.arm(next_deadline)?;
//!     poll.poll(&mut events, timer.poll_timeout(now_ns(), Some(next_deadline)))?;
//!     for event in &events {
//!         if event.token() == TIMER {
//!             timer.acknowledge();
//!         }
//!         // handle socket events
//!     }
//!     if timer.is_due(now_ns(), next_deadline) {
//!         timer.spin_until(next_deadline);
//!         // fire every due schedule entry, then compute the next deadline
//! #       next_deadline += 1_000_000;
//! #       break;
//!     }
//! }
//! # Ok(())
//! # }
//! ```
//!
//! On Linux the timer is a `timerfd` on `CLOCK_MONOTONIC` that expires one
//! spin window before the deadline, so `epoll` wakes the thread with
//! microsecond accuracy and the rest is a busy wait. Elsewhere only the
//! millisecond-granular `Poll::poll` timeout is available, so the last
//! millisecond before a deadline is busy-polled; results are functionally
//! correct but not precise.

use std::time::Duration;

use crate::clock::now_ns;

/// Default busy-wait window before a deadline.
pub const DEFAULT_SPIN_WINDOW: Duration = Duration::from_micros(50);

/// Shortest non-zero timeout `Poll::poll` honours off Linux: mio rounds
/// sub-millisecond timeouts up to a whole millisecond there.
#[cfg(not(target_os = "linux"))]
const POLL_GRANULARITY_NS: u64 = 1_000_000;

/// Converts a duration to whole nanoseconds, saturating at `u64::MAX`.
fn duration_ns(d: Duration) -> u64 {
    u64::try_from(d.as_nanos()).unwrap_or(u64::MAX)
}

/// Busy-waits until [`now_ns`] reaches `deadline_ns` and returns the time
/// observed when it did.
#[inline]
pub fn spin_until(deadline_ns: u64) -> u64 {
    loop {
        let now = now_ns();
        if now >= deadline_ns {
            return now;
        }
        std::hint::spin_loop();
    }
}

/// Blocks the calling thread until `deadline_ns` (on the [`now_ns`] timeline).
///
/// Sleeps until one `spin_window` before the deadline and busy-waits for the
/// remainder. On Linux the sleep is `clock_nanosleep(CLOCK_MONOTONIC,
/// TIMER_ABSTIME)`; elsewhere `std::thread::sleep`. Returns the time observed
/// when the deadline was reached. Intended for threads that have no sockets to
/// service.
///
/// On Linux the first call on a thread lowers that thread's timer slack to
/// 1 ns (see [`set_min_timer_slack`]): the default 50 µs slack would let the
/// kernel delay the wakeup by a whole default spin window.
pub fn sleep_until(deadline_ns: u64, spin_window: Duration) -> u64 {
    let wake_ns = deadline_ns.saturating_sub(duration_ns(spin_window));
    imp::sleep_until_abs(wake_ns);
    spin_until(deadline_ns)
}

/// Lowers the calling thread's timer slack to the minimum of 1 ns.
///
/// Linux lets the kernel defer a sleeping thread's wakeup by up to its timer
/// slack (50 µs by default) to batch timer interrupts. `timerfd` expiries are
/// not affected, but `clock_nanosleep` and poll timeouts are. A no-op on other
/// platforms.
pub fn set_min_timer_slack() -> std::io::Result<()> {
    imp::set_min_timer_slack()
}

/// A deadline timer that integrates with a `mio::Poll` loop.
///
/// See the [module documentation](self) for the intended loop shape.
#[derive(Debug)]
pub struct DeadlineTimer {
    spin_window_ns: u64,
    /// Absolute expiry currently programmed into the kernel timer, used to
    /// skip redundant `timerfd_settime` calls.
    armed_expiry_ns: Option<u64>,
    #[cfg(target_os = "linux")]
    fd: std::os::fd::OwnedFd,
}

impl DeadlineTimer {
    /// Creates a timer with the given busy-wait window.
    pub fn new(spin_window: Duration) -> std::io::Result<Self> {
        Ok(Self {
            spin_window_ns: duration_ns(spin_window),
            armed_expiry_ns: None,
            #[cfg(target_os = "linux")]
            fd: imp::timerfd_create()?,
        })
    }

    /// The configured busy-wait window in nanoseconds.
    pub fn spin_window_ns(&self) -> u64 {
        self.spin_window_ns
    }

    /// Registers the timer with a poll registry under `token`.
    ///
    /// On Linux this registers the `timerfd` for readability. On other
    /// platforms there is nothing to register and the call is a no-op; wakeups
    /// come solely from [`poll_timeout`](Self::poll_timeout).
    pub fn register(&self, registry: &mio::Registry, token: mio::Token) -> std::io::Result<()> {
        #[cfg(target_os = "linux")]
        {
            use std::os::fd::AsRawFd;
            let raw = self.fd.as_raw_fd();
            registry.register(
                &mut mio::unix::SourceFd(&raw),
                token,
                mio::Interest::READABLE,
            )
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = (registry, token);
            Ok(())
        }
    }

    /// Removes the timer from a poll registry. No-op off Linux.
    pub fn deregister(&self, registry: &mio::Registry) -> std::io::Result<()> {
        #[cfg(target_os = "linux")]
        {
            use std::os::fd::AsRawFd;
            let raw = self.fd.as_raw_fd();
            registry.deregister(&mut mio::unix::SourceFd(&raw))
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = registry;
            Ok(())
        }
    }

    /// Programs the timer to wake the poll one spin window before
    /// `deadline_ns`. Re-arming with an unchanged deadline costs no syscall.
    pub fn arm(&mut self, deadline_ns: u64) -> std::io::Result<()> {
        // A zero it_value would disarm the timerfd, so the earliest expiry is 1.
        let expiry = deadline_ns.saturating_sub(self.spin_window_ns).max(1);
        if self.armed_expiry_ns == Some(expiry) {
            return Ok(());
        }
        #[cfg(target_os = "linux")]
        imp::timerfd_set_abs(&self.fd, expiry)?;
        self.armed_expiry_ns = Some(expiry);
        Ok(())
    }

    /// Cancels any programmed expiry, e.g. when the schedule becomes empty.
    pub fn disarm(&mut self) -> std::io::Result<()> {
        if self.armed_expiry_ns.take().is_some() {
            #[cfg(target_os = "linux")]
            imp::timerfd_set_abs(&self.fd, 0)?;
        }
        Ok(())
    }

    /// Clears the timer's readiness after its token was reported by the poll.
    ///
    /// Calling it when the timer has not expired is harmless.
    pub fn acknowledge(&mut self) {
        #[cfg(target_os = "linux")]
        if imp::timerfd_drain(&self.fd) {
            self.armed_expiry_ns = None;
        }
    }

    /// The timeout to pass to `Poll::poll`.
    ///
    /// - `None` when there is no deadline: block until a socket event.
    /// - Zero when the deadline is within the spin window: do not block, the
    ///   caller is about to spin.
    /// - Otherwise the time until the timer expiry. On Linux the `timerfd`
    ///   wakes the poll first and this is only a safety net; elsewhere it is
    ///   the sole wakeup source.
    ///
    /// Off Linux a timeout below one millisecond would be rounded up to a
    /// millisecond and overshoot the deadline, so it becomes zero instead and
    /// the loop busy-polls through its final millisecond.
    pub fn poll_timeout(&self, now_ns: u64, deadline_ns: Option<u64>) -> Option<Duration> {
        let deadline = deadline_ns?;
        let remaining = deadline.saturating_sub(now_ns);
        if remaining <= self.spin_window_ns {
            return Some(Duration::ZERO);
        }
        let timeout = remaining - self.spin_window_ns;
        #[cfg(not(target_os = "linux"))]
        if timeout < POLL_GRANULARITY_NS {
            return Some(Duration::ZERO);
        }
        Some(Duration::from_nanos(timeout))
    }

    /// Whether `deadline_ns` is close enough that the caller should
    /// [`spin_until`](Self::spin_until) it now.
    #[inline]
    pub fn is_due(&self, now_ns: u64, deadline_ns: u64) -> bool {
        deadline_ns.saturating_sub(now_ns) <= self.spin_window_ns
    }

    /// Busy-waits until `deadline_ns` and returns the time observed when it
    /// was reached. The programmed expiry is considered consumed.
    #[inline]
    pub fn spin_until(&mut self, deadline_ns: u64) -> u64 {
        if self
            .armed_expiry_ns
            .is_some_and(|expiry| expiry <= deadline_ns)
        {
            self.armed_expiry_ns = None;
        }
        spin_until(deadline_ns)
    }
}

#[cfg(target_os = "linux")]
mod imp {
    use std::io;
    use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};

    fn ns_to_timespec(ns: u64) -> libc::timespec {
        libc::timespec {
            tv_sec: libc::time_t::try_from(ns / 1_000_000_000).unwrap_or(libc::time_t::MAX),
            // Always < 1e9, so it fits every c_long.
            tv_nsec: libc::c_long::try_from(ns % 1_000_000_000).unwrap_or(0),
        }
    }

    pub(super) fn timerfd_create() -> io::Result<OwnedFd> {
        // SAFETY: plain syscall with constant, valid arguments.
        let fd = unsafe {
            libc::timerfd_create(
                libc::CLOCK_MONOTONIC,
                libc::TFD_NONBLOCK | libc::TFD_CLOEXEC,
            )
        };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: `fd` is a freshly created descriptor owned by nobody else.
        Ok(unsafe { OwnedFd::from_raw_fd(fd) })
    }

    /// Sets an absolute `CLOCK_MONOTONIC` expiry; `0` disarms.
    pub(super) fn timerfd_set_abs(fd: &OwnedFd, expiry_ns: u64) -> io::Result<()> {
        let spec = libc::itimerspec {
            it_interval: ns_to_timespec(0),
            it_value: ns_to_timespec(expiry_ns),
        };
        // SAFETY: `fd` is a live timerfd, `spec` is a valid itimerspec and a
        // null old_value pointer is permitted.
        let rc = unsafe {
            libc::timerfd_settime(
                fd.as_raw_fd(),
                libc::TFD_TIMER_ABSTIME,
                &raw const spec,
                std::ptr::null_mut(),
            )
        };
        if rc < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    /// Reads the expiration counter; returns whether the timer had expired.
    pub(super) fn timerfd_drain(fd: &OwnedFd) -> bool {
        let mut expirations = 0u64;
        // SAFETY: reads at most 8 bytes into a live, properly aligned u64.
        let n = unsafe {
            libc::read(
                fd.as_raw_fd(),
                (&raw mut expirations).cast::<libc::c_void>(),
                std::mem::size_of::<u64>(),
            )
        };
        // EAGAIN (not expired) is the only expected failure on a nonblocking
        // timerfd; either way there is nothing more to consume.
        n == 8 && expirations > 0
    }

    thread_local! {
        static SLACK_LOWERED: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    }

    pub(super) fn set_min_timer_slack() -> io::Result<()> {
        let slack_ns: libc::c_ulong = 1;
        // SAFETY: PR_SET_TIMERSLACK takes one unsigned long argument and only
        // affects the calling thread.
        let rc = unsafe { libc::prctl(libc::PR_SET_TIMERSLACK, slack_ns) };
        if rc < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    pub(super) fn sleep_until_abs(wake_ns: u64) {
        if !SLACK_LOWERED.get() {
            // PR_SET_TIMERSLACK with a positive value cannot fail; should it
            // anyway, the sleep only loses precision, so retrying on the next
            // call is enough.
            SLACK_LOWERED.set(set_min_timer_slack().is_ok());
        }
        let target = ns_to_timespec(wake_ns);
        loop {
            // SAFETY: `target` is a valid timespec; the remain pointer may be
            // null for absolute sleeps.
            let rc = unsafe {
                libc::clock_nanosleep(
                    libc::CLOCK_MONOTONIC,
                    libc::TIMER_ABSTIME,
                    &raw const target,
                    std::ptr::null_mut(),
                )
            };
            // clock_nanosleep returns the error number directly; only EINTR is
            // possible with valid arguments, and absolute sleeps simply resume.
            if rc != libc::EINTR {
                return;
            }
        }
    }
}

#[cfg(not(target_os = "linux"))]
mod imp {
    use crate::clock::now_ns;

    #[expect(
        clippy::unnecessary_wraps,
        reason = "mirrors the fallible Linux implementation"
    )]
    pub(super) fn set_min_timer_slack() -> std::io::Result<()> {
        Ok(())
    }

    pub(super) fn sleep_until_abs(wake_ns: u64) {
        let now = now_ns();
        if wake_ns > now {
            std::thread::sleep(std::time::Duration::from_nanos(wake_ns - now));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn poll_timeout_shapes() {
        let timer = DeadlineTimer::new(Duration::from_micros(50)).unwrap();
        assert_eq!(timer.poll_timeout(1_000, None), None);
        assert_eq!(
            timer.poll_timeout(1_000, Some(1_000 + 50_000)),
            Some(Duration::ZERO)
        );
        assert_eq!(timer.poll_timeout(5_000, Some(1_000)), Some(Duration::ZERO));
        assert_eq!(
            timer.poll_timeout(0, Some(1_050_000)),
            Some(Duration::from_millis(1))
        );
        assert!(timer.is_due(10, 50_010));
        assert!(!timer.is_due(10, 50_011));
        // 200 µs out: a 150 µs timeout on Linux; off Linux it would round up
        // to 1 ms and overshoot, so the loop busy-polls instead.
        let expected = if cfg!(target_os = "linux") {
            Duration::from_micros(150)
        } else {
            Duration::ZERO
        };
        assert_eq!(timer.poll_timeout(0, Some(200_000)), Some(expected));
    }

    #[test]
    fn sleep_until_reaches_deadline() {
        let deadline = now_ns() + 2_000_000;
        let woke = sleep_until(deadline, DEFAULT_SPIN_WINDOW);
        assert!(woke >= deadline);
        assert!(now_ns() >= deadline);
        set_min_timer_slack().unwrap();
    }

    /// The first `sleep_until` on a thread lowers its timer slack, and the
    /// wakeups then land close to the deadline.
    #[cfg(target_os = "linux")]
    #[test]
    fn sleep_until_lowers_timer_slack() {
        std::thread::spawn(|| {
            // SAFETY: PR_GET_TIMERSLACK takes no argument and only reads the
            // calling thread's slack.
            let slack = || unsafe { libc::prctl(libc::PR_GET_TIMERSLACK) };
            let mut late = Vec::new();
            for _ in 0..50 {
                let deadline = now_ns() + 300_000;
                late.push(sleep_until(deadline, DEFAULT_SPIN_WINDOW) - deadline);
            }
            assert_eq!(slack(), 1);
            late.sort_unstable();
            // Generous: the VM is shared with concurrent builds. A spin that
            // absorbs the wakeup keeps the median within a few microseconds.
            assert!(late[late.len() / 2] < 200_000, "lateness {late:?}");
        })
        .join()
        .unwrap();
    }

    #[test]
    fn event_loop_wakes_at_deadline() {
        const TIMER: mio::Token = mio::Token(7);
        let mut poll = mio::Poll::new().unwrap();
        let mut events = mio::Events::with_capacity(8);
        let mut timer = DeadlineTimer::new(DEFAULT_SPIN_WINDOW).unwrap();
        timer.register(poll.registry(), TIMER).unwrap();

        for _ in 0..5 {
            let deadline = now_ns() + 3_000_000;
            let fired_at = loop {
                timer.arm(deadline).unwrap();
                poll.poll(&mut events, timer.poll_timeout(now_ns(), Some(deadline)))
                    .unwrap();
                for event in &events {
                    assert_eq!(event.token(), TIMER);
                    timer.acknowledge();
                }
                if timer.is_due(now_ns(), deadline) {
                    break timer.spin_until(deadline);
                }
            };
            assert!(fired_at >= deadline);
            // Generous bound: the VM may be shared with concurrent builds.
            // Off Linux the poll timeout is only millisecond-granular.
            let bound = if cfg!(target_os = "linux") {
                5_000_000
            } else {
                50_000_000
            };
            assert!(
                fired_at - deadline < bound,
                "late by {}",
                fired_at - deadline
            );
        }
        timer.disarm().unwrap();
        timer.deregister(poll.registry()).unwrap();
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn timerfd_reports_readiness() {
        const TIMER: mio::Token = mio::Token(1);
        let mut poll = mio::Poll::new().unwrap();
        let mut events = mio::Events::with_capacity(8);
        let mut timer = DeadlineTimer::new(Duration::ZERO).unwrap();
        timer.register(poll.registry(), TIMER).unwrap();
        let deadline = now_ns() + 1_000_000;
        timer.arm(deadline).unwrap();
        // Blocks without a timeout: only the timerfd can wake it.
        poll.poll(&mut events, None).unwrap();
        assert!(events.iter().any(|e| e.token() == TIMER));
        assert!(now_ns() >= deadline);
        timer.acknowledge();
        assert_eq!(timer.armed_expiry_ns, None);
    }
}
