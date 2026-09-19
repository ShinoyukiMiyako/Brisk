//! Lock-free byte budget for request bodies held in memory (R20) and for
//! response bytes retained for usage parsing.
//!
//! Reservations never wait: a request that does not fit is refused at once,
//! so the budget needs no waiter queue and one atomic word is enough.

use std::sync::atomic::{AtomicUsize, Ordering};

/// Lock-free byte budget (R20): one `AtomicUsize` of remaining bytes.
/// Replaces a tokio `Semaphore`, whose `release` always locks its waiter
/// list although this budget never waits.
#[derive(Debug)]
pub struct ByteBudget {
    remaining: AtomicUsize,
    capacity: usize,
}

// Relaxed throughout: the budget is a counter and publishes no other memory.
// Each read-modify-write is still atomic, so concurrent reservations can never
// take more than the capacity together.
impl ByteBudget {
    /// A budget of `bytes`, all of them available.
    pub fn new(bytes: usize) -> Self {
        Self {
            remaining: AtomicUsize::new(bytes),
            capacity: bytes,
        }
    }

    /// Bytes not currently reserved.
    pub fn available(&self) -> usize {
        self.remaining.load(Ordering::Relaxed)
    }

    /// Takes `bytes` if that many remain (`fetch_update`); never blocks.
    pub fn try_reserve(&self, bytes: usize) -> bool {
        self.remaining
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |remaining| {
                remaining.checked_sub(bytes)
            })
            .is_ok()
    }

    /// Returns bytes taken by `try_reserve`.
    pub fn release(&self, bytes: usize) {
        let before = self.remaining.fetch_add(bytes, Ordering::Relaxed);
        debug_assert!(
            before
                .checked_add(bytes)
                .is_some_and(|after| after <= self.capacity),
            "released {bytes} bytes that were never reserved (remaining {before}, capacity {})",
            self.capacity
        );
    }

    /// RAII form used by ingress; `None` when the budget is short.
    pub fn try_permit(&self, bytes: usize) -> Option<BudgetPermit<'_>> {
        // Not `bool::then_some`: it builds the permit eagerly, and dropping
        // that unused permit would release bytes that were never reserved.
        if self.try_reserve(bytes) {
            Some(BudgetPermit {
                budget: self,
                bytes,
            })
        } else {
            None
        }
    }
}

/// Reserved bytes, returned on drop. Borrows the budget instead of holding
/// an `Arc`, so taking a permit costs no reference-count traffic.
#[derive(Debug)]
pub struct BudgetPermit<'a> {
    budget: &'a ByteBudget,
    bytes: usize,
}

impl BudgetPermit<'_> {
    /// Bytes this permit holds.
    pub fn bytes(&self) -> usize {
        self.bytes
    }

    /// Grows the permit (chunked bodies); `false` leaves it unchanged.
    pub fn try_grow(&mut self, more: usize) -> bool {
        if !self.budget.try_reserve(more) {
            return false;
        }
        // Cannot overflow: everything this permit holds fits in the capacity.
        self.bytes += more;
        true
    }
}

impl Drop for BudgetPermit<'_> {
    fn drop(&mut self) {
        if self.bytes > 0 {
            self.budget.release(self.bytes);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Barrier;
    use std::thread;

    use super::*;

    #[test]
    fn reserve_until_exhausted_then_release() {
        let budget = ByteBudget::new(100);
        assert_eq!(budget.available(), 100);
        assert!(budget.try_reserve(60));
        assert_eq!(budget.available(), 40);
        assert!(!budget.try_reserve(41));
        assert_eq!(
            budget.available(),
            40,
            "a refused reservation takes nothing"
        );
        assert!(budget.try_reserve(40));
        assert_eq!(budget.available(), 0);
        assert!(!budget.try_reserve(1));
        assert!(budget.try_reserve(0));

        budget.release(60);
        assert_eq!(budget.available(), 60);
        budget.release(40);
        assert_eq!(budget.available(), 100);
    }

    #[test]
    fn permit_returns_its_bytes_on_drop() {
        let budget = ByteBudget::new(1024);
        let permit = budget.try_permit(1000).expect("fits");
        assert_eq!(permit.bytes(), 1000);
        assert_eq!(budget.available(), 24);
        assert!(budget.try_permit(25).is_none());
        drop(permit);
        assert_eq!(budget.available(), 1024);
    }

    #[test]
    fn try_grow_extends_the_permit() {
        let budget = ByteBudget::new(10);
        let mut permit = budget.try_permit(0).expect("an empty permit always fits");
        assert!(permit.try_grow(4));
        assert!(permit.try_grow(6));
        assert_eq!(permit.bytes(), 10);
        assert_eq!(budget.available(), 0);
        drop(permit);
        assert_eq!(budget.available(), 10);
    }

    #[test]
    fn failed_try_grow_changes_nothing() {
        let budget = ByteBudget::new(10);
        let other = budget.try_permit(3).expect("fits");
        let mut permit = budget.try_permit(5).expect("fits");
        assert!(!permit.try_grow(3));
        assert_eq!(permit.bytes(), 5);
        assert_eq!(budget.available(), 2);
        drop(permit);
        assert_eq!(budget.available(), 7);
        drop(other);
        assert_eq!(budget.available(), 10);
    }

    #[test]
    fn concurrent_reservations_never_exceed_the_capacity() {
        const THREADS: usize = 8;
        const ROUNDS: usize = 10_000;
        const CHUNK: usize = 7;
        // Smaller than what all threads may hold at once (8 x 14 bytes), so
        // the threads compete for the last bytes.
        const CAPACITY: usize = 50;

        let budget = ByteBudget::new(CAPACITY);
        let held = AtomicUsize::new(0);
        let peak = AtomicUsize::new(0);
        let granted = AtomicUsize::new(0);
        let refused = AtomicUsize::new(0);
        let start = Barrier::new(THREADS);
        thread::scope(|scope| {
            for _ in 0..THREADS {
                scope.spawn(|| {
                    start.wait();
                    for _ in 0..ROUNDS {
                        let Some(mut permit) = budget.try_permit(CHUNK) else {
                            refused.fetch_add(1, Ordering::Relaxed);
                            continue;
                        };
                        granted.fetch_add(1, Ordering::Relaxed);
                        let grown = permit.try_grow(CHUNK);
                        let bytes = if grown {
                            granted.fetch_add(1, Ordering::Relaxed);
                            2 * CHUNK
                        } else {
                            refused.fetch_add(1, Ordering::Relaxed);
                            CHUNK
                        };
                        assert_eq!(permit.bytes(), bytes);
                        let now = held.fetch_add(bytes, Ordering::SeqCst) + bytes;
                        peak.fetch_max(now, Ordering::SeqCst);
                        // Holding the permit across a yield makes the threads
                        // overlap even on a runner with few cores.
                        thread::yield_now();
                        held.fetch_sub(bytes, Ordering::SeqCst);
                    }
                });
            }
        });

        // Without these the test would also pass if every reservation failed.
        assert!(granted.load(Ordering::Relaxed) > 0);
        assert!(
            refused.load(Ordering::Relaxed) > 0,
            "the threads never competed for the capacity"
        );
        assert!(
            peak.load(Ordering::SeqCst) > 2 * CHUNK,
            "no two permits were ever held at once"
        );
        assert!(peak.load(Ordering::SeqCst) <= CAPACITY);
        assert_eq!(budget.available(), CAPACITY);
    }
}
