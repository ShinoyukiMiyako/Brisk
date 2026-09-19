//! The gateway handle: validates a spec, builds the upstream clients and the
//! configuration snapshot without network I/O, and serves the data plane
//! until shutdown (R1, R10, R16).

use std::cell::Cell;
use std::sync::atomic::{AtomicU64, Ordering};

/// Bits of a request id that count requests within one thread.
const REQUEST_SEQUENCE_BITS: u32 = 48;
const REQUEST_SEQUENCE_MASK: u64 = (1 << REQUEST_SEQUENCE_BITS) - 1;

/// Next thread ordinal, the high 16 bits of request ids. Starts at 1 so that
/// no request id is 0, which marks an uninitialised thread below.
static NEXT_THREAD_ORDINAL: AtomicU64 = AtomicU64::new(1);

thread_local! {
    /// The next request id this thread hands out; 0 before the first call.
    static NEXT_REQUEST_ID: Cell<u64> = const { Cell::new(0) };
}

/// A process-unique request id, not monotonic across threads.
///
/// The high 16 bits are an ordinal taken from a global counter the first
/// time a thread asks, the low 48 bits count that thread's requests, so the
/// common call touches only thread-local state instead of a cross-core
/// `fetch_add` per request (D27). A thread that exhausts its 48-bit range
/// takes a fresh ordinal.
///
/// # Panics
///
/// When more than 65 535 ordinals have been handed out in the process; the
/// data plane runs on a fixed set of runtime workers, far below that.
pub(crate) fn next_request_id() -> u64 {
    NEXT_REQUEST_ID.with(|next| {
        let mut id = next.get();
        if id & REQUEST_SEQUENCE_MASK == 0 {
            id = take_thread_ordinal() << REQUEST_SEQUENCE_BITS;
        }
        // Past the end of the range the sequence wraps to zero (and the last
        // ordinal wraps to 0), which the branch above replaces on the next
        // call.
        next.set(id.wrapping_add(1));
        id
    })
}

/// Reserves a new thread ordinal.
fn take_thread_ordinal() -> u64 {
    // Only uniqueness matters; no other memory is published through it.
    let ordinal = NEXT_THREAD_ORDINAL.fetch_add(1, Ordering::Relaxed);
    assert!(
        u16::try_from(ordinal).is_ok(),
        "request id thread ordinals exhausted"
    );
    ordinal
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::thread;

    use super::*;

    #[test]
    fn request_ids_are_unique_across_threads() {
        const THREADS: usize = 8;
        const PER_THREAD: usize = 20_000;
        let handles: Vec<_> = (0..THREADS)
            .map(|_| {
                thread::spawn(|| {
                    (0..PER_THREAD)
                        .map(|_| next_request_id())
                        .collect::<Vec<_>>()
                })
            })
            .collect();
        let mut all = HashSet::new();
        let mut ordinals = HashSet::new();
        for handle in handles {
            let ids = handle.join().expect("thread panicked");
            let ordinal = ids[0] >> REQUEST_SEQUENCE_BITS;
            assert!(ordinal > 0);
            assert!(
                ids.iter().all(|id| id >> REQUEST_SEQUENCE_BITS == ordinal),
                "one thread keeps one ordinal"
            );
            assert!(ordinals.insert(ordinal), "ordinal {ordinal} reused");
            all.extend(ids);
        }
        assert_eq!(all.len(), THREADS * PER_THREAD);
        assert!(!all.contains(&0));
    }

    #[test]
    fn a_thread_counts_up_within_its_ordinal() {
        let first = next_request_id();
        let second = next_request_id();
        assert_eq!(second, first + 1);
    }

    #[test]
    fn an_exhausted_sequence_takes_a_fresh_ordinal() {
        thread::spawn(|| {
            let first = next_request_id();
            let ordinal = first >> REQUEST_SEQUENCE_BITS;
            // Jump to the last id of this ordinal's range.
            NEXT_REQUEST_ID
                .with(|next| next.set((ordinal << REQUEST_SEQUENCE_BITS) | REQUEST_SEQUENCE_MASK));
            let last = next_request_id();
            assert_eq!(last >> REQUEST_SEQUENCE_BITS, ordinal);
            let fresh = next_request_id();
            assert_ne!(fresh >> REQUEST_SEQUENCE_BITS, ordinal);
            assert_eq!(fresh & REQUEST_SEQUENCE_MASK, 0);
        })
        .join()
        .expect("thread panicked");
    }
}
