//! An SPSC broadcast channel.
//!
//! - The value can only be a `usize`.
//! - The consumer is only notified if the value is different.
//! - The value `0` is reserved for closed.

use atomic_waker::AtomicWaker;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::task;

type Value = usize;

pub(crate) const CLOSED: usize = 0;

pub(crate) fn channel(initial: Value) -> (Sender, Receiver) {
    debug_assert!(
        initial != CLOSED,
        "watch::channel initial state of 0 is reserved"
    );

    let shared = Arc::new(Shared {
        value: AtomicUsize::new(initial),
        waker: AtomicWaker::new(),
    });

    (
        Sender {
            shared: shared.clone(),
        },
        Receiver { shared },
    )
}

pub(crate) struct Sender {
    shared: Arc<Shared>,
}

pub(crate) struct Receiver {
    shared: Arc<Shared>,
}

struct Shared {
    value: AtomicUsize,
    waker: AtomicWaker,
}

impl Sender {
    pub(crate) fn send(&mut self, value: Value) {
        if self.shared.value.swap(value, Ordering::SeqCst) != value {
            self.shared.waker.wake();
        }
    }
}

impl Drop for Sender {
    fn drop(&mut self) {
        self.send(CLOSED);
    }
}

impl Receiver {
    pub(crate) fn load(&mut self, cx: &mut task::Context<'_>) -> Value {
        self.shared.waker.register(cx.waker());
        self.shared.value.load(Ordering::SeqCst)
    }

    pub(crate) fn peek(&self) -> Value {
        self.shared.value.load(Ordering::Relaxed)
    }
}

impl Drop for Receiver {
    fn drop(&mut self) {
        // The task that registered this waker no longer waits for a change,
        // so a later `send` (the sender's own drop included) must not wake
        // it. Otherwise dropping a request body on the task that also runs
        // the connection's dispatcher wakes that task while it is being
        // polled, and the runtime reschedules it as a yield.
        self.shared.waker.take();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    use std::task::{Context, Wake, Waker};

    struct CountingWaker(AtomicUsize);

    impl Wake for CountingWaker {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn counting_waker() -> (Arc<CountingWaker>, Waker) {
        let counter = Arc::new(CountingWaker(AtomicUsize::new(0)));
        let waker = Waker::from(counter.clone());
        (counter, waker)
    }

    #[test]
    fn a_dropped_receiver_is_not_woken_by_a_later_send() {
        let (mut tx, mut rx) = channel(1);
        let (counter, waker) = counting_waker();
        assert_eq!(rx.load(&mut Context::from_waker(&waker)), 1);

        drop(rx);
        tx.send(2);
        drop(tx);

        assert_eq!(counter.0.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn a_live_receiver_is_woken_once_per_change() {
        let (mut tx, mut rx) = channel(1);
        let (counter, waker) = counting_waker();
        rx.load(&mut Context::from_waker(&waker));

        tx.send(2);
        tx.send(2);
        assert_eq!(counter.0.load(Ordering::SeqCst), 1);

        rx.load(&mut Context::from_waker(&waker));
        drop(tx);
        assert_eq!(counter.0.load(Ordering::SeqCst), 2);
        assert_eq!(rx.peek(), CLOSED);
    }
}
