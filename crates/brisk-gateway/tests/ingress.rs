//! `ingress::read_body`: zero-copy and single-copy assembly, the size limit,
//! the in-flight budget, the total deadline, the minimum rate and the
//! Content-Length progress floor (D25). Time-dependent cases run on tokio's
//! paused clock, so their instants are exact.

use std::convert::Infallible;
use std::io;
use std::pin::{Pin, pin};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use brisk_gateway::BoxError;
use brisk_gateway::budget::ByteBudget;
use brisk_gateway::ingress::{IngressError, read_body};
use brisk_gateway::spec::LimitsSpec;
use bytes::Bytes;
use http_body::{Body, Frame, SizeHint};
use http_body_util::channel::{Channel, Sender};
use tokio::time::{Instant, sleep};

const KIB: usize = 1 << 10;
const MIB: usize = 1 << 20;

/// A body with an exact size hint, as hyper reports a Content-Length.
struct Declared<B> {
    inner: B,
    length: u64,
}

impl<B: Body + Unpin> Body for Declared<B> {
    type Data = B::Data;
    type Error = B::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        Pin::new(&mut self.inner).poll_frame(cx)
    }

    fn size_hint(&self) -> SizeHint {
        SizeHint::with_exact(self.length)
    }
}

/// Declares `length` bytes and panics when polled.
struct Unreadable {
    length: u64,
}

impl Body for Unreadable {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Infallible>>> {
        panic!("a body over the limit must not be read");
    }

    fn size_hint(&self) -> SizeHint {
        SizeHint::with_exact(self.length)
    }
}

fn channel() -> (Sender<Bytes, io::Error>, Channel<Bytes, io::Error>) {
    Channel::new(64)
}

fn declared<B>(inner: B, length: usize) -> Declared<B> {
    Declared {
        inner,
        length: length as u64,
    }
}

fn filled(length: usize, byte: u8) -> Bytes {
    Bytes::from(vec![byte; length])
}

fn limits() -> LimitsSpec {
    LimitsSpec::default()
}

/// A channel body whose frames are all sent and whose sender is closed.
fn buffered(frames: &[Bytes]) -> Channel<Bytes, io::Error> {
    let (mut tx, body) = channel();
    for frame in frames {
        tx.try_send(Frame::data(frame.clone()))
            .expect("the channel has room");
    }
    body
}

#[tokio::test(start_paused = true)]
async fn single_frame_is_returned_without_copying() {
    let budget = ByteBudget::new(MIB);
    let frame = filled(2 * KIB, b'a');
    let (bytes, permit) = read_body(buffered(std::slice::from_ref(&frame)), &limits(), &budget)
        .await
        .expect("body reads");
    assert_eq!(bytes.as_ptr(), frame.as_ptr());
    assert_eq!(bytes.len(), frame.len());
    assert_eq!(permit.bytes(), 2 * KIB);

    let (bytes, _permit) = read_body(
        declared(buffered(std::slice::from_ref(&frame)), 2 * KIB),
        &limits(),
        &budget,
    )
    .await
    .expect("body reads");
    assert_eq!(bytes.as_ptr(), frame.as_ptr());
}

#[tokio::test(start_paused = true)]
async fn several_frames_are_joined_in_order() {
    let budget = ByteBudget::new(MIB);
    let frames = [
        filled(100, b'a'),
        Bytes::new(),
        filled(200, b'b'),
        filled(300, b'c'),
    ];
    let expected: Vec<u8> = frames
        .iter()
        .flat_map(|frame| frame.iter().copied())
        .collect();

    for with_length in [false, true] {
        let (bytes, permit) = if with_length {
            read_body(declared(buffered(&frames), 600), &limits(), &budget).await
        } else {
            read_body(buffered(&frames), &limits(), &budget).await
        }
        .expect("body reads");
        assert_eq!(bytes, expected);
        assert!(
            frames
                .iter()
                .all(|frame| frame.is_empty() || bytes.as_ptr() != frame.as_ptr())
        );
        assert_eq!(permit.bytes(), 600);
    }
    assert_eq!(budget.available(), MIB);
}

#[test]
fn buffered_body_needs_no_timer() {
    // No runtime here: creating a timer or reading tokio's clock would panic,
    // so a `Ready` from the first poll proves neither happened.
    let budget = ByteBudget::new(MIB);
    let limits = limits();
    let mut cx = Context::from_waker(Waker::noop());

    let frame = filled(2 * KIB, b'x');
    let mut read = pin!(read_body(
        declared(buffered(std::slice::from_ref(&frame)), 2 * KIB),
        &limits,
        &budget
    ));
    match read.as_mut().poll(&mut cx) {
        Poll::Ready(Ok((bytes, permit))) => {
            assert_eq!(bytes.as_ptr(), frame.as_ptr());
            assert_eq!(permit.bytes(), 2 * KIB);
        }
        other => panic!("expected the body at once, got {other:?}"),
    }

    let frames = [filled(10, b'x'), filled(20, b'y')];
    let mut read = pin!(read_body(buffered(&frames), &limits, &budget));
    match read.as_mut().poll(&mut cx) {
        Poll::Ready(Ok((bytes, _permit))) => assert_eq!(bytes.len(), 30),
        other => panic!("expected the body at once, got {other:?}"),
    }
    assert_eq!(budget.available(), MIB);
}

#[tokio::test(start_paused = true)]
async fn declared_length_over_the_limit_is_refused_unread() {
    let budget = ByteBudget::new(64 * MIB);
    let limits = LimitsSpec {
        max_body: 1000,
        ..limits()
    };
    let result = read_body(Unreadable { length: 1001 }, &limits, &budget).await;
    assert!(
        matches!(result, Err(IngressError::TooLarge { limit: 1000 })),
        "{result:?}"
    );
    assert_eq!(budget.available(), 64 * MIB);

    let default_limit = read_body(
        Unreadable {
            length: 32 * MIB as u64 + 1,
        },
        &LimitsSpec::default(),
        &budget,
    )
    .await;
    assert!(matches!(
        default_limit,
        Err(IngressError::TooLarge { limit }) if limit == 32 << 20
    ));

    let at_limit = read_body(
        declared(buffered(&[filled(1000, b'z')]), 1000),
        &limits,
        &budget,
    )
    .await;
    assert_eq!(
        at_limit.expect("exactly max_body is accepted").0.len(),
        1000
    );
}

#[tokio::test(start_paused = true)]
async fn chunked_body_over_the_limit_is_refused() {
    let budget = ByteBudget::new(64 * MIB);
    let limits = LimitsSpec {
        max_body: 1000,
        ..limits()
    };
    let result = read_body(
        buffered(&[filled(600, b'a'), filled(401, b'b')]),
        &limits,
        &budget,
    )
    .await;
    assert!(
        matches!(result, Err(IngressError::TooLarge { limit: 1000 })),
        "{result:?}"
    );
    assert_eq!(budget.available(), 64 * MIB);
}

#[tokio::test(start_paused = true)]
async fn body_longer_than_declared_is_still_limited() {
    let budget = ByteBudget::new(64 * MIB);
    let limits = LimitsSpec {
        max_body: 1000,
        ..limits()
    };
    let lying = declared(buffered(&[filled(600, b'a'), filled(600, b'b')]), 600);
    let result = read_body(lying, &limits, &budget).await;
    assert!(
        matches!(result, Err(IngressError::TooLarge { limit: 1000 })),
        "{result:?}"
    );

    let lying = declared(buffered(&[filled(600, b'a'), filled(100, b'b')]), 600);
    let (bytes, permit) = read_body(lying, &limits, &budget)
        .await
        .expect("within max_body");
    assert_eq!(bytes.len(), 700);
    assert_eq!(permit.bytes(), 700, "the excess is reserved as well");
}

#[tokio::test(start_paused = true)]
async fn stalled_body_is_too_slow_at_the_first_window() {
    let budget = ByteBudget::new(64 * MIB);
    let (mut tx, body) = channel();
    tx.send_data(filled(KIB, b'a'))
        .await
        .expect("receiver alive");

    let start = Instant::now();
    let result = read_body(body, &limits(), &budget).await;
    assert!(matches!(result, Err(IngressError::TooSlow)), "{result:?}");
    assert_eq!(start.elapsed(), Duration::from_secs(10));
    assert_eq!(budget.available(), 64 * MIB);
    drop(tx);
}

#[tokio::test(start_paused = true)]
async fn slow_large_declared_body_fails_the_progress_floor() {
    // 64 KiB every 10 s meets the fixed minimum rate but not the D25 floor of
    // a 32 MiB body (about 2.7 MiB per window), so the reservation of the
    // whole 32 MiB ends at the first window instead of after 60 s.
    const LENGTH: usize = 32 * MIB;
    let budget = ByteBudget::new(256 * MIB);
    let (mut tx, body) = channel();
    let sender = tokio::spawn(async move {
        loop {
            if tx.send_data(filled(64 * KIB, b's')).await.is_err() {
                break;
            }
            sleep(Duration::from_secs(10)).await;
        }
    });

    let start = Instant::now();
    let limits = limits();
    let mut read = pin!(read_body(declared(body, LENGTH), &limits, &budget));
    let mut cx = Context::from_waker(Waker::noop());
    assert!(read.as_mut().poll(&mut cx).is_pending());
    assert_eq!(
        budget.available(),
        256 * MIB - LENGTH,
        "the declared length is reserved up front"
    );
    let result = read.await;
    assert!(matches!(result, Err(IngressError::TooSlow)), "{result:?}");
    assert_eq!(start.elapsed(), Duration::from_secs(10));
    assert_eq!(budget.available(), 256 * MIB, "the reservation is returned");
    sender.await.expect("sender task");
}

#[tokio::test(start_paused = true)]
async fn small_declared_body_keeps_the_fixed_floor() {
    // For 2 KB the proportional rate is 171 bytes per window, below the fixed
    // 64 KiB, so the floor is the fixed rate capped at the length.
    let budget = ByteBudget::new(MIB);

    let (mut tx, body) = channel();
    tx.send_data(filled(KIB, b'a'))
        .await
        .expect("receiver alive");
    let start = Instant::now();
    let result = read_body(declared(body, 2 * KIB), &limits(), &budget).await;
    assert!(matches!(result, Err(IngressError::TooSlow)), "{result:?}");
    assert_eq!(start.elapsed(), Duration::from_secs(10));
    drop(tx);

    let (mut tx, body) = channel();
    let sender = tokio::spawn(async move {
        tx.send_data(filled(KIB, b'a'))
            .await
            .expect("receiver alive");
        sleep(Duration::from_secs(9)).await;
        tx.send_data(filled(KIB, b'b'))
            .await
            .expect("receiver alive");
    });
    let start = Instant::now();
    let (bytes, _permit) = read_body(declared(body, 2 * KIB), &limits(), &budget)
        .await
        .expect("2 KB within the first window");
    assert_eq!(bytes.len(), 2 * KIB);
    assert_eq!(start.elapsed(), Duration::from_secs(9));
    sender.await.expect("sender task");
}

#[tokio::test(start_paused = true)]
async fn medium_declared_body_at_the_fixed_rate_runs_into_the_total_timeout() {
    // 512 KiB: the proportional rate (about 43 KiB per window) is below the
    // fixed 64 KiB, so 64 KiB per window passes every window check and the
    // body ends only at the total deadline.
    const LENGTH: usize = 512 * KIB;
    let budget = ByteBudget::new(MIB);
    let (mut tx, body) = channel();
    let sender = tokio::spawn(async move {
        tx.send_data(filled(64 * KIB, b'm'))
            .await
            .expect("receiver alive");
        sleep(Duration::from_secs(9)).await;
        loop {
            if tx.send_data(filled(64 * KIB, b'm')).await.is_err() {
                break;
            }
            sleep(Duration::from_secs(10)).await;
        }
    });

    let start = Instant::now();
    let result = read_body(declared(body, LENGTH), &limits(), &budget).await;
    assert!(matches!(result, Err(IngressError::Timeout)), "{result:?}");
    assert_eq!(start.elapsed(), Duration::from_secs(60));
    assert_eq!(budget.available(), MIB);
    sender.await.expect("sender task");
}

#[tokio::test(start_paused = true)]
async fn body_that_never_ends_times_out() {
    let budget = ByteBudget::new(64 * MIB);
    let (mut tx, body) = channel();
    let sender = tokio::spawn(async move {
        loop {
            if tx.send_data(filled(100 * KIB, b't')).await.is_err() {
                break;
            }
            sleep(Duration::from_secs(5)).await;
        }
    });

    let start = Instant::now();
    let result = read_body(body, &limits(), &budget).await;
    assert!(matches!(result, Err(IngressError::Timeout)), "{result:?}");
    assert_eq!(start.elapsed(), Duration::from_secs(60));
    assert_eq!(budget.available(), 64 * MIB);
    sender.await.expect("sender task");
}

#[tokio::test(start_paused = true)]
async fn shortened_limits_apply() {
    let budget = ByteBudget::new(MIB);
    let limits = LimitsSpec {
        body_read_timeout: Duration::from_millis(300),
        body_min_rate_bytes: 100,
        body_min_rate_window: Duration::from_millis(100),
        ..limits()
    };

    let (mut tx, body) = channel();
    let sender = tokio::spawn(async move {
        for _ in 0..3 {
            tx.send_data(filled(150, b'r'))
                .await
                .expect("receiver alive");
            sleep(Duration::from_millis(90)).await;
        }
    });
    let start = Instant::now();
    let (bytes, _permit) = read_body(body, &limits, &budget)
        .await
        .expect("fast enough");
    assert_eq!(bytes.len(), 450);
    assert_eq!(start.elapsed(), Duration::from_millis(270));
    sender.await.expect("sender task");

    let (mut tx, body) = channel();
    let sender = tokio::spawn(async move {
        tx.send_data(filled(150, b'r'))
            .await
            .expect("receiver alive");
        sleep(Duration::from_millis(150)).await;
        tx.send_data(filled(10, b'r')).await.ok();
        sleep(Duration::from_secs(1)).await;
    });
    let start = Instant::now();
    let result = read_body(body, &limits, &budget).await;
    assert!(matches!(result, Err(IngressError::TooSlow)), "{result:?}");
    // 150 bytes pass the first window (100), 160 fail the second (200).
    assert_eq!(start.elapsed(), Duration::from_millis(200));
    sender.await.expect("sender task");
}

#[tokio::test(start_paused = true)]
async fn zero_window_leaves_only_the_total_deadline() {
    let budget = ByteBudget::new(MIB);
    let limits = LimitsSpec {
        body_read_timeout: Duration::from_secs(5),
        body_min_rate_window: Duration::ZERO,
        ..limits()
    };
    let (mut tx, body) = channel();
    tx.send_data(filled(1, b'z')).await.expect("receiver alive");
    let start = Instant::now();
    let result = read_body(body, &limits, &budget).await;
    assert!(matches!(result, Err(IngressError::Timeout)), "{result:?}");
    assert_eq!(start.elapsed(), Duration::from_secs(5));
    drop(tx);
}

#[tokio::test(start_paused = true)]
async fn exhausted_budget_is_overloaded_until_a_permit_drops() {
    let budget = ByteBudget::new(3 * KIB);
    let first = read_body(
        declared(buffered(&[filled(2 * KIB, b'1')]), 2 * KIB),
        &limits(),
        &budget,
    )
    .await
    .expect("fits");
    assert_eq!(budget.available(), KIB);

    let second = read_body(
        Unreadable {
            length: 2 * KIB as u64,
        },
        &limits(),
        &budget,
    )
    .await;
    assert!(
        matches!(second, Err(IngressError::Overloaded)),
        "{second:?}"
    );
    assert_eq!(
        budget.available(),
        KIB,
        "a refused reservation takes nothing"
    );

    drop(first);
    assert_eq!(budget.available(), 3 * KIB);
    let third = read_body(
        declared(buffered(&[filled(2 * KIB, b'3')]), 2 * KIB),
        &limits(),
        &budget,
    )
    .await
    .expect("fits again");
    assert_eq!(third.1.bytes(), 2 * KIB);
}

#[tokio::test(start_paused = true)]
async fn chunked_body_reserves_frame_by_frame() {
    let budget = ByteBudget::new(KIB);
    let (mut tx, body) = channel();
    tx.send_data(filled(300, b'a'))
        .await
        .expect("receiver alive");

    let limits = limits();
    let mut read = pin!(read_body(body, &limits, &budget));
    let mut cx = Context::from_waker(Waker::noop());
    assert!(read.as_mut().poll(&mut cx).is_pending());
    assert_eq!(
        budget.available(),
        KIB - 300,
        "only the received bytes are reserved"
    );

    tx.send_data(filled(400, b'b'))
        .await
        .expect("receiver alive");
    assert!(read.as_mut().poll(&mut cx).is_pending());
    assert_eq!(budget.available(), KIB - 700);

    drop(tx);
    let (bytes, permit) = read.await.expect("body reads");
    assert_eq!(bytes.len(), 700);
    assert_eq!(permit.bytes(), 700);
    drop(permit);
    assert_eq!(budget.available(), KIB);

    let result = read_body(
        buffered(&[filled(600, b'a'), filled(600, b'b')]),
        &limits,
        &budget,
    )
    .await;
    assert!(
        matches!(result, Err(IngressError::Overloaded)),
        "{result:?}"
    );
    assert_eq!(budget.available(), KIB);
}

#[tokio::test(start_paused = true)]
async fn empty_body_reads_as_empty() {
    let budget = ByteBudget::new(KIB);
    let (bytes, permit) = read_body(buffered(&[]), &limits(), &budget)
        .await
        .expect("empty body");
    assert!(bytes.is_empty());
    assert_eq!(permit.bytes(), 0);
    let (bytes, _permit) = read_body(declared(buffered(&[]), 0), &limits(), &budget)
        .await
        .expect("empty declared body");
    assert!(bytes.is_empty());
}

#[tokio::test(start_paused = true)]
async fn client_abort_is_reported_with_its_cause() {
    let budget = ByteBudget::new(KIB);
    let (mut tx, body) = channel();
    tx.send_data(filled(100, b'a'))
        .await
        .expect("receiver alive");
    tx.abort(io::Error::new(io::ErrorKind::ConnectionReset, "peer reset"));

    let result = read_body(body, &limits(), &budget).await;
    match result {
        Err(IngressError::Aborted(cause)) => {
            let cause: BoxError = cause;
            let io = cause.downcast::<io::Error>().expect("the body's own error");
            assert_eq!(io.kind(), io::ErrorKind::ConnectionReset);
        }
        other => panic!("expected Aborted, got {other:?}"),
    }
    assert_eq!(budget.available(), KIB);
}
