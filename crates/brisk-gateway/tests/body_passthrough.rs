//! Tests of the streamed response body and of its test upstream, `MemBody`.

mod body_support;

use std::task::{Context, Poll, Waker};
use std::time::Duration;

use body_support::{MemBody, Step, split_at, split_random};
use bytes::Bytes;
use http::{HeaderMap, HeaderValue};
use http_body::Body;
use http_body_util::BodyExt;
use tokio::time::Instant;

fn poll_once(
    body: &mut MemBody,
) -> Poll<Option<Result<http_body::Frame<Bytes>, brisk_gateway::BoxError>>> {
    std::pin::Pin::new(body).poll_frame(&mut Context::from_waker(Waker::noop()))
}

#[tokio::test]
async fn mem_body_returns_the_same_bytes_in_order() {
    let frames = [
        Bytes::from_static(b"data: 1\n\n"),
        Bytes::from_static(b"data: 2\n\n"),
    ];
    let mut body = MemBody::from_frames(frames.clone());
    for expected in &frames {
        let frame = body.frame().await.expect("a frame").expect("no error");
        let data = frame.into_data().expect("a data frame");
        assert_eq!(data.as_ptr(), expected.as_ptr());
        assert_eq!(data.len(), expected.len());
    }
    assert!(body.is_end_stream());
    assert!(body.frame().await.is_none());
}

#[test]
fn mem_body_pending_steps_return_pending_once() {
    let frame = Bytes::from_static(b"x");
    let mut body = MemBody::pending_before_each([frame.clone(), frame.clone()]);
    for _ in 0..2 {
        assert!(poll_once(&mut body).is_pending());
        match poll_once(&mut body) {
            Poll::Ready(Some(Ok(data))) => assert_eq!(data.into_data().ok(), Some(frame.clone())),
            other => panic!("expected a data frame, got {other:?}"),
        }
    }
    assert!(matches!(poll_once(&mut body), Poll::Ready(None)));
}

#[tokio::test(start_paused = true)]
async fn mem_body_delays_on_the_tokio_clock() {
    let mut body = MemBody::new([
        Step::Delay(Duration::from_millis(300)),
        Step::Data(Bytes::from_static(b"late")),
    ]);
    let start = Instant::now();
    let frame = body.frame().await.expect("a frame").expect("no error");
    assert_eq!(frame.into_data().ok(), Some(Bytes::from_static(b"late")));
    assert_eq!(start.elapsed(), Duration::from_millis(300));
}

#[tokio::test]
async fn mem_body_errors_and_trailers() {
    let mut trailers = HeaderMap::new();
    trailers.insert("x-trailer", HeaderValue::from_static("1"));
    let mut body = MemBody::new([
        Step::Trailers(trailers.clone()),
        Step::Error("upstream reset"),
    ]);
    let frame = body.frame().await.expect("a frame").expect("no error");
    assert_eq!(frame.into_trailers().ok(), Some(trailers));
    let error = body.frame().await.expect("a frame").expect_err("an error");
    assert_eq!(error.to_string(), "upstream reset");
    assert!(body.frame().await.is_none());
}

#[tokio::test]
async fn mem_body_exact_size_hint_counts_down() {
    let mut body = MemBody::new([
        Step::Data(Bytes::from_static(b"abc")),
        Step::Pending,
        Step::Data(Bytes::from_static(b"de")),
    ])
    .with_exact_size();
    assert_eq!(body.size_hint().exact(), Some(5));
    body.frame().await.expect("a frame").expect("no error");
    assert_eq!(body.size_hint().exact(), Some(2));
    body.frame().await.expect("a frame").expect("no error");
    assert_eq!(body.size_hint().exact(), Some(0));
    assert_eq!(MemBody::from_frames([]).size_hint().exact(), None);
}

#[test]
fn split_random_is_reproducible_and_zero_copy() {
    let bytes = Bytes::from((0..=255u8).cycle().take(10_000).collect::<Vec<u8>>());
    let pieces = split_random(&bytes, 7, 64, 512);
    assert_eq!(pieces, split_random(&bytes, 7, 64, 512));
    let (last, whole) = pieces.split_last().expect("pieces");
    assert!(whole.iter().all(|piece| (64..=512).contains(&piece.len())));
    assert!((1..=512).contains(&last.len()));

    let mut offset = 0;
    for piece in &pieces {
        assert_eq!(piece.as_ptr(), bytes[offset..].as_ptr());
        offset += piece.len();
    }
    assert_eq!(offset, bytes.len());
}

#[test]
fn split_at_skips_empty_pieces() {
    let bytes = Bytes::from_static(b"data: x\r\n\r\n");
    let pieces = split_at(&bytes, &[0, 8, 8, 9]);
    assert_eq!(
        pieces,
        [
            Bytes::from_static(b"data: x\r"),
            Bytes::from_static(b"\n"),
            Bytes::from_static(b"\r\n"),
        ]
    );
}
