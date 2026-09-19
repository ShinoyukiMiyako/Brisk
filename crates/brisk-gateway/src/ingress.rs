//! Request-body intake under the in-flight byte budget, with a size limit, a
//! total read deadline and a minimum transfer rate, so a slow or oversized
//! body cannot hold memory for long (R13, R20).
//!
//! With a Content-Length the whole body is reserved in the budget before the
//! first byte is read, so an overloaded gateway answers 503 at once (R20).
//! Such a reservation must then be justified by progress: besides the fixed
//! minimum rate, a declared length has to arrive at least at half the
//! average rate that would finish it within the read timeout (D25), so a
//! trickling client cannot sit on a 32 MiB reservation for the whole minute.

use std::future::poll_fn;
use std::pin::{Pin, pin};
use std::task::{Context, Poll, ready};
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use http::HeaderMap;
use http::header::CONTENT_ENCODING;
use tokio::time::{Instant, Sleep, sleep_until};

use crate::BoxError;
use crate::budget::{BudgetPermit, ByteBudget};
use crate::spec::LimitsSpec;

/// Reads the whole body. With an exact size hint (Content-Length) the budget
/// is reserved once up front; otherwise the permit grows frame by frame.
/// A single-frame body is returned without copying; several frames are
/// copied once into a buffer sized from the hint when known.
///
/// Deadlines (R13): the total `body_read_timeout` counts from the call. A body
/// that is already complete in the transport's buffer is returned from the
/// first poll without reading the clock or creating a timer; the timer is
/// created at the first `Pending`, which on that path comes right after the
/// call. At the end of the k-th `body_min_rate_window` of an unfinished body
/// (k from 1) the bytes received so far must reach the progress floor:
///
/// - without a Content-Length: `k × body_min_rate_bytes`;
/// - with a Content-Length `CL`: `min(CL, k × max(body_min_rate_bytes,
///   ceil(CL × window / (2 × body_read_timeout))))` (D25).
///
/// Both checks share the one timer. A zero `body_min_rate_window` disables
/// the progress floor and leaves only the total deadline.
///
/// Trailers are ignored. Bytes beyond a declared length (a body lying about
/// its size) are reserved like chunked bytes, so the permit always covers
/// the returned bytes. On every error the permit is dropped and its bytes
/// return to the budget.
pub async fn read_body<'b, B>(
    body: B,
    limits: &LimitsSpec,
    budget: &'b ByteBudget,
) -> Result<(Bytes, BudgetPermit<'b>), IngressError>
where
    B: http_body::Body<Data = Bytes> + Unpin,
    B::Error: Into<BoxError>,
{
    let too_large = IngressError::TooLarge {
        limit: limits.max_body,
    };
    let declared = body.size_hint().exact();
    let reserve = match declared {
        Some(length) if length > u64::from(limits.max_body) => return Err(too_large),
        Some(length) => usize::try_from(length).map_err(|_| too_large)?,
        None => 0,
    };
    let permit = budget.try_permit(reserve).ok_or(IngressError::Overloaded)?;

    let mut reader = Reader {
        body,
        limits,
        declared,
        permit,
        received: 0,
        first: None,
        joined: None,
        deadlines: None,
    };
    let mut timer = pin!(None::<Sleep>);
    poll_fn(|cx| reader.poll_read(cx, timer.as_mut())).await?;
    let bytes = reader.take_bytes();
    Ok((bytes, reader.permit))
}

/// The request's `Content-Encoding` names a coding other than `identity`
/// (D29): the body must be refused with 415 before any byte is read, since
/// Brisk does not decompress and compressed bytes would otherwise surface as
/// a confusing 400 `invalid_json`.
///
/// Values are comma-separated codings compared case-insensitively; empty
/// list elements are ignored, as RFC 9110 section 5.6.1 allows. A value that
/// is not valid text counts as compressed.
pub(crate) fn has_compressed_body(headers: &HeaderMap) -> bool {
    headers.get_all(CONTENT_ENCODING).iter().any(|value| {
        value.as_bytes().split(|&byte| byte == b',').any(|coding| {
            let coding = coding.trim_ascii();
            !coding.is_empty() && !coding.eq_ignore_ascii_case(b"identity")
        })
    })
}

/// Why a request body was not read.
#[derive(Debug, thiserror::Error)]
pub enum IngressError {
    /// The declared or received size exceeds `max_body` (413).
    #[error("request body exceeds {limit} bytes")]
    TooLarge {
        /// The configured `max_body`.
        limit: u32,
    },
    /// The in-flight budget cannot hold the body (503).
    #[error("request body budget exhausted")]
    Overloaded,
    /// The body did not finish within `body_read_timeout` (408).
    #[error("request body not received within the read timeout")]
    Timeout,
    /// The body fell below the progress floor at a window end (408).
    #[error("request body below the minimum rate")]
    TooSlow,
    /// The client aborted the request body; no response is written.
    #[error("client aborted the request body")]
    Aborted(#[source] BoxError),
}

/// State of one `read_body` call.
struct Reader<'l, 'b, B> {
    body: B,
    limits: &'l LimitsSpec,
    /// The exact size hint, i.e. the Content-Length.
    declared: Option<u64>,
    permit: BudgetPermit<'b>,
    received: u64,
    /// The first data frame, returned as-is if no other frame follows.
    first: Option<Bytes>,
    /// All frames so far, once a second one arrived.
    joined: Option<BytesMut>,
    /// Set when the body first returned `Pending`.
    deadlines: Option<Deadlines>,
}

/// Deadlines of a body that was not complete on the first poll.
#[derive(Debug, Clone, Copy)]
struct Deadlines {
    start: Instant,
    /// `start + body_read_timeout`.
    total: Instant,
    /// Index of the window whose end the timer waits for, from 1.
    window: u32,
}

impl<B> Reader<'_, '_, B>
where
    B: http_body::Body<Data = Bytes> + Unpin,
    B::Error: Into<BoxError>,
{
    /// Reads frames until the end of the body or `Pending`; on `Pending`,
    /// polls the shared timer and enforces the deadlines.
    fn poll_read(
        &mut self,
        cx: &mut Context<'_>,
        mut timer: Pin<&mut Option<Sleep>>,
    ) -> Poll<Result<(), IngressError>> {
        loop {
            match Pin::new(&mut self.body).poll_frame(cx) {
                Poll::Ready(Some(Ok(frame))) => {
                    if let Ok(data) = frame.into_data() {
                        self.push(data)?;
                    }
                }
                Poll::Ready(Some(Err(err))) => {
                    return Poll::Ready(Err(IngressError::Aborted(err.into())));
                }
                Poll::Ready(None) => return Poll::Ready(Ok(())),
                Poll::Pending => {
                    ready!(self.poll_deadlines(cx, timer.as_mut()))?;
                }
            }
        }
    }

    /// `Pending` while every deadline holds; an error once one is missed.
    /// Never `Ready(Ok)`.
    fn poll_deadlines(
        &mut self,
        cx: &mut Context<'_>,
        mut timer: Pin<&mut Option<Sleep>>,
    ) -> Poll<Result<(), IngressError>> {
        if self.deadlines.is_none() {
            let start = Instant::now();
            let deadlines = Deadlines {
                start,
                total: start + self.limits.body_read_timeout,
                window: 1,
            };
            self.deadlines = Some(deadlines);
            timer.set(Some(sleep_until(self.next_deadline(deadlines))));
        }
        let (Some(mut deadlines), Some(mut sleep)) = (self.deadlines, timer.as_mut().as_pin_mut())
        else {
            unreachable!("the deadlines and the timer are created together");
        };
        loop {
            ready!(sleep.as_mut().poll(cx));
            if sleep.deadline() >= deadlines.total {
                return Poll::Ready(Err(IngressError::Timeout));
            }
            if self.received < self.progress_floor(deadlines.window) {
                return Poll::Ready(Err(IngressError::TooSlow));
            }
            deadlines.window += 1;
            self.deadlines = Some(deadlines);
            sleep.as_mut().reset(self.next_deadline(deadlines));
        }
    }

    /// The end of the current window, or the total deadline if that is
    /// earlier or the progress floor is disabled.
    fn next_deadline(&self, deadlines: Deadlines) -> Instant {
        let window = self.limits.body_min_rate_window;
        if window.is_zero() {
            return deadlines.total;
        }
        deadlines
            .start
            .checked_add(window.saturating_mul(deadlines.window))
            .map_or(deadlines.total, |end| end.min(deadlines.total))
    }

    /// Bytes that must have arrived by the end of window `k`.
    fn progress_floor(&self, k: u32) -> u64 {
        let min_rate = u64::from(self.limits.body_min_rate_bytes);
        let Some(declared) = self.declared else {
            return min_rate.saturating_mul(u64::from(k));
        };
        let proportional = proportional_rate(
            declared,
            self.limits.body_min_rate_window,
            self.limits.body_read_timeout,
        );
        declared.min(min_rate.max(proportional).saturating_mul(u64::from(k)))
    }

    fn push(&mut self, data: Bytes) -> Result<(), IngressError> {
        if data.is_empty() {
            return Ok(());
        }
        let too_large = IngressError::TooLarge {
            limit: self.limits.max_body,
        };
        self.received += data.len() as u64;
        if self.received > u64::from(self.limits.max_body) {
            return Err(too_large);
        }
        let received = usize::try_from(self.received).map_err(|_| too_large)?;
        let held = self.permit.bytes();
        if received > held && !self.permit.try_grow(received - held) {
            return Err(IngressError::Overloaded);
        }

        if let Some(joined) = &mut self.joined {
            joined.extend_from_slice(&data);
        } else if let Some(first) = self.first.take() {
            let capacity = self
                .declared
                .and_then(|length| usize::try_from(length).ok())
                .map_or(received, |length| length.max(received));
            let mut joined = BytesMut::with_capacity(capacity);
            joined.extend_from_slice(&first);
            joined.extend_from_slice(&data);
            self.joined = Some(joined);
        } else {
            self.first = Some(data);
        }
        Ok(())
    }

    fn take_bytes(&mut self) -> Bytes {
        match (self.joined.take(), self.first.take()) {
            (Some(joined), _) => joined.freeze(),
            (None, Some(first)) => first,
            (None, None) => Bytes::new(),
        }
    }
}

/// `ceil(declared × window / (2 × timeout))`: half the average rate that
/// would deliver `declared` bytes within `timeout`, per window.
fn proportional_rate(declared: u64, window: Duration, timeout: Duration) -> u64 {
    let numerator = u128::from(declared) * window.as_nanos();
    // A zero timeout expires before any window ends, so this rate is never
    // compared then; `max(1)` only keeps the division defined.
    let denominator = (2 * timeout.as_nanos()).max(1);
    u64::try_from(numerator.div_ceil(denominator)).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn headers(values: &[&str]) -> HeaderMap {
        let mut map = HeaderMap::new();
        for value in values {
            map.append(CONTENT_ENCODING, value.parse().expect("valid header value"));
        }
        map
    }

    #[test]
    fn identity_or_absent_encoding_is_accepted() {
        assert!(!has_compressed_body(&HeaderMap::new()));
        assert!(!has_compressed_body(&headers(&["identity"])));
        assert!(!has_compressed_body(&headers(&["Identity"])));
        assert!(!has_compressed_body(&headers(&[" identity , IDENTITY"])));
        assert!(!has_compressed_body(&headers(&["", "identity"])));
    }

    #[test]
    fn any_other_coding_is_compressed() {
        for value in [
            "zstd",
            "gzip",
            "br",
            "deflate",
            "x-custom",
            "identity, gzip",
        ] {
            assert!(has_compressed_body(&headers(&[value])), "{value}");
        }
        assert!(has_compressed_body(&headers(&["identity", "zstd"])));
    }

    #[test]
    fn proportional_rate_matches_d25() {
        let window = Duration::from_secs(10);
        let timeout = Duration::from_secs(60);
        // 32 MiB × 10 s / 120 s, rounded up: about 2.7 MiB per window.
        assert_eq!(proportional_rate(32 << 20, window, timeout), 2_796_203);
        assert_eq!(proportional_rate(2048, window, timeout), 171);
        assert_eq!(proportional_rate(12, window, timeout), 1);
        assert_eq!(proportional_rate(0, window, timeout), 0);
        assert_eq!(
            proportional_rate(u64::MAX, window, Duration::ZERO),
            u64::MAX
        );
    }
}
