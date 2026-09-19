//! The committed response that is not SSE: forwarded frame by frame while
//! a bounded copy of the references is kept, so that the usage of a JSON
//! `chat.completion` can be parsed at EOF (2.2). Forwarded non-2xx
//! responses go through here too, without retention.

use std::fmt;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use brisk_proto::usage::{CompletionFacts, UsageError, parse_completion, parse_completion_from};
use brisk_proto::{UsageErrorKind, UsageTokens};
use bytes::Bytes;
use http_body::{Body, Frame, SizeHint};

use super::idle::{Expired, IdleClock};
use super::settle::{DropPlan, Ending, SettleCtx};
use super::{BodyError, BodyTiming, HeldFrames};
use crate::BoxError;
use crate::outcome::OutcomeStatus;
use crate::secret::scrub_error;

/// Largest body retained for usage parsing. A longer body is forwarded in
/// full but settled as having no usage.
pub const MAX_TAP_BYTES: usize = 16 << 20;

/// Budget reserved per step when the body's length is not known up front,
/// so that the shared budget is touched once per MiB and not once per frame.
const RESERVE_STEP: usize = 1 << 20;

/// How `forward` committed a response that is not SSE.
#[derive(Debug)]
pub struct TapPlan {
    /// False for forwarded non-2xx responses: nothing is retained or parsed,
    /// nothing is billed.
    pub usage_expected: bool,
    /// The request's timer and deadlines.
    pub timing: BodyTiming,
}

/// Body of a committed response that is not SSE.
pub struct ResponseTap<B> {
    upstream: B,
    clock: IdleClock,
    usage_expected: bool,
    /// Frames kept for parsing, sharing their buffers with the frames sent.
    retained: HeldFrames,
    /// Still retaining: usage is expected, and neither the shared budget nor
    /// [`MAX_TAP_BYTES`] has been exceeded.
    retaining: bool,
    /// Bytes reserved from `SettleShared::tap_budget`.
    reserved: usize,
    /// Body bytes sent downstream so far.
    response_bytes: u64,
    /// `None` once the Outcome was emitted.
    settle: Option<SettleCtx>,
}

impl<B> ResponseTap<B>
where
    B: Body<Data = Bytes> + Unpin + Send + 'static,
    B::Error: Into<BoxError>,
{
    /// Reserves the whole body's retention up front when its exact length is
    /// known.
    pub fn new(upstream: B, plan: TapPlan, settle: SettleCtx) -> Self {
        let exact = upstream.size_hint().exact();
        let ended = upstream.is_end_stream();
        let mut tap = Self {
            upstream,
            clock: IdleClock::new(plan.timing),
            usage_expected: plan.usage_expected,
            retained: HeldFrames::default(),
            retaining: plan.usage_expected,
            reserved: 0,
            response_bytes: 0,
            settle: Some(settle),
        };
        if tap.retaining
            && let Some(exact) = exact
        {
            tap.reserve_exact(exact);
        }
        if ended {
            // hyper does not poll a body that reports its end up front.
            tap.finish();
        }
        tap
    }

    fn end_if_exhausted(&mut self) {
        if self.settle.is_some() && self.upstream.is_end_stream() {
            self.finish();
        }
    }

    /// EOF (`ResponseTap` rules 4 and 5): parses what was retained, then
    /// settles. Without retention the body settles as having no usage; a
    /// forwarded non-2xx becomes a `ForwardedError` in [`Self::settle`].
    fn finish(&mut self) {
        if !self.retaining {
            self.settle(OutcomeStatus::Completed, None, None);
            return;
        }
        let (status, usage, usage_error) = match parse(&self.retained) {
            Ok(CompletionFacts {
                usage: Some(usage), ..
            }) => (OutcomeStatus::Completed, Some(usage), None),
            // D22: a 2xx that carried only an error generated nothing.
            Ok(CompletionFacts {
                usage: None,
                error: true,
            }) => {
                let status = self.settle.as_ref().map_or(200, |ctx| ctx.http_status);
                (OutcomeStatus::ForwardedError { status }, None, None)
            }
            Ok(CompletionFacts {
                usage: None,
                error: false,
            }) => (OutcomeStatus::Completed, None, None),
            Err(error) => (OutcomeStatus::Completed, None, Some(error.kind())),
        };
        self.settle(status, usage, usage_error);
    }
}

impl<B> ResponseTap<B> {
    fn reserve_exact(&mut self, exact: u64) {
        let Some(ctx) = &self.settle else {
            return;
        };
        match usize::try_from(exact) {
            // A body known to exceed the cap would leave retention once it
            // passed it; leaving right away spares holding the budget and
            // the frames until then.
            Ok(len) if len <= MAX_TAP_BYTES && ctx.shared.tap_budget.try_reserve(len) => {
                self.reserved = len;
            }
            _ => self.retaining = false,
        }
    }

    /// Keeps a reference to `data` for parsing at EOF, within the budget.
    fn retain(&mut self, data: &Bytes) {
        let Some(ctx) = &self.settle else {
            return;
        };
        let needed = self.retained.len_bytes() + data.len();
        if needed > MAX_TAP_BYTES {
            self.stop_retaining();
            return;
        }
        if needed > self.reserved {
            let grant = (needed - self.reserved)
                .max(RESERVE_STEP)
                .min(MAX_TAP_BYTES - self.reserved);
            if !ctx.shared.tap_budget.try_reserve(grant) {
                self.stop_retaining();
                return;
            }
            self.reserved += grant;
        }
        self.retained.push(data.clone());
    }

    /// Gives up on the usage: frees the frames and returns the budget. The
    /// body then settles as having no usage (`usage_missing`).
    fn stop_retaining(&mut self) {
        self.retaining = false;
        self.retained = HeldFrames::default();
        self.release_budget();
    }

    fn release_budget(&mut self) {
        if self.reserved == 0 {
            return;
        }
        if let Some(ctx) = &self.settle {
            ctx.shared.tap_budget.release(self.reserved);
        }
        self.reserved = 0;
    }

    fn fail(&mut self, status: OutcomeStatus) {
        self.settle(status, None, None);
    }

    /// Emits the Outcome once and returns the budget. A forwarded non-2xx
    /// response is a `ForwardedError` however it ends, and is never billed.
    fn settle(
        &mut self,
        status: OutcomeStatus,
        usage: Option<UsageTokens>,
        usage_error: Option<UsageErrorKind>,
    ) {
        self.release_budget();
        self.retained = HeldFrames::default();
        let Some(ctx) = self.settle.take() else {
            return;
        };
        let status = if self.usage_expected {
            status
        } else {
            OutcomeStatus::ForwardedError {
                status: ctx.http_status,
            }
        };
        ctx.settle(Ending {
            status,
            usage,
            // The usage of a complete JSON body is its final one.
            usage_final: usage.is_some(),
            usage_error,
            response_bytes: self.response_bytes,
        });
    }
}

impl<B> Body for ResponseTap<B>
where
    B: Body<Data = Bytes> + Unpin + Send + 'static,
    B::Error: Into<BoxError>,
{
    type Data = Bytes;
    type Error = BodyError;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, BodyError>>> {
        let this = self.get_mut();
        loop {
            if this.settle.is_none() {
                return Poll::Ready(None);
            }
            match Pin::new(&mut this.upstream).poll_frame(cx) {
                Poll::Ready(Some(Ok(frame))) => match frame.into_data() {
                    Ok(data) if !data.is_empty() => {
                        this.clock.on_bytes();
                        this.response_bytes += data.len() as u64;
                        if this.retaining {
                            this.retain(&data);
                        }
                        this.end_if_exhausted();
                        return Poll::Ready(Some(Ok(Frame::data(data))));
                    }
                    // Trailers are not forwarded; an empty frame carries nothing.
                    _ => this.end_if_exhausted(),
                },
                Poll::Ready(Some(Err(error))) => {
                    this.fail(OutcomeStatus::UpstreamTruncated);
                    return Poll::Ready(Some(Err(BodyError::Upstream(scrub_error(error.into())))));
                }
                Poll::Ready(None) => this.finish(),
                Poll::Pending => {
                    return match this.clock.poll_expired(cx) {
                        Poll::Pending => Poll::Pending,
                        Poll::Ready(Expired::FirstByte) => {
                            this.fail(OutcomeStatus::FirstByteTimeout);
                            Poll::Ready(Some(Err(BodyError::FirstByte)))
                        }
                        Poll::Ready(Expired::Idle(idle)) => {
                            this.fail(OutcomeStatus::IdleTimeout);
                            Poll::Ready(Some(Err(BodyError::Idle(idle))))
                        }
                    };
                }
            }
        }
    }

    fn is_end_stream(&self) -> bool {
        self.settle.is_none()
    }

    fn size_hint(&self) -> SizeHint {
        self.upstream.size_hint()
    }
}

impl<B> Drop for ResponseTap<B> {
    /// Section 2.7, rules 1, 2 and 4: no drain for a response that is not
    /// SSE (D16).
    fn drop(&mut self) {
        let status = match &self.settle {
            None => return,
            Some(ctx) => match ctx.shared.plan_drop(false) {
                DropPlan::ShutdownAborted => OutcomeStatus::ShutdownAborted,
                DropPlan::Cancelled | DropPlan::Drain(_) => OutcomeStatus::ClientCancelled,
            },
        };
        self.settle(status, None, None);
    }
}

impl<B> fmt::Debug for ResponseTap<B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ResponseTap")
            .field("clock", &self.clock)
            .field("usage_expected", &self.usage_expected)
            .field("retained", &self.retained)
            .field("retaining", &self.retaining)
            .field("reserved", &self.reserved)
            .field("response_bytes", &self.response_bytes)
            .field("settle", &self.settle)
            .finish_non_exhaustive()
    }
}

/// Parses the retained body without joining its frames first.
fn parse(frames: &HeldFrames) -> Result<CompletionFacts, UsageError> {
    let mut iter = frames.iter();
    match (iter.next(), iter.next()) {
        (Some(only), None) => parse_completion(only),
        (None, _) => parse_completion(&[]),
        (Some(_), Some(_)) => parse_completion_from(FramesReader {
            frames: frames.iter(),
            current: &[],
        }),
    }
}

/// `io::Read` over a sequence of frames, in order.
struct FramesReader<'a, I> {
    frames: I,
    /// The unread rest of the current frame.
    current: &'a [u8],
}

impl<'a, I> io::Read for FramesReader<'a, I>
where
    I: Iterator<Item = &'a Bytes>,
{
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        while self.current.is_empty() {
            match self.frames.next() {
                Some(frame) => self.current = frame,
                None => return Ok(0),
            }
        }
        let n = self.current.len().min(buf.len());
        buf[..n].copy_from_slice(&self.current[..n]);
        self.current = &self.current[n..];
        Ok(n)
    }
}

#[cfg(test)]
mod tests {
    use std::io::Read;

    use super::*;

    #[test]
    fn frames_reader_reads_across_frames() {
        let mut held = HeldFrames::default();
        for piece in [&b"ab"[..], b"", b"cde", b"f"] {
            held.push(Bytes::copy_from_slice(piece));
        }
        let mut reader = FramesReader {
            frames: held.iter(),
            current: &[],
        };
        let mut small = [0u8; 2];
        assert_eq!(reader.read(&mut small).unwrap(), 2);
        assert_eq!(&small, b"ab");
        let mut rest = Vec::new();
        reader.read_to_end(&mut rest).unwrap();
        assert_eq!(rest, b"cdef");
        assert_eq!(reader.read(&mut small).unwrap(), 0);
    }

    #[test]
    fn multi_frame_parse_matches_single_frame() {
        let body: &[u8] = br#"{"id":"x","choices":[{"message":{"content":"pong"}}],"usage":{"prompt_tokens":213,"completion_tokens":128}}"#;
        let single = parse_completion(body).unwrap();
        for cut in 1..body.len() {
            let mut held = HeldFrames::default();
            held.push(Bytes::copy_from_slice(&body[..cut]));
            held.push(Bytes::copy_from_slice(&body[cut..]));
            assert_eq!(parse(&held).unwrap(), single, "cut at {cut}");
        }
    }
}
