//! The committed SSE response: forwards the upstream's frames as they are
//! (R6), or in strip mode without the usage-only events the gateway asked
//! for itself, while framing events, watching the idle and first-byte
//! deadlines and settling once (sections 2.5 to 2.8).

use std::fmt;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use http_body::{Body, Frame, SizeHint};
use tokio::runtime::Handle;

use super::chunk::{StreamScan, Strip};
use super::idle::{Expired, IdleClock};
use super::settle::{self, DropPlan, Ending, SettleCtx};
use super::{BodyError, BodyTiming, HeldFrames};
use crate::BoxError;
use crate::outcome::OutcomeStatus;
use crate::secret::scrub_error;

/// How `forward` committed an SSE response.
#[derive(Debug)]
pub struct StreamPlan {
    /// The gateway injected `include_usage`; remove usage-only events.
    pub strip_usage_only: bool,
    /// Committed on the last attempt although the first data event carried a
    /// top-level `error` (2.3); the Outcome is `ForwardedError` with the
    /// response status, 200 in practice (D22).
    pub first_event_error: bool,
    /// The request's timer and deadlines.
    pub timing: BodyTiming,
}

/// Starts the drain task of section 2.7 for an upstream body of type `B`.
type SpawnDrain<B> = fn(&Handle, B, HeldFrames, StreamScan, SettleCtx, u64);

/// Body of a committed SSE response.
pub struct PassthroughBody<B> {
    /// `None` only once `Drop` handed it to the drain task.
    upstream: Option<B>,
    /// Frames received before commit, sent before any later frame.
    held: HeldFrames,
    clock: IdleClock,
    scan: StreamScan,
    /// Present in strip mode.
    strip: Option<Strip>,
    first_event_error: bool,
    /// Body bytes sent downstream so far.
    response_bytes: u64,
    /// `None` once the Outcome was emitted.
    settle: Option<SettleCtx>,
    /// Instantiated in [`PassthroughBody::new`], where `B` is known to be a
    /// `Send + 'static` body, so that `Drop`, which cannot require bounds the
    /// type does not declare, can still spawn the drain.
    spawn_drain: SpawnDrain<B>,
}

fn spawn_drain<B>(
    handle: &Handle,
    upstream: B,
    held: HeldFrames,
    scan: StreamScan,
    ctx: SettleCtx,
    response_bytes: u64,
) where
    B: Body<Data = Bytes> + Unpin + Send + 'static,
{
    handle.spawn(settle::drain(upstream, held, scan, ctx, response_bytes));
}

impl<B> PassthroughBody<B>
where
    B: Body<Data = Bytes> + Unpin + Send + 'static,
    B::Error: Into<BoxError>,
{
    /// `held` are the frames received before commit, in order, not yet
    /// scanned; they are emitted first through the same path as later frames.
    pub fn new(upstream: B, held: HeldFrames, plan: StreamPlan, settle: SettleCtx) -> Self {
        let ended = held.is_empty() && upstream.is_end_stream();
        let mut body = Self {
            upstream: Some(upstream),
            held,
            clock: IdleClock::new(plan.timing),
            scan: StreamScan::default(),
            strip: plan.strip_usage_only.then(Strip::new),
            first_event_error: plan.first_event_error,
            response_bytes: 0,
            settle: Some(settle),
            spawn_drain: spawn_drain::<B>,
        };
        if ended {
            // An empty body: hyper sees `is_end_stream()` before polling and
            // never polls, so the end is settled here. Nothing was staged.
            body.settle_end(0);
        }
        body
    }

    /// Settles as the end of the body if the upstream has nothing left.
    fn end_if_exhausted(&mut self) -> Result<(), BodyError> {
        if self.settle.is_some()
            && self.held.is_empty()
            && self.upstream.as_ref().is_some_and(B::is_end_stream)
        {
            self.end_of_body()
        } else {
            Ok(())
        }
    }
}

impl<B> PassthroughBody<B> {
    /// EOF (1.4.10, rule 9): releases what strip mode holds, then settles.
    fn end_of_body(&mut self) -> Result<(), BodyError> {
        let mut queued = 0;
        if let Some(strip) = &mut self.strip {
            if let Err(error) = strip.finish(&mut self.scan) {
                return Err(self.fail_protocol(error));
            }
            queued = strip.ready_bytes();
        }
        self.settle_end(queued);
        Ok(())
    }

    /// Settles a body whose upstream ended; `queued` bytes are about to be
    /// sent and count as forwarded.
    fn settle_end(&mut self, queued: u64) {
        let status = if self.scan.facts.done {
            OutcomeStatus::Completed
        } else {
            OutcomeStatus::UpstreamTruncated
        };
        self.scan.finish();
        self.settle(status, queued);
    }

    fn fail_upstream(&mut self, error: BoxError) -> BodyError {
        self.abandon(OutcomeStatus::UpstreamTruncated);
        BodyError::Upstream(scrub_error(error))
    }

    fn fail_protocol(&mut self, error: brisk_proto::sse::SseError) -> BodyError {
        self.abandon(OutcomeStatus::UpstreamProtocol);
        BodyError::Protocol(error)
    }

    fn expire(&mut self, expired: Expired) -> BodyError {
        match expired {
            Expired::FirstByte => {
                self.abandon(OutcomeStatus::FirstByteTimeout);
                BodyError::FirstByte
            }
            Expired::Idle(idle) => {
                self.abandon(OutcomeStatus::IdleTimeout);
                BodyError::Idle(idle)
            }
        }
    }

    /// Ends the stream on an error: nothing held is sent any more, and the
    /// upstream connection is closed at once.
    fn abandon(&mut self, status: OutcomeStatus) {
        if let Some(strip) = &mut self.strip {
            strip.clear();
        }
        self.settle(status, 0);
        self.upstream = None;
    }

    fn settle(&mut self, status: OutcomeStatus, queued: u64) {
        let Some(ctx) = self.settle.take() else {
            return;
        };
        let ending = self.ending(status, ctx.http_status, queued);
        ctx.settle(ending);
    }

    fn ending(&self, status: OutcomeStatus, http_status: u16, queued: u64) -> Ending {
        let facts = &self.scan.facts;
        Ending {
            // D22: an error-only stream is a forwarded error however it ends.
            status: if self.first_event_error {
                OutcomeStatus::ForwardedError {
                    status: http_status,
                }
            } else {
                status
            },
            usage: facts.usage.get(),
            usage_final: facts.usage_final,
            usage_error: facts.usage_error,
            response_bytes: self.response_bytes + queued,
        }
    }
}

impl<B> Body for PassthroughBody<B>
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
            if let Some(frame) = this.strip.as_mut().and_then(Strip::pop_ready) {
                this.response_bytes += frame.len() as u64;
                return Poll::Ready(Some(Ok(Frame::data(frame))));
            }
            if this.settle.is_none() {
                return Poll::Ready(None);
            }
            // Held frames arrived before commit, where the idle clock starts,
            // so only upstream frames count as bytes for it.
            let (polled, arrived) = match this.held.pop_front() {
                Some(frame) => (Poll::Ready(Some(Ok(Frame::data(frame)))), false),
                None => match this.upstream.as_mut() {
                    Some(upstream) => (Pin::new(upstream).poll_frame(cx), true),
                    None => (Poll::Ready(None), true),
                },
            };
            let data = match polled {
                Poll::Ready(Some(Ok(frame))) => match frame.into_data() {
                    Ok(data) if !data.is_empty() => data,
                    // Trailers are not forwarded (rule 8), and an empty frame
                    // carries nothing to forward.
                    _ => {
                        if let Err(error) = this.end_if_exhausted() {
                            return Poll::Ready(Some(Err(error)));
                        }
                        continue;
                    }
                },
                Poll::Ready(Some(Err(error))) => {
                    return Poll::Ready(Some(Err(this.fail_upstream(error.into()))));
                }
                Poll::Ready(None) => {
                    if let Err(error) = this.end_of_body() {
                        return Poll::Ready(Some(Err(error)));
                    }
                    continue;
                }
                Poll::Pending => {
                    return match this.clock.poll_expired(cx) {
                        Poll::Pending => Poll::Pending,
                        Poll::Ready(expired) => Poll::Ready(Some(Err(this.expire(expired)))),
                    };
                }
            };
            if arrived {
                this.clock.on_bytes();
            }
            let pushed = match &mut this.strip {
                Some(strip) => strip.push(&mut this.scan, data),
                None => match this.scan.scan(&data) {
                    Ok(()) => {
                        this.response_bytes += data.len() as u64;
                        if let Err(error) = this.end_if_exhausted() {
                            return Poll::Ready(Some(Err(error)));
                        }
                        return Poll::Ready(Some(Ok(Frame::data(data))));
                    }
                    Err(error) => Err(error),
                },
            };
            if let Err(error) = pushed {
                return Poll::Ready(Some(Err(this.fail_protocol(error))));
            }
            if let Err(error) = this.end_if_exhausted() {
                return Poll::Ready(Some(Err(error)));
            }
        }
    }

    fn is_end_stream(&self) -> bool {
        self.settle.is_none() && self.strip.as_ref().is_none_or(Strip::is_drained)
    }

    fn size_hint(&self) -> SizeHint {
        if self.strip.is_some() {
            return SizeHint::default();
        }
        let held = self.held.len_bytes() as u64;
        let upstream = self
            .upstream
            .as_ref()
            .map_or_else(|| SizeHint::with_exact(0), B::size_hint);
        let mut hint = SizeHint::new();
        hint.set_lower(upstream.lower().saturating_add(held));
        if let Some(upper) = upstream.upper() {
            hint.set_upper(upper.saturating_add(held));
        }
        hint
    }
}

impl<B> Drop for PassthroughBody<B> {
    /// Section 2.7: only atomics, `try_send`, `Handle::try_current` and
    /// `Handle::spawn` happen here (R15).
    fn drop(&mut self) {
        let Some(ctx) = self.settle.take() else {
            return;
        };
        let facts = &self.scan.facts;
        // After `[DONE]` nothing more is coming, and an error-only stream
        // stays unbilled whatever follows (D22).
        let wants_drain =
            facts.finished && !facts.usage_final && !facts.done && !self.first_event_error;
        let status = match ctx.shared.plan_drop(wants_drain) {
            DropPlan::ShutdownAborted => OutcomeStatus::ShutdownAborted,
            DropPlan::Cancelled => OutcomeStatus::ClientCancelled,
            DropPlan::Drain(handle) => {
                if let Some(upstream) = self.upstream.take() {
                    (self.spawn_drain)(
                        &handle,
                        upstream,
                        std::mem::take(&mut self.held),
                        std::mem::take(&mut self.scan),
                        ctx,
                        self.response_bytes,
                    );
                    return;
                }
                OutcomeStatus::ClientCancelled
            }
        };
        let ending = self.ending(status, ctx.http_status, 0);
        ctx.settle(ending);
    }
}

impl<B> fmt::Debug for PassthroughBody<B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PassthroughBody")
            .field("held", &self.held)
            .field("clock", &self.clock)
            .field("scan", &self.scan)
            .field("strip", &self.strip)
            .field("first_event_error", &self.first_event_error)
            .field("response_bytes", &self.response_bytes)
            .field("settle", &self.settle)
            .finish_non_exhaustive()
    }
}
