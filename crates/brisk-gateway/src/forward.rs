//! The forwarding core for `POST /v1/chat/completions`: authentication, body
//! intake, head parsing, channel selection, request rewriting, failover
//! strictly before commit, and the hand-over of a committed response to its
//! body (R4, R13, R14, R16, R17, R19).
//!
//! Every request produces exactly one `Outcome`. Until commit it is owned by
//! [`Pending`], which emits it on every early answer and, from its `Drop`,
//! when hyper drops the handler because the client left. At commit the
//! responsibility moves to the response body with its `SettleCtx`.
//!
//! The request has one timer (D31): created when the first attempt is sent,
//! reset for each later attempt and for the commit window, and handed to the
//! body at commit.

use std::error::Error as StdError;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use brisk_proto::jsonhead::{ChatHead, HeadError};
use brisk_proto::splice::{Rewrite, SpliceError, plan_chat};
use brisk_proto::sse::{Data, Event, MAX_EVENT_BYTES, SseError, SseScanner};
use brisk_proto::usage::{may_carry_error, parse_chunk};
use bytes::Bytes;
use http::header::CONTENT_TYPE;
use http::{HeaderMap, Method, Request, Response, StatusCode};
use http_body_util::{BodyExt, Full};
use tokio::time::{Instant, Sleep, sleep_until};

use crate::BoxError;
use crate::auth::AuthError;
use crate::body::{
    BodyTiming, HeldFrames, PassthroughBody, ResponseBody, ResponseTap, SettleCtx, SpliceBody,
    StreamPlan, TapPlan,
};
use crate::gateway::{Gateway, next_request_id};
use crate::headers::{downstream_response_headers, upstream_request_headers};
use crate::ingress::{IngressError, has_compressed_body, read_body};
use crate::outcome::{self, FailureClass, Outcome, OutcomeStatus, RejectReason, SettleInput};
use crate::reply::{ErrorCode, error_response, invalid_json};
use crate::secret::scrub_error;
use crate::select::pick;
use crate::spec::{ChannelId, ForwardingSpec, KeyId, SpliceMode, StreamUsage};
use crate::state::Snapshot;
use crate::upstream::registry::ChannelRuntime;

/// Handles `POST /v1/chat/completions`; every failure becomes a response.
///
/// Steps 4 to 15 of section 2.1: authentication, the `Content-Encoding`
/// check (D29), body intake, head parsing, candidate channels, then attempts
/// until one commits or none is left (2.3, 2.4).
pub(crate) async fn chat_completions<B>(
    gateway: &Gateway,
    request: Request<B>,
) -> Response<ResponseBody>
where
    B: http_body::Body<Data = Bytes> + Unpin + Send,
    B::Error: Into<BoxError>,
{
    let mut pending = Pending::new(gateway);
    let snapshot = gateway.state().resolve();
    let (parts, body) = request.into_parts();

    let key = match snapshot.keys.authenticate(&parts.headers) {
        Ok(entry) => entry.id,
        Err(AuthError::Missing) => {
            return pending.reject(RejectReason::MissingKey, ErrorCode::InvalidApiKey);
        }
        Err(AuthError::Malformed | AuthError::Unknown) => {
            return pending.reject(RejectReason::InvalidKey, ErrorCode::InvalidApiKey);
        }
    };
    pending.key = Some(key);

    // D29: refused before a byte of the body is budgeted or read.
    if has_compressed_body(&parts.headers) {
        return pending.reject(
            RejectReason::UnsupportedEncoding,
            ErrorCode::UnsupportedContentEncoding,
        );
    }

    let (body, permit) = match read_body(body, gateway.limits(), gateway.body_budget()).await {
        Ok(read) => read,
        Err(error) => return pending.reject_body(&error),
    };
    pending.request_bytes = body.len() as u64;

    let head = match ChatHead::parse(&body) {
        Ok(head) => head,
        Err(error) => return pending.reject_head(&error),
    };
    pending.stream = head.stream;

    let candidates = snapshot.routes.candidates(&head.model_name);
    if candidates == 0 {
        return pending.reject(RejectReason::ModelNotFound, ErrorCode::ModelNotFound);
    }

    let exchange = Exchange {
        snapshot,
        client_headers: &parts.headers,
        body: &body,
        head: &head,
        key,
    };
    let response = exchange.forward(&mut pending, candidates).await;
    // R14: the budget and the request body are released once the response
    // head is decided, not held while the response body streams.
    drop(permit);
    response
}

/// Answers a request Brisk refuses without forwarding it, such as a route
/// miss, and records it as `Rejected(reason)`.
pub(crate) fn reject(
    gateway: &Gateway,
    reason: RejectReason,
    code: ErrorCode,
) -> Response<ResponseBody> {
    Pending::new(gateway).reject(reason, code)
}

/// The request as far as it is known before commit, and the owner of its
/// `Outcome` until then.
struct Pending<'g> {
    gateway: &'g Gateway,
    request_id: u64,
    started: Instant,
    key: Option<KeyId>,
    /// The channel of the latest attempt.
    channel: Option<ChannelId>,
    attempts: u8,
    stream: bool,
    request_bytes: u64,
    /// The `Outcome` is still this struct's to emit.
    armed: bool,
}

impl<'g> Pending<'g> {
    fn new(gateway: &'g Gateway) -> Self {
        Self {
            gateway,
            request_id: next_request_id(),
            started: Instant::now(),
            key: None,
            channel: None,
            attempts: 0,
            stream: false,
            request_bytes: 0,
            armed: true,
        }
    }

    /// Brisk's own error response for `code`, recorded as `Rejected(reason)`.
    fn reject(&mut self, reason: RejectReason, code: ErrorCode) -> Response<ResponseBody> {
        self.answer(OutcomeStatus::Rejected(reason), error_response(code))
    }

    /// Every upstream attempt failed before commit (2.3, last attempt).
    fn fail(&mut self, class: FailureClass, code: ErrorCode) -> Response<ResponseBody> {
        self.answer(OutcomeStatus::Failed(class), error_response(code))
    }

    fn reject_body(&mut self, error: &IngressError) -> Response<ResponseBody> {
        match error {
            IngressError::TooLarge { .. } => {
                self.reject(RejectReason::BodyTooLarge, ErrorCode::RequestTooLarge)
            }
            IngressError::Overloaded => {
                self.reject(RejectReason::Overloaded, ErrorCode::Overloaded)
            }
            IngressError::Timeout => {
                self.reject(RejectReason::BodyTimeout, ErrorCode::RequestTimeout)
            }
            IngressError::TooSlow => {
                self.reject(RejectReason::BodyTooSlow, ErrorCode::RequestTimeout)
            }
            IngressError::Aborted(cause) => {
                tracing::debug!(
                    request_id = self.request_id,
                    error = %Chain(cause.as_ref()),
                    "client aborted the request body"
                );
                // No response reaches a client whose body broke off, so the
                // outcome carries no status; the 400 only completes the
                // service call.
                self.emit(OutcomeStatus::Rejected(RejectReason::ClientAborted), None);
                let mut response = Response::new(ResponseBody::from(Full::new(Bytes::new())));
                *response.status_mut() = StatusCode::BAD_REQUEST;
                response
            }
        }
    }

    fn reject_head(&mut self, error: &HeadError) -> Response<ResponseBody> {
        if matches!(error, HeadError::SpanOutsideBody) {
            tracing::error!(
                request_id = self.request_id,
                "the request head parser returned a span outside the body"
            );
            return self.reject(RejectReason::Internal, ErrorCode::InternalError);
        }
        self.answer(
            OutcomeStatus::Rejected(RejectReason::BadRequest),
            invalid_json(error),
        )
    }

    fn answer(
        &mut self,
        status: OutcomeStatus,
        response: Response<Full<Bytes>>,
    ) -> Response<ResponseBody> {
        self.emit(status, Some(response.status().as_u16()));
        response.map(ResponseBody::from)
    }

    /// Emits the `Outcome` of a request that ends before commit.
    fn emit(&mut self, status: OutcomeStatus, http_status: Option<u16>) {
        self.armed = false;
        let estimate = outcome::estimate(self.request_bytes, 0);
        let billed = outcome::billed(&SettleInput {
            status,
            usage: None,
            usage_final: false,
            usage_error: false,
            estimate,
            before_commit: true,
        });
        self.gateway.sink().emit(Outcome {
            request_id: self.request_id,
            key: self.key,
            channel: self.channel,
            attempts: self.attempts,
            stream: self.stream,
            status,
            http_status,
            usage: None,
            usage_error: None,
            estimate,
            billed,
            request_bytes: self.request_bytes,
            response_bytes: 0,
            elapsed: self.started.elapsed(),
            to_commit: None,
        });
    }

    /// Hands the `Outcome` over to the committed body.
    fn commit(&mut self, key: KeyId, channel: ChannelId, http_status: StatusCode) -> SettleCtx {
        self.armed = false;
        SettleCtx {
            shared: Arc::clone(self.gateway.settle()),
            request_id: self.request_id,
            key,
            channel,
            attempts: self.attempts,
            stream: self.stream,
            http_status: http_status.as_u16(),
            request_bytes: self.request_bytes,
            started: self.started,
            committed: Instant::now(),
        }
    }
}

impl Drop for Pending<'_> {
    /// hyper dropped the handler before commit: the client left (2.7).
    fn drop(&mut self) {
        if self.armed {
            self.emit(OutcomeStatus::ClientCancelled, None);
        }
    }
}

/// What every attempt of one request shares.
struct Exchange<'a> {
    snapshot: &'a Snapshot,
    client_headers: &'a HeaderMap,
    body: &'a Bytes,
    head: &'a ChatHead<'a>,
    key: KeyId,
}

/// Per-attempt facts that decide how a response is handled.
#[derive(Debug, Clone, Copy)]
struct AttemptPlan {
    /// No attempt follows this one (2.3).
    last: bool,
    /// The gateway injected `include_usage`, so usage-only events are
    /// stripped from the stream (D8).
    injected: bool,
}

impl Exchange<'_> {
    /// Attempts channels until one commits; answers with the last failure
    /// when none does.
    async fn forward(&self, pending: &mut Pending<'_>, candidates: u64) -> Response<ResponseBody> {
        let gateway = pending.gateway;
        let channels = &self.snapshot.channels;
        let max_attempts =
            u32::from(gateway.forwarding().max_attempts).min(candidates.count_ones());
        let mut tried = 0_u64;
        let mut timer = None;
        let mut failure = None;
        while u32::from(pending.attempts) < max_attempts {
            let Some(index) = pick(channels.weights(), candidates, tried, |n| {
                fastrand::u64(0..n)
            }) else {
                break;
            };
            tried |= 1 << index;
            let channel = channels.get(index);
            pending.attempts += 1;
            pending.channel = Some(channel.id);
            let last = u32::from(pending.attempts) == max_attempts || candidates & !tried == 0;

            let (upstream_body, injected) = match self.upstream_body(gateway, channel) {
                Ok(planned) => planned,
                Err(error) => {
                    tracing::error!(
                        request_id = pending.request_id,
                        channel = %channel.name,
                        %error,
                        "the request rewrite violated its invariants"
                    );
                    return pending.reject(RejectReason::Internal, ErrorCode::InternalError);
                }
            };
            let plan = AttemptPlan { last, injected };
            match self
                .attempt(pending, channel, upstream_body, plan, timer.take())
                .await
            {
                Ok(response) => return response,
                Err(Failed {
                    failure: next,
                    timer: returned,
                }) => {
                    log_failure(pending.request_id, channel, &next, last);
                    timer = Some(returned);
                    failure = Some(next);
                }
            }
        }
        match failure {
            Some(Failure {
                class,
                reply: Some(code),
                ..
            }) => pending.fail(class, code),
            Some(Failure {
                class, reply: None, ..
            }) => {
                tracing::error!(
                    request_id = pending.request_id,
                    ?class,
                    "the last attempt ended in a failure that only earlier attempts can have"
                );
                pending.reject(RejectReason::Internal, ErrorCode::InternalError)
            }
            None => {
                tracing::error!(
                    request_id = pending.request_id,
                    "no channel could be picked although the model has candidates"
                );
                pending.reject(RejectReason::Internal, ErrorCode::InternalError)
            }
        }
    }

    /// The upstream request body for `channel` (2.1, step 10) and whether
    /// `include_usage` was injected.
    fn upstream_body(
        &self,
        gateway: &Gateway,
        channel: &ChannelRuntime,
    ) -> Result<(reqwest::Body, bool), SpliceError> {
        let head = self.head;
        // D8: only a streaming request that did not ask for usage gets it,
        // and a non-streaming request is never rewritten for it.
        let inject =
            channel.stream_usage == StreamUsage::Inject && head.stream && !head.requests_usage();
        let rewrite = Rewrite {
            model: channel.model_map.get(&*head.model_name),
            inject_include_usage: inject,
        };
        let splice = plan_chat(self.body, head, rewrite)?;
        let body = if splice.is_identity() {
            // reqwest's reusable-bytes path: not boxed, exact length, and
            // the common CPA passthrough case.
            reqwest::Body::from(self.body.clone())
        } else {
            match gateway.experiments().splice {
                SpliceMode::Segments => reqwest::Body::wrap(SpliceBody::new(splice)),
                SpliceMode::Concat => reqwest::Body::from(splice.to_contiguous()),
            }
        };
        Ok((body, inject))
    }

    /// Sends one attempt and classifies its result (2.3); commits when the
    /// response may reach the client.
    async fn attempt(
        &self,
        pending: &mut Pending<'_>,
        channel: &ChannelRuntime,
        body: reqwest::Body,
        plan: AttemptPlan,
        timer: Option<Pin<Box<Sleep>>>,
    ) -> Attempt {
        let mut request = reqwest::Request::new(Method::POST, channel.chat_url.clone());
        *request.headers_mut() =
            upstream_request_headers(self.client_headers, channel.auth.expose());
        *request.body_mut() = Some(body);

        let send_start = Instant::now();
        let first_byte_deadline = deadline_after(send_start, channel.timeouts.first_byte);
        let mut timer = arm(timer, first_byte_deadline);

        let response = tokio::select! {
            biased;
            sent = channel.client.execute(request) => match sent {
                Ok(response) => response,
                Err(error) => return failed(Failure::sending(error), timer),
            },
            () = timer.as_mut() => {
                return failed(
                    Failure::new(FailureClass::FirstByteTimeout, Some(ErrorCode::UpstreamTimeout)),
                    timer,
                );
            }
        };

        let status = response.status();
        let forward_as_is = match classify_status(status) {
            StatusClass::Success => false,
            StatusClass::PassThrough => true,
            // The last attempt forwards 429 and 5xx; a 408 becomes 504 (D3).
            StatusClass::Failover(FailureClass::Status)
                if plan.last && status != StatusCode::REQUEST_TIMEOUT =>
            {
                true
            }
            StatusClass::Failover(class) => {
                abandon(response, pending.gateway.forwarding());
                return failed(Failure::new(class, status_reply(status)), timer);
            }
        };

        let response = http::Response::from(response);
        // D4: only a 2xx SSE response has a first event worth waiting for.
        let sse = !forward_as_is && is_event_stream(response.headers());
        if sse && !channel.timeouts.commit_hold.is_zero() {
            return self
                .hold(pending, channel, response, plan, timer, first_byte_deadline)
                .await;
        }
        let timing = BodyTiming {
            timer,
            first_byte_deadline: Some(first_byte_deadline),
            idle: channel.timeouts.idle,
        };
        let kind = if sse {
            Kind::Stream {
                held: HeldFrames::default(),
                plan: StreamPlan {
                    strip_usage_only: plan.injected,
                    first_event_error: false,
                    timing,
                },
            }
        } else {
            // A forwarded error is neither parsed nor billed.
            Kind::Tap(TapPlan {
                usage_expected: !forward_as_is,
                timing,
            })
        };
        Ok(self.commit(pending, channel, response, kind))
    }

    /// The commit window of a 2xx SSE response (2.4): waits for the first
    /// data event, the end of `commit_hold`, or the first-byte deadline.
    async fn hold(
        &self,
        pending: &mut Pending<'_>,
        channel: &ChannelRuntime,
        response: http::Response<reqwest::Body>,
        plan: AttemptPlan,
        mut timer: Pin<Box<Sleep>>,
        first_byte_deadline: Instant,
    ) -> Attempt {
        let (parts, mut body) = response.into_parts();
        let hold_deadline = deadline_after(Instant::now(), channel.timeouts.commit_hold);
        let hold_ends_first = hold_deadline < first_byte_deadline;
        timer.as_mut().reset(if hold_ends_first {
            hold_deadline
        } else {
            first_byte_deadline
        });

        let mut held = HeldFrames::default();
        let mut first_event = FirstEvent::default();
        let verdict = loop {
            let frame = tokio::select! {
                biased;
                frame = body.frame() => frame,
                () = timer.as_mut() => {
                    // Bytes that are not yet a data event (comments) satisfy
                    // the first-byte deadline; commit with what arrived.
                    if hold_ends_first || held.len_bytes() > 0 {
                        break Verdict::Commit;
                    }
                    return failed(
                        Failure::new(FailureClass::FirstByteTimeout, Some(ErrorCode::UpstreamTimeout)),
                        timer,
                    );
                }
            };
            match frame {
                None => {
                    return failed(
                        Failure::new(
                            FailureClass::EmptyStream,
                            Some(ErrorCode::UpstreamEmptyResponse),
                        ),
                        timer,
                    );
                }
                Some(Err(error)) => {
                    let failure = Failure {
                        class: FailureClass::Transport,
                        reply: Some(ErrorCode::UpstreamUnreachable),
                        error: Some(scrub_error(Box::new(error))),
                    };
                    return failed(failure, timer);
                }
                Some(Ok(frame)) => {
                    let Ok(data) = frame.into_data() else {
                        continue;
                    };
                    if data.is_empty() {
                        continue;
                    }
                    let seen = first_event.feed(&data);
                    held.push(data);
                    match seen {
                        Ok(Some(verdict)) => break verdict,
                        // Frames held before commit are read without
                        // backpressure; an upstream that keeps sending
                        // without completing a data event has answered, so
                        // commit instead of buffering until the deadline.
                        Ok(None) if held.len_bytes() > MAX_HELD_BYTES => break Verdict::Commit,
                        Ok(None) => {}
                        // D17: before commit an oversized event is a channel error.
                        Err(SseError::EventTooLarge { .. }) => {
                            return failed(
                                Failure::new(
                                    FailureClass::Protocol,
                                    Some(ErrorCode::UpstreamProtocolError),
                                ),
                                timer,
                            );
                        }
                    }
                }
            }
        };

        let first_event_error = match verdict {
            Verdict::Commit => false,
            // The last attempt forwards the error stream unbilled (D22).
            Verdict::FirstEventError if plan.last => true,
            // Dropping the body closes the connection; the stream is abandoned.
            Verdict::FirstEventError => {
                return failed(Failure::new(FailureClass::FirstEventError, None), timer);
            }
        };
        let stream = StreamPlan {
            strip_usage_only: plan.injected,
            first_event_error,
            timing: BodyTiming {
                timer,
                // CPA-M1-3: a commit at `commit_hold` expiry before any byte
                // leaves the first-byte deadline to the body.
                first_byte_deadline: (held.len_bytes() == 0).then_some(first_byte_deadline),
                idle: channel.timeouts.idle,
            },
        };
        let response = http::Response::from_parts(parts, body);
        Ok(self.commit(
            pending,
            channel,
            response,
            Kind::Stream { held, plan: stream },
        ))
    }

    /// Step 15 of 2.1: the response head goes out with the allowlisted
    /// headers and the body takes over settlement.
    fn commit(
        &self,
        pending: &mut Pending<'_>,
        channel: &ChannelRuntime,
        response: http::Response<reqwest::Body>,
        kind: Kind,
    ) -> Response<ResponseBody> {
        let (parts, upstream) = response.into_parts();
        let sse = matches!(kind, Kind::Stream { .. });
        let headers =
            downstream_response_headers(&parts.headers, channel.expose_ratelimit_headers, sse);
        let settle = pending.commit(self.key, channel.id, parts.status);
        let body = match kind {
            Kind::Stream { held, plan } => {
                ResponseBody::Stream(PassthroughBody::new(upstream, held, plan, settle))
            }
            Kind::Tap(plan) => ResponseBody::Tap(ResponseTap::new(upstream, plan, settle)),
        };
        let mut response = Response::new(body);
        *response.status_mut() = parts.status;
        *response.headers_mut() = headers;
        response
    }
}

/// How a committed response is carried.
enum Kind {
    /// SSE, through `PassthroughBody`.
    Stream { held: HeldFrames, plan: StreamPlan },
    /// Anything else, through `ResponseTap`.
    Tap(TapPlan),
}

/// Result of one attempt: the committed response, which goes to the client,
/// or a failure before commit.
type Attempt = Result<Response<ResponseBody>, Failed>;

/// An attempt that failed before commit.
struct Failed {
    failure: Failure,
    /// The request's timer, for the next attempt.
    timer: Pin<Box<Sleep>>,
}

fn failed(failure: Failure, timer: Pin<Box<Sleep>>) -> Attempt {
    Err(Failed { failure, timer })
}

/// Why an attempt failed, and Brisk's answer if it was the last one.
#[derive(Debug)]
struct Failure {
    class: FailureClass,
    /// `None` for failures the last attempt forwards instead (429 and 5xx,
    /// a first-event error), which therefore never end a request.
    reply: Option<ErrorCode>,
    /// The transport error, URL removed (R17), for the log only.
    error: Option<BoxError>,
}

impl Failure {
    fn new(class: FailureClass, reply: Option<ErrorCode>) -> Self {
        Self {
            class,
            reply,
            error: None,
        }
    }

    /// No response head arrived: the connection failed, including addresses
    /// the resolver refused, or sending and reading the head broke.
    fn sending(error: reqwest::Error) -> Self {
        let class = if error.is_connect() {
            FailureClass::Connect
        } else {
            FailureClass::Transport
        };
        Self {
            class,
            reply: Some(ErrorCode::UpstreamUnreachable),
            error: Some(scrub_error(Box::new(error))),
        }
    }
}

/// Brisk's answer when a failed-over status ends the last attempt (2.3).
fn status_reply(status: StatusCode) -> Option<ErrorCode> {
    match status.as_u16() {
        300..=399 => Some(ErrorCode::UpstreamRedirect),
        401 | 403 => Some(ErrorCode::UpstreamAuthFailed),
        // D3: the timeout lies between Brisk and the upstream.
        408 => Some(ErrorCode::UpstreamTimeout),
        // Forwarded as they are on the last attempt.
        429 | 500..=599 => None,
        // A status that cannot end a response is a broken exchange.
        _ => Some(ErrorCode::UpstreamUnreachable),
    }
}

/// Disposes of a response a failover abandons (D26). A small body of known
/// length is read to its end by a background task, so its pooled connection
/// survives a rate-limit or 5xx burst; anything else is dropped, which
/// closes the connection.
fn abandon(response: reqwest::Response, forwarding: &ForwardingSpec) {
    let small = response
        .content_length()
        .is_some_and(|length| length <= u64::from(forwarding.drain_max_bytes));
    if !small {
        return;
    }
    let limit = forwarding.drain_timeout;
    let mut response = response;
    tokio::spawn(async move {
        // Only a body read to its end returns the connection to the pool; a
        // failed or late read ends with the drop, which is all it can do.
        let _ = tokio::time::timeout(limit, async move {
            while let Ok(Some(_)) = response.chunk().await {}
        })
        .await;
    });
}

/// Watches the frames before commit for the first data event (2.4, step 3).
/// Used once and dropped: the committed body scans the held frames again.
#[derive(Debug, Default)]
struct FirstEvent {
    scanner: SseScanner,
}

/// What the first data event means for the commit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Verdict {
    /// Content, `[DONE]` or something the body will record: commit.
    Commit,
    /// A top-level `error`: fail over, or forward on the last attempt.
    FirstEventError,
}

impl FirstEvent {
    /// Frames `chunk`; `Some` once the first data event is complete.
    fn feed(&mut self, chunk: &[u8]) -> Result<Option<Verdict>, SseError> {
        let mut verdict = None;
        let scanned = self.scanner.feed(chunk, |event| {
            if verdict.is_none() && !event.is_comment_only() {
                verdict = judge(&event);
            }
        });
        match (verdict, scanned) {
            // An oversized event after the first data event concerns the
            // committed stream, which ends it itself.
            (Some(verdict), _) => Ok(Some(verdict)),
            (None, Err(error)) => Err(error),
            (None, Ok(_)) => Ok(None),
        }
    }
}

/// The verdict of an event, `None` when it carries no `data`.
fn judge(event: &Event<'_>) -> Option<Verdict> {
    match event.data() {
        Data::None => None,
        Data::Single(payload) => Some(judge_payload(payload)),
        Data::Multi => {
            let mut joined = Vec::new();
            event.join_data(&mut joined);
            Some(judge_payload(&joined))
        }
    }
}

/// A payload that fails to parse commits: the upstream has started
/// generating, a resend is costly, and the body records the error when it
/// scans the frame again.
fn judge_payload(payload: &[u8]) -> Verdict {
    if payload != b"[DONE]"
        && may_carry_error(payload)
        && parse_chunk(payload).is_ok_and(|facts| facts.error)
    {
        Verdict::FirstEventError
    } else {
        Verdict::Commit
    }
}

/// Most bytes held before commit. Twice the event limit, so that a single
/// oversized first event is still reported as a protocol error (D17) before
/// this bound applies.
const MAX_HELD_BYTES: usize = 2 * MAX_EVENT_BYTES;

/// tokio's own horizon for a deadline that does not fit an `Instant`.
const FAR_FUTURE: Duration = Duration::from_hours(24 * 365 * 30);

/// `start + duration`; a duration too large for an `Instant` means no
/// practical deadline, as it does for `tokio::time::sleep`.
fn deadline_after(start: Instant, duration: Duration) -> Instant {
    start
        .checked_add(duration)
        .unwrap_or_else(|| start + FAR_FUTURE)
}

/// The request's timer, created on the first send and reset afterwards (D31).
fn arm(timer: Option<Pin<Box<Sleep>>>, deadline: Instant) -> Pin<Box<Sleep>> {
    match timer {
        Some(mut timer) => {
            timer.as_mut().reset(deadline);
            timer
        }
        None => Box::pin(sleep_until(deadline)),
    }
}

/// Logs a failed attempt with its cause; server-side only (D23).
fn log_failure(request_id: u64, channel: &ChannelRuntime, failure: &Failure, last: bool) {
    let class = failure.class;
    let cause = Cause(failure.error.as_deref());
    if last {
        let reply = failure.reply.map_or("-", ErrorCode::as_str);
        tracing::warn!(
            request_id,
            channel = %channel.name,
            ?class,
            reply,
            error = %cause,
            "upstream attempt failed; no attempt left"
        );
    } else {
        tracing::info!(
            request_id,
            channel = %channel.name,
            ?class,
            error = %cause,
            "upstream attempt failed; failing over"
        );
    }
}

/// An optional error with its sources, `outer: inner: root`, or `-`.
struct Cause<'a>(Option<&'a (dyn StdError + Send + Sync + 'static)>);

impl fmt::Display for Cause<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            Some(error) => Chain(error).fmt(f),
            None => f.write_str("-"),
        }
    }
}

/// An error with its sources, `outer: inner: root`, written without
/// allocating.
struct Chain<'a>(&'a (dyn StdError + Send + Sync + 'static));

impl fmt::Display for Chain<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)?;
        let mut source = self.0.source();
        while let Some(error) = source {
            write!(f, ": {error}")?;
            source = error.source();
        }
        Ok(())
    }
}

/// Classification of an upstream status line (section 2.3 of the M1
/// contract).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StatusClass {
    /// 2xx: a candidate for commit.
    Success,
    /// Try the next channel; on the last attempt the class decides the reply.
    Failover(FailureClass),
    /// Forwarded as-is without failover: the client's own request is at fault.
    PassThrough,
}

/// Classification of an upstream status line (pure, table-tested).
///
/// 401 and 403 are channel errors (D2): they mean the gateway's upstream
/// credential is wrong, which the client cannot fix. A 1xx status cannot end
/// a response and a status of 600 or above is outside RFC 9110; both mean
/// the response head could not be read as a valid answer and are classed
/// with transport failures.
pub(crate) fn classify_status(status: StatusCode) -> StatusClass {
    match status.as_u16() {
        200..=299 => StatusClass::Success,
        300..=399 => StatusClass::Failover(FailureClass::Redirect),
        401 | 403 => StatusClass::Failover(FailureClass::UpstreamAuth),
        408 | 429 | 500..=599 => StatusClass::Failover(FailureClass::Status),
        400..=499 => StatusClass::PassThrough,
        _ => StatusClass::Failover(FailureClass::Transport),
    }
}

/// `content-type` media type is `text/event-stream`: the part before `;`,
/// trimmed, compared ASCII case-insensitively; parameters are ignored and a
/// missing header means not SSE.
pub(crate) fn is_event_stream(headers: &HeaderMap) -> bool {
    let Some(value) = headers.get(CONTENT_TYPE) else {
        return false;
    };
    let bytes = value.as_bytes();
    let media_type = bytes
        .iter()
        .position(|&byte| byte == b';')
        .map_or(bytes, |end| &bytes[..end]);
    media_type
        .trim_ascii()
        .eq_ignore_ascii_case(b"text/event-stream")
}

#[cfg(test)]
mod tests {
    use http::HeaderValue;

    use super::*;

    fn class(code: u16) -> StatusClass {
        classify_status(StatusCode::from_u16(code).expect("valid status code"))
    }

    #[test]
    fn success_statuses_are_commit_candidates() {
        for code in [200, 201, 202, 204, 206, 299] {
            assert_eq!(class(code), StatusClass::Success, "{code}");
        }
    }

    #[test]
    fn redirects_fail_over() {
        for code in [300, 301, 302, 303, 304, 307, 308, 399] {
            assert_eq!(
                class(code),
                StatusClass::Failover(FailureClass::Redirect),
                "{code}"
            );
        }
    }

    #[test]
    fn upstream_auth_failures_fail_over() {
        for code in [401, 403] {
            assert_eq!(
                class(code),
                StatusClass::Failover(FailureClass::UpstreamAuth),
                "{code}"
            );
        }
    }

    #[test]
    fn request_timeout_fails_over_as_a_status_failure() {
        assert_eq!(class(408), StatusClass::Failover(FailureClass::Status));
    }

    #[test]
    fn rate_limits_and_server_errors_fail_over() {
        for code in [429, 500, 501, 502, 503, 504, 529, 599] {
            assert_eq!(
                class(code),
                StatusClass::Failover(FailureClass::Status),
                "{code}"
            );
        }
    }

    #[test]
    fn other_client_errors_pass_through() {
        for code in [400, 402, 404, 405, 409, 410, 413, 415, 422, 499] {
            assert_eq!(class(code), StatusClass::PassThrough, "{code}");
        }
    }

    #[test]
    fn informational_and_out_of_range_statuses_are_transport_failures() {
        for code in [100, 101, 103, 600, 999] {
            assert_eq!(
                class(code),
                StatusClass::Failover(FailureClass::Transport),
                "{code}"
            );
        }
    }

    fn with_content_type(value: &'static str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static(value));
        headers
    }

    #[test]
    fn event_stream_media_types_are_recognised() {
        for value in [
            "text/event-stream",
            "text/event-stream; charset=utf-8",
            "text/event-stream;charset=utf-8",
            "Text/Event-Stream",
            "TEXT/EVENT-STREAM ; charset=UTF-8",
            " text/event-stream ",
            "text/event-stream;",
        ] {
            assert!(is_event_stream(&with_content_type(value)), "{value:?}");
        }
    }

    #[test]
    fn other_media_types_are_not_event_streams() {
        for value in [
            "application/json",
            "application/json; charset=utf-8",
            "text/event-streams",
            "text/event",
            "text/plain; format=text/event-stream",
            "",
        ] {
            assert!(!is_event_stream(&with_content_type(value)), "{value:?}");
        }
    }

    #[test]
    fn missing_content_type_is_not_an_event_stream() {
        assert!(!is_event_stream(&HeaderMap::new()));
    }

    #[test]
    fn only_the_first_content_type_counts() {
        let mut headers = with_content_type("application/json");
        headers.append(CONTENT_TYPE, HeaderValue::from_static("text/event-stream"));
        assert!(!is_event_stream(&headers));
    }

    #[test]
    fn non_ascii_header_bytes_do_not_match() {
        let mut headers = HeaderMap::new();
        headers.insert(
            CONTENT_TYPE,
            HeaderValue::from_bytes(b"text/event-stream\xff").expect("obs-text is a valid value"),
        );
        assert!(!is_event_stream(&headers));
    }

    #[test]
    fn last_attempt_replies_follow_section_2_3() {
        let reply = |code: u16| status_reply(StatusCode::from_u16(code).expect("valid status"));
        for code in [300, 301, 307, 308] {
            assert_eq!(reply(code), Some(ErrorCode::UpstreamRedirect), "{code}");
        }
        for code in [401, 403] {
            assert_eq!(reply(code), Some(ErrorCode::UpstreamAuthFailed), "{code}");
        }
        assert_eq!(reply(408), Some(ErrorCode::UpstreamTimeout), "D3");
        for code in [429, 500, 502, 503, 599] {
            assert_eq!(reply(code), None, "{code} is forwarded, not answered");
        }
        for code in [101, 600] {
            assert_eq!(reply(code), Some(ErrorCode::UpstreamUnreachable), "{code}");
        }
    }

    const CONTENT: &[u8] = br#"{"id":"c","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"content":"pong"},"finish_reason":null}]}"#;

    #[test]
    fn content_and_done_commit() {
        assert_eq!(judge_payload(CONTENT), Verdict::Commit);
        assert_eq!(judge_payload(b"[DONE]"), Verdict::Commit);
        assert_eq!(
            judge_payload(br#"{"choices":[],"error":null}"#),
            Verdict::Commit
        );
    }

    #[test]
    fn a_top_level_error_is_a_first_event_error() {
        for payload in [
            &br#"{"error":{"message":"quota","type":"server_error"}}"#[..],
            br#"{"error":"Invalid API key"}"#,
            br#"{"error" : {"message":"x"}}"#,
        ] {
            assert_eq!(
                judge_payload(payload),
                Verdict::FirstEventError,
                "{}",
                String::from_utf8_lossy(payload)
            );
        }
    }

    #[test]
    fn an_unparsable_candidate_commits() {
        // 2.4, step 3: the body records the parse error after commit.
        assert_eq!(judge_payload(br#"{"error":{"message":"#), Verdict::Commit);
        assert_eq!(
            judge_payload(br#"{"choices":[{"delta":{"content":"\"error\": {"}}]}"#),
            Verdict::Commit
        );
    }

    fn event(payload: &[u8]) -> Vec<u8> {
        [b"data: ", payload, b"\n\n"].concat()
    }

    #[test]
    fn comments_and_events_without_data_are_skipped() {
        let mut first = FirstEvent::default();
        assert_eq!(first.feed(b": keep-alive\n\n"), Ok(None));
        assert_eq!(first.feed(b"event: ping\n\n"), Ok(None));
        assert_eq!(first.feed(&event(CONTENT)), Ok(Some(Verdict::Commit)));
    }

    #[test]
    fn a_first_event_split_across_frames_is_judged_when_complete() {
        let error = event(br#"{"error":"Invalid API key"}"#);
        let (head, tail) = error.split_at(error.len() - 1);
        let mut first = FirstEvent::default();
        assert_eq!(first.feed(head), Ok(None));
        assert_eq!(first.feed(tail), Ok(Some(Verdict::FirstEventError)));
    }

    #[test]
    fn only_the_first_data_event_counts() {
        let stream = [event(CONTENT), event(br#"{"error":"late"}"#)].concat();
        let mut first = FirstEvent::default();
        assert_eq!(first.feed(&stream), Ok(Some(Verdict::Commit)));
    }

    #[test]
    fn several_data_lines_are_joined_before_judging() {
        let mut first = FirstEvent::default();
        assert_eq!(
            first.feed(b"data: {\"error\":\ndata: \"Invalid API key\"}\n\n"),
            Ok(Some(Verdict::FirstEventError))
        );
    }

    #[test]
    fn an_oversized_event_before_any_data_event_is_a_protocol_error() {
        let mut huge = b"data: \"".to_vec();
        huge.resize(huge.len() + brisk_proto::sse::MAX_EVENT_BYTES + 1, b'a');
        let mut first = FirstEvent::default();
        assert!(matches!(
            first.feed(&huge),
            Err(SseError::EventTooLarge { .. })
        ));
        // After a complete data event the oversized one is the committed
        // stream's to report.
        let mut after = event(CONTENT);
        after.extend_from_slice(&huge);
        let mut first = FirstEvent::default();
        assert_eq!(first.feed(&after), Ok(Some(Verdict::Commit)));
    }

    #[test]
    fn deadlines_saturate_at_the_far_future() {
        let start = Instant::now();
        assert_eq!(
            deadline_after(start, Duration::from_secs(600)),
            start + Duration::from_secs(600)
        );
        assert_eq!(deadline_after(start, Duration::MAX), start + FAR_FUTURE);
    }

    #[test]
    fn a_cause_lists_its_sources() {
        #[derive(Debug)]
        struct Outer(std::io::Error);
        impl fmt::Display for Outer {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("error sending request")
            }
        }
        impl StdError for Outer {
            fn source(&self) -> Option<&(dyn StdError + 'static)> {
                Some(&self.0)
            }
        }
        let error: BoxError = Box::new(Outer(std::io::Error::other("connection refused")));
        assert_eq!(
            Cause(Some(error.as_ref())).to_string(),
            "error sending request: connection refused"
        );
        assert_eq!(Cause(None).to_string(), "-");
    }

    mod pending {
        use std::time::Duration;

        use brisk_proto::UsageTokens;

        use super::super::*;
        use crate::auth::key_digest;
        use crate::outcome::{OutcomeReceiver, outcome_channel};
        use crate::secret::Redacted;
        use crate::server::ServerConfig;
        use crate::spec::{
            ChannelSpec, ExperimentSpec, GatewaySpec, KeySpec, LimitsSpec, Timeouts, WarmupSpec,
            WarmupTarget,
        };
        use crate::upstream::UpstreamClientConfig;

        fn gateway() -> (Gateway, OutcomeReceiver) {
            let (sink, outcomes) = outcome_channel(8);
            let spec = GatewaySpec {
                server: ServerConfig::default(),
                limits: LimitsSpec::default(),
                forwarding: ForwardingSpec::default(),
                warmup: WarmupSpec::default(),
                experiments: ExperimentSpec::default(),
                keys: vec![KeySpec {
                    name: String::from("k"),
                    sha256: key_digest(b"bk-unit"),
                }],
                channels: vec![ChannelSpec {
                    name: String::from("a"),
                    base_url: String::from("http://127.0.0.1:9/v1"),
                    api_key: Redacted::new(String::from("upstream-key")),
                    weight: 1,
                    models: Vec::new(),
                    model_map: Vec::new(),
                    stream_usage: StreamUsage::Inject,
                    timeouts: Timeouts::default(),
                    client: UpstreamClientConfig {
                        allow_private: true,
                        ..UpstreamClientConfig::default()
                    },
                    warmup: WarmupTarget::default(),
                    expose_ratelimit_headers: false,
                }],
            };
            let gateway = Gateway::new(spec, sink).expect("a valid spec");
            (gateway, outcomes)
        }

        fn only(outcomes: &mut OutcomeReceiver) -> Outcome {
            let outcome = outcomes.try_recv().expect("one outcome");
            assert!(outcomes.try_recv().is_err(), "exactly one outcome");
            outcome
        }

        #[test]
        fn an_early_answer_emits_one_outcome() {
            let (gateway, mut outcomes) = gateway();
            let mut pending = Pending::new(&gateway);
            pending.key = Some(KeyId(0));
            pending.request_bytes = 100;
            let response = pending.reject(RejectReason::ModelNotFound, ErrorCode::ModelNotFound);
            drop(pending);
            assert_eq!(response.status(), StatusCode::NOT_FOUND);
            let outcome = only(&mut outcomes);
            assert_eq!(
                outcome.status,
                OutcomeStatus::Rejected(RejectReason::ModelNotFound)
            );
            assert_eq!(outcome.http_status, Some(404));
            assert_eq!(outcome.key, Some(KeyId(0)));
            assert_eq!(outcome.billed, UsageTokens::default());
            assert_eq!(outcome.estimate.input, 25);
            assert_eq!(outcome.to_commit, None);
        }

        #[test]
        fn a_final_failure_answers_with_its_code() {
            let (gateway, mut outcomes) = gateway();
            let mut pending = Pending::new(&gateway);
            pending.attempts = 2;
            pending.channel = Some(ChannelId(0));
            let response = pending.fail(FailureClass::Connect, ErrorCode::UpstreamUnreachable);
            drop(pending);
            assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
            let outcome = only(&mut outcomes);
            assert_eq!(outcome.status, OutcomeStatus::Failed(FailureClass::Connect));
            assert_eq!(outcome.attempts, 2);
            assert_eq!(outcome.channel, Some(ChannelId(0)));
            assert_eq!(outcome.billed, UsageTokens::default());
        }

        #[test]
        fn dropping_a_pending_request_bills_the_prompt_only() {
            let (gateway, mut outcomes) = gateway();
            let mut pending = Pending::new(&gateway);
            pending.key = Some(KeyId(0));
            pending.request_bytes = 852;
            pending.attempts = 1;
            pending.stream = true;
            drop(pending);
            let outcome = only(&mut outcomes);
            assert_eq!(outcome.status, OutcomeStatus::ClientCancelled);
            assert_eq!(outcome.http_status, None);
            assert_eq!(
                outcome.billed,
                UsageTokens {
                    input: 213,
                    ..UsageTokens::default()
                }
            );
        }

        #[test]
        fn commit_hands_the_outcome_to_the_body() {
            let (gateway, mut outcomes) = gateway();
            let mut pending = Pending::new(&gateway);
            pending.request_bytes = 852;
            pending.attempts = 2;
            pending.stream = true;
            let settle = pending.commit(KeyId(0), ChannelId(0), StatusCode::OK);
            drop(pending);
            assert!(outcomes.try_recv().is_err(), "the body emits, not forward");
            assert_eq!(settle.key, KeyId(0));
            assert_eq!(settle.channel, ChannelId(0));
            assert_eq!(settle.attempts, 2);
            assert!(settle.stream);
            assert_eq!(settle.http_status, 200);
            assert_eq!(settle.request_bytes, 852);
            assert!(settle.committed >= settle.started);
        }

        #[test]
        fn body_errors_map_to_their_replies() {
            let (gateway, mut outcomes) = gateway();
            let cases = [
                (
                    IngressError::TooLarge { limit: 1 },
                    413,
                    RejectReason::BodyTooLarge,
                ),
                (IngressError::Overloaded, 503, RejectReason::Overloaded),
                (IngressError::Timeout, 408, RejectReason::BodyTimeout),
                (IngressError::TooSlow, 408, RejectReason::BodyTooSlow),
            ];
            for (error, status, reason) in cases {
                let response = Pending::new(&gateway).reject_body(&error);
                assert_eq!(response.status().as_u16(), status, "{error}");
                let outcome = only(&mut outcomes);
                assert_eq!(outcome.status, OutcomeStatus::Rejected(reason));
                assert_eq!(outcome.http_status, Some(status));
            }

            let aborted = IngressError::Aborted(Box::new(std::io::Error::other("reset")));
            let _ = Pending::new(&gateway).reject_body(&aborted);
            let outcome = only(&mut outcomes);
            assert_eq!(
                outcome.status,
                OutcomeStatus::Rejected(RejectReason::ClientAborted)
            );
            assert_eq!(outcome.http_status, None, "no response reaches the client");
        }

        #[test]
        fn head_errors_are_client_errors_except_the_internal_one() {
            let (gateway, mut outcomes) = gateway();
            let response = Pending::new(&gateway).reject_head(&HeadError::MissingModel);
            assert_eq!(response.status(), StatusCode::BAD_REQUEST);
            assert_eq!(
                only(&mut outcomes).status,
                OutcomeStatus::Rejected(RejectReason::BadRequest)
            );

            let response = Pending::new(&gateway).reject_head(&HeadError::SpanOutsideBody);
            assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
            assert_eq!(
                only(&mut outcomes).status,
                OutcomeStatus::Rejected(RejectReason::Internal)
            );
        }

        #[tokio::test(start_paused = true)]
        async fn elapsed_time_is_measured_from_arrival() {
            let (gateway, mut outcomes) = gateway();
            let pending = Pending::new(&gateway);
            tokio::time::advance(Duration::from_millis(20)).await;
            drop(pending);
            assert_eq!(only(&mut outcomes).elapsed, Duration::from_millis(20));
        }
    }
}
