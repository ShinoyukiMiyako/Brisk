//! Pre-built HTTP/1.1 requests whose per-request fields are patched in place.
//!
//! Every request of a run has the same head and body except for the stream
//! id and the chunk count in the bench directive. The template writes both
//! as fixed-width, zero-padded decimals (the directive parser accepts leading
//! zeros), so the body length never changes and producing the next request
//! costs two small digit writes instead of a body rebuild. That keeps the
//! work between the scheduled time and the `write` syscall constant, and it
//! makes multi-megabyte bodies free to reuse.

use brisk_bench_core::http1::{BodyFraming, write_request_head};
use brisk_bench_core::wire::{BenchParams, BodyShape, DIRECTIVE_PREFIX, chat_request_body};
use memchr::memmem;

use crate::cli::Header;

/// Width of the zero-padded `sid` field.
const SID_DIGITS: usize = 20;
/// Width of the zero-padded `chunks` field.
const CHUNKS_DIGITS: usize = 10;
/// 20-digit `sid` written at build time and located afterwards.
const SID_PLACEHOLDER: u64 = 10_000_000_000_000_000_000;
/// 10-digit `chunks` written at build time and located afterwards.
const CHUNKS_PLACEHOLDER: u32 = 4_000_000_000;

/// Bodies at least this large carry an image part.
pub(crate) const IMAGE_THRESHOLD: usize = 10 * 1024 * 1024;
/// Share of an image-carrying body that is base64 image data, in tenths.
const IMAGE_TENTHS: usize = 9;

/// Errors while building a template.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub(crate) enum RequestError {
    /// The requested exact body size is below the fixed JSON overhead.
    #[error("body size {requested} is below the minimum of {minimum} bytes")]
    BodyTooSmall {
        /// Requested size.
        requested: usize,
        /// Smallest size the chosen shape can have.
        minimum: usize,
    },
}

/// How large the request body is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BodySpec {
    /// A plain-text user message of this many bytes.
    Prompt(usize),
    /// A body of exactly this many bytes; from [`IMAGE_THRESHOLD`] on,
    /// nine tenths of it is a base64 image.
    Exact(usize),
}

/// Everything that is constant across the requests of a run.
#[derive(Debug, Clone)]
pub(crate) struct RequestSpec<'a> {
    /// `Host` header value.
    pub(crate) authority: &'a str,
    /// Request target.
    pub(crate) path: &'a str,
    /// Extra headers.
    pub(crate) headers: &'a [Header],
    /// `model` field.
    pub(crate) model: &'a str,
    /// Streaming (SSE) response requested.
    pub(crate) stream: bool,
    /// `stream_options.include_usage`.
    pub(crate) include_usage: bool,
    /// Directive `ttft_us`.
    pub(crate) ttft_us: u64,
    /// Directive `interval_us`.
    pub(crate) interval_us: u64,
    /// Directive `chunk_bytes`.
    pub(crate) chunk_bytes: u32,
    /// Directive `resp_bytes`.
    pub(crate) resp_bytes: u32,
    /// Body size.
    pub(crate) body: BodySpec,
}

/// A complete request (head and body) with patchable `sid` and `chunks`.
#[derive(Debug, Clone)]
pub(crate) struct RequestTemplate {
    bytes: Vec<u8>,
    head_len: usize,
    sid_at: usize,
    chunks_at: usize,
}

impl RequestTemplate {
    /// Builds the template.
    pub(crate) fn build(spec: &RequestSpec<'_>) -> Result<Self, RequestError> {
        let params = BenchParams {
            ttft_us: spec.ttft_us,
            interval_us: spec.interval_us,
            chunks: CHUNKS_PLACEHOLDER,
            chunk_bytes: spec.chunk_bytes,
            sid: SID_PLACEHOLDER,
            resp_bytes: spec.resp_bytes,
        };
        let body_of =
            |shape| chat_request_body(spec.model, spec.stream, spec.include_usage, &params, shape);
        let body = match spec.body {
            BodySpec::Prompt(bytes) => body_of(BodyShape::Text { bytes }),
            BodySpec::Exact(total) => body_of(fit_shape(total, |shape| body_of(shape).len())?),
        };

        let mut bytes = Vec::with_capacity(body.len() + 512);
        let accept = if spec.stream {
            "text/event-stream"
        } else {
            "application/json"
        };
        let mut headers: Vec<(&str, &str)> =
            vec![("content-type", "application/json"), ("accept", accept)];
        headers.extend(
            spec.headers
                .iter()
                .map(|h| (h.name.as_str(), h.value.as_str())),
        );
        write_request_head(
            &mut bytes,
            "POST",
            spec.path,
            spec.authority,
            &headers,
            BodyFraming::Length(body.len() as u64),
        );
        let head_len = bytes.len();
        let directive = memmem::find(&body, DIRECTIVE_PREFIX)
            .expect("chat_request_body always embeds the directive");
        let field = |key: &[u8], digits: usize, placeholder: &str| {
            let at = directive
                + memmem::find(&body[directive..], key).expect("directive carries every key")
                + key.len();
            assert_eq!(
                &body[at..at + digits],
                placeholder.as_bytes(),
                "placeholder has the field width"
            );
            head_len + at
        };
        let sid_at = field(b";sid=", SID_DIGITS, &SID_PLACEHOLDER.to_string());
        let chunks_at = field(b";chunks=", CHUNKS_DIGITS, &CHUNKS_PLACEHOLDER.to_string());
        bytes.extend_from_slice(&body);
        Ok(Self {
            bytes,
            head_len,
            sid_at,
            chunks_at,
        })
    }

    /// Patches `sid` and `chunks` and returns the whole request.
    pub(crate) fn render(&mut self, sid: u64, chunks: u32) -> &[u8] {
        write_padded(&mut self.bytes[self.sid_at..self.sid_at + SID_DIGITS], sid);
        write_padded(
            &mut self.bytes[self.chunks_at..self.chunks_at + CHUNKS_DIGITS],
            u64::from(chunks),
        );
        &self.bytes
    }

    /// Length of the request body.
    pub(crate) fn body_len(&self) -> usize {
        self.bytes.len() - self.head_len
    }

    /// Length of head and body together.
    pub(crate) fn len(&self) -> usize {
        self.bytes.len()
    }
}

/// Picks the body shape that makes the body exactly `total` bytes long.
///
/// `len_of` returns the body length for a shape; the payload lengths enter
/// it additively, so one probe with an empty payload gives the overhead.
fn fit_shape(total: usize, len_of: impl Fn(BodyShape) -> usize) -> Result<BodyShape, RequestError> {
    if total >= IMAGE_THRESHOLD {
        let overhead = len_of(BodyShape::Image {
            base64_bytes: 0,
            text_bytes: 0,
        });
        // Multiple of 4 so the payload is valid padded base64 of that size.
        let base64_bytes = total / 10 * IMAGE_TENTHS / 4 * 4;
        let text_bytes =
            total
                .checked_sub(overhead + base64_bytes)
                .ok_or(RequestError::BodyTooSmall {
                    requested: total,
                    minimum: overhead + base64_bytes,
                })?;
        Ok(BodyShape::Image {
            base64_bytes,
            text_bytes,
        })
    } else {
        let overhead = len_of(BodyShape::Text { bytes: 0 });
        let bytes = total
            .checked_sub(overhead)
            .ok_or(RequestError::BodyTooSmall {
                requested: total,
                minimum: overhead,
            })?;
        Ok(BodyShape::Text { bytes })
    }
}

/// Writes `value` as exactly `out.len()` zero-padded decimal digits.
fn write_padded(out: &mut [u8], mut value: u64) {
    for slot in out.iter_mut().rev() {
        *slot = b'0' + u8::try_from(value % 10).expect("a decimal digit fits u8");
        value /= 10;
    }
    debug_assert_eq!(value, 0, "value wider than its field");
}

#[cfg(test)]
mod tests {
    use brisk_bench_core::http1::request_body_framing;
    use brisk_bench_core::wire::find_directive;

    use super::*;

    fn spec(body: BodySpec, stream: bool) -> RequestSpec<'static> {
        const HEADERS: &[Header] = &[];
        RequestSpec {
            authority: "127.0.0.1:19100",
            path: "/v1/chat/completions",
            headers: HEADERS,
            model: "m",
            stream,
            include_usage: stream,
            ttft_us: 300_000,
            interval_us: 33_333,
            chunk_bytes: 128,
            resp_bytes: 512,
            body,
        }
    }

    /// Splits a rendered request into its parsed framing and body.
    fn split(request: &[u8]) -> (u64, &[u8]) {
        let mut headers = [httparse::EMPTY_HEADER; 16];
        let mut req = httparse::Request::new(&mut headers);
        let httparse::Status::Complete(head_len) = req.parse(request).unwrap() else {
            panic!("incomplete head");
        };
        assert_eq!(req.method, Some("POST"));
        assert_eq!(req.path, Some("/v1/chat/completions"));
        let BodyFraming::Length(len) = request_body_framing(req.headers).unwrap() else {
            panic!("not length framed");
        };
        (len, &request[head_len..])
    }

    #[test]
    fn render_patches_sid_and_chunks() {
        let mut template = RequestTemplate::build(&spec(BodySpec::Prompt(1000), true)).unwrap();
        let len = template.len();
        for (sid, chunks) in [(0, 1), (42, 150), (u64::MAX, u32::MAX), (7, 0)] {
            let request = template.render(sid, chunks).to_vec();
            assert_eq!(request.len(), len);
            let (content_length, body) = split(&request);
            assert_eq!(content_length, body.len() as u64);
            let params = find_directive(body).unwrap().unwrap();
            assert_eq!(
                params,
                BenchParams {
                    ttft_us: 300_000,
                    interval_us: 33_333,
                    chunks,
                    chunk_bytes: 128,
                    sid,
                    resp_bytes: 512,
                }
            );
            let json: serde_json::Value = serde_json::from_slice(body).unwrap();
            assert_eq!(json["stream"], true);
            assert_eq!(json["messages"][1]["content"].as_str().unwrap().len(), 1000);
        }
    }

    #[test]
    fn exact_bodies_hit_the_requested_size() {
        for total in [
            2_000,
            102_400,
            1_048_576,
            IMAGE_THRESHOLD,
            IMAGE_THRESHOLD + 3,
        ] {
            let mut template = RequestTemplate::build(&spec(BodySpec::Exact(total), true)).unwrap();
            assert_eq!(template.body_len(), total);
            let request = template.render(9, 1).to_vec();
            let (_, body) = split(&request);
            assert_eq!(body.len(), total);
            let json: serde_json::Value = serde_json::from_slice(body).unwrap();
            let content = &json["messages"][1]["content"];
            if total >= IMAGE_THRESHOLD {
                let url = content[1]["image_url"]["url"].as_str().unwrap();
                let b64 = url.strip_prefix("data:image/png;base64,").unwrap();
                assert!(b64.len() >= total / 10 * 9 - 3, "{}", b64.len());
            } else {
                assert!(content.is_string());
            }
            assert_eq!(find_directive(body).unwrap().unwrap().sid, 9);
        }
        assert!(matches!(
            RequestTemplate::build(&spec(BodySpec::Exact(10), true)),
            Err(RequestError::BodyTooSmall { requested: 10, .. })
        ));
    }

    #[test]
    fn nonstream_head_and_extra_headers() {
        let headers = [Header {
            name: "authorization".into(),
            value: "Bearer k".into(),
        }];
        let mut s = spec(BodySpec::Prompt(10), false);
        s.headers = &headers;
        let mut template = RequestTemplate::build(&s).unwrap();
        let request = template.render(1, 1).to_vec();
        let text = String::from_utf8(request).unwrap();
        assert!(text.contains("authorization: Bearer k\r\n"));
        assert!(text.contains("accept: application/json\r\n"));
        assert!(text.contains("host: 127.0.0.1:19100\r\n"));
        assert!(text.contains("\"stream\":false"));
    }
}
