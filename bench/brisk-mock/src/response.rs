//! Response byte builders: SSE stream events, the non-streaming completion
//! and the small JSON endpoints.
//!
//! Every builder appends to a caller-owned buffer so a shard reuses one
//! scratch allocation for all its writes.

use std::io::Write as _;

use brisk_bench_core::http1::{self, BodyFraming};
use brisk_bench_core::wire::{self, MARKER_LEN, Marker};

/// Suffix of a content event after the content string.
const CONTENT_EVENT_SUFFIX: &[u8] = b"\"},\"finish_reason\":null}]}\n\n";
/// The final SSE event of a stream.
const DONE_EVENT: &[u8] = b"data: [DONE]\n\n";

/// Head of every streaming response.
pub(crate) fn append_stream_head(out: &mut Vec<u8>, keep_alive: bool) {
    let headers: &[(&str, &str)] = &[
        ("content-type", "text/event-stream"),
        ("cache-control", "no-cache"),
        ("connection", "close"),
    ];
    let used = if keep_alive { 2 } else { 3 };
    http1::write_response_head(out, 200, "OK", &headers[..used], BodyFraming::Chunked);
}

/// Per-stream constants of the SSE events.
#[derive(Debug, Clone)]
pub(crate) struct StreamTemplate {
    /// `{"id":...,"object":"chat.completion.chunk","created":...,"model":...,`
    /// shared by all events of the stream.
    common: Vec<u8>,
}

impl StreamTemplate {
    /// Builds the template for stream `sid`, created at `created_unix_s`.
    pub(crate) fn new(sid: u64, created_unix_s: u64, model: &str) -> Self {
        let mut common = Vec::with_capacity(128);
        write!(
            common,
            "{{\"id\":\"chatcmpl-mock-{sid}\",\"object\":\"chat.completion.chunk\",\"created\":{created_unix_s},\"model\":"
        )
        .expect("writing to a Vec cannot fail");
        serde_json::to_writer(&mut common, model).expect("serializing a str cannot fail");
        common.push(b',');
        Self { common }
    }

    fn content_prefix_len(&self, first: bool) -> usize {
        b"data: ".len() + self.common.len() + content_delta_open(first).len()
    }

    /// Appends one content event as a single chunked frame and returns the
    /// offset of its marker in `out`, for [`wire::patch_t_write`].
    ///
    /// The marker's `t_write` is left as given; the caller patches it right
    /// before the write.
    pub(crate) fn append_content_event(
        &self,
        out: &mut Vec<u8>,
        marker: &Marker,
        chunk_bytes: u32,
    ) -> usize {
        let first = marker.seq == 0;
        let content_len = usize::try_from(chunk_bytes).expect("u32 fits usize");
        let data_len = self.content_prefix_len(first) + content_len + CONTENT_EVENT_SUFFIX.len();
        write!(out, "{data_len:x}\r\n").expect("writing to a Vec cannot fail");
        out.extend_from_slice(b"data: ");
        out.extend_from_slice(&self.common);
        out.extend_from_slice(content_delta_open(first));
        let marker_at = wire::append_content(out, marker, chunk_bytes);
        out.extend_from_slice(CONTENT_EVENT_SUFFIX);
        out.extend_from_slice(b"\r\n");
        marker_at
    }

    /// Appends the end of a stream: the `finish_reason: "stop"` event, the
    /// usage event if requested, `data: [DONE]` and the last chunk.
    pub(crate) fn append_tail(&self, out: &mut Vec<u8>, usage: Option<Usage>) {
        let mut event = Vec::with_capacity(256);
        event.extend_from_slice(b"data: ");
        event.extend_from_slice(&self.common);
        event.extend_from_slice(
            b"\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
        );
        http1::write_chunk(out, &event);
        if let Some(usage) = usage {
            event.clear();
            event.extend_from_slice(b"data: ");
            event.extend_from_slice(&self.common);
            event.extend_from_slice(b"\"choices\":[],\"usage\":");
            usage.append_json(&mut event);
            event.extend_from_slice(b"}\n\n");
            http1::write_chunk(out, &event);
        }
        http1::write_chunk(out, DONE_EVENT);
        out.extend_from_slice(http1::LAST_CHUNK);
    }
}

fn content_delta_open(first: bool) -> &'static [u8] {
    if first {
        b"\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\""
    } else {
        b"\"choices\":[{\"index\":0,\"delta\":{\"content\":\""
    }
}

/// Synthetic token accounting.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Usage {
    /// Prompt tokens.
    pub(crate) prompt_tokens: u64,
    /// Completion tokens.
    pub(crate) completion_tokens: u64,
}

impl Usage {
    /// Estimates usage the way a rough tokenizer would: four body bytes per
    /// prompt token.
    pub(crate) fn estimate(body_len: usize, completion_tokens: u64) -> Self {
        Self {
            prompt_tokens: u64::try_from(body_len.div_ceil(4)).expect("usize fits u64"),
            completion_tokens,
        }
    }

    fn append_json(self, out: &mut Vec<u8>) {
        write!(
            out,
            "{{\"prompt_tokens\":{},\"completion_tokens\":{},\"total_tokens\":{}}}",
            self.prompt_tokens,
            self.completion_tokens,
            self.prompt_tokens + self.completion_tokens
        )
        .expect("writing to a Vec cannot fail");
    }
}

/// Appends a complete non-streaming `chat.completion` response whose content
/// is `resp_bytes` long. Returns the offset of the embedded marker in `out`
/// when `resp_bytes` leaves room for one.
pub(crate) fn append_completion(
    out: &mut Vec<u8>,
    marker: &Marker,
    resp_bytes: u32,
    created_unix_s: u64,
    model: &str,
    usage: Usage,
    keep_alive: bool,
) -> Option<usize> {
    let mut body = Vec::with_capacity(usize::try_from(resp_bytes).expect("u32 fits usize") + 384);
    write!(
        body,
        "{{\"id\":\"chatcmpl-mock-{}\",\"object\":\"chat.completion\",\"created\":{created_unix_s},\"model\":",
        marker.sid
    )
    .expect("writing to a Vec cannot fail");
    serde_json::to_writer(&mut body, model).expect("serializing a str cannot fail");
    body.extend_from_slice(
        b",\"choices\":[{\"index\":0,\"message\":{\"role\":\"assistant\",\"content\":\"",
    );
    let marker_in_body = if resp_bytes >= wire::MARKER_LEN_U32 {
        Some(wire::append_content(&mut body, marker, resp_bytes))
    } else {
        let start = body.len();
        body.resize(
            start + usize::try_from(resp_bytes).expect("u32 fits usize"),
            b'x',
        );
        None
    };
    body.extend_from_slice(b"\"},\"finish_reason\":\"stop\"}],\"usage\":");
    usage.append_json(&mut body);
    body.push(b'}');

    append_json_head(out, 200, "OK", body.len(), keep_alive);
    let body_at = out.len();
    out.extend_from_slice(&body);
    debug_assert!(marker_in_body.is_none_or(|at| at + MARKER_LEN <= body.len()));
    marker_in_body.map(|at| body_at + at)
}

fn append_json_head(out: &mut Vec<u8>, status: u16, reason: &str, len: usize, keep_alive: bool) {
    let mut headers: [(&str, &str); 2] = [("content-type", "application/json"), ("", "")];
    let used = if keep_alive {
        1
    } else {
        headers[1] = ("connection", "close");
        2
    };
    http1::write_response_head(
        out,
        status,
        reason,
        &headers[..used],
        BodyFraming::Length(u64::try_from(len).expect("usize fits u64")),
    );
}

/// Appends a complete JSON response.
pub(crate) fn append_json(
    out: &mut Vec<u8>,
    status: u16,
    reason: &str,
    body: &[u8],
    keep_alive: bool,
) {
    append_json_head(out, status, reason, body.len(), keep_alive);
    out.extend_from_slice(body);
}

/// Appends a complete `OpenAI`-style JSON error response.
pub(crate) fn append_error(
    out: &mut Vec<u8>,
    status: u16,
    reason: &str,
    message: &str,
    keep_alive: bool,
) {
    let body = serde_json::json!({
        "error": {
            "message": message,
            "type": "invalid_request_error",
            "code": serde_json::Value::Null,
        }
    });
    let body = serde_json::to_vec(&body).expect("serializing a JSON value cannot fail");
    append_json(out, status, reason, &body, keep_alive);
}

/// Appends an empty `204 No Content` response.
pub(crate) fn append_no_content(out: &mut Vec<u8>, keep_alive: bool) {
    let headers: &[(&str, &str)] = if keep_alive {
        &[]
    } else {
        &[("connection", "close")]
    };
    // 204 carries no body, so no framing header is written.
    http1::write_response_head(out, 204, "No Content", headers, BodyFraming::Unframed);
}

/// The interim response to `Expect: 100-continue`.
pub(crate) const CONTINUE: &[u8] = b"HTTP/1.1 100 Continue\r\n\r\n";

/// Body of `GET /v1/models`.
pub(crate) fn models_body(model: &str) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "object": "list",
        "data": [{
            "id": model,
            "object": "model",
            "created": 0,
            "owned_by": "brisk-mock",
        }]
    }))
    .expect("serializing a JSON value cannot fail")
}

#[cfg(test)]
mod tests {
    use brisk_bench_core::http1::ChunkedDecoder;

    use super::*;

    fn marker(seq: u32) -> Marker {
        Marker {
            sid: 7,
            seq,
            t_sched: 1_000,
            t_write: 0,
        }
    }

    /// Decodes a chunked body and splits it into SSE `data:` payloads.
    fn sse_payloads(chunked: &[u8]) -> Vec<String> {
        let mut payload = Vec::new();
        let mut decoder = ChunkedDecoder::new();
        let result = decoder
            .decode(chunked, |d| payload.extend_from_slice(d))
            .unwrap();
        assert!(result.done);
        assert_eq!(result.consumed, chunked.len());
        String::from_utf8(payload)
            .unwrap()
            .split("\n\n")
            .filter(|e| !e.is_empty())
            .map(|e| e.strip_prefix("data: ").unwrap().to_owned())
            .collect()
    }

    #[test]
    fn content_event_is_one_frame_with_patchable_marker() {
        let template = StreamTemplate::new(7, 1_700_000_000, "m\"odel");
        let mut out = Vec::new();
        let at = template.append_content_event(&mut out, &marker(0), 120);
        wire::patch_t_write(&mut out[at..], 2_000);
        let second_at = template.append_content_event(&mut out, &marker(1), 78);
        wire::patch_t_write(&mut out[second_at..], 3_000);
        template.append_tail(
            &mut out,
            Some(Usage {
                prompt_tokens: 3,
                completion_tokens: 2,
            }),
        );

        let events = sse_payloads(&out);
        assert_eq!(events.len(), 5);
        let first: serde_json::Value = serde_json::from_str(&events[0]).unwrap();
        assert_eq!(first["object"], "chat.completion.chunk");
        assert_eq!(first["model"], "m\"odel");
        assert_eq!(first["choices"][0]["delta"]["role"], "assistant");
        let content = first["choices"][0]["delta"]["content"].as_str().unwrap();
        assert_eq!(content.len(), 120);
        let parsed = Marker::parse(&content.as_bytes()[..MARKER_LEN]).unwrap();
        assert_eq!(
            parsed,
            Marker {
                t_write: 2_000,
                ..marker(0)
            }
        );

        let second: serde_json::Value = serde_json::from_str(&events[1]).unwrap();
        assert!(second["choices"][0]["delta"].get("role").is_none());
        let content = second["choices"][0]["delta"]["content"].as_str().unwrap();
        assert_eq!(Marker::parse(content.as_bytes()).unwrap().t_write, 3_000);

        let finish: serde_json::Value = serde_json::from_str(&events[2]).unwrap();
        assert_eq!(finish["choices"][0]["finish_reason"], "stop");
        let usage: serde_json::Value = serde_json::from_str(&events[3]).unwrap();
        assert_eq!(usage["usage"]["total_tokens"], 5);
        assert_eq!(usage["choices"].as_array().unwrap().len(), 0);
        assert_eq!(events[4], "[DONE]");
    }

    #[test]
    fn tail_without_usage() {
        let template = StreamTemplate::new(1, 0, "m");
        let mut out = Vec::new();
        template.append_tail(&mut out, None);
        let events = sse_payloads(&out);
        assert_eq!(events.len(), 2);
        assert_eq!(events[1], "[DONE]");
    }

    fn split_response(out: &[u8]) -> (u16, Vec<(String, String)>, &[u8]) {
        let mut headers = [httparse::EMPTY_HEADER; 16];
        let mut resp = httparse::Response::new(&mut headers);
        let httparse::Status::Complete(len) = resp.parse(out).unwrap() else {
            panic!("incomplete head");
        };
        let headers = resp
            .headers
            .iter()
            .map(|h| {
                (
                    h.name.to_ascii_lowercase(),
                    String::from_utf8(h.value.to_vec()).unwrap(),
                )
            })
            .collect();
        (resp.code.unwrap(), headers, &out[len..])
    }

    #[test]
    fn completion_has_exact_content_length_and_marker() {
        let mut out = b"previous".to_vec();
        let usage = Usage::estimate(10, 1);
        let at = append_completion(&mut out, &marker(0), 300, 5, "m", usage, false).unwrap();
        wire::patch_t_write(&mut out[at..], 9_999);

        let (status, headers, body) = split_response(&out[b"previous".len()..]);
        assert_eq!(status, 200);
        assert!(headers.contains(&("connection".into(), "close".into())));
        assert!(headers.contains(&("content-length".into(), body.len().to_string())));
        let json: serde_json::Value = serde_json::from_slice(body).unwrap();
        assert_eq!(json["object"], "chat.completion");
        let content = json["choices"][0]["message"]["content"].as_str().unwrap();
        assert_eq!(content.len(), 300);
        assert_eq!(
            Marker::parse(&content.as_bytes()[..MARKER_LEN])
                .unwrap()
                .t_write,
            9_999
        );
        assert_eq!(json["usage"]["prompt_tokens"], 3);
    }

    #[test]
    fn short_completion_has_no_marker() {
        let mut out = Vec::new();
        let at = append_completion(
            &mut out,
            &marker(0),
            10,
            5,
            "m",
            Usage::estimate(0, 1),
            true,
        );
        assert!(at.is_none());
        let (_, headers, body) = split_response(&out);
        assert!(!headers.iter().any(|(name, _)| name == "connection"));
        let json: serde_json::Value = serde_json::from_slice(body).unwrap();
        assert_eq!(json["choices"][0]["message"]["content"], "xxxxxxxxxx");
    }

    #[test]
    fn error_and_models_are_valid_json() {
        let mut out = Vec::new();
        append_error(&mut out, 404, "Not Found", "no such route", true);
        let (status, _, body) = split_response(&out);
        assert_eq!(status, 404);
        let json: serde_json::Value = serde_json::from_slice(body).unwrap();
        assert_eq!(json["error"]["message"], "no such route");

        let models: serde_json::Value = serde_json::from_slice(&models_body("m")).unwrap();
        assert_eq!(models["data"][0]["id"], "m");
    }
}
