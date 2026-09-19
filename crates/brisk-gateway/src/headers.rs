//! Request and response header allowlists: which client headers reach the
//! upstream and which upstream headers reach the client. Credentials, cookies,
//! hop-by-hop and forwarding headers never pass in either direction (R19).
//!
//! Allowlists instead of denylists: a header nobody thought about (a new
//! tracing header carrying a session token, a CPA diagnostic header) is
//! dropped by default. The lists deliberately leave out `authorization`,
//! `x-api-key`, `x-goog-api-key`, `cookie`, `accept-encoding`, `host`,
//! `content-length`, `transfer-encoding`, every hop-by-hop header,
//! `forwarded`, `x-forwarded-*`, `openai-organization` and `openai-project`
//! on the request side, and CPA's `Access-Control-*`, `Connection`,
//! `X-Cpa-Trace-Id` and `Set-Cookie` on the response side (CPA-M1-2,
//! CPA-M1-8).

use http::header::{
    self, CACHE_CONTROL, CONNECTION, CONTENT_TYPE, HeaderMap, HeaderName, HeaderValue,
};

/// Headers copied from the client request, plus the prefixes below. Built
/// with `HeaderName::from_static` (a `const fn`), so matching compares
/// `HeaderName`s without parsing strings per request.
///
/// A `static` array rather than the `const &[HeaderName]` of the contract:
/// `HeaderName` holds `Bytes`, whose atomic pointer makes it interior
/// mutable, and a `const` may not borrow such a value for `'static` (E0492).
/// Matching is the same.
pub(crate) static REQUEST_ALLOW: [HeaderName; 18] = [
    header::ACCEPT,
    header::USER_AGENT,
    HeaderName::from_static("idempotency-key"),
    HeaderName::from_static("x-request-id"),
    HeaderName::from_static("openai-beta"),
    HeaderName::from_static("anthropic-version"),
    HeaderName::from_static("anthropic-beta"),
    HeaderName::from_static("originator"),
    HeaderName::from_static("x-codex-beta-features"),
    HeaderName::from_static("x-openai-subagent"),
    HeaderName::from_static("x-claude-code-session-id"),
    HeaderName::from_static("x-claude-code-agent-id"),
    HeaderName::from_static("x-claude-code-parent-agent-id"),
    HeaderName::from_static("session_id"),
    HeaderName::from_static("session-id"),
    HeaderName::from_static("x-session-id"),
    HeaderName::from_static("x-parent-session-id"),
    HeaderName::from_static("conversation_id"),
];
/// Prefixes of client headers copied as well (the `OpenAI` SDKs' telemetry).
pub(crate) const REQUEST_ALLOW_PREFIXES: &[&str] = &["x-stainless-"];

/// Headers copied from the upstream response.
pub(crate) const RESPONSE_ALLOW: &[&str] = &["content-type", "x-request-id", "retry-after"];
/// Added to `RESPONSE_ALLOW` when the channel sets `expose_ratelimit_headers`.
pub(crate) const RATELIMIT_ALLOW: &[&str] = &["ratelimit", "ratelimit-policy"];
/// Prefixes added with [`RATELIMIT_ALLOW`].
pub(crate) const RATELIMIT_ALLOW_PREFIXES: &[&str] = &["x-ratelimit-"];

const APPLICATION_JSON: HeaderValue = HeaderValue::from_static("application/json");
const NO_CACHE: HeaderValue = HeaderValue::from_static("no-cache");

/// Allowlisted client headers plus `content-type: application/json` and the
/// channel's sensitive `authorization` value.
///
/// A header the client's `Connection` header names (RFC 9110, section
/// 7.6.1) is hop-by-hop for that request and is not copied even when
/// allowlisted. `Connection` is parsed only when present. The map is sized
/// once, so a typical request costs two allocations (index and entries).
pub(crate) fn upstream_request_headers(client: &HeaderMap, auth: &HeaderValue) -> HeaderMap {
    let mut upstream = HeaderMap::with_capacity(client.keys_len() + 2);
    let has_connection = client.contains_key(CONNECTION);
    for (name, value) in client {
        if is_request_allowed(name) && !(has_connection && named_by_connection(client, name)) {
            upstream.append(name.clone(), value.clone());
        }
    }
    upstream.insert(CONTENT_TYPE, APPLICATION_JSON);
    upstream.insert(header::AUTHORIZATION, auth.clone());
    upstream
}

/// Allowlisted upstream headers; for a committed SSE response (`sse`) Brisk
/// also sets its own `cache-control: no-cache`.
///
/// The upstream's own `Cache-Control` is not allowlisted: the SSE value
/// states Brisk's semantics as the event-stream source, so caches in front of
/// the client neither buffer nor store the stream.
pub(crate) fn downstream_response_headers(
    upstream: &HeaderMap,
    expose_ratelimit: bool,
    sse: bool,
) -> HeaderMap {
    let mut downstream = HeaderMap::with_capacity(RESPONSE_ALLOW.len() + 1);
    for (name, value) in upstream {
        if is_response_allowed(name.as_str(), expose_ratelimit) {
            downstream.append(name.clone(), value.clone());
        }
    }
    if sse {
        downstream.insert(CACHE_CONTROL, NO_CACHE);
    }
    downstream
}

fn is_request_allowed(name: &HeaderName) -> bool {
    REQUEST_ALLOW.contains(name)
        || REQUEST_ALLOW_PREFIXES
            .iter()
            .any(|prefix| name.as_str().starts_with(prefix))
}

fn is_response_allowed(name: &str, expose_ratelimit: bool) -> bool {
    RESPONSE_ALLOW.contains(&name)
        || (expose_ratelimit
            && (RATELIMIT_ALLOW.contains(&name)
                || RATELIMIT_ALLOW_PREFIXES
                    .iter()
                    .any(|prefix| name.starts_with(prefix))))
}

/// `name` appears among the comma-separated options of any `Connection`
/// value. Header names are lowercase, options compare case-insensitively.
fn named_by_connection(headers: &HeaderMap, name: &HeaderName) -> bool {
    let name = name.as_str().as_bytes();
    headers.get_all(CONNECTION).iter().any(|value| {
        value
            .as_bytes()
            .split(|&byte| byte == b',')
            .any(|option| option.trim_ascii().eq_ignore_ascii_case(name))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const INJECTED: HeaderValue = HeaderValue::from_static("Bearer sk-upstream");

    fn map(pairs: &[(&str, &str)]) -> HeaderMap {
        let mut headers = HeaderMap::new();
        for &(name, value) in pairs {
            headers.append(
                HeaderName::from_bytes(name.as_bytes()).expect("valid header name"),
                HeaderValue::from_str(value).expect("valid header value"),
            );
        }
        headers
    }

    fn names(headers: &HeaderMap) -> Vec<&str> {
        let mut names: Vec<&str> = headers.keys().map(HeaderName::as_str).collect();
        names.sort_unstable();
        names
    }

    fn injected_auth() -> HeaderValue {
        let mut value = INJECTED;
        value.set_sensitive(true);
        value
    }

    #[test]
    fn every_allowlisted_request_header_is_copied() {
        for allowed in &REQUEST_ALLOW {
            let client = map(&[(allowed.as_str(), "v")]);
            let upstream = upstream_request_headers(&client, &injected_auth());
            assert_eq!(
                upstream.get(allowed).map(HeaderValue::as_bytes),
                Some(&b"v"[..]),
                "{allowed}"
            );
            assert_eq!(upstream.len(), 3, "{allowed}");
        }
    }

    #[test]
    fn stainless_headers_are_copied() {
        let client = map(&[
            ("x-stainless-os", "Linux"),
            ("x-stainless-lang", "python"),
            ("x-stainless-retry-count", "0"),
        ]);
        let upstream = upstream_request_headers(&client, &injected_auth());
        assert_eq!(
            names(&upstream),
            [
                "authorization",
                "content-type",
                "x-stainless-lang",
                "x-stainless-os",
                "x-stainless-retry-count"
            ]
        );
        assert_eq!(upstream["x-stainless-os"], "Linux");
    }

    #[test]
    fn credentials_and_transport_headers_are_dropped() {
        let dropped = [
            ("authorization", "Bearer bk-client"),
            ("x-api-key", "bk-client"),
            ("x-goog-api-key", "bk-client"),
            ("cookie", "session=1"),
            ("accept-encoding", "gzip, br, zstd"),
            ("host", "gateway.test"),
            ("content-length", "12"),
            ("content-type", "text/plain"),
            ("transfer-encoding", "chunked"),
            ("connection", "keep-alive"),
            ("keep-alive", "timeout=5"),
            ("te", "trailers"),
            ("trailer", "x-checksum"),
            ("upgrade", "h2c"),
            ("proxy-authorization", "Basic dXNlcjpwYXNz"),
            ("proxy-connection", "keep-alive"),
            ("forwarded", "for=10.0.0.1"),
            ("x-forwarded-for", "10.0.0.1"),
            ("x-forwarded-host", "gateway.test"),
            ("x-forwarded-proto", "https"),
            ("x-real-ip", "10.0.0.1"),
            ("openai-organization", "org-1"),
            ("openai-project", "proj-1"),
            ("x-stainless", "no prefix match"),
        ];
        let upstream = upstream_request_headers(&map(&dropped), &injected_auth());
        assert_eq!(names(&upstream), ["authorization", "content-type"]);
        assert_eq!(upstream[CONTENT_TYPE], "application/json");
        assert_eq!(upstream[header::AUTHORIZATION], INJECTED);
        assert!(upstream[header::AUTHORIZATION].is_sensitive());
        assert_eq!(upstream.get_all(header::AUTHORIZATION).iter().count(), 1);
    }

    #[test]
    fn typical_client_request() {
        let client = map(&[
            ("host", "127.0.0.1:8080"),
            ("authorization", "Bearer bk-client"),
            ("content-type", "application/json"),
            ("content-length", "2048"),
            ("accept", "text/event-stream"),
            ("accept-encoding", "gzip"),
            ("user-agent", "OpenAI/Python 2.9.0"),
            ("idempotency-key", "stainless-python-retry-1"),
            ("x-stainless-os", "Windows"),
            ("session_id", "s-1"),
        ]);
        let upstream = upstream_request_headers(&client, &injected_auth());
        assert_eq!(
            names(&upstream),
            [
                "accept",
                "authorization",
                "content-type",
                "idempotency-key",
                "session_id",
                "user-agent",
                "x-stainless-os"
            ]
        );
        assert_eq!(upstream[header::ACCEPT], "text/event-stream");
    }

    #[test]
    fn repeated_allowlisted_headers_keep_every_value() {
        let client = map(&[
            ("accept", "text/event-stream"),
            ("accept", "application/json"),
        ]);
        let upstream = upstream_request_headers(&client, &injected_auth());
        let values: Vec<_> = upstream.get_all(header::ACCEPT).iter().collect();
        assert_eq!(values, ["text/event-stream", "application/json"]);
    }

    #[test]
    fn headers_named_by_connection_are_not_copied() {
        let client = map(&[
            ("connection", "x-stainless-os"),
            ("x-stainless-os", "Linux"),
            ("x-stainless-lang", "python"),
        ]);
        let upstream = upstream_request_headers(&client, &injected_auth());
        assert_eq!(
            names(&upstream),
            ["authorization", "content-type", "x-stainless-lang"]
        );

        let client = map(&[
            ("connection", "keep-alive, User-Agent"),
            ("connection", " Session_ID ,,Idempotency-Key "),
            ("user-agent", "curl/8"),
            ("session_id", "s-1"),
            ("idempotency-key", "k"),
            ("accept", "*/*"),
        ]);
        let upstream = upstream_request_headers(&client, &injected_auth());
        assert_eq!(
            names(&upstream),
            ["accept", "authorization", "content-type"]
        );
    }

    /// Response headers as CPA v7.3.8 sends them (from
    /// `docs/samples/cpa/grok46-xhigh-chat-stream.headers`, trace id zeroed
    /// as in the sanitised fixtures).
    const CPA_STREAM_HEADERS: &[u8] = b"HTTP/1.1 200 OK\r\n\
Access-Control-Allow-Headers: *\r\n\
Access-Control-Allow-Methods: GET, POST, PUT, PATCH, DELETE, OPTIONS\r\n\
Access-Control-Allow-Origin: *\r\n\
Access-Control-Expose-Headers: X-CPA-TRACE-ID, X-CPA-VERSION, X-CPA-COMMIT, X-CPA-BUILD-DATE, X-CPA-SUPPORT-PLUGIN, X-CPA-HOME-VERSION, X-CPA-HOME-BUILD-DATE, X-SERVER-VERSION, X-SERVER-BUILD-DATE, Location, Retry-After, X-Request-Id, OpenAI-Request-Id\r\n\
Cache-Control: no-cache\r\n\
Connection: keep-alive\r\n\
Content-Type: text/event-stream\r\n\
X-Cpa-Trace-Id: 20260919011838-0-0\r\n\
Date: Sat, 19 Sep 2026 01:18:40 GMT\r\n\
Transfer-Encoding: chunked\r\n\
\r\n";

    /// From `docs/samples/cpa/grok46-xhigh-chat-nonstream.headers`.
    const CPA_JSON_HEADERS: &[u8] = b"HTTP/1.1 200 OK\r\n\
Access-Control-Allow-Headers: *\r\n\
Access-Control-Allow-Methods: GET, POST, PUT, PATCH, DELETE, OPTIONS\r\n\
Access-Control-Allow-Origin: *\r\n\
Access-Control-Expose-Headers: X-CPA-TRACE-ID, X-CPA-VERSION, X-CPA-COMMIT, X-CPA-BUILD-DATE, X-CPA-SUPPORT-PLUGIN, X-CPA-HOME-VERSION, X-CPA-HOME-BUILD-DATE, X-SERVER-VERSION, X-SERVER-BUILD-DATE, Location, Retry-After, X-Request-Id, OpenAI-Request-Id\r\n\
Content-Type: application/json\r\n\
X-Cpa-Trace-Id: 20260919011841-0-0\r\n\
Date: Sat, 19 Sep 2026 01:18:44 GMT\r\n\
Content-Length: 525\r\n\
\r\n";

    /// From `docs/samples/cpa/error-bad-key.headers`.
    const CPA_ERROR_HEADERS: &[u8] = b"HTTP/1.1 401 Unauthorized\r\n\
Access-Control-Allow-Headers: *\r\n\
Access-Control-Allow-Methods: GET, POST, PUT, PATCH, DELETE, OPTIONS\r\n\
Access-Control-Allow-Origin: *\r\n\
Access-Control-Expose-Headers: X-CPA-TRACE-ID, X-CPA-VERSION, X-CPA-COMMIT, X-CPA-BUILD-DATE, X-CPA-SUPPORT-PLUGIN, X-CPA-HOME-VERSION, X-CPA-HOME-BUILD-DATE, X-SERVER-VERSION, X-SERVER-BUILD-DATE, Location, Retry-After, X-Request-Id, OpenAI-Request-Id\r\n\
Content-Type: application/json; charset=utf-8\r\n\
Date: Sat, 19 Sep 2026 01:15:06 GMT\r\n\
Content-Length: 27\r\n\
\r\n";

    fn parse_response_head(raw: &[u8]) -> HeaderMap {
        let mut slots = [httparse::EMPTY_HEADER; 32];
        let mut response = httparse::Response::new(&mut slots);
        assert!(
            response
                .parse(raw)
                .expect("valid response head")
                .is_complete(),
            "incomplete response head"
        );
        let mut headers = HeaderMap::new();
        for field in response.headers.iter() {
            headers.append(
                HeaderName::from_bytes(field.name.as_bytes()).expect("valid header name"),
                HeaderValue::from_bytes(field.value).expect("valid header value"),
            );
        }
        headers
    }

    #[test]
    fn cpa_response_headers_reduce_to_content_type() {
        for (raw, content_type) in [
            (CPA_STREAM_HEADERS, "text/event-stream"),
            (CPA_JSON_HEADERS, "application/json"),
            (CPA_ERROR_HEADERS, "application/json; charset=utf-8"),
        ] {
            let upstream = parse_response_head(raw);
            let downstream = downstream_response_headers(&upstream, false, false);
            assert_eq!(names(&downstream), ["content-type"]);
            assert_eq!(downstream[CONTENT_TYPE], content_type);

            let exposed = downstream_response_headers(&upstream, true, false);
            assert_eq!(names(&exposed), ["content-type"]);
        }
    }

    #[test]
    fn committed_sse_gets_brisks_own_cache_control() {
        let upstream = parse_response_head(CPA_STREAM_HEADERS);
        let downstream = downstream_response_headers(&upstream, false, true);
        assert_eq!(names(&downstream), ["cache-control", "content-type"]);
        assert_eq!(downstream[CACHE_CONTROL], "no-cache");
        assert_eq!(downstream.get_all(CACHE_CONTROL).iter().count(), 1);

        let upstream = map(&[
            ("content-type", "text/event-stream"),
            ("cache-control", "public, max-age=600"),
        ]);
        let downstream = downstream_response_headers(&upstream, false, true);
        let values: Vec<_> = downstream.get_all(CACHE_CONTROL).iter().collect();
        assert_eq!(values, ["no-cache"]);
    }

    #[test]
    fn every_allowlisted_response_header_is_copied() {
        let upstream = map(&[
            ("content-type", "application/json"),
            ("x-request-id", "req-1"),
            ("retry-after", "3"),
        ]);
        let downstream = downstream_response_headers(&upstream, false, false);
        assert_eq!(
            names(&downstream),
            ["content-type", "retry-after", "x-request-id"]
        );
        assert_eq!(downstream["retry-after"], "3");
        assert_eq!(downstream["x-request-id"], "req-1");
    }

    #[test]
    fn sensitive_and_hop_by_hop_response_headers_are_dropped() {
        let upstream = map(&[
            ("content-type", "application/json"),
            ("set-cookie", "session=1"),
            ("openai-organization", "org-1"),
            ("openai-project", "proj-1"),
            ("openai-processing-ms", "12"),
            ("cf-ray", "abc"),
            ("connection", "close"),
            ("keep-alive", "timeout=5"),
            ("transfer-encoding", "chunked"),
            ("content-length", "10"),
            ("content-encoding", "gzip"),
            ("x-cpa-trace-id", "t"),
            ("access-control-allow-origin", "*"),
            ("strict-transport-security", "max-age=1"),
            ("server", "cpa"),
        ]);
        let downstream = downstream_response_headers(&upstream, true, false);
        assert_eq!(names(&downstream), ["content-type"]);
    }

    #[test]
    fn ratelimit_headers_only_with_the_switch() {
        let upstream = map(&[
            ("content-type", "application/json"),
            ("ratelimit", "limit=10, remaining=9, reset=1"),
            ("ratelimit-policy", "10;w=1"),
            ("x-ratelimit-limit-requests", "100"),
            ("x-ratelimit-remaining-tokens", "5000"),
            ("x-ratelimited", "no prefix match"),
        ]);
        let hidden = downstream_response_headers(&upstream, false, false);
        assert_eq!(names(&hidden), ["content-type"]);

        let exposed = downstream_response_headers(&upstream, true, false);
        assert_eq!(
            names(&exposed),
            [
                "content-type",
                "ratelimit",
                "ratelimit-policy",
                "x-ratelimit-limit-requests",
                "x-ratelimit-remaining-tokens"
            ]
        );
        assert_eq!(exposed["x-ratelimit-remaining-tokens"], "5000");
    }
}
