//! Responses Brisk generates itself: `OpenAI`-style error bodies with fixed
//! messages, so no upstream detail, header or secret can reach the client
//! through them, and the health and readiness bodies (R17).
//!
//! Every fixed body is a `'static` string assembled at compile time, so
//! building one of these responses allocates only the response head. The
//! one dynamic message, [`invalid_json`], echoes the client's own request
//! back to it and nothing else (D23).

use brisk_proto::jsonhead::HeadError;
use brisk_proto::splice::json_string;
use bytes::{BufMut, Bytes, BytesMut};
use http::header::CONTENT_TYPE;
use http::{HeaderValue, Response, StatusCode};
use http_body_util::Full;

/// An error Brisk reports with a fixed message: every row of the contract's
/// 1.4.8 table except `invalid_json`, whose message is the [`HeadError`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum ErrorCode {
    /// 401: missing, malformed or unknown virtual key.
    InvalidApiKey,
    /// 415: `Content-Encoding` other than `identity` (D29).
    UnsupportedContentEncoding,
    /// 404: no channel serves the requested model (D30).
    ModelNotFound,
    /// 404: unknown path.
    NotFound,
    /// 405: known path, wrong method.
    MethodNotAllowed,
    /// 408: the request body did not arrive in time or arrived too slowly.
    RequestTimeout,
    /// 413: the request body exceeds `max_body`.
    RequestTooLarge,
    /// 503: the in-flight byte budget is exhausted.
    Overloaded,
    /// 500: an internal invariant was violated; the caller logs the cause.
    InternalError,
    /// 502: the last attempt failed to connect or broke in transport.
    UpstreamUnreachable,
    /// 502: the last attempt got a 3xx.
    UpstreamRedirect,
    /// 502: the last attempt got a 401 or 403.
    UpstreamAuthFailed,
    /// 502: the last attempt produced an empty stream.
    UpstreamEmptyResponse,
    /// 502: the last attempt produced a malformed stream.
    UpstreamProtocolError,
    /// 504: the last attempt timed out before the first byte, or the
    /// upstream answered 408 (D3).
    UpstreamTimeout,
}

/// Builds the complete error body from string literals at compile time.
/// The literals contain no character that needs JSON escaping, which the
/// tests verify by parsing every body.
macro_rules! error_body {
    ($message:literal, $kind:literal, $code:literal) => {
        concat!(
            r#"{"error":{"message":""#,
            $message,
            r#"","type":""#,
            $kind,
            r#"","code":""#,
            $code,
            r#"","param":null}}"#
        )
    };
}

impl ErrorCode {
    /// Every variant, in table order.
    #[cfg(test)]
    const ALL: [Self; 15] = [
        Self::InvalidApiKey,
        Self::UnsupportedContentEncoding,
        Self::ModelNotFound,
        Self::NotFound,
        Self::MethodNotAllowed,
        Self::RequestTimeout,
        Self::RequestTooLarge,
        Self::Overloaded,
        Self::InternalError,
        Self::UpstreamUnreachable,
        Self::UpstreamRedirect,
        Self::UpstreamAuthFailed,
        Self::UpstreamEmptyResponse,
        Self::UpstreamProtocolError,
        Self::UpstreamTimeout,
    ];

    /// The HTTP status of this error.
    pub(crate) const fn status(self) -> StatusCode {
        match self {
            Self::InvalidApiKey => StatusCode::UNAUTHORIZED,
            Self::UnsupportedContentEncoding => StatusCode::UNSUPPORTED_MEDIA_TYPE,
            Self::ModelNotFound | Self::NotFound => StatusCode::NOT_FOUND,
            Self::MethodNotAllowed => StatusCode::METHOD_NOT_ALLOWED,
            Self::RequestTimeout => StatusCode::REQUEST_TIMEOUT,
            Self::RequestTooLarge => StatusCode::PAYLOAD_TOO_LARGE,
            Self::Overloaded => StatusCode::SERVICE_UNAVAILABLE,
            Self::InternalError => StatusCode::INTERNAL_SERVER_ERROR,
            Self::UpstreamUnreachable
            | Self::UpstreamRedirect
            | Self::UpstreamAuthFailed
            | Self::UpstreamEmptyResponse
            | Self::UpstreamProtocolError => StatusCode::BAD_GATEWAY,
            Self::UpstreamTimeout => StatusCode::GATEWAY_TIMEOUT,
        }
    }

    /// The `error.code` string of this error.
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::InvalidApiKey => "invalid_api_key",
            Self::UnsupportedContentEncoding => "unsupported_content_encoding",
            Self::ModelNotFound => "model_not_found",
            Self::NotFound => "not_found",
            Self::MethodNotAllowed => "method_not_allowed",
            Self::RequestTimeout => "request_timeout",
            Self::RequestTooLarge => "request_too_large",
            Self::Overloaded => "overloaded",
            Self::InternalError => "internal_error",
            Self::UpstreamUnreachable => "upstream_unreachable",
            Self::UpstreamRedirect => "upstream_redirect",
            Self::UpstreamAuthFailed => "upstream_auth_failed",
            Self::UpstreamEmptyResponse => "upstream_empty_response",
            Self::UpstreamProtocolError => "upstream_protocol_error",
            Self::UpstreamTimeout => "upstream_timeout",
        }
    }

    /// The complete JSON body of this error.
    const fn body(self) -> &'static str {
        match self {
            Self::InvalidApiKey => {
                error_body!(
                    "invalid API key",
                    "invalid_request_error",
                    "invalid_api_key"
                )
            }
            Self::UnsupportedContentEncoding => error_body!(
                "request bodies must not be compressed",
                "invalid_request_error",
                "unsupported_content_encoding"
            ),
            Self::ModelNotFound => error_body!(
                "no channel serves this model",
                "invalid_request_error",
                "model_not_found"
            ),
            Self::NotFound => error_body!("unknown path", "invalid_request_error", "not_found"),
            Self::MethodNotAllowed => error_body!(
                "method not allowed",
                "invalid_request_error",
                "method_not_allowed"
            ),
            Self::RequestTimeout => error_body!(
                "request body not received in time",
                "invalid_request_error",
                "request_timeout"
            ),
            Self::RequestTooLarge => error_body!(
                "request body too large",
                "invalid_request_error",
                "request_too_large"
            ),
            Self::Overloaded => error_body!("gateway overloaded", "server_error", "overloaded"),
            Self::InternalError => error_body!("internal error", "server_error", "internal_error"),
            Self::UpstreamUnreachable => error_body!(
                "upstream unavailable",
                "server_error",
                "upstream_unreachable"
            ),
            Self::UpstreamRedirect => error_body!(
                "upstream returned a redirect",
                "server_error",
                "upstream_redirect"
            ),
            Self::UpstreamAuthFailed => error_body!(
                "upstream rejected the gateway credentials",
                "server_error",
                "upstream_auth_failed"
            ),
            Self::UpstreamEmptyResponse => error_body!(
                "upstream returned an empty stream",
                "server_error",
                "upstream_empty_response"
            ),
            Self::UpstreamProtocolError => error_body!(
                "upstream sent a malformed stream",
                "server_error",
                "upstream_protocol_error"
            ),
            Self::UpstreamTimeout => error_body!(
                "upstream did not respond in time",
                "server_error",
                "upstream_timeout"
            ),
        }
    }
}

/// The fixed-message error of `code`.
pub(crate) fn error_response(code: ErrorCode) -> Response<Full<Bytes>> {
    json_response(code.status(), Bytes::from_static(code.body().as_bytes()))
}

/// The `invalid_json` body around its quoted message.
const INVALID_JSON_HEAD: &[u8] = br#"{"error":{"message":"#;
const INVALID_JSON_TAIL: &[u8] =
    br#","type":"invalid_request_error","code":"invalid_json","param":null}}"#;

/// 400 `invalid_json` with `err`'s Display as the message.
///
/// [`HeadError::SpanOutsideBody`] is not a client error but a broken
/// internal invariant, so it yields the 500 `internal_error` of the table
/// instead; the caller still logs it. Mapping it here keeps a caller that
/// forwards every `HeadError` from answering 400 for Brisk's own fault.
pub(crate) fn invalid_json(err: &HeadError) -> Response<Full<Bytes>> {
    if matches!(err, HeadError::SpanOutsideBody) {
        return error_response(ErrorCode::InternalError);
    }
    // `json_string` quotes and escapes: the Display of `HeadError::Json`
    // may quote the client's bytes, including quotes and control characters.
    let message = json_string(&err.to_string());
    let mut body =
        BytesMut::with_capacity(INVALID_JSON_HEAD.len() + message.len() + INVALID_JSON_TAIL.len());
    body.put_slice(INVALID_JSON_HEAD);
    body.put_slice(&message);
    body.put_slice(INVALID_JSON_TAIL);
    json_response(StatusCode::BAD_REQUEST, body.freeze())
}

/// `/healthz`: 200 `{"status":"ok"}` whenever the process serves requests.
pub(crate) fn live() -> Response<Full<Bytes>> {
    json_response(StatusCode::OK, Bytes::from_static(br#"{"status":"ok"}"#))
}

/// `/readyz`: 200 `{"status":"ready"}` once warm-up allows it, otherwise
/// 503 `{"status":"warming"}`.
pub(crate) fn health(ready: bool) -> Response<Full<Bytes>> {
    if ready {
        json_response(StatusCode::OK, Bytes::from_static(br#"{"status":"ready"}"#))
    } else {
        json_response(
            StatusCode::SERVICE_UNAVAILABLE,
            Bytes::from_static(br#"{"status":"warming"}"#),
        )
    }
}

fn json_response(status: StatusCode, body: Bytes) -> Response<Full<Bytes>> {
    let mut response = Response::new(Full::new(body));
    *response.status_mut() = status;
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    response
}

#[cfg(test)]
#[allow(
    clippy::disallowed_types,
    clippy::disallowed_macros,
    reason = "tests compare whole bodies as JSON values; R2 governs the data plane"
)]
mod tests {
    use super::*;
    use http_body_util::BodyExt;
    use serde_json::{Value, json};

    /// The contract's table: code, status, type, fixed message.
    const TABLE: [(ErrorCode, u16, &str, &str); 15] = [
        (
            ErrorCode::InvalidApiKey,
            401,
            "invalid_request_error",
            "invalid API key",
        ),
        (
            ErrorCode::UnsupportedContentEncoding,
            415,
            "invalid_request_error",
            "request bodies must not be compressed",
        ),
        (
            ErrorCode::ModelNotFound,
            404,
            "invalid_request_error",
            "no channel serves this model",
        ),
        (
            ErrorCode::NotFound,
            404,
            "invalid_request_error",
            "unknown path",
        ),
        (
            ErrorCode::MethodNotAllowed,
            405,
            "invalid_request_error",
            "method not allowed",
        ),
        (
            ErrorCode::RequestTimeout,
            408,
            "invalid_request_error",
            "request body not received in time",
        ),
        (
            ErrorCode::RequestTooLarge,
            413,
            "invalid_request_error",
            "request body too large",
        ),
        (
            ErrorCode::Overloaded,
            503,
            "server_error",
            "gateway overloaded",
        ),
        (
            ErrorCode::InternalError,
            500,
            "server_error",
            "internal error",
        ),
        (
            ErrorCode::UpstreamUnreachable,
            502,
            "server_error",
            "upstream unavailable",
        ),
        (
            ErrorCode::UpstreamRedirect,
            502,
            "server_error",
            "upstream returned a redirect",
        ),
        (
            ErrorCode::UpstreamAuthFailed,
            502,
            "server_error",
            "upstream rejected the gateway credentials",
        ),
        (
            ErrorCode::UpstreamEmptyResponse,
            502,
            "server_error",
            "upstream returned an empty stream",
        ),
        (
            ErrorCode::UpstreamProtocolError,
            502,
            "server_error",
            "upstream sent a malformed stream",
        ),
        (
            ErrorCode::UpstreamTimeout,
            504,
            "server_error",
            "upstream did not respond in time",
        ),
    ];

    async fn parts(response: Response<Full<Bytes>>) -> (StatusCode, Option<HeaderValue>, Value) {
        let status = response.status();
        let content_type = response.headers().get(CONTENT_TYPE).cloned();
        let bytes = response
            .into_body()
            .collect()
            .await
            .expect("Full is infallible")
            .to_bytes();
        let value = serde_json::from_slice(&bytes).expect("body is valid JSON");
        (status, content_type, value)
    }

    #[tokio::test]
    async fn every_code_has_its_status_type_code_and_fixed_message() {
        assert_eq!(TABLE.map(|row| row.0), ErrorCode::ALL);
        for (code, status, kind, message) in TABLE {
            let (actual_status, content_type, body) = parts(error_response(code)).await;
            assert_eq!(actual_status.as_u16(), status, "{code:?}");
            assert_eq!(code.status().as_u16(), status, "{code:?}");
            assert_eq!(
                content_type,
                Some(HeaderValue::from_static("application/json")),
                "{code:?}"
            );
            assert_eq!(
                body,
                json!({
                    "error": {
                        "message": message,
                        "type": kind,
                        "code": code.as_str(),
                        "param": null,
                    }
                }),
                "{code:?}"
            );
        }
    }

    #[test]
    fn code_strings_are_unique_and_snake_case() {
        let mut seen = std::collections::HashSet::new();
        for code in ErrorCode::ALL {
            let name = code.as_str();
            assert!(seen.insert(name), "duplicate code {name}");
            assert!(
                name.bytes().all(|b| b.is_ascii_lowercase() || b == b'_'),
                "{name}"
            );
        }
        assert!(!seen.contains("invalid_json"));
    }

    #[test]
    fn type_follows_status_class() {
        for code in ErrorCode::ALL {
            let expected = if code.status().is_client_error() {
                "invalid_request_error"
            } else {
                assert!(code.status().is_server_error(), "{code:?}");
                "server_error"
            };
            let body: Value = serde_json::from_str(code.body()).expect("valid JSON");
            assert_eq!(body["error"]["type"], expected, "{code:?}");
        }
    }

    #[tokio::test]
    async fn invalid_json_message_is_the_head_error_display() {
        let cases = [
            HeadError::NotAnObject,
            HeadError::MissingModel,
            HeadError::ModelNotString,
            HeadError::EmptyModel,
            HeadError::InvalidStreamOptions,
            HeadError::AmbiguousKey("model"),
            HeadError::Json(
                serde_json::from_str::<Value>("{\"a\":\"x\u{1}\"}").expect_err("control character"),
            ),
            HeadError::Json(serde_json::from_str::<Value>("{\"a\" 1}").expect_err("missing colon")),
        ];
        for err in &cases {
            let (status, content_type, body) = parts(invalid_json(err)).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{err:?}");
            assert_eq!(
                content_type,
                Some(HeaderValue::from_static("application/json"))
            );
            assert_eq!(
                body,
                json!({
                    "error": {
                        "message": err.to_string(),
                        "type": "invalid_request_error",
                        "code": "invalid_json",
                        "param": null,
                    }
                }),
                "{err:?}"
            );
        }
    }

    #[tokio::test]
    async fn invalid_json_escapes_client_bytes_in_the_message() {
        // The recognized field name is the only caller-chosen text in these
        // variants; a quote and a newline in it prove escaping, not luck.
        let err = HeadError::AmbiguousKey("mo\"del\n");
        let (_, _, body) = parts(invalid_json(&err)).await;
        assert_eq!(body["error"]["message"], err.to_string());
    }

    #[tokio::test]
    async fn span_outside_body_is_an_internal_error() {
        let (status, _, body) = parts(invalid_json(&HeadError::SpanOutsideBody)).await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(body["error"]["code"], "internal_error");
        assert_eq!(body["error"]["message"], "internal error");
    }

    #[tokio::test]
    async fn health_bodies() {
        let (status, content_type, body) = parts(live()).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            content_type,
            Some(HeaderValue::from_static("application/json"))
        );
        assert_eq!(body, json!({"status": "ok"}));

        let (status, _, body) = parts(health(true)).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, json!({"status": "ready"}));

        let (status, _, body) = parts(health(false)).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body, json!({"status": "warming"}));
    }
}
