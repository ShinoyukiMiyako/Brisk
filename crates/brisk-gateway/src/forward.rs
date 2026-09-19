//! The forwarding core for `POST /v1/chat/completions`: authentication, body
//! intake, head parsing, channel selection, request rewriting, failover
//! strictly before commit, and the hand-over of a committed response to its
//! body (R4, R13, R14, R16, R17, R19).

use http::header::CONTENT_TYPE;
use http::{HeaderMap, StatusCode};

use crate::outcome::FailureClass;

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
}
