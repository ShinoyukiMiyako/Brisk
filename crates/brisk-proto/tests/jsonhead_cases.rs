//! Table-driven cases for `ChatHead::parse` (contract 05, 1.3.2 and 4.2).

use std::borrow::Cow;

use brisk_proto::Span;
use brisk_proto::jsonhead::{ChatHead, HeadError, IncludeUsage, StreamOptions};

const TEST_MODEL: &str = "grok-4.6(xhigh)";

fn parse(body: &str) -> Result<ChatHead<'_>, HeadError> {
    ChatHead::parse(body.as_bytes())
}

fn ok(body: &str) -> ChatHead<'_> {
    match parse(body) {
        Ok(head) => head,
        Err(error) => panic!("{body:?} was rejected: {error}"),
    }
}

fn err(body: &str) -> HeadError {
    match parse(body) {
        Ok(head) => panic!("{body:?} was accepted: {head:?}"),
        Err(error) => error,
    }
}

fn slice(body: &str, span: Span) -> &str {
    &body[span.start..span.end]
}

fn assert_json(body: &str) {
    assert!(
        matches!(err(body), HeadError::Json(_)),
        "{body:?} should be a JSON error"
    );
}

#[test]
fn minimal_request() {
    let body = r#"{"model":"m"}"#;
    let head = ok(body);
    assert_eq!(head.model, Span { start: 9, end: 12 });
    assert_eq!(head.model_name, "m");
    assert!(!head.stream);
    assert_eq!(head.stream_options, StreamOptions::Absent);
    assert_eq!(head.object_end, 12);
    assert!(!head.requests_usage());
}

#[test]
fn bracketed_model_passes_through_borrowed() {
    let body = format!(r#"{{"model":"{TEST_MODEL}","stream":true}}"#);
    let head = ok(&body);
    assert_eq!(head.model_name, TEST_MODEL);
    assert!(matches!(head.model_name, Cow::Borrowed(_)));
    assert_eq!(slice(&body, head.model), format!("\"{TEST_MODEL}\""));
    assert!(head.stream);
}

#[test]
fn escaped_model_is_decoded() {
    let body = "{\"model\":\"grok-4.6\\u0028xhigh)\"}";
    let head = ok(body);
    assert_eq!(head.model_name, TEST_MODEL);
    assert!(matches!(head.model_name, Cow::Owned(_)));
    // The span still covers the literal as written.
    assert_eq!(slice(body, head.model), "\"grok-4.6\\u0028xhigh)\"");

    let head = ok("{\"model\":\"a\\\"b\\\\c\\/d\\ud83d\\ude00\"}");
    assert_eq!(head.model_name, "a\"b\\c/d\u{1f600}");
}

#[test]
fn repeated_members_are_rejected() {
    for body in [
        r#"{"model":"a","model":"b"}"#,
        r#"{"model":"a","model":"a"}"#,
        "{\"model\":\"a\",\"mod\\u0065l\":\"b\"}",
        "{\"mod\\u0065l\":\"a\",\"model\":\"b\"}",
        r#"{"model":"m","stream":true,"stream":false}"#,
        r#"{"model":"m","stream":null,"stream":true}"#,
        "{\"model\":\"m\",\"stream\":true,\"str\\u0065am\":true}",
        r#"{"model":"m","stream_options":null,"stream_options":{}}"#,
        r#"{"model":"m","stream_options":{"include_usage":true,"include_usage":false}}"#,
        "{\"model\":\"m\",\"stream_options\":{\"include_usage\":true,\"include\\u005fusage\":true}}",
    ] {
        assert_json(body);
    }
}

#[test]
fn only_a_nested_model_is_missing() {
    assert!(matches!(
        err(r#"{"messages":[{"role":"user","model":"x"}],"stream_options":{"model":"y"}}"#),
        HeadError::MissingModel
    ));
    assert!(matches!(err("{}"), HeadError::MissingModel));
}

#[test]
fn model_types() {
    assert!(matches!(
        err(r#"{"model":null}"#),
        HeadError::ModelNotString
    ));
    assert!(matches!(err(r#"{"model":7}"#), HeadError::ModelNotString));
    assert!(matches!(
        err(r#"{"model":["m"]}"#),
        HeadError::ModelNotString
    ));
    assert!(matches!(err(r#"{"model":""}"#), HeadError::EmptyModel));
}

#[test]
fn top_level_must_be_an_object() {
    for body in [r#"["m",true]"#, r#""model""#, "42", "null", " [] "] {
        assert!(
            matches!(err(body), HeadError::NotAnObject),
            "{body:?} should not be an object"
        );
    }
}

#[test]
fn malformed_bodies_are_json_errors() {
    for body in [
        "",
        "   ",
        r#"{"model":"m"} x"#,
        r#"{"model":"m"}{}"#,
        r#"{"model":"m""#,
        r#"{"model":"m",}"#,
        "\u{feff}{\"model\":\"m\"}",
        "[1,",
    ] {
        assert_json(body);
    }
}

#[test]
fn stream_values() {
    assert!(!ok(r#"{"model":"m","stream":null}"#).stream);
    assert!(!ok(r#"{"model":"m","stream":false}"#).stream);
    assert!(ok(r#"{"model":"m","stream":true}"#).stream);
    assert_json(r#"{"model":"m","stream":"true"}"#);
    assert_json(r#"{"model":"m","stream":1}"#);
}

#[test]
fn stream_options_values() {
    let body = r#"{"model":"m","stream_options":null}"#;
    let StreamOptions::Null { value } = ok(body).stream_options else {
        panic!("expected Null");
    };
    assert_eq!(slice(body, value), "null");

    let body = r#"{"model":"m","stream_options":{}}"#;
    let StreamOptions::Object {
        value,
        include_usage,
    } = ok(body).stream_options
    else {
        panic!("expected Object");
    };
    assert_eq!(slice(body, value), "{}");
    assert_eq!(include_usage, IncludeUsage::Absent);

    for body in [
        r#"{"model":"m","stream_options":[]}"#,
        r#"{"model":"m","stream_options":"x"}"#,
        r#"{"model":"m","stream_options":true}"#,
        r#"{"model":"m","stream_options":0}"#,
    ] {
        assert!(
            matches!(err(body), HeadError::InvalidStreamOptions),
            "{body:?}"
        );
    }
}

#[test]
fn include_usage_values() {
    for (literal, expected) in [("true", Some(true)), ("false", Some(false)), ("null", None)] {
        let body = format!(r#"{{"model":"m","stream_options":{{"include_usage":{literal}}}}}"#);
        let StreamOptions::Object {
            include_usage: IncludeUsage::Present { value, span },
            ..
        } = ok(&body).stream_options
        else {
            panic!("expected include_usage in {body}");
        };
        assert_eq!(value, expected);
        assert_eq!(slice(&body, span), literal);
    }
    for literal in [r#""yes""#, "1", "{}", "[]"] {
        let body = format!(r#"{{"model":"m","stream_options":{{"include_usage":{literal}}}}}"#);
        assert!(
            matches!(err(&body), HeadError::InvalidStreamOptions),
            "{body}"
        );
    }
}

#[test]
fn nesting_is_limited_to_128_levels() {
    let nested = |depth: usize| format!("{}{}", "[".repeat(depth), "]".repeat(depth));
    ok(&format!(r#"{{"model":"m","x":{}}}"#, nested(100)));
    assert_json(&format!(r#"{{"model":"m","x":{}}}"#, nested(200)));
    assert_json(&format!(
        r#"{{"model":"m","stream_options":{{"x":{}}}}}"#,
        nested(200)
    ));
    assert_json(&nested(200));
}

/// `serde_json` allows 127 nested arrays and objects, the top-level object
/// included. `stream_options` is captured without a depth limit and then
/// parsed on its own, so the limit there is checked separately and must land
/// on the same boundary.
#[test]
fn nesting_limit_is_exact_inside_stream_options() {
    let nested = |depth: usize| format!("{}{}", "[".repeat(depth), "]".repeat(depth));
    // Top-level object, then the member value: 1 + depth levels.
    ok(&format!(r#"{{"model":"m","x":{}}}"#, nested(126)));
    assert_json(&format!(r#"{{"model":"m","x":{}}}"#, nested(127)));
    // Top-level object, `stream_options`, then the member value.
    ok(&format!(
        r#"{{"model":"m","stream_options":{{"x":{}}}}}"#,
        nested(125)
    ));
    assert_json(&format!(
        r#"{{"model":"m","stream_options":{{"x":{}}}}}"#,
        nested(126)
    ));
}

#[test]
fn spans_are_exact_among_whitespace() {
    let body = "\r\n\t { \n \"stream\" :\ttrue , \"model\"\r\n:\n  \"grok-4.6(xhigh)\"  ,\
                \"stream_options\" : { \"include_usage\" :  false  } \n } \n\t";
    let head = ok(body);
    assert_eq!(slice(body, head.model), "\"grok-4.6(xhigh)\"");
    assert!(head.stream);
    let StreamOptions::Object {
        value,
        include_usage: IncludeUsage::Present { span, value: flag },
    } = head.stream_options
    else {
        panic!("expected include_usage");
    };
    assert_eq!(slice(body, value), "{ \"include_usage\" :  false  }");
    assert_eq!(slice(body, span), "false");
    assert_eq!(flag, Some(false));
    assert_eq!(&body[head.object_end..=head.object_end], "}");
    assert_eq!(body[head.object_end + 1..].trim(), "");
}

#[test]
fn undeclared_members_may_repeat() {
    let head = ok(r#"{"model":"m","messages":[],"messages":[1],"temperature":1,"temperature":2}"#);
    assert_eq!(head.model_name, "m");
}

#[test]
fn case_folded_keys_are_ambiguous() {
    let cases: &[(&str, &str)] = &[
        (r#"{"Model":"x","model":"m"}"#, "model"),
        (r#"{"model":"m","Model":"x"}"#, "model"),
        (r#"{"model":"m","MODEL":"x"}"#, "model"),
        (r#"{"Model":"x"}"#, "model"),
        ("{\"model\":\"m\",\"Mod\\u0065l\":\"x\"}", "model"),
        ("{\"model\":\"m\",\"\u{17f}tream\":true}", "stream"),
        (
            "{\"\u{17f}tream\":true,\"model\":\"m\",\"stream\":false}",
            "stream",
        ),
        (
            "{\"model\":\"m\",\"stream_option\u{17f}\":null}",
            "stream_options",
        ),
        (
            "{\"model\":\"m\",\"STREAM_OPTIONS\":null}",
            "stream_options",
        ),
        (
            "{\"model\":\"m\",\"stream_options\":{\"\u{131}nclude_usage\":true}}",
            "include_usage",
        ),
        (
            "{\"model\":\"m\",\"stream_options\":{\"\u{130}nclude_usage\":true,\"include_usage\":true}}",
            "include_usage",
        ),
        (
            "{\"model\":\"m\",\"stream_options\":{\"include_usage\":true,\"Include_Usage\":false}}",
            "include_usage",
        ),
    ];
    for (body, field) in cases {
        match err(body) {
            HeadError::AmbiguousKey(name) => assert_eq!(name, *field, "{body}"),
            other => panic!("{body:?}: expected AmbiguousKey, got {other:?}"),
        }
    }
}

#[test]
fn similar_keys_outside_the_folding_window_are_ignored() {
    let head = ok(
        r#"{"models":["a"],"model_name":"b","streaming":true,"stream_options_extra":1,"model":"m"}"#,
    );
    assert_eq!(head.model_name, "m");
    assert!(!head.stream);
    assert_eq!(head.stream_options, StreamOptions::Absent);
}

#[test]
fn ambiguous_key_display_does_not_echo_the_key() {
    let error = err(r#"{"model":"m","MoDeL":"x"}"#);
    let message = error.to_string();
    assert!(message.contains("`model`"));
    assert!(!message.contains("MoDeL"));
}

#[test]
fn requests_usage_only_for_streaming_with_include_usage_true() {
    assert!(
        ok(r#"{"model":"m","stream":true,"stream_options":{"include_usage":true}}"#)
            .requests_usage()
    );
    assert!(!ok(r#"{"model":"m","stream_options":{"include_usage":true}}"#).requests_usage());
}
