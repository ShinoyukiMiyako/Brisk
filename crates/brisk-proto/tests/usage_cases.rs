//! Usage, finish and error extraction (contract 05, 1.3.5 and 4.2) on the
//! CPA fixtures and on hand-written edge cases.

use std::io::Read;

use brisk_proto::UsageErrorKind;
use brisk_proto::UsageTokens;
use brisk_proto::sse::{Data, SseScanner};
use brisk_proto::usage::{
    ChunkFacts, UsageAcc, UsageError, may_carry_error, may_carry_finish, may_carry_usage,
    parse_chunk, parse_completion, parse_completion_from,
};

macro_rules! fixture {
    ($name:literal) => {
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../fixtures/cpa/",
            $name
        ))
        .as_slice()
    };
}

const fn tokens(input: u64, output: u64, cached: u64, reasoning: u64) -> UsageTokens {
    UsageTokens {
        input,
        output,
        cached_input: Some(cached),
        reasoning_output: Some(reasoning),
    }
}

fn is_candidate(payload: &[u8]) -> bool {
    may_carry_usage(payload) || may_carry_finish(payload) || may_carry_error(payload)
}

/// Frames a fixture and runs every candidate `data` payload through
/// `parse_chunk`, the way the response body does.
fn scan_stream(stream: &[u8]) -> (Option<UsageTokens>, Vec<ChunkFacts>) {
    let mut scanner = SseScanner::new();
    let mut acc = UsageAcc::default();
    let mut candidates = Vec::new();
    scanner
        .feed(stream, |event| {
            let Data::Single(payload) = event.data() else {
                return;
            };
            if !is_candidate(payload) {
                return;
            }
            let facts = parse_chunk(payload).unwrap();
            if let Some(usage) = facts.usage {
                acc.observe(usage);
            }
            candidates.push(facts);
        })
        .unwrap();
    (acc.get(), candidates)
}

fn chunk(payload: &str) -> ChunkFacts {
    match parse_chunk(payload.as_bytes()) {
        Ok(facts) => facts,
        Err(error) => panic!("{payload}: {error}"),
    }
}

fn chunk_error(payload: &str) -> UsageError {
    match parse_chunk(payload.as_bytes()) {
        Ok(facts) => panic!("{payload} was accepted: {facts:?}"),
        Err(error) => error,
    }
}

#[test]
fn cpa_stream_fixtures_carry_usage_in_the_finish_chunk() {
    for (stream, expected) in [
        (fixture!("chat-stream-gpt55.sse"), tokens(307, 5, 0, 0)),
        (
            fixture!("chat-stream-grok46-xhigh.sse"),
            tokens(213, 71, 0, 70),
        ),
        (
            fixture!("chat-stream-grok46-suffix.sse"),
            tokens(213, 105, 128, 104),
        ),
        (fixture!("chat-stream-grok43.sse"), tokens(197, 216, 0, 215)),
    ] {
        let (usage, candidates) = scan_stream(stream);
        assert_eq!(usage, Some(expected));
        // Only the last content chunk is a candidate: every other chunk has
        // `"finish_reason":null` and no usage.
        assert_eq!(
            candidates,
            [ChunkFacts {
                usage: Some(expected),
                finished: true,
                usage_only: false,
                error: false,
            }]
        );
    }
}

#[test]
fn standalone_usage_chunk_is_usage_only() {
    let facts = chunk(
        r#"{"id":"c","object":"chat.completion.chunk","choices":[],"usage":{"prompt_tokens":10,"completion_tokens":2,"total_tokens":12}}"#,
    );
    assert_eq!(
        facts,
        ChunkFacts {
            usage: Some(UsageTokens {
                input: 10,
                output: 2,
                cached_input: None,
                reasoning_output: None,
            }),
            finished: false,
            usage_only: true,
            error: false,
        }
    );
    assert!(chunk(r#"{"usage":{"prompt_tokens":1,"completion_tokens":1}}"#).usage_only);
    assert!(
        chunk(r#"{"choices":null,"usage":{"prompt_tokens":1,"completion_tokens":1}}"#).usage_only
    );
    // A usage chunk that also reports an error is never stripped.
    assert!(
        !chunk(r#"{"choices":[],"error":{},"usage":{"prompt_tokens":1,"completion_tokens":1}}"#)
            .usage_only
    );
}

#[test]
fn usage_prefilter() {
    assert!(!may_carry_usage(
        br#"{"choices":[{"delta":{"content":"x"},"finish_reason":null}],"usage":null}"#
    ));
    assert!(may_carry_usage(br#"{"usage" : {"prompt_tokens":1}}"#));
    assert!(may_carry_usage(b"{\"usage\"\n:\r\n\t{}}"));
    assert!(!may_carry_usage(br#"{"content":"no usage here"}"#));
    // A string that merely contains the word is not a candidate either.
    assert!(!may_carry_usage(br#"{"content":"usage: {}"}"#));
}

#[test]
fn only_top_level_usage_counts() {
    let payload = r#"{"choices":[{"delta":{"attribution":{"usage":{"prompt_tokens":9,"completion_tokens":9}}},"finish_reason":null}]}"#;
    assert!(may_carry_usage(payload.as_bytes()));
    assert_eq!(chunk(payload), ChunkFacts::default());

    let in_string = r#"{"choices":[{"delta":{"content":"\"usage\":{\"prompt_tokens\":1}"},"finish_reason":"stop"}]}"#;
    let facts = chunk(in_string);
    assert_eq!(facts.usage, None);
    assert!(facts.finished);
}

#[test]
fn cpa_error_shapes_are_errors() {
    for payload in [
        r#"{"error":"Invalid API key"}"#,
        r#"{"error":{"message":"x","type":"server_error"}}"#,
    ] {
        assert!(may_carry_error(payload.as_bytes()));
        let facts = chunk(payload);
        assert!(facts.error);
        assert_eq!(facts.usage, None);
        assert!(!facts.usage_only);
    }
    assert!(!chunk(r#"{"error":null,"choices":[{"finish_reason":"stop"}]}"#).error);
}

#[test]
fn invalid_usage_is_reported() {
    let missing = chunk_error(r#"{"usage":{"completion_tokens":5}}"#);
    assert!(matches!(missing, UsageError::MissingField("prompt_tokens")));
    let missing = chunk_error(r#"{"usage":{"prompt_tokens":5,"completion_tokens":null}}"#);
    assert!(matches!(
        missing,
        UsageError::MissingField("completion_tokens")
    ));

    for payload in [
        r#"{"usage":{"prompt_tokens":213.0,"completion_tokens":71}}"#,
        r#"{"usage":{"prompt_tokens":-1,"completion_tokens":71}}"#,
        r#"{"usage":{"prompt_tokens":"213","completion_tokens":71}}"#,
        r#"{"usage":[213,71]}"#,
        r#"{"usage":{"prompt_tokens":1,"completion_tokens":1},"usage":{"prompt_tokens":2,"completion_tokens":2}}"#,
        r#"{"usage":{"prompt_tokens":1,"prompt_tokens":2,"completion_tokens":1}}"#,
        r#"{"usage":{"prompt_tokens":1,"completion_tokens":1}} trailing"#,
    ] {
        assert!(
            matches!(chunk_error(payload), UsageError::Json(_)),
            "{payload}"
        );
    }
}

#[test]
fn error_kind_carries_no_payload_text() {
    let error = chunk_error(r#"{"usage":{"prompt_tokens":"leaked-secret","completion_tokens":1}}"#);
    let kind = error.kind();
    let UsageErrorKind::Json { category, column } = kind else {
        panic!("expected a JSON error kind, got {kind:?}");
    };
    assert_eq!(category, serde_json::error::Category::Data);
    assert!(column > 0);
    assert!(!format!("{kind:?}").contains("leaked-secret"));

    assert_eq!(
        chunk_error(r#"{"usage":{}}"#).kind(),
        UsageErrorKind::MissingField("prompt_tokens")
    );
}

#[test]
fn escaped_delta_does_not_disturb_the_final_chunk() {
    let payload = r#"{"id":"00000000-0000-0000-0000-000000000001","choices":[{"index":0,"delta":{"role":"assistant","reasoning_content":"say \"pong\"\n\ttab \\ end"},"finish_reason":"stop","native_finish_reason":"stop"}],"usage":{"completion_tokens":71,"total_tokens":284,"prompt_tokens":213,"prompt_tokens_details":{"cached_tokens":0},"completion_tokens_details":{"reasoning_tokens":70}}}"#;
    assert_eq!(
        chunk(payload),
        ChunkFacts {
            usage: Some(tokens(213, 71, 0, 70)),
            finished: true,
            usage_only: false,
            error: false,
        }
    );
}

#[test]
fn any_non_null_finish_reason_finishes() {
    for reason in [r#""stop""#, "1", "{}", "[]", "true", r#""a\"b""#] {
        let payload = format!(r#"{{"choices":[{{"index":0,"finish_reason":{reason}}}]}}"#);
        assert!(chunk(&payload).finished, "{payload}");
    }
    assert!(!chunk(r#"{"choices":[{"finish_reason":null},{}]}"#).finished);
    assert!(chunk(r#"{"choices":[{"finish_reason":null},{"finish_reason":"length"}]}"#).finished);
}

#[test]
fn nonstream_fixtures() {
    let grok = parse_completion(fixture!("chat-nonstream-grok46-xhigh.json")).unwrap();
    assert_eq!(grok.usage, Some(tokens(213, 128, 128, 127)));
    assert!(!grok.error);

    let gpt = parse_completion(fixture!("chat-nonstream-gpt55.json")).unwrap();
    assert_eq!(gpt.usage, Some(tokens(307, 5, 0, 0)));
    assert!(!gpt.error);

    for body in [
        fixture!("error-bad-key.json"),
        fixture!("error-bogus-effort.json"),
    ] {
        let facts = parse_completion(body).unwrap();
        assert!(facts.error);
        assert_eq!(facts.usage, None);
    }
}

#[test]
fn completion_edge_cases() {
    let facts = parse_completion(br#"{"usage":null,"choices":[]}"#).unwrap();
    assert_eq!(facts.usage, None);
    assert!(!facts.error);

    let facts =
        parse_completion(br#"{"usage":{"prompt_tokens":3,"completion_tokens":4}}"#).unwrap();
    assert_eq!(
        facts.usage,
        Some(UsageTokens {
            input: 3,
            output: 4,
            cached_input: None,
            reasoning_output: None,
        })
    );
    assert!(matches!(
        parse_completion(br#"{"usage":{"prompt_tokens":3}}"#),
        Err(UsageError::MissingField("completion_tokens"))
    ));
    assert!(matches!(
        parse_completion(br#"{"usage":{"prompt_tokens":3,"completion_tokens":4}"#),
        Err(UsageError::Json(_))
    ));
}

#[test]
fn multi_frame_parse_matches_contiguous_parse() {
    for body in [
        fixture!("chat-nonstream-grok46-xhigh.json"),
        fixture!("chat-nonstream-gpt55.json"),
        fixture!("error-bad-key.json"),
        fixture!("error-bogus-effort.json"),
    ] {
        let contiguous = parse_completion(body).unwrap();
        for piece in [1, 7, 64, body.len()] {
            let mut frames = body.chunks(piece);
            let first = frames.next().unwrap();
            let reader = frames.fold(Box::new(first) as Box<dyn Read>, |reader, frame| {
                Box::new(reader.chain(frame))
            });
            assert_eq!(parse_completion_from(reader).unwrap(), contiguous);
        }
    }

    let truncated = &fixture!("chat-nonstream-gpt55.json")[..100];
    assert!(matches!(
        parse_completion_from(truncated.chain(&b""[..])),
        Err(UsageError::Json(_))
    ));
}
