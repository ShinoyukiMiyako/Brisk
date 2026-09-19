//! Every CPA fixture under `fixtures/cpa` is sanitised (contract 05, 4.4):
//! request and response ids are placeholders, the cache key, safety
//! identifier and fingerprint are zeroed, the trace id keeps only its
//! timestamp, and no credential trace is left.
//!
//! This is the CI counterpart of `scripts/fixtures/sanitize-cpa.py --check`,
//! which needs Python. It cannot see an operator-chosen CPA key; that check
//! runs locally with `--secret-env`.

use memchr::memmem;

macro_rules! fixture {
    ($name:literal) => {
        (
            $name,
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/../../fixtures/cpa/",
                $name
            ))
            .as_slice(),
        )
    };
}

const FIXTURES: &[(&str, &[u8])] = &[
    fixture!("chat-stream-grok46-xhigh.sse"),
    fixture!("chat-stream-grok46-xhigh.headers"),
    fixture!("chat-stream-grok46-suffix.sse"),
    fixture!("chat-stream-grok46-suffix.headers"),
    fixture!("chat-stream-grok43.sse"),
    fixture!("chat-stream-gpt55.sse"),
    fixture!("chat-stream-gpt55.headers"),
    fixture!("chat-nonstream-grok46-xhigh.json"),
    fixture!("chat-nonstream-grok46-xhigh.headers"),
    fixture!("chat-nonstream-gpt55.json"),
    fixture!("error-bad-key.json"),
    fixture!("error-bad-key.headers"),
    fixture!("error-bogus-effort.json"),
    fixture!("error-bogus-effort.headers"),
    fixture!("responses-stream-grok46-xhigh.sse"),
    fixture!("anthropic-stream-grok46-suffix.sse"),
    fixture!("gemini-sse-grok46-suffix.sse"),
];

/// Members whose string value identifies a request or response.
const ID_KEYS: &[&str] = &["id", "item_id", "responseId", "response_id"];

/// Members that are zeroed after the prefix that is kept.
const ZEROED_KEYS: &[(&str, &str)] = &[
    ("prompt_cache_key", ""),
    ("safety_identifier", "user-"),
    ("system_fingerprint", "fp_"),
];

const TRACE_ID_HEADER: &[u8] = b"x-cpa-trace-id:";

fn is_json_ws(byte: u8) -> bool {
    matches!(byte, b' ' | b'\t' | b'\r' | b'\n')
}

fn skip_ws(data: &[u8], mut at: usize) -> usize {
    while data.get(at).copied().is_some_and(is_json_ws) {
        at += 1;
    }
    at
}

fn line_of(data: &[u8], at: usize) -> usize {
    memchr::memchr_iter(b'\n', &data[..at]).count() + 1
}

/// The raw value of every `"key": "..."` member, or `None` for a string
/// value that contains an escape (never produced by the sanitiser, so it is
/// reported). Members with a non-string value are skipped.
fn string_members<'a>(data: &'a [u8], key: &str) -> Vec<(usize, Option<&'a [u8]>)> {
    let needle = format!("\"{key}\"");
    let mut found = Vec::new();
    for at in memmem::find_iter(data, needle.as_bytes()) {
        let colon = skip_ws(data, at + needle.len());
        if data.get(colon) != Some(&b':') {
            continue;
        }
        let open = skip_ws(data, colon + 1);
        if data.get(open) != Some(&b'"') {
            continue;
        }
        let value = memchr::memchr(b'"', &data[open + 1..])
            .map(|len| &data[open + 1..open + 1 + len])
            .filter(|value| !value.contains(&b'\\'));
        found.push((at, value));
    }
    found
}

/// Placeholders are small serials padded with zeros; a real hex id or UUID
/// is never all digits with this many leading zeros.
fn is_placeholder(value: &[u8]) -> bool {
    let rest = match memchr::memchr(b'_', value) {
        Some(underscore) => &value[underscore + 1..],
        None => value,
    };
    let digits: Vec<u8> = rest.iter().copied().filter(|&byte| byte != b'-').collect();
    let significant = digits.iter().skip_while(|&&byte| byte == b'0').count();
    !digits.is_empty() && digits.iter().all(u8::is_ascii_digit) && significant <= 6
}

fn is_zeroed(value: &[u8], prefix: &str) -> bool {
    let rest = value.strip_prefix(prefix.as_bytes()).unwrap_or(value);
    rest.iter().all(|&byte| byte == b'0' || byte == b'-')
}

fn contains_ignore_case(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window.eq_ignore_ascii_case(needle))
}

fn line_starts(data: &[u8]) -> impl Iterator<Item = usize> + '_ {
    std::iter::once(0).chain(memchr::memchr_iter(b'\n', data).map(|at| at + 1))
}

/// A trace id line keeps only its timestamp: `<digits>-<zeros>-<zeros>`.
fn trace_id_is_sanitised(line: &[u8]) -> bool {
    let value = line[TRACE_ID_HEADER.len()..].trim_ascii();
    let mut segments = value.split(|&byte| byte == b'-');
    let (Some(timestamp), Some(first), Some(second), None) = (
        segments.next(),
        segments.next(),
        segments.next(),
        segments.next(),
    ) else {
        return false;
    };
    !timestamp.is_empty()
        && timestamp.iter().all(u8::is_ascii_digit)
        && !first.is_empty()
        && first.iter().all(|&byte| byte == b'0')
        && !second.is_empty()
        && second.iter().all(|&byte| byte == b'0')
}

/// `prefix` followed by an alphanumeric byte and not preceded by one, so that
/// `sk-` in `task-list` does not count.
fn has_key_prefix(data: &[u8], prefix: &[u8]) -> Option<usize> {
    memmem::find_iter(data, prefix).find(|&at| {
        let before = at.checked_sub(1).map(|index| data[index]);
        let after = data.get(at + prefix.len()).copied();
        !before.is_some_and(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
            && after.is_some_and(|byte| byte.is_ascii_alphanumeric())
    })
}

/// Every violation in `data`, as `name:line: what`.
fn problems(name: &str, data: &[u8]) -> Vec<String> {
    let mut out = Vec::new();
    for key in ID_KEYS {
        for (at, value) in string_members(data, key) {
            if !value.is_some_and(is_placeholder) {
                out.push(format!(
                    "{name}:{}: `{key}` is not a placeholder",
                    line_of(data, at)
                ));
            }
        }
    }
    for (key, prefix) in ZEROED_KEYS {
        for (at, value) in string_members(data, key) {
            if !value.is_some_and(|value| is_zeroed(value, prefix)) {
                out.push(format!(
                    "{name}:{}: `{key}` is not zeroed",
                    line_of(data, at)
                ));
            }
        }
    }
    for start in line_starts(data) {
        let end = memchr::memchr(b'\n', &data[start..]).map_or(data.len(), |len| start + len);
        let line = &data[start..end];
        let starts_with = |prefix: &[u8]| {
            line.len() >= prefix.len() && line[..prefix.len()].eq_ignore_ascii_case(prefix)
        };
        if starts_with(TRACE_ID_HEADER) && !trace_id_is_sanitised(line) {
            out.push(format!(
                "{name}:{}: trace id keeps its hash segments",
                line_of(data, start)
            ));
        }
        for header in [&b"authorization:"[..], b"cookie:", b"set-cookie:"] {
            if starts_with(header) {
                out.push(format!(
                    "{name}:{}: credential header",
                    line_of(data, start)
                ));
            }
        }
    }
    for prefix in [&b"bk-"[..], b"sk-"] {
        if let Some(at) = has_key_prefix(data, prefix) {
            out.push(format!("{name}:{}: looks like a key", line_of(data, at)));
        }
    }
    for needle in [
        &b"bearer "[..],
        b"x-api-key",
        b"x-goog-api-key",
        b"?key=",
        b"&key=",
        b"auth_token=",
    ] {
        if let Some(at) = contains_ignore_case(data, needle) {
            out.push(format!("{name}:{}: credential trace", line_of(data, at)));
        }
    }
    out
}

#[test]
fn every_fixture_is_sanitised() {
    let found: Vec<String> = FIXTURES
        .iter()
        .flat_map(|(name, data)| problems(name, data))
        .collect();
    assert!(
        found.is_empty(),
        "unsanitised fixtures:\n{}",
        found.join("\n")
    );
}

#[test]
fn fixtures_keep_their_ids_as_placeholders() {
    // Guards against the check passing because the sanitiser deleted the
    // members instead of rewriting them.
    let (_, xhigh) = fixture!("chat-stream-grok46-xhigh.sse");
    let ids = string_members(xhigh, "id");
    assert_eq!(ids.len(), 15);
    assert!(
        ids.iter()
            .all(|(_, value)| *value == Some(&b"00000000-0000-0000-0000-000000000001"[..]))
    );
    let (_, responses) = fixture!("responses-stream-grok46-xhigh.sse");
    assert_eq!(string_members(responses, "prompt_cache_key").len(), 3);
    assert_eq!(string_members(responses, "system_fingerprint").len(), 3);
}

#[test]
fn headers_keep_crlf_line_endings() {
    // `.gitattributes` marks fixtures `-text`; without it git would rewrite
    // these to LF and break byte-level replays.
    for (name, data) in FIXTURES {
        if name.ends_with(".headers") {
            let lines = memchr::memchr_iter(b'\n', data).count();
            let crlf = memmem::find_iter(data, b"\r\n").count();
            assert!(
                lines > 0 && crlf == lines,
                "{name} lost its CRLF line endings"
            );
        }
    }
}

#[test]
fn the_checks_catch_unsanitised_content() {
    let cases: &[(&str, &[u8])] = &[
        (
            "uuid id",
            br#"{"id":"aa087779-9289-9cfb-8f52-4dfc33590f98"}"#,
        ),
        (
            "prefixed hex id",
            br#"{"id":"resp_04fa48bd2846b00a016aade20d66cc87d0a3576ebf0f02e58f"}"#,
        ),
        (
            "item id with whitespace",
            b"{\"item_id\" :\n \"rs_f5f1b988-c10e-9ce7-beae-93ce3cfe1bf5\"}",
        ),
        (
            "gemini response id",
            br#"{"responseId":"31d38a12-6d2c-97cf-aa2f-de99d438b420"}"#,
        ),
        ("escaped id", br#"{"id":"\u0061bc"}"#),
        (
            "cache key",
            br#"{"prompt_cache_key":"4f7c7ed7-3afc-5169-8f1f-bad75c178a20"}"#,
        ),
        (
            "safety identifier",
            br#"{"safety_identifier":"user-gFjMciMxaiEuiv08IP9PzGJx"}"#,
        ),
        (
            "fingerprint",
            br#"{"system_fingerprint":"fp_08d0bc26c22b024e"}"#,
        ),
        (
            "trace id",
            b"X-Cpa-Trace-Id: 20260919011838-c5c79277c98627c7-31459b28\r\n",
        ),
        ("sk key", b"{\"error\":\"key sk-proj-abc is invalid\"}"),
        ("bk key", b"bk-0123456789"),
        ("bearer", b"Authorization: Bearer abc\r\n"),
        ("x-api-key", b"X-Api-Key: abc\r\n"),
        ("goog key", b"x-goog-api-key: abc\r\n"),
        (
            "query key",
            b"/v1beta/models/m:generateContent?alt=sse&key=abc",
        ),
        ("cookie", b"Set-Cookie: session=abc\r\n"),
    ];
    for (what, data) in cases {
        assert!(!problems("sample", data).is_empty(), "missed: {what}");
    }

    let clean: &[(&str, &[u8])] = &[
        (
            "placeholders",
            br#"{"id":"resp_000001","item_id":"rs_00000000-0000-0000-0000-000000000002"}"#,
        ),
        ("null members", br#"{"id":null,"safety_identifier":null}"#),
        ("words ending in sk", b"task-list desk-lamp"),
        ("id as a value", br#"{"type":"id","x":1}"#),
    ];
    for (what, data) in clean {
        assert_eq!(problems("sample", data), Vec::<String>::new(), "{what}");
    }
}
