//! Allocation gate for the per-request public functions (sections 4.7 and
//! 7.4), on a 2 KiB reference request for `grok-4.6(xhigh)` without escapes:
//! removing credential query parameters when there are none, authenticating
//! the key and parsing the head allocate nothing; planning the rewrite
//! allocates nothing in release builds and at most once in debug builds,
//! where `plan_chat` checks its output (1.3.3, invariant 4).

use brisk_gateway::auth::{
    KEY_RANDOM_BYTES, KeyTable, format_key, key_digest, strip_credential_query,
};
use brisk_gateway::spec::KeySpec;
use brisk_proto::jsonhead::ChatHead;
use brisk_proto::splice::{Rewrite, json_string, plan_chat};
use bytes::Bytes;
use http::Uri;
use http::header::{AUTHORIZATION, CONTENT_TYPE, HeaderMap, HeaderValue, USER_AGENT};

#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

const TEST_MODEL: &str = "grok-4.6(xhigh)";
const REQUEST_BYTES: usize = 2048;

/// A streaming chat request of exactly [`REQUEST_BYTES`] bytes without any
/// escape, padded in the prompt.
fn reference_body() -> Bytes {
    let head = format!(r#"{{"model":"{TEST_MODEL}","messages":[{{"role":"user","content":""#);
    let tail = r#""}],"stream":true}"#;
    let padding = REQUEST_BYTES - head.len() - tail.len();
    let body = format!("{head}{}{tail}", "p".repeat(padding));
    assert_eq!(body.len(), REQUEST_BYTES);
    let body = Bytes::from(body);
    // A `Bytes` made from a `Vec` allocates its shared header on the first
    // clone or slice; do that now, outside the counted calls.
    drop(body.slice(1..2));
    body
}

/// `total_blocks` allocated while `f` runs.
fn blocks<T>(f: impl FnOnce() -> T) -> (u64, T) {
    let before = dhat::HeapStats::get().total_blocks;
    let value = f();
    let after = dhat::HeapStats::get().total_blocks;
    (after - before, value)
}

#[test]
fn per_request_functions_stay_off_the_heap() {
    // Every input is built before the profiler starts.
    let body = reference_body();
    let uri: Uri = "/v1/chat/completions?alt=sse&trace=1"
        .parse()
        .expect("a valid URI");
    let key = format_key(&[42_u8; KEY_RANDOM_BYTES]).into_inner();
    let table = KeyTable::build(&[KeySpec {
        name: String::from("dhat"),
        sha256: key_digest(key.as_bytes()),
    }])
    .expect("one key");
    let mut headers = HeaderMap::new();
    headers.insert(
        AUTHORIZATION,
        HeaderValue::from_str(&format!("Bearer {key}")).expect("a valid header value"),
    );
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    headers.insert(USER_AGENT, HeaderValue::from_static("OpenAI/Python 2.1.0"));
    let mapped = json_string("grok-4.6-xhigh-upstream");

    let profiler = dhat::Profiler::builder().testing().build();

    let (strip_blocks, stripped) = blocks(|| strip_credential_query(&uri));
    let (auth_blocks, authenticated) =
        blocks(|| table.authenticate(&headers).map(|entry| entry.id));
    let (parse_blocks, parsed) = blocks(|| ChatHead::parse(&body));
    let head = parsed.expect("the reference request parses");
    let (inject_blocks, injected) = blocks(|| {
        plan_chat(
            &body,
            &head,
            Rewrite {
                model: None,
                inject_include_usage: true,
            },
        )
    });
    let (map_blocks, remapped) = blocks(|| {
        plan_chat(
            &body,
            &head,
            Rewrite {
                model: Some(&mapped),
                inject_include_usage: false,
            },
        )
    });

    println!(
        "dhat_request: strip_credential_query {strip_blocks}, authenticate {auth_blocks}, \
         ChatHead::parse {parse_blocks}, plan_chat inject {inject_blocks}, \
         plan_chat model_map {map_blocks} blocks"
    );

    // The calls did their work, so the counts describe the real paths.
    assert!(stripped.is_none(), "no credential parameter to remove");
    assert!(authenticated.is_ok(), "the key authenticates");
    assert!(!head.requests_usage());
    assert_eq!(&*head.model_name, TEST_MODEL);
    let injected = injected.expect("injection plans");
    let remapped = remapped.expect("the model mapping plans");
    assert!(!injected.is_identity());
    assert!(!remapped.is_identity());

    let plan_limit = u64::from(cfg!(debug_assertions));
    dhat::assert_eq!(strip_blocks, 0);
    dhat::assert_eq!(auth_blocks, 0);
    dhat::assert_eq!(parse_blocks, 0);
    dhat::assert!(inject_blocks <= plan_limit, "{inject_blocks} blocks");
    dhat::assert!(map_blocks <= plan_limit, "{map_blocks} blocks");
    drop(profiler);
}
