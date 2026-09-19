//! Property tests for `ChatHead::parse` against a `serde_json::Value`
//! oracle (contract 05, 4.2 `jsonhead_props`): random member order, random
//! whitespace, escaped keys and values, optional and `null` members, extra
//! members with nested `model` keys, and injected repeated or
//! fold-equivalent keys.

#![allow(
    clippy::disallowed_types,
    clippy::disallowed_macros,
    reason = "the differential oracle is a serde_json::Value tree by design (contract 05, 1.3)"
)]

mod common;

use brisk_proto::jsonhead::{ChatHead, HeadError};
use proptest::prelude::*;
use proptest::test_runner::TestCaseError;
use serde_json::Value;

use common::json::{Json, Rng, Style, json, model_name, request, style, write};
use common::oracle::verify_head;

/// A recognized member to repeat or to spell with folded case.
#[derive(Debug, Clone, Copy)]
enum Target {
    Model,
    Stream,
    StreamOptions,
    IncludeUsage,
}

impl Target {
    const fn field(self) -> &'static str {
        match self {
            Self::Model => "model",
            Self::Stream => "stream",
            Self::StreamOptions => "stream_options",
            Self::IncludeUsage => "include_usage",
        }
    }
}

fn target() -> impl Strategy<Value = Target> {
    prop_oneof![
        Just(Target::Model),
        Just(Target::Stream),
        Just(Target::StreamOptions),
        Just(Target::IncludeUsage),
    ]
}

/// A value of the type `target` accepts, so that only the repetition itself
/// can make the request invalid.
fn accepted_value(target: Target, choice: u8, name: String) -> Json {
    match (target, choice % 3) {
        (Target::Model, _) => Json::Str(name),
        (Target::StreamOptions, 0) => Json::Obj(Vec::new()),
        (_, 0) | (Target::StreamOptions, _) => Json::Null,
        (_, 1) => Json::Bool(true),
        (_, _) => Json::Bool(false),
    }
}

/// `field` with random letter case and, at random, `s` written as U+017F and
/// `i` as U+0131 or U+0130; never `field` itself.
fn fold_spelling(field: &str, seed: u64) -> String {
    let mut rng = Rng::new(seed);
    let mut spelled: String = field
        .chars()
        .map(|c| match c {
            's' if rng.one_in(3) => '\u{17f}',
            'i' if rng.one_in(3) => {
                if rng.one_in(2) {
                    '\u{131}'
                } else {
                    '\u{130}'
                }
            }
            c if rng.one_in(2) => c.to_ascii_uppercase(),
            c => c,
        })
        .collect();
    if spelled == field {
        spelled = field.to_ascii_uppercase();
    }
    spelled
}

/// Adds `member` to the object that holds `target`: the request itself, or
/// its `stream_options`, which is made an object first when it is not one.
/// The recognized member it collides with is added first if missing.
fn inject(
    mut members: Vec<(String, Json)>,
    target: Target,
    member: (String, Json),
    at: usize,
) -> Vec<(String, Json)> {
    let Target::IncludeUsage = target else {
        if !members.iter().any(|(key, _)| key == target.field()) {
            members.push((
                target.field().to_owned(),
                accepted_value(target, 1, "m".to_owned()),
            ));
        }
        let at = at % (members.len() + 1);
        members.insert(at, member);
        return members;
    };

    let position = members.iter().position(|(key, _)| key == "stream_options");
    let mut options = match position.map(|index| members.remove(index).1) {
        Some(Json::Obj(options)) => options,
        _ => Vec::new(),
    };
    if !options.iter().any(|(key, _)| key == "include_usage") {
        options.push(("include_usage".to_owned(), Json::Bool(false)));
    }
    let at = at % (options.len() + 1);
    options.insert(at, member);
    members.push(("stream_options".to_owned(), Json::Obj(options)));
    members
}

fn fail(message: String) -> TestCaseError {
    TestCaseError::fail(message)
}

fn body_text(body: &[u8]) -> String {
    String::from_utf8_lossy(body).into_owned()
}

proptest! {
    /// Any generated request, valid or not: `ChatHead` agrees with the oracle
    /// on the outcome, the decoded values and every span.
    #[test]
    fn agrees_with_the_value_oracle(request in request(false), style in style()) {
        let body = request.body(style);
        prop_assert!(serde_json::from_slice::<Value>(&body).is_ok(), "generated invalid JSON");
        verify_head(&body).map_err(fail)?;
    }

    /// Without repeated or fold-equivalent keys, a well-typed request is
    /// always accepted, whatever the layout and escapes.
    #[test]
    fn well_typed_requests_are_accepted(request in request(true), style in style()) {
        let body = request.body(style);
        prop_assert!(serde_json::from_slice::<Value>(&body).is_ok(), "generated invalid JSON");
        if let Err(error) = ChatHead::parse(&body) {
            return Err(fail(format!("rejected ({error}): {}", body_text(&body))));
        }
        verify_head(&body).map_err(fail)?;
    }

    /// Repeating a recognized member, whether or not either key is written
    /// with escapes, is a JSON error, although `Value` accepts the body.
    #[test]
    fn a_repeated_member_is_a_json_error(
        request in request(true),
        target in target(),
        choice in any::<u8>(),
        name in model_name(),
        at in any::<usize>(),
        style in style(),
    ) {
        let value = accepted_value(target, choice, name);
        let members = inject(request.members(), target, (target.field().to_owned(), value), at);
        let body = write(&Json::Obj(members), style);
        prop_assert!(serde_json::from_slice::<Value>(&body).is_ok(), "generated invalid JSON");
        match ChatHead::parse(&body) {
            Err(HeadError::Json(_)) => {}
            other => return Err(fail(format!("{other:?} for {}", body_text(&body)))),
        }
        verify_head(&body).map_err(fail)?;
    }

    /// A key equal to a recognized member only under case folding is
    /// ambiguous wherever it appears, before or after the member itself, and
    /// the error names the member, never the client's key.
    #[test]
    fn a_fold_equivalent_key_is_ambiguous(
        request in request(true),
        target in target(),
        seed in any::<u64>(),
        value in json(),
        at in any::<usize>(),
        style in style(),
    ) {
        let key = fold_spelling(target.field(), seed);
        let members = inject(request.members(), target, (key.clone(), value), at);
        let body = write(&Json::Obj(members), style);
        prop_assert!(serde_json::from_slice::<Value>(&body).is_ok(), "generated invalid JSON");
        match ChatHead::parse(&body) {
            Err(error @ HeadError::AmbiguousKey(field)) => {
                prop_assert_eq!(field, target.field());
                prop_assert!(!error.to_string().contains(&key), "{} echoes {}", error, key);
            }
            other => return Err(fail(format!("{other:?} for {}", body_text(&body)))),
        }
        verify_head(&body).map_err(fail)?;
    }
}

#[test]
fn fold_spelling_never_returns_the_field() {
    for seed in 0..512 {
        for field in ["model", "stream", "stream_options", "include_usage"] {
            let spelled = fold_spelling(field, seed);
            assert_ne!(spelled, field);
            assert!(common::oracle::fold_equivalent(&spelled, field));
        }
    }
}

#[test]
fn compact_bracketed_model_round_trips() {
    let body = write(
        &Json::Obj(vec![(
            "model".to_owned(),
            Json::Str(common::TEST_MODEL.to_owned()),
        )]),
        Style::COMPACT,
    );
    let head = ChatHead::parse(&body).unwrap();
    assert_eq!(head.model_name, common::TEST_MODEL);
    verify_head(&body).unwrap();
}

/// The fuzz seeds are meaningful starting points only if the oracle and
/// `ChatHead` already agree on them.
#[test]
fn fuzz_seeds_agree_with_the_oracle() {
    macro_rules! seed {
        ($name:literal) => {
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/fuzz/seeds/head/",
                $name
            ))
            .as_slice()
        };
    }
    for body in [
        seed!("basic.json"),
        seed!("escaped.json"),
        seed!("spaced.json"),
        seed!("folded.json"),
    ] {
        verify_head(body).unwrap();
    }
    assert!(matches!(
        ChatHead::parse(seed!("folded.json")),
        Err(HeadError::AmbiguousKey("stream"))
    ));
    assert_eq!(
        ChatHead::parse(seed!("escaped.json")).unwrap().model_name,
        common::TEST_MODEL
    );
}
