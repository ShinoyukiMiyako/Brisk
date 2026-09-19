//! Property tests for the usage prefilters and extraction (contract 05, 4.2
//! `usage_props`): a `usage`, `finish_reason` or `error` member placed at a
//! random position with random JSON whitespace around its colon is never
//! missed by the prefilter, and generated chunks and completions yield
//! exactly the facts their structure implies.

mod common;

use std::io::{self, Read};

use brisk_proto::UsageTokens;
use brisk_proto::usage::{
    ChunkFacts, CompletionFacts, may_carry_error, may_carry_finish, may_carry_usage, parse_chunk,
    parse_completion, parse_completion_from,
};
use proptest::collection::vec;
use proptest::option;
use proptest::prelude::*;
use proptest::sample::Index;
use proptest::test_runner::TestCaseError;

use common::TEST_MODEL;
use common::json::{Json, Rng, Style, text, write};

fn whitespace() -> impl Strategy<Value = Vec<u8>> {
    vec(
        prop_oneof![Just(b' '), Just(b'\n'), Just(b'\t'), Just(b'\r')],
        0..4,
    )
}

/// What may follow `"error":`.
fn error_opener() -> impl Strategy<Value = u8> {
    prop_oneof![Just(b'{'), Just(b'"')]
}

/// `before` + `"key"` ws `:` ws `opener` + `after`.
fn embed(before: &[u8], key: &str, ws: (&[u8], &[u8]), opener: u8, after: &[u8]) -> Vec<u8> {
    let mut payload = before.to_vec();
    payload.push(b'"');
    payload.extend_from_slice(key.as_bytes());
    payload.push(b'"');
    payload.extend_from_slice(ws.0);
    payload.push(b':');
    payload.extend_from_slice(ws.1);
    payload.push(opener);
    payload.extend_from_slice(after);
    payload
}

proptest! {
    #[test]
    fn prefilters_find_an_embedded_member(
        before in vec(any::<u8>(), 0..48),
        after in vec(any::<u8>(), 0..48),
        ws_before in whitespace(),
        ws_after in whitespace(),
        error_opener in error_opener(),
    ) {
        let ws = (&ws_before[..], &ws_after[..]);
        let usage = embed(&before, "usage", ws, b'{', &after);
        prop_assert!(may_carry_usage(&usage));
        prop_assert!(may_carry_finish(&embed(&before, "finish_reason", ws, b'"', &after)));
        prop_assert!(may_carry_error(&embed(&before, "error", ws, error_opener, &after)));
    }
}

/// A count, or the optional details objects around one.
#[derive(Debug, Clone)]
enum Detail {
    Absent,
    Null,
    /// The details object with the count `null` or a number.
    Object(Option<u64>),
}

fn detail() -> impl Strategy<Value = Detail> {
    prop_oneof![
        Just(Detail::Absent),
        Just(Detail::Null),
        option::of(any::<u64>()).prop_map(Detail::Object),
    ]
}

#[derive(Debug, Clone)]
struct Usage {
    prompt: u64,
    completion: u64,
    total: Option<u64>,
    cached: Detail,
    reasoning: Detail,
}

impl Usage {
    fn tokens(&self) -> UsageTokens {
        let count = |detail: &Detail| match detail {
            Detail::Object(count) => *count,
            Detail::Absent | Detail::Null => None,
        };
        UsageTokens {
            input: self.prompt,
            output: self.completion,
            cached_input: count(&self.cached),
            reasoning_output: count(&self.reasoning),
        }
    }

    fn json(&self) -> Json {
        let number = |n: u64| Json::Num(n.to_string());
        let mut members = vec![
            ("prompt_tokens".to_owned(), number(self.prompt)),
            ("completion_tokens".to_owned(), number(self.completion)),
        ];
        if let Some(total) = self.total {
            members.push(("total_tokens".to_owned(), number(total)));
        }
        for (name, inner, detail) in [
            ("prompt_tokens_details", "cached_tokens", &self.cached),
            (
                "completion_tokens_details",
                "reasoning_tokens",
                &self.reasoning,
            ),
        ] {
            match detail {
                Detail::Absent => {}
                Detail::Null => members.push((name.to_owned(), Json::Null)),
                Detail::Object(count) => members.push((
                    name.to_owned(),
                    Json::Obj(vec![(inner.to_owned(), count.map_or(Json::Null, number))]),
                )),
            }
        }
        Json::Obj(members)
    }
}

fn usage() -> impl Strategy<Value = Usage> {
    (
        any::<u64>(),
        any::<u64>(),
        option::of(any::<u64>()),
        detail(),
        detail(),
    )
        .prop_map(|(prompt, completion, total, cached, reasoning)| Usage {
            prompt,
            completion,
            total,
            cached,
            reasoning,
        })
}

/// The top-level `usage` member.
#[derive(Debug, Clone)]
enum UsageMember {
    Absent,
    Null,
    Object(Usage),
}

fn usage_member() -> impl Strategy<Value = UsageMember> {
    prop_oneof![
        1 => Just(UsageMember::Absent),
        1 => Just(UsageMember::Null),
        3 => usage().prop_map(UsageMember::Object),
    ]
}

/// `finish_reason` of one choice.
#[derive(Debug, Clone)]
enum Finish {
    Absent,
    Null,
    Str(String),
    /// A non-string, non-null value: still a finish.
    Other(Json),
}

fn finish() -> impl Strategy<Value = Finish> {
    prop_oneof![
        2 => Just(Finish::Absent),
        2 => Just(Finish::Null),
        2 => prop_oneof![Just("stop".to_owned()), text()].prop_map(Finish::Str),
        1 => prop_oneof![
            Just(Json::Num("1".to_owned())),
            Just(Json::Bool(true)),
            Just(Json::Obj(Vec::new())),
        ]
        .prop_map(Finish::Other),
    ]
}

#[derive(Debug, Clone)]
enum ErrorMember {
    Absent,
    Null,
    Str(String),
    Obj(String),
}

impl ErrorMember {
    fn present(&self) -> bool {
        matches!(self, Self::Str(_) | Self::Obj(_))
    }
}

fn error_member() -> impl Strategy<Value = ErrorMember> {
    prop_oneof![
        3 => Just(ErrorMember::Absent),
        1 => Just(ErrorMember::Null),
        1 => text().prop_map(ErrorMember::Str),
        1 => text().prop_map(ErrorMember::Obj),
    ]
}

/// `choices`: absent, `null`, or an array of choices.
#[derive(Debug, Clone)]
enum Choices {
    Absent,
    Null,
    List(Vec<(String, Finish)>),
}

fn choices() -> impl Strategy<Value = Choices> {
    prop_oneof![
        1 => Just(Choices::Absent),
        1 => Just(Choices::Null),
        4 => vec((text(), finish()), 0..3).prop_map(Choices::List),
    ]
}

/// A generated `chat.completion.chunk` (or, read as such, a completion).
#[derive(Debug, Clone)]
struct Chunk {
    usage: UsageMember,
    choices: Choices,
    error: ErrorMember,
    /// A nested object holding a `usage` key, which must not count.
    decoy: bool,
    order: u64,
}

impl Chunk {
    fn json(&self) -> Json {
        let mut members = vec![
            ("id".to_owned(), Json::Str("chatcmpl-0".to_owned())),
            (
                "object".to_owned(),
                Json::Str("chat.completion.chunk".to_owned()),
            ),
            ("model".to_owned(), Json::Str(TEST_MODEL.to_owned())),
        ];
        match &self.usage {
            UsageMember::Absent => {}
            UsageMember::Null => members.push(("usage".to_owned(), Json::Null)),
            UsageMember::Object(usage) => members.push(("usage".to_owned(), usage.json())),
        }
        match &self.choices {
            Choices::Absent => {}
            Choices::Null => members.push(("choices".to_owned(), Json::Null)),
            Choices::List(list) => {
                let items = list
                    .iter()
                    .enumerate()
                    .map(|(index, (content, finish))| {
                        let mut choice = vec![
                            ("index".to_owned(), Json::Num(index.to_string())),
                            (
                                "delta".to_owned(),
                                Json::Obj(vec![
                                    ("content".to_owned(), Json::Str(content.clone())),
                                    (
                                        "reasoning_content".to_owned(),
                                        Json::Str(format!("say \"hi\"\n{content}")),
                                    ),
                                ]),
                            ),
                        ];
                        match finish {
                            Finish::Absent => {}
                            Finish::Null => choice.push(("finish_reason".to_owned(), Json::Null)),
                            Finish::Str(reason) => {
                                choice
                                    .push(("finish_reason".to_owned(), Json::Str(reason.clone())));
                            }
                            Finish::Other(value) => {
                                choice.push(("finish_reason".to_owned(), value.clone()));
                            }
                        }
                        Json::Obj(choice)
                    })
                    .collect();
                members.push(("choices".to_owned(), Json::Arr(items)));
            }
        }
        match &self.error {
            ErrorMember::Absent => {}
            ErrorMember::Null => members.push(("error".to_owned(), Json::Null)),
            ErrorMember::Str(message) => {
                members.push(("error".to_owned(), Json::Str(message.clone())));
            }
            ErrorMember::Obj(message) => members.push((
                "error".to_owned(),
                Json::Obj(vec![("message".to_owned(), Json::Str(message.clone()))]),
            )),
        }
        if self.decoy {
            members.push((
                "attribution".to_owned(),
                Json::Obj(vec![(
                    "usage".to_owned(),
                    Json::Obj(vec![(
                        "prompt_tokens".to_owned(),
                        Json::Num("7".to_owned()),
                    )]),
                )]),
            ));
        }
        Rng::new(self.order).shuffle(&mut members);
        Json::Obj(members)
    }

    fn usage_tokens(&self) -> Option<UsageTokens> {
        match &self.usage {
            UsageMember::Object(usage) => Some(usage.tokens()),
            UsageMember::Absent | UsageMember::Null => None,
        }
    }

    fn facts(&self) -> ChunkFacts {
        let usage = self.usage_tokens();
        let error = self.error.present();
        let (finished, no_choices) = match &self.choices {
            Choices::Absent | Choices::Null => (false, true),
            Choices::List(list) => (
                list.iter()
                    .any(|(_, finish)| matches!(finish, Finish::Str(_) | Finish::Other(_))),
                list.is_empty(),
            ),
        };
        ChunkFacts {
            usage,
            finished,
            usage_only: usage.is_some() && no_choices && !error,
            error,
        }
    }

    fn has_string_finish(&self) -> bool {
        matches!(&self.choices, Choices::List(list)
            if list.iter().any(|(_, finish)| matches!(finish, Finish::Str(_))))
    }
}

fn chunk() -> impl Strategy<Value = Chunk> {
    (
        usage_member(),
        choices(),
        error_member(),
        any::<bool>(),
        any::<u64>(),
    )
        .prop_map(|(usage, choices, error, decoy, order)| Chunk {
            usage,
            choices,
            error,
            decoy,
            order,
        })
}

/// Chunk layouts: random whitespace and escaped string values, but keys
/// written plainly; the prefilters promise nothing for escaped keys.
fn layout() -> impl Strategy<Value = Style> {
    (any::<u64>(), any::<bool>(), any::<bool>()).prop_map(|(seed, whitespace, escape_strings)| {
        Style {
            seed,
            whitespace,
            escape_keys: false,
            escape_strings,
        }
    })
}

/// Hands out `parts` one per `read` call, like body frames.
struct Frames {
    parts: Vec<Vec<u8>>,
    next: usize,
    offset: usize,
}

impl Read for Frames {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        while let Some(part) = self.parts.get(self.next) {
            let rest = &part[self.offset..];
            if rest.is_empty() {
                self.next += 1;
                self.offset = 0;
                continue;
            }
            let n = rest.len().min(buf.len());
            buf[..n].copy_from_slice(&rest[..n]);
            self.offset += n;
            return Ok(n);
        }
        Ok(0)
    }
}

proptest! {
    #[test]
    fn chunk_facts_follow_the_structure(chunk in chunk(), style in layout()) {
        let payload = write(&chunk.json(), style);
        let facts = parse_chunk(&payload).map_err(|error| {
            TestCaseError::fail(format!("{error}: {}", String::from_utf8_lossy(&payload)))
        })?;
        prop_assert_eq!(facts, chunk.facts());

        // No false negatives on anything a chunk can carry.
        if facts.usage.is_some() {
            prop_assert!(may_carry_usage(&payload));
        }
        if chunk.has_string_finish() {
            prop_assert!(may_carry_finish(&payload));
        }
        if facts.error {
            prop_assert!(may_carry_error(&payload));
        }
    }

    #[test]
    fn completion_facts_do_not_depend_on_framing(
        chunk in chunk(),
        style in layout(),
        picks in vec(any::<Index>(), 0..8),
    ) {
        let body = write(&chunk.json(), style);
        let whole = parse_completion(&body).map_err(|error| TestCaseError::fail(error.to_string()))?;
        prop_assert_eq!(
            whole,
            CompletionFacts {
                usage: chunk.usage_tokens(),
                error: chunk.error.present(),
            }
        );

        let mut cuts: Vec<usize> = picks.iter().map(|pick| pick.index(body.len() + 1)).collect();
        cuts.sort_unstable();
        let mut parts = Vec::new();
        let mut start = 0;
        for cut in cuts.into_iter().chain(std::iter::once(body.len())) {
            parts.push(body[start..cut].to_vec());
            start = cut;
        }
        let framed = parse_completion_from(Frames { parts, next: 0, offset: 0 })
            .map_err(|error| TestCaseError::fail(error.to_string()))?;
        prop_assert_eq!(framed, whole);
    }
}
