//! Usage, `finish_reason` and top-level error detection in streamed
//! `chat.completion.chunk` payloads and non-streaming `chat.completion`
//! bodies.
//!
//! Byte prefilters select candidate payloads, so only the few that can carry
//! these facts are parsed, and only their top-level members are decoded (R8).
//!
//! Both usage shapes are final values: the `OpenAI` one arrives in its own
//! chunk after the finish, CPA's shares the chunk that carries
//! `finish_reason` (CPA-M1-6), so a chunk can be `finished` and carry usage
//! at once and is then never `usage_only`.

use std::fmt;
use std::marker::PhantomData;
use std::sync::LazyLock;

use memchr::memmem::Finder;
use serde::Deserialize;
use serde::de::value::MapAccessDeserializer;
use serde::de::{Deserializer, IgnoredAny, MapAccess, SeqAccess, Visitor};

use crate::{UsageErrorKind, UsageTokens};

// Built once: constructing a searcher per call would cost more than the
// search itself on the short payloads of a chunk.
static USAGE_KEY: LazyLock<Finder<'static>> = LazyLock::new(|| Finder::new(br#""usage""#));
static FINISH_KEY: LazyLock<Finder<'static>> = LazyLock::new(|| Finder::new(br#""finish_reason""#));
static ERROR_KEY: LazyLock<Finder<'static>> = LazyLock::new(|| Finder::new(br#""error""#));

/// Byte prefilters (R8). False positives are allowed; false negatives are not,
/// for compact JSON and for JSON whitespace between the tokens.
/// `"usage"` ws* `:` ws* `{`
pub fn may_carry_usage(payload: &[u8]) -> bool {
    key_then(payload, &USAGE_KEY, |byte| byte == b'{')
}

/// `"finish_reason"` ws* `:` ws* `"`
pub fn may_carry_finish(payload: &[u8]) -> bool {
    key_then(payload, &FINISH_KEY, |byte| byte == b'"')
}

/// `"error"` ws* `:` ws* (`{` | `"`)
pub fn may_carry_error(payload: &[u8]) -> bool {
    key_then(payload, &ERROR_KEY, |byte| byte == b'{' || byte == b'"')
}

/// Some occurrence of the quoted key is followed by a colon and a value that
/// starts with a byte `opens` accepts.
fn key_then(payload: &[u8], key: &Finder<'_>, opens: impl Fn(u8) -> bool) -> bool {
    key.find_iter(payload).any(|at| {
        let after_key = &payload[at + key.needle().len()..];
        match skip_whitespace(after_key) {
            [b':', value @ ..] => skip_whitespace(value).first().copied().is_some_and(&opens),
            _ => false,
        }
    })
}

fn skip_whitespace(bytes: &[u8]) -> &[u8] {
    let start = bytes
        .iter()
        .position(|&byte| !matches!(byte, b' ' | b'\n' | b'\t' | b'\r'))
        .unwrap_or(bytes.len());
    &bytes[start..]
}

/// Facts about one `chat.completion.chunk` payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ChunkFacts {
    /// Top-level `usage`, when present and not null.
    pub usage: Option<UsageTokens>,
    /// Some choice has a non-null `finish_reason`.
    pub finished: bool,
    /// `usage` present, `choices` absent or `[]`, no `error`: safe to strip.
    pub usage_only: bool,
    /// Top-level `error` present and not null.
    pub error: bool,
}

/// Facts about a non-streaming `chat.completion` body.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct CompletionFacts {
    /// Top-level `usage`, when present and not null.
    pub usage: Option<UsageTokens>,
    /// Top-level `error` present and not null (D22).
    pub error: bool,
}

/// Parses a candidate chunk payload (one that a prefilter selected).
pub fn parse_chunk(payload: &[u8]) -> Result<ChunkFacts, UsageError> {
    let mut de = serde_json::Deserializer::from_slice(payload);
    let Object(chunk) = Object::<ChunkWire>::deserialize(&mut de).map_err(UsageError::Json)?;
    de.end().map_err(UsageError::Json)?;

    let usage = chunk
        .usage
        .map(|Object(usage)| usage.tokens())
        .transpose()?;
    let error = chunk.error.is_some();
    let (finished, no_choices) = chunk
        .choices
        .map_or((false, true), |choices| (choices.finished, choices.empty));
    Ok(ChunkFacts {
        usage,
        finished,
        usage_only: usage.is_some() && no_choices && !error,
        error,
    })
}

/// A body held in one contiguous buffer.
pub fn parse_completion(body: &[u8]) -> Result<CompletionFacts, UsageError> {
    let mut de = serde_json::Deserializer::from_slice(body);
    completion(&mut de)
}

/// A body held in several frames, read without concatenating them first.
pub fn parse_completion_from<R: std::io::Read>(reader: R) -> Result<CompletionFacts, UsageError> {
    let mut de = serde_json::Deserializer::from_reader(reader);
    completion(&mut de)
}

fn completion<'de, R>(de: &mut serde_json::Deserializer<R>) -> Result<CompletionFacts, UsageError>
where
    R: serde_json::de::Read<'de>,
{
    let Object(body) = Object::<CompletionWire>::deserialize(&mut *de).map_err(UsageError::Json)?;
    de.end().map_err(UsageError::Json)?;
    Ok(CompletionFacts {
        usage: body.usage.map(|Object(usage)| usage.tokens()).transpose()?,
        error: body.error.is_some(),
    })
}

/// Keeps the most recent usage (R8: overwrite, never sum).
#[derive(Debug, Clone, Copy, Default)]
pub struct UsageAcc {
    last: Option<UsageTokens>,
}

impl UsageAcc {
    /// Records `usage`, replacing any earlier one.
    pub fn observe(&mut self, usage: UsageTokens) {
        self.last = Some(usage);
    }

    /// The most recent usage observed.
    pub fn get(&self) -> Option<UsageTokens> {
        self.last
    }
}

/// Why a usage candidate was rejected.
#[derive(Debug, thiserror::Error)]
pub enum UsageError {
    /// Malformed JSON, a wrong type, a fraction, a negative number or a
    /// duplicate key.
    #[error("invalid chunk JSON: {0}")]
    Json(#[source] serde_json::Error),
    /// A required count is absent (or `null`).
    #[error("usage lacks `{0}`")]
    MissingField(&'static str),
}

impl UsageError {
    /// Payload-free summary carried in `Outcome::usage_error`.
    pub fn kind(&self) -> UsageErrorKind {
        match self {
            Self::Json(error) => UsageErrorKind::Json {
                category: error.classify(),
                column: error.column(),
            },
            Self::MissingField(field) => UsageErrorKind::MissingField(field),
        }
    }
}

/// `T`, accepted from a JSON object only. A derived struct deserializer also
/// accepts an array of its fields in order, which would read `[213,71]` as a
/// usage or let `[{"usage":{...}}]` pass a nested object off as the top
/// level.
struct Object<T>(T);

impl<'de, T: Deserialize<'de>> Deserialize<'de> for Object<T> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer
            .deserialize_map(ObjectOnly(PhantomData))
            .map(Object)
    }
}

struct ObjectOnly<T>(PhantomData<T>);

impl<'de, T: Deserialize<'de>> Visitor<'de> for ObjectOnly<T> {
    type Value = T;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a JSON object")
    }

    fn visit_map<A>(self, map: A) -> Result<T, A::Error>
    where
        A: MapAccess<'de>,
    {
        T::deserialize(MapAccessDeserializer::new(map))
    }
}

/// The top-level members of a chunk that matter; all others are skipped
/// without being decoded.
#[derive(Deserialize)]
struct ChunkWire {
    #[serde(default)]
    usage: Option<Object<UsageWire>>,
    #[serde(default)]
    choices: Option<Choices>,
    /// `Option<IgnoredAny>`: `null` and absence are `None`, any other value
    /// `Some`, without decoding a string that may hold escapes.
    #[serde(default)]
    error: Option<IgnoredAny>,
}

#[derive(Deserialize)]
struct CompletionWire {
    #[serde(default)]
    usage: Option<Object<UsageWire>>,
    #[serde(default)]
    error: Option<IgnoredAny>,
}

/// The counts are optional here so that a missing one is reported as
/// [`UsageError::MissingField`] rather than as a generic JSON error.
/// `total_tokens` is not declared: it is not trusted.
#[derive(Deserialize)]
struct UsageWire {
    #[serde(default)]
    prompt_tokens: Option<u64>,
    #[serde(default)]
    completion_tokens: Option<u64>,
    #[serde(default)]
    prompt_tokens_details: Option<Object<PromptDetails>>,
    #[serde(default)]
    completion_tokens_details: Option<Object<CompletionDetails>>,
}

#[derive(Deserialize)]
struct PromptDetails {
    #[serde(default)]
    cached_tokens: Option<u64>,
}

#[derive(Deserialize)]
struct CompletionDetails {
    #[serde(default)]
    reasoning_tokens: Option<u64>,
}

impl UsageWire {
    fn tokens(self) -> Result<UsageTokens, UsageError> {
        Ok(UsageTokens {
            input: self
                .prompt_tokens
                .ok_or(UsageError::MissingField("prompt_tokens"))?,
            output: self
                .completion_tokens
                .ok_or(UsageError::MissingField("completion_tokens"))?,
            cached_input: self
                .prompt_tokens_details
                .and_then(|Object(details)| details.cached_tokens),
            reasoning_output: self
                .completion_tokens_details
                .and_then(|Object(details)| details.reasoning_tokens),
        })
    }
}

/// `choices`, reduced while it is read to the two facts settlement needs, so
/// that no `Vec` is built.
struct Choices {
    empty: bool,
    finished: bool,
}

#[derive(Deserialize)]
struct Choice {
    #[serde(default)]
    finish_reason: Option<IgnoredAny>,
}

impl<'de> Deserialize<'de> for Choices {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_seq(ChoicesVisitor)
    }
}

struct ChoicesVisitor;

impl<'de> Visitor<'de> for ChoicesVisitor {
    type Value = Choices;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("an array of choices")
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Choices, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut choices = Choices {
            empty: true,
            finished: false,
        };
        while let Some(Object(choice)) = seq.next_element::<Object<Choice>>()? {
            choices.empty = false;
            choices.finished |= choice.finish_reason.is_some();
        }
        Ok(choices)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prefilters_need_the_value_to_open() {
        assert!(may_carry_usage(br#"{"usage":{"prompt_tokens":1}}"#));
        assert!(may_carry_usage(b"{\"usage\" :\n\t{ }}"));
        assert!(!may_carry_usage(br#"{"usage":null}"#));
        assert!(!may_carry_usage(br#"{"usage":"x"}"#));
        assert!(!may_carry_usage(br#"{"usage"}"#));
        assert!(may_carry_usage(br#"{"usage":null,"x":{"usage":{}}}"#));

        assert!(may_carry_finish(
            br#"{"choices":[{"finish_reason":"stop"}]}"#
        ));
        assert!(may_carry_finish(br#"{"finish_reason" : "length"}"#));
        assert!(!may_carry_finish(
            br#"{"choices":[{"finish_reason":null}]}"#
        ));

        assert!(may_carry_error(br#"{"error":{"message":"x"}}"#));
        assert!(may_carry_error(br#"{"error":"Invalid API key"}"#));
        assert!(!may_carry_error(br#"{"error":null}"#));
        assert!(!may_carry_error(br#"{"type":"error"}"#));
    }

    #[test]
    fn acc_keeps_the_last_usage() {
        let mut acc = UsageAcc::default();
        assert_eq!(acc.get(), None);
        let first = UsageTokens {
            input: 7,
            output: 0,
            cached_input: None,
            reasoning_output: None,
        };
        let last = UsageTokens {
            input: 307,
            output: 5,
            cached_input: Some(0),
            reasoning_output: None,
        };
        acc.observe(first);
        acc.observe(last);
        assert_eq!(acc.get(), Some(last));
    }

    #[test]
    fn kind_carries_no_payload_text() {
        let error =
            parse_chunk(br#"{"usage":{"prompt_tokens":"secret-text","completion_tokens":1}}"#)
                .unwrap_err();
        assert!(error.to_string().contains("secret-text"));
        let kind = error.kind();
        assert!(matches!(
            kind,
            UsageErrorKind::Json {
                category: serde_json::error::Category::Data,
                ..
            }
        ));
        assert!(!format!("{kind:?}").contains("secret-text"));

        let missing = parse_chunk(br#"{"usage":{"completion_tokens":1}}"#).unwrap_err();
        assert_eq!(
            missing.kind(),
            UsageErrorKind::MissingField("prompt_tokens")
        );
    }

    #[test]
    fn arrays_are_not_objects() {
        assert!(matches!(
            parse_chunk(br#"[{"usage":{"prompt_tokens":1,"completion_tokens":1}}]"#),
            Err(UsageError::Json(_))
        ));
        assert!(matches!(
            parse_completion(br#"[{"prompt_tokens":1,"completion_tokens":1}]"#),
            Err(UsageError::Json(_))
        ));
    }
}
