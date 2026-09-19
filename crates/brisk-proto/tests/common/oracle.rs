//! Differential oracle for `ChatHead::parse` and `plan_chat`, built on
//! `serde_json::Value` (contract 05, 4.2 `jsonhead_props` and `splice_props`,
//! 4.8 `head` and `splice`).
//!
//! The reference reads the body into a tree whose objects keep every member
//! in document order, duplicates included, so the rules `ChatHead` applies on
//! top of plain JSON (a repeated recognized member is an error, a key equal to
//! one under Go's case folding is ambiguous) can be derived independently of
//! the crate's own key classification.

#![allow(
    clippy::disallowed_types,
    clippy::disallowed_macros,
    reason = "the differential oracle is a serde_json::Value tree by design (contract 05, 1.3)"
)]

use std::fmt;

use brisk_proto::jsonhead::{ChatHead, HeadError, IncludeUsage, StreamOptions};
use brisk_proto::splice::{Rewrite, SpliceError, json_string, plan_chat};
use bytes::Bytes;
use serde::de::{Deserialize, Deserializer, MapAccess, SeqAccess, Visitor};
use serde_json::Value;

/// Recognized members of the request object.
pub(crate) const TOP_FIELDS: [&str; 3] = ["model", "stream", "stream_options"];
/// Recognized members of the `stream_options` object.
pub(crate) const OPTIONS_FIELDS: [&str; 1] = ["include_usage"];

/// `key` differs from `field` but matches it under the folding Go's
/// `encoding/json` applies to lowercase ASCII field names: ASCII case, plus
/// U+017F onto `s`, U+0130 and U+0131 onto `i`, U+212A onto `k` (D20).
pub(crate) fn fold_equivalent(key: &str, field: &str) -> bool {
    key != field && key.chars().map(fold).eq(field.chars())
}

fn fold(c: char) -> char {
    match c {
        '\u{17f}' => 's',
        '\u{130}' | '\u{131}' => 'i',
        '\u{212a}' => 'k',
        c => c.to_ascii_lowercase(),
    }
}

/// How a key relates to the recognized members of its object.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum KeyClass {
    Field(&'static str),
    Folded(&'static str),
    Other,
}

/// Exact matches win over folded ones, and fields are tried in order.
pub(crate) fn classify(key: &str, fields: &[&'static str]) -> KeyClass {
    if let Some(&field) = fields.iter().find(|&&field| field == key) {
        return KeyClass::Field(field);
    }
    fields
        .iter()
        .find(|&&field| fold_equivalent(key, field))
        .map_or(KeyClass::Other, |&field| KeyClass::Folded(field))
}

/// A JSON value whose objects keep their members in order, duplicates
/// included. Values nested in arrays are plain `Value`s: the rules under test
/// only look at the top-level object and `stream_options`.
#[derive(Debug, Clone)]
enum Ordered {
    Object(Vec<(String, Ordered)>),
    Other(Value),
}

impl Ordered {
    /// The value `serde_json` builds for the same text: the last of several
    /// equal keys wins.
    fn to_value(&self) -> Value {
        match self {
            Self::Other(value) => value.clone(),
            Self::Object(members) => Value::Object(
                members
                    .iter()
                    .map(|(key, value)| (key.clone(), value.to_value()))
                    .collect(),
            ),
        }
    }
}

impl<'de> Deserialize<'de> for Ordered {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_any(OrderedVisitor)
    }
}

struct OrderedVisitor;

impl<'de> Visitor<'de> for OrderedVisitor {
    type Value = Ordered;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("any JSON value")
    }

    fn visit_bool<E>(self, value: bool) -> Result<Ordered, E> {
        Ok(Ordered::Other(Value::Bool(value)))
    }

    fn visit_i64<E>(self, value: i64) -> Result<Ordered, E> {
        Ok(Ordered::Other(Value::from(value)))
    }

    fn visit_u64<E>(self, value: u64) -> Result<Ordered, E> {
        Ok(Ordered::Other(Value::from(value)))
    }

    fn visit_f64<E>(self, value: f64) -> Result<Ordered, E> {
        Ok(Ordered::Other(Value::from(value)))
    }

    fn visit_str<E>(self, value: &str) -> Result<Ordered, E> {
        Ok(Ordered::Other(Value::String(value.to_owned())))
    }

    fn visit_string<E>(self, value: String) -> Result<Ordered, E> {
        Ok(Ordered::Other(Value::String(value)))
    }

    fn visit_unit<E>(self) -> Result<Ordered, E> {
        Ok(Ordered::Other(Value::Null))
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Ordered, A::Error> {
        let mut items = Vec::new();
        while let Some(item) = seq.next_element::<Value>()? {
            items.push(item);
        }
        Ok(Ordered::Other(Value::Array(items)))
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Ordered, A::Error> {
        let mut members = Vec::new();
        while let Some(member) = map.next_entry::<String, Ordered>()? {
            members.push(member);
        }
        Ok(Ordered::Object(members))
    }
}

/// What `ChatHead::parse` must return for a body.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Expected {
    /// `serde_json` rejects the text; any error is acceptable.
    Invalid,
    NotAnObject,
    Json,
    Ambiguous(&'static str),
    MissingModel,
    ModelNotString,
    EmptyModel,
    InvalidStreamOptions,
    Ok(ExpectedHead),
}

/// The accepted head, as values rather than spans.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ExpectedHead {
    pub(crate) model_name: String,
    pub(crate) stream: bool,
    pub(crate) stream_options: ExpectedOptions,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum ExpectedOptions {
    Absent,
    Null,
    Object {
        value: Value,
        /// `None` when absent; otherwise `null` or a boolean.
        include_usage: Option<Value>,
    },
}

/// The reference decision for `body`, following contract 05 1.3.2: members
/// are checked in document order during the top-level pass (a repeated
/// recognized member or a wrong-typed `stream` is `Json`, a fold-equivalent
/// key `Ambiguous`), then `model`, then `stream_options` and its members.
pub(crate) fn expected_head(body: &[u8]) -> Expected {
    let Ok(document) = serde_json::from_slice::<Ordered>(body) else {
        return Expected::Invalid;
    };
    let Ordered::Object(members) = document else {
        return Expected::NotAnObject;
    };

    let mut model = None;
    let mut stream = None;
    let mut stream_options = None;
    for (key, value) in members {
        match classify(&key, &TOP_FIELDS) {
            KeyClass::Field("model") => {
                if model.replace(value).is_some() {
                    return Expected::Json;
                }
            }
            KeyClass::Field("stream") => {
                let flag = match value.to_value() {
                    Value::Null => false,
                    Value::Bool(flag) => flag,
                    _ => return Expected::Json,
                };
                if stream.replace(flag).is_some() {
                    return Expected::Json;
                }
            }
            KeyClass::Field(_) => {
                if stream_options.replace(value).is_some() {
                    return Expected::Json;
                }
            }
            KeyClass::Folded(field) => return Expected::Ambiguous(field),
            KeyClass::Other => {}
        }
    }

    let model_name = match model.map(|model| model.to_value()) {
        None => return Expected::MissingModel,
        Some(Value::String(name)) if name.is_empty() => return Expected::EmptyModel,
        Some(Value::String(name)) => name,
        Some(_) => return Expected::ModelNotString,
    };

    let stream_options = match stream_options {
        None => ExpectedOptions::Absent,
        Some(Ordered::Other(Value::Null)) => ExpectedOptions::Null,
        Some(Ordered::Other(_)) => return Expected::InvalidStreamOptions,
        Some(object @ Ordered::Object(_)) => {
            let value = object.to_value();
            let Ordered::Object(members) = object else {
                unreachable!("matched as an object above");
            };
            let mut include_usage = None;
            for (key, member) in members {
                match classify(&key, &OPTIONS_FIELDS) {
                    KeyClass::Field(_) => {
                        if include_usage.replace(member.to_value()).is_some() {
                            return Expected::Json;
                        }
                    }
                    KeyClass::Folded(field) => return Expected::Ambiguous(field),
                    KeyClass::Other => {}
                }
            }
            if include_usage
                .as_ref()
                .is_some_and(|flag| !matches!(flag, Value::Null | Value::Bool(_)))
            {
                return Expected::InvalidStreamOptions;
            }
            ExpectedOptions::Object {
                value,
                include_usage,
            }
        }
    };

    Expected::Ok(ExpectedHead {
        model_name,
        stream: stream.unwrap_or(false),
        stream_options,
    })
}

/// Runs `ChatHead::parse` on `body` and compares it with [`expected_head`]:
/// the outcome, every decoded value, and that every span cuts out bytes
/// `serde_json` parses to the corresponding value.
pub(crate) fn verify_head(body: &[u8]) -> Result<(), String> {
    let expected = expected_head(body);
    let actual = ChatHead::parse(body);
    let agrees = match (&expected, &actual) {
        (Expected::Invalid, Err(_))
        | (Expected::NotAnObject, Err(HeadError::NotAnObject))
        | (Expected::Json, Err(HeadError::Json(_)))
        | (Expected::MissingModel, Err(HeadError::MissingModel))
        | (Expected::ModelNotString, Err(HeadError::ModelNotString))
        | (Expected::EmptyModel, Err(HeadError::EmptyModel))
        | (Expected::InvalidStreamOptions, Err(HeadError::InvalidStreamOptions)) => true,
        (Expected::Ambiguous(want), Err(HeadError::AmbiguousKey(got))) => want == got,
        (Expected::Ok(want), Ok(head)) => {
            return compare_head(body, want, head).map_err(|mismatch| {
                format!("{mismatch}\n  body: {}", String::from_utf8_lossy(body))
            });
        }
        _ => false,
    };
    if agrees {
        Ok(())
    } else {
        Err(format!(
            "expected {expected:?}, got {actual:?}\n  body: {}",
            String::from_utf8_lossy(body)
        ))
    }
}

fn compare_head(body: &[u8], want: &ExpectedHead, head: &ChatHead<'_>) -> Result<(), String> {
    let model_value = value_at(body, head.model.start, head.model.end)?;
    if model_value != Value::String(want.model_name.clone()) {
        return Err(format!("model span holds {model_value}"));
    }
    if head.model_name != want.model_name {
        return Err(format!(
            "model_name {:?}, want {:?}",
            head.model_name, want.model_name
        ));
    }
    if head.stream != want.stream {
        return Err(format!("stream {}, want {}", head.stream, want.stream));
    }
    let last = body
        .iter()
        .rposition(|byte| !matches!(byte, b' ' | b'\n' | b'\t' | b'\r'));
    if last != Some(head.object_end) || body.get(head.object_end) != Some(&b'}') {
        return Err(format!(
            "object_end {} is not the closing brace",
            head.object_end
        ));
    }

    match (&want.stream_options, head.stream_options) {
        (ExpectedOptions::Absent, StreamOptions::Absent) => Ok(()),
        (ExpectedOptions::Null, StreamOptions::Null { value }) => {
            match value_at(body, value.start, value.end)? {
                Value::Null => Ok(()),
                other => Err(format!("stream_options null span holds {other}")),
            }
        }
        (
            ExpectedOptions::Object {
                value: want_value,
                include_usage: want_flag,
            },
            StreamOptions::Object {
                value,
                include_usage,
            },
        ) => {
            let got = value_at(body, value.start, value.end)?;
            if &got != want_value {
                return Err(format!("stream_options span holds {got}"));
            }
            match (want_flag, include_usage) {
                (None, IncludeUsage::Absent) => Ok(()),
                (Some(want_flag), IncludeUsage::Present { value, span }) => {
                    let got = value_at(body, span.start, span.end)?;
                    let decoded = value.map_or(Value::Null, Value::Bool);
                    if &got == want_flag && decoded == got {
                        Ok(())
                    } else {
                        Err(format!(
                            "include_usage {value:?} spanning {got}, want {want_flag}"
                        ))
                    }
                }
                (want_flag, got) => Err(format!("include_usage {got:?}, want {want_flag:?}")),
            }
        }
        (want, got) => Err(format!("stream_options {got:?}, want {want:?}")),
    }
}

fn value_at(body: &[u8], start: usize, end: usize) -> Result<Value, String> {
    let bytes = body
        .get(start..end)
        .ok_or_else(|| format!("span {start}..{end} outside a body of {}", body.len()))?;
    serde_json::from_slice(bytes).map_err(|error| {
        format!(
            "span {start}..{end} ({}) is not JSON: {error}",
            String::from_utf8_lossy(bytes)
        )
    })
}

/// Runs `plan_chat` with the given rewrite on a body `head` was parsed from,
/// and checks every `splice_props` equation (contract 05, 4.2): the output is
/// byte-exact to the edit table applied to `body`, parses to the input with
/// `model` replaced and `stream_options.include_usage` set, `len()` is exact,
/// there are at most five non-empty segments, and injecting into a
/// non-streaming request is refused.
pub(crate) fn verify_splice(
    body: &Bytes,
    head: &ChatHead<'_>,
    model: Option<&str>,
    inject: bool,
) -> Result<(), String> {
    let model_literal = model.map(json_string);
    let rewrite = Rewrite {
        model: model_literal.as_ref(),
        inject_include_usage: inject,
    };
    let result = plan_chat(body, head, rewrite);
    if inject && !head.stream {
        return match result {
            Err(SpliceError::InjectWithoutStream) => Ok(()),
            other => Err(format!(
                "injecting into a non-streaming request gave {other:?}"
            )),
        };
    }
    let splice = result.map_err(|error| format!("plan_chat failed: {error}"))?;

    let segments = splice.segments();
    if segments.len() > brisk_proto::splice::MAX_SEGMENTS || segments.iter().any(Bytes::is_empty) {
        return Err(format!("bad segments: {segments:?}"));
    }
    let output = splice.to_contiguous();
    let joined: Vec<u8> = segments
        .iter()
        .flat_map(|segment| segment.iter().copied())
        .collect();
    if output[..] != joined[..] || splice.len() != output.len() as u64 {
        return Err(format!(
            "len() {} or to_contiguous() disagrees with {} segment bytes",
            splice.len(),
            joined.len()
        ));
    }

    let edits = expected_edits(body, head, model_literal.as_ref(), inject);
    if splice.is_identity() != edits.is_empty() {
        return Err(format!(
            "is_identity() is {} with {} edits",
            splice.is_identity(),
            edits.len()
        ));
    }
    let mut want = Vec::with_capacity(body.len() + 64);
    let mut cursor = 0;
    for (start, end, with) in &edits {
        want.extend_from_slice(&body[cursor..*start]);
        want.extend_from_slice(with);
        cursor = *end;
    }
    want.extend_from_slice(&body[cursor..]);
    if output[..] != want[..] {
        return Err(format!(
            "output {:?}\n  want {:?}",
            String::from_utf8_lossy(&output),
            String::from_utf8_lossy(&want)
        ));
    }

    let mut expected: Value = serde_json::from_slice(body)
        .map_err(|error| format!("a body ChatHead accepted is not JSON: {error}"))?;
    if let Some(model) = model {
        expected["model"] = Value::String(model.to_owned());
    }
    if inject {
        match &mut expected["stream_options"] {
            Value::Object(options) => {
                options.insert("include_usage".to_owned(), Value::Bool(true));
            }
            other => {
                let mut options = serde_json::Map::new();
                options.insert("include_usage".to_owned(), Value::Bool(true));
                *other = Value::Object(options);
            }
        }
    }
    let got: Value =
        serde_json::from_slice(&output).map_err(|error| format!("output is not JSON: {error}"))?;
    if got != expected {
        return Err(format!("output parses to {got}\n  want {expected}"));
    }

    // An empty replacement model is the caller's error, not plan_chat's, and
    // makes the output a request ChatHead rightly refuses.
    if model != Some("") {
        let reparsed = ChatHead::parse(&output)
            .map_err(|error| format!("ChatHead rejects the output: {error}"))?;
        if inject && !reparsed.requests_usage() {
            return Err("output does not request usage after injection".to_owned());
        }
    }
    Ok(())
}

/// The edits contract 05 1.3.3 prescribes, as `(start, end, replacement)`
/// sorted by position.
fn expected_edits(
    body: &[u8],
    head: &ChatHead<'_>,
    model: Option<&Bytes>,
    inject: bool,
) -> Vec<(usize, usize, Vec<u8>)> {
    let mut edits = Vec::new();
    if let Some(model) = model {
        edits.push((head.model.start, head.model.end, model.to_vec()));
    }
    if inject {
        let edit: Option<(usize, usize, &[u8])> = match head.stream_options {
            StreamOptions::Absent => Some((
                head.object_end,
                head.object_end,
                br#","stream_options":{"include_usage":true}"#,
            )),
            StreamOptions::Null { value } => {
                Some((value.start, value.end, br#"{"include_usage":true}"#))
            }
            StreamOptions::Object {
                include_usage:
                    IncludeUsage::Present {
                        value: Some(true), ..
                    },
                ..
            } => None,
            StreamOptions::Object {
                include_usage: IncludeUsage::Present { span, .. },
                ..
            } => Some((span.start, span.end, b"true")),
            StreamOptions::Object {
                value,
                include_usage: IncludeUsage::Absent,
            } => {
                let inside = &body[value.start + 1..value.end - 1];
                let with: &[u8] = if inside
                    .iter()
                    .all(|byte| matches!(byte, b' ' | b'\n' | b'\t' | b'\r'))
                {
                    br#""include_usage":true"#
                } else {
                    br#""include_usage":true,"#
                };
                Some((value.start + 1, value.start + 1, with))
            }
        };
        if let Some((start, end, with)) = edit {
            edits.push((start, end, with.to_vec()));
        }
    }
    edits.sort_by_key(|&(start, _, _)| start);
    edits
}
