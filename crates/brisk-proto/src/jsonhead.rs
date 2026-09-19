//! Borrowed parsing of the request-body members Brisk routes and bills on:
//! `model`, `stream` and `stream_options` of an `OpenAI` Chat Completions
//! request, each with its exact byte span in the body.
//!
//! `serde_json` validates the whole body, but only these members are decoded.
//! A repeated member, or a key that equals a recognized member under the case
//! folding Go's `encoding/json` applies, is rejected: an upstream that
//! resolves such keys differently would run a model other than the one Brisk
//! routed and billed (R4, D20).
//!
//! The top-level object and `stream_options` are read by hand-written
//! visitors with hand-written field identifiers, because a derived
//! deserializer only rejects keys that are byte-equal after unescaping, and
//! also accepts a JSON array in place of the object.

use std::borrow::Cow;
use std::fmt;

use memchr::memchr;
use serde::de::{self, Deserialize, DeserializeSeed, Deserializer, MapAccess, SeqAccess, Visitor};
use serde_json::value::RawValue;

use crate::Span;

/// What Brisk needs from an `OpenAI` Chat Completions request body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChatHead<'a> {
    /// Span of the whole `model` value, quotes included.
    pub model: Span,
    /// Decoded model name; borrowed from the body unless it contains escapes.
    pub model_name: Cow<'a, str>,
    /// `stream` is `true`; absent or `null` means `false`.
    pub stream: bool,
    /// The `stream_options` member as it appears in the body.
    pub stream_options: StreamOptions,
    /// Offset of the closing `}` of the top-level object.
    pub object_end: usize,
}

/// The `stream_options` member of a request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamOptions {
    /// The member is not present.
    Absent,
    /// `"stream_options": null`; `value` spans the `null`.
    Null {
        /// Span of the `null` literal.
        value: Span,
    },
    /// An object; `value` spans it from `{` to `}`.
    Object {
        /// Span of the object, braces included.
        value: Span,
        /// Its `include_usage` member.
        include_usage: IncludeUsage,
    },
}

/// The `include_usage` member of a `stream_options` object.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IncludeUsage {
    /// The member is not present.
    Absent,
    /// Present; `value` is `None` for JSON `null`.
    Present {
        /// The boolean, or `None` for `null`.
        value: Option<bool>,
        /// Span of the value literal.
        span: Span,
    },
}

/// Why a request body was rejected.
#[derive(Debug, thiserror::Error)]
pub enum HeadError {
    /// Valid JSON whose top-level value is not an object.
    #[error("request body must be a JSON object")]
    NotAnObject,
    /// Malformed JSON, a wrong type for `stream`, more than 127 nested
    /// arrays and objects (`serde_json`'s recursion limit), or a recognized
    /// member given twice.
    #[error("invalid request body: {0}")]
    Json(#[source] serde_json::Error),
    /// No top-level `model` member.
    #[error("`model` is required")]
    MissingModel,
    /// `model` is present but not a string (`null` included).
    #[error("`model` must be a string")]
    ModelNotString,
    /// `model` decodes to the empty string.
    #[error("`model` must not be empty")]
    EmptyModel,
    /// `stream_options` or its `include_usage` has an unaccepted type.
    #[error("`stream_options` must be an object or null, and `include_usage` a boolean or null")]
    InvalidStreamOptions,
    /// A key that is not byte-equal to a recognized field but matches it
    /// under case folding (D20). Carries the recognized field's name, never
    /// the client's key.
    #[error("a key differs from `{0}` only by letter case; this request is ambiguous")]
    AmbiguousKey(&'static str),
    /// A borrowed value did not point into `body`; an internal invariant
    /// violation, returned instead of panicking.
    #[error("internal error: parsed value lies outside the request body")]
    SpanOutsideBody,
}

impl<'a> ChatHead<'a> {
    /// Parses `body`, borrowing from it.
    ///
    /// The whole body is validated (syntax, trailing bytes, at most 127
    /// nested arrays and objects), but only `model`, `stream`, `stream_options` and
    /// `stream_options.include_usage` are decoded. Without escapes in the
    /// model name nothing is allocated.
    pub fn parse(body: &'a [u8]) -> Result<Self, HeadError> {
        if first_significant(body) != Some(b'{') {
            return Err(classify_non_object(body));
        }

        let mut ambiguous = None;
        let mut de = serde_json::Deserializer::from_slice(body);
        let members = HeadSeed {
            ambiguous: &mut ambiguous,
        }
        .deserialize(&mut de)
        .and_then(|members| de.end().map(|()| members))
        .map_err(|error| ambiguous.map_or(HeadError::Json(error), HeadError::AmbiguousKey))?;

        let raw_model = members.model.ok_or(HeadError::MissingModel)?;
        let model_name = decode_model(raw_model.get())?;
        if model_name.is_empty() {
            return Err(HeadError::EmptyModel);
        }
        let stream_options = match members.stream_options {
            None => StreamOptions::Absent,
            Some(raw) => parse_stream_options(body, raw.get())?,
        };

        Ok(Self {
            model: span_of(body, raw_model.get())?,
            model_name,
            stream: members.stream,
            stream_options,
            object_end: object_end(body)?,
        })
    }

    /// `stream` is true and `stream_options.include_usage` is `true`.
    pub fn requests_usage(&self) -> bool {
        self.stream
            && matches!(
                self.stream_options,
                StreamOptions::Object {
                    include_usage: IncludeUsage::Present {
                        value: Some(true),
                        ..
                    },
                    ..
                }
            )
    }
}

/// The byte set `serde_json` skips as whitespace between tokens.
pub(crate) const fn is_json_whitespace(byte: u8) -> bool {
    matches!(byte, b' ' | b'\n' | b'\t' | b'\r')
}

fn first_significant(body: &[u8]) -> Option<u8> {
    body.iter().copied().find(|&byte| !is_json_whitespace(byte))
}

/// Tells apart JSON of another type, which is `NotAnObject`, from bytes that
/// are not JSON at all (a byte order mark, an empty body), which are `Json`
/// with `serde_json`'s description of the syntax error.
fn classify_non_object(body: &[u8]) -> HeadError {
    let mut de = serde_json::Deserializer::from_slice(body);
    match Skip::deserialize(&mut de).and_then(|Skip| de.end()) {
        Ok(()) => HeadError::NotAnObject,
        Err(error) => HeadError::Json(error),
    }
}

/// Offset of the last non-whitespace byte, which `serde_json` has already
/// established is the `}` closing the top-level object.
fn object_end(body: &[u8]) -> Result<usize, HeadError> {
    match body.iter().rposition(|&byte| !is_json_whitespace(byte)) {
        Some(end) if body[end] == b'}' => Ok(end),
        _ => Err(HeadError::SpanOutsideBody),
    }
}

/// The span of `part` inside `body`. `part` must be a slice of `body`;
/// anything else is reported rather than trusted.
fn span_of(body: &[u8], part: &str) -> Result<Span, HeadError> {
    let start = part
        .as_ptr()
        .addr()
        .checked_sub(body.as_ptr().addr())
        .ok_or(HeadError::SpanOutsideBody)?;
    let end = start
        .checked_add(part.len())
        .filter(|&end| end <= body.len())
        .ok_or(HeadError::SpanOutsideBody)?;
    Ok(Span { start, end })
}

/// Decodes the raw `model` value. A name without escapes is borrowed from
/// the body; one with escapes is decoded into a single allocation.
fn decode_model(raw: &str) -> Result<Cow<'_, str>, HeadError> {
    let Some(inner) = raw
        .strip_prefix('"')
        .and_then(|rest| rest.strip_suffix('"'))
    else {
        return Err(HeadError::ModelNotString);
    };
    if memchr(b'\\', inner.as_bytes()).is_none() {
        return Ok(Cow::Borrowed(inner));
    }
    match unescape(inner) {
        Some(name) => Ok(Cow::Owned(name)),
        // An escaped surrogate without its pair: serde_json accepts it while
        // skipping a value but rejects it when decoding a string, so decoding
        // with serde_json yields the precise error.
        None => serde_json::from_str::<String>(raw)
            .map(Cow::Owned)
            .map_err(HeadError::Json),
    }
}

/// Decodes the escapes in the body of a JSON string that `serde_json` has
/// already validated. `None` for a lone UTF-16 surrogate.
fn unescape(escaped: &str) -> Option<String> {
    let mut out = String::with_capacity(escaped.len());
    let mut rest = escaped;
    while let Some(at) = memchr(b'\\', rest.as_bytes()) {
        out.push_str(&rest[..at]);
        let (decoded, tail) = decode_escape(&rest[at + 1..])?;
        out.push(decoded);
        rest = tail;
    }
    out.push_str(rest);
    Some(out)
}

/// Decodes the escape whose backslash precedes `escape`, returning the
/// character and the text after the escape.
fn decode_escape(escape: &str) -> Option<(char, &str)> {
    let tail = escape.get(1..)?;
    let decoded = match escape.as_bytes().first()? {
        b'"' => '"',
        b'\\' => '\\',
        b'/' => '/',
        b'b' => '\u{8}',
        b'f' => '\u{c}',
        b'n' => '\n',
        b'r' => '\r',
        b't' => '\t',
        b'u' => {
            let (unit, tail) = hex4(tail)?;
            return match unit {
                0xD800..=0xDBFF => {
                    let (low, tail) = hex4(tail.strip_prefix("\\u")?)?;
                    if !(0xDC00..=0xDFFF).contains(&low) {
                        return None;
                    }
                    let code = 0x1_0000 + ((unit - 0xD800) << 10) + (low - 0xDC00);
                    Some((char::from_u32(code)?, tail))
                }
                _ => Some((char::from_u32(unit)?, tail)),
            };
        }
        _ => return None,
    };
    Some((decoded, tail))
}

/// Four hexadecimal digits at the start of `text`.
fn hex4(text: &str) -> Option<(u32, &str)> {
    let digits = text.get(..4)?;
    if !digits.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return None;
    }
    Some((u32::from_str_radix(digits, 16).ok()?, &text[4..]))
}

fn parse_stream_options(body: &[u8], raw: &str) -> Result<StreamOptions, HeadError> {
    let value = span_of(body, raw)?;
    if raw == "null" {
        return Ok(StreamOptions::Null { value });
    }
    if !raw.starts_with('{') {
        return Err(HeadError::InvalidStreamOptions);
    }
    // The top-level pass captured `raw` without a depth limit, and the
    // second pass below starts a fresh one, so a value nested exactly one
    // level too deep for the whole body would pass both.
    if nesting_depth(raw.as_bytes()) >= MAX_NESTING {
        return Err(HeadError::Json(de::Error::custom(
            "recursion limit exceeded",
        )));
    }

    // `raw` was only syntax-checked by the top-level pass; this second pass
    // over the (short) object applies the duplicate, case-folding and
    // nesting rules to its members. `from_str` borrows from `raw`, which
    // borrows from `body`, so the spans below still point into `body`.
    let mut ambiguous = None;
    let mut de = serde_json::Deserializer::from_str(raw);
    let include_usage = OptionsSeed {
        ambiguous: &mut ambiguous,
    }
    .deserialize(&mut de)
    .and_then(|include_usage| de.end().map(|()| include_usage))
    .map_err(|error| ambiguous.map_or(HeadError::Json(error), HeadError::AmbiguousKey))?;

    let include_usage = match include_usage {
        None => IncludeUsage::Absent,
        Some(raw) => {
            let raw = raw.get();
            let value = match raw {
                "true" => Some(true),
                "false" => Some(false),
                "null" => None,
                _ => return Err(HeadError::InvalidStreamOptions),
            };
            IncludeUsage::Present {
                value,
                span: span_of(body, raw)?,
            }
        }
    };
    Ok(StreamOptions::Object {
        value,
        include_usage,
    })
}

/// Most arrays and objects `serde_json` lets a document nest, the top-level
/// object included; one more is a recursion-limit error.
const MAX_NESTING: usize = 127;

/// Deepest nesting of arrays and objects in `raw`, a value `serde_json` has
/// already found syntactically valid, so brackets inside strings are the
/// only ones to skip.
fn nesting_depth(raw: &[u8]) -> usize {
    let mut depth = 0_usize;
    let mut deepest = 0;
    let mut in_string = false;
    let mut escaped = false;
    for &byte in raw {
        if in_string {
            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
            } else if byte == b'"' {
                in_string = false;
            }
            continue;
        }
        match byte {
            b'"' => in_string = true,
            b'[' | b'{' => {
                depth += 1;
                deepest = deepest.max(depth);
            }
            b']' | b'}' => depth = depth.saturating_sub(1),
            _ => {}
        }
    }
    deepest
}

/// A fold-equivalent key is at most this many bytes longer than its field:
/// each rune that folds onto an ASCII letter is two or three bytes, and the
/// declared fields contain at most three `s`, `i` and `k` in total.
const MAX_FOLD_GROWTH: usize = 4;

/// How a map key relates to the fields declared for its object.
#[derive(Debug, Clone, Copy)]
enum Key<F> {
    /// Byte-equal to a declared field once `serde_json` has decoded escapes.
    Field(F),
    /// Equal to the named field only under Go's case folding (D20).
    Folded(&'static str),
    /// Any other key.
    Other,
}

/// Hand-written field identifier: sorts one map key against `fields`.
#[derive(Debug, Clone, Copy)]
struct KeySeed<F: 'static>(&'static [(&'static str, F)]);

impl<'de, F: Copy> DeserializeSeed<'de> for KeySeed<F> {
    type Value = Key<F>;

    fn deserialize<D>(self, deserializer: D) -> Result<Key<F>, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_identifier(self)
    }
}

impl<'de, F: Copy> Visitor<'de> for KeySeed<F> {
    type Value = Key<F>;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("an object key")
    }

    fn visit_str<E: de::Error>(self, key: &str) -> Result<Key<F>, E> {
        Ok(classify(key.as_bytes(), self.0))
    }

    fn visit_borrowed_str<E: de::Error>(self, key: &'de str) -> Result<Key<F>, E> {
        Ok(classify(key.as_bytes(), self.0))
    }

    fn visit_bytes<E: de::Error>(self, key: &[u8]) -> Result<Key<F>, E> {
        Ok(classify(key, self.0))
    }
}

fn classify<F: Copy>(key: &[u8], fields: &[(&'static str, F)]) -> Key<F> {
    if let Some(&(_, field)) = fields.iter().find(|(name, _)| name.as_bytes() == key) {
        return Key::Field(field);
    }
    fields
        .iter()
        .find(|(name, _)| folds_to(key, name.as_bytes()))
        .map_or(Key::Other, |&(name, _)| Key::Folded(name))
}

/// Go's `encoding/json` falls back to matching a key to a field when both
/// are equal after mapping every rune `r` to `ToUpper(ToLower(r))`. For the
/// lowercase ASCII names declared here that is ASCII case-insensitivity plus
/// four runes that fold onto ASCII letters: U+017F (long s) onto `s`, U+212A
/// (Kelvin sign) onto `k`, and U+0130 and U+0131 (dotted and dotless I) onto
/// `i`.
fn folds_to(key: &[u8], field: &[u8]) -> bool {
    if key.len() < field.len() || key.len() > field.len() + MAX_FOLD_GROWTH {
        return false;
    }
    let mut rest = key;
    for &want in field {
        rest = match rest {
            [byte, tail @ ..] if byte.is_ascii() => {
                if byte.to_ascii_lowercase() != want {
                    return false;
                }
                tail
            }
            [0xC5, 0xBF, tail @ ..] if want == b's' => tail,
            [0xE2, 0x84, 0xAA, tail @ ..] if want == b'k' => tail,
            [0xC4, 0xB0 | 0xB1, tail @ ..] if want == b'i' => tail,
            _ => return false,
        };
    }
    rest.is_empty()
}

/// Returned from a visitor to abort the parse on a fold-equivalent key; the
/// caller reports the field recorded next to it, never this message or the
/// client's key.
fn ambiguous_key<E: de::Error>(slot: &mut Option<&'static str>, field: &'static str) -> E {
    *slot = Some(field);
    E::custom(format_args!(
        "a key differs from `{field}` only by letter case"
    ))
}

#[derive(Debug, Clone, Copy)]
enum TopField {
    Model,
    Stream,
    StreamOptions,
}

const TOP_FIELDS: &[(&str, TopField)] = &[
    ("model", TopField::Model),
    ("stream", TopField::Stream),
    ("stream_options", TopField::StreamOptions),
];

/// The top-level members, undecoded apart from `stream`.
///
/// `model` and `stream_options` are read as `&RawValue` and wrapped in
/// `Some` by hand: `Option<&RawValue>` would turn a JSON `null` into `None`
/// and lose its span (R5).
#[derive(Debug, Default)]
struct TopMembers<'de> {
    model: Option<&'de RawValue>,
    /// `stream` was present, possibly as `null`.
    stream_seen: bool,
    /// `stream` was `true`.
    stream: bool,
    stream_options: Option<&'de RawValue>,
}

#[derive(Debug)]
struct HeadSeed<'s> {
    ambiguous: &'s mut Option<&'static str>,
}

impl<'de> DeserializeSeed<'de> for HeadSeed<'_> {
    type Value = TopMembers<'de>;

    fn deserialize<D>(self, deserializer: D) -> Result<TopMembers<'de>, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(self)
    }
}

impl<'de> Visitor<'de> for HeadSeed<'_> {
    type Value = TopMembers<'de>;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a JSON object")
    }

    fn visit_map<A>(self, mut map: A) -> Result<TopMembers<'de>, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut members = TopMembers::default();
        while let Some(key) = map.next_key_seed(KeySeed(TOP_FIELDS))? {
            match key {
                Key::Field(TopField::Model) => {
                    if members.model.is_some() {
                        return Err(de::Error::duplicate_field("model"));
                    }
                    members.model = Some(map.next_value::<&'de RawValue>()?);
                }
                Key::Field(TopField::Stream) => {
                    if members.stream_seen {
                        return Err(de::Error::duplicate_field("stream"));
                    }
                    members.stream_seen = true;
                    members.stream = map.next_value::<Option<bool>>()? == Some(true);
                }
                Key::Field(TopField::StreamOptions) => {
                    if members.stream_options.is_some() {
                        return Err(de::Error::duplicate_field("stream_options"));
                    }
                    members.stream_options = Some(map.next_value::<&'de RawValue>()?);
                }
                Key::Folded(field) => return Err(ambiguous_key(self.ambiguous, field)),
                Key::Other => {
                    map.next_value::<Skip>()?;
                }
            }
        }
        Ok(members)
    }
}

#[derive(Debug, Clone, Copy)]
enum OptionsField {
    IncludeUsage,
}

const OPTIONS_FIELDS: &[(&str, OptionsField)] = &[("include_usage", OptionsField::IncludeUsage)];

/// Reads a `stream_options` object into its raw `include_usage` value.
#[derive(Debug)]
struct OptionsSeed<'s> {
    ambiguous: &'s mut Option<&'static str>,
}

impl<'de> DeserializeSeed<'de> for OptionsSeed<'_> {
    type Value = Option<&'de RawValue>;

    fn deserialize<D>(self, deserializer: D) -> Result<Option<&'de RawValue>, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(self)
    }
}

impl<'de> Visitor<'de> for OptionsSeed<'_> {
    type Value = Option<&'de RawValue>;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a JSON object")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Option<&'de RawValue>, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut include_usage = None;
        while let Some(key) = map.next_key_seed(KeySeed(OPTIONS_FIELDS))? {
            match key {
                Key::Field(OptionsField::IncludeUsage) => {
                    if include_usage.is_some() {
                        return Err(de::Error::duplicate_field("include_usage"));
                    }
                    include_usage = Some(map.next_value::<&'de RawValue>()?);
                }
                Key::Folded(field) => return Err(ambiguous_key(self.ambiguous, field)),
                Key::Other => {
                    map.next_value::<Skip>()?;
                }
            }
        }
        Ok(include_usage)
    }
}

/// Skips one JSON value of any type, checking its syntax.
///
/// `serde::de::IgnoredAny` is not used: `serde_json` skips it with an
/// iterative scanner that applies no recursion limit and keeps its stack of
/// open brackets in the deserializer's scratch buffer, so it allocates on
/// every request whose `messages` array holds objects. Going through
/// `deserialize_any` keeps the 128-level limit and allocates only to decode a
/// string that contains escapes. The price is that strings are decoded and
/// checked as UTF-8, so an unpaired `\uD800` anywhere in the body is rejected.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Skip;

impl<'de> Deserialize<'de> for Skip {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(Skip)
    }
}

impl<'de> Visitor<'de> for Skip {
    type Value = Skip;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("any JSON value")
    }

    fn visit_bool<E: de::Error>(self, _: bool) -> Result<Skip, E> {
        Ok(Skip)
    }

    fn visit_i64<E: de::Error>(self, _: i64) -> Result<Skip, E> {
        Ok(Skip)
    }

    fn visit_u64<E: de::Error>(self, _: u64) -> Result<Skip, E> {
        Ok(Skip)
    }

    fn visit_f64<E: de::Error>(self, _: f64) -> Result<Skip, E> {
        Ok(Skip)
    }

    fn visit_str<E: de::Error>(self, _: &str) -> Result<Skip, E> {
        Ok(Skip)
    }

    fn visit_unit<E: de::Error>(self) -> Result<Skip, E> {
        Ok(Skip)
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Skip, A::Error>
    where
        A: SeqAccess<'de>,
    {
        while seq.next_element::<Skip>()?.is_some() {}
        Ok(Skip)
    }

    fn visit_map<A>(self, mut map: A) -> Result<Skip, A::Error>
    where
        A: MapAccess<'de>,
    {
        while map.next_key::<Skip>()?.is_some() {
            map.next_value::<Skip>()?;
        }
        Ok(Skip)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(body: &str) -> Result<ChatHead<'_>, HeadError> {
        ChatHead::parse(body.as_bytes())
    }

    #[test]
    fn folds_like_go_encoding_json() {
        assert!(folds_to(b"Model", b"model"));
        assert!(folds_to(b"MODEL", b"model"));
        assert!(folds_to("\u{17f}tream".as_bytes(), b"stream"));
        assert!(folds_to("\u{130}nclude_usage".as_bytes(), b"include_usage"));
        assert!(folds_to(
            "\u{131}nclude_u\u{17f}age".as_bytes(),
            b"include_usage"
        ));
        assert!(folds_to(
            "\u{17f}tream_option\u{17f}".as_bytes(),
            b"stream_options"
        ));
        assert!(folds_to(b"STREAM_OPTIONS", b"stream_options"));

        assert!(!folds_to(b"models", b"model"));
        assert!(!folds_to(b"mode", b"model"));
        assert!(!folds_to(b"streaming", b"stream"));
        assert!(!folds_to(b"stream-options", b"stream_options"));
        // Only the four listed runes fold onto ASCII; other look-alikes do not.
        assert!(!folds_to("m\u{f6}del".as_bytes(), b"model"));
        assert!(!folds_to("\u{ff4d}odel".as_bytes(), b"model"));
        // The Kelvin sign folds onto `k`, which no declared field contains.
        assert!(folds_to("\u{212a}ey".as_bytes(), b"key"));
    }

    #[test]
    fn nesting_depth_skips_brackets_in_strings() {
        assert_eq!(nesting_depth(b"1"), 0);
        assert_eq!(nesting_depth(b"{}"), 1);
        assert_eq!(nesting_depth(br#"{"a":[[1],{"b":[]}]}"#), 4);
        assert_eq!(nesting_depth(br#"{"a":"[[[{{\"]]"}"#), 1);
        assert_eq!(nesting_depth(br#"{"a\\":["\\"]}"#), 2);
    }

    #[test]
    fn classify_prefers_the_exact_field() {
        assert!(matches!(
            classify(b"model", TOP_FIELDS),
            Key::Field(TopField::Model)
        ));
        assert!(matches!(
            classify(b"Stream_Options", TOP_FIELDS),
            Key::Folded("stream_options")
        ));
        assert!(matches!(classify(b"messages", TOP_FIELDS), Key::Other));
    }

    #[test]
    fn unescape_decodes_every_escape() {
        assert_eq!(
            unescape(r#"a\"b\\c\/d\be\ff\ng\rh\ti"#).as_deref(),
            Some("a\"b\\c/d\u{8}e\u{c}f\ng\rh\ti")
        );
        assert_eq!(
            unescape(r"grok-4.6\u0028xhigh)").as_deref(),
            Some("grok-4.6(xhigh)")
        );
        assert_eq!(unescape(r"\ud83d\ude00").as_deref(), Some("\u{1f600}"));
        assert_eq!(unescape(r"\u00e9\u4e2d").as_deref(), Some("\u{e9}\u{4e2d}"));
        assert_eq!(unescape(r"\ud83d"), None);
        assert_eq!(unescape(r"\ude00"), None);
        assert_eq!(unescape(r"\ud83dx"), None);
        assert_eq!(unescape(r"\ud83d\u0041"), None);
    }

    #[test]
    fn escaped_model_is_decoded_once() {
        let head = parse(r#"{"model":"grok-4.6\u0028xhigh)"}"#).unwrap();
        assert!(matches!(head.model_name, Cow::Owned(_)));
        assert_eq!(head.model_name, "grok-4.6(xhigh)");

        let plain = parse(r#"{"model":"grok-4.6(xhigh)"}"#).unwrap();
        assert!(matches!(plain.model_name, Cow::Borrowed(_)));
    }

    #[test]
    fn lone_surrogate_in_model_is_a_json_error() {
        assert!(matches!(
            parse(r#"{"model":"a\ud800b"}"#),
            Err(HeadError::Json(_))
        ));
    }

    #[test]
    fn span_of_rejects_foreign_slices() {
        let body = br#"{"model":"m"}"#;
        assert!(matches!(
            span_of(body, "elsewhere"),
            Err(HeadError::SpanOutsideBody)
        ));
        let inside = std::str::from_utf8(&body[9..12]).unwrap();
        assert_eq!(span_of(body, inside).unwrap(), Span { start: 9, end: 12 });
    }

    #[test]
    fn requests_usage_needs_stream_and_true() {
        let with = |body| parse(body).unwrap().requests_usage();
        assert!(with(
            r#"{"model":"m","stream":true,"stream_options":{"include_usage":true}}"#
        ));
        assert!(!with(
            r#"{"model":"m","stream":false,"stream_options":{"include_usage":true}}"#
        ));
        assert!(!with(
            r#"{"model":"m","stream":true,"stream_options":{"include_usage":false}}"#
        ));
        assert!(!with(
            r#"{"model":"m","stream":true,"stream_options":{"include_usage":null}}"#
        ));
        assert!(!with(r#"{"model":"m","stream":true,"stream_options":{}}"#));
        assert!(!with(
            r#"{"model":"m","stream":true,"stream_options":null}"#
        ));
        assert!(!with(r#"{"model":"m","stream":true}"#));
    }
}
