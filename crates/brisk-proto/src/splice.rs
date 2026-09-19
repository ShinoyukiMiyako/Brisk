//! Zero-copy rewriting of a request body for one upstream attempt.
//!
//! The rewritten body is at most five `Bytes` segments that replace the
//! `model` value and set `stream_options.include_usage`; every byte outside
//! the edited value spans reaches the upstream unchanged (R5).

use bytes::{Bytes, BytesMut};

use crate::Span;
use crate::jsonhead::{ChatHead, IncludeUsage, StreamOptions, is_json_whitespace};

/// Most segments a rewritten body can have: two edits split the body into
/// three pieces and add two replacements.
pub const MAX_SEGMENTS: usize = 5;

/// Inserted before the closing `}` when `stream_options` is absent. The body
/// always has a `model` member, so the object is never empty and the leading
/// comma is always valid.
const INSERT_STREAM_OPTIONS: &[u8] = br#","stream_options":{"include_usage":true}"#;
/// Replaces a `null` `stream_options`.
const STREAM_OPTIONS_OBJECT: &[u8] = br#"{"include_usage":true}"#;
/// Inserted after the `{` of an empty `stream_options` object.
const INCLUDE_USAGE_ONLY: &[u8] = br#""include_usage":true"#;
/// Inserted after the `{` of a `stream_options` object with other members.
const INCLUDE_USAGE_FIRST: &[u8] = br#""include_usage":true,"#;
/// Replaces an `include_usage` of `false` or `null`.
const TRUE: &[u8] = b"true";

/// A request body after rewriting: up to five zero-copy `Bytes` segments.
#[derive(Debug, Clone)]
pub struct Splice {
    segments: [Bytes; MAX_SEGMENTS],
    used: usize,
    len: u64,
}

impl Splice {
    /// The unmodified body as a single segment.
    pub fn identity(body: Bytes) -> Self {
        let mut splice = Self::empty();
        splice.push(body);
        splice
    }

    /// The segments in order; none of them is empty.
    pub fn segments(&self) -> &[Bytes] {
        &self.segments[..self.used]
    }

    /// Exact total length in bytes.
    pub fn len(&self) -> u64 {
        self.len
    }

    /// True when the body has no bytes.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// True when the body is sent unchanged.
    ///
    /// Every edit contributes a non-empty replacement next to a non-empty
    /// piece of the body (an edit never starts at offset 0, where the `{`
    /// is), so an edited body always has at least two segments.
    pub fn is_identity(&self) -> bool {
        self.used <= 1
    }

    /// One contiguous copy (E2 `concat` mode). The identity case returns the
    /// original `Bytes` without copying.
    pub fn to_contiguous(&self) -> Bytes {
        match self.segments() {
            [] => Bytes::new(),
            [only] => only.clone(),
            segments => {
                let len = segments.iter().map(Bytes::len).sum();
                let mut out = BytesMut::with_capacity(len);
                for segment in segments {
                    out.extend_from_slice(segment);
                }
                out.freeze()
            }
        }
    }

    const fn empty() -> Self {
        Self {
            segments: [const { Bytes::new() }; MAX_SEGMENTS],
            used: 0,
            len: 0,
        }
    }

    /// Appends a segment, dropping empty ones. Callers stay within
    /// `MAX_SEGMENTS` by construction (see [`plan_chat`]).
    fn push(&mut self, segment: Bytes) {
        if segment.is_empty() {
            return;
        }
        self.len += segment.len() as u64;
        self.segments[self.used] = segment;
        self.used += 1;
    }
}

/// Edits requested for one upstream attempt.
#[derive(Debug, Clone, Copy, Default)]
pub struct Rewrite<'r> {
    /// Replacement `model` value, already a JSON string literal with quotes
    /// (see [`json_string`]).
    pub model: Option<&'r Bytes>,
    /// Make `stream_options.include_usage` true. Only valid when
    /// `head.stream` is true (D8).
    pub inject_include_usage: bool,
}

/// Why a rewrite could not be planned.
#[derive(Debug, thiserror::Error)]
pub enum SpliceError {
    /// An edit refers to bytes the body does not have; `head` was not parsed
    /// from this body.
    #[error("span {0:?} lies outside the body")]
    OutOfBounds(Span),
    /// Two edits cover the same bytes; `head` was not parsed from this body.
    #[error("edits overlap")]
    Overlap,
    /// `inject_include_usage` on a non-streaming request; `OpenAI` rejects
    /// `stream_options` unless `stream` is true (D8).
    #[error("include_usage can only be injected into a streaming request")]
    InjectWithoutStream,
}

/// One replacement of `span` by `with`; an empty span is an insertion.
#[derive(Debug, Clone)]
struct Edit {
    span: Span,
    with: Bytes,
}

/// Plans the rewritten body. `head` must have been parsed from `body`.
///
/// Replacements are the caller's pre-escaped `model` or static constants, so
/// nothing is allocated here, except that debug builds check once that the
/// result is valid JSON.
pub fn plan_chat(
    body: &Bytes,
    head: &ChatHead<'_>,
    rewrite: Rewrite<'_>,
) -> Result<Splice, SpliceError> {
    if rewrite.inject_include_usage && !head.stream {
        return Err(SpliceError::InjectWithoutStream);
    }

    let mut edits: [Option<Edit>; 2] = [None, None];
    if let Some(model) = rewrite.model {
        edits[0] = Some(Edit {
            span: head.model,
            with: model.clone(),
        });
    }
    if rewrite.inject_include_usage {
        edits[1] = include_usage_edit(body, head)?;
    }
    for edit in edits.iter().flatten() {
        if edit.span.start > edit.span.end || edit.span.end > body.len() {
            return Err(SpliceError::OutOfBounds(edit.span));
        }
    }

    let (first, second) = match edits {
        [None, None] => return Ok(Splice::identity(body.clone())),
        [Some(only), None] | [None, Some(only)] => (only, None),
        [Some(a), Some(b)] if a.span.start <= b.span.start => (a, Some(b)),
        [Some(a), Some(b)] => (b, Some(a)),
    };
    if let Some(second) = &second
        && first.span.end > second.span.start
    {
        return Err(SpliceError::Overlap);
    }

    let mut splice = Splice::empty();
    let mut cursor = 0;
    for edit in std::iter::once(first).chain(second) {
        splice.push(body.slice(cursor..edit.span.start));
        splice.push(edit.with);
        cursor = edit.span.end;
    }
    splice.push(body.slice(cursor..));

    #[cfg(debug_assertions)]
    debug_check_json(&splice);

    Ok(splice)
}

/// The edit that makes `stream_options.include_usage` true, or `None` when
/// it already is.
fn include_usage_edit(body: &Bytes, head: &ChatHead<'_>) -> Result<Option<Edit>, SpliceError> {
    let (span, with) = match head.stream_options {
        StreamOptions::Absent => (at(head.object_end), INSERT_STREAM_OPTIONS),
        StreamOptions::Null { value } => (value, STREAM_OPTIONS_OBJECT),
        StreamOptions::Object {
            include_usage:
                IncludeUsage::Present {
                    value: Some(true), ..
                },
            ..
        } => return Ok(None),
        StreamOptions::Object {
            include_usage: IncludeUsage::Present { span, .. },
            ..
        } => (span, TRUE),
        StreamOptions::Object {
            value,
            include_usage: IncludeUsage::Absent,
        } => {
            let inside = Span {
                start: value.start + 1,
                end: value.end.saturating_sub(1),
            };
            let members = inside.get(body).ok_or(SpliceError::OutOfBounds(value))?;
            let with = if members.iter().copied().all(is_json_whitespace) {
                INCLUDE_USAGE_ONLY
            } else {
                INCLUDE_USAGE_FIRST
            };
            (at(inside.start), with)
        }
    };
    Ok(Some(Edit {
        span,
        with: Bytes::from_static(with),
    }))
}

/// An empty span at `offset`: an insertion point.
const fn at(offset: usize) -> Span {
    Span {
        start: offset,
        end: offset,
    }
}

/// The pre-send check the design asks of debug builds: the rewritten body
/// still parses as JSON.
#[cfg(debug_assertions)]
fn debug_check_json(splice: &Splice) {
    use serde::Deserialize;

    let joined = splice.to_contiguous();
    let mut de = serde_json::Deserializer::from_slice(&joined);
    let parsed = crate::jsonhead::Skip::deserialize(&mut de).and_then(|_| de.end());
    debug_assert!(
        parsed.is_ok(),
        "plan_chat produced invalid JSON: {parsed:?}"
    );
}

/// JSON string literal for `value`, quotes included (escaping by
/// `serde_json`).
///
/// The result is already in `Bytes`' shared representation, so cloning it
/// on the request path only increments a reference count.
///
/// # Panics
///
/// Never: serializing a `str` into a `Vec` has no failure mode, and the
/// `expect` only states that to the type system.
pub fn json_string(value: &str) -> Bytes {
    let mut out = Vec::with_capacity(value.len() + 2);
    serde_json::to_writer(&mut out, value).expect("serializing a str into a Vec cannot fail");
    let bytes = Bytes::from(out);
    // A `Bytes` made from a `Vec` moves to its shared representation on the
    // first clone, which allocates; doing that here keeps the allocation off
    // the request path, where `plan_chat` clones it.
    drop(bytes.clone());
    bytes
}

#[cfg(test)]
mod tests {
    use super::*;

    fn apply(
        body: &str,
        model: Option<&str>,
        inject: bool,
    ) -> Result<(Splice, String), SpliceError> {
        let body = Bytes::copy_from_slice(body.as_bytes());
        let head = ChatHead::parse(&body).unwrap();
        let model = model.map(json_string);
        let splice = plan_chat(
            &body,
            &head,
            Rewrite {
                model: model.as_ref(),
                inject_include_usage: inject,
            },
        )?;
        let joined = splice.to_contiguous();
        assert_eq!(splice.len(), joined.len() as u64);
        assert!(splice.segments().len() <= MAX_SEGMENTS);
        assert!(splice.segments().iter().all(|segment| !segment.is_empty()));
        Ok((splice, String::from_utf8(joined.to_vec()).unwrap()))
    }

    fn rewritten(body: &str, model: Option<&str>, inject: bool) -> String {
        apply(body, model, inject).unwrap().1
    }

    #[test]
    fn no_edit_is_identity_without_copying() {
        let body = Bytes::from_static(br#"{"model":"grok-4.6(xhigh)","stream":true}"#);
        let head = ChatHead::parse(&body).unwrap();
        let splice = plan_chat(&body, &head, Rewrite::default()).unwrap();
        assert!(splice.is_identity());
        assert_eq!(splice.segments().len(), 1);
        assert_eq!(splice.segments()[0].as_ptr(), body.as_ptr());
        assert_eq!(splice.to_contiguous().as_ptr(), body.as_ptr());
        assert_eq!(splice.len(), body.len() as u64);
    }

    #[test]
    fn inject_when_include_usage_is_already_true_is_identity() {
        let (splice, out) = apply(
            r#"{"model":"m","stream":true,"stream_options":{"include_usage":true}}"#,
            None,
            true,
        )
        .unwrap();
        assert!(splice.is_identity());
        assert_eq!(
            out,
            r#"{"model":"m","stream":true,"stream_options":{"include_usage":true}}"#
        );
    }

    #[test]
    fn model_is_replaced_in_place() {
        let (splice, out) = apply(
            r#"{ "model" : "grok-4.6(xhigh)" , "messages":[] }"#,
            Some("grok-4.6-build"),
            false,
        )
        .unwrap();
        assert!(!splice.is_identity());
        assert_eq!(splice.segments().len(), 3);
        assert_eq!(out, r#"{ "model" : "grok-4.6-build" , "messages":[] }"#);
    }

    #[test]
    fn inject_into_every_stream_options_shape() {
        assert_eq!(
            rewritten(r#"{"model":"m","stream":true}"#, None, true),
            r#"{"model":"m","stream":true,"stream_options":{"include_usage":true}}"#
        );
        assert_eq!(
            rewritten(r#"{"model":"m","stream":true}  "#, None, true),
            r#"{"model":"m","stream":true,"stream_options":{"include_usage":true}}  "#
        );
        assert_eq!(
            rewritten(
                r#"{"model":"m","stream":true,"stream_options":null}"#,
                None,
                true
            ),
            r#"{"model":"m","stream":true,"stream_options":{"include_usage":true}}"#
        );
        assert_eq!(
            rewritten(
                r#"{"model":"m","stream":true,"stream_options":{"include_usage":false}}"#,
                None,
                true
            ),
            r#"{"model":"m","stream":true,"stream_options":{"include_usage":true}}"#
        );
        assert_eq!(
            rewritten(
                r#"{"model":"m","stream":true,"stream_options":{"include_usage":null}}"#,
                None,
                true
            ),
            r#"{"model":"m","stream":true,"stream_options":{"include_usage":true}}"#
        );
        assert_eq!(
            rewritten(
                r#"{"model":"m","stream":true,"stream_options":{ }}"#,
                None,
                true
            ),
            r#"{"model":"m","stream":true,"stream_options":{"include_usage":true }}"#
        );
        assert_eq!(
            rewritten(
                r#"{"model":"m","stream":true,"stream_options":{"x":1}}"#,
                None,
                true
            ),
            r#"{"model":"m","stream":true,"stream_options":{"include_usage":true,"x":1}}"#
        );
    }

    #[test]
    fn model_and_injection_together_use_five_segments() {
        let (splice, out) = apply(
            r#"{"model":"a","stream":true,"stream_options":{"include_usage":false},"n":1}"#,
            Some("b"),
            true,
        )
        .unwrap();
        assert_eq!(splice.segments().len(), MAX_SEGMENTS);
        assert_eq!(
            out,
            r#"{"model":"b","stream":true,"stream_options":{"include_usage":true},"n":1}"#
        );

        // Edits are applied in body order whatever order the members have.
        let out = rewritten(
            r#"{"stream_options":null,"stream":true,"model":"a"}"#,
            Some("b"),
            true,
        );
        assert_eq!(
            out,
            r#"{"stream_options":{"include_usage":true},"stream":true,"model":"b"}"#
        );
    }

    #[test]
    fn inject_without_stream_is_an_error() {
        for body in [
            r#"{"model":"m"}"#,
            r#"{"model":"m","stream":false}"#,
            r#"{"model":"m","stream":null}"#,
        ] {
            assert!(matches!(
                apply(body, None, true),
                Err(SpliceError::InjectWithoutStream)
            ));
        }
    }

    #[test]
    fn head_from_another_body_is_rejected() {
        let long = Bytes::from_static(br#"{"model":"a-long-model-name","stream":true}"#);
        let short = Bytes::from_static(br#"{"model":"m"}"#);
        let head = ChatHead::parse(&long).unwrap();
        let model = json_string("x");
        let rewrite = Rewrite {
            model: Some(&model),
            inject_include_usage: false,
        };
        assert!(matches!(
            plan_chat(&short, &head, rewrite),
            Err(SpliceError::OutOfBounds(_))
        ));
    }

    #[test]
    fn overlapping_edits_are_rejected() {
        let body = Bytes::from_static(br#"{"model":"m","stream":true,"stream_options":null}"#);
        let mut head = ChatHead::parse(&body).unwrap();
        head.stream_options = StreamOptions::Null {
            value: Span {
                start: head.model.start,
                end: head.model.end + 2,
            },
        };
        let model = json_string("x");
        let rewrite = Rewrite {
            model: Some(&model),
            inject_include_usage: true,
        };
        assert!(matches!(
            plan_chat(&body, &head, rewrite),
            Err(SpliceError::Overlap)
        ));
    }

    #[test]
    fn json_string_escapes_like_serde_json() {
        assert_eq!(&json_string("grok-4.6(xhigh)")[..], br#""grok-4.6(xhigh)""#);
        assert_eq!(
            &json_string("a\"b\\c\nd\u{1}")[..],
            br#""a\"b\\c\nd\u0001""#
        );
        assert_eq!(&json_string("")[..], br#""""#);
        assert_eq!(&json_string("\u{4e2d}/")[..], "\"\u{4e2d}/\"".as_bytes());
    }

    #[test]
    fn identity_of_an_empty_body_has_no_segments() {
        let splice = Splice::identity(Bytes::new());
        assert!(splice.is_identity());
        assert!(splice.is_empty());
        assert!(splice.segments().is_empty());
        assert!(splice.to_contiguous().is_empty());
    }
}
