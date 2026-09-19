//! Brisk protocol layer: the synchronous parsing and rewriting that runs on
//! every request and every streamed chunk.
//!
//! - [`jsonhead`]: borrowed parsing of the request-body members Brisk routes
//!   and bills on.
//! - [`splice`]: zero-copy request-body rewriting.
//! - [`sse`]: incremental SSE event framing.
//! - [`usage`]: usage, finish and error detection in response payloads.
//! - [`tokens`]: the token counts settlement works with.
//!
//! The crate performs no I/O and depends on no async runtime, so everything in
//! it can be tested, fuzzed and measured in isolation.

#![forbid(unsafe_code)]

pub mod jsonhead;
pub mod splice;
pub mod sse;
pub mod tokens;
pub mod usage;

pub use tokens::{UsageErrorKind, UsageTokens};

/// Half-open byte range `[start, end)` into a buffer the caller owns.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Span {
    /// Offset of the first byte.
    pub start: usize,
    /// Offset just past the last byte.
    pub end: usize,
}

impl Span {
    /// Number of bytes covered. As with `Range`, a span whose `start` exceeds
    /// its `end` covers nothing.
    pub const fn len(self) -> usize {
        self.end.saturating_sub(self.start)
    }

    /// True when the span covers no byte.
    pub const fn is_empty(self) -> bool {
        self.start >= self.end
    }

    /// `None` when the span does not lie inside `buf`.
    pub fn get(self, buf: &[u8]) -> Option<&[u8]> {
        buf.get(self.start..self.end)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn len_and_is_empty() {
        let span = Span { start: 2, end: 5 };
        assert_eq!(span.len(), 3);
        assert!(!span.is_empty());

        let empty = Span { start: 4, end: 4 };
        assert_eq!(empty.len(), 0);
        assert!(empty.is_empty());

        let reversed = Span { start: 5, end: 2 };
        assert_eq!(reversed.len(), 0);
        assert!(reversed.is_empty());
    }

    #[test]
    fn get_slices_inside_the_buffer() {
        let body = br#"{"model":"m"}"#;
        assert_eq!(Span { start: 9, end: 12 }.get(body), Some(&br#""m""#[..]));
        assert_eq!(
            Span {
                start: 0,
                end: body.len()
            }
            .get(body),
            Some(&body[..])
        );
        let at_end = Span {
            start: body.len(),
            end: body.len(),
        };
        assert_eq!(at_end.get(body), Some(&b""[..]));
    }

    #[test]
    fn get_rejects_spans_outside_the_buffer() {
        let body = b"abc";
        assert_eq!(Span { start: 1, end: 4 }.get(body), None);
        assert_eq!(Span { start: 4, end: 4 }.get(body), None);
        assert_eq!(Span { start: 2, end: 1 }.get(body), None);
    }
}
