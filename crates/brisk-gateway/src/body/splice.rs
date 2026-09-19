//! The rewritten upstream request body (2.1, step 10): the segments of a
//! [`Splice`] sent one frame each, with an exact size hint so that hyper
//! writes `Content-Length` instead of chunked framing.

use std::convert::Infallible;
use std::pin::Pin;
use std::task::{Context, Poll};

use brisk_proto::splice::Splice;
use bytes::Bytes;
use http_body::{Body, Frame, SizeHint};

/// Upstream request body over `Splice` segments with an exact size hint, so
/// hyper writes Content-Length. Used only for non-identity splices in
/// `segments` mode (2.1 step 10).
#[derive(Debug)]
pub struct SpliceBody {
    splice: Splice,
    /// Index of the next segment to send.
    next: usize,
    /// Bytes of the segments not sent yet.
    remaining: u64,
}

impl SpliceBody {
    /// A body sending `splice`'s segments in order.
    pub fn new(splice: Splice) -> Self {
        let remaining = splice.len();
        Self {
            splice,
            next: 0,
            remaining,
        }
    }
}

impl Body for SpliceBody {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Infallible>>> {
        let this = self.get_mut();
        let Some(segment) = this.splice.segments().get(this.next) else {
            return Poll::Ready(None);
        };
        this.next += 1;
        this.remaining -= segment.len() as u64;
        // A reference-count increment: segments are slices of the request
        // body or static replacements, never copies.
        Poll::Ready(Some(Ok(Frame::data(segment.clone()))))
    }

    fn is_end_stream(&self) -> bool {
        self.next >= self.splice.segments().len()
    }

    fn size_hint(&self) -> SizeHint {
        SizeHint::with_exact(self.remaining)
    }
}

#[cfg(test)]
mod tests {
    use brisk_proto::jsonhead::ChatHead;
    use brisk_proto::splice::{Rewrite, json_string, plan_chat};
    use http_body_util::BodyExt;

    use super::*;

    const BODY: &[u8] = br#"{"model":"grok-4.6(xhigh)","stream":true,"messages":[{"role":"user","content":"Reply with exactly: pong"}]}"#;

    fn two_edit_splice() -> (Splice, Vec<u8>) {
        let body = Bytes::from_static(BODY);
        let head = ChatHead::parse(&body).expect("valid request");
        let model = json_string("grok-4.6-upstream");
        let splice = plan_chat(
            &body,
            &head,
            Rewrite {
                model: Some(&model),
                inject_include_usage: true,
            },
        )
        .expect("valid rewrite");
        let expected = splice.to_contiguous().to_vec();
        (splice, expected)
    }

    #[tokio::test]
    async fn sends_every_segment_in_order_with_an_exact_size() {
        let (splice, expected) = two_edit_splice();
        let segments = splice.segments().to_vec();
        assert!(segments.len() > 1, "the rewrite needs several segments");
        let mut body = SpliceBody::new(splice);
        assert_eq!(body.size_hint().exact(), Some(expected.len() as u64));
        assert!(!body.is_end_stream());

        let mut sent = Vec::new();
        let mut remaining = expected.len() as u64;
        for segment in &segments {
            let frame = body.frame().await.expect("a frame").expect("infallible");
            let data = frame.into_data().expect("a data frame");
            // The segment itself, not a copy.
            assert_eq!(data.as_ptr(), segment.as_ptr());
            assert_eq!(data.len(), segment.len());
            remaining -= data.len() as u64;
            assert_eq!(body.size_hint().exact(), Some(remaining));
            sent.extend_from_slice(&data);
        }
        assert!(body.is_end_stream());
        assert!(body.frame().await.is_none());
        assert_eq!(sent, expected);
        assert!(sent.windows(19).any(|w| w == br#""grok-4.6-upstream""#));
        assert!(
            sent.ends_with(br#","stream_options":{"include_usage":true}}"#),
            "include_usage is inserted before the closing brace"
        );
    }

    #[tokio::test]
    async fn identity_splice_is_one_frame() {
        let body = Bytes::from_static(BODY);
        let mut splice_body = SpliceBody::new(Splice::identity(body.clone()));
        assert_eq!(splice_body.size_hint().exact(), Some(BODY.len() as u64));
        let frame = splice_body
            .frame()
            .await
            .expect("a frame")
            .expect("infallible");
        assert_eq!(frame.into_data().expect("data").as_ptr(), body.as_ptr());
        assert!(splice_body.is_end_stream());
        assert_eq!(splice_body.size_hint().exact(), Some(0));
    }
}
