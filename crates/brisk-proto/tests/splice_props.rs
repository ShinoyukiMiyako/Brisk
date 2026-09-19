//! Property tests for `plan_chat` (contract 05, 4.2 `splice_props`): for any
//! generated request and any `Rewrite`, the output is the edit table applied
//! to the body byte for byte, parses to the input with `model` replaced and
//! `stream_options.include_usage` set, has an exact `len()` and at most five
//! segments, and injecting into a non-streaming request is refused.

mod common;

use brisk_proto::jsonhead::ChatHead;
use bytes::Bytes;
use proptest::option;
use proptest::prelude::*;
use proptest::test_runner::TestCaseError;

use common::json::{Json, Style, model_name, request, style, write};
use common::oracle::verify_splice;

proptest! {
    #[test]
    fn output_follows_the_edit_table(
        request in request(true),
        model in option::of(model_name()),
        inject in any::<bool>(),
        style in style(),
    ) {
        let body = Bytes::from(request.body(style));
        let head = ChatHead::parse(&body)
            .map_err(|error| TestCaseError::fail(format!("generated request rejected: {error}")))?;
        verify_splice(&body, &head, model.as_deref(), inject).map_err(TestCaseError::fail)?;
    }

    /// Streaming requests exercise every injection row of the table.
    #[test]
    fn injection_into_streaming_requests(
        request in request(true),
        model in option::of(model_name()),
        style in style(),
    ) {
        let mut request = request;
        request.stream = Some(Json::Bool(true));
        let body = Bytes::from(request.body(style));
        let head = ChatHead::parse(&body)
            .map_err(|error| TestCaseError::fail(format!("generated request rejected: {error}")))?;
        verify_splice(&body, &head, model.as_deref(), true).map_err(TestCaseError::fail)?;
    }
}

#[test]
fn identity_keeps_the_bracketed_model() {
    let body = Bytes::from(write(
        &Json::Obj(vec![
            ("model".to_owned(), Json::Str(common::TEST_MODEL.to_owned())),
            ("stream".to_owned(), Json::Bool(true)),
        ]),
        Style::COMPACT,
    ));
    let head = ChatHead::parse(&body).unwrap();
    verify_splice(&body, &head, None, false).unwrap();
    verify_splice(&body, &head, Some(common::TEST_MODEL), true).unwrap();
}
