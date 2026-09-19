//! `plan_chat` on every body `ChatHead::parse` accepts, with an arbitrary
//! replacement model and injection flag (contract 05, 4.8): the output must
//! parse as JSON and satisfy the `tests/splice_props.rs` equations, checked
//! by the oracle both share.

#![no_main]

use brisk_proto::jsonhead::ChatHead;
use bytes::Bytes;
use libfuzzer_sys::arbitrary::{self, Arbitrary};
use libfuzzer_sys::fuzz_target;

#[path = "../../tests/common/oracle.rs"]
#[allow(
    dead_code,
    reason = "this target uses only the splice half of the oracle"
)]
mod oracle;

#[derive(Debug, Arbitrary)]
struct Input<'a> {
    inject: bool,
    model: Option<&'a str>,
    body: &'a [u8],
}

fuzz_target!(|input: Input<'_>| {
    let body = Bytes::copy_from_slice(input.body);
    let Ok(head) = ChatHead::parse(&body) else {
        return;
    };
    if let Err(mismatch) = oracle::verify_splice(&body, &head, input.model, input.inject) {
        panic!("plan_chat broke an equation: {mismatch}");
    }
});
