//! `ChatHead::parse` on arbitrary bytes, differentially against the
//! `serde_json::Value` oracle shared with `tests/jsonhead_props.rs`
//! (contract 05, 4.8): same outcome, same decoded values, spans that cut out
//! the right values, and `AmbiguousKey` whenever a key folds onto a
//! recognized member.

#![no_main]

use libfuzzer_sys::fuzz_target;

#[path = "../../tests/common/oracle.rs"]
#[allow(
    dead_code,
    reason = "this target uses only the head half of the oracle"
)]
mod oracle;

fuzz_target!(|body: &[u8]| {
    if let Err(mismatch) = oracle::verify_head(body) {
        panic!("ChatHead disagrees with the Value oracle: {mismatch}");
    }
});
