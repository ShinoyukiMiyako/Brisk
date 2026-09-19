//! `SseScanner` on arbitrary bytes cut at arbitrary offsets (contract 05,
//! 4.8): every cut must frame exactly like the whole-stream reference framer
//! shared with `tests/sse_props.rs`, and nothing may panic.

#![no_main]

use brisk_proto::sse::MAX_EVENT_BYTES;
use libfuzzer_sys::arbitrary::{self, Arbitrary};
use libfuzzer_sys::{Corpus, fuzz_target};

#[path = "../../tests/common/sse_ref.rs"]
mod sse_ref;

#[derive(Debug, Arbitrary)]
struct Input<'a> {
    /// Cut offsets, reduced modulo the stream length.
    cuts: Vec<u16>,
    stream: &'a [u8],
}

fuzz_target!(|input: Input<'_>| -> Corpus {
    let len = input.stream.len();
    // Past the event size limit the scanner rightly fails where the
    // reference, which has no limit, does not; those inputs are not kept.
    if len > MAX_EVENT_BYTES {
        return Corpus::Reject;
    }
    let mut cuts: Vec<usize> = if len < 2 {
        Vec::new()
    } else {
        input
            .cuts
            .iter()
            .map(|&cut| 1 + usize::from(cut) % (len - 1))
            .collect()
    };
    cuts.sort_unstable();
    cuts.dedup();
    if let Err(mismatch) = sse_ref::verify_split(input.stream, &cuts) {
        panic!("split framing differs from the whole stream: {mismatch}");
    }
    Corpus::Keep
});
