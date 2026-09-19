//! Allocation gate for `SseScanner::feed` (contract 05, 4.7 and R9).
//!
//! Ten thousand events of up to `MAX_EVENT` bytes are fed in random pieces,
//! so many of them span chunks. The scanner may allocate only while its
//! carry buffer grows to the largest event seen, which the warm-up does by
//! starting with an event of exactly that size, split across chunks. After
//! that, framing must not allocate at all, and neither may one more event
//! spanning chunks that is no larger than the warm-up one.
//!
//! The file holds a single test because dhat's profiler is process-wide.

use brisk_proto::sse::{Data, EndState, SseScanner};

#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

const EVENTS: usize = 10_000;
/// Size of the largest event, its terminating blank line excluded; fixed
/// before the stream is generated.
const MAX_EVENT: usize = 4096;
/// Chunks are 1 to `MAX_PIECE` bytes, well under `MAX_EVENT`, so the large
/// events always span several chunks.
const MAX_PIECE: usize = 512;

/// xorshift64: deterministic, and generated entirely before counting starts.
struct Rng(u64);

impl Rng {
    fn below(&mut self, bound: usize) -> usize {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        usize::try_from(x % bound as u64).expect("below a usize bound")
    }

    /// A value in `low..=high`.
    fn between(&mut self, low: usize, high: usize) -> usize {
        low + self.below(high - low + 1)
    }
}

/// Appends one `data` event whose lines total `size` bytes, followed by its
/// blank line; line endings vary so every framing path is exercised.
fn push_event(stream: &mut Vec<u8>, size: usize, rng: &mut Rng) {
    let eol: &[u8] = match rng.below(3) {
        0 => b"\n",
        1 => b"\r\n",
        _ => b"\r",
    };
    let prefix = b"data: ";
    let filler = size - prefix.len() - eol.len();
    stream.extend_from_slice(prefix);
    stream.extend((0..filler).map(|i| b'a' + u8::try_from(i % 26).expect("below 26")));
    stream.extend_from_slice(eol);
    stream.extend_from_slice(eol);
}

/// Chunk ranges covering `len` bytes in pieces of 1 to `MAX_PIECE` bytes,
/// one-byte pieces included.
fn pieces(len: usize, rng: &mut Rng) -> Vec<(usize, usize)> {
    let mut ranges = Vec::new();
    let mut start = 0;
    while start < len {
        let size = if rng.below(8) == 0 {
            1
        } else {
            rng.between(1, MAX_PIECE)
        };
        let end = (start + size).min(len);
        ranges.push((start, end));
        start = end;
    }
    ranges
}

#[test]
fn feed_does_not_allocate_after_warm_up() {
    let mut rng = Rng(0x9E37_79B9_7F4A_7C15);

    let mut stream = Vec::new();
    push_event(&mut stream, MAX_EVENT, &mut rng);
    let first_event_end = stream.len();
    for _ in 1..EVENTS {
        let size = rng.between(16, MAX_EVENT);
        push_event(&mut stream, size, &mut rng);
    }
    let chunks = pieces(stream.len(), &mut rng);
    // Warm-up ends with the chunk that completes the first, largest event.
    let warm_up = chunks
        .iter()
        .position(|&(_, end)| end >= first_event_end)
        .expect("the first event ends inside the stream")
        + 1;
    assert!(warm_up > 1, "the largest event must span chunks");

    let mut extra = Vec::new();
    push_event(&mut extra, MAX_EVENT - 100, &mut rng);
    let extra_chunks = pieces(extra.len(), &mut rng);
    assert!(extra_chunks.len() > 1, "the extra event must span chunks");

    let profiler = dhat::Profiler::builder().testing().build();
    let mut scanner = SseScanner::new();
    let mut events = 0_usize;
    let mut data_bytes = 0_usize;
    let mut on_event = |event: brisk_proto::sse::Event<'_>| {
        events += 1;
        if let Data::Single(value) = event.data() {
            data_bytes += value.len();
        }
    };

    let start = dhat::HeapStats::get();
    for &(from, to) in &chunks[..warm_up] {
        scanner
            .feed(&stream[from..to], &mut on_event)
            .expect("event within limit");
    }
    let warmed = dhat::HeapStats::get();
    for &(from, to) in &chunks[warm_up..] {
        scanner
            .feed(&stream[from..to], &mut on_event)
            .expect("event within limit");
    }
    let steady = dhat::HeapStats::get();
    for &(from, to) in &extra_chunks {
        scanner
            .feed(&extra[from..to], &mut on_event)
            .expect("event within limit");
    }
    let after_extra = dhat::HeapStats::get();
    let end = scanner.finish();

    let warm_up_blocks = warmed.total_blocks - start.total_blocks;
    let steady_blocks = steady.total_blocks - warmed.total_blocks;
    let extra_blocks = after_extra.total_blocks - steady.total_blocks;
    println!(
        "dhat_sse: {} chunks, {events} events; allocations: warm-up {warm_up_blocks}, \
         steady {steady_blocks}, extra spanning event {extra_blocks}",
        chunks.len() + extra_chunks.len(),
    );

    assert_eq!(events, EVENTS + 1);
    assert!(data_bytes > 0);
    assert_eq!(end, EndState::Clean);
    dhat::assert_eq!(steady_blocks, 0);
    dhat::assert_eq!(extra_blocks, 0);
    drop(profiler);
}
