//! Framing cases for `SseScanner` (contract 05, 1.3.4 and 4.2), including
//! every shape CPA-M1-4 lists and the CPA fixtures.

use brisk_proto::sse::{ChunkScan, Data, EndState, MAX_EVENT_BYTES, SseError, SseScanner};

macro_rules! fixture {
    ($name:literal) => {
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../fixtures/cpa/",
            $name
        ))
        .as_slice()
    };
}

/// Owned copy of one reported event.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Seen {
    raw: Vec<u8>,
    data: Option<Vec<u8>>,
    event: Option<Vec<u8>>,
    comment_only: bool,
}

/// Everything the scanner reported for one chunked stream.
#[derive(Debug)]
struct Run {
    events: Vec<Seen>,
    scans: Vec<ChunkScan>,
    end: EndState,
}

fn run(chunks: &[&[u8]]) -> Run {
    let mut scanner = SseScanner::new();
    let mut events = Vec::new();
    let mut scans = Vec::new();
    for chunk in chunks {
        let scan = scanner
            .feed(chunk, |event| {
                let data = match event.data() {
                    Data::None => None,
                    Data::Single(value) => Some(value.to_vec()),
                    Data::Multi => {
                        let mut joined = Vec::new();
                        event.join_data(&mut joined);
                        Some(joined)
                    }
                };
                events.push(Seen {
                    raw: event.raw.to_vec(),
                    data,
                    event: event.field(b"event").map(<[u8]>::to_vec),
                    comment_only: event.is_comment_only(),
                });
            })
            .unwrap();
        scans.push(scan);
    }
    Run {
        events,
        scans,
        end: scanner.finish(),
    }
}

fn whole(stream: &[u8]) -> Run {
    run(&[stream])
}

fn bytewise(stream: &[u8]) -> Run {
    let chunks: Vec<&[u8]> = stream.chunks(1).collect();
    run(&chunks)
}

fn datas(run: &Run) -> Vec<Option<&[u8]>> {
    run.events.iter().map(|seen| seen.data.as_deref()).collect()
}

#[test]
fn keep_alive_comment_is_a_comment_only_event() {
    let run = whole(b": keep-alive\n\ndata: {}\n\n");
    assert_eq!(run.events.len(), 2);
    assert!(run.events[0].comment_only);
    assert_eq!(run.events[0].data, None);
    assert!(!run.events[1].comment_only);
    assert_eq!(run.end, EndState::Clean);
}

#[test]
fn blank_lines_alone_are_not_events() {
    let run = whole(b"\n\n\ndata: a\n\n\n\n\ndata: b\n\n\n");
    assert_eq!(datas(&run), [Some(&b"a"[..]), Some(&b"b"[..])]);
    assert_eq!(run.end, EndState::Clean);
    // Leading and trailing blank lines of a Responses stream.
    let run = whole(b"\nevent: response.created\ndata: {}\n\n\n");
    assert_eq!(run.events.len(), 1);
    assert_eq!(
        run.events[0].event.as_deref(),
        Some(&b"response.created"[..])
    );
    assert_eq!(run.end, EndState::Clean);
}

#[test]
fn data_with_and_without_a_space() {
    let run = whole(b"data: x\n\ndata:x\n\ndata:  x\n\ndata:\n\n");
    assert_eq!(
        datas(&run),
        [
            Some(&b"x"[..]),
            Some(&b"x"[..]),
            Some(&b" x"[..]),
            Some(&b""[..])
        ]
    );
}

#[test]
fn every_line_ending_and_mixes() {
    let lf = whole(b"event: e\ndata: 1\n\n");
    let crlf = whole(b"event: e\r\ndata: 1\r\n\r\n");
    let cr = whole(b"event: e\rdata: 1\r\r");
    let mixed = whole(b"event: e\rdata: 1\n\r\n");
    for run in [&lf, &crlf, &cr, &mixed] {
        assert_eq!(run.events.len(), 1);
        assert_eq!(run.events[0].event.as_deref(), Some(&b"e"[..]));
        assert_eq!(run.events[0].data.as_deref(), Some(&b"1"[..]));
        assert_eq!(run.end, EndState::Clean);
    }
    assert_eq!(cr.events[0].raw, b"event: e\rdata: 1\r");
    // `\r\n` is one line ending, `\n\r` two: the second line is blank.
    let run = whole(b"data: 1\n\rdata: 2\r\n\n");
    assert_eq!(datas(&run), [Some(&b"1"[..]), Some(&b"2"[..])]);
}

#[test]
fn multi_line_data_joins_with_newlines() {
    let run = whole(b"data: a\ndata: b\r\ndata:c\n\n");
    assert_eq!(run.events[0].data.as_deref(), Some(&b"a\nb\nc"[..]));
}

#[test]
fn comment_inside_an_event_belongs_to_it() {
    let stream = b": xai-usage {\"input_tokens\":213}\nevent: response.output_text.done\ndata: {\"x\":1}\n\n";
    let run = whole(stream);
    assert_eq!(run.events.len(), 1);
    let seen = &run.events[0];
    assert!(!seen.comment_only);
    assert_eq!(
        seen.event.as_deref(),
        Some(&b"response.output_text.done"[..])
    );
    assert_eq!(seen.data.as_deref(), Some(&b"{\"x\":1}"[..]));
}

#[test]
fn leading_bom_is_ignored() {
    let run = whole(b"\xEF\xBB\xBFdata: a\n\n");
    assert_eq!(run.events[0].raw, b"data: a\n");
    assert_eq!(bytewise(b"\xEF\xBB\xBFdata: a\n\n").events, run.events);
}

#[test]
fn field_without_a_colon_has_an_empty_value() {
    let run = whole(b"data\nevent\n\n");
    assert_eq!(run.events[0].data.as_deref(), Some(&b""[..]));
    assert_eq!(run.events[0].event.as_deref(), Some(&b""[..]));
}

#[test]
fn cr_at_chunk_end_and_lf_at_next_start() {
    let split = run(&[b"data: a\r", b"\n\r", b"\ndata: b\r", b"\r\n"]);
    let joined = whole(b"data: a\r\n\r\ndata: b\r\r\n");
    assert_eq!(split.events, joined.events);
    assert_eq!(datas(&split), [Some(&b"a"[..]), Some(&b"b"[..])]);
    assert_eq!(split.end, EndState::Clean);
    // The `\n` opening the third chunk only completes the blank line.
    assert_eq!(split.scans[1].last_boundary, Some(2));
    assert_eq!(split.scans[2].last_boundary, Some(1));
}

#[test]
fn at_boundary_is_false_while_a_cr_is_pending() {
    let mut scanner = SseScanner::new();
    assert!(scanner.at_boundary());
    scanner.feed(b"data: a\n\r", |_| {}).unwrap();
    assert!(!scanner.at_boundary());
    scanner.feed(b"\n", |_| {}).unwrap();
    assert!(scanner.at_boundary());
    scanner.feed(b"data: b\n", |_| {}).unwrap();
    assert!(!scanner.at_boundary());
    scanner.feed(b"\n", |_| {}).unwrap();
    assert!(scanner.at_boundary());
    scanner.feed(b"\r", |_| {}).unwrap();
    assert!(!scanner.at_boundary());
    scanner.feed(b"data: c", |_| {}).unwrap();
    assert!(!scanner.at_boundary());
}

#[test]
fn event_offsets_within_a_chunk() {
    let mut scanner = SseScanner::new();
    let mut offsets = Vec::new();
    let scan = scanner
        .feed(b"\n: ka\n\ndata: 1\r\n\r\ndata: 2", |event| {
            offsets.push((event.start, event.end));
        })
        .unwrap();
    assert_eq!(offsets, [(Some(1), 7), (Some(7), 18)]);
    assert_eq!(scan.last_boundary, Some(18));
    assert_eq!(scanner.pending_bytes(), 7);
}

#[test]
fn oversized_events_fail() {
    let limit = || SseError::EventTooLarge {
        limit: MAX_EVENT_BYTES,
    };
    let mut big = b"data: ".to_vec();
    big.resize(MAX_EVENT_BYTES + 10, b'x');

    // Unfinished in one chunk.
    let mut scanner = SseScanner::new();
    assert_eq!(scanner.feed(&big, |_| {}), Err(limit()));

    // Carried across chunks.
    let mut scanner = SseScanner::new();
    let (head, tail) = big.split_at(MAX_EVENT_BYTES / 2);
    scanner.feed(head, |_| {}).unwrap();
    assert_eq!(scanner.feed(tail, |_| {}), Err(limit()));

    // Complete in one chunk.
    let mut complete = big.clone();
    complete.extend_from_slice(b"\n\n");
    let mut scanner = SseScanner::new();
    let mut called = false;
    assert_eq!(scanner.feed(&complete, |_| called = true), Err(limit()));
    assert!(!called);
}

#[test]
fn truncated_event_at_eof() {
    let run = whole(b"data: a\n\ndata: {\"partial\"");
    assert_eq!(run.events.len(), 1);
    assert_eq!(run.end, EndState::Truncated { pending: 16 });

    // A finished line without its blank line is still unfinished.
    let run = whole(b"data: [DONE]\n");
    assert!(run.events.is_empty());
    assert_eq!(run.end, EndState::Truncated { pending: 13 });
}

#[test]
fn chat_fixtures_frame_into_their_data_events() {
    for (stream, count) in [
        (fixture!("chat-stream-grok46-xhigh.sse"), 16),
        (fixture!("chat-stream-grok46-suffix.sse"), 17),
        (fixture!("chat-stream-grok43.sse"), 15),
        (fixture!("chat-stream-gpt55.sse"), 3),
    ] {
        let run = whole(stream);
        assert_eq!(run.events.len(), count);
        assert_eq!(
            run.events.last().unwrap().data.as_deref(),
            Some(&b"[DONE]"[..])
        );
        assert!(run.events.iter().all(|seen| seen.event.is_none()));
        assert_eq!(run.end, EndState::Clean);
        assert_eq!(run.scans[0].last_boundary, Some(stream.len()));
        assert_eq!(bytewise(stream).events, run.events);
    }
}

#[test]
fn responses_fixture_keeps_comment_lines_inside_events() {
    let stream = fixture!("responses-stream-grok46-xhigh.sse");
    let run = whole(stream);
    assert_eq!(run.events.len(), 26);
    assert!(run.events.iter().all(|seen| seen.event.is_some()));
    let with_usage: Vec<&Seen> = run
        .events
        .iter()
        .filter(|seen| seen.raw.starts_with(b": xai-usage"))
        .collect();
    assert_eq!(with_usage.len(), 2);
    assert!(
        with_usage
            .iter()
            .all(|seen| !seen.comment_only && seen.data.is_some())
    );
    assert_eq!(
        run.events.last().unwrap().event.as_deref(),
        Some(&b"response.completed"[..])
    );
    // The stream ends in an extra blank line, which is not an event.
    assert!(stream.ends_with(b"\n\n\n"));
    assert_eq!(run.end, EndState::Clean);
    assert_eq!(bytewise(stream).events, run.events);
}

#[test]
fn anthropic_and_gemini_fixtures_frame_cleanly() {
    let anthropic = fixture!("anthropic-stream-grok46-suffix.sse");
    let run = whole(anthropic);
    assert_eq!(run.events.len(), 22);
    assert_eq!(run.events[0].event.as_deref(), Some(&b"message_start"[..]));
    assert_eq!(bytewise(anthropic).events, run.events);

    let gemini = fixture!("gemini-sse-grok46-suffix.sse");
    let run = whole(gemini);
    assert_eq!(run.events.len(), 17);
    assert!(run.events.iter().all(|seen| seen.event.is_none()));
    assert_eq!(bytewise(gemini).events, run.events);
}

#[test]
fn fixtures_with_crlf_line_endings_frame_the_same() {
    let stream = fixture!("chat-stream-grok46-xhigh.sse");
    let crlf: Vec<u8> = stream
        .iter()
        .flat_map(|&byte| {
            if byte == b'\n' {
                vec![b'\r', b'\n']
            } else {
                vec![byte]
            }
        })
        .collect();
    let lf = whole(stream);
    let run = bytewise(&crlf);
    assert_eq!(datas(&run), datas(&lf));
    assert_eq!(run.end, EndState::Clean);
}
