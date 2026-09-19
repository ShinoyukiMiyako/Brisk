//! Shard event-loop tests against a small blocking responder.

use std::collections::BTreeMap;
use std::io::{Read as _, Write as _};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::mpsc;
use std::thread;

use brisk_bench_core::http1::{BodyFraming, LAST_CHUNK, request_body_framing, write_chunk};
use brisk_bench_core::wire::{MARKER_LEN_U32, Marker, append_content, find_directive};

use super::*;
use crate::request::{BodySpec, RequestSpec, RequestTemplate};
use crate::schedule::ramp_segments;

/// Chunks per streaming request.
const CHUNKS: u32 = 3;
/// Chunk interval the requests ask for.
const INTERVAL_NS: u64 = 1_000_000;
/// Content length of the non-streaming responses: room for a marker.
const RESP_BYTES: u32 = 100;

const STREAM: Mode = Mode::Stream {
    ttft_ns: 0,
    interval_ns: INTERVAL_NS,
};
const WHOLE: Mode = Mode::Whole {
    ttft_ns: 0,
    marker: true,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Behavior {
    /// Answer every request.
    Serve,
    /// Answer the first request of a connection, then read the second one
    /// and close without answering: a keep-alive race lost by the client.
    DropSecondRequest,
    /// Like [`Self::DropSecondRequest`], but close only after working on
    /// the second request for a while: a server failure, not a race.
    DropSecondRequestLate,
    /// Read the request and close without answering.
    CloseWithoutResponse,
    /// Read the request and never answer.
    Hold,
    /// Send the head and the first chunk of a stream, then stall.
    StallAfterFirstChunk,
}

fn spawn_server(behavior: Behavior) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    thread::spawn(move || {
        for stream in listener.incoming() {
            let stream = stream.unwrap();
            thread::spawn(move || serve(stream, behavior));
        }
    });
    addr
}

/// Reads one request; returns its body, or `None` at EOF.
fn read_request(stream: &mut TcpStream, buf: &mut Vec<u8>) -> Option<Vec<u8>> {
    loop {
        let mut headers = [httparse::EMPTY_HEADER; 32];
        let mut req = httparse::Request::new(&mut headers);
        if let httparse::Status::Complete(head) = req.parse(buf).unwrap() {
            let BodyFraming::Length(len) = request_body_framing(req.headers).unwrap() else {
                panic!("loadgen sends content-length bodies");
            };
            let total = head + usize::try_from(len).unwrap();
            if buf.len() >= total {
                let body = buf[head..total].to_vec();
                buf.drain(..total);
                return Some(body);
            }
        }
        let mut chunk = vec![0u8; 65_536];
        match stream.read(&mut chunk) {
            Ok(0) | Err(_) => return None,
            Ok(n) => buf.extend_from_slice(&chunk[..n]),
        }
    }
}

const SSE_HEAD: &[u8] =
    b"HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ntransfer-encoding: chunked\r\n\r\n";

/// One chunked SSE event carrying marker `seq` of stream `sid`.
fn sse_event(out: &mut Vec<u8>, sid: u64, seq: u32) {
    let mut event = b"data: {\"choices\":[{\"delta\":{\"content\":\"".to_vec();
    let t = now_ns();
    append_content(
        &mut event,
        &Marker {
            sid,
            seq,
            t_sched: t,
            t_write: t,
        },
        100,
    );
    event.extend_from_slice(b"\"}}]}\n\n");
    write_chunk(out, &event);
}

fn sse_response(sid: u64, chunks: u32) -> Vec<u8> {
    let mut out = SSE_HEAD.to_vec();
    for seq in 0..chunks {
        sse_event(&mut out, sid, seq);
    }
    write_chunk(&mut out, b"data: [DONE]\n\n");
    out.extend_from_slice(LAST_CHUNK);
    out
}

/// A `chat.completion` like the mock's: the content carries one marker when
/// `resp_bytes` has room for it.
fn completion_response(sid: u64, resp_bytes: u32) -> Vec<u8> {
    let mut body =
        b"{\"object\":\"chat.completion\",\"choices\":[{\"message\":{\"content\":\"".to_vec();
    if resp_bytes >= MARKER_LEN_U32 {
        let t = now_ns();
        let marker = Marker {
            sid,
            seq: 0,
            t_sched: t,
            t_write: t,
        };
        append_content(&mut body, &marker, resp_bytes);
    } else {
        body.resize(body.len() + usize::try_from(resp_bytes).unwrap(), b'x');
    }
    body.extend_from_slice(b"\"}}]}");
    let mut out = format!("HTTP/1.1 200 OK\r\ncontent-length: {}\r\n\r\n", body.len()).into_bytes();
    out.extend_from_slice(&body);
    out
}

fn serve(mut stream: TcpStream, behavior: Behavior) {
    stream.set_nodelay(true).unwrap();
    let mut buf = Vec::new();
    let mut served = 0;
    while let Some(body) = read_request(&mut stream, &mut buf) {
        match behavior {
            Behavior::CloseWithoutResponse => return,
            Behavior::DropSecondRequest if served == 1 => return,
            Behavior::DropSecondRequestLate if served == 1 => {
                thread::sleep(Duration::from_millis(30));
                return;
            }
            Behavior::Hold => {
                thread::sleep(Duration::from_secs(5));
                return;
            }
            _ => {}
        }
        let params = find_directive(&body).unwrap().unwrap();
        let streaming = memchr::memmem::find(&body, b"\"stream\":true").is_some();
        if behavior == Behavior::StallAfterFirstChunk {
            let mut out = SSE_HEAD.to_vec();
            sse_event(&mut out, params.sid, 0);
            stream.write_all(&out).unwrap();
            thread::sleep(Duration::from_secs(5));
            return;
        }
        let response = if streaming {
            sse_response(params.sid, params.chunks)
        } else {
            completion_response(params.sid, params.resp_bytes)
        };
        if stream.write_all(&response).is_err() {
            return;
        }
        served += 1;
    }
}

struct Run {
    output: ShardOutput,
    errors: BTreeMap<String, u64>,
    requests: u64,
    histograms: BTreeMap<Metric, Histogram<u64>>,
}

impl Run {
    fn count(&self, metric: Metric) -> u64 {
        self.histograms.get(&metric).map_or(0, Histogram::len)
    }
}

fn run_shard(
    addr: SocketAddr,
    mode: Mode,
    segments: Vec<RateSegment>,
    end_ns: u64,
    timeout_ns: u64,
    ramp: Option<RampSpec>,
) -> Run {
    let authority = addr.to_string();
    let template = RequestTemplate::build(&RequestSpec {
        authority: &authority,
        path: "/v1/chat/completions",
        headers: &[],
        model: "m",
        stream: matches!(mode, Mode::Stream { .. }),
        include_usage: false,
        ttft_us: 0,
        interval_us: INTERVAL_NS / 1_000,
        chunk_bytes: 100,
        resp_bytes: RESP_BYTES,
        body: BodySpec::Prompt(32),
    })
    .unwrap();
    let origin_ns = segments[0].start_ns;
    let output = run(ShardSpec {
        index: 0,
        origin_ns,
        end_ns,
        cpus: None,
        spin_window: precise::DEFAULT_SPIN_WINDOW,
        connector: Connector { addr, tls: None },
        template,
        schedule: Schedule::Fixed(FixedArrivals::new(segments, 0, 1, CHUNKS)),
        mode,
        planned: None,
        request_timeout_ns: timeout_ns,
        live: Arc::new(Live::default()),
        ramp,
    })
    .unwrap();
    let mut errors = BTreeMap::new();
    let mut histograms: BTreeMap<Metric, Histogram<u64>> = BTreeMap::new();
    let mut requests = 0;
    for interval in &output.intervals {
        requests += interval.requests;
        for (kind, n) in &interval.errors {
            *errors.entry(kind.clone()).or_insert(0) += n;
        }
        for metric in Metric::ALL {
            if let Some(h) = interval.histogram(metric).unwrap() {
                histograms
                    .entry(metric)
                    .or_insert_with(stats::new_histogram)
                    .add(&h)
                    .unwrap();
            }
        }
    }
    Run {
        output,
        errors,
        requests,
        histograms,
    }
}

fn fixed(rate: f64, secs_ms: u64) -> (Vec<RateSegment>, u64) {
    let origin = now_ns() + 20_000_000;
    let end = origin + secs_ms * 1_000_000;
    (
        vec![RateSegment {
            start_ns: origin,
            end_ns: end,
            rate,
            step: 0,
        }],
        end,
    )
}

#[test]
fn streams_record_ttft_and_chunk_metrics() {
    let addr = spawn_server(Behavior::Serve);
    let (segments, end) = fixed(20.0, 1_000);
    let run = run_shard(addr, STREAM, segments, end + 100_000_000, SEC_NS * 5, None);
    let c = run.output.counters;
    assert_eq!(c.requests_scheduled, 20);
    assert!(run.errors.is_empty(), "{:?}", run.errors);
    assert_eq!(run.requests + c.open_at_end, 20);
    assert_eq!(run.requests, 20, "all done long before the end");
    assert_eq!(run.count(Metric::Ttft), 20);
    assert_eq!(run.count(Metric::ChunkLatency), 60);
    assert_eq!(run.count(Metric::ChunkWire), 60);
    assert_eq!(run.count(Metric::MockWriteLag), 60);
    assert_eq!(run.count(Metric::EmitLag), 20);
    assert_eq!(run.count(Metric::RequestLatency), 0);
    assert_eq!(
        run.output.intervals.iter().map(|i| i.chunks).sum::<u64>(),
        60
    );
    // Sequential requests share one keep-alive connection.
    assert_eq!(c.connections_opened, 1);
    assert_eq!(c.reused_sends, 19);
    assert_eq!(
        c.rx_batches_without_timestamp == 0,
        cfg!(target_os = "linux")
    );
    assert_eq!(run.output.clock_steps, 0);
    assert_eq!(c.censored_requests, 0);
    // The first request pays for the connection; the pooled ones measure
    // the slip up to the responder, which stamps its markers on reading.
    let d = crate::diagnostics::Diagnostics::summarize(&run.output.diagnostics, 0);
    assert_eq!(d.fresh_conn_sends, 1);
    assert_eq!(d.fresh_conn_ttft.map(|s| s.count), Some(1));
    let slip = d.request_slip.expect("pooled sends measure the slip");
    assert_eq!(slip.count, 19);
    assert!(slip.max_ns < 100_000_000, "{slip:?}");
    assert_eq!(d.negative_slips, 0);
}

#[test]
fn whole_responses_record_request_latency_and_mock_write_lag() {
    let addr = spawn_server(Behavior::Serve);
    let (segments, end) = fixed(20.0, 1_000);
    let run = run_shard(addr, WHOLE, segments, end + 100_000_000, SEC_NS * 5, None);
    assert!(run.errors.is_empty(), "{:?}", run.errors);
    assert_eq!(run.requests, 20);
    assert_eq!(run.count(Metric::RequestLatency), 20);
    // One marker per response: the mock's write lag and the wire time.
    assert_eq!(run.count(Metric::MockWriteLag), 20);
    assert_eq!(run.count(Metric::ChunkWire), 20);
    // A whole response is not a stream: no TTFT, chunk latency or chunks.
    assert_eq!(run.count(Metric::Ttft), 0);
    assert_eq!(run.count(Metric::ChunkLatency), 0);
    assert_eq!(
        run.output.intervals.iter().map(|i| i.chunks).sum::<u64>(),
        0
    );
}

#[test]
fn whole_responses_without_their_marker_are_errors() {
    let addr = spawn_server(Behavior::Serve);
    let (segments, end) = fixed(10.0, 500);
    // The template asks for 100 content bytes, but the shard is told a
    // marker-free response is expected: the marker is one too many.
    let plain = Mode::Whole {
        ttft_ns: 0,
        marker: false,
    };
    let run = run_shard(addr, plain, segments, end + 100_000_000, SEC_NS * 5, None);
    assert_eq!(run.requests, 0);
    assert_eq!(
        run.errors.get("marker_mismatch"),
        Some(&run.output.counters.requests_scheduled),
        "{:?}",
        run.errors
    );
}

#[test]
fn stale_keep_alive_connections_are_retried_once() {
    let addr = spawn_server(Behavior::DropSecondRequest);
    let (segments, end) = fixed(10.0, 1_000);
    let run = run_shard(addr, WHOLE, segments, end + 200_000_000, SEC_NS * 5, None);
    let c = run.output.counters;
    assert_eq!(c.requests_scheduled, 10);
    assert_eq!(run.requests, 10, "{:?}", run.errors);
    // Every request after the first finds its pooled connection dead and
    // succeeds on a fresh one.
    assert_eq!(run.errors.get(STALE_RETRY), Some(&9), "{:?}", run.errors);
    assert_eq!(run.errors.len(), 1, "{:?}", run.errors);
    assert_eq!(c.stale_retries, 9);
    assert_eq!(c.connections_opened, 10);
    assert_eq!(run.count(Metric::RequestLatency), 10);
    // The retry keeps the original scheduled time, so its latency includes
    // the failed attempt; the emit lag is recorded once per request.
    assert_eq!(run.count(Metric::EmitLag), 10);
}

#[test]
fn pooled_connections_closed_after_the_stale_window_are_errors() {
    let addr = spawn_server(Behavior::DropSecondRequestLate);
    let (segments, end) = fixed(10.0, 1_000);
    let run = run_shard(addr, WHOLE, segments, end + 200_000_000, SEC_NS * 5, None);
    let c = run.output.counters;
    // The server takes 30 ms to fail each second request of a connection:
    // it worked on the request, so no retry hides the failure. Requests
    // alternate between a fresh connection (served) and its reuse (failed).
    assert_eq!(c.requests_scheduled, 10);
    assert_eq!(c.stale_retries, 0);
    assert_eq!(run.errors.get("eof"), Some(&5), "{:?}", run.errors);
    assert_eq!(run.errors.len(), 1, "{:?}", run.errors);
    assert_eq!(run.requests, 5);
}

#[test]
fn fresh_connections_closed_without_response_are_errors() {
    let addr = spawn_server(Behavior::CloseWithoutResponse);
    let (segments, end) = fixed(10.0, 500);
    let run = run_shard(addr, WHOLE, segments, end + 200_000_000, SEC_NS * 5, None);
    let c = run.output.counters;
    assert_eq!(run.requests, 0);
    assert_eq!(c.stale_retries, 0);
    assert_eq!(
        run.errors.get("eof"),
        Some(&c.requests_scheduled),
        "{:?}",
        run.errors
    );
}

#[test]
fn timed_out_requests_leave_their_latency_as_a_lower_bound() {
    let addr = spawn_server(Behavior::Hold);
    let (segments, end) = fixed(4.0, 500);
    // Timeouts are checked once per second.
    let run = run_shard(
        addr,
        WHOLE,
        segments,
        end + 1_600_000_000,
        300_000_000,
        None,
    );
    let c = run.output.counters;
    assert_eq!(run.requests, 0);
    assert_eq!(
        run.errors.get("timeout"),
        Some(&c.requests_scheduled),
        "{:?}",
        run.errors
    );
    assert_eq!(c.open_at_end, 0);
    // Each timed-out request is in the latency distribution with at least
    // the timeout, not silently dropped.
    let latency = &run.histograms[&Metric::RequestLatency];
    assert_eq!(latency.len(), c.requests_scheduled);
    assert!(latency.min() >= 299_000_000, "{}", latency.min());
    assert_eq!(c.censored_requests, c.requests_scheduled);
    assert_eq!(c.censored_samples, c.requests_scheduled);
}

#[test]
fn requests_open_at_the_end_leave_their_latency_as_a_lower_bound() {
    let addr = spawn_server(Behavior::Hold);
    let (segments, end) = fixed(10.0, 300);
    let run = run_shard(addr, WHOLE, segments, end + 100_000_000, SEC_NS * 10, None);
    let c = run.output.counters;
    assert_eq!(c.requests_scheduled, 3);
    assert_eq!(c.open_at_end, 3);
    assert!(run.errors.is_empty(), "{:?}", run.errors);
    // Scheduled 400, 300 and 200 ms before the end.
    let latency = &run.histograms[&Metric::RequestLatency];
    assert_eq!(latency.len(), 3);
    assert!(latency.min() >= 199_000_000, "{}", latency.min());
    assert!(latency.max() >= 399_000_000, "{}", latency.max());
    assert_eq!((c.censored_requests, c.censored_samples), (3, 3));
    // The bounds join the last interval of the run.
    let last = run.output.intervals.last().unwrap();
    assert_eq!(
        last.histogram(Metric::RequestLatency)
            .unwrap()
            .unwrap()
            .len(),
        3
    );
}

#[test]
fn unanswered_streams_leave_a_ttft_lower_bound() {
    let addr = spawn_server(Behavior::Hold);
    let (segments, end) = fixed(10.0, 300);
    let run = run_shard(addr, STREAM, segments, end + 100_000_000, SEC_NS * 10, None);
    let c = run.output.counters;
    assert_eq!(c.open_at_end, 3);
    let ttft = &run.histograms[&Metric::Ttft];
    assert_eq!(ttft.len(), 3);
    assert!(ttft.min() >= 199_000_000, "{}", ttft.min());
    // Without a marker the mock's chunk schedule is unknown.
    assert_eq!(run.count(Metric::ChunkLatency), 0);
    assert_eq!((c.censored_requests, c.censored_samples), (3, 3));
}

#[test]
fn stalled_streams_leave_lower_bounds_for_their_overdue_chunks() {
    let addr = spawn_server(Behavior::StallAfterFirstChunk);
    let (segments, end) = fixed(10.0, 300);
    let run = run_shard(addr, STREAM, segments, end + 100_000_000, SEC_NS * 10, None);
    let c = run.output.counters;
    assert_eq!(c.open_at_end, 3);
    // The first chunk of each stream arrived; chunks 1 and 2 were due 1 and
    // 2 ms after it, which is up to 400 ms before the end (less the time the
    // responder took to accept and answer).
    assert_eq!(run.count(Metric::Ttft), 3);
    let chunks = &run.histograms[&Metric::ChunkLatency];
    assert_eq!(chunks.len(), 3 + 3 * 2);
    assert!(
        chunks.max() >= 300_000_000 && chunks.max() < 400_000_000,
        "{}",
        chunks.max()
    );
    assert_eq!(chunks.count_between(150_000_000, 400_000_000), 6);
    assert_eq!((c.censored_requests, c.censored_samples), (3, 6));
}

#[test]
fn ramp_steps_are_reported_once_judgeable() {
    let addr = spawn_server(Behavior::Serve);
    let origin = now_ns() + 20_000_000;
    let segments = ramp_segments(origin, 200_000_000, 20.0, 50.0, 300_000_000, 2);
    let steps: Vec<RateSegment> = segments.iter().copied().filter(|s| s.step > 0).collect();
    let end = steps.last().unwrap().end_ns + 100_000_000;
    let (tx, rx) = mpsc::channel();
    let run = run_shard(
        addr,
        WHOLE,
        segments,
        end,
        SEC_NS * 5,
        Some(RampSpec {
            steps,
            threshold_ns: 2_000_000,
            reports: tx,
        }),
    );
    let reports: Vec<StepReport> = rx.try_iter().collect();
    assert_eq!(reports.iter().map(|r| r.step).collect::<Vec<_>>(), [1, 2]);
    // 20/s and 30/s for 300 ms each.
    assert_eq!(reports[0].requests + reports[0].censored, 6);
    assert_eq!(reports[1].requests + reports[1].censored, 9);
    assert!(
        reports
            .iter()
            .all(|r| r.errors == 0 && r.latency.len() == r.requests + r.censored)
    );
    assert_eq!(run.output.counters.requests_scheduled, 4 + 6 + 9);
}
