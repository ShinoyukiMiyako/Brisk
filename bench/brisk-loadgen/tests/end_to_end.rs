//! Runs the real binary against a small in-test responder that paces SSE
//! chunks like `brisk-mock` and writes timestamp markers.

use std::io::{Read as _, Write as _};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::thread;
use std::time::Duration;

use brisk_bench_core::clock::now_ns;
use brisk_bench_core::http1::{BodyFraming, LAST_CHUNK, request_body_framing, write_chunk};
use brisk_bench_core::result::RunResult;
use brisk_bench_core::stats::Metric;
use brisk_bench_core::wire::{
    BenchParams, MARKER_LEN_U32, Marker, append_content, find_directive, patch_t_write,
};

const BIN: &str = env!("CARGO_BIN_EXE_brisk-loadgen");

/// Accepts connections and answers every request on a thread of its own.
fn spawn_responder() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    thread::spawn(move || {
        for stream in listener.incoming() {
            let stream = stream.unwrap();
            thread::spawn(move || serve(stream));
        }
    });
    addr
}

fn read_request(stream: &mut TcpStream, buf: &mut Vec<u8>) -> Option<Vec<u8>> {
    loop {
        let mut headers = [httparse::EMPTY_HEADER; 32];
        let mut req = httparse::Request::new(&mut headers);
        if let httparse::Status::Complete(head) = req.parse(buf).unwrap() {
            let BodyFraming::Length(len) = request_body_framing(req.headers).unwrap() else {
                panic!("the load generator sends content-length bodies");
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

fn sleep_until(deadline_ns: u64) {
    let now = now_ns();
    if deadline_ns > now {
        thread::sleep(Duration::from_nanos(deadline_ns - now));
    }
}

/// Streams `params.chunks` events at `t0 + ttft + k · interval`.
fn stream_response(stream: &mut TcpStream, params: &BenchParams, t0: u64) -> std::io::Result<()> {
    stream.write_all(
        b"HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ntransfer-encoding: chunked\r\n\r\n",
    )?;
    for seq in 0..params.chunks {
        let t_sched = t0 + params.ttft_us * 1_000 + u64::from(seq) * params.interval_us * 1_000;
        sleep_until(t_sched);
        let mut event = b"data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"".to_vec();
        let at = append_content(
            &mut event,
            &Marker {
                sid: params.sid,
                seq,
                t_sched,
                t_write: 0,
            },
            params.chunk_bytes,
        );
        event.extend_from_slice(b"\"}}]}\n\n");
        let mut frame = Vec::new();
        write_chunk(&mut frame, &event);
        let marker_at = frame.len() - event.len() - 2 + at;
        patch_t_write(&mut frame[marker_at..], now_ns());
        stream.write_all(&frame)?;
    }
    let mut tail = Vec::new();
    write_chunk(
        &mut tail,
        b"data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
    );
    write_chunk(&mut tail, b"data: [DONE]\n\n");
    tail.extend_from_slice(LAST_CHUNK);
    stream.write_all(&tail)
}

/// Answers at `t0 + ttft` with a `chat.completion` whose content carries a
/// marker when `resp_bytes` has room for one, as `brisk-mock` does.
fn completion_response(
    stream: &mut TcpStream,
    params: &BenchParams,
    t0: u64,
) -> std::io::Result<()> {
    let t_sched = t0 + params.ttft_us * 1_000;
    let mut body =
        b"{\"object\":\"chat.completion\",\"choices\":[{\"message\":{\"content\":\"".to_vec();
    let marker_at = (params.resp_bytes >= MARKER_LEN_U32).then(|| {
        let marker = Marker {
            sid: params.sid,
            seq: 0,
            t_sched,
            t_write: 0,
        };
        append_content(&mut body, &marker, params.resp_bytes)
    });
    if marker_at.is_none() {
        body.resize(
            body.len() + usize::try_from(params.resp_bytes).unwrap(),
            b'x',
        );
    }
    body.extend_from_slice(b"\"}}]}");
    let mut response = format!(
        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n",
        body.len()
    )
    .into_bytes();
    let body_at = response.len();
    response.extend_from_slice(&body);
    sleep_until(t_sched);
    if let Some(at) = marker_at {
        patch_t_write(&mut response[body_at + at..], now_ns());
    }
    stream.write_all(&response)
}

fn serve(mut stream: TcpStream) {
    stream.set_nodelay(true).unwrap();
    let mut buf = Vec::new();
    while let Some(body) = read_request(&mut stream, &mut buf) {
        let t0 = now_ns();
        let params = find_directive(&body).unwrap().unwrap();
        let result = if memchr::memmem::find(&body, b"\"stream\":true").is_some() {
            stream_response(&mut stream, &params, t0)
        } else {
            completion_response(&mut stream, &params, t0)
        };
        if result.is_err() {
            return;
        }
    }
}

fn temp_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "brisk-loadgen-{name}-{}-{}",
        std::process::id(),
        now_ns()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn run(args: &[&str]) -> Output {
    let output = Command::new(BIN).args(args).output().unwrap();
    eprintln!(
        "$ brisk-loadgen {}\n{}{}",
        args.join(" "),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    output
}

fn read_document(path: &Path) -> serde_json::Value {
    serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap()
}

fn stream_run(url: &str, out: &Path, label: &str) -> RunResult {
    let output = run(&[
        "stream",
        url,
        "--concurrency",
        "16",
        "--chunk-rate",
        "20",
        "--dur-median",
        "0.5",
        "--dur-p99",
        "2",
        "--dur-max",
        "3",
        "--ttft-us",
        "20000",
        "--chunk-bytes",
        "96",
        "--include-usage",
        "--warmup-s",
        "1",
        "--measure-s",
        "3",
        "--label",
        label,
        "--out",
        out.to_str().unwrap(),
    ]);
    assert!(output.status.success());
    RunResult::read_json(out).unwrap()
}

#[test]
fn stream_records_ttft_and_chunk_metrics_and_compares() {
    let addr = spawn_responder();
    let dir = temp_dir("stream");
    let url = format!("http://{addr}");
    let a = dir.join("a.json");
    let b = dir.join("b.json");
    let run_a = stream_run(&url, &a, "direct-P");
    stream_run(&url, &b, "floor-P");

    assert_eq!(run_a.tool, "brisk-loadgen");
    assert_eq!(run_a.scenario, "stream");
    assert_eq!(run_a.label, "direct-P");
    assert_eq!(run_a.warmup_intervals, 1);
    assert_eq!(run_a.intervals.len(), 4);
    assert_eq!(run_a.params["shape"]["concurrency"], 16);
    let ttft = run_a.summary[&Metric::Ttft];
    assert!(ttft.count > 0);
    // The responder waits 20 ms before the first chunk; TTFT runs from the
    // scheduled start, so it can only be longer.
    assert!(ttft.min_ns >= 20_000_000, "{ttft:?}");
    let chunks = run_a.summary[&Metric::ChunkLatency];
    assert!(chunks.count > ttft.count, "{chunks:?} {ttft:?}");
    assert_eq!(run_a.summary[&Metric::ChunkWire].count, chunks.count);
    assert_eq!(run_a.summary[&Metric::MockWriteLag].count, chunks.count);
    assert!(run_a.summary[&Metric::EmitLag].count > 0);
    let errors: u64 = run_a
        .intervals
        .iter()
        .map(brisk_bench_core::result::IntervalResult::error_count)
        .sum();
    assert_eq!(errors, 0, "{:?}", run_a.intervals);

    // The extension carries the offered load and the load check.
    let doc = read_document(&a);
    let ext = &doc["loadgen"];
    assert_eq!(ext["planned"].as_array().unwrap().len(), 4);
    assert_eq!(ext["load_check"]["checked_intervals"], 3);
    assert_eq!(ext["load_check"]["nominal_concurrency"], 16);
    assert!(ext["stream_plan"]["arrival_rate"].as_f64().unwrap() > 0.0);
    assert!(ext["counters"]["connections_opened"].as_u64().unwrap() >= 16);

    // The in-test responder sleeps with millisecond precision, so the mock
    // write lag rule invalidates the runs; compare refuses them by default.
    let cmp = dir.join("cmp.json");
    let (a_path, b_path, cmp_path) = (
        a.to_str().unwrap(),
        b.to_str().unwrap(),
        cmp.to_str().unwrap(),
    );
    let compare = |extra: &[&str]| {
        let mut args = vec![
            "compare",
            "--a",
            a_path,
            "--b",
            b_path,
            "--metric",
            "chunk_latency,ttft",
            "--quantiles",
            "50,99",
            "--resamples",
            "200",
            "--out",
            cmp_path,
        ];
        args.extend_from_slice(extra);
        run(&args)
    };
    if !run_a.validity.valid {
        assert!(!compare(&[]).status.success());
    }
    assert!(compare(&["--allow-invalid"]).status.success());
    let doc = read_document(&cmp);
    assert_eq!(doc["scenario"], "stream");
    let comparisons = doc["comparisons"].as_array().unwrap();
    assert_eq!(comparisons.len(), 2);
    assert_eq!(comparisons[0]["metric"], "chunk_latency");
    assert_eq!(comparisons[0]["quantiles"].as_array().unwrap().len(), 2);
    assert_eq!(comparisons[0]["quantiles"][1]["quantile"], 0.99);
    assert!(doc["baseline_p99_ci_ok"].is_boolean());
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn nonstream_ramp_records_request_latency_per_step() {
    let addr = spawn_responder();
    let dir = temp_dir("ramp");
    let out = dir.join("s2.json");
    let output = run(&[
        "nonstream",
        &format!("http://{addr}"),
        "--ramp-start",
        "20",
        "--ramp-step-pct",
        "50",
        "--ramp-step-s",
        "1",
        "--ramp-max-steps",
        "2",
        "--stop-p99-ms",
        "1000",
        "--warmup-s",
        "1",
        "--resp-bytes",
        "256",
        "--out",
        out.to_str().unwrap(),
    ]);
    assert!(output.status.success());
    let result = RunResult::read_json(&out).unwrap();
    assert_eq!(result.scenario, "nonstream-ramp");
    let latency = result.summary[&Metric::RequestLatency];
    assert_eq!(latency.count, 20 + 30);
    assert!(!result.summary.contains_key(&Metric::Ttft));
    // Every completion carries the responder's marker.
    assert_eq!(result.summary[&Metric::MockWriteLag].count, 20 + 30);
    assert_eq!(result.summary[&Metric::ChunkWire].count, 20 + 30);
    let ramp = &read_document(&out)["loadgen"]["ramp"];
    let steps = ramp["steps"].as_array().unwrap();
    assert_eq!(steps.len(), 2);
    assert_eq!(steps[0]["requests"], 20);
    assert_eq!(steps[1]["requests"], 30);
    assert_eq!(ramp["max_sustainable_rate"], 30.0);

    // A ramp mixes the latencies of all its rates: not comparable.
    let path = out.to_str().unwrap();
    let cmp = dir.join("cmp.json");
    let compare = run(&[
        "compare",
        "--a",
        path,
        "--b",
        path,
        "--metric",
        "request_latency",
        "--allow-invalid",
        "--out",
        cmp.to_str().unwrap(),
    ]);
    assert!(!compare.status.success());
    assert!(String::from_utf8_lossy(&compare.stderr).contains("ramp"));
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn bigbody_writes_one_result_per_size() {
    let addr = spawn_responder();
    let dir = temp_dir("bigbody");
    let out = dir.join("s3.json");
    let output = run(&[
        "bigbody",
        &format!("http://{addr}"),
        "--sizes",
        "100k,10m",
        "--rate",
        "4",
        "--chunks",
        "2",
        "--interval-us",
        "1000",
        "--warmup-s",
        "0",
        "--measure-s",
        "1",
        "--out",
        out.to_str().unwrap(),
    ]);
    assert!(output.status.success());
    for (label, bytes) in [("100k", 102_400), ("10m", 10_485_760)] {
        let path = dir.join(format!("s3-{label}.json"));
        let run = RunResult::read_json(&path).unwrap();
        assert_eq!(run.scenario, format!("bigbody-{label}"));
        assert_eq!(run.summary[&Metric::Ttft].count, 4);
        assert_eq!(run.summary[&Metric::ChunkLatency].count, 8);
        let ext = &read_document(&path)["loadgen"];
        assert_eq!(ext["body_bytes"], bytes);
        assert_eq!(ext["body_size"]["label"], label);
    }
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn https_target_without_ca_is_rejected_before_running() {
    let dir = temp_dir("tls");
    let output = run(&[
        "stream",
        "https://127.0.0.1:1",
        "--concurrency",
        "1",
        "--out",
        dir.join("x.json").to_str().unwrap(),
    ]);
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("--tls-ca"));
    std::fs::remove_dir_all(&dir).unwrap();
}
