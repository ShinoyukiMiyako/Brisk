//! The Brisk gateway binary as a library: command line, configuration
//! loading with secret handling, virtual-key generation, runtime assembly,
//! outcome logging and signal handling around `brisk_gateway::Gateway`.
//!
//! It is a library so that the configuration and live upstream tests drive
//! exactly the code the binary runs.
//!
//! # Commands
//!
//! - `brisk serve --config <PATH>` serves until the first `SIGINT` or
//!   `SIGTERM` (Ctrl-C on Windows), drains in-flight connections for
//!   `server.graceful_shutdown_timeout`, and gives the outcome logger up to
//!   3 s to empty its queue (2.9). A second signal abandons the drain.
//! - `brisk check-config --config <PATH>` loads the configuration, builds the
//!   inbound TLS acceptor and the gateway as `serve` would, and prints a
//!   summary without secrets on stdout.
//! - `brisk keygen --name <NAME>` prints a new virtual key in the frozen
//!   stdout format of [`keygen`].
//!
//! Logs, warnings and hints go to stderr; stdout carries only command output.

#![forbid(unsafe_code)]

pub mod cli;
pub mod config;
pub mod keygen;
mod logger;
mod runtime;
mod signals;
mod tls;

use std::fmt;
use std::io::{self, Write};
use std::net::SocketAddr;
use std::path::Path;

use anyhow::Context as _;
use brisk_gateway::Gateway;
use brisk_gateway::net::raise_nofile_soft_limit;
use brisk_gateway::outcome::{OutcomeReceiver, OutcomeSink, OutcomeTally, outcome_channel};
use brisk_gateway::server;
use brisk_gateway::upstream::chat_url;
use brisk_gateway::upstream::registry::ChannelWarning;
use brisk_gateway::upstream::resolver::validate_base_url;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinError;
use tokio_rustls::TlsAcceptor;

use crate::cli::{Cli, Command};
use crate::config::LoadedConfig;
use crate::signals::{ShutdownRequests, ShutdownSignals};

/// Entry point used by main.rs; returns the process result.
///
/// Expects tracing to be initialised already: configuration warnings,
/// channel warnings and outcome records are logged through it.
pub fn run(cli: Cli) -> anyhow::Result<()> {
    match cli.command {
        Command::Serve { config } => run_serve(&config),
        Command::CheckConfig { config } => run_check_config(&config, &mut io::stdout().lock()),
        Command::Keygen { name } => {
            run_keygen(&name, &mut io::stdout().lock(), &mut io::stderr().lock())
        }
    }
}

/// `brisk serve`: loads the configuration, builds the runtime and serves
/// until a shutdown signal.
fn run_serve(path: &Path) -> anyhow::Result<()> {
    let (config, nofile) = load_config(path)?;
    let tls = inbound_tls(&config)?;
    let (runtime, workers) =
        runtime::build(config.workers).context("building the Tokio runtime")?;
    runtime.block_on(async {
        let mut signals = ShutdownSignals::install().context("installing signal handlers")?;
        let tls_enabled = tls.is_some();
        let bound = bind(config, tls)?;
        tracing::info!(
            listen = %bound.listener.local_addr().context("reading the listen address")?,
            tls = tls_enabled,
            workers = workers.get(),
            nofile,
            "brisk listening"
        );
        serve_bound(bound, &mut signals).await
    })?;
    tracing::info!("brisk stopped");
    Ok(())
}

/// Raises the descriptor limit, loads the configuration and logs its
/// warnings.
///
/// The limit is raised first because loading builds
/// `ServerConfig::default()`, which derives the connection limit from it.
/// `check-config` goes through here too, so it validates exactly the
/// configuration `serve` would run. Returns the new soft limit.
fn load_config(path: &Path) -> anyhow::Result<(LoadedConfig, Option<u64>)> {
    let nofile = raise_nofile_soft_limit().context("raising the RLIMIT_NOFILE soft limit")?;
    let config =
        config::load(path).with_context(|| format!("loading configuration {}", path.display()))?;
    for warning in &config.warnings {
        tracing::warn!("{warning}");
    }
    Ok((config, nofile))
}

/// The inbound TLS acceptor, when `[server.tls]` is configured.
fn inbound_tls(config: &LoadedConfig) -> anyhow::Result<Option<TlsAcceptor>> {
    config
        .tls
        .as_ref()
        .map(tls::acceptor)
        .transpose()
        .context("loading the inbound TLS certificate and key")
}

/// A built gateway with its bound listener and outcome queue, not yet
/// serving.
struct Bound {
    gateway: Gateway,
    listener: TcpListener,
    tls: Option<TlsAcceptor>,
    /// The logger's handle on the counters.
    sink: OutcomeSink,
    outcomes: OutcomeReceiver,
}

/// Builds the gateway, then binds its listener. Must run inside the runtime,
/// whose reactor the listener registers with.
fn bind(config: LoadedConfig, tls: Option<TlsAcceptor>) -> anyhow::Result<Bound> {
    let (sink, outcomes) = outcome_channel(config.outcome_queue);
    let server_config = config.spec.server.clone();
    let gateway = Gateway::new(config.spec, sink.clone()).context("building the gateway")?;
    let listener = server::bind(config.listen, &server_config)
        .with_context(|| format!("binding {}", config.listen))?;
    Ok(Bound {
        gateway,
        listener,
        tls,
        sink,
        outcomes,
    })
}

/// Runs the outcome logger and the gateway until `requests` stops it, then
/// gives the logger up to [`logger::FINAL_DRAIN`] to empty the queue; that
/// wait applies after an abandoned drain as well. Returns the totals the
/// logger reported.
async fn serve_bound(
    bound: Bound,
    requests: &mut impl ShutdownRequests,
) -> anyhow::Result<OutcomeTally> {
    let Bound {
        gateway,
        listener,
        tls,
        sink,
        outcomes,
    } = bound;
    let (stop_logger, logger_stop) = oneshot::channel::<()>();
    let logger = tokio::spawn(logger::run(
        outcomes,
        sink,
        async move {
            // A dropped sender means the same as a sent one: stop.
            let _ = logger_stop.await;
        },
        logger::FINAL_DRAIN,
    ));

    let served = serve_until_shutdown(gateway, listener, tls, requests).await;
    // Fails only if the logger already ended, which awaiting it reports.
    let _ = stop_logger.send(());
    let (tally, _) = logger.await.context("the outcome logger task failed")?;
    served.map(|()| tally)
}

/// Serves until the first shutdown request, then drains until the grace
/// period ends or a second request abandons it.
async fn serve_until_shutdown(
    gateway: Gateway,
    listener: TcpListener,
    tls: Option<TlsAcceptor>,
    requests: &mut impl ShutdownRequests,
) -> anyhow::Result<()> {
    let (begin_shutdown, shutdown) = oneshot::channel::<()>();
    // Spawned so the accept loop runs on the workers rather than on the
    // thread blocked in `block_on`, which only waits here.
    let mut serving = tokio::spawn(gateway.serve(listener, tls, async move {
        // A dropped sender means the same as a sent one: stop.
        let _ = shutdown.await;
    }));

    let request = tokio::select! {
        served = &mut serving => return serve_outcome(served),
        request = requests.next() => request,
    };
    tracing::info!(
        signal = request,
        "shutting down; signal again to abandon the drain"
    );
    // Fails only if serving already ended, which the next select reports.
    let _ = begin_shutdown.send(());
    tokio::select! {
        served = &mut serving => serve_outcome(served),
        request = requests.next() => {
            serving.abort();
            match serving.await {
                Err(err) if err.is_cancelled() => anyhow::bail!(
                    "{request} received during the drain; abandoned in-flight connections"
                ),
                // Serving ended on its own before the abort took effect.
                served => serve_outcome(served),
            }
        }
    }
}

fn serve_outcome(served: Result<io::Result<()>, JoinError>) -> anyhow::Result<()> {
    served
        .context("the serve task did not complete")?
        .context("serving")
}

/// `brisk check-config`: loads the configuration and builds the inbound TLS
/// acceptor and the gateway exactly as `serve` does, without a runtime or a
/// listener, then writes the summary on `out`. The summary holds no secret:
/// names, addresses, URLs and warnings only, the loader's warnings first.
fn run_check_config(path: &Path, out: &mut dyn Write) -> anyhow::Result<()> {
    let (config, _) = load_config(path)?;
    let tls = inbound_tls(&config)?.is_some();
    let channels: Vec<(String, String, bool)> = config
        .spec
        .channels
        .iter()
        .map(|channel| {
            (
                channel.name.clone(),
                channel.base_url.clone(),
                channel.client.allow_private,
            )
        })
        .collect();
    // Nothing is served, so the queue is never read.
    let (sink, _outcomes) = outcome_channel(config.outcome_queue);
    let gateway = Gateway::new(config.spec, sink).context("building the gateway")?;
    let channels = channels
        .into_iter()
        .map(|(name, base_url, allow_private)| {
            // `Gateway::new` accepted this URL under the same policy; the
            // gateway keeps its chat URL private.
            let base = validate_base_url(&base_url, allow_private)
                .with_context(|| format!("channel {name:?}: invalid base_url"))?;
            Ok((name, chat_url(&base).to_string()))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;

    Summary {
        path,
        listen: config.listen,
        tls,
        channels: &channels,
        load_warnings: &config.warnings,
        channel_warnings: gateway.warnings(),
    }
    .write(out)
    .context("writing the summary to stdout")
}

/// What `check-config` prints.
struct Summary<'a> {
    path: &'a Path,
    listen: SocketAddr,
    tls: bool,
    /// Name and chat URL of every channel, in configuration order.
    channels: &'a [(String, String)],
    load_warnings: &'a [String],
    channel_warnings: &'a [ChannelWarning],
}

impl Summary<'_> {
    /// Channel names are printed with `Debug` escapes: a name is free text
    /// and must not smuggle control characters onto a terminal.
    fn write(&self, out: &mut dyn Write) -> io::Result<()> {
        writeln!(out, "configuration: {}", self.path.display())?;
        let transport = if self.tls { "TLS" } else { "plaintext" };
        writeln!(out, "listen: {} ({transport})", self.listen)?;
        writeln!(out, "channels: {}", self.channels.len())?;
        for (name, url) in self.channels {
            writeln!(out, "  {name:?} -> {url}")?;
        }
        let warnings = self.load_warnings.len() + self.channel_warnings.len();
        writeln!(out, "warnings: {warnings}")?;
        for warning in self.load_warnings {
            writeln!(out, "  {warning}")?;
        }
        for warning in self.channel_warnings {
            writeln!(out, "  {}", Describe(warning))?;
        }
        out.flush()
    }
}

/// A [`ChannelWarning`] as one line of the `check-config` summary.
struct Describe<'a>(&'a ChannelWarning);

impl fmt::Display for Describe<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            ChannelWarning::SplitPool { host } => write!(
                f,
                "upstream {host} is reached through several client profiles, \
                 so its connections are split across pools"
            ),
            ChannelWarning::PlaintextRemote { channel } => write!(
                f,
                "channel {channel:?} uses plain http to a remote host; \
                 its key travels in cleartext"
            ),
            ChannelWarning::NoVersionPath { channel } => write!(
                f,
                "channel {channel:?} has a base_url without a path, so /chat/completions \
                 is requested at the root; a version prefix such as /v1 is usually missing"
            ),
        }
    }
}

/// `brisk keygen`: the frozen format of [`keygen`] on `out`, a reminder on
/// `err`.
fn run_keygen(name: &str, out: &mut dyn Write, err: &mut dyn Write) -> anyhow::Result<()> {
    let generated = keygen::generate(name).context("generating a virtual key")?;
    generated
        .write_stdout(out)
        .context("writing the key to stdout")?;
    writeln!(
        err,
        "The key above is shown only once: hand it to the client now. \
         Add the [[keys]] table to the configuration; it stores only the key's SHA-256 digest."
    )
    .context("writing to stderr")
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;
    use std::net::SocketAddr;
    use std::sync::Arc;
    use std::time::Duration;

    use bytes::Bytes;
    use http_body_util::{BodyExt as _, Full};
    use hyper::body::Incoming;
    use hyper::header::{AUTHORIZATION, CONTENT_TYPE};
    use hyper::server::conn::http1;
    use hyper::service::service_fn;
    use hyper::{Request, Response, StatusCode};
    use hyper_util::rt::TokioIo;
    use tokio::sync::{Notify, mpsc};
    use tokio::task::JoinHandle;
    use tokio::time::{Instant, sleep, timeout};

    use super::*;
    use crate::keygen::GeneratedKey;

    /// Default model of every test (4.1); the brackets must pass through.
    const TEST_MODEL: &str = "grok-4.6(xhigh)";

    /// The channel's upstream key; the upstream answers 401 to anything else.
    const UPSTREAM_KEY: &str = "sk-upstream-smoke";

    /// A non-streamed completion with usage, as CPA answers.
    const COMPLETION: &str = r#"{"id":"chatcmpl-1","object":"chat.completion","created":0,"model":"grok-4.6(xhigh)","choices":[{"index":0,"message":{"role":"assistant","content":"pong"},"finish_reason":"stop"}],"usage":{"prompt_tokens":11,"completion_tokens":7,"total_tokens":18}}"#;

    /// Shutdown requests the test sends by hand.
    impl ShutdownRequests for mpsc::UnboundedReceiver<&'static str> {
        async fn next(&mut self) -> &'static str {
            match self.recv().await {
                Some(name) => name,
                // The test sends no more requests.
                None => std::future::pending().await,
            }
        }
    }

    /// An HTTP/1.1 upstream: an empty 200 for warm-up, [`COMPLETION`] after
    /// `chat_delay` for chat requests.
    struct Upstream {
        addr: SocketAddr,
        /// Notified when a chat request arrives.
        chat_arrived: Arc<Notify>,
        task: JoinHandle<()>,
    }

    impl Drop for Upstream {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    async fn start_upstream(chat_delay: Duration) -> Upstream {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind the upstream");
        let addr = listener.local_addr().expect("upstream address");
        let chat_arrived = Arc::new(Notify::new());
        let notify = Arc::clone(&chat_arrived);
        let task = tokio::spawn(async move {
            loop {
                let (stream, _) = listener.accept().await.expect("accept");
                let notify = Arc::clone(&notify);
                tokio::spawn(async move {
                    let service = service_fn(move |request| {
                        let notify = Arc::clone(&notify);
                        async move { Ok::<_, Infallible>(reply(request, chat_delay, &notify).await) }
                    });
                    // The gateway may reset the connection at shutdown; that
                    // is the gateway's business, not the upstream's.
                    let _ = http1::Builder::new()
                        .serve_connection(TokioIo::new(stream), service)
                        .await;
                });
            }
        });
        Upstream {
            addr,
            chat_arrived,
            task,
        }
    }

    async fn reply(
        request: Request<Incoming>,
        chat_delay: Duration,
        chat_arrived: &Notify,
    ) -> Response<Full<Bytes>> {
        let is_chat = request.uri().path() == "/v1/chat/completions";
        let authorized = request
            .headers()
            .get(AUTHORIZATION)
            .is_some_and(|value| value.as_bytes() == format!("Bearer {UPSTREAM_KEY}").as_bytes());
        request
            .into_body()
            .collect()
            .await
            .expect("read the request body");
        if !is_chat {
            return Response::new(Full::default());
        }
        chat_arrived.notify_one();
        if !authorized {
            let mut response = Response::new(Full::default());
            *response.status_mut() = StatusCode::UNAUTHORIZED;
            return response;
        }
        sleep(chat_delay).await;
        Response::builder()
            .header(CONTENT_TYPE, "application/json")
            .body(Full::new(Bytes::from_static(COMPLETION.as_bytes())))
            .expect("valid response")
    }

    /// `smoke.local.toml` with one channel on `upstream` and the `[[keys]]`
    /// table `keygen` printed for `key`.
    fn smoke_config(upstream: SocketAddr, key: &GeneratedKey) -> LoadedConfig {
        let text = format!(
            r#"
[server]
listen = "127.0.0.1:0"

{snippet}

[[channels]]
name = "local"
base_url = "http://{upstream}/v1"
api_key = {{ value = "{UPSTREAM_KEY}" }}
allow_private = true
models = ["{TEST_MODEL}"]
warmup = {{ method = "GET", path = "/v1/models" }}
"#,
            snippet = key.toml_snippet
        );
        config::load_str(&text, Path::new("."), "smoke.local.toml", &|_| None)
            .unwrap_or_else(|err| panic!("{err}"))
    }

    fn client() -> reqwest::Client {
        reqwest::Client::builder()
            .no_proxy()
            .build()
            .expect("build the test client")
    }

    fn chat_request(
        client: &reqwest::Client,
        addr: SocketAddr,
        key: &GeneratedKey,
    ) -> reqwest::RequestBuilder {
        client
            .post(format!("http://{addr}/v1/chat/completions"))
            .bearer_auth(key.key.expose())
            .header(CONTENT_TYPE, "application/json")
            .body(format!(
                r#"{{"model":"{TEST_MODEL}","messages":[{{"role":"user","content":"Reply with exactly: pong"}}]}}"#
            ))
    }

    /// Polls `/readyz` until it answers 200.
    async fn wait_ready(client: &reqwest::Client, addr: SocketAddr) {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let response = client
                .get(format!("http://{addr}/readyz"))
                .send()
                .await
                .expect("/readyz answers");
            if response.status() == StatusCode::OK {
                return;
            }
            assert!(Instant::now() < deadline, "the gateway never became ready");
            sleep(Duration::from_millis(20)).await;
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn serves_a_request_and_stops_gracefully_on_the_first_signal() {
        let upstream = start_upstream(Duration::ZERO).await;
        let key = keygen::generate("smoke").expect("generate a key");
        let bound = bind(smoke_config(upstream.addr, &key), None).expect("bind the gateway");
        let addr = bound.listener.local_addr().expect("gateway address");
        let (send_request, mut requests) = mpsc::unbounded_channel();
        let client = client();

        let exercise = async {
            wait_ready(&client, addr).await;
            let response = chat_request(&client, addr, &key)
                .send()
                .await
                .expect("the gateway answers");
            assert_eq!(response.status(), StatusCode::OK);
            let body = response.bytes().await.expect("read the body");
            assert_eq!(body, COMPLETION.as_bytes());
            send_request
                .send("test signal")
                .expect("the server listens");
        };
        let (served, ()) = tokio::join!(
            timeout(Duration::from_secs(30), serve_bound(bound, &mut requests)),
            exercise
        );
        let tally = served
            .expect("the gateway stopped within 30s")
            .unwrap_or_else(|err| panic!("{err:#}"));
        assert_eq!(tally.requests, 1);
        assert_eq!(tally.completed, 1);
        assert_eq!(tally.failovers, 0);
        assert_eq!((tally.billed_input, tally.billed_output), (11, 7));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_second_signal_abandons_the_drain() {
        // Longer than the test: the request stays in flight until abandoned.
        let upstream = start_upstream(Duration::from_secs(3600)).await;
        let key = keygen::generate("smoke").expect("generate a key");
        let bound = bind(smoke_config(upstream.addr, &key), None).expect("bind the gateway");
        let addr = bound.listener.local_addr().expect("gateway address");
        let (send_request, mut requests) = mpsc::unbounded_channel();
        let client = client();

        let started = Instant::now();
        let exercise = async {
            wait_ready(&client, addr).await;
            let in_flight = chat_request(&client, addr, &key).send();
            let signal_twice = async {
                upstream.chat_arrived.notified().await;
                // The in-flight request would hold the drain open for the
                // whole 30s grace period; the second request cuts it short.
                send_request.send("first").expect("the server listens");
                send_request.send("second").expect("the server listens");
            };
            let (response, ()) = tokio::join!(in_flight, signal_twice);
            response
        };
        let (served, response) = tokio::join!(
            timeout(Duration::from_secs(30), serve_bound(bound, &mut requests)),
            exercise
        );
        let err = served
            .expect("the gateway stopped within 30s")
            .expect_err("an abandoned drain is an error");
        assert_eq!(
            err.to_string(),
            "second received during the drain; abandoned in-flight connections"
        );
        // Well below the grace period, including the logger's final wait.
        assert!(
            started.elapsed() < Duration::from_secs(20),
            "{:?}",
            started.elapsed()
        );
        assert!(response.is_err(), "{response:?}");
    }
}
