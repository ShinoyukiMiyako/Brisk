//! Server startup: listeners, shard threads, CPU pinning and shutdown.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, mpsc};
use std::thread::JoinHandle;
use std::time::Duration;
use std::{io, thread};

use brisk_bench_core::cpu;
use brisk_bench_core::precise::DEFAULT_SPIN_WINDOW;
use brisk_bench_core::transport::{ListenOptions, bind_listener};
use brisk_bench_core::wire::{BenchParams, WireError};

use crate::emit::{DEFAULT_COMMIT_WINDOW, EmitPolicy, EmitSettings};
use crate::shard::{Settings, Shard, WAKER};
use crate::stats::Stats;

/// Everything needed to start a mock server.
#[derive(Debug, Clone)]
pub struct MockConfig {
    /// Address to listen on; port 0 picks a free port shared by all shards.
    pub listen: SocketAddr,
    /// Number of shard threads. More than one requires Linux
    /// (`SO_REUSEPORT`).
    pub shards: usize,
    /// Busy-wait window before each emission.
    pub spin_window: Duration,
    /// How close to an emission a shard keeps serving sockets.
    pub emit_policy: EmitPolicy,
    /// Commit window of the fixed policy: how close the next emission may
    /// come before a shard stops serving sockets and spins to it. Capped at
    /// the spin window.
    pub commit_window: Duration,
    /// CPUs to pin shards to; shard `i` gets `cpus[i % cpus.len()]`.
    /// Unpinned when `None`.
    pub cpus: Option<Vec<usize>>,
    /// TLS configuration; plaintext when `None`.
    pub tls: Option<Arc<rustls::ServerConfig>>,
    /// Parameters for requests without a `bench:v1;` directive. Their `sid`
    /// is echoed in the markers of such requests.
    pub defaults: BenchParams,
    /// Model name reported in responses and by `/v1/models`.
    pub model: String,
    /// `listen(2)` backlog of every listener.
    pub backlog: i32,
}

impl MockConfig {
    /// A plaintext single-shard configuration with the given defaults and
    /// the fixed emission policy.
    pub fn new(listen: SocketAddr, defaults: BenchParams) -> Self {
        Self {
            listen,
            shards: 1,
            spin_window: DEFAULT_SPIN_WINDOW,
            emit_policy: EmitPolicy::default(),
            commit_window: DEFAULT_COMMIT_WINDOW,
            cpus: None,
            tls: None,
            defaults,
            model: "brisk-mock".to_owned(),
            backlog: ListenOptions::default().backlog,
        }
    }
}

/// Largest accepted `chunk_bytes`. Every chunk is built in memory before its
/// write, so a mistyped directive must not make a shard allocate gigabytes.
pub const MAX_CHUNK_BYTES: u32 = 1024 * 1024;
/// Largest accepted `resp_bytes`, for the same reason as [`MAX_CHUNK_BYTES`].
pub const MAX_RESP_BYTES: u32 = 64 * 1024 * 1024;

/// Parameters that are valid on the wire but exceed what the mock serves.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ParamsError {
    /// `chunk_bytes` exceeds [`MAX_CHUNK_BYTES`].
    #[error("chunk_bytes {0} exceeds the limit of {MAX_CHUNK_BYTES}")]
    ChunkBytes(u32),
    /// `resp_bytes` exceeds [`MAX_RESP_BYTES`].
    #[error("resp_bytes {0} exceeds the limit of {MAX_RESP_BYTES}")]
    RespBytes(u32),
}

/// Checks the size limits of the mock on top of [`BenchParams::validate`].
pub(crate) fn check_limits(params: &BenchParams) -> Result<(), ParamsError> {
    if params.chunk_bytes > MAX_CHUNK_BYTES {
        Err(ParamsError::ChunkBytes(params.chunk_bytes))
    } else if params.resp_bytes > MAX_RESP_BYTES {
        Err(ParamsError::RespBytes(params.resp_bytes))
    } else {
        Ok(())
    }
}

/// Why the server could not start or stopped with an error.
#[derive(Debug, thiserror::Error)]
pub enum ServerError {
    /// Zero shards requested.
    #[error("at least one shard is required")]
    NoShards,
    /// Several shards need one `SO_REUSEPORT` listener each.
    #[error("{0} shards requested, but multiple shards need SO_REUSEPORT (Linux only)")]
    ShardsUnsupported(usize),
    /// The default parameters are invalid.
    #[error("invalid default parameters: {0}")]
    Defaults(#[from] WireError),
    /// The default parameters exceed the size limits.
    #[error("default parameters out of range: {0}")]
    DefaultsOutOfRange(#[from] ParamsError),
    /// An empty CPU list was given.
    #[error("the CPU list is empty")]
    EmptyCpuList,
    /// Binding a listener failed.
    #[error("binding {addr}: {source}")]
    Bind {
        /// The address that failed.
        addr: SocketAddr,
        /// The underlying error.
        #[source]
        source: io::Error,
    },
    /// Creating a shard's poll, timer or thread failed.
    #[error("setting up shard {shard}: {source}")]
    Setup {
        /// The shard index.
        shard: usize,
        /// The underlying error.
        #[source]
        source: io::Error,
    },
    /// Pinning a shard thread failed.
    #[error("pinning shard {shard} to CPU {cpu}: {source}")]
    Pin {
        /// The shard index.
        shard: usize,
        /// The requested CPU.
        cpu: usize,
        /// The underlying error.
        #[source]
        source: io::Error,
    },
    /// A shard's event loop failed.
    #[error("shard {shard} failed: {source}")]
    Shard {
        /// The shard index.
        shard: usize,
        /// The underlying error.
        #[source]
        source: io::Error,
    },
    /// A shard thread panicked.
    #[error("shard {0} panicked")]
    Panicked(usize),
}

/// A running mock server. Dropping it stops and joins every shard.
#[derive(Debug)]
pub struct ServerHandle {
    addr: SocketAddr,
    stats: Arc<Stats>,
    stop: Arc<AtomicBool>,
    wakers: Vec<mio::Waker>,
    threads: Vec<JoinHandle<io::Result<()>>>,
    /// Receives the index of every shard thread as it exits.
    exited: mpsc::Receiver<usize>,
}

/// Starts shard `index` on its own thread, pinned to `cpu` if given. The
/// thread reports on `ready` once pinned (or why pinning failed) and on
/// `notice` when it exits.
fn spawn_shard(
    shard: Shard,
    index: usize,
    cpu: Option<usize>,
    ready: mpsc::Sender<Result<(), ServerError>>,
    notice: ExitNotice,
) -> io::Result<JoinHandle<io::Result<()>>> {
    thread::Builder::new()
        .name(format!("mock-shard-{index}"))
        .spawn(move || {
            let _notice = notice;
            if let Some(cpu) = cpu
                && let Err(source) = cpu::pin_current_thread(&[cpu])
            {
                let _ = ready.send(Err(ServerError::Pin {
                    shard: index,
                    cpu,
                    source,
                }));
                return Ok(());
            }
            let _ = ready.send(Ok(()));
            shard.run()
        })
}

/// Reports a shard thread's exit, including exit by panic, when dropped.
struct ExitNotice {
    shard: usize,
    tx: mpsc::Sender<usize>,
}

impl Drop for ExitNotice {
    fn drop(&mut self) {
        // The receiver is gone only when the handle is, and then nobody waits.
        let _ = self.tx.send(self.shard);
    }
}

impl ServerHandle {
    /// Binds the listeners and starts the shard threads. Returns once every
    /// shard is pinned (if requested) and accepting.
    pub fn start(config: MockConfig) -> Result<Self, ServerError> {
        let MockConfig {
            listen,
            shards,
            spin_window,
            emit_policy,
            commit_window,
            cpus,
            tls,
            defaults,
            model,
            backlog,
        } = config;
        if shards == 0 {
            return Err(ServerError::NoShards);
        }
        if shards > 1 && !cfg!(target_os = "linux") {
            return Err(ServerError::ShardsUnsupported(shards));
        }
        if cpus.as_ref().is_some_and(Vec::is_empty) {
            return Err(ServerError::EmptyCpuList);
        }
        defaults.validate()?;
        check_limits(&defaults)?;

        let options = ListenOptions {
            reuse_port: shards > 1,
            backlog,
        };
        let bind = |addr| {
            bind_listener(addr, options).map_err(|source| ServerError::Bind { addr, source })
        };
        let first = bind(listen)?;
        // With port 0 the first bind picks the port all other shards share.
        let addr = first.local_addr().map_err(|source| ServerError::Bind {
            addr: listen,
            source,
        })?;
        let mut listeners = vec![first];
        for _ in 1..shards {
            listeners.push(bind(addr)?);
        }

        let emit = EmitSettings {
            policy: emit_policy,
            spin_window,
            commit_window,
        };
        let settings = Arc::new(Settings {
            defaults,
            model,
            tls,
            emit,
        });
        let stats = Stats::new(shards, emit);
        let stop = Arc::new(AtomicBool::new(false));
        let (exit_tx, exited) = mpsc::channel();
        let mut handle = Self {
            addr,
            stats: Arc::clone(&stats),
            stop: Arc::clone(&stop),
            wakers: Vec::with_capacity(shards),
            threads: Vec::with_capacity(shards),
            exited,
        };

        let (ready_tx, ready_rx) = mpsc::channel();
        for (index, listener) in listeners.into_iter().enumerate() {
            let setup = |source| ServerError::Setup {
                shard: index,
                source,
            };
            let poll = mio::Poll::new().map_err(setup)?;
            handle
                .wakers
                .push(mio::Waker::new(poll.registry(), WAKER).map_err(setup)?);
            let shard = Shard::new(
                index,
                poll,
                listener,
                Arc::clone(&settings),
                Arc::clone(&stats),
                Arc::clone(&stop),
            )
            .map_err(setup)?;
            let cpu = cpus.as_ref().map(|cpus| cpus[index % cpus.len()]);
            let notice = ExitNotice {
                shard: index,
                tx: exit_tx.clone(),
            };
            let thread = spawn_shard(shard, index, cpu, ready_tx.clone(), notice).map_err(setup)?;
            handle.threads.push(thread);
        }
        drop(ready_tx);
        for _ in 0..shards {
            match ready_rx.recv() {
                Ok(Ok(())) => {}
                Ok(Err(err)) => return Err(err),
                // A shard thread died before reporting; `wait` surfaces why.
                Err(mpsc::RecvError) => break,
            }
        }
        Ok(handle)
    }

    /// The address every shard listens on.
    pub fn local_addr(&self) -> SocketAddr {
        self.addr
    }

    /// The server's counters.
    pub fn stats(&self) -> &Arc<Stats> {
        &self.stats
    }

    fn signal_stop(&self) {
        self.stop.store(true, Ordering::Release);
        for waker in &self.wakers {
            // A failed wake leaves that shard running until its next event;
            // the stop flag is still observed then.
            let _ = waker.wake();
        }
    }

    fn join_all(&mut self) -> Result<(), ServerError> {
        let mut first_error = None;
        let threads = std::mem::take(&mut self.threads);
        for (shard, thread) in threads.into_iter().enumerate() {
            let result = match thread.join() {
                Ok(Ok(())) => Ok(()),
                Ok(Err(source)) => Err(ServerError::Shard { shard, source }),
                Err(_) => Err(ServerError::Panicked(shard)),
            };
            if let Err(err) = result {
                // One failed shard takes the whole server down with it.
                self.signal_stop();
                first_error.get_or_insert(err);
            }
        }
        first_error.map_or(Ok(()), Err)
    }

    /// Blocks until a shard exits, which only happens when it fails, then
    /// stops the others and reports the failure.
    pub fn wait(mut self) -> Result<(), ServerError> {
        // Every shard holds a sender, so this returns as soon as one exits.
        let _ = self.exited.recv();
        self.signal_stop();
        self.join_all()
    }

    /// Stops every shard and waits for them to exit.
    pub fn shutdown(mut self) -> Result<(), ServerError> {
        self.signal_stop();
        self.join_all()
    }
}

impl Drop for ServerHandle {
    fn drop(&mut self) {
        if !self.threads.is_empty() {
            self.signal_stop();
            // Errors were only of interest to `wait`/`shutdown` callers.
            let _ = self.join_all();
        }
    }
}
