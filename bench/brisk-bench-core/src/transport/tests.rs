use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use mio::{Events, Poll, Token};

use super::certs::CertBundle;
use super::{Conn, ListenOptions, bind_listener, tls};

const LISTENER: Token = Token(0);
const CLIENT: Token = Token(1);
const SERVER: Token = Token(2);

fn loopback() -> SocketAddr {
    "127.0.0.1:0".parse().unwrap()
}

struct Tls {
    server: Arc<rustls::ServerConfig>,
    client: Arc<rustls::ClientConfig>,
}

fn tls_configs() -> Tls {
    let bundle = CertBundle::generate(&["localhost".to_owned(), "127.0.0.1".to_owned()]).unwrap();
    Tls {
        server: tls::server_config_from_pem(
            bundle.server_cert_pem.as_bytes(),
            bundle.server_key_pem.as_bytes(),
        )
        .unwrap(),
        client: tls::client_config_from_pem(bundle.ca_cert_pem.as_bytes()).unwrap(),
    }
}

/// Reads until `WouldBlock`, appending plaintext to `buf`. Returns whether the
/// peer closed.
fn drain(conn: &mut Conn, buf: &mut Vec<u8>) -> io::Result<bool> {
    loop {
        match conn.read(buf) {
            Ok(batch) if batch.eof => return Ok(true),
            Ok(_) => {}
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => return Ok(false),
            Err(err) => return Err(err),
        }
    }
}

/// Client sends `payload`; the server echoes everything back; returns what the
/// client received once it has the full echo.
fn echo_round_trip(tls: Option<&Tls>, payload: &[u8]) -> io::Result<Vec<u8>> {
    let mut poll = Poll::new()?;
    let mut events = Events::with_capacity(64);
    let mut listener = bind_listener(loopback(), ListenOptions::default())?;
    let addr = listener.local_addr()?;
    poll.registry()
        .register(&mut listener, LISTENER, mio::Interest::READABLE)?;

    let mut client = match tls {
        Some(t) => Conn::connect_tls(
            addr,
            t.client.clone(),
            tls::server_name("localhost").unwrap(),
        ),
        None => Conn::connect_plain(addr),
    }?;
    client.write(payload)?;
    client.register(poll.registry(), CLIENT)?;

    let mut server: Option<Conn> = None;
    let mut client_rx = Vec::new();
    let mut server_rx = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(20);

    while client_rx.len() < payload.len() {
        assert!(Instant::now() < deadline, "echo timed out");
        poll.poll(&mut events, Some(Duration::from_millis(100)))?;
        for event in &events {
            match event.token() {
                LISTENER => {
                    let (stream, _) = listener.accept()?;
                    let mut conn = match tls {
                        Some(t) => Conn::accept_tls(stream, t.server.clone()),
                        None => Conn::accept_plain(stream),
                    }?;
                    assert!(conn.stream().nodelay()?);
                    conn.register(poll.registry(), SERVER)?;
                    server = Some(conn);
                }
                SERVER => {
                    let conn = server.as_mut().expect("server accepted");
                    if event.is_readable() {
                        drain(conn, &mut server_rx)?;
                        if !server_rx.is_empty() {
                            conn.write(&server_rx)?;
                            server_rx.clear();
                        }
                    }
                    if event.is_writable() {
                        conn.flush()?;
                    }
                    conn.sync_interest(poll.registry(), SERVER)?;
                }
                CLIENT => {
                    if event.is_writable() {
                        client.flush()?;
                    }
                    if event.is_readable() && drain(&mut client, &mut client_rx)? {
                        return Err(io::ErrorKind::UnexpectedEof.into());
                    }
                    client.sync_interest(poll.registry(), CLIENT)?;
                }
                other => panic!("unexpected token {other:?}"),
            }
        }
    }
    assert!(!client.is_handshaking());
    assert!(client.stream().nodelay()?);
    if tls.is_some() {
        assert_eq!(
            client.tls().unwrap().alpn_protocol(),
            Some(tls::ALPN_HTTP11)
        );
    }
    Ok(client_rx)
}

fn payload(len: usize) -> Vec<u8> {
    (0..len)
        .map(|i| u8::try_from(i % 251).expect("< 251"))
        .collect()
}

#[test]
fn plaintext_echo() {
    let data = payload(4 * 1024 * 1024);
    assert_eq!(echo_round_trip(None, &data).unwrap(), data);
}

#[test]
fn tls_echo() {
    let tls = tls_configs();
    let data = payload(4 * 1024 * 1024);
    assert_eq!(echo_round_trip(Some(&tls), &data).unwrap(), data);
}

#[test]
fn tls_rejects_untrusted_server() {
    let good = tls_configs();
    let other = tls_configs();
    let mixed = Tls {
        server: good.server,
        client: other.client,
    };
    let err = echo_round_trip(Some(&mixed), b"hi").unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::InvalidData, "{err}");
}

#[test]
fn certs_and_configs_from_files() {
    let dir = std::env::temp_dir().join(format!(
        "brisk-bench-core-certs-{}-{}",
        std::process::id(),
        crate::clock::now_ns()
    ));
    super::certs::generate(&dir, &["localhost".to_owned(), "::1".to_owned()]).unwrap();
    tls::server_config(
        &dir.join(super::certs::SERVER_CERT_FILE),
        &dir.join(super::certs::SERVER_KEY_FILE),
    )
    .unwrap();
    tls::client_config(&dir.join(super::certs::CA_CERT_FILE)).unwrap();
    assert!(matches!(
        tls::client_config(&dir.join("missing.pem")),
        Err(tls::TlsError::Io { .. })
    ));
    assert!(matches!(
        tls::client_config_from_pem(b"not pem"),
        Err(tls::TlsError::NoCertificates(_))
    ));
    std::fs::remove_dir_all(&dir).unwrap();
    assert!(matches!(
        super::certs::CertBundle::generate(&[]),
        Err(super::certs::CertError::NoSans)
    ));
}

#[cfg(not(target_os = "linux"))]
#[test]
fn reuse_port_is_rejected_off_linux() {
    let err = bind_listener(
        loopback(),
        ListenOptions {
            reuse_port: true,
            ..ListenOptions::default()
        },
    )
    .unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::Unsupported);
}

#[cfg(target_os = "linux")]
#[test]
fn reuse_port_allows_sharded_listeners() {
    let options = ListenOptions {
        reuse_port: true,
        ..ListenOptions::default()
    };
    let first = bind_listener(loopback(), options).unwrap();
    let addr = first.local_addr().unwrap();
    let second = bind_listener(addr, options).unwrap();
    assert_eq!(second.local_addr().unwrap(), addr);
}

/// Kernel receive timestamps are realtime and convert into the monotonic
/// window around the send and the read.
#[cfg(target_os = "linux")]
#[test]
fn rx_timestamp_converts_to_monotonic() {
    use crate::clock::{RealtimeOffset, now_ns};

    let mut poll = Poll::new().unwrap();
    let mut events = Events::with_capacity(16);
    let listener = bind_listener(loopback(), ListenOptions::default()).unwrap();
    let addr = listener.local_addr().unwrap();
    let mut client = Conn::connect_plain(addr).unwrap();
    client.register(poll.registry(), CLIENT).unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    while !client.flush().unwrap() {
        assert!(Instant::now() < deadline);
        poll.poll(&mut events, Some(Duration::from_millis(50)))
            .unwrap();
    }
    let (stream, _) = loop {
        match listener.accept() {
            Ok(accepted) => break accepted,
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                assert!(Instant::now() < deadline);
                std::thread::sleep(Duration::from_millis(1));
            }
            Err(err) => panic!("{err}"),
        }
    };
    let mut server = Conn::accept_plain(stream).unwrap();
    server.enable_rx_timestamps().unwrap();
    server.register(poll.registry(), SERVER).unwrap();

    let offset = RealtimeOffset::new();
    let sent_at = now_ns();
    client.write(b"timestamp me").unwrap();
    let mut buf = Vec::new();
    let batch = loop {
        assert!(Instant::now() < deadline);
        poll.poll(&mut events, Some(Duration::from_millis(50)))
            .unwrap();
        match server.read(&mut buf) {
            Ok(batch) => break batch,
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => {}
            Err(err) => panic!("{err}"),
        }
    };
    let read_at = now_ns();
    assert_eq!(&buf[..], b"timestamp me");
    let rx_real = batch
        .kernel_rx_ns
        .expect("SO_TIMESTAMPNS yields a timestamp");
    let rx_mono = offset.to_mono(rx_real);
    let slack = 200_000 + offset.uncertainty_ns();
    assert!(
        rx_mono + slack >= sent_at && rx_mono <= read_at + slack,
        "rx_mono={rx_mono} sent_at={sent_at} read_at={read_at}"
    );
}

/// A registered, connected client/server pair whose handshake has completed
/// (one ping/pong exchange), with both receive queues drained.
struct Pair {
    poll: Poll,
    events: Events,
    client: Conn,
    server: Conn,
}

impl Pair {
    fn connect(tls: Option<&Tls>) -> Self {
        let poll = Poll::new().unwrap();
        let events = Events::with_capacity(64);
        let listener = bind_listener(loopback(), ListenOptions::default()).unwrap();
        let addr = listener.local_addr().unwrap();
        let mut client = match tls {
            Some(t) => Conn::connect_tls(
                addr,
                t.client.clone(),
                tls::server_name("localhost").unwrap(),
            ),
            None => Conn::connect_plain(addr),
        }
        .unwrap();
        client.register(poll.registry(), CLIENT).unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        let (stream, _) = loop {
            match listener.accept() {
                Ok(accepted) => break accepted,
                Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                    assert!(Instant::now() < deadline, "accept timed out");
                    std::thread::sleep(Duration::from_millis(1));
                }
                Err(err) => panic!("{err}"),
            }
        };
        let mut server = match tls {
            Some(t) => Conn::accept_tls(stream, t.server.clone()),
            None => Conn::accept_plain(stream),
        }
        .unwrap();
        server.register(poll.registry(), SERVER).unwrap();
        let mut pair = Self {
            poll,
            events,
            client,
            server,
        };
        pair.client.write(b"ping").unwrap();
        let (mut client_rx, mut server_rx) = (Vec::new(), Vec::new());
        // Both sides read in every round so the TLS handshake can progress.
        pair.pump_until(|p| {
            drain(&mut p.client, &mut client_rx).unwrap();
            drain(&mut p.server, &mut server_rx).unwrap();
            server_rx == b"ping"
        });
        pair.server.write(b"pong").unwrap();
        pair.pump_until(|p| {
            drain(&mut p.client, &mut client_rx).unwrap();
            drain(&mut p.server, &mut server_rx).unwrap();
            client_rx == b"pong" && !p.client.is_handshaking() && !p.server.is_handshaking()
        });
        pair
    }

    /// Flushes both sides and polls until `done` holds.
    fn pump_until(&mut self, mut done: impl FnMut(&mut Self) -> bool) {
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            self.client.flush().unwrap();
            self.server.flush().unwrap();
            if done(self) {
                return;
            }
            self.client
                .sync_interest(self.poll.registry(), CLIENT)
                .unwrap();
            self.server
                .sync_interest(self.poll.registry(), SERVER)
                .unwrap();
            assert!(Instant::now() < deadline, "pump timed out");
            self.poll
                .poll(&mut self.events, Some(Duration::from_millis(20)))
                .unwrap();
        }
    }
}

/// Writes more than the socket accepts, shuts down and checks that the peer
/// receives every byte before the end-of-stream.
fn shutdown_delivers_pending(tls: Option<&Tls>) {
    let mut pair = Pair::connect(tls);
    // Small buffers on both ends make the kernel refuse most of the payload
    // (Windows loopback otherwise buffers several megabytes).
    socket2::SockRef::from(pair.client.stream())
        .set_send_buffer_size(4096)
        .unwrap();
    socket2::SockRef::from(pair.server.stream())
        .set_recv_buffer_size(4096)
        .unwrap();
    // Windows may accept one oversized send whole, so keep writing slices
    // until output is left pending.
    let slice = payload(256 * 1024);
    let mut data = Vec::new();
    loop {
        assert!(data.len() < 512 * 1024 * 1024, "kernel took everything");
        data.extend_from_slice(&slice);
        if !pair.client.write(&slice).unwrap() {
            break;
        }
    }
    assert!(
        !pair.client.shutdown().unwrap(),
        "shut down with output pending"
    );
    assert!(pair.client.pending_bytes() > 0);
    assert_eq!(
        pair.client.write(b"late").unwrap_err().kind(),
        io::ErrorKind::BrokenPipe
    );

    let mut rx = Vec::new();
    let mut eof = false;
    let mut shut = false;
    pair.pump_until(|p| {
        shut = shut || p.client.shutdown().unwrap();
        eof = eof || drain(&mut p.server, &mut rx).unwrap();
        eof
    });
    assert!(shut, "eof arrived before the shutdown completed");
    assert_eq!(rx.len(), data.len());
    assert!(rx.iter().eq(&data), "payload corrupted");
    assert!(pair.client.shutdown().unwrap(), "shutdown is idempotent");
}

#[test]
fn plaintext_shutdown_delivers_pending_output() {
    shutdown_delivers_pending(None);
}

#[test]
fn tls_shutdown_delivers_pending_output() {
    shutdown_delivers_pending(Some(&tls_configs()));
}

/// A TLS peer that closes the TCP connection without `close_notify` ends the
/// stream like a FIN instead of failing the read.
#[test]
fn tls_peer_close_without_close_notify_is_eof() {
    let mut pair = Pair::connect(Some(&tls_configs()));
    pair.client.write(b"last words").unwrap();
    pair.client.flush().unwrap();
    pair.client
        .stream()
        .shutdown(std::net::Shutdown::Write)
        .unwrap();
    let mut rx = Vec::new();
    pair.pump_until(|p| drain(&mut p.server, &mut rx).unwrap());
    assert_eq!(&rx[..], b"last words");
}

/// A plaintext FIN is reported as `eof` after all data.
#[test]
fn plaintext_peer_close_is_eof() {
    let mut pair = Pair::connect(None);
    pair.client.write(b"bye").unwrap();
    assert!(pair.client.shutdown().unwrap());
    let mut rx = Vec::new();
    pair.pump_until(|p| drain(&mut p.server, &mut rx).unwrap());
    assert_eq!(&rx[..], b"bye");
}

#[test]
fn connect_refused_is_reported() {
    let addr = {
        let listener = bind_listener(loopback(), ListenOptions::default()).unwrap();
        listener.local_addr().unwrap()
    };
    let mut poll = Poll::new().unwrap();
    let mut events = Events::with_capacity(8);
    let mut client = match Conn::connect_plain(addr) {
        Ok(client) => client,
        Err(err) => {
            assert_eq!(err.kind(), io::ErrorKind::ConnectionRefused, "{err}");
            return;
        }
    };
    client.register(poll.registry(), CLIENT).unwrap();
    // Windows retries refused loopback connects for about two seconds.
    let deadline = Instant::now() + Duration::from_secs(10);
    let err = loop {
        assert!(Instant::now() < deadline, "connect error not reported");
        poll.poll(&mut events, Some(Duration::from_millis(50)))
            .unwrap();
        match client.flush() {
            Ok(false) => {}
            Ok(true) => panic!("connected to a closed port"),
            Err(err) => break err,
        }
    };
    // Windows keeps no SO_ERROR for a failed nonblocking connect, so mio's
    // prescribed check surfaces the failure as the `getpeername` error.
    if cfg!(not(windows)) {
        assert_eq!(err.kind(), io::ErrorKind::ConnectionRefused, "{err}");
    }
}

/// One TLS read takes up to `read_size` bytes of ciphertext from the kernel,
/// not just the 4 KiB rustls reads on its own.
#[test]
fn tls_read_uses_read_size() {
    let mut pair = Pair::connect(Some(&tls_configs()));
    let data = payload(256 * 1024);
    pair.client.write(&data).unwrap();
    pair.pump_until(|p| p.client.pending_bytes() == 0);
    // Let the whole payload reach the server's receive queue.
    std::thread::sleep(Duration::from_millis(100));
    let mut rx = Vec::new();
    let first = pair.server.read(&mut rx).unwrap();
    assert!(
        first.plaintext_bytes > 16 * 1024,
        "one read yielded {} bytes",
        first.plaintext_bytes
    );
    pair.pump_until(|p| {
        drain(&mut p.server, &mut rx).unwrap();
        rx.len() == data.len()
    });
    assert!(rx.iter().eq(&data), "payload corrupted");
}

#[cfg(target_os = "linux")]
#[test]
fn tls_reads_carry_rx_timestamps() {
    let mut pair = Pair::connect(Some(&tls_configs()));
    pair.server.enable_rx_timestamps().unwrap();
    pair.client.write(b"stamp").unwrap();
    let mut rx = Vec::new();
    let mut stamped = None;
    pair.pump_until(|p| {
        loop {
            match p.server.read(&mut rx) {
                Ok(batch) if batch.plaintext_bytes > 0 => stamped = Some(batch.kernel_rx_ns),
                Ok(_) => {}
                Err(err) if err.kind() == io::ErrorKind::WouldBlock => break,
                Err(err) => panic!("{err}"),
            }
        }
        rx == b"stamp"
    });
    assert!(
        stamped.expect("a batch carried the plaintext").is_some(),
        "TLS batch without kernel timestamp"
    );
}

#[cfg(unix)]
#[test]
fn rewritten_key_file_is_private() {
    use std::os::unix::fs::PermissionsExt as _;

    let dir = std::env::temp_dir().join(format!(
        "brisk-bench-core-perm-{}-{}",
        std::process::id(),
        crate::clock::now_ns()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let key = dir.join(super::certs::SERVER_KEY_FILE);
    std::fs::write(&key, "old").unwrap();
    std::fs::set_permissions(&key, std::fs::Permissions::from_mode(0o644)).unwrap();
    super::certs::generate(&dir, &["localhost".to_owned()]).unwrap();
    let mode = std::fs::metadata(&key).unwrap().permissions().mode() & 0o777;
    assert_eq!(mode, 0o600);
    std::fs::remove_dir_all(&dir).unwrap();
}
