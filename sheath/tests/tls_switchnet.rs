//! sheath's TLS client over switchnet TCP (as switch tasks) against a kernel rustls server
//! across net_tap: the alert for a rejected record, and close_notify, reach the server before the
//! connection may be dropped. Needs CAP_NET_ADMIN and /dev/net/tun (the Docker test container).

use std::cell::RefCell;
use std::future::poll_fn;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::os::fd::AsRawFd;
use std::pin::pin;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::Poll;
use std::thread;
use std::time::{Duration, Instant};

use rustls::pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer};
use rustls::version::TLS13;
use rustls::{ClientConfig, RootCertStore, ServerConfig, ServerConnection};
use sheath::switchnet::{Error as TlsError, Tls as ClientTls, send_bytes, tls_connect};
use switch::{Runtime, Spawner};
use switchnet::testkit::{Backend, KERNEL_ADDR, KernelPeer, ROUND_SLEEP, TapConfig, client_server, next_local_port, with_tap_net};
use switchnet::{Error as NetError, IpEndpoint, IpListenEndpoint, NetHandle, TcpStream, tcp};

const SERVER_PORT: u16 = 8443;
const HOST: &str = "tls.test";

const TAP: TapConfig = TapConfig {
    name: "tls",
    socket: tcp::Config { rx_capacity: 64 * 1024, tx_capacity: 64 * 1024, mss_cap: None },
    max_sockets: 1,
    random_seed: 1,
};

fn tls_configs() -> (Arc<ServerConfig>, Arc<ClientConfig>) {
    let rcgen::CertifiedKey { cert, signing_key } =
        rcgen::generate_simple_self_signed(vec![HOST.to_owned()]).expect("generate test certificate");
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(signing_key.serialize_der()));
    let server = ServerConfig::builder_with_provider(provider.clone())
        .with_protocol_versions(&[&TLS13])
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(vec![cert.der().clone()], key)
        .unwrap();
    let mut roots = RootCertStore::empty();
    roots.add(cert.der().clone()).unwrap();
    let client = ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&TLS13])
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth();
    (Arc::new(server), Arc::new(client))
}

/// One client against one kernel peer (see `client_server`), with the TLS configs they use.
fn run<T: Send + 'static, F: Future<Output = ()> + 'static>(
    backend: Backend,
    serve: impl FnOnce(std::net::TcpStream, Arc<ServerConfig>) -> T + Send + 'static,
    client: impl FnOnce(NetHandle, TcpStream, Arc<ClientConfig>) -> F + 'static,
) -> T {
    let (server_tls, client_tls) = tls_configs();
    client_server(
        backend,
        &TAP,
        SERVER_PORT,
        |_| {},
        move |stream| serve(stream, server_tls),
        move |handle, stream| client(handle, stream, client_tls),
        || {},
    )
}

/// Bytes the server sends after the record the client rejects: still unread when the client
/// drops its socket, which then aborts with RST (as Linux `close()` does).
const TRAILING: usize = 16 * 1024;

/// Everything the client sends until its FIN or RST, given to the server's `conn`; returns the
/// error the server's rustls saw (the client's alert, if it got out before an RST).
fn read_alert(conn: &mut ServerConnection, stream: &mut std::net::TcpStream) -> Result<(), rustls::Error> {
    let mut sent = Vec::new();
    let mut buf = [0; 4096];
    loop {
        match stream.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => sent.extend_from_slice(&buf[..n]),
            Err(e) if e.kind() == std::io::ErrorKind::ConnectionReset => break,
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
            Err(e) => panic!("read the client's records: {e}"),
        }
    }
    let mut rest = &sent[..];
    while !rest.is_empty() {
        conn.read_tls(&mut rest).expect("buffer the client's records");
    }
    conn.process_new_packets().map(|_| ())
}

const BAD_RECORD_MAC: Result<(), rustls::Error> =
    Err(rustls::Error::AlertReceived(rustls::AlertDescription::BadRecordMac));

/// Runs the server handshake until it has its first flight to write; returns that flight.
fn server_flight(conn: &mut ServerConnection, stream: &mut std::net::TcpStream) -> Vec<u8> {
    while !conn.wants_write() {
        conn.read_tls(stream).expect("read the ClientHello");
        conn.process_new_packets().expect("process the ClientHello");
    }
    let mut flight = Vec::new();
    while conn.wants_write() {
        conn.write_tls(&mut flight).unwrap();
    }
    flight
}

/// The server's handshake flight arrives with its last record (the one carrying Finished)
/// corrupted, and more data behind it. rustls rejects the record; `tls_connect` sends rustls's
/// bad_record_mac alert and FIN, and they reach the server before the RST that dropping the
/// socket with the trailing data unread causes.
fn rejected_handshake_sends_the_alert_before_the_rst(backend: Backend) {
    let seen = run(
        backend,
        |mut stream, config| {
            let mut conn = ServerConnection::new(config).unwrap();
            let mut flight = server_flight(&mut conn, &mut stream);
            *flight.last_mut().unwrap() ^= 1;
            flight.resize(flight.len() + TRAILING, 0);
            stream.write_all(&flight).unwrap();
            read_alert(&mut conn, &mut stream)
        },
        |handle, stream, config| async move {
            match tls_connect(&handle, stream, config, HOST, 1).await {
                Err(TlsError::Tls(sheath::Error::Tls(rustls::Error::DecryptError))) => {}
                Err(e) => panic!("unexpected error {e}"),
                Ok(_) => panic!("the corrupted Finished record was accepted"),
            }
        },
    );
    assert_eq!(seen, BAD_RECORD_MAC);
}

/// A corrupted application data record after the handshake, with more data behind it:
/// `Tls::ready` returns the error after the alert and FIN went out, so the client may drop the
/// connection at once.
fn receive_error_after_the_handshake_sends_the_alert_before_the_rst(backend: Backend) {
    let seen = run(
        backend,
        |mut stream, config| {
            let mut conn = ServerConnection::new(config).unwrap();
            while conn.is_handshaking() {
                conn.complete_io(&mut stream).expect("server handshake");
            }
            conn.writer().write_all(b"corrupted").unwrap();
            let mut record = Vec::new();
            while conn.wants_write() {
                conn.write_tls(&mut record).unwrap();
            }
            *record.last_mut().unwrap() ^= 1;
            record.resize(record.len() + TRAILING, 0);
            stream.write_all(&record).unwrap();
            read_alert(&mut conn, &mut stream)
        },
        |handle, stream, config| async move {
            let mut tls: ClientTls = tls_connect(&handle, stream, config, HOST, 1).await.expect("handshake");
            match tls.ready().await {
                Err(TlsError::Tls(sheath::Error::Tls(rustls::Error::DecryptError))) => {}
                other => panic!("unexpected {other:?}"),
            }
        },
    );
    assert_eq!(seen, BAD_RECORD_MAC);
}


/// Every test's client and every peer socket read and write.
const LIMIT: Duration = Duration::from_secs(60);
/// The server's SO_RCVBUF in the backlog tests: its window takes a few KiB of `BULK`, the rest
/// waits in the client's socket (64 KiB of send buffer).
const SMALL_RCVBUF: libc::c_int = 4096;
const BULK: usize = 32 * 1024;
/// How long a server that waits for the client to reach its last step sleeps on, so that the
/// client, if it did not wait for the server, has finished and the Net stopped turning.
const SETTLE: Duration = Duration::from_millis(200);
/// A server read stalling this long means the client sends nothing more.
const STALL: Duration = Duration::from_secs(5);

/// Connects to the kernel peer from a fresh local port, runs `client` on the stream, and puts
/// what it hands back in `held`.
#[switch::task]
async fn connect_client<
    H: 'static,
    F: Future<Output = H> + 'static,
    C: FnOnce(NetHandle, Spawner, TcpStream, Arc<ClientConfig>) -> F + 'static,
>(
    handle: NetHandle,
    spawner: Spawner,
    tls: Arc<ClientConfig>,
    client: C,
    held: Rc<RefCell<Option<H>>>,
) {
    let remote = IpEndpoint::new(KERNEL_ADDR.into(), SERVER_PORT);
    let local = IpListenEndpoint { addr: None, port: next_local_port() };
    let stream = handle.tcp_connect(TAP.socket, remote, local).await.expect("TCP connect");
    *held.borrow_mut() = Some(client(handle, spawner, stream, tls).await);
}

/// One client against one kernel peer, like `client_server`, except that the Net is turned only
/// until the client task finishes, and the task gets the runtime's spawner (for timers). What
/// the peer reads after that was on the wire by then, not sent by later turns. What the client
/// returns is dropped only after the peer is done.
fn until_client<T: Send + 'static, H: 'static, F: Future<Output = H> + 'static>(
    backend: Backend,
    listener_setup: impl FnOnce(&TcpListener),
    serve: impl FnOnce(std::net::TcpStream, Arc<ServerConfig>) -> T + Send + 'static,
    client: impl FnOnce(NetHandle, Spawner, TcpStream, Arc<ClientConfig>) -> F + 'static,
) -> T {
    let (server_tls, client_tls) = tls_configs();
    with_tap_net(backend, &TAP, |net, clock| {
        let mut peer = KernelPeer::listen(SERVER_PORT, listener_setup, move |stream| serve(stream, server_tls));
        let mut rt = Runtime::new(0);
        let handle = net.handle();
        let spawner = rt.spawner();
        let held: Rc<RefCell<Option<H>>> = Rc::default();
        let task = connect_client(handle, spawner, client_tls, client, held.clone());
        rt.spawner().spawn(task).expect("spawn the client");
        while held.borrow().is_none() {
            assert!(clock.elapsed() < LIMIT, "client not done");
            peer.accept();
            net.turn(clock.now()).expect("turn the Net");
            rt.poll(clock.ms()).expect("poll the client");
            thread::sleep(ROUND_SLEEP);
        }
        let saw = peer.join();
        drop(held.borrow_mut().take());
        rt.shutdown();
        saw
    })
}

/// SO_RCVBUF of `size` bytes on `listener`; accepted sockets inherit it.
fn set_rcvbuf(listener: &TcpListener, size: libc::c_int) {
    // SAFETY: a valid socket and an int option of the right size.
    let rc = unsafe {
        libc::setsockopt(
            listener.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_RCVBUF,
            (&size as *const libc::c_int).cast(),
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        )
    };
    assert_eq!(rc, 0, "set SO_RCVBUF");
}

fn server_handshake(stream: &mut std::net::TcpStream, config: Arc<ServerConfig>) -> ServerConnection {
    let mut conn = ServerConnection::new(config).unwrap();
    while conn.is_handshaking() {
        conn.complete_io(stream).expect("server handshake");
    }
    conn
}

/// Writes an application data record the client's rustls rejects (bad_record_mac), then
/// `trailing` zero bytes.
fn write_corrupted(conn: &mut ServerConnection, stream: &mut std::net::TcpStream, trailing: usize) {
    conn.writer().write_all(b"corrupted").unwrap();
    let mut record = Vec::new();
    while conn.wants_write() {
        conn.write_tls(&mut record).unwrap();
    }
    *record.last_mut().unwrap() ^= 1;
    record.resize(record.len() + trailing, 0);
    stream.write_all(&record).unwrap();
}

/// Sleeps until `flag` is set, then `SETTLE` more.
fn wait_for(flag: &AtomicBool) {
    let started = Instant::now();
    while !flag.load(Ordering::Acquire) {
        assert!(started.elapsed() < LIMIT, "the client never got there");
        thread::sleep(Duration::from_millis(1));
    }
    thread::sleep(SETTLE);
}

/// What the server read from the client until end of stream (or a `STALL`).
#[derive(Debug, PartialEq)]
struct Seen {
    plaintext: usize,
    /// The client's close_notify was read.
    close_notify: bool,
    /// The error the server's rustls saw (the client's alert).
    tls: Result<(), rustls::Error>,
    /// TCP end of stream (the client's FIN) was read.
    eof: bool,
}

fn read_to_end(conn: &mut ServerConnection, stream: &mut std::net::TcpStream) -> Seen {
    stream.set_read_timeout(Some(STALL)).unwrap();
    let mut seen = Seen { plaintext: 0, close_notify: false, tls: Ok(()), eof: false };
    let mut buf = vec![0; 64 * 1024];
    loop {
        let n = match stream.read(&mut buf) {
            Ok(0) => {
                seen.eof = true;
                return seen;
            }
            Ok(n) => n,
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(e) if matches!(e.kind(), std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut) => return seen,
            Err(e) => panic!("read the client: {e}"),
        };
        let mut rest = &buf[..n];
        while !rest.is_empty() && seen.tls.is_ok() {
            conn.read_tls(&mut rest).expect("buffer the client's records");
            // records before a failing one were decrypted: their plaintext is read either way
            seen.tls = conn.process_new_packets().map(|_| ());
            loop {
                match conn.reader().read(&mut [0; 4096]) {
                    Ok(0) => {
                        seen.close_notify = true;
                        break;
                    }
                    Ok(n) => seen.plaintext += n,
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                    Err(_) if seen.tls.is_err() => break,
                    Err(e) => panic!("read the client's plaintext: {e}"),
                }
            }
        }
    }
}

/// `work`'s output, or `None` once `ms` milliseconds passed.
async fn within<T>(spawner: &Spawner, ms: u64, work: impl Future<Output = T>) -> Option<T> {
    let mut work = pin!(work);
    let mut limit = pin!(spawner.sleep(ms).expect("the runtime is open"));
    poll_fn(|cx| {
        if let Poll::Ready(out) = work.as_mut().poll(cx) {
            return Poll::Ready(Some(out));
        }
        limit.as_mut().poll(cx).map(|_| None)
    })
    .await
}

/// The client's records wait behind `BULK` bytes the server has no room for yet, and the client
/// is not turned once its task finishes: the bad_record_mac alert and FIN reach the server only
/// because `Tls::ready` returns its error after the server acked them.
fn receive_error_returns_once_the_alert_and_fin_are_delivered(backend: Backend) {
    let failing = Arc::new(AtomicBool::new(false));
    let server_failing = failing.clone();
    let seen = until_client(
        backend,
        |listener| set_rcvbuf(listener, SMALL_RCVBUF),
        move |mut stream, config| {
            let mut conn = server_handshake(&mut stream, config);
            write_corrupted(&mut conn, &mut stream, 0);
            wait_for(&server_failing);
            read_to_end(&mut conn, &mut stream)
        },
        move |handle, _, stream, config| async move {
            let mut tls: ClientTls = tls_connect(&handle, stream, config, HOST, 1).await.expect("handshake");
            send_bytes(&mut tls.tx, &[7; BULK]).await.expect("queue the bulk");
            failing.store(true, Ordering::Release);
            match tls.ready().await {
                Err(TlsError::Tls(sheath::Error::Tls(rustls::Error::DecryptError))) => {}
                other => panic!("unexpected {other:?}"),
            }
            tls
        },
    );
    assert_eq!(seen, Seen { plaintext: BULK, close_notify: false, tls: BAD_RECORD_MAC, eof: true });
}

/// As above for `Tls::close`: close_notify and FIN reach the server behind the backlog.
fn close_returns_once_close_notify_and_fin_are_delivered(backend: Backend) {
    let closing = Arc::new(AtomicBool::new(false));
    let server_closing = closing.clone();
    let seen = until_client(
        backend,
        |listener| set_rcvbuf(listener, SMALL_RCVBUF),
        move |mut stream, config| {
            let mut conn = server_handshake(&mut stream, config);
            wait_for(&server_closing);
            read_to_end(&mut conn, &mut stream)
        },
        move |handle, _, stream, config| async move {
            let mut tls: ClientTls = tls_connect(&handle, stream, config, HOST, 1).await.expect("handshake");
            send_bytes(&mut tls.tx, &[7; BULK]).await.expect("queue the bulk");
            closing.store(true, Ordering::Release);
            tls.close().await.expect("close");
            tls
        },
    );
    assert_eq!(seen, Seen { plaintext: BULK, close_notify: true, tls: Ok(()), eof: true });
}

/// A server that never reads, so it never acks the alert and FIN queued behind the backlog:
/// `Tls::ready` stays pending past the caller's deadline, and dropping the connection then frees
/// its socket slot (the only one): the next connect gets it (refused by a closed port).
fn receive_error_waits_for_a_server_that_never_reads_until_the_caller_gives_up(backend: Backend) {
    let done = Arc::new(AtomicBool::new(false));
    let server_done = done.clone();
    until_client(
        backend,
        |listener| set_rcvbuf(listener, SMALL_RCVBUF),
        move |mut stream, config| {
            let mut conn = server_handshake(&mut stream, config);
            // left unread by the client, so dropping its socket aborts with RST
            write_corrupted(&mut conn, &mut stream, TRAILING);
            wait_for(&server_done);
        },
        move |handle, spawner, stream, config| async move {
            let mut tls: ClientTls = tls_connect(&handle, stream, config, HOST, 1).await.expect("handshake");
            send_bytes(&mut tls.tx, &[7; BULK]).await.expect("queue the bulk");
            if let Some(out) = within(&spawner, 500, tls.ready()).await {
                panic!("returned before the server acked the alert: {out:?}");
            }
            drop(tls);
            let remote = IpEndpoint::new(KERNEL_ADDR.into(), SERVER_PORT + 1);
            let local = IpListenEndpoint { addr: None, port: next_local_port() };
            let started = Instant::now();
            let result = loop {
                match handle.tcp_connect(TAP.socket, remote, local).await {
                    Err(NetError::SocketsFull) => {
                        assert!(started.elapsed() < Duration::from_secs(5), "the dropped connection kept its slot");
                        spawner.sleep(1).expect("the runtime is open").await.expect("sleep");
                    }
                    result => break result,
                }
            };
            assert_eq!(result.err(), Some(NetError::Refused));
            done.store(true, Ordering::Release);
        },
    );
}

/// TCP_MAXSEG of `mss` on `listener`: accepted connections advertise it in their SYN-ACK.
fn set_maxseg(listener: &TcpListener, mss: libc::c_int) {
    // SAFETY: a valid socket and an int option of the right size.
    let rc = unsafe {
        libc::setsockopt(
            listener.as_raw_fd(),
            libc::IPPROTO_TCP,
            libc::TCP_MAXSEG,
            (&mss as *const libc::c_int).cast(),
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        )
    };
    assert_eq!(rc, 0, "set TCP_MAXSEG");
}

/// The peer advertises an MSS of 1200: SwitchTx sizes its frames by it (each frame one
/// segment, sent without a copy), and 64 KiB sent in those frames arrive whole and in order.
/// (Before, frames were sized by a fixed 1460 B whatever the peer's MSS.)
fn frames_follow_the_peer_mss(backend: Backend) {
    use sheath::FrameTx;
    use sheath::switchnet::SwitchTx;
    const LEN: usize = 64 * 1024;
    let data: Vec<u8> = (0..LEN).map(|i| (i * 7) as u8).collect();
    let sent = data.clone();
    let got = until_client(
        backend,
        |listener| set_maxseg(listener, 1200),
        |mut stream, _| {
            let mut got = vec![0; LEN];
            stream.read_exact(&mut got).unwrap();
            got
        },
        move |handle, _, stream, _| async move {
            let (rx, tcp) = stream.split();
            let mut tx = SwitchTx::new(&handle, tcp, 1).await.expect("setup");
            assert_eq!(tx.plain(), (0, 1200));
            send_bytes(&mut tx, &sent).await.expect("send");
            // the Net turns only until this returns: wait until the peer has every byte
            tx.shutdown();
            tx.delivered().await;
            (rx, tx)
        },
    );
    assert!(got == data, "the bytes arrived changed or out of order");
}

/// The server sends half-RTT data (RFC 8446 §2.3: application data right after its Finished,
/// before the client's Finished), 100 B in two records (its fragment limit is 64 B): the
/// handshake completes and the data is the first thing the client reads, whole.
fn half_rtt_data_right_after_the_server_finished_is_read(backend: Backend) {
    until_client(
        backend,
        |_| {},
        |mut stream, config| {
            let mut config = (*config).clone();
            config.send_half_rtt_data = true;
            config.max_fragment_size = Some(64);
            let mut conn = ServerConnection::new(Arc::new(config)).unwrap();
            while !conn.wants_write() {
                conn.read_tls(&mut stream).unwrap();
                conn.process_new_packets().unwrap();
            }
            conn.writer().write_all(&[5; 100]).unwrap();
            let mut flight = Vec::new();
            while conn.wants_write() {
                conn.write_tls(&mut flight).unwrap();
            }
            stream.write_all(&flight).unwrap();
            while conn.is_handshaking() {
                conn.complete_io(&mut stream).expect("server handshake");
            }
            let mut rest = Vec::new();
            let _ = stream.read_to_end(&mut rest);
        },
        move |handle, _, stream, config| async move {
            let mut tls: ClientTls = tls_connect(&handle, stream, config, HOST, 1).await.expect("handshake");
            let mut got = Vec::new();
            while got.len() < 100 {
                assert!(tls.ready().await.expect("receive"), "the stream ended");
                got.extend_from_slice(tls.rx.current());
                let n = tls.rx.current().len();
                tls.rx.consume(n);
            }
            assert_eq!(got, [5; 100]);
            tls.close().await.expect("close");
            tls
        },
    );
}

// Every TLS/socket contract uses the same assertions on both transports.
macro_rules! backend_tests {
    ($($body:ident => ($conduit:ident, $junction:ident)),+ $(,)?) => {
        $(
            #[zhida_test_support::pinned_test]
            fn $conduit() {
                $body(Backend::Conduit);
            }

            #[zhida_test_support::pinned_test]
            fn $junction() {
                $body(Backend::Junction);
            }
        )+
    };
}

backend_tests! {
    rejected_handshake_sends_the_alert_before_the_rst => (
        rejected_handshake_sends_the_alert_before_the_rst_conduit,
        rejected_handshake_sends_the_alert_before_the_rst_junction
    ),
    receive_error_after_the_handshake_sends_the_alert_before_the_rst => (
        receive_error_after_the_handshake_sends_the_alert_before_the_rst_conduit,
        receive_error_after_the_handshake_sends_the_alert_before_the_rst_junction
    ),
    receive_error_returns_once_the_alert_and_fin_are_delivered => (
        receive_error_returns_once_the_alert_and_fin_are_delivered_conduit,
        receive_error_returns_once_the_alert_and_fin_are_delivered_junction
    ),
    close_returns_once_close_notify_and_fin_are_delivered => (
        close_returns_once_close_notify_and_fin_are_delivered_conduit,
        close_returns_once_close_notify_and_fin_are_delivered_junction
    ),
    receive_error_waits_for_a_server_that_never_reads_until_the_caller_gives_up => (
        receive_error_waits_for_a_server_that_never_reads_until_the_caller_gives_up_conduit,
        receive_error_waits_for_a_server_that_never_reads_until_the_caller_gives_up_junction
    ),
    frames_follow_the_peer_mss => (
        frames_follow_the_peer_mss_conduit,
        frames_follow_the_peer_mss_junction
    ),
    half_rtt_data_right_after_the_server_finished_is_read => (
        half_rtt_data_right_after_the_server_finished_is_read_conduit,
        half_rtt_data_right_after_the_server_finished_is_read_junction
    ),
}
