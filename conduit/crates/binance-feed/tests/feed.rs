//! The binance_feed client (relay's wss client over sheath, as a switch task on a switchnet Net)
//! against a kernel TCP TLS WebSocket server (rustls + tungstenite) across net_tap through
//! Zhida's IPv4 gateway, as in the live run. These tests need CAP_NET_ADMIN and /dev/net/tun,
//! and no internet.

use std::borrow::Cow;
use std::cell::Cell;
use std::ffi::CStr;
use std::io::{ErrorKind, Read};
use std::net::{Ipv4Addr, SocketAddrV4, TcpListener};
use std::panic::resume_unwind;
use std::process::Command;
use std::rc::Rc;
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use binance_feed::{
    client_config, connect, run, socket, stream, webpki_client_config, BookTicker, Clock, Event,
    FeedError, Summary, Target, INGRESS_BUDGET,
};
use dpdk_smoltcp::smoltcp::iface::PathMtu;
use dpdk_smoltcp::smoltcp::socket::tcp;
use dpdk_smoltcp::smoltcp::time::Instant as StackInstant;
use dpdk_smoltcp::smoltcp::wire::{EthernetAddress, Ipv4Cidr};
use dpdk_smoltcp::{
    frame_reserve, rx_reserve, AddTcpError, Offloads, Port, PortConfig, Stack,
    StackConfig, DEFAULT_NEIGHBOR_CAPACITY,
};
use relay::UpgradeError;
use relay::switchnet::Error as WsError;
use rustls::pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer};
use rustls::{RootCertStore, ServerConfig, ServerConnection, StreamOwned};
use switch::Spawner;
use switchnet::{first, Net, NetConfig, NetHandle, OpenError};
use tungstenite::handshake::server::{ErrorResponse, Request, Response};
use tungstenite::protocol::CloseFrame;
use tungstenite::protocol::frame::coding::CloseCode;
use tungstenite::{Error, Message};
use zhida_buffer::PoolConfig;
use zhida_dpdk_sys::RTE_PKTMBUF_HEADROOM;
use zhida_test_kit::{tap::TapVdev, with_eal};

const VDEV: &CStr = c"net_tap_bnc";
const IFACE: &CStr = c"cdtap_bnc";
const RX_POOL: &CStr = c"bnc_rx";
const TX_POOL: &CStr = c"bnc_tx";
const FRAME_POOL: &CStr = c"bnc_frame";
const ZHIDA_ADDR: Ipv4Addr = Ipv4Addr::new(192, 0, 2, 2);
const KERNEL_ADDR: Ipv4Addr = Ipv4Addr::new(192, 0, 2, 1);
const SERVER_ADDR: Ipv4Addr = Ipv4Addr::new(198, 51, 100, 10);
const SERVER_PORT: u16 = 9443;
const HOST: &str = "binance.test";
const PATH: &str = "/ws/btcusdt@bookTicker";
const LOCAL_PORT: u16 = 49152;
const CONNECT_LIMIT: Duration = Duration::from_secs(20);
const CLOSE_LIMIT: Duration = Duration::from_secs(10);
/// Every test's client, and the server's accept and socket reads and writes.
const LIMIT: Duration = Duration::from_secs(60);
const IDLE_SLEEP: Duration = Duration::from_millis(1);

fn port_config(port_id: u16, mss_cap: Option<u16>) -> PortConfig<'static> {
    let pool = |name, count| PoolConfig {
        name,
        count,
        cache_size: 8,
        data_room_size: RTE_PKTMBUF_HEADROOM as u16 + 2048,
        socket_id: -1,
    };
    PortConfig {
        port_id,
        mtu: 1500,
        rx_descriptors: 16,
        tx_descriptors: 16,
        rx_burst: 32,
        tx_burst: 32,
        queue_socket: -1,
        rx_pool: pool(
            RX_POOL,
            u32::try_from(
                rx_reserve(16, 32, 8)
                    .checked_add(tcp::rx_mbuf_budget(&socket(mss_cap), 1500))
                    .expect("RX pool count fits usize"),
            )
            .unwrap(),
        ),
        tx_pool: pool(TX_POOL, 127),
        frame_pool: pool(
            FRAME_POOL,
            u32::try_from(
                frame_reserve(16, 32)
                    .checked_add(tcp::tx_mbuf_budget(&socket(mss_cap), 1500))
                    .expect("frame pool count fits usize"),
            )
            .unwrap(),
        ),
        offloads: Offloads::default(),
    }
}

fn add_address(addr: Ipv4Addr, prefix: u8) {
    let cidr = format!("{addr}/{prefix}");
    let output = Command::new("ip")
        .args([
            "address",
            "add",
            &cidr,
            "dev",
            IFACE.to_str().expect("IFACE is ASCII"),
        ])
        .output()
        .expect("run ip address add");
    assert!(
        output.status.success(),
        "ip address add {cidr}: {}",
        String::from_utf8_lossy(&output.stderr),
    );
}

/// A Net with one socket slot on a fresh net_tap Port, Zhida's gateway the kernel side, and a
/// kernel listener on the off-subnet SERVER_ADDR:SERVER_PORT.
fn with_net<T>(
    paths: &[PathMtu],
    mss_cap: Option<u16>,
    body: impl FnOnce(&mut Net, &Clock, TcpListener) -> T,
) -> T {
    with_eal(|eal| {
        let tap = TapVdev::new(VDEV, IFACE);
        // SAFETY: net_tap was just created and this EAL thread alone configures
        // and uses its Port and packets. The offload-free net_tap profile returns
        // single-segment RX buffers and frees TX buffers synchronously. The Port
        // closes before the vdev is removed, and EAL outlives both.
        let port = unsafe { Port::open(eal, port_config(tap.port_id(), mss_cap)) }
            .expect("open net_tap Port");
        add_address(KERNEL_ADDR, 24);
        add_address(SERVER_ADDR, 32);
        let listener = TcpListener::bind(SocketAddrV4::new(SERVER_ADDR, SERVER_PORT))
            .expect("bind the off-subnet TLS server");
        let config = StackConfig {
            max_udp_sockets: 0,
            ethernet: EthernetAddress([0x02, 0, 0, 0, 0, 0x01]),
            ipv4: Ipv4Cidr::new(ZHIDA_ADDR, 24),
            gateway: Some(KERNEL_ADDR),
            paths,
            owned_ipv4: &[],
            random_seed: 1,
            neighbor_capacity: DEFAULT_NEIGHBOR_CAPACITY,
            max_sockets: 1,
            rx_staging: 32,
            tx_pending: 32,
        };
        let clock = Clock::new(Instant::now());
        let stack = Stack::new(port, config, StackInstant::ZERO)
            .map_err(|(_, error)| error)
            .expect("create Zhida Stack");
        let mut net = Net::new(
            stack,
            NetConfig {
                ingress_budget: INGRESS_BUDGET,
            },
        );
        let result = body(&mut net, &clock, listener);
        let stack = net
            .into_stack()
            .ok()
            .expect("the test dropped every handle and socket");
        stack
            .into_port()
            .try_close()
            .expect("close Port with all buffers returned");
        tap.close();
        result
    })
}

fn server_config() -> (Arc<ServerConfig>, RootCertStore) {
    let rcgen::CertifiedKey { cert, signing_key } =
        rcgen::generate_simple_self_signed(vec![HOST.to_owned()])
            .expect("generate test certificate");
    let mut roots = RootCertStore::empty();
    roots
        .add(cert.der().clone())
        .expect("add test certificate to roots");
    let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(signing_key.serialize_der()));
    let config =
        ServerConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
            .with_safe_default_protocol_versions()
            .expect("ring supports rustls's safe default protocol versions")
            .with_no_client_auth()
            .with_single_cert(vec![cert.der().clone()], key)
            .expect("the generated certificate and key match");
    (Arc::new(config), roots)
}

enum ServerPlan {
    Tickers(Vec<String>),
    Refuse451,
    /// The client rejects the certificate; the server's handshake ends at its alert.
    TlsOnly,
    /// After the upgrade the server closes: 1001 "maintenance".
    CloseAway,
    /// Accepts TCP and never answers; ends once the client's socket is gone.
    Silent,
}

#[derive(Default)]
struct ServerSaw {
    path: String,
    host: String,
    pong: bool,
    close: bool,
    error: Option<String>,
}

fn serve(listener: TcpListener, config: Arc<ServerConfig>, plan: ServerPlan) -> ServerSaw {
    listener.set_nonblocking(true).expect("nonblocking accept");
    let deadline = Instant::now()
        .checked_add(LIMIT)
        .expect("accept deadline fits wall time");
    let stream = loop {
        assert!(Instant::now() < deadline, "no client within {LIMIT:?}");
        match listener.accept() {
            Ok((stream, _)) => break stream,
            Err(error) if error.kind() == ErrorKind::WouldBlock => thread::sleep(IDLE_SLEEP),
            Err(error) if error.kind() == ErrorKind::Interrupted => {}
            Err(error) => panic!("accept: {error}"),
        }
    };
    stream.set_nonblocking(false).expect("blocking TLS socket");
    if matches!(plan, ServerPlan::Silent) {
        let mut stream = stream;
        stream.set_read_timeout(Some(LIMIT)).expect("read timeout");
        let mut rest = Vec::new();
        // the ClientHello, then the end (FIN or RST) once the client dropped its socket
        let _ = stream.read_to_end(&mut rest);
        return ServerSaw::default();
    }
    stream
        .set_read_timeout(Some(LIMIT))
        .expect("TLS read timeout");
    stream
        .set_write_timeout(Some(LIMIT))
        .expect("TLS write timeout");
    let conn = ServerConnection::new(config).expect("create TLS server connection");
    let tls = StreamOwned::new(conn, stream);
    let mut saw = ServerSaw::default();
    let handshake = tungstenite::accept_hdr(tls, |request: &Request, response: Response| {
        saw.path = request.uri().path().to_owned();
        saw.host = request
            .headers()
            .get("Host")
            .expect("client sends Host")
            .to_str()
            .expect("Host is ASCII")
            .to_owned();
        if matches!(plan, ServerPlan::Refuse451) {
            let response: ErrorResponse = Response::builder()
                .status(451)
                .body(Some(
                    "Service unavailable from a restricted location".to_owned(),
                ))
                .expect("valid HTTP refusal");
            Err(response)
        } else {
            Ok(response)
        }
    })
    .map_err(|error| error.to_string());
    let mut ws = match handshake {
        Ok(ws) => ws,
        Err(error) => {
            saw.error = Some(error);
            return saw;
        }
    };
    let texts = match plan {
        ServerPlan::Tickers(texts) => texts,
        ServerPlan::CloseAway => {
            let frame = CloseFrame { code: CloseCode::Away, reason: "maintenance".into() };
            ws.close(Some(frame)).expect("send the server's Close");
            Vec::new()
        }
        ServerPlan::Silent => unreachable!("answered above"),
        ServerPlan::Refuse451 => panic!("HTTP 451 was accepted"),
        ServerPlan::TlsOnly => panic!("the client trusted an unknown certificate"),
    };
    for (index, text) in texts.into_iter().enumerate() {
        if index == 3 {
            ws.write(Message::Ping(b"keepalive".to_vec().into()))
                .expect("send Ping");
        }
        ws.write(Message::Text(text.into()))
            .expect("send bookTicker");
    }
    ws.flush().expect("flush bookTickers and Ping");
    loop {
        match ws.read() {
            Ok(Message::Pong(payload)) if payload.as_ref() == b"keepalive" => saw.pong = true,
            Ok(Message::Close(_)) => saw.close = true,
            Ok(_) => {}
            // tungstenite returns this once it sent its reply to the client's Close, without
            // reading further; the client's close_notify and FIN are read below.
            Err(Error::ConnectionClosed) => break,
            Err(Error::Io(error)) if error.kind() == ErrorKind::Interrupted => {}
            Err(error) => panic!("server WebSocket read: {error}"),
        }
    }
    // The client's close_notify (a clean TLS end, not truncation) and then its FIN, both sent
    // before its close returned: the Net is not turned once `run` returned.
    let mut tls = ws.into_inner();
    assert_eq!(read_retrying(&mut tls).expect("clean TLS end: close_notify"), 0);
    assert_eq!(read_retrying(&mut tls.sock).expect("TCP end of stream"), 0);
    saw
}

/// One byte's read, again after EINTR (a blocking read returns it when a signal arrives).
fn read_retrying(from: &mut impl Read) -> std::io::Result<usize> {
    let mut byte = [0; 1];
    loop {
        match from.read(&mut byte) {
            Err(error) if error.kind() == ErrorKind::Interrupted => {}
            read => return read,
        }
    }
}

fn join(server: JoinHandle<ServerSaw>) -> ServerSaw {
    match server.join() {
        Ok(saw) => saw,
        Err(panic) => resume_unwind(panic),
    }
}

fn target() -> Target {
    Target {
        addr: SocketAddrV4::new(SERVER_ADDR, SERVER_PORT),
        host: HOST.to_owned(),
        path: PATH.to_owned(),
    }
}

fn closed_target() -> Target {
    Target {
        addr: SocketAddrV4::new(SERVER_ADDR, SERVER_PORT + 1),
        host: HOST.to_owned(),
        path: PATH.to_owned(),
    }
}

/// Asked each round by `run`: panics once the test ran `LIMIT`.
fn guard(clock: &Clock) {
    assert!(
        clock.ms() < LIMIT.as_millis() as u64,
        "test not done within {LIMIT:?}"
    );
}

/// A connect to a closed port on the Net's only slot: it must be refused (the slot is free).
/// With `at_once` the first attempt must get the slot; otherwise attempts retry every
/// millisecond while the slot is still taken.
async fn connect_refused(handle: &NetHandle, spawner: &Spawner, at_once: bool) {
    let target = closed_target();
    loop {
        let error = match connect(
            handle,
            spawner,
            &target,
            socket(None),
            webpki_client_config(),
            LOCAL_PORT + 1,
            CONNECT_LIMIT,
        )
        .await
        {
            Ok(_) => panic!("a closed port opened a feed"),
            Err(error) => error,
        };
        match error {
            FeedError::Closed {
                addr,
                reset_by_peer: true,
            } if addr == target.addr => return,
            FeedError::Open(OpenError::Add(AddTcpError::SocketsFull(_))) if !at_once => {
                spawner
                    .sleep(1)
                    .expect("open Runtime")
                    .await
                    .expect("sleep");
            }
            other => panic!("connect to a closed port: {other:?}"),
        }
    }
}

#[zhida_test_support::pinned_test]
fn refused_connect_releases_the_only_slot() {
    with_net(&[], None, |net, clock, _listener| {
        run(
            net,
            clock,
            Some(IDLE_SLEEP),
            || {
                guard(clock);
                false
            },
            |handle, spawner, _stop| async move {
                connect_refused(&handle, &spawner, true).await;
                connect_refused(&handle, &spawner, true).await;
            },
        )
        .expect("run the client");
    });
}

#[zhida_test_support::pinned_test]
fn timed_out_connect_releases_the_only_slot() {
    with_net(&[], None, |net, clock, _listener| {
        run(
            net,
            clock,
            Some(IDLE_SLEEP),
            || {
                guard(clock);
                false
            },
            |handle, spawner, _stop| async move {
                // 192.0.2.3 is unassigned in this TAP fixture; connect waits for ARP.
                let target = Target {
                    addr: SocketAddrV4::new(Ipv4Addr::new(192, 0, 2, 3), SERVER_PORT),
                    host: HOST.to_owned(),
                    path: PATH.to_owned(),
                };
                let limit = Duration::from_millis(100);
                let started = Instant::now();
                let error = match connect(
                    &handle,
                    &spawner,
                    &target,
                    socket(None),
                    webpki_client_config(),
                    LOCAL_PORT,
                    limit,
                )
                .await
                {
                    Ok(_) => panic!("an unanswered ARP opened a feed"),
                    Err(error) => error,
                };
                assert!(
                    matches!(error, FeedError::Timeout { limit: actual, .. } if actual == limit),
                    "{error:?}",
                );
                // Runtime ticks are whole milliseconds of `clock`, and `started` is read within
                // the tick the sleep counts from: up to 1 ms of the limit may precede it.
                assert!(
                    started.elapsed() + Duration::from_millis(1) >= limit,
                    "timed out after {:?}",
                    started.elapsed()
                );
                // A dropped SYN-SENT socket closes without sending anything (Linux
                // tcp_close), so the very next connect gets the slot.
                connect_refused(&handle, &spawner, true).await;
            },
        )
        .expect("run the client");
    });
}

/// A ticker as text, to compare what `Feed::next` lends with an expected one.
fn debug(ticker: &BookTicker<'_>) -> String {
    format!("{ticker:?}")
}

fn tickers() -> (Vec<String>, Vec<BookTicker<'static>>) {
    let mut texts = Vec::new();
    let mut expected = Vec::new();
    for index in 0..12u64 {
        let update_id = 400900217 + index;
        let bid_price = format!("{}.29000000", 80680 + index);
        let ask_price = format!("{}.30000000", 80680 + index);
        let bid_qty = "3.06547000";
        let ask_qty = "0.85179000";
        texts.push(format!(
            "{{\"u\":{update_id},\"s\":\"BTCUSDT\",\"b\":\"{bid_price}\",\"B\":\"{bid_qty}\",\
             \"a\":\"{ask_price}\",\"A\":\"{ask_qty}\"}}",
        ));
        expected.push(BookTicker {
            update_id,
            symbol: "BTCUSDT".into(),
            bid_price: bid_price.into(),
            bid_qty: bid_qty.into(),
            ask_price: ask_price.into(),
            ask_qty: ask_qty.into(),
        });
    }
    (texts, expected)
}

/// 12 updates with a Ping after the third, through a 1400-byte path MTU and a capped MSS. Once
/// all were read, `run`'s stop ends `next`; then close.
#[zhida_test_support::pinned_test]
fn feed_reads_tickers_answers_ping_and_closes() {
    let paths = [PathMtu {
        prefix: Ipv4Cidr::new(SERVER_ADDR, 32),
        mtu: 1400,
    }];
    with_net(&paths, Some(1360), |net, clock, listener| {
        let (server_tls, roots) = server_config();
        let (texts, expected) = tickers();
        let server = thread::spawn(move || serve(listener, server_tls, ServerPlan::Tickers(texts)));
        let read_all = Rc::new(Cell::new(false));
        let client_read_all = read_all.clone();
        let should_stop = || {
            guard(clock);
            read_all.get()
        };
        run(
            net,
            clock,
            Some(IDLE_SLEEP),
            should_stop,
            |handle, spawner, mut stop| async move {
                let tls = client_config(roots);
                let mut feed = connect(
                    &handle,
                    &spawner,
                    &target(),
                    socket(Some(1360)),
                    tls,
                    LOCAL_PORT,
                    CONNECT_LIMIT,
                )
                .await
                .expect("connect Zhida TLS WebSocket client");
                for ticker in expected {
                    assert_eq!(
                        feed.next(&mut stop, debug).await.expect("read bookTicker"),
                        Some(debug(&ticker))
                    );
                }
                client_read_all.set(true);
                assert_eq!(feed.next(&mut stop, debug).await.expect("stop"), None);
                feed.close(&spawner, CLOSE_LIMIT)
                    .await
                    .expect("close Zhida feed");
            },
        )
        .expect("run the client");
        let saw = join(server);
        assert_eq!(saw.path, PATH);
        assert_eq!(saw.host, "binance.test:9443");
        assert!(saw.pong, "server did not receive Pong(keepalive)");
        assert!(saw.close, "server did not receive the client's Close");
        assert!(saw.error.is_none(), "server failed: {:?}", saw.error);
    });
}

/// `stream` after a failed read (a text that is not a bookTicker, after two that are) still
/// closes: the server gets the Close, then close_notify and FIN (checked by `serve`), and the
/// read error is what `stream` returns.
#[zhida_test_support::pinned_test]
fn stream_closes_after_a_read_error() {
    with_net(&[], None, |net, clock, listener| {
        let (server_tls, roots) = server_config();
        let (mut texts, expected) = tickers();
        texts.truncate(2);
        texts.push("not a bookTicker".to_owned());
        let server = thread::spawn(move || serve(listener, server_tls, ServerPlan::Tickers(texts)));
        let should_stop = || {
            guard(clock);
            false
        };
        let events = run(net, clock, Some(IDLE_SLEEP), should_stop, |handle, spawner, stop| async move {
            let mut events = Vec::new();
            let result = stream(
                &handle,
                &spawner,
                &target(),
                socket(None),
                client_config(roots),
                LOCAL_PORT,
                CONNECT_LIMIT,
                CLOSE_LIMIT,
                stop,
                |event| events.push(format!("{event:?}")),
            )
            .await;
            match result {
                Err(FeedError::Parse { text, .. }) => assert_eq!(text, "not a bookTicker"),
                other => panic!("expected the parse error, got {other:?}"),
            }
            events
        })
        .expect("run the client");
        let saw = join(server);
        assert!(saw.close, "server did not receive the client's Close");
        assert!(saw.error.is_none(), "server failed: {:?}", saw.error);
        let summary = Summary {
            count: 2,
            update_ids: Some((expected[0].update_id, expected[1].update_id)),
        };
        assert_eq!(
            events,
            [
                "Connected".to_owned(),
                format!("{:?}", Event::Ticker { count: 1, ticker: &expected[0] }),
                format!("{:?}", Event::Ticker { count: 2, ticker: &expected[1] }),
                format!("{:?}", Event::Read(summary)),
                "Closed".to_owned(),
            ]
        );
    });
}

/// A finished stop does not hide an update already received: `next` returns it first.
#[zhida_test_support::pinned_test]
fn received_update_comes_before_a_finished_stop() {
    with_net(&[], None, |net, clock, listener| {
        let (server_tls, roots) = server_config();
        let (texts, expected) = tickers();
        let server = thread::spawn(move || serve(listener, server_tls, ServerPlan::Tickers(texts)));
        run(
            net,
            clock,
            Some(IDLE_SLEEP),
            || {
                guard(clock);
                false
            },
            |handle, spawner, _stop| async move {
                let tls = client_config(roots);
                let mut feed = connect(
                    &handle,
                    &spawner,
                    &target(),
                    socket(None),
                    tls,
                    LOCAL_PORT,
                    CONNECT_LIMIT,
                )
                .await
                .expect("connect Zhida TLS WebSocket client");
                let first = feed
                    .next(std::future::pending::<()>(), debug)
                    .await
                    .expect("read bookTicker");
                assert_eq!(first, Some(debug(&expected[0])));
                // The server wrote all 12 in one flush; wait until the last is surely here.
                spawner
                    .sleep(200)
                    .expect("open Runtime")
                    .await
                    .expect("sleep");
                for ticker in &expected[1..] {
                    let next = feed
                        .next(std::future::ready(()), debug)
                        .await
                        .expect("read bookTicker");
                    assert_eq!(next, Some(debug(ticker)));
                }
                assert_eq!(feed.next(std::future::ready(()), debug).await.expect("stop"), None);
                feed.close(&spawner, CLOSE_LIMIT)
                    .await
                    .expect("close Zhida feed");
            },
        )
        .expect("run the client");
        let saw = join(server);
        assert!(
            saw.close && saw.error.is_none(),
            "server close {} error {:?}",
            saw.close,
            saw.error
        );
    });
}

/// The server closes: `next` reports its status and reason, after relay echoed the close and
/// sent close_notify and FIN (checked by `serve`, the Net not turned once `run` returned).
#[zhida_test_support::pinned_test]
fn server_close_reports_its_status_and_reason() {
    with_net(&[], None, |net, clock, listener| {
        let (server_tls, roots) = server_config();
        let server = thread::spawn(move || serve(listener, server_tls, ServerPlan::CloseAway));
        let should_stop = || {
            guard(clock);
            false
        };
        run(net, clock, Some(IDLE_SLEEP), should_stop, |handle, spawner, mut stop| async move {
            let target = target();
            let connecting = connect(&handle, &spawner, &target, socket(None), client_config(roots), LOCAL_PORT, CONNECT_LIMIT);
            let mut feed = connecting.await.expect("connect Zhida TLS WebSocket client");
            match feed.next(&mut stop, |_| ()).await {
                Err(FeedError::ServerClosed { code: 1001, reason }) => assert_eq!(reason, "maintenance"),
                other => panic!("expected the server's close, got {other:?}"),
            }
        })
        .expect("run the client");
        let saw = join(server);
        assert!(saw.close, "server did not receive the echo");
        assert!(saw.error.is_none(), "server failed: {:?}", saw.error);
    });
}

/// A stop while `connect` waits (the server never answers the ClientHello) ends it at once,
/// before `CONNECT_LIMIT`, and drops its socket: the server reads the end of the stream (the
/// run stops once the server returned).
#[zhida_test_support::pinned_test]
fn stop_during_connect_ends_it_and_drops_the_socket() {
    with_net(&[], None, |net, clock, listener| {
        let (server_tls, roots) = server_config();
        let server = thread::spawn(move || serve(listener, server_tls, ServerPlan::Silent));
        let should_stop = || {
            guard(clock);
            server.is_finished()
        };
        run(net, clock, Some(IDLE_SLEEP), should_stop, |handle, spawner, stop| async move {
            let target = target();
            let connecting = connect(&handle, &spawner, &target, socket(None), client_config(roots), LOCAL_PORT, CONNECT_LIMIT);
            let early = spawner.sleep(200).expect("open Runtime");
            let started = Instant::now();
            assert!(first(connecting, early).await.is_none(), "connect finished against a silent server");
            assert!(started.elapsed() < CONNECT_LIMIT / 2, "connect outlived the stop");
            let _ = stop.await;
        })
        .expect("run the client");
        join(server);
    });
}

#[zhida_test_support::pinned_test]
fn upgrade_refused_with_http_451_reports_the_status() {
    with_net(&[], None, |net, clock, listener| {
        let (server_tls, roots) = server_config();
        let server = thread::spawn(move || serve(listener, server_tls, ServerPlan::Refuse451));
        let should_stop = || {
            guard(clock);
            server.is_finished()
        };
        run(
            net,
            clock,
            Some(IDLE_SLEEP),
            should_stop,
            |handle, spawner, stop| async move {
                let error = match connect(
                    &handle,
                    &spawner,
                    &target(),
                    socket(None),
                    client_config(roots),
                    LOCAL_PORT,
                    CONNECT_LIMIT,
                )
                .await
                {
                    Ok(_) => panic!("HTTP 451 opened a WebSocket"),
                    Err(error) => error,
                };
                match &error {
                    FeedError::Ws(WsError::Upgrade(UpgradeError::Status(Some(451)))) => {}
                    other => panic!("expected the HTTP 451 refusal, got {other:?}"),
                }
                assert!(error.to_string().contains("451"), "{error}");
                connect_refused(&handle, &spawner, false).await;
                let _ = stop.await;
            },
        )
        .expect("run the client");
        let saw = join(server);
        assert!(saw.error.is_some(), "server accepted the upgrade");
    });
}

#[zhida_test_support::pinned_test]
fn unknown_certificate_fails_tls_and_alerts_the_server() {
    with_net(&[], None, |net, clock, listener| {
        let (server_tls, _) = server_config();
        let server = thread::spawn(move || serve(listener, server_tls, ServerPlan::TlsOnly));
        let should_stop = || {
            guard(clock);
            server.is_finished()
        };
        run(
            net,
            clock,
            Some(IDLE_SLEEP),
            should_stop,
            |handle, spawner, stop| async move {
                let error = match connect(
                    &handle,
                    &spawner,
                    &target(),
                    socket(None),
                    webpki_client_config(),
                    LOCAL_PORT,
                    CONNECT_LIMIT,
                )
                .await
                {
                    Ok(_) => panic!("Mozilla roots trusted the self-signed certificate"),
                    Err(error) => error,
                };
                assert!(
                    matches!(
                        error,
                        FeedError::Ws(WsError::Tls(sheath::Error::Tls(
                            rustls::Error::InvalidCertificate(
                                rustls::CertificateError::UnknownIssuer
                            )
                        )))
                    ),
                    "expected UnknownIssuer, got {error:?}",
                );
                connect_refused(&handle, &spawner, false).await;
                let _ = stop.await;
            },
        )
        .expect("run the client");
        let saw = join(server);
        let error = saw.error.expect("server TLS handshake did not fail");
        assert!(
            error.contains("UnknownCA"),
            "server did not receive the client's alert: {error}"
        );
    });
}

/// Fields borrow from the message text; a JSON-escaped field (Binance does not escape, but the
/// text may) still parses, into an owned string.
#[test]
fn book_ticker_borrows_the_text_and_owns_only_escaped_fields() {
    let text = r#"{"u":7,"s":"BTC\u0055SDT","b":"1.5","B":"2","a":"1.6","A":"3"}"#;
    let ticker = serde_json::from_str::<BookTicker>(text).expect("parse an escaped symbol");
    assert_eq!(ticker.symbol, "BTCUSDT");
    assert!(matches!(ticker.symbol, Cow::Owned(_)), "escaped symbol must be owned");
    for field in [&ticker.bid_price, &ticker.bid_qty, &ticker.ask_price, &ticker.ask_qty] {
        let Cow::Borrowed(field) = field else { panic!("{field} copied, not borrowed") };
        assert!(text.as_bytes().as_ptr_range().contains(&field.as_ptr()), "{field} not in the text");
    }
}

#[test]
fn book_ticker_parses_binance_shape_and_ignores_extra_fields() {
    let text = r#"{
        "u":400900217, "s":"BTCUSDT",
        "b":"80680.29000000", "B":"3.06547000",
        "a":"80680.30000000", "A":"0.85179000", "E":123
    }"#;
    let ticker = serde_json::from_str::<BookTicker>(text).expect("parse Binance bookTicker shape");
    assert_eq!(
        ticker,
        BookTicker {
            update_id: 400900217,
            symbol: "BTCUSDT".into(),
            bid_price: "80680.29000000".into(),
            bid_qty: "3.06547000".into(),
            ask_price: "80680.30000000".into(),
            ask_qty: "0.85179000".into(),
        },
    );
    assert_eq!(
        ticker.to_string(),
        "u=400900217 BTCUSDT bid 80680.29000000 x 3.06547000 ask 80680.30000000 x 0.85179000",
    );
}
