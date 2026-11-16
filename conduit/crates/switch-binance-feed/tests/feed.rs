//! The binance_feed client (switch-ws's wss client over switch-tls, as a switch task on a switch-net Net)
//! against a kernel TCP TLS WebSocket server (rustls + tungstenite) across net_tap through
//! Switch's IPv4 gateway, as in the live run. These tests need CAP_NET_ADMIN and /dev/net/tun,
//! and no internet.

use std::cell::Cell;
use std::ffi::CStr;
use std::io::{ErrorKind, Read};
use std::net::{Ipv4Addr, SocketAddrV4, TcpListener};
use std::panic::resume_unwind;
use std::process::Command;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use switch_binance_feed::{
    client_config, connect, feed, socket, webpki_client_config, BookTicker, Clock, FeedError,
    Latencies, Outcome, Summary, Target, INGRESS_BUDGET,
};
use dpdk_smoltcp::smoltcp::iface::PathMtu;
use dpdk_smoltcp::smoltcp::socket::tcp;
use dpdk_smoltcp::smoltcp::time::Instant as StackInstant;
use dpdk_smoltcp::smoltcp::wire::{EthernetAddress, Ipv4Cidr};
use dpdk_smoltcp::{
    frame_reserve, rx_reserve, Offloads, Port, PortConfig, Stack,
    StackConfig, DEFAULT_NEIGHBOR_CAPACITY,
};
use switch_ws::UpgradeError;
use switch_ws::switch_net::Error as WsError;
use rustls::pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer};
use rustls::{RootCertStore, ServerConfig, ServerConnection, StreamOwned};
use switch_runtime::{oneshot, Runtime, Spawner};
use switch_net::{Error as NetError, Net, NetConfig, NetHandle};
use tungstenite::handshake::server::{ErrorResponse, Request, Response};
use tungstenite::protocol::CloseFrame;
use tungstenite::protocol::frame::coding::CloseCode;
use tungstenite::{Error, Message};
use switch_buffer::PoolConfig;
use switch_dpdk_sys::RTE_PKTMBUF_HEADROOM;
use switch_test_kit::{tap::TapVdev, with_eal};
use switch_test_kit::ring_port::RingPort;

const VDEV: &CStr = c"net_tap_bnc";
const IFACE: &CStr = c"cdtap_bnc";
const RX_POOL: &CStr = c"bnc_rx";
const TX_POOL: &CStr = c"bnc_tx";
const FRAME_POOL: &CStr = c"bnc_frame";
const SWITCH_ADDR: Ipv4Addr = Ipv4Addr::new(192, 0, 2, 2);
const KERNEL_ADDR: Ipv4Addr = Ipv4Addr::new(192, 0, 2, 1);
const SERVER_ADDR: Ipv4Addr = Ipv4Addr::new(198, 51, 100, 10);
const SERVER_PORT: u16 = 9443;
const HOST: &str = "binance.test";
const PATH: &str = "/ws/btcusdt@bookTicker";
const LOCAL_PORT: u16 = 49152;
const CONNECT_LIMIT: Duration = Duration::from_secs(20);
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
        rx_pool: pool(RX_POOL, 8191),
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

/// A Net with one socket slot on a fresh net_tap Port, switch's gateway the kernel side, and a
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
            ipv4: Ipv4Cidr::new(SWITCH_ADDR, 24),
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
            .expect("create switch Stack");
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

#[switch_test_support::pinned_test]
fn feed_sized_turn_imports_and_processes_one_hundred_packets() {
    use dpdk_smoltcp::smoltcp::phy::ChecksumCapabilities;
    use dpdk_smoltcp::smoltcp::wire::{
        EthernetFrame, EthernetProtocol, EthernetRepr, IpProtocol, Ipv4Address, Ipv4Packet,
        Ipv4Repr, UdpPacket, UdpRepr,
    };
    use dpdk_smoltcp::UdpConfig;

    with_eal(|eal| {
        let fixture = RingPort::new("feedburst", 128, 16);
        let mut port_config = fixture.config();
        port_config.rx_burst = INGRESS_BUDGET as u16;
        port_config.rx_pool.count = u32::try_from(rx_reserve(
            port_config.rx_descriptors,
            port_config.rx_burst,
            INGRESS_BUDGET,
            port_config.rx_pool.cache_size,
        ) + 100).unwrap();
        let port = fixture.open_with(eal, port_config);
        let config = StackConfig {
            max_udp_sockets: 1,
            ethernet: EthernetAddress([0x02, 0, 0, 0, 0, 1]),
            ipv4: Ipv4Cidr::new(Ipv4Address::new(192, 0, 2, 2), 24),
            gateway: None,
            paths: &[],
            owned_ipv4: &[],
            random_seed: 1,
            neighbor_capacity: DEFAULT_NEIGHBOR_CAPACITY,
            max_sockets: 1,
            rx_staging: INGRESS_BUDGET,
            tx_pending: 32,
        };
        let mut stack = Stack::new(port, config, StackInstant::ZERO)
            .map_err(|(_, error)| error)
            .unwrap();
        let socket = stack.add_udp(UdpConfig {
            port: 9000, local_addr: None, rx_queue: 128, tx_checksum: true, broadcast: false,
        }).unwrap();
        let src = Ipv4Address::new(192, 0, 2, 3);
        let dst = Ipv4Address::new(192, 0, 2, 2);
        let eth = EthernetRepr {
            src_addr: EthernetAddress([0x02, 0, 0, 0, 0, 3]),
            dst_addr: EthernetAddress([0x02, 0, 0, 0, 0, 1]),
            ethertype: EthernetProtocol::Ipv4,
        };
        let udp = UdpRepr { src_port: 9001, dst_port: 9000 };
        let ip = Ipv4Repr { src_addr: src, dst_addr: dst, next_header: IpProtocol::Udp, payload_len: udp.header_len() + 1, hop_limit: 64 };
        let caps = ChecksumCapabilities::default();
        let packets: Vec<_> = (0..100u8).map(|i| {
            let mut bytes = vec![0; eth.buffer_len() + ip.buffer_len() + ip.payload_len];
            let mut frame = EthernetFrame::new_unchecked(&mut bytes);
            eth.emit(&mut frame);
            let mut packet = Ipv4Packet::new_unchecked(frame.payload_mut());
            ip.emit(&mut packet, &caps);
            udp.emit(&mut UdpPacket::new_unchecked(packet.payload_mut()), &src.into(), &dst.into(), 1, |b| b[0] = i, &caps);
            bytes
        }).collect();
        fixture.inject(&packets);
        let turn = stack.turn(StackInstant::ZERO, INGRESS_BUDGET).unwrap();
        assert_eq!((turn.rx_received, turn.rx_rejected, turn.rx_processed, turn.rx_staged), (100, 0, 100, 0));
        for i in 0..100u8 {
            assert_eq!(stack.udp_mut(&socket).recv().unwrap().payload(), &[i]);
        }
        assert!(fixture.take_tx().is_empty());
        stack.into_port().try_close().unwrap();
        fixture.close();
    });
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
    /// The texts, a Ping after the third; the flag is set once all were flushed.
    Tickers(Vec<String>, Arc<AtomicBool>),
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
    let (texts, flushed) = match plan {
        ServerPlan::Tickers(texts, flushed) => (texts, Some(flushed)),
        ServerPlan::CloseAway => {
            let frame = CloseFrame { code: CloseCode::Away, reason: "maintenance".into() };
            ws.close(Some(frame)).expect("send the server's Close");
            (Vec::new(), None)
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
    if let Some(flushed) = flushed {
        flushed.store(true, Ordering::Relaxed);
    }
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

/// Turns `net` and polls `rt` until `done()`, sleeping IDLE_SLEEP after a round that moved
/// nothing; panics once the test ran LIMIT.
fn turn_until(net: &mut Net, clock: &Clock, rt: &mut Runtime, mut done: impl FnMut() -> bool) {
    while !done() {
        assert!(clock.ms() < LIMIT.as_millis() as u64, "test not done within {LIMIT:?}");
        let (now, tick) = clock.sample();
        if !switch_net::round(net, rt, now, tick).expect("round") {
            thread::sleep(IDLE_SLEEP);
        }
    }
}

/// Spawns the feed task on `target()`, reading until its stop (the returned sender; dropping it
/// stops the feed too).
fn spawn_feed(net: &Net, rt: &Runtime, roots: RootCertStore, mss_cap: Option<u16>) -> (oneshot::Sender<()>, Outcome) {
    spawn_feed_with_reset_check(net, rt, roots, mss_cap, || false)
}

fn spawn_feed_with_reset_check(
    net: &Net,
    rt: &Runtime,
    roots: RootCertStore,
    mss_cap: Option<u16>,
    clock_reset: impl FnOnce() -> bool + 'static,
) -> (oneshot::Sender<()>, Outcome) {
    let (stop, stop_rx) = oneshot::channel();
    let out = Outcome::default();
    let task = feed(
        net.handle(),
        rt.spawner(),
        target(),
        socket(mss_cap),
        client_config(roots),
        LOCAL_PORT,
        "test",
        usize::MAX,
        None,
        stop_rx,
        out.clone(),
        clock_reset,
    );
    rt.spawner().spawn(task).expect("spawn the feed");
    (stop, out)
}

/// Turns until the feed task finished; what it read and its result.
fn finish(net: &mut Net, clock: &Clock, rt: &mut Runtime, out: &Outcome) -> (Summary, Result<(), FeedError>) {
    let mut outcome = None;
    turn_until(net, clock, rt, || {
        outcome = out.take();
        outcome.is_some()
    });
    outcome.expect("the loop ends once the feed finished")
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
            FeedError::Open(NetError::SocketsFull) if !at_once => {
                spawner
                    .sleep(1_000_000)
                    .expect("open Runtime")
                    .await
                    .expect("sleep");
            }
            other => panic!("connect to a closed port: {other:?}"),
        }
    }
}

#[switch_runtime::task]
async fn refused_twice(handle: NetHandle, spawner: Spawner, done: Rc<Cell<bool>>) {
    connect_refused(&handle, &spawner, true).await;
    connect_refused(&handle, &spawner, true).await;
    done.set(true);
}

#[switch_test_support::pinned_test]
fn refused_connect_releases_the_only_slot() {
    with_net(&[], None, |net, clock, _listener| {
        let mut rt = Runtime::new(clock.ns());
        let done = Rc::new(Cell::new(false));
        rt.spawner().spawn(refused_twice(net.handle(), rt.spawner(), done.clone())).expect("spawn the client");
        turn_until(net, clock, &mut rt, || done.get());
    });
}

#[switch_runtime::task]
async fn timed_out_then_refused(handle: NetHandle, spawner: Spawner, done: Rc<Cell<bool>>) {
    // 192.0.2.3 is unassigned in this TAP fixture; connect waits for ARP.
    let target = Target {
        addr: SocketAddrV4::new(Ipv4Addr::new(192, 0, 2, 3), SERVER_PORT),
        host: HOST.to_owned(),
        path: PATH.to_owned(),
    };
    let limit = Duration::from_millis(100);
    let started = Instant::now();
    let error = match connect(&handle, &spawner, &target, socket(None), webpki_client_config(), LOCAL_PORT, limit).await {
        Ok(_) => panic!("an unanswered ARP opened a feed"),
        Err(error) => error,
    };
    assert!(
        matches!(error, FeedError::Timeout { limit: actual, .. } if actual == limit),
        "{error:?}",
    );
    // The Runtime tick is sampled before this task runs, so `started` can be later.
    // Allow 1 ms for that gap.
    assert!(
        started.elapsed() + Duration::from_millis(1) >= limit,
        "timed out after {:?}",
        started.elapsed()
    );
    // A dropped SYN-SENT socket closes without sending anything (Linux
    // tcp_close), so the very next connect gets the slot.
    connect_refused(&handle, &spawner, true).await;
    done.set(true);
}

#[switch_test_support::pinned_test]
fn timed_out_connect_releases_the_only_slot() {
    with_net(&[], None, |net, clock, _listener| {
        let mut rt = Runtime::new(clock.ns());
        let done = Rc::new(Cell::new(false));
        let task = timed_out_then_refused(net.handle(), rt.spawner(), done.clone());
        rt.spawner().spawn(task).expect("spawn the client");
        turn_until(net, clock, &mut rt, || done.get());
    });
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

/// What the feed read of all 12 `tickers()` on net_tap, which has no RX timestamps.
fn read_all_twelve() -> Summary {
    let (_, expected) = tickers();
    Summary {
        count: 12,
        update_ids: Some((expected[0].update_id, expected[11].update_id)),
        latencies: Latencies { no_stamp: 12, ..Latencies::default() },
    }
}

/// 12 updates with a Ping after the third, through a 1400-byte path MTU and a capped MSS. 200 ms
/// after the server sent them all, the stop ends the waiting read; then the close.
#[switch_test_support::pinned_test]
fn feed_reads_tickers_answers_ping_and_closes() {
    let paths = [PathMtu {
        prefix: Ipv4Cidr::new(SERVER_ADDR, 32),
        mtu: 1400,
    }];
    with_net(&paths, Some(1360), |net, clock, listener| {
        let (server_tls, roots) = server_config();
        let (texts, _) = tickers();
        let flushed = Arc::new(AtomicBool::new(false));
        let plan = ServerPlan::Tickers(texts, flushed.clone());
        let server = thread::spawn(move || serve(listener, server_tls, plan));
        let mut rt = Runtime::new(clock.ns());
        let (stop, out) = spawn_feed(net, &rt, roots, Some(1360));
        turn_until(net, clock, &mut rt, || flushed.load(Ordering::Relaxed) || server.is_finished());
        let sent = clock.ms();
        turn_until(net, clock, &mut rt, || clock.ms() >= sent + 200);
        let _ = stop.send(());
        let (summary, result) = finish(net, clock, &mut rt, &out);
        result.expect("feed reads, then closes");
        assert_eq!(summary, read_all_twelve());
        let saw = join(server);
        assert_eq!(saw.path, PATH);
        assert_eq!(saw.host, "binance.test:9443");
        assert!(saw.pong, "server did not receive Pong(keepalive)");
        assert!(saw.close, "server did not receive the client's Close");
        assert!(saw.error.is_none(), "server failed: {:?}", saw.error);
    });
}

/// Run the actual feed in a child so the assertion checks its emitted latency line, not just
/// a formatting helper. The reset is synchronous; no clock thread or timing race is needed.
#[switch_test_support::pinned_test]
fn reset_after_last_clock_poll_suppresses_feed_histogram() {
    const CHILD: &str = "BINANCE_FEED_RESET_REPORT_CHILD";
    if std::env::var_os(CHILD).is_none() {
        let output = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact", "reset_after_last_clock_poll_suppresses_feed_histogram",
                "--nocapture", "--test-threads=1",
            ])
            .env(CHILD, "1")
            .output()
            .expect("run the feed reset regression");
        let stdout = String::from_utf8(output.stdout).unwrap();
        assert!(output.status.success(), "{stdout}\n{}", String::from_utf8_lossy(&output.stderr));
        let lines: Vec<_> = stdout.lines()
            .filter(|line| line.starts_with("test: feed_latency:"))
            .collect();
        assert_eq!(lines, ["test: feed_latency: latency invalid: NIC clock jumped"], "{stdout}");
        return;
    }

    with_net(&[], None, |net, clock, listener| {
        let (server_tls, roots) = server_config();
        let (texts, _) = tickers();
        let flushed = Arc::new(AtomicBool::new(false));
        let plan = ServerPlan::Tickers(texts, flushed.clone());
        let server = thread::spawn(move || serve(listener, server_tls, plan));
        let resets = dpdk_smoltcp::port_resets();
        let mut rt = Runtime::new(clock.ns());
        let (stop, out) = spawn_feed_with_reset_check(
            net, &rt, roots, None,
            move || dpdk_smoltcp::port_resets() != resets,
        );
        // Give the feed its messages, but leave the read pending until the explicit stop.
        turn_until(net, clock, &mut rt, || flushed.load(Ordering::Relaxed) || server.is_finished());
        let sent = clock.ms();
        turn_until(net, clock, &mut rt, || clock.ms() >= sent + 200);
        assert!(out.take().is_none(), "the feed must not have reported yet");
        assert_eq!(dpdk_smoltcp::port_resets(), resets, "last clock poll sees no reset");

        with_eal(|eal| {
            let mut pair = switch_test_kit::pair::new_pair(eal, ["bncra", "bncrb"]);
            let port = dpdk_smoltcp::port_by_name(eal, "net_ring_bncrb").unwrap();
            assert_eq!(unsafe { switch_dpdk_sys::rte_eth_dev_stop(port) }, 0);
            pair.b.raise_reset();
            let now = pair.now();
            pair.b.turn(now, INGRESS_BUDGET).expect("recover the reset port");
            assert_eq!(dpdk_smoltcp::port_resets(), resets + 1);
            pair.close();
        });

        let _ = stop.send(());
        let (summary, result) = finish(net, clock, &mut rt, &out);
        result.expect("feed reads and closes after reset");
        assert_eq!(summary, read_all_twelve());
        let saw = join(server);
        assert!(saw.close && saw.error.is_none(), "server close {} error {:?}", saw.close, saw.error);
    });
}

/// A finished stop does not hide updates already received: the Net is turned without running the
/// feed until all 12 are surely here, then the stop is sent; the feed reads all 12 first.
#[switch_test_support::pinned_test]
fn received_updates_come_before_a_finished_stop() {
    with_net(&[], None, |net, clock, listener| {
        let (server_tls, roots) = server_config();
        let (texts, _) = tickers();
        let flushed = Arc::new(AtomicBool::new(false));
        let plan = ServerPlan::Tickers(texts, flushed.clone());
        let server = thread::spawn(move || serve(listener, server_tls, plan));
        let mut rt = Runtime::new(clock.ns());
        let (stop, out) = spawn_feed(net, &rt, roots, None);
        turn_until(net, clock, &mut rt, || flushed.load(Ordering::Relaxed) || server.is_finished());
        let sent = clock.ms();
        while clock.ms() < sent + 200 {
            net.receive(clock.now()).expect("receive");
            net.send().expect("send");
            thread::sleep(IDLE_SLEEP);
        }
        let _ = stop.send(());
        let (summary, result) = finish(net, clock, &mut rt, &out);
        result.expect("feed reads, then closes");
        assert_eq!(summary, read_all_twelve());
        let saw = join(server);
        assert!(saw.close && saw.error.is_none(), "server close {} error {:?}", saw.close, saw.error);
    });
}

/// After a failed read (a text that is not a bookTicker, after two that are) the feed still
/// closes: the server gets the Close, then close_notify and FIN (checked by `serve`), and the
/// read error is the feed's result.
#[switch_test_support::pinned_test]
fn feed_closes_after_a_read_error() {
    with_net(&[], None, |net, clock, listener| {
        let (server_tls, roots) = server_config();
        let (mut texts, expected) = tickers();
        texts.truncate(2);
        texts.push("not a bookTicker".to_owned());
        let plan = ServerPlan::Tickers(texts, Arc::default());
        let server = thread::spawn(move || serve(listener, server_tls, plan));
        let mut rt = Runtime::new(clock.ns());
        let (_stop, out) = spawn_feed(net, &rt, roots, None);
        let (summary, result) = finish(net, clock, &mut rt, &out);
        match result {
            Err(FeedError::Parse { text, .. }) => assert_eq!(text, "not a bookTicker"),
            other => panic!("expected the parse error, got {other:?}"),
        }
        // Every message, the unparsable one too, is unstamped.
        let read = Summary {
            count: 2,
            update_ids: Some((expected[0].update_id, expected[1].update_id)),
            latencies: Latencies { no_stamp: 3, ..Latencies::default() },
        };
        assert_eq!(summary, read);
        let saw = join(server);
        assert!(saw.close, "server did not receive the client's Close");
        assert!(saw.error.is_none(), "server failed: {:?}", saw.error);
    });
}

/// The server closes: the feed reports its status and reason, after switch-ws echoed the close and
/// sent close_notify and FIN (checked by `serve`, the Net not turned once the feed finished).
#[switch_test_support::pinned_test]
fn server_close_reports_its_status_and_reason() {
    with_net(&[], None, |net, clock, listener| {
        let (server_tls, roots) = server_config();
        let server = thread::spawn(move || serve(listener, server_tls, ServerPlan::CloseAway));
        let mut rt = Runtime::new(clock.ns());
        let (_stop, out) = spawn_feed(net, &rt, roots, None);
        let (summary, result) = finish(net, clock, &mut rt, &out);
        match result {
            Err(FeedError::ServerClosed { code: 1001, reason }) => assert_eq!(reason, "maintenance"),
            other => panic!("expected the server's close, got {other:?}"),
        }
        assert_eq!(summary.count, 0);
        let saw = join(server);
        assert!(saw.close, "server did not receive the echo");
        assert!(saw.error.is_none(), "server failed: {:?}", saw.error);
    });
}

/// A stop while the connect waits (the server never answers the ClientHello) ends it at once,
/// before CONNECT_LIMIT, without an error, and drops its socket: the server reads the end of the
/// stream.
#[switch_test_support::pinned_test]
fn stop_during_connect_ends_it_and_drops_the_socket() {
    with_net(&[], None, |net, clock, listener| {
        let (server_tls, roots) = server_config();
        let server = thread::spawn(move || serve(listener, server_tls, ServerPlan::Silent));
        let mut rt = Runtime::new(clock.ns());
        let started = Instant::now();
        let (stop, out) = spawn_feed(net, &rt, roots, None);
        turn_until(net, clock, &mut rt, || started.elapsed() >= Duration::from_millis(200));
        let _ = stop.send(());
        let (summary, result) = finish(net, clock, &mut rt, &out);
        assert!(started.elapsed() < CONNECT_LIMIT / 2, "connect outlived the stop");
        result.expect("a stop during the connect is no error");
        assert_eq!(summary, Summary::default());
        turn_until(net, clock, &mut rt, || server.is_finished());
        join(server);
    });
}

#[switch_runtime::task]
async fn refused_451_then_slot_free(handle: NetHandle, spawner: Spawner, roots: RootCertStore, done: Rc<Cell<bool>>) {
    let error = match connect(&handle, &spawner, &target(), socket(None), client_config(roots), LOCAL_PORT, CONNECT_LIMIT).await {
        Ok(_) => panic!("HTTP 451 opened a WebSocket"),
        Err(error) => error,
    };
    match &error {
        FeedError::Ws(WsError::Upgrade(UpgradeError::Status(Some(451)))) => {}
        other => panic!("expected the HTTP 451 refusal, got {other:?}"),
    }
    assert!(error.to_string().contains("451"), "{error}");
    connect_refused(&handle, &spawner, false).await;
    done.set(true);
}

#[switch_test_support::pinned_test]
fn upgrade_refused_with_http_451_reports_the_status() {
    with_net(&[], None, |net, clock, listener| {
        let (server_tls, roots) = server_config();
        let server = thread::spawn(move || serve(listener, server_tls, ServerPlan::Refuse451));
        let mut rt = Runtime::new(clock.ns());
        let done = Rc::new(Cell::new(false));
        let task = refused_451_then_slot_free(net.handle(), rt.spawner(), roots, done.clone());
        rt.spawner().spawn(task).expect("spawn the client");
        turn_until(net, clock, &mut rt, || done.get() && server.is_finished());
        let saw = join(server);
        assert!(saw.error.is_some(), "server accepted the upgrade");
    });
}

#[switch_runtime::task]
async fn unknown_issuer_then_slot_free(handle: NetHandle, spawner: Spawner, done: Rc<Cell<bool>>) {
    let error = match connect(&handle, &spawner, &target(), socket(None), webpki_client_config(), LOCAL_PORT, CONNECT_LIMIT).await {
        Ok(_) => panic!("Mozilla roots trusted the self-signed certificate"),
        Err(error) => error,
    };
    assert!(
        matches!(
            error,
            FeedError::Ws(WsError::Tls(switch_tls::Error::Tls(rustls::Error::InvalidCertificate(
                rustls::CertificateError::UnknownIssuer
            ))))
        ),
        "expected UnknownIssuer, got {error:?}",
    );
    connect_refused(&handle, &spawner, false).await;
    done.set(true);
}

#[switch_test_support::pinned_test]
fn unknown_certificate_fails_tls_and_alerts_the_server() {
    with_net(&[], None, |net, clock, listener| {
        let (server_tls, _) = server_config();
        let server = thread::spawn(move || serve(listener, server_tls, ServerPlan::TlsOnly));
        let mut rt = Runtime::new(clock.ns());
        let done = Rc::new(Cell::new(false));
        let task = unknown_issuer_then_slot_free(net.handle(), rt.spawner(), done.clone());
        rt.spawner().spawn(task).expect("spawn the client");
        turn_until(net, clock, &mut rt, || done.get() && server.is_finished());
        let saw = join(server);
        let error = saw.error.expect("server TLS handshake did not fail");
        assert!(error.contains("UnknownCA"), "server did not receive the client's alert: {error}");
    });
}

/// A JSON-escaped field (Binance does not escape, but the text may) still parses.
#[test]
fn book_ticker_decodes_escaped_fields() {
    let text = r#"{"u":7,"s":"BTC\u0055SDT","b":"1.5","B":"2","a":"1.6","A":"3"}"#;
    let ticker = serde_json::from_str::<BookTicker>(text).expect("parse an escaped symbol");
    assert_eq!(ticker.symbol, "BTCUSDT");
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
