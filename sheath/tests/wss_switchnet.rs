//! The wss client (sheath + relay over switchnet TCP, as switch tasks) against a kernel wss
//! echo server (blocking tungstenite + rustls on a std thread) across net_tap. Needs
//! CAP_NET_ADMIN and /dev/net/tun (the Docker test container).

use std::ffi::CStr;
use std::io::{Read, Write};
use std::net::{Ipv4Addr, SocketAddrV4, TcpListener};
use std::os::fd::AsRawFd;
use std::panic::resume_unwind;
use std::pin::Pin;
use std::process::Command;
use std::rc::Rc;
use std::cell::{Cell, RefCell};
use std::sync::Arc;
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use dpdk_smoltcp::smoltcp::time::Instant as StackInstant;
use dpdk_smoltcp::smoltcp::wire::{EthernetAddress, Ipv4Cidr};
use dpdk_smoltcp::{
    DEFAULT_NEIGHBOR_CAPACITY, Offloads, Port, PortConfig, Stack, StackConfig, frame_reserve, rx_reserve,
};
use relay::Kind;
use rustls::pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer};
use rustls::version::TLS13;
use rustls::{ClientConfig, RootCertStore, ServerConfig, ServerConnection, StreamOwned};
use sheath::switchnet::{Wss, connect};
use switch::Runtime;
use switchnet::{IpEndpoint, IpListenEndpoint, Net, NetConfig, tcp};
use tungstenite::protocol::CloseFrame;
use tungstenite::protocol::frame::coding::CloseCode;
use tungstenite::{Error as WsError, Message};
use zhida_buffer::PoolConfig;
use zhida_dpdk_sys::RTE_PKTMBUF_HEADROOM;
use zhida_test_kit::{tap::TapVdev, with_eal};

const VDEV: &CStr = c"net_tap_wss";
const IFACE: &CStr = c"cdtap_wss";
const RX_POOL: &CStr = c"wss_rx";
const TX_POOL: &CStr = c"wss_tx";
const FRAME_POOL: &CStr = c"wss_frame";
const ZHIDA_ADDR: Ipv4Addr = Ipv4Addr::new(192, 0, 2, 2);
const KERNEL_ADDR: Ipv4Addr = Ipv4Addr::new(192, 0, 2, 1);
const SERVER_PORT: u16 = 9443;
const LOCAL_PORT: u16 = 49152;
const HOST: &str = "wss.test";
const PATH: &str = "/echo";
const MTU: u16 = 1500;
const INGRESS_BUDGET: usize = 32;
/// The whole client run, and every server socket read and write.
const LIMIT: Duration = Duration::from_secs(60);
/// The main loop sleeps this long each round so the server thread, on the same CPU, runs.
const ROUND_SLEEP: Duration = Duration::from_micros(50);

type ServerWs = tungstenite::WebSocket<StreamOwned<ServerConnection, std::net::TcpStream>>;

fn socket() -> tcp::Config {
    tcp::Config { rx_capacity: 64 * 1024, tx_capacity: 64 * 1024, mss_cap: None }
}

fn port_config(port_id: u16) -> PortConfig<'static> {
    let pool = |name, count: usize| PoolConfig {
        name,
        count: u32::try_from(count).unwrap(),
        cache_size: 8,
        data_room_size: RTE_PKTMBUF_HEADROOM as u16 + 2048,
        socket_id: -1,
    };
    PortConfig {
        port_id,
        mtu: MTU,
        rx_descriptors: 16,
        tx_descriptors: 16,
        rx_burst: 32,
        tx_burst: 32,
        queue_socket: -1,
        rx_pool: pool(RX_POOL, rx_reserve(16, 32, 8) + tcp::rx_mbuf_budget(&socket(), MTU.into())),
        tx_pool: pool(TX_POOL, 127),
        frame_pool: pool(FRAME_POOL, frame_reserve(16, 32) + tcp::tx_mbuf_budget(&socket(), MTU.into())),
        offloads: Offloads::default(),
    }
}

fn add_address(addr: Ipv4Addr, prefix: u8) {
    let cidr = format!("{addr}/{prefix}");
    let output = Command::new("ip")
        .args(["address", "add", &cidr, "dev", IFACE.to_str().unwrap()])
        .output()
        .expect("run ip address add");
    assert!(output.status.success(), "ip address add {cidr}: {}", String::from_utf8_lossy(&output.stderr));
}

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

type Tls = StreamOwned<ServerConnection, std::net::TcpStream>;

/// Accepts one connection; the TLS handshake runs on its first read.
fn accept(listener: TcpListener, config: Arc<ServerConfig>) -> Tls {
    let (stream, _) = listener.accept().expect("accept the client");
    stream.set_read_timeout(Some(LIMIT)).unwrap();
    stream.set_write_timeout(Some(LIMIT)).unwrap();
    StreamOwned::new(ServerConnection::new(config).unwrap(), stream)
}

/// The WebSocket upgrade, server side; checks the client's path and Host.
fn upgrade(tls: Tls) -> ServerWs {
    tungstenite::accept_hdr(tls, |request: &tungstenite::handshake::server::Request, response| {
        assert_eq!(request.uri().path(), PATH);
        assert_eq!(request.headers()["Host"], format!("{HOST}:{SERVER_PORT}").as_str());
        Ok(response)
    })
    .expect("server WebSocket handshake")
}

/// After the close handshake: the client's close_notify (rustls reads a clean end) and then its
/// FIN (the socket reads end of stream).
fn assert_close_notify_then_fin(ws: ServerWs) {
    let mut tls = ws.into_inner();
    let mut byte = [0; 1];
    assert_eq!(tls.read(&mut byte).expect("clean TLS end: close_notify, not truncation"), 0);
    assert_eq!(tls.sock.read(&mut byte).expect("TCP end of stream"), 0);
}

#[switch::task]
fn boxed(future: Pin<Box<dyn Future<Output = ()>>>) -> impl Future<Output = ()> {
    future
}

/// One run: a net_tap Stack under a switchnet Net; the kernel server on a thread runs `serve`
/// on the accepted TLS stream (after `listener_setup` on its listener); `client` runs as a
/// switch task on the connected `Wss` and hands it back. Turns the Net, calling `each_round`
/// each time, until the client and the server are both done; the client's `Wss` is dropped
/// only then, so what the server saw came from the client, not from its sockets closing.
/// Returns what `serve` returned.
fn wss_run<T: Send + 'static, F: Future<Output = Wss> + 'static>(
    listener_setup: impl FnOnce(&TcpListener),
    serve: impl FnOnce(Tls) -> T + Send + 'static,
    client: impl FnOnce(Wss) -> F + 'static,
    mut each_round: impl FnMut(),
) -> T {
    with_eal(|eal| {
        let tap = TapVdev::new(VDEV, IFACE);
        // SAFETY: net_tap was just created and this EAL thread alone configures and uses its
        // Port and packets. The offload-free net_tap profile returns single-segment RX buffers
        // and frees TX buffers synchronously. The Port closes before the vdev is removed, and
        // EAL outlives both.
        let port = unsafe { Port::open(eal, port_config(tap.port_id())) }.expect("open net_tap Port");
        add_address(KERNEL_ADDR, 24);
        let listener = TcpListener::bind(SocketAddrV4::new(KERNEL_ADDR, SERVER_PORT)).expect("bind the server");
        listener_setup(&listener);
        let (server_tls, client_tls) = tls_configs();
        let server = thread::spawn(move || serve(accept(listener, server_tls)));

        let config = StackConfig {
            max_udp_sockets: 0,
            ethernet: EthernetAddress([0x02, 0, 0, 0, 0, 0x01]),
            ipv4: Ipv4Cidr::new(ZHIDA_ADDR, 24),
            gateway: None,
            paths: &[],
            random_seed: 1,
            neighbor_capacity: DEFAULT_NEIGHBOR_CAPACITY,
            segments_per_visit: 1,
            max_sockets: 1,
            rx_staging: 32,
            tx_pending: 32,
        };
        let start = Instant::now();
        let now = || StackInstant::from_micros(i64::try_from(start.elapsed().as_micros()).unwrap());
        let stack = Stack::new(port, config, now()).map_err(|(_, e)| e).expect("create Zhida Stack");
        let mut net = Net::new(stack, NetConfig { ingress_budget: INGRESS_BUDGET });
        let mut rt = Runtime::new(0);
        let handle = net.handle();
        let held: Rc<RefCell<Option<Wss>>> = Rc::default();
        let task_held = held.clone();
        let task = async move {
            let remote = IpEndpoint::new(KERNEL_ADDR.into(), SERVER_PORT);
            let local = IpListenEndpoint { addr: None, port: LOCAL_PORT };
            let stream = handle.tcp_connect(socket(), remote, local).expect("open TCP").await.expect("TCP connect");
            let wss = connect(&handle, stream, client_tls, HOST, SERVER_PORT, PATH).await.expect("wss connect");
            *task_held.borrow_mut() = Some(client(wss).await);
        };
        rt.spawner().spawn(boxed(Box::pin(task))).expect("spawn the client");

        let client_done = || held.borrow().is_some();
        while !(client_done() && server.is_finished()) {
            assert!(start.elapsed() < LIMIT, "client done {}, server done {}", client_done(), server.is_finished());
            net.turn(now()).expect("turn the Net");
            rt.poll(0).expect("poll the client");
            each_round();
            thread::sleep(ROUND_SLEEP);
        }
        let saw = match server.join() {
            Ok(saw) => saw,
            Err(panic) => resume_unwind(panic),
        };
        drop(held.borrow_mut().take());
        rt.shutdown();
        let stack = net.into_stack().ok().expect("the client's sockets are gone");
        stack.into_port().try_close().expect("close the Port with every buffer returned");
        tap.close();
        saw
    })
}

/// Text of `len` bytes, `seed`-dependent, with multibyte characters.
fn text(seed: usize, len: usize) -> String {
    let mut s = String::with_capacity(len);
    let mut i = seed;
    while s.len() < len {
        let c = ['a', '0', '{', '€', 'ß', '"'][i % 6];
        if s.len() + c.len_utf8() > len {
            s.push('x');
        } else {
            s.push(c);
        }
        i += 1;
    }
    s
}

fn message_len(i: usize) -> usize {
    1 + (i * 7919) % (16 * 1024)
}

/// Echoes every data message until the client's close; returns how many.
fn echo(mut ws: ServerWs) -> usize {
    let mut echoed = 0;
    loop {
        match ws.read() {
            Ok(m @ (Message::Text(_) | Message::Binary(_))) => {
                ws.send(m).expect("echo");
                echoed += 1;
            }
            Ok(Message::Close(_)) => {}
            Ok(other) => panic!("unexpected {other:?}"),
            Err(WsError::ConnectionClosed) => return echoed,
            Err(e) => panic!("server read: {e}"),
        }
    }
}

async fn recv_text(wss: &mut Wss) -> String {
    let m = wss.recv().await.expect("recv").expect("a message, not the end");
    m.text().expect("a text message").to_owned()
}

/// The client closes and waits for the end.
async fn close(wss: &mut Wss) {
    wss.close(1000, "done").await.expect("send close");
    assert!(wss.recv().await.expect("recv after close").is_none(), "data after close");
}

#[zhida_test_support::pinned_test]
fn thousand_mixed_messages_echo_in_order() {
    let echoed = wss_run(
        |_| {},
        |tls| echo(upgrade(tls)),
        |mut wss| async move {
            for i in 0..1000 {
                let sent = text(i, message_len(i));
                wss.send(Kind::Text, sent.as_bytes()).await.expect("send");
                assert_eq!(recv_text(&mut wss).await, sent, "message {i}");
            }
            close(&mut wss).await;
            wss
        },
        || {},
    );
    assert_eq!(echoed, 1000);
}

#[zhida_test_support::pinned_test]
fn message_larger_than_a_frame_echoes() {
    let echoed = wss_run(
        |_| {},
        |tls| echo(upgrade(tls)),
        |mut wss| async move {
            let sent: Vec<u8> = (0..64 * 1024).map(|i| (i * 31 % 251) as u8).collect();
            wss.send(Kind::Binary, &sent).await.expect("send");
            let m = wss.recv().await.expect("recv").expect("the echo");
            assert_eq!(m.kind(), Kind::Binary);
            assert_eq!(m.bytes(), &sent[..]);
            close(&mut wss).await;
            wss
        },
        || {},
    );
    assert_eq!(echoed, 1);
}

/// The server pings; the client only receives, and its receive answers the ping.
#[zhida_test_support::pinned_test]
fn server_ping_is_answered_inside_recv() {
    let pong = wss_run(
        |_| {},
        |tls| {
            let mut ws = upgrade(tls);
            ws.write(Message::Ping(b"hb".to_vec().into())).unwrap();
            ws.write(Message::Text("after the ping".into())).unwrap();
            ws.flush().unwrap();
            let mut pong = None;
            loop {
                match ws.read() {
                    Ok(Message::Pong(p)) => pong = Some(p.to_vec()),
                    Ok(Message::Close(_)) => {}
                    Ok(other) => panic!("unexpected {other:?}"),
                    Err(WsError::ConnectionClosed) => return pong,
                    Err(e) => panic!("server read: {e}"),
                }
            }
        },
        |mut wss| async move {
            assert_eq!(recv_text(&mut wss).await, "after the ping");
            close(&mut wss).await;
            wss
        },
        || {},
    );
    assert_eq!(pong.as_deref(), Some(&b"hb"[..]));
}

/// The client closes: the server sees the close frame, then close_notify, then FIN; the
/// client's recv ends cleanly after the server's echo.
#[zhida_test_support::pinned_test]
fn client_close_reaches_the_server_as_close_close_notify_fin() {
    let close_frame = wss_run(
        |_| {},
        |tls| {
            let mut ws = upgrade(tls);
            let frame = match ws.read() {
                Ok(Message::Close(frame)) => frame,
                other => panic!("expected the close: {other:?}"),
            };
            // tungstenite sends its echo, then reports the finished handshake.
            assert!(matches!(ws.read(), Err(WsError::ConnectionClosed)));
            assert_close_notify_then_fin(ws);
            frame
        },
        |mut wss| async move {
            wss.close(1000, "bye").await.expect("send close");
            assert!(wss.recv().await.expect("recv after close").is_none());
            wss
        },
        || {},
    );
    let frame = close_frame.expect("a close payload");
    assert_eq!((frame.code, frame.reason.as_str()), (CloseCode::Normal, "bye"));
}

/// The server closes: the client's recv returns the end, after echoing the close and ending
/// TLS and TCP.
#[zhida_test_support::pinned_test]
fn server_close_ends_recv_cleanly() {
    let echo = wss_run(
        |_| {},
        |tls| {
            let mut ws = upgrade(tls);
            ws.close(Some(CloseFrame { code: CloseCode::Away, reason: "bye".into() })).unwrap();
            let echo = match ws.read() {
                Ok(Message::Close(frame)) => frame,
                other => panic!("expected the echoed close: {other:?}"),
            };
            assert!(matches!(ws.read(), Err(WsError::ConnectionClosed)));
            assert_close_notify_then_fin(ws);
            echo
        },
        |mut wss| async move {
            assert!(wss.recv().await.expect("recv").is_none(), "a message instead of the end");
            wss
        },
        || {},
    );
    let echo = echo.expect("the echoed close payload");
    assert_eq!((echo.code, echo.reason.as_str()), (CloseCode::Away, "bye"));
}

/// The server stops reading: with its small receive buffer full, the client's sends stop in
/// the wait for room; once the server reads again every message arrives byte-exact.
#[zhida_test_support::pinned_test]
fn sends_wait_for_room_while_the_server_stops_reading() {
    const MESSAGES: usize = 64;
    const LEN: usize = 64 * 1024;
    let payload = |i: usize| -> Vec<u8> { (0..LEN).map(|j| (i * 131 + j * 7) as u8).collect() };
    let (go_tx, go_rx) = mpsc::channel::<()>();
    let sent = Rc::new(Cell::new(0));
    let client_sent = sent.clone();
    // The round at which `sent` last changed, and the count then.
    let seen = RefCell::new((Instant::now(), 0));
    let stalled = Cell::new(false);
    let received = wss_run(
        |listener| {
            let size: libc::c_int = 4096;
            // SAFETY: a valid socket and an int option of the right size; accepted sockets
            // inherit the buffer size.
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
        },
        move |tls| {
            let mut ws = upgrade(tls);
            go_rx.recv_timeout(LIMIT).expect("the go signal");
            let mut received = 0;
            while received < MESSAGES {
                match ws.read() {
                    Ok(Message::Binary(b)) => {
                        assert!(b[..] == payload(received)[..], "message {received} differs");
                        received += 1;
                    }
                    other => panic!("unexpected {other:?}"),
                }
            }
            ws.send(Message::Text(format!("got {received}").into())).unwrap();
            echo(ws);
            received
        },
        move |mut wss| async move {
            for i in 0..MESSAGES {
                wss.send(Kind::Binary, &payload(i)).await.expect("send");
                client_sent.set(i + 1);
            }
            assert_eq!(recv_text(&mut wss).await, format!("got {MESSAGES}"));
            close(&mut wss).await;
            wss
        },
        || {
            let mut seen = seen.borrow_mut();
            if sent.get() != seen.1 {
                *seen = (Instant::now(), sent.get());
            } else if !stalled.get() && sent.get() > 0 && seen.0.elapsed() > Duration::from_millis(300) {
                // Parked: nothing sent for 300 ms with messages left.
                assert!(sent.get() < MESSAGES, "all sent while the server did not read");
                stalled.set(true);
                go_tx.send(()).unwrap();
            }
        },
    );
    assert!(stalled.get());
    assert_eq!(received, MESSAGES);
}

/// The server writes its 101 response and a first text frame in one write (one TLS record):
/// the bytes after the response head reach the client as that message.
#[zhida_test_support::pinned_test]
fn message_in_the_same_record_as_the_upgrade_response() {
    let echoed = wss_run(
        |_| {},
        |mut tls| {
            let mut head = Vec::new();
            let mut byte = [0; 1];
            while !head.ends_with(b"\r\n\r\n") {
                assert_eq!(tls.read(&mut byte).expect("read the upgrade request"), 1);
                head.push(byte[0]);
            }
            let head = String::from_utf8(head).unwrap();
            let key = head
                .lines()
                .find_map(|l| l.strip_prefix("Sec-WebSocket-Key: "))
                .expect("the client sends a key");
            let accept = tungstenite::handshake::derive_accept_key(key.as_bytes());
            let mut out = format!(
                "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\
                 Sec-WebSocket-Accept: {accept}\r\n\r\n"
            )
            .into_bytes();
            out.extend_from_slice(&[0x81, 5]);
            out.extend_from_slice(b"first");
            tls.write_all(&out).unwrap();
            tls.flush().unwrap();
            echo(tungstenite::WebSocket::from_raw_socket(tls, tungstenite::protocol::Role::Server, None))
        },
        |mut wss| async move {
            assert_eq!(recv_text(&mut wss).await, "first");
            wss.send(Kind::Text, b"second").await.expect("send");
            assert_eq!(recv_text(&mut wss).await, "second");
            close(&mut wss).await;
            wss
        },
        || {},
    );
    assert_eq!(echoed, 1);
}
