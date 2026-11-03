//! The WebSocket clients (relay over switchnet TCP, wss:// through sheath, as switch tasks)
//! against kernel servers (blocking tungstenite, with rustls for wss) across net_tap. Needs
//! CAP_NET_ADMIN and /dev/net/tun (the Docker test container).

use std::cell::{Cell, RefCell};
use std::io::{Read, Write};
use std::net::TcpListener;
use std::os::fd::AsRawFd;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::mpsc;
use std::time::{Duration, Instant};

use relay::{Kind, Limits};
use relay::switchnet::{Error as WssError, Ws, Wss, connect, connect_ws};
use rustls::pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer};
use rustls::version::TLS13;
use rustls::{ClientConfig, RootCertStore, ServerConfig, ServerConnection, StreamOwned};
use switchnet::testkit::{KERNEL_ADDR, KernelPeer, ROUND_SLEEP, TapConfig, client_server, next_local_port, with_tap_net};
use switchnet::{IpEndpoint, IpListenEndpoint};
use switchnet::{Net, NetHandle, TcpStream, tcp};
use tungstenite::protocol::CloseFrame;
use tungstenite::protocol::frame::coding::CloseCode;
use tungstenite::{Error as WsError, Message};

const SERVER_PORT: u16 = 9443;
const HOST: &str = "wss.test";
const PATH: &str = "/echo";
/// Every peer socket read and write.
const LIMIT: Duration = Duration::from_secs(60);

const TAP: TapConfig = TapConfig {
    name: "wss",
    socket: tcp::Config { rx_capacity: 64 * 1024, tx_capacity: 64 * 1024, mss_cap: None },
    max_sockets: 1,
    random_seed: 1,
};

type ServerWs = tungstenite::WebSocket<StreamOwned<ServerConnection, std::net::TcpStream>>;

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
fn assert_close_notify_then_fin(mut tls: Tls) {
    let mut byte = [0; 1];
    assert_eq!(tls.read(&mut byte).expect("clean TLS end: close_notify, not truncation"), 0);
    assert_eq!(tls.sock.read(&mut byte).expect("TCP end of stream"), 0);
}

/// One client against one kernel peer (see `client_server`), with the TLS configs the peer
/// and the client use.
fn run<T: Send + 'static, H: 'static, F: Future<Output = H> + 'static>(
    listener_setup: impl FnOnce(&TcpListener),
    serve: impl FnOnce(std::net::TcpStream, Arc<ServerConfig>) -> T + Send + 'static,
    client: impl FnOnce(NetHandle, TcpStream, Arc<ClientConfig>) -> F + 'static,
    each_round: impl FnMut(),
) -> T {
    let (server_tls, client_tls) = tls_configs();
    client_server(
        &TAP,
        SERVER_PORT,
        listener_setup,
        move |stream| serve(stream, server_tls),
        move |handle, stream| client(handle, stream, client_tls),
        each_round,
    )
}

/// `run` with a TLS server, which runs `serve` on the accepted TLS stream (handshake on its first
/// read), and a wss client, which runs `client` on the connected `Wss` and hands it back.
fn wss_run<T: Send + 'static, F: Future<Output = Wss> + 'static>(
    listener_setup: impl FnOnce(&TcpListener),
    serve: impl FnOnce(Tls) -> T + Send + 'static,
    client: impl FnOnce(Wss) -> F + 'static,
    each_round: impl FnMut(),
) -> T {
    run(
        listener_setup,
        |stream, config| serve(StreamOwned::new(ServerConnection::new(config).unwrap(), stream)),
        |handle, stream, config| async move {
            let wss = connect(&handle, stream, config, HOST, SERVER_PORT, PATH, Limits::default()).await.expect("wss connect");
            client(wss).await
        },
        each_round,
    )
}

/// One client against one kernel peer, like `client_server`, except that the Net is turned only
/// until the client task finishes: what the peer reads afterwards was on the wire by then, not
/// sent by later turns. What the client returns is dropped only after the peer is done.
fn until_client<T: Send + 'static, H: 'static, F: Future<Output = H> + 'static>(
    listener_setup: impl FnOnce(&TcpListener),
    serve: impl FnOnce(std::net::TcpStream) -> T + Send + 'static,
    client: impl FnOnce(NetHandle, TcpStream) -> F + 'static,
) -> T {
    until_client_on(&TAP, listener_setup, serve, client)
}

/// Connects to the kernel peer from a fresh local port, runs `client` on the stream, and puts
/// what it hands back in `held`.
#[switch::task]
async fn connect_client<H: 'static, F: Future<Output = H> + 'static, C: FnOnce(NetHandle, TcpStream) -> F + 'static>(
    handle: NetHandle,
    socket: tcp::Config,
    client: C,
    held: Rc<RefCell<Option<H>>>,
) {
    let remote = IpEndpoint::new(KERNEL_ADDR.into(), SERVER_PORT);
    let local = IpListenEndpoint { addr: None, port: next_local_port() };
    let stream = handle.tcp_connect(socket, remote, local).expect("open TCP").await.expect("TCP connect");
    *held.borrow_mut() = Some(client(handle, stream).await);
}

/// `until_client` on `tap`'s Net and socket config.
fn until_client_on<T: Send + 'static, H: 'static, F: Future<Output = H> + 'static>(
    tap: &TapConfig,
    listener_setup: impl FnOnce(&TcpListener),
    serve: impl FnOnce(std::net::TcpStream) -> T + Send + 'static,
    client: impl FnOnce(NetHandle, TcpStream) -> F + 'static,
) -> T {
    until_client_watching(tap, listener_setup, serve, client, |_| {})
}

/// `until_client_on`, calling `each_turn` with the Net after every receive (before the client is
/// polled in that round).
fn until_client_watching<T: Send + 'static, H: 'static, F: Future<Output = H> + 'static>(
    tap: &TapConfig,
    listener_setup: impl FnOnce(&TcpListener),
    serve: impl FnOnce(std::net::TcpStream) -> T + Send + 'static,
    client: impl FnOnce(NetHandle, TcpStream) -> F + 'static,
    mut each_turn: impl FnMut(&Net),
) -> T {
    let socket = tap.socket;
    with_tap_net(tap, |net, clock| {
        let mut peer = KernelPeer::listen(SERVER_PORT, listener_setup, serve);
        let mut rt = switch::Runtime::new(0);
        let handle = net.handle();
        let held: Rc<RefCell<Option<H>>> = Rc::default();
        rt.spawner().spawn(connect_client(handle, socket, client, held.clone())).expect("spawn the client");
        while held.borrow().is_none() {
            assert!(clock.elapsed() < LIMIT, "client not done");
            peer.accept();
            net.receive(clock.now()).expect("receive");
            each_turn(net);
            rt.poll(clock.ms()).expect("poll the client");
            net.send().expect("send");
            std::thread::sleep(ROUND_SLEEP);
        }
        let saw = peer.join();
        drop(held.borrow_mut().take());
        rt.shutdown();
        saw
    })
}

/// `until_client` with a TLS server running `serve` on the upgraded stream's raw bytes (the
/// tungstenite upgrade done, nothing read past it) and a wss client running `client`.
fn wss_until_client<T: Send + 'static, F: Future<Output = Wss> + 'static>(
    serve: impl FnOnce(Tls) -> T + Send + 'static,
    client: impl FnOnce(Wss) -> F + 'static,
) -> T {
    let (server_tls, client_tls) = tls_configs();
    until_client(
        |_| {},
        move |stream| serve(upgrade(StreamOwned::new(ServerConnection::new(server_tls).unwrap(), stream)).into_inner()),
        move |handle, stream| async move {
            let wss = connect(&handle, stream, client_tls, HOST, SERVER_PORT, PATH, Limits::default()).await.expect("wss connect");
            client(wss).await
        },
    )
}

/// `until_client` with a plain server running `serve` on the upgraded stream and a ws client
/// running `client`.
fn ws_until_client<T: Send + 'static, F: Future<Output = Ws> + 'static>(
    listener_setup: impl FnOnce(&TcpListener),
    serve: impl FnOnce(std::net::TcpStream) -> T + Send + 'static,
    client: impl FnOnce(Ws) -> F + 'static,
) -> T {
    until_client(
        listener_setup,
        move |stream| serve(tungstenite::accept(stream).expect("server WebSocket handshake").into_inner()),
        move |handle, stream| async move {
            let ws = connect_ws(&handle, stream, HOST, SERVER_PORT, PATH, Limits::default()).await.expect("ws connect");
            client(ws).await
        },
    )
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

/// One masked client frame off `r`: the first byte (FIN and opcode) and the unmasked payload.
fn read_frame(r: &mut impl Read) -> (u8, Vec<u8>) {
    let mut head = [0; 2];
    r.read_exact(&mut head).expect("a frame head");
    assert_eq!(head[1] & 0x80, 0x80, "client frames are masked");
    let len = match head[1] & 0x7f {
        126 => {
            let mut ext = [0; 2];
            r.read_exact(&mut ext).unwrap();
            usize::from(u16::from_be_bytes(ext))
        }
        127 => panic!("no test frame is that long"),
        n => usize::from(n),
    };
    let mut key = [0; 4];
    r.read_exact(&mut key).unwrap();
    let mut payload = vec![0; len];
    r.read_exact(&mut payload).unwrap();
    payload.iter_mut().zip(key.iter().cycle()).for_each(|(b, k)| *b ^= k);
    (head[0], payload)
}

/// An unmasked server frame, FIN set, under 126 bytes.
fn server_frame(opcode: u8, payload: &[u8]) -> Vec<u8> {
    [&[0x80 | opcode, payload.len() as u8][..], payload].concat()
}

const TEXT: u8 = 0x1;
const CLOSE: u8 = 0x8;
const PING: u8 = 0x9;

/// A close payload: the status code, big-endian, then the reason.
fn close_payload(code: u16, reason: &str) -> Vec<u8> {
    [&code.to_be_bytes()[..], reason.as_bytes()].concat()
}

/// Reads nothing for 300 ms: no byte, and not the end of the stream.
fn assert_quiet(sock: &std::net::TcpStream, r: &mut impl Read) {
    sock.set_read_timeout(Some(Duration::from_millis(300))).unwrap();
    let quiet = r.read(&mut [0; 1]).expect_err("a byte or the end before the echo");
    assert!(matches!(quiet.kind(), std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut), "{quiet}");
    sock.set_read_timeout(Some(LIMIT)).unwrap();
}

/// The end of the stream, with no byte before it.
fn assert_fin(sock: &mut std::net::TcpStream) {
    let mut rest = Vec::new();
    sock.read_to_end(&mut rest).expect("FIN, not RST");
    assert_eq!(rest, b"", "bytes before FIN");
}

/// `f`'s output if it finishes within `rounds` of the client's runtime, else `None` (`f`
/// dropped).
async fn within_rounds<F: Future>(rounds: usize, f: F) -> Option<F::Output> {
    let mut f = std::pin::pin!(f);
    for _ in 0..rounds {
        if let std::task::Poll::Ready(out) = std::future::poll_fn(|cx| std::task::Poll::Ready(f.as_mut().poll(cx))).await {
            return Some(out);
        }
        switch::yield_now().await;
    }
    None
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
fn echo<S: Read + Write>(mut ws: tungstenite::WebSocket<S>) -> usize {
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
            // a plain socket's blocking read returns EINTR when a signal arrives; tungstenite
            // keeps its partial frame, so reading again continues it (rustls retries itself)
            Err(WsError::Io(e)) if e.kind() == std::io::ErrorKind::Interrupted => {}
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

/// The client closes: the server sees the close frame alone, and close_notify and FIN only
/// after its echo; the client's recv ends cleanly with that echo, and by then (the Net not
/// turned again) close_notify and FIN are on the wire.
#[zhida_test_support::pinned_test]
fn client_close_sends_close_notify_and_fin_after_the_echo() {
    let close = wss_until_client(
        |mut tls| {
            let close = read_frame(&mut tls);
            assert_quiet(&tls.sock.try_clone().unwrap(), &mut tls);
            tls.write_all(&server_frame(CLOSE, &close_payload(1000, "bye"))).unwrap();
            tls.flush().unwrap();
            assert_close_notify_then_fin(tls);
            close
        },
        |mut wss| async move {
            wss.close(1000, "bye").await.expect("send close");
            assert!(wss.recv().await.expect("recv after close").is_none());
            wss
        },
    );
    assert_eq!(close, (0x80 | CLOSE, close_payload(1000, "bye")));
}

/// The same for ws://: the server sees the close frame alone, FIN only after its echo, and the
/// FIN is on the wire once the client's recv ended.
#[zhida_test_support::pinned_test]
fn ws_client_close_sends_fin_after_the_echo() {
    let close = ws_until_client(
        |_| {},
        |mut sock| {
            let close = read_frame(&mut sock);
            assert_quiet(&sock.try_clone().unwrap(), &mut sock);
            sock.write_all(&server_frame(CLOSE, &close_payload(1000, "bye"))).unwrap();
            assert_fin(&mut sock);
            close
        },
        |mut ws| async move {
            ws.close(1000, "bye").await.expect("send close");
            assert!(ws.recv().await.expect("recv after close").is_none());
            ws
        },
    );
    assert_eq!(close, (0x80 | CLOSE, close_payload(1000, "bye")));
}

/// After our close the server still sends a Text and a Ping before its echo: recv returns the
/// Text, answers no Ping (a close is queued), then ends; the server reads nothing after our close
/// but close_notify and FIN (Wss) or FIN (Ws): no Pong, no second close.
#[zhida_test_support::pinned_test]
fn data_and_ping_after_our_close_then_the_echo() {
    let late = |w: &mut dyn Write| {
        let frames = [server_frame(TEXT, b"late"), server_frame(PING, b"p"), server_frame(CLOSE, &close_payload(1000, ""))];
        w.write_all(&frames.concat()).unwrap();
        w.flush().unwrap();
    };
    let close = wss_until_client(
        move |mut tls| {
            let close = read_frame(&mut tls);
            late(&mut tls);
            assert_close_notify_then_fin(tls);
            close
        },
        |mut wss| async move {
            wss.close(1000, "").await.expect("send close");
            assert_eq!(recv_text(&mut wss).await, "late");
            assert!(wss.recv().await.expect("recv after close").is_none());
            wss
        },
    );
    assert_eq!(close, (0x80 | CLOSE, close_payload(1000, "")));
    let close = ws_until_client(
        |_| {},
        move |mut sock| {
            let close = read_frame(&mut sock);
            late(&mut sock);
            assert_fin(&mut sock);
            close
        },
        |mut ws| async move {
            ws.close(1000, "").await.expect("send close");
            let m = ws.recv().await.expect("recv").expect("the late Text");
            assert_eq!(m.text(), Some("late"));
            assert!(ws.recv().await.expect("recv after close").is_none());
            ws
        },
    );
    assert_eq!(close, (0x80 | CLOSE, close_payload(1000, "")));
}

/// Crossing closes: the server sends its close right after the upgrade and the client closes
/// without having read it. The server reads one close (ours, 1000, not an echo of its 1001), then
/// FIN.
#[zhida_test_support::pinned_test]
fn crossing_closes_send_one_close_and_fin() {
    let close = ws_until_client(
        |_| {},
        |mut sock| {
            sock.write_all(&server_frame(CLOSE, &close_payload(1001, "away"))).unwrap();
            let close = read_frame(&mut sock);
            assert_fin(&mut sock);
            close
        },
        |mut ws| async move {
            ws.close(1000, "mine").await.expect("send close");
            assert!(ws.recv().await.expect("recv after close").is_none());
            ws
        },
    );
    assert_eq!(close, (0x80 | CLOSE, close_payload(1000, "mine")));
}

/// A server that never echoes: the client's recv after its close stays pending (a caller bounds
/// it), and nothing but the close was sent: no FIN.
#[zhida_test_support::pinned_test]
fn recv_after_close_waits_for_the_echo() {
    let (tx, rx) = mpsc::channel::<()>();
    let close = ws_until_client(
        |_| {},
        move |mut sock| {
            let close = read_frame(&mut sock);
            rx.recv_timeout(LIMIT).expect("the client gave up");
            // no FIN without the echo (the client still holds the connection)
            assert_quiet(&sock.try_clone().unwrap(), &mut sock);
            close
        },
        |mut ws| async move {
            ws.close(1000, "").await.expect("send close");
            assert!(within_rounds(2000, ws.recv()).await.is_none(), "recv ended without the echo");
            tx.send(()).unwrap();
            ws
        },
    );
    assert_eq!(close.0, 0x80 | CLOSE);
}

/// A server that stops reading: the client's one-byte messages fill its window and then the
/// send buffer or frame slots, so `close` waits for room; that wait is dropped. The next recv
/// still sends the queued close, after every message sent before it; the server echoes it and
/// sees FIN. The send dropped in its wait went out too if it was accepted: it had a frame (so
/// was sealed) and only TCP's room was missing; dropped waiting for a frame, it was not.
#[zhida_test_support::pinned_test]
fn close_dropped_while_waiting_for_room_is_sent_by_recv() {
    let (go_tx, go_rx) = mpsc::channel::<()>();
    let sent = Rc::new(Cell::new(0usize));
    let client_sent = sent.clone();
    let (frames, close) = ws_until_client(
        |listener| set_rcvbuf(listener, 4096),
        move |mut sock| {
            go_rx.recv_timeout(LIMIT).expect("the client's go");
            let mut frames = 0usize;
            let close = loop {
                match read_frame(&mut sock) {
                    (0x82, payload) => {
                        assert_eq!(payload, [frames as u8], "message {frames}");
                        frames += 1;
                    }
                    close => break close,
                }
            };
            sock.write_all(&server_frame(CLOSE, &close.1)).unwrap();
            assert_fin(&mut sock);
            (frames, close)
        },
        |mut ws| async move {
            let mut n = 0usize;
            while within_rounds(2000, ws.send(Kind::Binary, &[n as u8])).await.is_some() {
                n += 1;
            }
            client_sent.set(n);
            assert!(within_rounds(2000, ws.close(1000, "full")).await.is_none(), "close found room");
            go_tx.send(()).unwrap();
            assert!(ws.recv().await.expect("recv after close").is_none());
            ws
        },
    );
    assert!(sent.get() > 0);
    // each message's payload is its index (checked as read), so none was lost or doubled
    assert!(
        frames == sent.get() || frames == sent.get() + 1,
        "the server read {frames} messages, not the {} sends that finished (plus one if the dropped one was accepted)",
        sent.get()
    );
    assert_eq!(close, (0x80 | CLOSE, close_payload(1000, "full")));
}

/// A server that reads nothing until told, then reads messages (answering a Binary with Text
/// "got") until the stream ends; what it read.
fn read_after_go(go: mpsc::Receiver<()>) -> impl FnOnce(std::net::TcpStream) -> Vec<Message> + Send + 'static {
    move |sock| {
        go.recv_timeout(LIMIT).expect("the client's go");
        let mut ws = tungstenite::WebSocket::from_raw_socket(sock, tungstenite::protocol::Role::Server, None);
        let mut read = Vec::new();
        loop {
            match ws.read() {
                Ok(m) => {
                    if m.is_binary() {
                        ws.send(Message::Text("got".into())).expect("answer");
                    }
                    read.push(m);
                }
                Err(WsError::ConnectionClosed) => return read,
                Err(WsError::Io(e)) if e.kind() == std::io::ErrorKind::Interrupted => {}
                Err(e) => panic!("server read: {e}"),
            }
        }
    }
}

/// A 1 MiB send dropped mid-message (the server not reading), then `recv`: the message's rest
/// goes out while recv waits, so the server reads it whole and answers. (Before the fix recv
/// waited for input only and the server for the rest of the message: neither moved.)
#[zhida_test_support::pinned_test]
fn recv_sends_the_rest_of_a_dropped_send() {
    let (go_tx, go_rx) = mpsc::channel::<()>();
    let read = ws_until_client(
        |listener| set_rcvbuf(listener, 4096),
        read_after_go(go_rx),
        move |mut ws| async move {
            assert!(within_rounds(2000, ws.send(Kind::Binary, &[7; 1 << 20])).await.is_none(), "1 MiB found room");
            go_tx.send(()).unwrap();
            let answer = within_rounds(20_000, ws.recv()).await.expect("recv stalled behind the dropped send");
            assert_eq!(answer.expect("recv").expect("a message").text(), Some("got"));
            ws.close(1000, "").await.expect("close");
            assert!(ws.recv().await.expect("recv after close").is_none());
            ws
        },
    );
    assert_eq!(read[0], Message::Binary(vec![7; 1 << 20].into()));
    assert!(matches!(read[1..], [Message::Close(_)]), "{:?}", &read[1..]);
}

/// A 1 MiB send dropped mid-message, then a close dropped behind it, then `recv`: the
/// message's rest and the close go out, the server echoes, recv ends cleanly. (Before the fix
/// both stayed queued while recv waited for input.)
#[zhida_test_support::pinned_test]
fn recv_sends_a_dropped_close_behind_a_dropped_send() {
    let (go_tx, go_rx) = mpsc::channel::<()>();
    let read = ws_until_client(
        |listener| set_rcvbuf(listener, 4096),
        read_after_go(go_rx),
        move |mut ws| async move {
            assert!(within_rounds(2000, ws.send(Kind::Binary, &[7; 1 << 20])).await.is_none(), "1 MiB found room");
            assert!(within_rounds(50, ws.close(1000, "bye")).await.is_none(), "close found room");
            go_tx.send(()).unwrap();
            while let Some(m) = within_rounds(20_000, ws.recv()).await.expect("recv stalled behind the dropped send").expect("recv") {
                assert_eq!(m.text(), Some("got"));
            }
            ws
        },
    );
    assert_eq!(read[0], Message::Binary(vec![7; 1 << 20].into()));
    assert!(matches!(&read[1..], [Message::Close(Some(c))] if c.reason == "bye"), "{:?}", &read[1..]);
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
            assert_close_notify_then_fin(ws.into_inner());
            echo
        },
        |mut wss| async move {
            assert!(wss.recv().await.expect("recv").is_none(), "a message instead of the end");
            assert_eq!(wss.peer_close(), Some((1001, "bye")));
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
        |listener| set_rcvbuf(listener, 4096),
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

/// A record that fails authentication after the upgrade, with more data behind it: `Wss::recv`
/// returns the error once the alert and FIN reached the server, so the client may drop the
/// `Wss` (and the Net) at once: the server reads the alert, then the end, with the Net no longer
/// turned once the client finished.
#[zhida_test_support::pinned_test]
fn wss_receive_error_sends_the_alert_and_fin() {
    let (server_tls, client_tls) = tls_configs();
    let seen = until_client(
        |_| {},
        move |stream| {
            let mut ws = upgrade(StreamOwned::new(ServerConnection::new(server_tls).unwrap(), stream));
            let tls = ws.get_mut();
            let mut junk = vec![0x17, 0x03, 0x03, 0x00, 0x20];
            junk.resize(junk.len() + 32 + TRAILING, 0);
            tls.sock.write_all(&junk).unwrap();
            read_alert(&mut tls.conn, &mut tls.sock)
        },
        |handle, stream| async move {
            let mut wss = connect(&handle, stream, client_tls, HOST, SERVER_PORT, PATH, Limits::default()).await.expect("wss connect");
            match wss.recv().await {
                Err(WssError::Failed(relay::switchnet::Failure::Tls(sheath::Error::Tls(rustls::Error::DecryptError)))) => {}
                Err(e) => panic!("unexpected error {e}"),
                Ok(_) => panic!("a message from a corrupted record"),
            }
            wss
        },
    );
    assert_eq!(seen, BAD_RECORD_MAC);
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

/// ws:// over plain switchnet TCP: the upgrade (with `Host: host:port`), then text and binary
/// messages echo byte-exact, and the client's close ends with the server's echo and FIN.
#[zhida_test_support::pinned_test]
fn plain_ws_echoes_and_closes() {
    let echoed = run(
        |_| {},
        |stream, _| {
            let ws = tungstenite::accept_hdr(stream, |request: &tungstenite::handshake::server::Request, response| {
                assert_eq!(request.uri().path(), PATH);
                assert_eq!(request.headers()["Host"], format!("{HOST}:{SERVER_PORT}").as_str());
                Ok(response)
            })
            .expect("server WebSocket handshake");
            echo(ws)
        },
        |handle, stream, _| async move {
            let mut ws = connect_ws(&handle, stream, HOST, SERVER_PORT, PATH, Limits::default()).await.expect("ws connect");
            for i in 0..100 {
                let sent = text(i, message_len(i));
                ws.send(Kind::Text, sent.as_bytes()).await.expect("send");
                let m = ws.recv().await.expect("recv").expect("the echo");
                assert_eq!(m.text(), Some(sent.as_str()), "message {i}");
            }
            let big: Vec<u8> = (0..64 * 1024).map(|i| (i * 31 % 251) as u8).collect();
            ws.send(Kind::Binary, &big).await.expect("send");
            let m = ws.recv().await.expect("recv").expect("the echo");
            assert_eq!((m.kind(), m.bytes()), (Kind::Binary, &big[..]));
            ws.close(1000, "done").await.expect("send close");
            assert!(ws.recv().await.expect("recv after close").is_none(), "data after close");
            ws
        },
        || {},
    );
    assert_eq!(echoed, 101);
}

// Setup checks (slice 2): refusals before any byte, and socket death noticed while waiting for a
// frame.

/// The peer side of a refused setup: nothing arrives within a second (the Net stops turning
/// once the client is done, so its FIN may never leave).
fn sees_nothing(mut sock: std::net::TcpStream) {
    sock.set_read_timeout(Some(Duration::from_secs(1))).unwrap();
    let mut rest = Vec::new();
    let _ = sock.read_to_end(&mut rest);
    assert_eq!(rest, b"", "bytes from a refused setup");
}

/// A TapConfig whose sockets have a `tx`-byte send buffer.
const fn tap_with_send_buffer(tx: usize) -> TapConfig {
    TapConfig { socket: tcp::Config { tx_capacity: tx, ..TAP.socket }, ..TAP }
}

/// A stream from one Net with the handle of another: refused as `Config(ForeignHandle)` before
/// any byte (before the fix, frames from the other Net were refused as foreign and the request's
/// send retried forever).
#[zhida_test_support::pinned_test]
fn ws_connect_with_another_nets_handle_is_refused() {
    until_client(
        |_| {},
        sees_nothing,
        |_, stream| async move {
            const OTHER: TapConfig = TapConfig { name: "wsso", ..TAP };
            with_tap_net(&OTHER, |other, _| {
                // the other tap only lends a handle: take its address off so routes stay on ours
                let status = std::process::Command::new("ip")
                    .args(["address", "del", &format!("{KERNEL_ADDR}/24"), "dev", "cdtap_wsso"])
                    .status()
                    .unwrap();
                assert!(status.success(), "ip address del");
                let handle = other.handle();
                let mut connecting = std::pin::pin!(connect_ws(&handle, stream, HOST, SERVER_PORT, PATH, Limits::default()));
                let polled = connecting.as_mut().poll(&mut std::task::Context::from_waker(std::task::Waker::noop()));
                assert!(
                    matches!(polled, std::task::Poll::Ready(Err(WssError::Config(relay::switchnet::SetupError::ForeignHandle)))),
                    "{:?}",
                    polled.map(|r| r.map(drop))
                );
            });
        },
    );
}

/// A send buffer below the largest control frame (131 B as a client) is refused before any byte,
/// for ws:// and for wss:// (207 B: 131 + the TLS record overhead). Before the fix the first send
/// asked for a frame room's worth (1460 B) and hit switchnet's capacity assert.
#[zhida_test_support::pinned_test]
fn send_buffer_below_the_largest_control_frame_is_refused() {
    for (tls, tx, need) in [(false, 130, 131), (true, 206, 207)] {
        let (_, client_tls) = tls_configs();
        let tap = tap_with_send_buffer(tx);
        until_client_on(&tap, |_| {}, sees_nothing, move |handle, stream| async move {
            let got = if tls {
                connect(&handle, stream, client_tls, HOST, SERVER_PORT, PATH, Limits::default()).await.map(drop)
            } else {
                connect_ws(&handle, stream, HOST, SERVER_PORT, PATH, Limits::default()).await.map(drop)
            };
            let want = relay::switchnet::SetupError::RoomTooSmall { room: tx, need };
            assert!(matches!(got, Err(WssError::Config(e)) if e == want), "tls {tls}: {got:?}");
        });
    }
}

/// At exactly the largest control frame (131 B send buffer) the connection works: the upgrade
/// goes out in pieces, a 125 B ping is answered with its pong, and our close with a 123 B reason
/// goes out whole and is echoed.
#[zhida_test_support::pinned_test]
fn largest_control_frames_fit_a_131_byte_send_buffer() {
    let reason = "r".repeat(123);
    let want_close = close_payload(1000, &reason);
    let ping = vec![7u8; 125];
    let (pong, close) = until_client_on(
        &tap_with_send_buffer(131),
        |_| {},
        {
            let ping = ping.clone();
            move |sock| {
                let mut sock = tungstenite::accept(sock).expect("server WebSocket handshake").into_inner();
                sock.write_all(&[&[0x80 | PING, 125][..], &ping].concat()).unwrap();
                let pong = read_frame(&mut sock);
                let close = read_frame(&mut sock);
                sock.write_all(&[&[0x80 | CLOSE, 125][..], &close.1].concat()).unwrap();
                assert_fin(&mut sock);
                (pong, close)
            }
        },
        move |handle, stream| async move {
            let mut ws = connect_ws(&handle, stream, HOST, SERVER_PORT, PATH, Limits::default()).await.expect("ws connect");
            // the ping is answered inside recv, which then waits for a message: bound it
            assert!(within_rounds(2000, ws.recv()).await.is_none(), "a message besides the ping");
            ws.close(1000, &reason).await.expect("close");
            assert!(ws.recv().await.expect("recv after close").is_none());
            ws
        },
    );
    assert_eq!(pong, (0x80 | 0xa, ping));
    assert_eq!(close, (0x80 | CLOSE, want_close));
}

/// The peer advertises an MSS of 88 B, below the largest control frame (131 B, 207 B with its
/// TLS record): ws:// and wss:// still connect, a message of many segments echoes, and the
/// close handshake completes. Frames are as large as the largest control frame; TCP cuts them.
#[zhida_test_support::pinned_test]
fn ws_and_wss_setup_succeed_with_a_small_peer_mss() {
    let sent: Vec<u8> = (0..3000).map(|i| (i * 31 % 251) as u8).collect();
    for tls in [false, true] {
        let sent = sent.clone();
        let echoed = run(
            |listener| set_maxseg(listener, 88),
            move |stream, config| {
                if tls {
                    echo(upgrade(StreamOwned::new(ServerConnection::new(config).unwrap(), stream)))
                } else {
                    echo(tungstenite::accept(stream).expect("server WebSocket handshake"))
                }
            },
            move |handle, stream, config| async move {
                if tls {
                    let mut wss = connect(&handle, stream, config, HOST, SERVER_PORT, PATH, Limits::default()).await.expect("wss connect");
                    wss.send(Kind::Binary, &sent).await.expect("send");
                    let m = wss.recv().await.expect("recv").expect("the echo");
                    assert_eq!((m.kind(), m.bytes()), (Kind::Binary, &sent[..]));
                    close(&mut wss).await;
                    (Some(wss), None)
                } else {
                    let mut ws = connect_ws(&handle, stream, HOST, SERVER_PORT, PATH, Limits::default()).await.expect("ws connect");
                    ws.send(Kind::Binary, &sent).await.expect("send");
                    let m = ws.recv().await.expect("recv").expect("the echo");
                    assert_eq!((m.kind(), m.bytes()), (Kind::Binary, &sent[..]));
                    ws.close(1000, "").await.expect("send close");
                    assert!(ws.recv().await.expect("recv after close").is_none(), "data after close");
                    (None, Some(ws))
                }
            },
            || {},
        );
        assert_eq!(echoed, 1, "tls {tls}");
    }
}

/// Every frame of the Net taken, so a frame wait cannot end by a frame returning.
async fn take_every_frame(handle: &NetHandle) -> Vec<switchnet::TxFrame> {
    let mut held = Vec::new();
    while let Some(frame) = within_rounds(1, handle.tx_frame()).await {
        held.push(frame);
    }
    assert!(!held.is_empty());
    held
}

/// The peer resets (SO_LINGER 0, then close) once told to.
fn reset_when_told(go: mpsc::Receiver<()>) -> impl FnOnce(std::net::TcpStream) + Send + 'static {
    move |sock| {
        go.recv_timeout(LIMIT).expect("the client's go");
        let linger = libc::linger { l_onoff: 1, l_linger: 0 };
        // SAFETY: a valid socket and a linger option of the right size.
        let rc = unsafe {
            libc::setsockopt(
                sock.as_raw_fd(),
                libc::SOL_SOCKET,
                libc::SO_LINGER,
                (&linger as *const libc::linger).cast(),
                std::mem::size_of::<libc::linger>() as libc::socklen_t,
            )
        };
        assert_eq!(rc, 0, "set SO_LINGER");
        drop(sock);
    }
}

/// With the frame pool empty, connect_ws waits for its first frame; the peer resets, and the
/// wait ends with `Failed(Dead)` (before the fix it waited for a frame forever). With a frame
/// available, setup on a socket about to die may succeed: the death is reported by the first
/// later operation.
#[zhida_test_support::pinned_test]
fn reset_while_setup_waits_for_a_frame_is_dead() {
    let (go_tx, go_rx) = mpsc::channel();
    until_client(|_| {}, reset_when_told(go_rx), move |handle, stream| async move {
        let held = take_every_frame(&handle).await;
        let mut connecting = std::pin::pin!(connect_ws(&handle, stream, HOST, SERVER_PORT, PATH, Limits::default()));
        assert!(within_rounds(50, connecting.as_mut()).await.is_none(), "setup without a frame");
        go_tx.send(()).unwrap();
        let got = within_rounds(20_000, connecting).await.expect("setup still waiting after the reset");
        assert!(matches!(got, Err(WssError::Failed(relay::switchnet::Failure::Dead))), "{:?}", got.map(drop));
        drop(held);
    });
}

/// An open connection whose send waits for a frame (the pool empty) when the peer resets: the
/// send fails instead of waiting forever (before the fix it never returned).
#[zhida_test_support::pinned_test]
fn reset_while_send_waits_for_a_frame_fails_the_send() {
    let (go_tx, go_rx) = mpsc::channel();
    until_client(
        |_| {},
        move |sock| {
            let ws = tungstenite::accept(sock).expect("server WebSocket handshake");
            reset_when_told(go_rx)(ws.into_inner())
        },
        move |handle, stream| async move {
            let mut ws = connect_ws(&handle, stream, HOST, SERVER_PORT, PATH, Limits::default()).await.expect("ws connect");
            let held = take_every_frame(&handle).await;
            {
                let mut sending = std::pin::pin!(ws.send(Kind::Text, b"stuck"));
                assert!(within_rounds(50, sending.as_mut()).await.is_none(), "sent without a frame");
                go_tx.send(()).unwrap();
                let got = within_rounds(20_000, sending).await.expect("send still waiting after the reset");
                assert!(matches!(got, Err(WssError::Failed(relay::switchnet::Failure::Dead))), "{got:?}");
            }
            drop(held);
            ws
        },
    );
}

/// A Text message that is not UTF-8 and an unsendable close are refused with nothing sent; the
/// connection stays usable. Before the fix the invalid text went out and the 124 B reason
/// panicked.
#[zhida_test_support::pinned_test]
fn invalid_text_and_close_are_refused_before_anything_is_sent() {
    let frames = ws_until_client(
        |_| {},
        |mut sock| {
            let text = read_frame(&mut sock);
            let close = read_frame(&mut sock);
            sock.write_all(&server_frame(CLOSE, &close.1)).unwrap();
            assert_fin(&mut sock);
            (text, close)
        },
        |mut ws| async move {
            assert!(matches!(ws.send(Kind::Text, &[b'o', 0xff]).await, Err(WssError::InvalidText)));
            assert!(matches!(ws.close(1000, &"r".repeat(124)).await, Err(WssError::InvalidClose)));
            assert!(matches!(ws.close(1005, "").await, Err(WssError::InvalidClose)));
            ws.send(Kind::Text, b"ok").await.expect("send");
            ws.close(1000, "").await.expect("close");
            assert!(ws.recv().await.expect("recv after close").is_none());
            ws
        },
    );
    assert_eq!(frames.0, (0x80 | TEXT, b"ok".to_vec()));
    assert_eq!(frames.1, (0x80 | CLOSE, close_payload(1000, "")));
}


/// The server ends TLS with close_notify, sends no WebSocket close and keeps TCP open: `recv`
/// fails with `PeerEof` once our close_notify and FIN are out, instead of waiting for a FIN
/// that does not come (before the fix it waited, bounded only by the test).
#[zhida_test_support::pinned_test]
fn close_notify_without_a_close_is_peer_eof() {
    wss_until_client(
        |mut tls| {
            tls.conn.send_close_notify();
            while tls.conn.wants_write() {
                tls.conn.write_tls(&mut tls.sock).unwrap();
            }
            assert_close_notify_then_fin(tls);
        },
        |mut wss| async move {
            let got = within_rounds(20_000, wss.recv()).await.expect("recv waited for a FIN");
            let got = got.map(|m| m.is_some());
            assert!(matches!(got, Err(WssError::Failed(relay::switchnet::Failure::PeerEof))), "{got:?}");
            wss
        },
    );
}

/// Every frame of the Net held by the test: a message of many frames waits for frames, then
/// goes out whole once they come back, and once the connection closed every frame is back
/// for anyone to take.
#[zhida_test_support::pinned_test]
fn a_large_send_waits_for_frames_then_completes() {
    const LEN: usize = 100_000;
    let read = until_client(
        |_| {},
        |sock| {
            let mut ws = tungstenite::accept(sock).expect("server WebSocket handshake");
            let mut read = Vec::new();
            loop {
                match ws.read() {
                    Ok(Message::Close(_)) => {}
                    Ok(m) => read.push(m),
                    Err(WsError::ConnectionClosed) => return read,
                    Err(WsError::Io(e)) if e.kind() == std::io::ErrorKind::Interrupted => {}
                    Err(e) => panic!("server read: {e}"),
                }
            }
        },
        |handle, stream| async move {
            let mut ws = connect_ws(&handle, stream, HOST, SERVER_PORT, PATH, Limits::default()).await.expect("ws connect");
            let held = take_every_frame(&handle).await;
            let total = held.len();
            {
                let mut sending = std::pin::pin!(ws.send(Kind::Binary, &[5; LEN]));
                assert!(within_rounds(50, sending.as_mut()).await.is_none(), "sent without a frame");
                drop(held);
                within_rounds(20_000, sending).await.expect("send stuck after the frames came back").expect("send");
            }
            ws.close(1000, "").await.expect("close");
            assert!(ws.recv().await.expect("recv after close").is_none());
            assert_eq!(take_every_frame(&handle).await.len(), total, "frames not returned");
            ws
        },
    );
    assert_eq!(read, [Message::Binary(vec![5; LEN].into())]);
}

/// The peer resets the connection while every frame of the pool is held: once the client saw
/// the reset (its receive half reports it), an alloc on a sender set up before ends with
/// `InvalidState` at once instead of waiting for a frame that a dead socket can never use.
#[zhida_test_support::pinned_test]
fn alloc_on_a_reset_socket_with_an_empty_pool_returns_invalid_state() {
    use sheath::FrameTx;
    use sheath::switchnet::SwitchTx;
    let (go_tx, go_rx) = mpsc::channel();
    until_client(|_| {}, reset_when_told(go_rx), move |handle, stream| async move {
        let (mut rx, tcp) = stream.split();
        let mut tx = SwitchTx::new(&handle, tcp, 1).await.expect("setup");
        let held = take_every_frame(&handle).await;
        go_tx.send(()).unwrap();
        let seen = within_rounds(20_000, rx.recv()).await.expect("the reset never arrived");
        assert!(seen.is_err(), "data instead of the reset");
        let got = within_rounds(1, tx.alloc()).await.expect("alloc waited");
        assert!(matches!(got, Err(switchnet::SendError::InvalidState)), "{:?}", got.map(drop));
        drop(held);
    });
}

/// The peer resets the connection; once the client saw it (its receive half reports the reset),
/// setting up a sender on the dead socket is `Dead` although a frame is free: the probe frame is
/// taken, then the MSS read finds no connection.
#[zhida_test_support::pinned_test]
fn setup_on_a_reset_socket_with_a_free_frame_is_dead() {
    use sheath::switchnet::{SetupError, SwitchTx};
    let (go_tx, go_rx) = mpsc::channel();
    until_client(|_| {}, reset_when_told(go_rx), move |handle, stream| async move {
        let (mut rx, tcp) = stream.split();
        go_tx.send(()).unwrap();
        let seen = within_rounds(20_000, rx.recv()).await.expect("the reset never arrived");
        assert!(seen.is_err(), "data instead of the reset");
        let got = within_rounds(1, SwitchTx::new(&handle, tcp, 1)).await.expect("the setup waited");
        assert!(matches!(got, Err(SetupError::Dead)), "{:?}", got.map(drop));
    });
}

/// `tls_connect`'s plaintext minimum: 0 counts as one byte (a record's floor), so a 76 B send
/// buffer (one record's overhead, no byte of plaintext) is refused, where it once passed setup
/// and panicked after the handshake; a minimum past `usize::MAX` once its overhead is added is
/// refused too. Nothing reaches the server in either case.
#[zhida_test_support::pinned_test]
fn tls_minimum_zero_or_overflowing_is_refused_before_any_byte() {
    use sheath::switchnet::{Error as TlsError, SetupError, tls_connect};
    for (min_plain, tx, need) in [(0, 76, 77), (usize::MAX, 64 * 1024, usize::MAX)] {
        let (_, client_tls) = tls_configs();
        until_client_on(&tap_with_send_buffer(tx), |_| {}, sees_nothing, move |handle, stream| async move {
            let got = tls_connect(&handle, stream, client_tls, HOST, min_plain).await.map(drop);
            assert!(
                matches!(got, Err(TlsError::Setup(SetupError::RoomTooSmall { need: n, .. })) if n == need),
                "min {min_plain}: {got:?}"
            );
        });
    }
}

/// The smallest send buffer `tls_connect` accepts, 77 B (one byte of plaintext per record): the
/// handshake completes and `close` sends close_notify and FIN, which the server reads as a clean
/// end.
#[zhida_test_support::pinned_test]
fn tls_close_works_with_the_smallest_send_buffer() {
    use sheath::switchnet::tls_connect;
    let (server_tls, client_tls) = tls_configs();
    until_client_on(
        &tap_with_send_buffer(77),
        |_| {},
        move |sock| assert_close_notify_then_fin(StreamOwned::new(ServerConnection::new(server_tls).unwrap(), sock)),
        move |handle, stream| async move {
            let mut tls = tls_connect(&handle, stream, client_tls, HOST, 1).await.expect("handshake");
            tls.close().await.expect("close");
            tls
        },
    );
}

/// The TLS configs with the client's `max_fragment_size` set.
fn tls_configs_with_fragment(max_fragment_size: usize) -> (Arc<ServerConfig>, Arc<ClientConfig>) {
    let (server, client) = tls_configs();
    let mut client = (*client).clone();
    client.max_fragment_size = Some(max_fragment_size);
    (server, Arc::new(client))
}

/// A client whose `max_fragment_size` is 256 (251 B of plaintext per record): each frame is
/// sealed as one record of at most that, and a 1384 B message echoes whole. (Before, a full
/// frame's seal became several records past the frame and panicked.)
#[zhida_test_support::pinned_test]
fn wss_with_a_small_max_fragment_size_echoes() {
    let (server_tls, client_tls) = tls_configs_with_fragment(256);
    let echoed = until_client(
        |_| {},
        move |stream| echo(upgrade(StreamOwned::new(ServerConnection::new(server_tls).unwrap(), stream))),
        move |handle, stream| async move {
            let mut wss = connect(&handle, stream, client_tls, HOST, SERVER_PORT, PATH, Limits::default()).await.expect("wss connect");
            let payload: Vec<u8> = (0..1384).map(|i| i as u8).collect();
            wss.send(Kind::Binary, &payload).await.expect("send");
            let got = wss.recv().await.expect("recv").expect("the echo");
            assert_eq!(got.bytes(), &payload[..]);
            close(&mut wss).await;
            wss
        },
    );
    assert_eq!(echoed, 1);
}

/// A client whose `max_fragment_size` (64: 59 B per record) is below the largest control
/// frame (131 B): refused as `Config(RoomTooSmall)` before the ClientHello. (Before, the
/// handshake ran and the connection panicked when set up.)
#[zhida_test_support::pinned_test]
fn wss_with_a_fragment_limit_below_a_control_frame_is_refused() {
    let (_, client_tls) = tls_configs_with_fragment(64);
    until_client(|_| {}, sees_nothing, move |handle, stream| async move {
        let got = connect(&handle, stream, client_tls, HOST, SERVER_PORT, PATH, Limits::default()).await.map(drop);
        let want = relay::switchnet::SetupError::RoomTooSmall { room: 59 + 76, need: 131 + 76 };
        assert!(matches!(got, Err(WssError::Config(e)) if e == want), "{got:?}");
    });
}

/// The server sends a message and its FIN (then, if `reset`, a reset at once, with no
/// WebSocket close), and the client reads only after the Net took all of it in: by `recv`
/// (the message, then the outcome) or by `linger`.
///
/// The barrier, with a reset: the client reads only once its socket reports the peer's reset
/// (the Net has processed every segment before it too). Without one: once the server
/// returned after its FIN, and the Net turned since.
fn fin_then(reset: bool, lingering: bool) -> Result<(), WssError> {
    let (done_tx, done_rx) = mpsc::channel::<()>();
    let outcome = Rc::new(RefCell::new(None));
    let seen = outcome.clone();
    let taken_in = Rc::new(Cell::new(false));
    let client_taken_in = taken_in.clone();
    let mut rounds = 0;
    let mut told = false;
    until_client_watching(
        &TAP,
        |_| {},
        move |stream| {
            let mut sock = tungstenite::accept(stream).expect("server WebSocket handshake").into_inner();
            sock.write_all(&server_frame(TEXT, b"last")).unwrap();
            sock.shutdown(std::net::Shutdown::Write).unwrap();
            if reset {
                let (go_tx, go_rx) = mpsc::channel();
                go_tx.send(()).unwrap();
                reset_when_told(go_rx)(sock);
            } else {
                done_tx.send(()).unwrap();
            }
        },
        move |handle, stream| async move {
            let mut ws = connect_ws(&handle, stream, HOST, SERVER_PORT, PATH, Limits::default()).await.expect("ws connect");
            while !client_taken_in.get() {
                assert!(within_rounds(1, std::future::pending::<()>()).await.is_none());
            }
            let got = if lingering {
                ws.linger().await
            } else {
                let m = ws.recv().await.expect("the message").expect("a message, not the end");
                assert_eq!(m.text(), Some("last"));
                ws.recv().await.map(|m| assert!(m.is_none(), "a second message"))
            };
            *seen.borrow_mut() = Some(got);
            ws
        },
        move |net| {
            if taken_in.get() {
                return;
            }
            if reset {
                rounds += 1;
                assert!(rounds < 100_000, "the peer's reset never reached the client's socket");
                taken_in.set(net.tcp_reset_by_peer());
            } else {
                // a turn after the server's FIN was sent
                taken_in.set(told);
                told |= done_rx.try_recv().is_ok();
            }
        },
    );
    outcome.take().expect("the client ran")
}

/// The server's FIN alone, no close: recv gives the message, then `PeerEof`; linger ends
/// clean. (Unchanged.)
#[zhida_test_support::pinned_test]
fn peer_fin_without_a_close() {
    assert!(matches!(fin_then(false, false), Err(WssError::Failed(relay::switchnet::Failure::PeerEof))));
    assert!(matches!(fin_then(false, true), Ok(())));
}

/// The server's FIN then a reset, no close: recv gives the message, then `Failed(Dead)`, and
/// linger is `Failed(Dead)`: a reset before the peer's close. (Before, switchnet reported the
/// accepted FIN and hid the reset: PeerEof and Ok.)
#[zhida_test_support::pinned_test]
fn peer_fin_then_reset_without_a_close_is_dead() {
    for lingering in [false, true] {
        let got = fin_then(true, lingering);
        assert!(matches!(got, Err(WssError::Failed(relay::switchnet::Failure::Dead))), "lingering {lingering}: {got:?}");
    }
}

/// A ws:// client whose `max_message` is 1 MiB: a frame header advertising 16 MiB, followed by
/// one byte, fails the connection with `MessageTooBig` at the header, and FIN goes out with no
/// close frame.
#[zhida_test_support::pinned_test]
fn a_message_over_the_configured_limit_fails_at_its_header() {
    let limits = Limits { max_message: 1 << 20, ..Limits::default() };
    let rest = until_client(
        |_| {},
        |sock| {
            let mut sock = tungstenite::accept(sock).expect("server WebSocket handshake").into_inner();
            let mut frame = vec![0x82, 127];
            frame.extend_from_slice(&(16u64 << 20).to_be_bytes());
            frame.push(7);
            sock.write_all(&frame).unwrap();
            let mut rest = Vec::new();
            sock.read_to_end(&mut rest).expect("FIN, not RST");
            rest
        },
        move |handle, stream| async move {
            let mut ws = connect_ws(&handle, stream, HOST, SERVER_PORT, PATH, limits).await.expect("ws connect");
            let got = ws.recv().await.map(|m| m.is_some());
            assert!(
                matches!(got, Err(WssError::Failed(relay::switchnet::Failure::Protocol(relay::ProtocolError::MessageTooBig)))),
                "{got:?}"
            );
            ws
        },
    );
    assert_eq!(rest, b"", "a close frame after a protocol error");
}
