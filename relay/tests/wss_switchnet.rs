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

use relay::Kind;
use relay::switchnet::{Error as WssError, Wss, connect, connect_ws};
use rustls::pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer};
use rustls::version::TLS13;
use rustls::{ClientConfig, RootCertStore, ServerConfig, ServerConnection, StreamOwned};
use switchnet::testkit::{TapConfig, client_server};
use switchnet::{NetHandle, TcpStream, tcp};
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
fn assert_close_notify_then_fin(ws: ServerWs) {
    let mut tls = ws.into_inner();
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
            let wss = connect(&handle, stream, config, HOST, SERVER_PORT, PATH).await.expect("wss connect");
            client(wss).await
        },
        each_round,
    )
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
/// returns the error after the alert went out, so the client may drop the `Wss` at once.
#[zhida_test_support::pinned_test]
fn wss_receive_error_sends_the_alert_before_the_rst() {
    let seen = run(
        |_| {},
        |stream, config| {
            let mut ws = upgrade(StreamOwned::new(ServerConnection::new(config).unwrap(), stream));
            let tls = ws.get_mut();
            let mut junk = vec![0x17, 0x03, 0x03, 0x00, 0x20];
            junk.resize(junk.len() + 32 + TRAILING, 0);
            tls.sock.write_all(&junk).unwrap();
            read_alert(&mut tls.conn, &mut tls.sock)
        },
        |handle, stream, config| async move {
            let mut wss = connect(&handle, stream, config, HOST, SERVER_PORT, PATH).await.expect("wss connect");
            match wss.recv().await {
                Err(WssError::Tls(sheath::Error::Tls(rustls::Error::DecryptError))) => {}
                Err(e) => panic!("unexpected error {e}"),
                Ok(_) => panic!("a message from a corrupted record"),
            }
        },
        || {},
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
            let mut ws = connect_ws(&handle, stream, HOST, SERVER_PORT, PATH).await.expect("ws connect");
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
