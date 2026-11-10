//! Both roles on a net_tap Net against kernel peers on std threads: a tungstenite or raw TCP
//! client of the server role, a fake fuzzingserver for the client role. Needs CAP_NET_ADMIN and
//! /dev/net/tun (the Docker test container).

use std::io::{ErrorKind, Read, Write};
use std::net::{Shutdown, SocketAddrV4, TcpListener, TcpStream};
use std::os::fd::AsRawFd;
use std::panic::resume_unwind;
use std::thread;

use switchnet::testkit::{KERNEL_ADDR, TapConfig, ZHIDA_ADDR, with_tap_net};
use tungstenite::handshake::server::{Request, Response};
use tungstenite::{Error as TError, Message, WebSocket};

use super::*;

/// Every peer socket read and write, and the peer's wait for the server role's listener.
const LIMIT: Duration = Duration::from_secs(10);
/// The deadline in the tests that expect it to fire; `DEADLINE` elsewhere.
const SHORT: Duration = Duration::from_secs(1);

/// Runs `role` on a fresh Net until `peer` (on its own thread) returned; the run's result and
/// what the peer returned. A run that failed first keeps turning the Net, so the peer sees the
/// FIN or RST of the connections it dropped.
fn run<T: Send + 'static>(role: Role, deadline: Duration, peer: impl FnOnce() -> T + Send + 'static) -> (Result<(), Failure>, T) {
    let tap = TapConfig { name: "autobahn", socket: SOCKET, max_sockets: role.max_sockets(), random_seed: 1 };
    with_tap_net(&tap, |net, clock| {
        let peer = thread::spawn(peer);
        let result = drive(net, &|| clock.elapsed(), &mut || peer.is_finished(), &role, deadline);
        while !peer.is_finished() {
            net.turn(clock.now()).expect("turn the Net");
            thread::sleep(Duration::from_micros(50));
        }
        match peer.join() {
            Ok(saw) => (result, saw),
            Err(panic) => resume_unwind(panic),
        }
    })
}

/// A kernel TCP connection to the server role, retried while its listener is not open yet.
fn connect() -> TcpStream {
    let start = std::time::Instant::now();
    loop {
        match TcpStream::connect_timeout(&SocketAddrV4::new(ZHIDA_ADDR, ECHO_PORT).into(), LIMIT) {
            Ok(s) => {
                s.set_read_timeout(Some(LIMIT)).unwrap();
                s.set_write_timeout(Some(LIMIT)).unwrap();
                return s;
            }
            Err(e) if e.kind() == ErrorKind::ConnectionRefused && start.elapsed() < LIMIT => {
                thread::sleep(Duration::from_millis(10));
            }
            Err(e) => panic!("connect to the server role: {e}"),
        }
    }
}

fn ws_client() -> WebSocket<TcpStream> {
    tungstenite::client(format!("ws://{ZHIDA_ADDR}:{ECHO_PORT}/"), connect()).expect("upgrade").0
}

/// A raw connection past the upgrade: the request sent, the 101 read through its blank line.
fn raw_client() -> TcpStream {
    let mut s = connect();
    s.write_all(
        b"GET / HTTP/1.1\r\nHost: zhida\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\
          Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n",
    )
    .unwrap();
    let mut head = Vec::new();
    let mut byte = [0];
    while !head.ends_with(b"\r\n\r\n") {
        s.read_exact(&mut byte).expect("the 101 response");
        head.push(byte[0]);
    }
    assert!(head.starts_with(b"HTTP/1.1 101 "), "{}", String::from_utf8_lossy(&head));
    s
}

/// A masked client frame (key 0: the payload as is), FIN set, `first` the opcode byte.
fn masked(first: u8, payload: &[u8]) -> Vec<u8> {
    assert!(payload.len() < 126);
    [&[first, 0x80 | payload.len() as u8, 0, 0, 0, 0][..], payload].concat()
}

/// `ws.read()`, again after EINTR (a blocking socket read returns it when a signal arrives;
/// tungstenite keeps its partial frame, so reading again continues it).
fn read(ws: &mut WebSocket<TcpStream>) -> Result<Message, TError> {
    loop {
        match ws.read() {
            Err(TError::Io(e)) if e.kind() == ErrorKind::Interrupted => {}
            r => return r,
        }
    }
}

/// `ws.send(m)`, flushing again after EINTR (the message stays buffered).
fn send(ws: &mut WebSocket<TcpStream>, m: Message) -> Result<(), TError> {
    let mut sent = ws.send(m);
    while matches!(&sent, Err(TError::Io(e)) if e.kind() == ErrorKind::Interrupted) {
        sent = ws.flush();
    }
    sent
}

/// Reads the close reply (`Message::Close`), then the end of the WebSocket.
fn close_handshake(ws: &mut WebSocket<TcpStream>) {
    ws.close(None).unwrap();
    loop {
        match read(ws) {
            Ok(Message::Close(_)) => {}
            Ok(m) => panic!("after close: {m:?}"),
            Err(TError::ConnectionClosed) => return,
            Err(e) => panic!("after close: {e}"),
        }
    }
}

/// The TCP end of stream, with nothing before it.
fn assert_fin(s: &mut TcpStream) {
    let mut rest = Vec::new();
    s.read_to_end(&mut rest).expect("FIN, not RST");
    assert_eq!(rest, b"", "bytes before FIN");
}

/// Text and Binary are echoed and a Ping answered; after the close handshake the server ends
/// TCP first: the client reads FIN without having closed.
#[zhida_test_support::pinned_test]
fn server_echoes_answers_ping_and_fins_first() {
    let (result, ()) = run(Role::Server { connections: 1 }, DEADLINE, || {
        let mut ws = ws_client();
        send(&mut ws, Message::text("hi")).unwrap();
        send(&mut ws, Message::binary(vec![1, 2, 3])).unwrap();
        send(&mut ws, Message::Ping(b"p".to_vec().into())).unwrap();
        assert_eq!(read(&mut ws).unwrap(), Message::text("hi"));
        assert_eq!(read(&mut ws).unwrap(), Message::binary(vec![1, 2, 3]));
        assert_eq!(read(&mut ws).unwrap(), Message::Pong(b"p".to_vec().into()));
        close_handshake(&mut ws);
        assert_fin(ws.get_mut());
    });
    result.unwrap();
}

/// The client sends 1 MiB, its Close and FIN through a 4 KiB receive buffer and reads nothing
/// for a while, so the echo blocks with Close and FIN already received. The server finishes the
/// echo, then replies to the Close, then sends FIN.
#[zhida_test_support::pinned_test]
fn close_reply_sent_before_fin_under_backpressure() {
    let (result, ()) = run(Role::Server { connections: 1 }, DEADLINE, || {
        let mut ws = ws_client();
        let size: libc::c_int = 4096;
        // SAFETY: a valid socket and an int option of the right size.
        let rc = unsafe {
            libc::setsockopt(
                ws.get_ref().as_raw_fd(),
                libc::SOL_SOCKET,
                libc::SO_RCVBUF,
                (&size as *const libc::c_int).cast(),
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            )
        };
        assert_eq!(rc, 0, "set SO_RCVBUF");
        let big: Vec<u8> = (0..1 << 20).map(|i| (i * 31 % 251) as u8).collect();
        send(&mut ws, Message::binary(big.clone())).unwrap();
        ws.close(None).unwrap();
        ws.get_ref().shutdown(Shutdown::Write).unwrap();
        thread::sleep(Duration::from_millis(300));
        assert_eq!(read(&mut ws).unwrap(), Message::binary(big));
        assert!(matches!(read(&mut ws).unwrap(), Message::Close(_)));
        assert!(matches!(read(&mut ws), Err(TError::ConnectionClosed)));
        assert_fin(ws.get_mut());
    });
    result.unwrap();
}

/// A frame with RSV1 set, after a valid one in the same segment and followed by more bytes:
/// the valid one is echoed, then the server fails the connection with FIN and no close frame.
/// Bytes the client sends after that FIN are read and dropped until the client's own FIN, so
/// the connection ends without RST.
#[zhida_test_support::pinned_test]
fn protocol_error_ends_with_fin_and_no_close_frame() {
    let (result, ()) = run(Role::Server { connections: 1 }, DEADLINE, || {
        let mut s = raw_client();
        s.write_all(&[masked(0x81, b"ok"), masked(0xc1, b""), vec![0x55; 65]].concat()).unwrap();
        let mut echo = [0; 4];
        s.read_exact(&mut echo).unwrap();
        assert_eq!(echo, [0x81, 2, b'o', b'k']);
        assert_fin(&mut s);
        late_bytes_then_fin_end_without_rst(&mut s);
    });
    result.unwrap();
}

/// After the other side's FIN: more bytes, then our FIN; the stream must end, not reset (a
/// socket dropped with those bytes unread, or gone, would answer them with RST).
fn late_bytes_then_fin_end_without_rst(s: &mut TcpStream) {
    s.write_all(&[0x55; 100]).unwrap();
    thread::sleep(Duration::from_millis(200));
    s.shutdown(Shutdown::Write).unwrap();
    thread::sleep(Duration::from_millis(200));
    let mut byte = [0];
    assert_eq!(s.read(&mut byte).expect("the end, not a reset"), 0);
}

/// A close with a status code a peer may not send (999) still gets a close reply, with 1002.
#[zhida_test_support::pinned_test]
fn disallowed_close_code_is_answered_with_1002() {
    let (result, ()) = run(Role::Server { connections: 1 }, DEADLINE, || {
        let mut s = raw_client();
        s.write_all(&masked(0x88, &999u16.to_be_bytes())).unwrap();
        let mut reply = Vec::new();
        s.read_to_end(&mut reply).expect("FIN, not RST");
        assert_eq!(reply, [0x88, 2, 0x03, 0xea]);
    });
    result.unwrap();
}

/// Twenty connections, each opened right after the previous one's close handshake: each finds
/// a listener, and each leaves a TIME-WAIT socket on Zhida, which max_sockets covers.
#[zhida_test_support::pinned_test]
fn sequential_connections_are_each_accepted() {
    let (result, ()) = run(Role::Server { connections: 20 }, DEADLINE, || {
        for i in 0..20 {
            let mut ws = ws_client();
            send(&mut ws, Message::text(format!("{i}"))).unwrap();
            assert_eq!(read(&mut ws).unwrap(), Message::text(format!("{i}")));
            close_handshake(&mut ws);
            assert_fin(ws.get_mut());
        }
    });
    result.unwrap();
}

/// The tester exits without connecting: the accepted count is wrong.
#[zhida_test_support::pinned_test]
fn missing_connection_is_an_error() {
    let (result, ()) = run(Role::Server { connections: 1 }, DEADLINE, || {});
    assert!(matches!(result, Err(Failure::Accepted { got: 0, want: 1 })), "{result:?}");
}

/// The deadline ends the run for a connection stalled in its upgrade, and for an open one.
#[zhida_test_support::pinned_test]
fn server_deadline_expires_in_upgrade_and_open() {
    let stall = |mut s: TcpStream| {
        let mut rest = Vec::new();
        let _ = s.read_to_end(&mut rest);
    };
    let (result, ()) = run(Role::Server { connections: 1 }, SHORT, move || {
        let mut s = connect();
        s.write_all(b"GET / HTTP/1.1\r\n").unwrap();
        stall(s);
    });
    assert!(matches!(result, Err(Failure::Connection { n: 0, why: Why::Deadline(SHORT) })), "{result:?}");
    let (result, ()) = run(Role::Server { connections: 1 }, SHORT, move || stall(raw_client()));
    assert!(matches!(result, Err(Failure::Connection { n: 0, why: Why::Deadline(SHORT) })), "{result:?}");
}

/// One connection's request path and the local port it came from.
type Seen = (String, u16);

/// A fake fuzzingserver on 192.0.2.1:9001: writes `run`/server-ready once it listens, then
/// accepts `connections` upgrades and hands each to `serve` with its index. Then it stays up as
/// fuzzingserver does until peer.sh stops it: with `until_done` until `run`/client-done
/// exists, else for 1 s, which a failing client uses to fail.
fn fuzzingserver(
    run: PathBuf,
    connections: usize,
    until_done: bool,
    mut serve: impl FnMut(usize, WebSocket<TcpStream>) + Send + 'static,
) -> impl FnOnce() -> Vec<Seen> + Send + 'static {
    move || {
        let listener = TcpListener::bind(SocketAddrV4::new(KERNEL_ADDR, FUZZINGSERVER_PORT)).expect("bind");
        write_marker(&run.join("server-ready")).unwrap();
        let seen = (0..connections)
            .map(|i| {
                let (s, from) = listener.accept().expect("accept");
                s.set_read_timeout(Some(LIMIT)).unwrap();
                s.set_write_timeout(Some(LIMIT)).unwrap();
                let mut path = String::new();
                let ws = tungstenite::accept_hdr(s, |r: &Request, response: Response| {
                    path = r.uri().to_string();
                    Ok(response)
                })
                .expect("upgrade");
                serve(i, ws);
                (path, from.port())
            })
            .collect();
        let start = std::time::Instant::now();
        if !until_done {
            thread::sleep(Duration::from_secs(1));
        }
        while until_done && !run.join("client-done").exists() {
            assert!(start.elapsed() < LIMIT, "no client-done");
            thread::sleep(Duration::from_millis(10));
        }
        seen
    }
}

/// A fresh marker directory for a client-role test.
fn run_dir(test: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("relay-autobahn-{test}-{}", process::id()));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    dir
}

/// fuzzingserver's side of getCaseCount: the count, then its Close.
fn count(ws: &mut WebSocket<TcpStream>, n: &str) {
    send(ws, Message::text(n)).unwrap();
    close_handshake(ws);
}

/// fuzzingserver's side of updateReports: the client's Close, answered.
fn reports(ws: &mut WebSocket<TcpStream>) {
    assert!(matches!(read(ws).unwrap(), Message::Close(_)));
    assert!(matches!(read(ws), Err(TError::ConnectionClosed)));
}

/// A case: a Text and a Binary echoed exactly, then fuzzingserver's Close.
fn case(ws: &mut WebSocket<TcpStream>, i: usize) {
    send(ws, Message::text(format!("case {i}"))).unwrap();
    send(ws, Message::binary(vec![i as u8; 300])).unwrap();
    assert_eq!(read(ws).unwrap(), Message::text(format!("case {i}")));
    assert_eq!(read(ws).unwrap(), Message::binary(vec![i as u8; 300]));
    close_handshake(ws);
}

/// The whole sequence, each connection from the next local port, as agent `relay`; then the
/// client-done marker (the peer waits for it).
#[zhida_test_support::pinned_test]
fn client_runs_the_sequence_from_distinct_local_ports() {
    let dir = run_dir("sequence");
    let role = Role::Client { cases: 2, run: dir.clone() };
    let peer = fuzzingserver(dir.clone(), 4, true, |i, mut ws| match i {
        0 => count(&mut ws, "2"),
        3 => reports(&mut ws),
        i => case(&mut ws, i),
    });
    let (result, seen) = run(role, DEADLINE, peer);
    result.unwrap();
    let want = [
        "/getCaseCount".to_owned(),
        "/runCase?case=1&agent=relay".to_owned(),
        "/runCase?case=2&agent=relay".to_owned(),
        "/updateReports?agent=relay".to_owned(),
    ];
    assert_eq!(seen, want.into_iter().zip(FIRST_LOCAL_PORT..).collect::<Vec<_>>());
}

/// Case 1's server sends a frame with RSV1 set: the client fails the connection with FIN (no
/// close frame) before the server closes, so that socket stays in TIME-WAIT; with max_sockets =
/// cases + 2, case 2 still gets a socket and runs. The server's bytes after that FIN are read
/// and dropped until its FIN: no RST.
#[zhida_test_support::pinned_test]
fn client_protocol_error_fins_first_and_the_next_case_runs() {
    let dir = run_dir("protocol");
    let role = Role::Client { cases: 2, run: dir.clone() };
    let peer = fuzzingserver(dir, 4, true, |i, mut ws| match i {
        0 => count(&mut ws, "2"),
        1 => {
            ws.get_mut().write_all(&[0xc1, 0]).unwrap();
            assert_fin(ws.get_mut());
            late_bytes_then_fin_end_without_rst(ws.get_mut());
        }
        3 => reports(&mut ws),
        i => case(&mut ws, i),
    });
    let (result, _) = run(role, DEADLINE, peer);
    result.unwrap();
}

/// Each failure of the client's sequence ends the run.
#[zhida_test_support::pinned_test]
fn client_sequence_failures_are_errors() {
    let fails = |test: &str, cases: usize, connections: usize, serve: fn(usize, WebSocket<TcpStream>)| {
        let dir = run_dir(test);
        let (result, _) = run(Role::Client { cases, run: dir.clone() }, SHORT, fuzzingserver(dir, connections, false, serve));
        match result {
            Err(Failure::Connection { n, why }) => (n, why),
            other => panic!("{test}: {other:?}"),
        }
    };
    // the failing peers ignore errors: the client drops the connection once it fails
    let mismatch = fails("mismatch", 2, 1, |_, mut ws| {
        let _ = send(&mut ws, Message::text("3"));
        let _ = read(&mut ws);
    });
    assert!(matches!(mismatch, (0, Why::CaseCount(Some(3)))), "{mismatch:?}");
    let missing = fails("missing", 1, 1, |_, mut ws| {
        let _ = ws.close(None);
        while read(&mut ws).is_ok() {}
    });
    assert!(matches!(missing, (0, Why::NoCaseCount)), "{missing:?}");
    let extra = fails("extra", 1, 1, |_, mut ws| {
        let _ = send(&mut ws, Message::text("1"));
        let _ = send(&mut ws, Message::text("more"));
        while read(&mut ws).is_ok() {}
    });
    assert!(matches!(extra, (0, Why::Unexpected { kind: Kind::Text, len: 4 })), "{extra:?}");
    let no_reply = fails("noreply", 1, 3, |i, mut ws| match i {
        0 => count(&mut ws, "1"),
        1 => case(&mut ws, 1),
        _ => {
            let mut close = [0; 8];
            let _ = ws.get_mut().read_exact(&mut close);
            let _ = ws.get_mut().shutdown(Shutdown::Both);
        }
    });
    assert!(matches!(no_reply, (2, Why::NoCloseReply(Error::Failed(WsFailure::PeerEof)))), "{no_reply:?}");
    let stalled = fails("stalled", 1, 1, |_, mut ws| {
        let _ = read(&mut ws);
    });
    assert!(matches!(stalled, (0, Why::Deadline(SHORT))), "{stalled:?}");
}

/// No listener (the SYN gets RST), and a 404 for the upgrade: both end the run.
#[zhida_test_support::pinned_test]
fn client_connect_and_upgrade_failures_are_errors() {
    let dir = run_dir("refused");
    let marker = dir.clone();
    let (result, ()) = run(Role::Client { cases: 1, run: dir }, DEADLINE, move || {
        write_marker(&marker.join("server-ready")).unwrap();
        thread::sleep(Duration::from_secs(2));
    });
    assert!(matches!(result, Err(Failure::Connection { n: 0, why: Why::Open(NetError::Refused) })), "{result:?}");

    let dir = run_dir("404");
    let marker = dir.clone();
    let (result, ()) = run(Role::Client { cases: 1, run: dir }, DEADLINE, move || {
        let listener = TcpListener::bind(SocketAddrV4::new(KERNEL_ADDR, FUZZINGSERVER_PORT)).expect("bind");
        write_marker(&marker.join("server-ready")).unwrap();
        let (mut s, _) = listener.accept().unwrap();
        s.set_read_timeout(Some(LIMIT)).unwrap();
        let mut request = [0; 1024];
        let _ = s.read(&mut request).unwrap();
        s.write_all(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n").unwrap();
        thread::sleep(Duration::from_secs(1));
    });
    let refused = matches!(
        result,
        Err(Failure::Connection { n: 0, why: Why::Upgrade(Error::Upgrade(relay::UpgradeError::Status(Some(404)))) })
    );
    assert!(refused, "{result:?}");
}

/// The tester exits before server-ready exists: the sequence did not complete.
#[zhida_test_support::pinned_test]
fn client_tester_exit_before_server_ready_is_an_error() {
    let (result, ()) = run(Role::Client { cases: 1, run: run_dir("early") }, DEADLINE, || {});
    assert!(matches!(result, Err(Failure::Incomplete { started: 0, of: 3 })), "{result:?}");
}

#[test]
fn parse_reads_both_roles_and_bounds_the_cases() {
    let args = |s: &str| s.split(' ').map(OsString::from).collect::<Vec<_>>();
    let (role, tester) = parse(&args("server --expect-connections 517 -- t a")).unwrap();
    assert_eq!((role, tester), (Role::Server { connections: 517 }, args("t a")));
    let (role, _) = parse(&args("client --expect-cases 16382 -- t")).unwrap();
    assert_eq!(role, Role::Client { cases: 16382, run: PathBuf::from(CLIENT_RUN) });
    assert_eq!(role.max_sockets(), 16384);
    for bad in [
        "client --expect-cases 16383 -- t",
        "client --expect-connections 1 -- t",
        "server --expect-connections 0 -- t",
        "server --expect-connections 1 t",
        "server --expect-connections 1 --",
        "both --expect-cases 1 -- t",
    ] {
        assert!(parse(&args(bad)).is_err(), "{bad}");
    }
}
