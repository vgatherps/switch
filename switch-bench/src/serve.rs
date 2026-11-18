//! The switch echo DUT (UDP, TCP, ws). The busy-poll loop does no syscall while serving.

use switch_runtime::{SpawnError, Spawner};
use switch_net::{
    Error, IpListenEndpoint, NetHandle, TcpListener, TcpStream, UdpConfig, UdpRx, UdpTx, tcp,
};
use switch_open::Sizing;

use crate::host::{self, Backend, Host};
use crate::load::{Proto, TCP_BUFFER};
use switch_ws::Limits;
use switch_ws::net::{Ws, accept_ws};

#[derive(Debug, Clone)]
pub struct Args {
    pub backend: Backend,
    pub proto: Proto,
    pub port: u16,
}

const UDP_QUEUE: usize = 4096;
/// TCP: connections served at once, and the listener's backlog.
const MAX_CONNS: usize = 256;
const BACKLOG: usize = 64;

pub fn run(args: &Args) -> Result<(), String> {
    let tcp_config = tcp::Config {
        rx_capacity: TCP_BUFFER,
        tx_capacity: TCP_BUFFER,
        mss_cap: None,
    };
    let sockets = socket_slots(args.proto);
    host::catch_stop()?;
    let mut host = Host::connect(&args.backend, sockets, |mtu| match args.proto {
        Proto::Udp => Sizing {
            frame_share: 64,
            max_sockets: 1,
            max_udp_sockets: 1,
            rx_staging: 128,
            tx_pending: 64,
            neighbor_capacity: 256,
        },
        Proto::Tcp | Proto::Ws => tcp_sizing(sockets, &tcp_config, mtu),
    })?;
    host.start_xstats("serve");
    let handle = host.net.handle();
    let startup_error = match args.proto {
        Proto::Udp => {
            udp_echo(&mut host, &handle, args.port)?;
            None
        }
        Proto::Tcp | Proto::Ws => {
            let startup_error = std::rc::Rc::new(std::cell::RefCell::new(None));
            spawn_accept_loop(
                &host.rt.spawner(), &handle, args.port, tcp_config,
                args.proto == Proto::Ws, BACKLOG, Some(startup_error.clone()),
            )?;
            host.round()?;
            let error = startup_error.borrow_mut().take();
            if let Some(error) = error {
                return Err(format!("tcp accept: {error:?}"));
            }
            Some(startup_error)
        }
    };
    eprintln!(
        "switch-bench serve: {} echo on port {}",
        args.proto.name(),
        args.port
    );
    while !host::stopped() {
        host.round()?;
        if let Some(error) = startup_error.as_ref().and_then(|error| error.borrow_mut().take()) {
            return Err(format!("tcp accept: {error:?}"));
        }
    }
    drop(handle);
    host.close()
}

fn socket_slots(proto: Proto) -> usize {
    match proto {
        Proto::Udp => 1,
        Proto::Tcp | Proto::Ws => MAX_CONNS + BACKLOG,
    }
}

fn tcp_sizing(sockets: usize, tcp_config: &tcp::Config, mtu: usize) -> Sizing {
    Sizing {
        frame_share: sockets * tcp::tx_mbuf_budget(tcp_config, mtu) + 64,
        max_sockets: sockets,
        max_udp_sockets: 0,
        rx_staging: 128,
        tx_pending: 64,
        neighbor_capacity: 256,
    }
}

fn spawn_error<T>(e: SpawnError<T>) -> String {
    format!("spawn: {:?}", e.reason)
}

/// Binds `port` and echoes every datagram to its source.
fn udp_echo(host: &mut Host, handle: &NetHandle, port: u16) -> Result<(), String> {
    let config = UdpConfig {
        port,
        local_addr: None,
        rx_queue: UDP_QUEUE,
        tx_checksum: true,
        broadcast: false,
    };
    let bound = std::rc::Rc::new(std::cell::RefCell::new(None));
    host.rt
        .spawner()
        .spawn(bind_echo(handle.clone(), config, bound.clone()))
        .map_err(spawn_error)?;
    while bound.borrow().is_none() {
        host.round()?;
    }
    let result = bound.borrow_mut().take().expect("binding completed");
    let (rx, tx) = result.map_err(|e| format!("udp bind: {e:?}"))?.split();
    host.rt
        .spawner()
        .spawn(echo_datagrams(handle.clone(), rx, tx))
        .map_err(spawn_error)
}

#[switch_runtime::task]
async fn bind_echo(
    handle: NetHandle,
    config: UdpConfig,
    bound: std::rc::Rc<std::cell::RefCell<Option<Result<switch_net::UdpSocket, Error>>>>,
) {
    *bound.borrow_mut() = Some(handle.udp_bind(config).await);
}

#[switch_runtime::task]
async fn echo_datagrams(handle: NetHandle, mut rx: UdpRx, mut tx: UdpTx) {
    loop {
        let datagram = rx.recv().await;
        let src = datagram.src();
        let mut frame = handle.tx_frame().await;
        let len = datagram.payload().len();
        frame.as_mut()[..len].copy_from_slice(datagram.payload());
        frame.truncate(len);
        let mut frame = frame.freeze();
        drop(datagram);
        while let Err((
            back,
            Error::Full | Error::NeighborPending,
        )) = tx.send(Some(src), frame)
        {
            frame = back;
            switch_runtime::yield_now().await;
        }
    }
}

/// Opens a listener of `backlog` sockets on `port` and spawns its accept loop.
fn spawn_accept_loop(
    spawner: &Spawner,
    handle: &NetHandle,
    port: u16,
    config: tcp::Config,
    ws: bool,
    backlog: usize,
    startup_error: Option<std::rc::Rc<std::cell::RefCell<Option<Error>>>>,
) -> Result<(), String> {
    let local = IpListenEndpoint { addr: None, port };
    let listener = handle
        .tcp_listen(config, local, backlog)
        .map_err(|error| format!("tcp listen: {error:?}"))?;
    spawner
        .spawn(accept_loop(listener, spawner.clone(), handle.clone(), port, ws, startup_error))
        .map_err(spawn_error)
}

/// Hands each connection the listener accepts to its own echo task. While the listener has no
/// socket left (every socket slot is in use) it retries each round.
#[switch_runtime::task]
async fn accept_loop(
    mut listener: TcpListener,
    spawner: Spawner,
    handle: NetHandle,
    port: u16,
    ws: bool,
    startup_error: Option<std::rc::Rc<std::cell::RefCell<Option<Error>>>>,
) {
    loop {
        let stream = match listener.accept().await {
            Ok(stream) => stream,
            Err(Error::SocketsFull) => {
                switch_runtime::yield_now().await;
                continue;
            }
            Err(error) => {
                if let Some(startup_error) = startup_error.as_ref() {
                    *startup_error.borrow_mut() = Some(error);
                }
                eprintln!("switch-bench serve: tcp accept on {port}: {error:?}");
                return;
            }
        };
        if let Err(e) = spawner.spawn(serve_conn(handle.clone(), stream, ws)) {
            eprintln!("switch-bench serve: {}", spawn_error(e));
        }
    }
}

/// Echoes one accepted connection.
#[switch_runtime::task]
async fn serve_conn(handle: NetHandle, stream: TcpStream, ws: bool) {
    if ws {
        match accept_ws(&handle, stream, Limits::default()).await {
            Ok(ws) => ws_echo(ws).await,
            Err(e) => eprintln!("switch-bench serve: ws upgrade: {e}"),
        }
    } else {
        tcp_echo(&handle, stream).await;
    }
}


/// Echoes all bytes received from a TCP connection.
async fn tcp_echo(handle: &NetHandle, stream: TcpStream) {
    let (mut rx, mut tx) = stream.split();
    loop {
        let Ok(received) = rx.recv().await else {
            return;
        };
        let payload = received.payload();
        let mut rest = payload;
        while !rest.is_empty() {
            let mut frame = handle.tx_frame().await;
            let len = rest.len().min(frame.as_mut().len());
            frame.as_mut()[..len].copy_from_slice(&rest[..len]);
            frame.truncate(len);
            let mut frame = frame.freeze();
            rest = &rest[len..];
            loop {
                match tx.try_send(frame) {
                    Ok(()) => break,
                    Err((back, Error::Full)) => {
                        frame = back;
                        if tx.room(len).await.is_err() {
                            return;
                        }
                    }
                    Err(_) => return,
                }
            }
        }
    }
}

/// One ws connection of the DUT: every message back with its kind, through switch-ws. Returns when
/// the peer closes or fails the connection.
async fn ws_echo(mut ws: Ws) {
    loop {
        let (message, mut sender) = match ws.recv_with_sender().await {
            Ok(Some(received)) => received,
            Ok(None) | Err(_) => return,
        };
        if sender.send(message.kind(), message.bytes()).await.is_err() {
            return;
        }
    }
}

#[cfg(test)]
mod sizing_tests {
    use super::*;

    #[test]
    fn udp_requests_one_session_slot_without_changing_tcp_or_ws() {
        assert_eq!(socket_slots(Proto::Udp), 1);
        assert_eq!(socket_slots(Proto::Tcp), 320);
        assert_eq!(socket_slots(Proto::Ws), 320);
    }
}

#[cfg(all(test, feature = "e2e"))]
mod tests {
    use super::*;
    #[test]
    fn tcp_and_websocket_startup_failure_precedes_ready_banner() {
        for proto in ["tcp", "ws"] {
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "serve::tests::listen_startup_failure_child",
                    "--ignored",
                ])
                .env("SWITCH_BENCH_STARTUP_PROTO", proto)
                .output()
                .unwrap();
            let stderr = String::from_utf8_lossy(&output.stderr);
            assert!(
                output.status.success(),
                "{proto} startup child failed: {}\n{}\n{stderr}",
                output.status,
                String::from_utf8_lossy(&output.stdout),
            );
            assert!(
                !stderr.contains("echo on port"),
                "{proto} printed readiness after startup failure:\n{stderr}",
            );
        }
    }

    #[switch_test_support::pinned_test]
    #[ignore = "run in a separate process by tcp_and_websocket_startup_failure_precedes_ready_banner"]
    fn listen_startup_failure_child() {
        let proto = match std::env::var("SWITCH_BENCH_STARTUP_PROTO").unwrap().as_str() {
            "tcp" => Proto::Tcp,
            "ws" => Proto::Ws,
            other => panic!("unexpected protocol {other}"),
        };
        let dir = std::env::temp_dir().join(format!("switch-bench-startup-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("dut.json");
        std::fs::write(
            &path,
            r#"{"version": 1, "device": {"vdev": "net_tap_benchstart,iface=cdbenchstart"}, "mac": null,
             "ipv4": "10.79.0.2/24", "gateway": null, "mtu": 1500, "paths": [], "offloads": "auto",
             "rx_timestamps": false, "descriptors": 512,
             "eal": {"file_prefix": "benchstart", "extra": ["--no-huge", "-m", "512", "--no-shconf"]}}"#,
        ).unwrap();
        host::catch_stop().unwrap();
        // Bound the mutated path that ignores the error and enters the serving loop.
        std::thread::spawn(|| {
            std::thread::sleep(std::time::Duration::from_secs(5));
            unsafe { libc::kill(libc::getpid(), libc::SIGTERM) };
        });
        let result = run(&Args { backend: Backend::Conduit(path), proto, port: 0 });
        std::fs::remove_dir_all(dir).unwrap();
        assert!(
            matches!(&result, Err(error) if error.starts_with("tcp listen:")),
            "invalid initial listen must return its error, got {result:?}",
        );
    }


    // Host::open initializes EAL once per process, so keep this host separate from the TCP test.
    #[test]
    fn websocket_echo_over_net_tap() {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "serve::tests::websocket_echo_over_net_tap_child", "--ignored"])
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "WebSocket echo child failed: {}\n{}\n{}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
    }

    #[switch_test_support::pinned_test]
    #[ignore = "run in a separate process by websocket_echo_over_net_tap"]
    fn websocket_echo_over_net_tap_child() {
        use std::time::{Duration, Instant};
        use tungstenite::Message;
        use tungstenite::protocol::frame::{Frame, coding::{Data, OpCode}};

        const PORT: u16 = 7101;
        let dir = std::env::temp_dir().join(format!("switch-bench-ws-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("dut.json");
        std::fs::write(
            &path,
            r#"{"version": 1, "device": {"vdev": "net_tap_benchws,iface=cdbenchws"}, "mac": null,
             "ipv4": "10.78.0.2/24", "gateway": null, "mtu": 1500, "paths": [], "offloads": "auto",
             "rx_timestamps": false, "descriptors": 512,
             "eal": {"file_prefix": "benchws", "extra": ["--no-huge", "-m", "512", "--no-shconf"]}}"#,
        ).unwrap();
        let config = Host::config(&path).unwrap();
        let tcp_config = tcp::Config { rx_capacity: TCP_BUFFER, tx_capacity: TCP_BUFFER, mss_cap: None };
        let mut host = Host::open(&config, &tcp_sizing(2, &tcp_config, 1500)).unwrap();
        for args in [
            &["address", "add", "10.78.0.1/24", "dev", "cdbenchws"][..],
            &["link", "set", "cdbenchws", "up"],
        ] {
            assert!(std::process::Command::new("ip").args(args).status().unwrap().success(), "ip {args:?}");
        }
        let handle = host.net.handle();
        let spawner = host.rt.spawner();
        spawn_accept_loop(&spawner, &handle, PORT, tcp_config, true, 1, None).unwrap();
        let peer = std::thread::spawn(|| {
            let stream = std::net::TcpStream::connect(("10.78.0.2", PORT)).unwrap();
            stream.set_read_timeout(Some(Duration::from_secs(20))).unwrap();
            stream.set_write_timeout(Some(Duration::from_secs(20))).unwrap();
            let (mut ws, _) = tungstenite::client(format!("ws://10.78.0.2:{PORT}/"), stream).unwrap();
            let text = Message::text("echo €");
            ws.send(text.clone()).unwrap();
            assert_eq!(ws.read().unwrap(), text);
            ws.write(Message::Frame(Frame::message(&[0, 255][..], OpCode::Data(Data::Binary), false))).unwrap();
            ws.write(Message::Frame(Frame::message(&[2, 3][..], OpCode::Data(Data::Continue), true))).unwrap();
            ws.flush().unwrap();
            assert_eq!(ws.read().unwrap(), Message::binary(vec![0, 255, 2, 3]));
            ws.close(None).unwrap();
            assert!(matches!(ws.read(), Ok(Message::Close(None))));
            assert!(matches!(ws.read(), Err(tungstenite::Error::ConnectionClosed)));
        });
        let deadline = Instant::now() + Duration::from_secs(30);
        while !peer.is_finished() {
            assert!(Instant::now() < deadline, "WebSocket echo did not finish within 30 s");
            host.round().unwrap();
            std::thread::sleep(Duration::from_micros(50));
        }
        peer.join().unwrap();
        drop((handle, spawner));
        host.close().unwrap();
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn accepts_resume_after_socket_exhaustion() {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "serve::tests::accepts_resume_after_socket_exhaustion_child", "--ignored"])
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "accept child failed: {}\n{}\n{}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
    }

    /// Two socket slots, backlog 1. Connections A and B open together take both slots, so the
    /// listener has no socket left; once both close, a third connection is still echoed.
    #[switch_test_support::pinned_test]
    #[ignore = "run in a separate process by accepts_resume_after_socket_exhaustion"]
    fn accepts_resume_after_socket_exhaustion_child() {
        use std::io::{Read, Write as _};
        use std::time::{Duration, Instant};

        const PORT: u16 = 7102;
        let dir = std::env::temp_dir().join(format!("switch-bench-acc-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("dut.json");
        std::fs::write(
            &path,
            r#"{"version": 1, "device": {"vdev": "net_tap_benchacc,iface=cdbenchacc"}, "mac": null,
             "ipv4": "10.79.0.2/24", "gateway": null, "mtu": 1500, "paths": [], "offloads": "auto",
             "rx_timestamps": false, "descriptors": 512,
             "eal": {"file_prefix": "benchacc", "extra": ["--no-huge", "-m", "512", "--no-shconf"]}}"#,
        ).unwrap();
        let config = Host::config(&path).unwrap();
        let tcp_config = tcp::Config { rx_capacity: TCP_BUFFER, tx_capacity: TCP_BUFFER, mss_cap: None };
        let mut host = Host::open(&config, &tcp_sizing(2, &tcp_config, 1500)).unwrap();
        for args in [
            &["address", "add", "10.79.0.1/24", "dev", "cdbenchacc"][..],
            &["link", "set", "cdbenchacc", "up"],
        ] {
            assert!(std::process::Command::new("ip").args(args).status().unwrap().success(), "ip {args:?}");
        }
        let handle = host.net.handle();
        let spawner = host.rt.spawner();
        spawn_accept_loop(&spawner, &handle, PORT, tcp_config, false, 1, None).unwrap();
        let peer = std::thread::spawn(|| {
            let echo = |s: &mut std::net::TcpStream| {
                s.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
                s.write_all(b"ping").unwrap();
                let mut back = [0; 4];
                s.read_exact(&mut back).unwrap();
                assert_eq!(&back, b"ping");
            };
            let mut a = std::net::TcpStream::connect(("10.79.0.2", PORT)).unwrap();
            echo(&mut a);
            let mut b = std::net::TcpStream::connect(("10.79.0.2", PORT)).unwrap();
            echo(&mut b);
            drop((a, b));
            // the slots free once the closes complete; until the listener opens a socket again
            // the SYN is dropped and the kernel retransmits it
            let deadline = Instant::now() + Duration::from_secs(10);
            let mut c = loop {
                match std::net::TcpStream::connect(("10.79.0.2", PORT)) {
                    Ok(c) => break c,
                    Err(e) => {
                        assert!(Instant::now() < deadline, "no accept after the slots freed: {e}");
                        std::thread::sleep(Duration::from_millis(20));
                    }
                }
            };
            echo(&mut c);
        });
        let deadline = Instant::now() + Duration::from_secs(30);
        while !peer.is_finished() {
            assert!(Instant::now() < deadline, "peer did not finish within 30 s");
            host.round().unwrap();
            std::thread::sleep(Duration::from_micros(50));
        }
        peer.join().unwrap();
        drop((handle, spawner));
        host.close().unwrap();
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// The switch TCP echo on net_tap against a kernel peer sending mixed segment sizes.
    #[switch_test_support::pinned_test]
    fn tcp_echo_returns_all_bytes() {
        use std::io::{Read, Write as _};
        use std::time::{Duration, Instant};

        const PORT: u16 = 7100;
        const ROUNDS: usize = 200;
        const SMALL: usize = 30;
        let dir = std::env::temp_dir().join(format!("switch-bench-echo-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("dut.json");
        std::fs::write(
            &path,
            r#"{"version": 1, "device": {"vdev": "net_tap_bencho,iface=cdbencho"}, "mac": null,
             "ipv4": "10.77.0.2/24", "gateway": null, "mtu": 1500, "paths": [], "offloads": "auto",
             "rx_timestamps": false, "descriptors": 512,
             "eal": {"file_prefix": "bencho", "extra": ["--no-huge", "-m", "512", "--no-shconf"]}}"#,
        )
        .unwrap();
        let config = Host::config(&path).unwrap();
        let tcp_config = tcp::Config {
            rx_capacity: TCP_BUFFER,
            tx_capacity: TCP_BUFFER,
            mss_cap: None,
        };
        let mut host = Host::open(&config, &tcp_sizing(2, &tcp_config, 1500)).unwrap();
        for args in [
            &["address", "add", "10.77.0.1/24", "dev", "cdbencho"][..],
            &["link", "set", "cdbencho", "up"],
        ] {
            let status = std::process::Command::new("ip")
                .args(args)
                .status()
                .unwrap();
            assert!(status.success(), "ip {args:?}");
        }
        let handle = host.net.handle();
        let spawner = host.rt.spawner();
        spawn_accept_loop(&spawner, &handle, PORT, tcp_config, false, 1, None).unwrap();

        let sent: Vec<u8> = (0..ROUNDS * (1460 + SMALL * 64))
            .map(|i| (i % 251) as u8)
            .collect();
        let expected = sent.clone();
        let peer = std::thread::spawn(move || {
            let stream = std::net::TcpStream::connect(("10.77.0.2", PORT)).unwrap();
            stream.set_nodelay(true).unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(20)))
                .unwrap();
            let mut writer = stream.try_clone().unwrap();
            let len = sent.len();
            let writes = std::thread::spawn(move || {
                let mut at = 0;
                for _ in 0..ROUNDS {
                    writer.write_all(&sent[at..at + 1460]).unwrap();
                    at += 1460;
                    for _ in 0..SMALL {
                        writer.write_all(&sent[at..at + 64]).unwrap();
                        at += 64;
                    }
                }
            });
            let mut back = vec![0u8; len];
            (&stream).read_exact(&mut back).unwrap();
            writes.join().unwrap();
            back
        });
        let deadline = Instant::now() + Duration::from_secs(30);
        while !peer.is_finished() {
            assert!(Instant::now() < deadline, "echo not back within 30 s");
            host.round().unwrap();
            std::thread::sleep(Duration::from_micros(50));
        }
        assert!(peer.join().unwrap() == expected, "echoed bytes differ");
        drop((handle, spawner));
        host.close().unwrap();
        std::fs::remove_dir_all(dir).unwrap();
    }
}
