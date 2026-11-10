//! The Zhida echo DUT (UDP, TCP, ws). The busy-poll loop does no syscall while serving.

use switch::{SpawnError, Spawner};
use switchnet::{
    Accepting, Error, IpListenEndpoint, NetHandle, TcpStream, UdpConfig, UdpRx,
    UdpTx, tcp,
};
use zhida_open::Sizing;

use crate::host::{self, Backend, Host};
use crate::load::{Proto, TCP_BUFFER};
use relay::Limits;
use relay::switchnet::{Ws, accept_ws};

#[derive(Debug, Clone)]
pub struct Args {
    pub backend: Backend,
    pub proto: Proto,
    pub port: u16,
}

const UDP_QUEUE: usize = 4096;
/// TCP: connections served at once, and accepts kept armed.
const MAX_CONNS: usize = 256;
const ACCEPTS: usize = 64;

pub fn run(args: &Args) -> Result<(), String> {
    let tcp_config = tcp::Config {
        rx_capacity: TCP_BUFFER,
        tx_capacity: TCP_BUFFER,
        mss_cap: None,
    };
    let sockets = MAX_CONNS + ACCEPTS;
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
    match args.proto {
        Proto::Udp => udp_echo(&mut host, &handle, args.port)?,
        Proto::Tcp | Proto::Ws => {
            let startup_error = std::rc::Rc::new(std::cell::RefCell::new(None));
            for _ in 0..ACCEPTS {
                arm_accept(
                    &host.rt.spawner(),
                    handle.clone(),
                    args.port,
                    tcp_config,
                    args.proto == Proto::Ws,
                    Some(startup_error.clone()),
                )?;
            }
            host.round()?;
            let error = startup_error.borrow_mut().take();
            if let Some(error) = error {
                return Err(format!("tcp accept: {error:?}"));
            }
        }
    }
    eprintln!(
        "zhida-bench serve: {} echo on port {}",
        args.proto.name(),
        args.port
    );
    while !host::stopped() {
        host.round()?;
    }
    drop(handle);
    host.close()
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

#[switch::task]
async fn bind_echo(
    handle: NetHandle,
    config: UdpConfig,
    bound: std::rc::Rc<std::cell::RefCell<Option<Result<switchnet::UdpSocket, Error>>>>,
) {
    *bound.borrow_mut() = Some(handle.udp_bind(config).await);
}

#[switch::task]
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
            switch::yield_now().await;
        }
    }
}

/// One pre-armed accept on `port` (conduit has no accept queue: each accept is a listening
/// socket); the connection it takes is echoed, and it arms the next accept first.
fn arm_accept(
    spawner: &Spawner,
    handle: NetHandle,
    port: u16,
    config: tcp::Config,
    ws: bool,
    startup_error: Option<std::rc::Rc<std::cell::RefCell<Option<Error>>>>,
) -> Result<(), String> {
    let accepting = handle.tcp_accept(config, IpListenEndpoint { addr: None, port });
    spawner
        .spawn(serve_accepted(
            accepting,
            spawner.clone(),
            handle,
            port,
            config,
            ws,
            startup_error,
        ))
        .map_err(spawn_error)
}

/// Waits for `accepting`'s connection, arms the next accept, and echoes the connection.
#[switch::task]
async fn serve_accepted(
    accepting: Accepting,
    spawner: Spawner,
    handle: NetHandle,
    port: u16,
    config: tcp::Config,
    ws: bool,
    startup_error: Option<std::rc::Rc<std::cell::RefCell<Option<Error>>>>,
) {
    let stream = match accepting.await {
        Ok(stream) => stream,
        Err(error) => {
            if let Some(startup_error) = startup_error {
                *startup_error.borrow_mut() = Some(error);
            }
            eprintln!("zhida-bench serve: tcp accept on {port}: {error:?}");
            return;
        }
    };
    drop(startup_error);
    if let Err(e) = arm_accept(&spawner, handle.clone(), port, config, ws, None) {
        eprintln!("zhida-bench serve: {e}");
    }
    if ws {
        match accept_ws(&handle, stream, Limits::default()).await {
            Ok(ws) => ws_echo(ws).await,
            Err(e) => eprintln!("zhida-bench serve: ws upgrade: {e}"),
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

/// One ws connection of the DUT: every message back with its kind, through relay. Returns when
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
                .env("ZHIDA_BENCH_STARTUP_PROTO", proto)
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

    #[zhida_test_support::pinned_test]
    #[ignore = "run in a separate process by tcp_and_websocket_startup_failure_precedes_ready_banner"]
    fn listen_startup_failure_child() {
        let proto = match std::env::var("ZHIDA_BENCH_STARTUP_PROTO").unwrap().as_str() {
            "tcp" => Proto::Tcp,
            "ws" => Proto::Ws,
            other => panic!("unexpected protocol {other}"),
        };
        let dir = std::env::temp_dir().join(format!("zhida-bench-startup-{}", std::process::id()));
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
            matches!(&result, Err(error) if error.starts_with("tcp accept:")),
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

    #[zhida_test_support::pinned_test]
    #[ignore = "run in a separate process by websocket_echo_over_net_tap"]
    fn websocket_echo_over_net_tap_child() {
        use std::time::{Duration, Instant};
        use tungstenite::Message;
        use tungstenite::protocol::frame::{Frame, coding::{Data, OpCode}};

        const PORT: u16 = 7101;
        let dir = std::env::temp_dir().join(format!("zhida-bench-ws-{}", std::process::id()));
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
        arm_accept(&spawner, handle.clone(), PORT, tcp_config, true, None).unwrap();
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

    /// The Zhida TCP echo on net_tap against a kernel peer sending mixed segment sizes.
    #[zhida_test_support::pinned_test]
    fn tcp_echo_returns_all_bytes() {
        use std::io::{Read, Write as _};
        use std::time::{Duration, Instant};

        const PORT: u16 = 7100;
        const ROUNDS: usize = 200;
        const SMALL: usize = 30;
        let dir = std::env::temp_dir().join(format!("zhida-bench-echo-{}", std::process::id()));
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
        arm_accept(&spawner, handle.clone(), PORT, tcp_config, false, None).unwrap();

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
