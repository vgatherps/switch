//! The Zhida echo DUT (UDP, TCP, ws). The busy-poll loop does no syscall while serving.

use std::path::PathBuf;

use switch::{SpawnError, Spawner};
use switchnet::{
    Accepting, IpListenEndpoint, NetHandle, SendFrameError, TcpStream, UdpConfig, UdpRx,
    UdpSendError, UdpSendFrameError, UdpTx, tcp,
};
use zhida_open::Sizing;

use crate::host::{self, Host};
use crate::load::{Proto, TCP_BUFFER};
use relay::Limits;
use relay::switchnet::{Ws, accept_ws};

#[derive(Debug, Clone)]
pub struct Args {
    pub config: PathBuf,
    pub proto: Proto,
    pub port: u16,
}

const UDP_QUEUE: usize = 4096;
/// TCP: connections served at once, and accepts kept armed.
const MAX_CONNS: usize = 256;
const ACCEPTS: usize = 64;

pub fn run(args: &Args) -> Result<(), String> {
    let config = Host::config(&args.config)?;
    let tcp_config = tcp::Config {
        rx_capacity: TCP_BUFFER,
        tx_capacity: TCP_BUFFER,
        mss_cap: None,
    };
    let mtu = usize::from(config.mtu);
    let sockets = MAX_CONNS + ACCEPTS;
    let sizing = match args.proto {
        Proto::Udp => Sizing {
            frame_share: 64,
            max_sockets: 1,
            max_udp_sockets: 1,
            rx_staging: 128,
            tx_pending: 64,
            neighbor_capacity: 256,
        },
        Proto::Tcp | Proto::Ws => tcp_sizing(sockets, &tcp_config, mtu),
    };
    host::catch_stop()?;
    let mut host = Host::open(&config, &sizing)?;
    host.start_xstats("serve");
    let handle = host.net.handle();
    match args.proto {
        Proto::Udp => udp_echo(&host, &handle, args.port)?,
        Proto::Tcp | Proto::Ws => {
            for _ in 0..ACCEPTS {
                arm_accept(
                    &host.rt.spawner(),
                    handle.clone(),
                    args.port,
                    tcp_config,
                    args.proto == Proto::Ws,
                )?;
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
fn udp_echo(host: &Host, handle: &NetHandle, port: u16) -> Result<(), String> {
    let config = UdpConfig {
        port,
        local_addr: None,
        rx_queue: UDP_QUEUE,
        tx_checksum: true,
        broadcast: false,
    };
    let (rx, tx) = handle
        .udp_bind(config)
        .map_err(|e| format!("udp bind: {e:?}"))?
        .split();
    host.rt
        .spawner()
        .spawn(echo_datagrams(handle.clone(), rx, tx))
        .map_err(spawn_error)
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
            UdpSendFrameError::Udp(UdpSendError::WouldBlock | UdpSendError::NeighborPending),
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
) -> Result<(), String> {
    let accepting = handle
        .tcp_accept(config, IpListenEndpoint { addr: None, port })
        .map_err(|e| format!("tcp accept on {port}: {e:?}"))?;
    spawner
        .spawn(serve_accepted(
            accepting,
            spawner.clone(),
            handle,
            port,
            config,
            ws,
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
) {
    let stream = accepting.await;
    if let Err(e) = arm_accept(&spawner, handle.clone(), port, config, ws) {
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
                    Err((back, SendFrameError::Full)) => {
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
        arm_accept(&spawner, handle.clone(), PORT, tcp_config, true).unwrap();
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
        arm_accept(&spawner, handle.clone(), PORT, tcp_config, false).unwrap();

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
