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
        frame.room_mut()[..len].copy_from_slice(datagram.payload());
        frame.set_len(len);
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

#[cfg(test)]
thread_local! {
    // the largest TCP frame payload received: the echo test's precondition
    static MAX_RX_FRAME: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// Echoes the connection's bytes. A received frame may hold more than one TX frame's room
/// (conduit appends small in-order segments into a frame's tailroom), so it goes back as as many
/// TX frames as it needs.
async fn tcp_echo(handle: &NetHandle, stream: TcpStream) {
    let (mut rx, mut tx) = stream.split();
    loop {
        let Ok(received) = rx.recv().await else {
            return;
        };
        let payload = received.payload();
        #[cfg(test)]
        MAX_RX_FRAME.with(|m| m.set(m.get().max(payload.len())));
        let mut rest = payload;
        while !rest.is_empty() {
            let mut frame = handle.tx_frame().await;
            let len = rest.len().min(frame.room_mut().len());
            frame.room_mut()[..len].copy_from_slice(&rest[..len]);
            frame.set_len(len);
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
    let mut buf = Vec::new();
    loop {
        let kind = match ws.recv().await {
            Ok(Some(m)) => {
                buf.clear();
                buf.extend_from_slice(m.bytes());
                m.kind()
            }
            Ok(None) | Err(_) => return,
        };
        if ws.send(kind, &buf).await.is_err() {
            return;
        }
    }
}

#[cfg(all(test, feature = "e2e"))]
mod tests {
    use super::*;

    /// The Zhida TCP echo on net_tap (the production `Host` and pools) against a kernel peer that
    /// sends full segments each followed by small ones: conduit appends the small ones into the
    /// full one's tailroom, so a received frame outgrows a TX frame. Every byte must come back.
    #[zhida_test_support::pinned_test]
    fn echo_returns_a_received_frame_larger_than_a_tx_frame() {
        use std::cell::Cell;
        use std::io::{Read, Write as _};
        use std::rc::Rc;
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
        #[switch::task]
        async fn frame_room(handle: NetHandle, room: Rc<Cell<usize>>) {
            room.set(handle.tx_frame().await.room_mut().len());
        }
        let room = Rc::new(Cell::new(0));
        spawner
            .spawn(frame_room(handle.clone(), room.clone()))
            .unwrap();

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
        let max_frame = MAX_RX_FRAME.with(|m| m.get());
        assert!(
            max_frame > room.get(),
            "precondition: largest received frame {max_frame} B, TX frame room {} B",
            room.get()
        );
        drop((handle, spawner));
        host.close().unwrap();
    }
}
