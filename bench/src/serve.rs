//! The Zhida echo DUT (UDP, TCP). The busy-poll loop does no syscall while serving: every
//! 100 ms it copies its cumulative counters into a preallocated ring; ethdev stats are read at
//! start and at stop only; everything is written to the counter log after it stops.

use std::cell::{Cell, RefCell};
use std::io::Write;
use std::path::PathBuf;
use std::rc::Rc;

use serde::Serialize;
use switch::Spawner;
use switchnet::{
    IpListenEndpoint, NetHandle, SendFrameError, TcpStream, UdpConfig, UdpSendError,
    UdpSendFrameError, UdpTx, boxed, tcp,
};
use zhida_open::Sizing;

use crate::host::{self, EthStats, Host, TurnSums};
use crate::load::{Proto, TCP_BUFFER};
use relay::switchnet::{Ws, accept_ws};

#[derive(Debug, Clone)]
pub struct Args {
    pub config: PathBuf,
    pub proto: Proto,
    pub port: u16,
    /// TCP: connections served at once, and accepts kept armed.
    pub max_conns: usize,
    pub accepts: usize,
    pub counter_log: Option<PathBuf>,
    /// Ring slots: samples kept, newest last (one per 100 ms).
    pub ring: usize,
}

const UDP_QUEUE: usize = 4096;
const SAMPLE_NS: u64 = 100_000_000;

#[derive(Debug, Clone, Copy, Serialize)]
pub struct Sample {
    pub unix_ns: u64,
    pub turns: TurnSums,
    /// Datagrams (UDP) or bytes (TCP) echoed.
    pub echoed: u64,
    pub connections: u64,
    pub send_errors: u64,
    pub rx_dropped_full: u64,
}

/// A fixed-capacity ring: pushing into a full ring overwrites the oldest sample.
pub struct Ring {
    slots: Vec<Sample>,
    next: usize,
    overwritten: u64,
}

impl Ring {
    pub fn new(capacity: usize) -> Ring {
        assert!(capacity > 0);
        Ring {
            slots: Vec::with_capacity(capacity),
            next: 0,
            overwritten: 0,
        }
    }

    #[inline]
    pub fn push(&mut self, sample: Sample) {
        if self.slots.len() < self.slots.capacity() {
            self.slots.push(sample);
        } else {
            self.slots[self.next] = sample;
            self.overwritten += 1;
        }
        self.next = (self.next + 1) % self.slots.capacity();
    }

    /// Oldest first.
    pub fn samples(&self) -> impl Iterator<Item = &Sample> {
        let (newer, older) = self
            .slots
            .split_at(if self.slots.len() < self.slots.capacity() {
                self.slots.len()
            } else {
                self.next
            });
        older.iter().chain(newer)
    }
}

pub fn run(args: &Args) -> Result<(), String> {
    let config = Host::config(&args.config)?;
    let tcp_config = tcp::Config {
        rx_capacity: TCP_BUFFER,
        tx_capacity: TCP_BUFFER,
        mss_cap: None,
    };
    let mtu = usize::from(config.mtu);
    let sockets = args.max_conns + args.accepts;
    let sizing = match args.proto {
        Proto::Udp => Sizing {
            rx_share: UDP_QUEUE,
            frame_share: 64,
            max_sockets: 1,
            max_udp_sockets: 1,
            rx_staging: 64,
            tx_pending: 64,
            neighbor_capacity: 256,
        },
        Proto::Tcp | Proto::Ws => tcp_sizing(sockets, &tcp_config, mtu),
    };
    host::catch_stop()?;
    let mut host = Host::open(&config, &sizing)?;
    let eth_start = EthStats::read(host.port_id)?;
    let handle = host.net.handle();
    let counts = Rc::new(Counts::default());
    let udp_tx = match args.proto {
        Proto::Udp => Some(udp_echo(&host, &handle, args.port, counts.clone())?),
        Proto::Tcp | Proto::Ws => {
            for _ in 0..args.accepts {
                arm_accept(
                    &host.rt.spawner(),
                    handle.clone(),
                    args.port,
                    tcp_config,
                    args.proto == Proto::Ws,
                    counts.clone(),
                )?;
            }
            None
        }
    };
    let mut ring = Ring::new(args.ring);
    let mut turns = TurnSums::default();
    let mut next_sample = host.clock.now_ns() + SAMPLE_NS;
    eprintln!(
        "zhida-bench serve: {} echo on port {}",
        args.proto.name(),
        args.port
    );
    while !host::stopped() {
        let now = host.round(&mut turns)?;
        if host::dump_requested() {
            eprintln!("zhida-bench serve: dump: {turns:?}");
            for (i, c) in counts.conns.borrow().iter().enumerate() {
                eprintln!(
                    "zhida-bench serve: conn {i} rx {} echoed {} max frame {} {}",
                    c.rx_bytes.get(),
                    c.echoed_bytes.get(),
                    c.max_rx_frame.get(),
                    c.phase.get()
                );
            }
        }
        if now >= next_sample {
            // the wall clock drifts from the monotonic one; samples are joined across hosts by it
            host.clock.resync();
            ring.push(Sample {
                unix_ns: host.clock.unix_ns(now),
                turns,
                echoed: counts.echoed.get(),
                connections: counts.connections.get(),
                send_errors: counts.send_errors.get(),
                rx_dropped_full: udp_tx
                    .as_ref()
                    .map_or(0, |tx| tx.borrow().stats().rx_dropped_full),
            });
            next_sample += SAMPLE_NS;
        }
    }
    let eth_end = EthStats::read(host.port_id)?;
    if let Some(path) = &args.counter_log {
        write_log(
            path,
            args.proto,
            &host,
            eth_start,
            eth_end,
            &ring,
            turns,
            counts.echoed.get(),
        )?;
    }
    drop((udp_tx, handle));
    host.close()
}

fn tcp_sizing(sockets: usize, tcp_config: &tcp::Config, mtu: usize) -> Sizing {
    Sizing {
        rx_share: sockets * tcp::rx_mbuf_budget(tcp_config, mtu),
        frame_share: sockets * tcp::tx_mbuf_budget(tcp_config, mtu) + 64,
        max_sockets: sockets,
        max_udp_sockets: 0,
        rx_staging: 64,
        tx_pending: 64,
        neighbor_capacity: 256,
    }
}

/// What the echo tasks count; read by the loop.
#[derive(Default)]
struct Counts {
    /// Datagrams (UDP), bytes (TCP) or messages (ws) echoed.
    echoed: Cell<u64>,
    connections: Cell<u64>,
    send_errors: Cell<u64>,
    /// Every TCP/ws connection taken, for the SIGUSR1 dump.
    conns: RefCell<Vec<Rc<ConnState>>>,
}

/// One echoed connection, for the SIGUSR1 dump.
#[derive(Default)]
struct ConnState {
    rx_bytes: Cell<u64>,
    echoed_bytes: Cell<u64>,
    /// Largest received frame's payload (TCP).
    max_rx_frame: Cell<usize>,
    /// What its task waits for: "upgrade", "recv", "frame" (a TX frame), "room" (send buffer
    /// space), or "ended".
    phase: Cell<&'static str>,
}

fn spawn(spawner: &Spawner, task: impl Future<Output = ()> + 'static) -> Result<(), String> {
    spawner
        .spawn(boxed(Box::pin(task)))
        .map_err(|e| format!("spawn: {:?}", e.reason))
}

/// Binds `port` and echoes every datagram to its source. Returns the send half (for its stats).
fn udp_echo(
    host: &Host,
    handle: &NetHandle,
    port: u16,
    counts: Rc<Counts>,
) -> Result<Rc<RefCell<UdpTx>>, String> {
    let config = UdpConfig {
        port,
        local_addr: None,
        rx_queue: UDP_QUEUE,
        tx_checksum: true,
        broadcast: false,
    };
    let (mut rx, tx) = handle
        .udp_bind(config)
        .map_err(|e| format!("udp bind: {e:?}"))?
        .split();
    let tx = Rc::new(RefCell::new(tx));
    let (task_tx, handle) = (tx.clone(), handle.clone());
    spawn(&host.rt.spawner(), async move {
        loop {
            let datagram = rx.recv().await;
            let src = datagram.src();
            let mut frame = handle.tx_frame().await;
            let len = datagram.payload().len();
            frame.room_mut()[..len].copy_from_slice(datagram.payload());
            frame.set_len(len);
            drop(datagram);
            loop {
                let sent = task_tx.borrow_mut().send(Some(src), frame);
                match sent {
                    Ok(()) => counts.echoed.set(counts.echoed.get() + 1),
                    Err((
                        back,
                        UdpSendFrameError::Udp(
                            UdpSendError::WouldBlock | UdpSendError::NeighborPending,
                        ),
                    )) => {
                        frame = back;
                        switch::yield_now().await;
                        continue;
                    }
                    Err(_) => counts.send_errors.set(counts.send_errors.get() + 1),
                }
                break;
            }
        }
    })?;
    Ok(tx)
}

/// One pre-armed accept on `port` (conduit has no accept queue: each accept is a listening
/// socket); the connection it takes is echoed, and it arms the next accept first.
fn arm_accept(
    spawner: &Spawner,
    handle: NetHandle,
    port: u16,
    config: tcp::Config,
    ws: bool,
    counts: Rc<Counts>,
) -> Result<(), String> {
    let accepting = handle
        .tcp_accept(config, IpListenEndpoint { addr: None, port })
        .map_err(|e| format!("tcp accept on {port}: {e:?}"))?;
    let task_spawner = spawner.clone();
    spawn(spawner, async move {
        let stream = accepting.await;
        counts.connections.set(counts.connections.get() + 1);
        let conn = Rc::new(ConnState::default());
        counts.conns.borrow_mut().push(conn.clone());
        if let Err(e) = arm_accept(
            &task_spawner,
            handle.clone(),
            port,
            config,
            ws,
            counts.clone(),
        ) {
            eprintln!("zhida-bench serve: {e}");
        }
        if ws {
            conn.phase.set("upgrade");
            match accept_ws(&handle, stream).await {
                Ok(ws) => ws_echo(ws, &counts.echoed, &conn).await,
                Err(e) => eprintln!("zhida-bench serve: ws upgrade: {e}"),
            }
            conn.phase.set("ended");
            return;
        }
        tcp_echo(&handle, stream, &counts, &conn).await;
        conn.phase.set("ended");
    })
}

/// Echoes the connection's bytes. A received frame may hold more than one TX frame's room
/// (conduit appends small in-order segments into a frame's tailroom), so it goes back as as many
/// TX frames as it needs.
async fn tcp_echo(handle: &NetHandle, stream: TcpStream, counts: &Counts, conn: &ConnState) {
    let (mut rx, mut tx) = stream.split();
    loop {
        conn.phase.set("recv");
        let Ok(received) = rx.recv().await else {
            return;
        };
        let payload = received.payload();
        conn.rx_bytes
            .set(conn.rx_bytes.get() + payload.len() as u64);
        conn.max_rx_frame
            .set(conn.max_rx_frame.get().max(payload.len()));
        let mut rest = payload;
        while !rest.is_empty() {
            conn.phase.set("frame");
            let mut frame = handle.tx_frame().await;
            let len = rest.len().min(frame.room_mut().len());
            frame.room_mut()[..len].copy_from_slice(&rest[..len]);
            frame.set_len(len);
            rest = &rest[len..];
            loop {
                match tx.try_send(frame) {
                    Ok(()) => {
                        counts.echoed.set(counts.echoed.get() + len as u64);
                        conn.echoed_bytes.set(conn.echoed_bytes.get() + len as u64);
                        break;
                    }
                    Err((back, SendFrameError::Full)) => {
                        frame = back;
                        conn.phase.set("room");
                        if tx.room(len).await.is_err() {
                            return;
                        }
                    }
                    Err(_) => {
                        counts.send_errors.set(counts.send_errors.get() + 1);
                        return;
                    }
                }
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn write_log(
    path: &std::path::Path,
    proto: Proto,
    host: &Host,
    eth_start: EthStats,
    eth_end: EthStats,
    ring: &Ring,
    turns: TurnSums,
    echoed: u64,
) -> Result<(), String> {
    let mut file = std::io::BufWriter::new(
        std::fs::File::create(path).map_err(|e| format!("{}: {e}", path.display()))?,
    );
    let mut line = |value: serde_json::Value| writeln!(file, "{value}").map_err(|e| e.to_string());
    line(
        serde_json::json!({"kind": "dut", "proto": proto.name(), "unix_ns": host.clock.unix_ns(0),
        "eth_start": eth_start, "eth_end": eth_end, "eth": eth_end.since(&eth_start),
        "turns": turns, "echoed": echoed, "overwritten_samples": ring.overwritten}),
    )?;
    for sample in ring.samples() {
        line(serde_json::json!({"kind": "sample", "sample": sample}))?;
    }
    Ok(())
}

/// One ws connection of the DUT: every message back with its kind, through relay. Returns when
/// the peer closes or fails the connection; counts echoed messages.
async fn ws_echo(mut ws: Ws, echoed: &Cell<u64>, conn: &ConnState) {
    let mut buf = Vec::new();
    loop {
        conn.phase.set("recv");
        let kind = match ws.recv().await {
            Ok(Some(m)) => {
                buf.clear();
                buf.extend_from_slice(m.bytes());
                m.kind()
            }
            Ok(None) | Err(_) => return,
        };
        conn.rx_bytes.set(conn.rx_bytes.get() + buf.len() as u64);
        conn.phase.set("send");
        if ws.send(kind, &buf).await.is_err() {
            return;
        }
        conn.echoed_bytes
            .set(conn.echoed_bytes.get() + buf.len() as u64);
        echoed.set(echoed.get() + 1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(i: u64) -> Sample {
        Sample {
            unix_ns: i,
            turns: TurnSums::default(),
            echoed: i,
            connections: 0,
            send_errors: 0,
            rx_dropped_full: 0,
        }
    }

    /// Below capacity every sample is kept in order; past it the oldest are overwritten and
    /// the rest still come out oldest first.
    #[test]
    fn ring_keeps_the_newest_in_order() {
        let mut ring = Ring::new(4);
        for i in 0..3 {
            ring.push(sample(i));
        }
        assert_eq!(
            ring.samples().map(|s| s.echoed).collect::<Vec<_>>(),
            [0, 1, 2]
        );
        for i in 3..10 {
            ring.push(sample(i));
        }
        assert_eq!(
            ring.samples().map(|s| s.echoed).collect::<Vec<_>>(),
            [6, 7, 8, 9]
        );
        assert_eq!(ring.overwritten, 6);
        assert_eq!(ring.slots.capacity(), 4, "no reallocation");
    }

    /// The Zhida TCP echo on net_tap (the production `Host` and pools) against a kernel peer that
    /// sends full segments each followed by small ones: conduit appends the small ones into the
    /// full one's tailroom, so a received frame outgrows a TX frame. Every byte must come back.
    #[cfg(feature = "e2e")]
    #[zhida_test_support::pinned_test]
    fn echo_returns_a_received_frame_larger_than_a_tx_frame() {
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
        let counts = Rc::new(Counts::default());
        let spawner = host.rt.spawner();
        arm_accept(
            &spawner,
            handle.clone(),
            PORT,
            tcp_config,
            false,
            counts.clone(),
        )
        .unwrap();
        let room = Rc::new(Cell::new(0));
        let (task_handle, task_room) = (handle.clone(), room.clone());
        spawn(&spawner, async move {
            task_room.set(task_handle.tx_frame().await.room_mut().len());
        })
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
        let mut turns = TurnSums::default();
        while !peer.is_finished() {
            assert!(Instant::now() < deadline, "echo not back within 30 s");
            host.round(&mut turns).unwrap();
            std::thread::sleep(Duration::from_micros(50));
        }
        assert!(peer.join().unwrap() == expected, "echoed bytes differ");
        let max_frame = counts.conns.borrow()[0].max_rx_frame.get();
        assert!(
            max_frame > room.get(),
            "precondition: largest received frame {max_frame} B, TX frame room {} B",
            room.get()
        );
        drop((counts, handle, spawner));
        host.close().unwrap();
    }
}
