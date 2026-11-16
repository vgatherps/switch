//! One echo on the Net against a raw network stack peer, criterion group "echo": `tcp/64`
//! echoes 64 B of TCP payload as `switch-bench/src/serve.rs`'s tcp echo does; `ws/64` echoes one 64 B
//! binary message through switch-ws (`accept_ws`, `recv_with_sender`), as serve.rs's ws echo does.
//!
//! Timed per echo: the Net's receive that takes the peer's segment in and wakes the echo task, the
//! task's poll (receive, send) and the Net's send that transmits the echo. Untimed: the peer's
//! send, its turn and the links. The clock stays at `T0`: each request carries the ACK of the
//! previous echo, so no timer runs.

use std::hint::black_box;
use std::time::{Duration as Wall, Instant as Clock};

use criterion::{Criterion, criterion_group, criterion_main};
use dpdk_smoltcp::{Eal, Stack, StackConfig, TcpHandle, frame_reserve, rx_reserve};
use switch_ws::Limits;
use switch_runtime::{Runtime, Spawner};
use switch_ws::net::accept_ws;
use switch_net::{Accepting, Error, IpEndpoint, IpListenEndpoint, Net, NetConfig, NetHandle, tcp};
use switch_test_kit::pair::{self, BUDGET, Side, T0, assert_lossless};
use switch_test_kit::ring_port::RingPort;
use switch_test_kit::wire::Link;
use switch_test_kit::with_eal;

const PAYLOAD: usize = 64;
const SERVER_PORT: u16 = 9001;
const CLIENT_PORT: u16 = 49152;
/// A key from RFC 6455 §1.3; the peer does not check the accept value.
const UPGRADE: &[u8] = b"GET / HTTP/1.1\r\nHost: bench\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\
Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n";

fn tcp_config() -> tcp::Config {
    tcp::Config { rx_capacity: 6 * 1460, tx_capacity: 4096, mss_cap: None }
}

/// A Stack with one TCP slot.
fn open(eal: &Eal, fixture: &RingPort, side: Side) -> Stack {
    let stack_config = StackConfig { max_sockets: 1, max_udp_sockets: 1, ..pair::config(side) };
    let mut config = fixture.config();
    let reserve = rx_reserve(config.rx_descriptors, config.rx_burst, stack_config.rx_staging, config.rx_pool.cache_size);
    config.rx_pool.count = u32::try_from(reserve + 64).unwrap();
    let frame_share = tcp::tx_mbuf_budget(&tcp_config(), config.mtu.into()) + 64;
    config.frame_pool.count =
        u32::try_from(frame_reserve(config.tx_descriptors, stack_config.tx_pending) + frame_share).unwrap();
    match Stack::new(fixture.open_with(eal, config), stack_config, T0) {
        Ok(stack) => stack,
        Err((_, error)) => panic!("{error}"),
    }
}

/// The echo on the Net (side A), its client a raw Stack (side B), over two lossless links.
struct Bench {
    rt: Runtime,
    to_peer: Link,
    to_net: Link,
    net: Net,
    peer: Stack,
    client: TcpHandle,
    fixtures: [RingPort; 2],
}

impl Bench {
    /// The client connected to the echo task `spawn` starts on the Net's accepted stream.
    fn new(eal: &Eal, name: &str, spawn: impl FnOnce(&Spawner, NetHandle, Accepting) -> bool) -> Self {
        let fixtures = ["a", "b"].map(|side| RingPort::new(&format!("{name}{side}"), 16, 16));
        let [a, b] = &fixtures;
        let net = Net::new(open(eal, a, Side::A), NetConfig { ingress_budget: BUDGET });
        let mut peer = open(eal, b, Side::B);
        let (to_peer, to_net) = (a.link_to(b), b.link_to(a));
        let rt = Runtime::new(0);
        let handle = net.handle();
        let accepting = handle.tcp_accept(tcp_config(), IpListenEndpoint { addr: None, port: SERVER_PORT });
        assert!(spawn(&rt.spawner(), handle, accepting), "spawn");
        let client = peer.add_tcp(tcp_config()).unwrap();
        let remote = IpEndpoint::new(Side::A.ip().into(), SERVER_PORT);
        peer.connect(&client, remote, IpListenEndpoint { addr: None, port: CLIENT_PORT }).unwrap();
        let mut bench = Self { rt, to_peer, to_net, net, peer, client, fixtures };
        bench.until(|b| b.peer.tcp(&b.client).state() == tcp::State::Established);
        bench
    }

    /// Net receive, task poll, Net send, Net→peer, peer turn, peer→Net.
    fn round(&mut self) {
        self.net_side();
        self.to_peer.forward();
        assert_lossless(&self.to_peer, "Net→peer");
        self.peer.turn(T0, BUDGET).unwrap();
        self.to_net.forward();
        assert_lossless(&self.to_net, "peer→Net");
    }

    /// What one echo costs the Net: the timed part, and the function to collect under callgrind
    /// (`--toggle-collect='*net_side*'`).
    #[inline(never)]
    fn net_side(&mut self) {
        self.net.receive(T0).unwrap();
        self.rt.poll(0).unwrap();
        self.net.send().unwrap();
    }

    fn until(&mut self, mut done: impl FnMut(&mut Self) -> bool) {
        for _ in 0..1000 {
            if done(self) {
                return;
            }
            self.round();
        }
        panic!("condition not reached");
    }

    /// Queues `bytes` at the client and hands them to the Net's RX ring.
    fn request(&mut self, bytes: &[u8]) {
        assert_eq!(self.peer.tcp_mut(&self.client).send_slice(bytes), Ok(bytes.len()));
        self.peer.turn(T0, BUDGET).unwrap();
        assert_eq!(self.to_net.forward(), 1, "one segment toward the Net");
    }

    /// Takes the Net's frames in at the client; the bytes it has read.
    fn response(&mut self) -> Vec<u8> {
        self.to_peer.forward();
        assert_lossless(&self.to_peer, "Net→peer");
        self.peer.turn(T0, BUDGET).unwrap();
        pair::drain_bytes(self.peer.tcp_mut(&self.client)).unwrap_or_default()
    }

    /// Timed: what one echo of `request` costs the Net; the client must read `echo` back.
    fn echo(&mut self, request: &[u8], echo: &[u8]) -> Wall {
        self.request(request);
        let start = Clock::now();
        self.net_side();
        let timed = start.elapsed();
        assert_eq!(self.response(), echo, "the echo came back whole in one round");
        timed
    }

    /// Every task and socket dropped, then both Stacks and Ports closed.
    fn close(self) {
        let Self { mut rt, mut to_peer, mut to_net, net, peer, client, fixtures } = self;
        rt.shutdown();
        drop(client);
        let net = net.into_stack().ok().expect("a socket is still alive");
        to_peer.free_held();
        to_net.free_held();
        for (fixture, port) in fixtures.iter().zip([net.into_port(), peer.into_port()]) {
            fixture.drain();
            assert_eq!(fixture.in_use(), (0, 0));
            port.try_close().unwrap();
        }
        for fixture in fixtures {
            fixture.close();
        }
    }
}

/// serve.rs's tcp echo: each received segment back, frame by frame.
#[switch_runtime::task]
async fn tcp_echo(handle: NetHandle, accepting: Accepting) {
    let Ok(stream) = accepting.await else { return };
    let (mut rx, mut tx) = stream.split();
    loop {
        let Ok(received) = rx.recv().await else {
            return;
        };
        let mut rest = received.payload();
        while !rest.is_empty() {
            let mut frame = handle.tx_frame().await;
            let len = rest.len().min(frame.len());
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

/// serve.rs's ws echo: each message back with its kind, through switch-ws.
#[switch_runtime::task]
async fn ws_echo(handle: NetHandle, accepting: Accepting) {
    let Ok(stream) = accepting.await else { return };
    let Ok(mut ws) = accept_ws(&handle, stream, Limits::default()).await else {
        return;
    };
    while let Ok(Some((message, mut sender))) = ws.recv_with_sender().await {
        if sender.send(message.kind(), message.bytes()).await.is_err() {
            return;
        }
    }
}

fn run(c: &mut Criterion, bench: &mut Bench, name: &str, request: &[u8], echo: &[u8]) {
    let mut group = c.benchmark_group("echo");
    group.bench_function(name, |b| {
        b.iter_custom(|iters| (0..iters).map(|_| black_box(bench.echo(request, echo))).sum())
    });
    group.finish();
}

fn echo(c: &mut Criterion) {
    with_eal(|eal| {
        let payload = [0x5a; PAYLOAD];

        let mut bench = Bench::new(eal, "et", |s, h, a| s.spawn(tcp_echo(h, a)).is_ok());
        run(c, &mut bench, &format!("tcp/{PAYLOAD}"), &payload, &payload);
        bench.close();

        let mut bench = Bench::new(eal, "ew", |s, h, a| s.spawn(ws_echo(h, a)).is_ok());
        bench.request(UPGRADE);
        let mut head = Vec::new();
        bench.until(|b| {
            head.extend(b.response());
            head.ends_with(b"\r\n\r\n")
        });
        assert!(head.starts_with(b"HTTP/1.1 101"), "{}", String::from_utf8_lossy(&head));
        // Client frames are masked; a zero mask leaves the payload as it is.
        let request = [&[0x82, 0x80 | PAYLOAD as u8, 0, 0, 0, 0][..], &payload].concat();
        let echo = [&[0x82, PAYLOAD as u8][..], &payload].concat();
        run(c, &mut bench, &format!("ws/{PAYLOAD}"), &request, &echo);
        bench.close();
    })
}

criterion_group!(benches, echo);
criterion_main!(benches);
