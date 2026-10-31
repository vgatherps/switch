//! `Net::turn` against the number of parked tasks, criterion groups
//! "stack_turn", "net_turn_idle" and "net_turn_ready".
//!
//! A task parked on a socket arms that socket's readiness token; a Stack
//! change that can make the socket ready fires it, and `Net::turn` checks only
//! the sockets whose tokens fired since the last turn. A turn's cost should
//! therefore not depend on how many tasks are parked.
//! - stack_turn/N: N UDP sockets bound directly on a Stack, nothing arriving;
//!   one bare `Stack::turn`. The floor for the idle cases.
//! - net_turn_idle/{udp,tcp}/N: N sockets on a Net, a switch task parked in
//!   `recv` on each (UDP: bound sockets; TCP: connections established to a
//!   raw conduit Stack peer). Nothing arrives, no token fires: one
//!   `Net::turn`, which finds an empty fired list.
//! - net_turn_ready/{udp,tcp}/N: the same, with one datagram (UDP) or one
//!   1-byte segment (TCP) for the middle socket on the Net's RX ring before
//!   each timed turn. The turn takes it in, fires that socket's token, checks
//!   that one socket and wakes its task. Untimed per iteration: the peer's
//!   send and turn, the link to the Net, the woken task's poll, and the link
//!   back; for TCP, whenever the peer has no free send frame slot (its bytes
//!   wait for the Net's delayed ACK), rounds with the clock past the 10 ms
//!   delayed ACK until nothing moves.
//!
//! Two net_ring ports, 16-slot rings; the clock stays at `T0` except for the
//! TCP flushes. UDP keeps the clock still, so the peer's neighbor entry for
//! the Net never expires.

use std::future::Future;
use std::hint::black_box;
use std::time::{Duration as Wall, Instant as Clock};

use criterion::{criterion_group, criterion_main, Criterion};
use dpdk_smoltcp::smoltcp::time::Duration;
use dpdk_smoltcp::{
    frame_reserve, rx_reserve, Eal, Port, Stack, StackConfig, TcpHandle, UdpHandle,
};
use switch::Runtime;
use switchnet::{boxed, tcp, Instant, IpEndpoint, IpListenEndpoint, Net, NetConfig, UdpConfig};
use zhida_test_kit::pair::{self, assert_lossless, Side, BUDGET, T0};
use zhida_test_kit::ring_port::RingPort;
use zhida_test_kit::wire::Link;
use zhida_test_kit::with_eal;

const SIZES: [usize; 5] = [0, 1, 16, 1000, 4096];
const RX_CAPACITY: usize = 6 * 1460;
const TX_CAPACITY: usize = 4096;
const UDP_QUEUE: usize = 4;
/// Connections opened per wave: a 16-slot ring holds 15 frames.
const WAVE: usize = 8;
const SERVER_BASE: u16 = 10_000;
const CLIENT_BASE: u16 = 20_000;
const UDP_BASE: u16 = 40_000;
const PEER_UDP: u16 = 6_000;

fn tcp_config() -> tcp::Config {
    tcp::Config { rx_capacity: RX_CAPACITY, tx_capacity: TX_CAPACITY, mss_cap: None }
}

fn udp_config(port: u16) -> UdpConfig {
    UdpConfig { port, local_addr: None, rx_queue: UDP_QUEUE, tx_checksum: true, broadcast: false }
}

/// A Stack with `tcp` TCP and `udp` UDP slots (at least one of each), pools
/// sized for every socket.
fn open(eal: &Eal, fixture: &RingPort, side: Side, tcp: usize, udp: usize) -> Stack {
    let stack_config = StackConfig { max_sockets: tcp.max(1), max_udp_sockets: udp.max(1), ..pair::config(side) };
    let mut config = fixture.config();
    // Every socket's window and UDP queue full at once: the bench asserts lossless links.
    let rx_share = tcp * tcp::rx_mbuf_budget(&tcp_config(), config.mtu.into()) + udp.max(1) * UDP_QUEUE;
    let frame_share = tcp * tcp::tx_mbuf_budget(&tcp_config(), config.mtu.into()) + 64;
    let reserve = rx_reserve(config.rx_descriptors, config.rx_burst, stack_config.rx_staging, config.rx_pool.cache_size);
    config.rx_pool.count = u32::try_from(reserve + rx_share).unwrap();
    config.frame_pool.count =
        u32::try_from(frame_reserve(config.tx_descriptors, stack_config.tx_pending) + frame_share).unwrap();
    match Stack::new(fixture.open_with(eal, config), stack_config, T0) {
        Ok(stack) => stack,
        Err((_, error)) => panic!("{error}"),
    }
}

fn close_port(fixture: &RingPort, port: Port) {
    fixture.drain();
    assert_eq!(fixture.in_use(), (0, 0));
    port.try_close().unwrap();
}

/// The Net (side A) and a raw Stack peer (side B) over two lossless links.
struct Bench {
    rt: Runtime,
    to_peer: Link,
    to_net: Link,
    net: Net,
    peer: Stack,
    now: Instant,
    tcp: bool,
    fixtures: [RingPort; 2],
}

impl Bench {
    fn new(eal: &Eal, kind: &str, n: usize, tcp: bool) -> Self {
        let fixtures = ["a", "b"].map(|side| RingPort::new(&format!("{kind}{n}{side}"), 16, 16));
        let (tcp_slots, udp_slots) = if tcp { (n, 0) } else { (0, n) };
        let net = open(eal, &fixtures[0], Side::A, tcp_slots, udp_slots);
        let peer = open(eal, &fixtures[1], Side::B, tcp_slots, 1);
        let [a, b] = &fixtures;
        Self {
            rt: Runtime::new(0),
            to_peer: a.link_to(b),
            to_net: b.link_to(a),
            net: Net::new(net, NetConfig { ingress_budget: BUDGET }),
            peer,
            now: T0,
            tcp,
            fixtures,
        }
    }

    fn spawn(&self, future: impl Future<Output = ()> + 'static) {
        self.rt.spawner().spawn(boxed(Box::pin(future))).expect("spawn");
    }

    /// Net turn, task poll, Net→peer, peer turn, peer→Net; whether a frame moved.
    fn round(&mut self) -> bool {
        self.net.turn(self.now).unwrap();
        self.rt.poll(0).unwrap();
        let out = self.to_peer.forward();
        assert_lossless(&self.to_peer, "Net→peer");
        self.peer.turn(self.now, BUDGET).unwrap();
        let back = self.to_net.forward();
        assert_lossless(&self.to_net, "peer→Net");
        out + back > 0
    }

    fn until(&mut self, mut done: impl FnMut(&mut Self) -> bool) {
        for _ in 0..10_000 {
            if done(self) {
                return;
            }
            if !self.round() {
                let deadline = [self.net.poll_at(), self.peer.poll_at()].into_iter().flatten().min();
                if let Some(at) = deadline {
                    self.now = self.now.max(at);
                }
            }
        }
        panic!("condition not reached");
    }

    /// TCP: the clock past the delayed ACK; then rounds until nothing moves.
    fn flush(&mut self) {
        if self.tcp {
            self.now += Duration::from_millis(11);
        }
        for _ in 0..16 {
            if !self.round() {
                return;
            }
        }
        panic!("frames still moving after 16 rounds");
    }

    /// Every task and socket dropped, then both Stacks and Ports closed.
    fn close(self) {
        let Self { mut rt, mut to_peer, mut to_net, net, peer, fixtures, .. } = self;
        rt.shutdown();
        let net = net.into_stack().ok().expect("a socket is still alive");
        to_peer.free_held();
        to_net.free_held();
        close_port(&fixtures[0], net.into_port());
        close_port(&fixtures[1], peer.into_port());
        for fixture in fixtures {
            fixture.close();
        }
    }
}

/// How the peer makes Net socket `target` ready.
enum Source {
    Tcp(Vec<TcpHandle>),
    Udp(UdpHandle),
}

impl Source {
    /// One byte or one datagram for `target`, on the Net's RX ring. A TCP byte
    /// takes a peer frame slot until the Net acks it: when none is free, flush.
    fn send(&self, bench: &mut Bench, target: usize) {
        match self {
            Source::Tcp(peers) => {
                if bench.peer.tcp(&peers[target]).frame_send_room() == 0 {
                    bench.flush();
                }
                assert_eq!(bench.peer.tcp_mut(&peers[target]).send_slice(b"x"), Ok(1));
            }
            Source::Udp(handle) => {
                let to = Some(IpEndpoint::new(Side::A.ip().into(), UDP_BASE + target as u16));
                bench.peer.udp_send(handle, to, b"x").unwrap();
            }
        }
        bench.peer.turn(bench.now, BUDGET).unwrap();
        assert_eq!(bench.to_net.forward(), 1, "one frame toward the Net");
    }
}

/// `n` connections, the Net's side each read by a task parked in `recv`.
fn setup_tcp(eal: &Eal, n: usize) -> (Bench, Source) {
    let mut bench = Bench::new(eal, "bt", n, true);
    let handle = bench.net.handle();
    let mut peers = Vec::with_capacity(n);
    for wave in (0..n).collect::<Vec<_>>().chunks(WAVE) {
        let start = peers.len();
        for &id in wave {
            let id = u16::try_from(id).unwrap();
            let peer = bench.peer.add_tcp(tcp_config()).unwrap();
            bench.peer.listen(&peer, SERVER_BASE + id).unwrap();
            peers.push(peer);
            let remote = IpEndpoint::new(Side::B.ip().into(), SERVER_BASE + id);
            let local = IpListenEndpoint { addr: None, port: CLIENT_BASE + id };
            let connecting = handle.tcp_connect(tcp_config(), remote, local).unwrap();
            bench.spawn(async move {
                let mut stream = connecting.await.expect("connect");
                loop {
                    drop(black_box(stream.recv().await.expect("recv")));
                }
            });
        }
        bench.until(|b| peers[start..].iter().all(|p| b.peer.tcp(p).state() == tcp::State::Established));
    }
    bench.flush();
    (bench, Source::Tcp(peers))
}

/// `n` bound sockets, each read by a task parked in `recv`.
fn setup_udp(eal: &Eal, n: usize) -> (Bench, Source) {
    let mut bench = Bench::new(eal, "bu", n, false);
    let handle = bench.net.handle();
    for id in 0..n {
        let mut socket = handle.udp_bind(udp_config(UDP_BASE + u16::try_from(id).unwrap())).unwrap();
        bench.spawn(async move {
            loop {
                drop(black_box(socket.recv().await));
            }
        });
    }
    let peer = bench.peer.add_udp(udp_config(PEER_UDP)).unwrap();
    bench.round();
    if n > 0 {
        // The first send waits for ARP.
        let to = Some(IpEndpoint::new(Side::A.ip().into(), UDP_BASE));
        bench.until(|b| match b.peer.udp_send(&peer, to, b"w") {
            Ok(()) => true,
            Err(error) => {
                assert_eq!(error, dpdk_smoltcp::UdpSendError::NeighborPending);
                false
            }
        });
        bench.flush();
    }
    (bench, Source::Udp(peer))
}

fn stack_turn(c: &mut Criterion) {
    with_eal(|eal| {
        let mut group = c.benchmark_group("stack_turn");
        for n in SIZES {
            let fixture = RingPort::new(&format!("bs{n}"), 16, 16);
            let mut stack = open(eal, &fixture, Side::A, 0, n);
            let handles: Vec<_> =
                (0..n).map(|i| stack.add_udp(udp_config(UDP_BASE + i as u16)).unwrap()).collect();
            group.bench_function(n.to_string(), |b| b.iter(|| black_box(stack.turn(black_box(T0), BUDGET).unwrap())));
            drop(handles);
            close_port(&fixture, stack.into_port());
            fixture.close();
        }
        group.finish();
    })
}

fn net_turn(c: &mut Criterion) {
    with_eal(|eal| {
        for (kind, setup) in [("udp", setup_udp as fn(&Eal, usize) -> (Bench, Source)), ("tcp", setup_tcp)] {
            for n in SIZES {
                let (mut bench, source) = setup(eal, n);
                assert_eq!(bench.rt.poll(0).unwrap().tasks_polled, 0, "every task parked");

                let mut idle = c.benchmark_group("net_turn_idle");
                idle.bench_function(format!("{kind}/{n}"), |b| {
                    let now = bench.now;
                    b.iter(|| black_box(bench.net.turn(black_box(now)).unwrap()))
                });
                idle.finish();
                assert_eq!(bench.rt.poll(0).unwrap().tasks_polled, 0, "an idle turn woke a task");

                if n > 0 {
                    let target = n / 2;
                    let mut ready = c.benchmark_group("net_turn_ready");
                    ready.sample_size(10).warm_up_time(Wall::from_millis(200)).measurement_time(Wall::from_millis(500));
                    ready.bench_function(format!("{kind}/{n}"), |b| {
                        b.iter_custom(|iters| {
                            let mut timed = Wall::ZERO;
                            for _ in 0..iters {
                                source.send(&mut bench, target);
                                let start = Clock::now();
                                black_box(bench.net.turn(black_box(bench.now)).unwrap());
                                timed += start.elapsed();
                                assert_eq!(bench.rt.poll(0).unwrap().tasks_polled, 1, "exactly the ready task woke");
                                bench.to_peer.forward();
                            }
                            if bench.tcp {
                                bench.flush();
                            }
                            timed
                        })
                    });
                    ready.finish();
                }
                drop(source);
                bench.close();
            }
        }
    })
}

criterion_group!(benches, stack_turn, net_turn);
criterion_main!(benches);
