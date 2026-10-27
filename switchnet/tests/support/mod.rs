//! One `Net` (side A, 10.0.0.1) facing a raw conduit `Stack` peer (side B,
//! 10.0.0.2) over two lossless net_ring links, and a probe executor that polls
//! futures by hand with a counting std `Waker`.

#![allow(dead_code)]

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Wake, Waker};

use dpdk_smoltcp::{rx_pool_count, rx_reserve, Eal, StackConfig};
use switchnet::{tcp, Instant, IpEndpoint, Net, NetConfig, NetHandle, Stack};
use zhida_test_kit::pair::{self, assert_lossless, Side, BUDGET, T0};
use zhida_test_kit::ring_port::RingPort;
use zhida_test_kit::wire::Link;

pub const MAX_TCP: usize = 4;
pub const MAX_UDP: usize = 4;
pub const UDP_QUEUE: usize = 8;
/// Six full-MSS frames: the smallest TCP receive budget conduit admits.
pub const RX_CAPACITY: usize = 6 * 1460;
pub const PEER_PORT: u16 = 80;

pub fn tcp_config() -> tcp::Config {
    tcp::Config { rx_capacity: RX_CAPACITY, tx_capacity: 64 * 1024, mss_cap: None }
}

fn stack_config(side: Side) -> StackConfig<'static> {
    StackConfig { max_sockets: MAX_TCP, max_udp_sockets: MAX_UDP, ..pair::config(side) }
}

pub fn net_ip() -> switchnet::Ipv4Address {
    Side::A.ip()
}

pub fn peer_ip() -> switchnet::Ipv4Address {
    Side::B.ip()
}

pub fn peer_endpoint(port: u16) -> IpEndpoint {
    IpEndpoint::new(peer_ip().into(), port)
}

/// Fields drop in order on a failed test; `close` is the clean teardown.
pub struct Mixed {
    pub to_peer: Link,
    pub to_net: Link,
    pub net: Net,
    pub peer: Stack,
    pub now: Instant,
    fixtures: [RingPort; 2],
}

impl Mixed {
    /// `names` are short and unique among live fixtures.
    pub fn new(eal: &Eal, names: [&str; 2]) -> Self {
        let fixtures = names.map(|name| RingPort::new(name, 16, 16));
        let [net_stack, peer] = [Side::A, Side::B].map(|side| {
            let fixture = &fixtures[side.index()];
            let stack_config = stack_config(side);
            let mut config = fixture.config();
            let tcp = std::iter::repeat_n(RX_CAPACITY, MAX_TCP);
            let reserve =
                rx_reserve(config.rx_descriptors, stack_config.rx_staging, config.rx_pool.cache_size);
            config.rx_pool.count = rx_pool_count(reserve, config.mtu, tcp)
                + u32::try_from(MAX_UDP * UDP_QUEUE).unwrap();
            let port = fixture.open_with(eal, config);
            match Stack::new(port, stack_config, T0) {
                Ok(stack) => stack,
                Err((_, error)) => panic!("{error}"),
            }
        });
        let [a, b] = &fixtures;
        Self {
            to_peer: a.link_to(b),
            to_net: b.link_to(a),
            net: Net::new(net_stack, NetConfig { ingress_budget: BUDGET }),
            peer,
            now: T0,
            fixtures,
        }
    }

    pub fn handle(&self) -> NetHandle {
        self.net.handle()
    }

    /// Net turn, Net→peer, peer turn, peer→Net, at `now`. Returns whether a
    /// link moved a frame.
    pub fn round(&mut self) -> bool {
        self.net.turn(self.now).unwrap();
        let out = self.to_peer.forward();
        assert_lossless(&self.to_peer, "Net→peer");
        self.peer.turn(self.now, BUDGET).unwrap();
        let back = self.to_net.forward();
        assert_lossless(&self.to_net, "peer→Net");
        out + back > 0
    }

    /// Rounds until `done`; a round that moves nothing advances the clock to
    /// the earliest deadline. Panics after 1000 rounds.
    pub fn until(&mut self, mut done: impl FnMut(&mut Self) -> bool) {
        for _ in 0..1000 {
            if done(self) {
                return;
            }
            if !self.round() {
                self.advance();
            }
        }
        panic!("condition not reached after 1000 rounds at {}", self.now);
    }

    /// `rounds` rounds, advancing the clock whenever nothing moved.
    pub fn rounds(&mut self, rounds: usize) {
        for _ in 0..rounds {
            if !self.round() {
                self.advance();
            }
        }
    }

    fn advance(&mut self) {
        let deadline = [self.net.poll_at(), self.peer.poll_at()].into_iter().flatten().min();
        if let Some(at) = deadline {
            self.now = self.now.max(at);
        }
    }

    /// Asserts `Net::into_stack` refuses, and keeps the Net it hands back.
    pub fn into_stack_refused(self) -> Self {
        let Self { to_peer, to_net, net, peer, now, fixtures } = self;
        let net = match net.into_stack() {
            Ok(_) => panic!("into_stack succeeded while a handle or socket lives"),
            Err(net) => net,
        };
        Self { to_peer, to_net, net, peer, now, fixtures }
    }

    /// Every socket and frame the test made must already be dropped. The Net
    /// gives its Stack back, both Ports close with every mbuf returned.
    pub fn close(self) {
        let Self { mut to_peer, mut to_net, net, peer, fixtures, .. } = self;
        let stack = net.into_stack().ok().expect("a Net handle or socket is still alive");
        to_peer.free_held();
        to_net.free_held();
        let ports = [stack.into_port(), peer.into_port()];
        for fixture in &fixtures {
            fixture.drain();
            assert_eq!(fixture.in_use(), (0, 0));
            fixture.assert_returned_exactly_once();
        }
        for port in ports {
            port.try_close().unwrap();
        }
        for fixture in fixtures {
            fixture.close();
        }
    }
}

/// Peer listener on `PEER_PORT`.
pub fn peer_listen(mixed: &mut Mixed) -> dpdk_smoltcp::TcpHandle {
    let handle = mixed.peer.add_tcp(tcp_config()).unwrap();
    mixed.peer.listen(&handle, PEER_PORT).unwrap();
    handle
}

pub struct Count(AtomicUsize);

impl Wake for Count {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

/// One task's waker; counts wakes.
pub struct Probe {
    count: Arc<Count>,
    waker: Waker,
}

impl Probe {
    pub fn new() -> Self {
        let count = Arc::new(Count(AtomicUsize::new(0)));
        Self { waker: Waker::from(count.clone()), count }
    }

    pub fn wakes(&self) -> usize {
        self.count.0.load(Ordering::Relaxed)
    }

    pub fn poll<F: Future + ?Sized>(&self, future: Pin<&mut F>) -> Poll<F::Output> {
        future.poll(&mut Context::from_waker(&self.waker))
    }

    /// Wakers this probe handed out that are still alive, beside its own.
    pub fn clones(&self) -> usize {
        Arc::strong_count(&self.count) - 2
    }
}

/// Polls `future` with `probe`; while pending, waits for a wake with rounds.
pub fn drive<F: Future + ?Sized>(mixed: &mut Mixed, probe: &Probe, mut future: Pin<&mut F>) -> F::Output {
    loop {
        if let Poll::Ready(out) = probe.poll(future.as_mut()) {
            return out;
        }
        let woken = probe.wakes();
        mixed.until(|_| probe.wakes() > woken);
    }
}
