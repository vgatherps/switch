//! One `Net` (side A, 10.0.0.1) facing a raw conduit `Stack` peer (side B,
//! 10.0.0.2) over two lossless net_ring links, with a switch `Runtime` whose
//! tasks run the switch-net futures.

#![allow(dead_code)]

use std::cell::{Cell, RefCell};
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};

use dpdk_smoltcp::smoltcp::wire::Ipv4Cidr;
use dpdk_smoltcp::{frame_reserve, rx_reserve, Eal, StackConfig};
use switch_runtime::Runtime;
use switch_net::{tcp, Instant, IpEndpoint, IpListenEndpoint, Net, NetConfig, NetHandle, Stack, TcpStream, UdpConfig};
use switch_test_kit::pair::{self, assert_lossless, Side, BUDGET, T0};
use switch_test_kit::ring_port::RingPort;
use switch_test_kit::wire::Link;

#[path = "../../src/testkit/host.rs"]
mod host;

#[derive(Clone, Copy, Debug)]
pub enum Backend {
    Conduit,
    Junction,
}

#[allow(unused_macros)]
macro_rules! both_backends {
    ($conduit:ident, $junction:ident, |$backend:ident| $body:block) => {
        #[switch_test_support::pinned_test]
        fn $conduit() {
            let $backend = support::Backend::Conduit;
            $body
        }
        #[switch_test_support::pinned_test]
        fn $junction() {
            let $backend = support::Backend::Junction;
            $body
        }
    };
}
#[allow(unused_imports)]
pub(crate) use both_backends;

pub const MAX_TCP: usize = 4;
pub const MAX_UDP: usize = 4;
pub const UDP_QUEUE: usize = 8;
/// Receive window target for these test connections.
pub const RX_CAPACITY: usize = 6 * 1460;
pub const PEER_PORT: u16 = 80;
pub const LOCAL_PORT: u16 = 49152;

pub fn tcp_config() -> tcp::Config {
    tcp::Config { rx_capacity: RX_CAPACITY, tx_capacity: 64 * 1024, mss_cap: None }
}

fn stack_config(side: Side, tcp: usize, udp: usize, owned: &'static [Ipv4Cidr]) -> StackConfig<'static> {
    StackConfig { max_sockets: tcp, max_udp_sockets: udp, owned_ipv4: owned, ..pair::config(side) }
}

pub fn net_ip() -> switch_net::Ipv4Address {
    Side::A.ip()
}

pub fn peer_ip() -> switch_net::Ipv4Address {
    Side::B.ip()
}

pub fn peer_endpoint(port: u16) -> IpEndpoint {
    IpEndpoint::new(peer_ip().into(), port)
}

/// Fields drop in order on a failed test; `close` is the clean teardown.
pub struct Mixed {
    pub rt: Runtime,
    pub to_peer: Link,
    pub to_net: Link,
    pub net: Net,
    pub peer: Stack,
    pub now: Instant,
    host: Option<host::Host>,
    fixtures: [RingPort; 2],
}

impl Mixed {
    /// `names` are short and unique among live fixtures.
    pub fn new(eal: &Eal, names: [&str; 2], backend: Backend) -> Self {
        Self::with_sockets(eal, names, MAX_TCP, MAX_UDP, backend)
    }

    /// `new` with `tcp` TCP and `udp` UDP socket slots on each side.
    pub fn with_sockets(eal: &Eal, names: [&str; 2], tcp: usize, udp: usize, backend: Backend) -> Self {
        Self::build(eal, names, tcp, udp, &[], backend)
    }

    /// `new` with the Net also owning `owned`; the peer owns only its own address.
    pub fn with_owned(eal: &Eal, names: [&str; 2], owned: &'static [Ipv4Cidr], backend: Backend) -> Self {
        Self::build(eal, names, MAX_TCP, MAX_UDP, owned, backend)
    }

    fn build(eal: &Eal, names: [&str; 2], tcp: usize, udp: usize, owned: &'static [Ipv4Cidr], backend: Backend) -> Self {
        let fixtures = names.map(|name| RingPort::new(name, 16, 16));
        let [net_stack, peer] = [Side::A, Side::B].map(|side| {
            let fixture = &fixtures[side.index()];
            let stack_config = stack_config(side, tcp, udp, if side == Side::A { owned } else { &[] });
            let mut config = fixture.config();
            // The shared RX pool covers the tests' retained 64 KiB transfer,
            // plus queued UDP datagrams and room for other TCP traffic.
            let rx_share = 64 * tcp.max(1) + udp * UDP_QUEUE;
            let frame_share = tcp::tx_mbuf_budget(&tcp_config(), config.mtu.into()) + 64;
            config.rx_pool.count = u32::try_from(
                rx_reserve(config.rx_descriptors, config.rx_burst, stack_config.rx_staging, config.rx_pool.cache_size)
                    + rx_share,
            )
            .unwrap();
            config.frame_pool.count =
                u32::try_from(frame_reserve(config.tx_descriptors, stack_config.tx_pending) + frame_share)
                    .unwrap();
            let port = fixture.open_with(eal, config);
            match Stack::new(port, stack_config, T0) {
                Ok(stack) => stack,
                Err((_, error)) => panic!("{error}"),
            }
        });
        let [a, b] = &fixtures;
        let (net, host) = match backend {
            Backend::Conduit => (Net::new(net_stack, NetConfig { ingress_budget: BUDGET }), None),
            Backend::Junction => {
                let (host, session) = host::Host::new(net_stack, T0, BUDGET, tcp + udp);
                let net = Net::remote(session, switch_net::RemoteConfig { tx_frames: 256 });
                (net, Some(host))
            }
        };
        Self {
            rt: Runtime::new(0),
            to_peer: a.link_to(b),
            to_net: b.link_to(a),
            net,
            peer,
            now: T0,
            fixtures,
            host,
        }
    }

    pub fn handle(&self) -> NetHandle {
        self.net.handle()
    }

    /// Runs `future` as a switch task; its output lands in the returned slot.
    pub fn spawn<T: 'static, F: Future<Output = T> + 'static>(&self, future: F) -> Slot<T> {
        let slot = Slot::default();
        self.rt.spawner().spawn(fill(future, slot.clone())).expect("spawn");
        slot
    }

    /// Rounds until `slot` is filled; takes its value.
    pub fn wait<T>(&mut self, slot: &Slot<T>) -> T {
        self.until(|_| slot.is_full());
        slot.take()
    }

    /// Net receive, task poll, Net send, Net→peer, peer turn, peer→Net, at `now`.
    /// Returns whether the host, Net, runtime, peer or links made progress.
    pub fn round(&mut self) -> bool {
        let moved = self.turn();
        let out = self.to_peer.forward();
        assert_lossless(&self.to_peer, "Net→peer");
        let peer = self.peer.turn(self.now, BUDGET).unwrap();
        let back = self.to_net.forward();
        assert_lossless(&self.to_net, "peer→Net");
        moved || peer.moved() || peer.tx_pending > 0 || peer.rx_staged > 0 || out + back > 0
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

    /// Net receive, one poll of the woken tasks, then Net send.
    pub fn turn(&mut self) -> bool {
        let mut moved = false;
        if self.host.is_some() {
            // Let the control thread deliver operations and replies around the Network turn.
            std::thread::sleep(std::time::Duration::from_millis(1));
            moved = self.host_turn();
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        switch_net::round(&mut self.net, &mut self.rt, self.now, 0).unwrap() || moved
    }

    /// Drive only the host, for tests of `switch_net::round` itself.
    pub fn host_turn(&mut self) -> bool {
        self.host.as_mut().is_some_and(|host| host.iteration(self.now))
    }

    fn advance(&mut self) {
        let deadline = [self.net.poll_at(), self.host.as_mut().and_then(|host| host.network.poll_at()), self.peer.poll_at()]
            .into_iter().flatten().min();
        if let Some(at) = deadline {
            self.now = self.now.max(at);
        }
    }

    /// Asserts `Net::into_stack` refuses, and keeps the Net it hands back.
    pub fn into_stack_refused(self) -> Self {
        let Self { rt, to_peer, to_net, net, peer, now, fixtures, host } = self;
        let net = match net.into_stack() {
            Ok(_) => panic!("into_stack succeeded while a handle or socket lives"),
            Err(net) => net,
        };
        Self { rt, to_peer, to_net, net, peer, now, fixtures, host }
    }

    /// Every socket and frame the test made must already be dropped. The Net
    /// gives its Stack back, both Ports close with every mbuf returned.
    pub fn close(mut self) {
        self.rt.shutdown();
        self.net.assert_remote_frames_returned();
        self.close_after_stacks(|| {});
    }

    /// `close`, running `held` once both Stacks are torn down and before the
    /// mbuf checks: it gets frames still held and must drop them.
    /// Skips the remote leak check because this path intentionally keeps RX frames past teardown.
    pub fn close_after_stacks(self, held: impl FnOnce()) {
        let Self { mut rt, mut to_peer, mut to_net, net, peer, fixtures, host, now } = self;
        rt.shutdown();
        let stack = match host {
            None => net.into_stack().ok().expect("a Net handle or socket is still alive"),
            Some(host) => {
                let session = net.into_session().ok().expect("a Net handle or socket is still alive");
                drop(session);
                host.finish(now)
            }
        };
        to_peer.free_held();
        to_net.free_held();
        let ports = [stack.into_port(), peer.into_port()];
        held();
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

pub fn local(port: u16) -> IpListenEndpoint {
    IpListenEndpoint { addr: None, port }
}

/// `port` on one of the Net's addresses only.
pub fn local_at(addr: switch_net::Ipv4Address, port: u16) -> IpListenEndpoint {
    IpListenEndpoint { addr: Some(addr.into()), port }
}

pub fn udp_config(port: u16) -> UdpConfig {
    UdpConfig { port, local_addr: None, rx_queue: UDP_QUEUE, tx_checksum: true, broadcast: false }
}

/// `udp_config` bound to one of the Net's addresses.
pub fn udp_config_at(addr: switch_net::Ipv4Address, port: u16) -> UdpConfig {
    UdpConfig { local_addr: Some(addr), ..udp_config(port) }
}

pub fn bind(mixed: &mut Mixed, config: UdpConfig) -> switch_net::UdpSocket {
    let slot = mixed.spawn(mixed.handle().udp_bind(config));
    mixed.rt.poll(0).expect("poll binding once before turning the Net");
    mixed.wait(&slot).expect("bind UDP")
}

/// Net connects from `port` to the peer's listener in a task; drives until
/// both ends are established.
pub fn connect(mixed: &mut Mixed, port: u16) -> (TcpStream, dpdk_smoltcp::TcpHandle) {
    connect_with(mixed, port, tcp_config())
}

/// `connect` with the Net socket's `config`.
pub fn connect_with(mixed: &mut Mixed, port: u16, config: tcp::Config) -> (TcpStream, dpdk_smoltcp::TcpHandle) {
    let peer = peer_listen(mixed);
    let connecting = mixed.handle().tcp_connect(config, peer_endpoint(PEER_PORT), local(port));
    let slot = mixed.spawn(connecting);
    let stream = mixed.wait(&slot).unwrap();
    mixed.until(|m| m.peer.tcp(&peer).state() == tcp::State::Established);
    (stream, peer)
}

pub fn peer_send(mixed: &mut Mixed, peer: &dpdk_smoltcp::TcpHandle, data: &[u8]) {
    assert_eq!(mixed.peer.tcp_mut(peer).send_slice(data), Ok(data.len()));
}

/// Peer sends `payload` to the Net's `port`, turning until the Net's MAC is resolved.
pub fn peer_udp_send(mixed: &mut Mixed, peer: &dpdk_smoltcp::UdpHandle, port: u16, payload: &[u8]) {
    peer_udp_send_to(mixed, peer, net_ip(), port, payload);
}

/// `peer_udp_send` to the Net's address `addr`, which the peer resolves on its own.
pub fn peer_udp_send_to(
    mixed: &mut Mixed,
    peer: &dpdk_smoltcp::UdpHandle,
    addr: switch_net::Ipv4Address,
    port: u16,
    payload: &[u8],
) {
    let to = Some(IpEndpoint::new(addr.into(), port));
    mixed.until(|m| match m.peer.udp_send(peer, to, payload) {
        Ok(()) => true,
        Err(error) => {
            assert_eq!(error, dpdk_smoltcp::UdpSendError::NeighborPending);
            false
        }
    });
}

/// Every in-order byte the peer has received so far.
pub fn peer_drain(mixed: &mut Mixed, peer: &dpdk_smoltcp::TcpHandle, into: &mut Vec<u8>) {
    while let Ok(Some(frame)) = mixed.peer.tcp_mut(peer).recv() {
        into.extend_from_slice(frame.payload());
    }
}

/// Runs `future` and puts its output in `out`.
#[switch_runtime::task]
async fn fill<T: 'static, F: Future<Output = T> + 'static>(future: F, out: Slot<T>) {
    *out.0.borrow_mut() = Some(future.await);
}

/// A spawned task's output.
pub struct Slot<T>(Rc<RefCell<Option<T>>>);

impl<T> Default for Slot<T> {
    fn default() -> Self {
        Self(Rc::new(RefCell::new(None)))
    }
}

impl<T> Clone for Slot<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<T> Slot<T> {
    pub fn is_full(&self) -> bool {
        self.0.borrow().is_some()
    }

    pub fn take(&self) -> T {
        self.0.borrow_mut().take().expect("task has not finished")
    }
}

/// Counts every poll of `future` into `polls`.
pub struct Counted<F> {
    pub future: Pin<Box<F>>,
    pub polls: Rc<Cell<usize>>,
}

impl<F: Future> Future for Counted<F> {
    type Output = F::Output;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<F::Output> {
        self.polls.set(self.polls.get() + 1);
        self.future.as_mut().poll(cx)
    }
}

/// `future` and a counter of its polls.
pub fn counted<F: Future>(future: F) -> (Counted<F>, Rc<Cell<usize>>) {
    let polls = Rc::new(Cell::new(0));
    (Counted { future: Box::pin(future), polls: polls.clone() }, polls)
}

/// Polls `future` once, which must be pending, then drops it.
pub async fn cancel_after_one_poll<F: Future>(future: F) {
    let mut future = std::pin::pin!(future);
    std::future::poll_fn(|cx| {
        assert!(future.as_mut().poll(cx).is_pending(), "future ready on its first poll");
        Poll::Ready(())
    })
    .await;
}
