//! Seeded random walk over application calls, peer segments, time and device behavior, with the
//! TCP set's bookkeeping and the observable guarantees checked after every event. Application
//! calls carry no poll between them. A failure prints the seed and the event list; rerun one seed
//! with `TCP_WALK_SEED=<seed>`.

use super::*;
use crate::iface::{OwnedIpv4, ReadySide, SocketHandle};
use crate::socket::tcp;
use crate::tests::TestingDevice;
use std::collections::BTreeMap;
use std::format;
use std::string::String;
use std::vec::Vec;

const SEEDS: u64 = 2000;
const STEPS: usize = 200;
const APPS: usize = 8;

/// The interface address.
const X: Ipv4Address = Ipv4Address::new(192, 168, 1, 1);
/// Owned, not an interface address.
const Y: Ipv4Address = Ipv4Address::new(10, 9, 0, 1);
/// Neither.
const Z: Ipv4Address = Ipv4Address::new(10, 8, 0, 1);
const LOCALS: [Ipv4Address; 3] = [X, Y, Z];
const PEERS: [Ipv4Address; 2] = [Ipv4Address::new(192, 168, 1, 2), Ipv4Address::new(192, 168, 1, 3)];
const PEER_MACS: [EthernetAddress; 2] = [
    EthernetAddress([0x02, 0, 0, 0, 0, 0x02]),
    EthernetAddress([0x02, 0, 0, 0, 0, 0x03]),
];
const LOCAL_PORTS: [u16; 2] = [1000, 1001];
const REMOTE_PORTS: [u16; 2] = [2000, 2001];
const STEP_TIMES: [Duration; 7] = [
    Duration::ZERO,
    Duration::from_millis(1),
    Duration::from_millis(1),
    Duration::from_millis(1001),
    Duration::from_millis(1001),
    Duration::from_millis(10_001),
    Duration::from_millis(60_001),
];

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }

    fn pick<T: Copy>(&mut self, items: &[T]) -> T {
        items[self.below(items.len())]
    }
}

/// A refusal count in front of `TestingDevice`.
struct WalkDevice {
    inner: TestingDevice,
    refuse: usize,
}

impl Device for WalkDevice {
    type RxToken<'a> = <TestingDevice as Device>::RxToken<'a>;
    type TxToken<'a> = <TestingDevice as Device>::TxToken<'a>;
    type FrameToken<'a> = <TestingDevice as Device>::FrameToken<'a>;

    fn capabilities(&self) -> DeviceCapabilities {
        self.inner.capabilities()
    }

    fn receive(&mut self, timestamp: Instant) -> Option<(Self::RxToken<'_>, Self::TxToken<'_>)> {
        self.inner.receive(timestamp)
    }

    fn transmit(&mut self, timestamp: Instant) -> Option<Self::TxToken<'_>> {
        if self.refuse > 0 {
            self.refuse -= 1;
            return None;
        }
        self.inner.transmit(timestamp)
    }

    fn transmit_frame(&mut self, timestamp: Instant) -> Option<Self::FrameToken<'_>> {
        if self.refuse > 0 {
            self.refuse -= 1;
            return None;
        }
        self.inner.transmit_frame(timestamp)
    }
}

/// What a readiness token's waiter can observe.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Readiness {
    state: tcp::State,
    is_open: bool,
    can_recv: bool,
    can_send: bool,
    may_recv: bool,
    may_send: bool,
}

impl Readiness {
    fn of(socket: &tcp::Socket) -> Self {
        Self {
            state: socket.state(),
            is_open: socket.is_open(),
            can_recv: socket.can_recv(),
            can_send: socket.can_send(),
            may_recv: socket.may_recv(),
            may_send: socket.may_send(),
        }
    }

    /// A change the waiter must be woken for: the state, or a predicate becoming true. A
    /// predicate turning false needs no wake.
    fn missed(&self, now: &Self) -> bool {
        let rose = |was: bool, is: bool| !was && is;
        self.state != now.state
            || self.is_open != now.is_open
            || rose(self.can_recv, now.can_recv)
            || rose(self.can_send, now.can_send)
            || rose(self.may_recv, now.may_recv)
            || rose(self.may_send, now.may_send)
    }
}

struct App {
    handle: SocketHandle,
    orphaned: bool,
    last_tuple: Option<(IpEndpoint, IpEndpoint)>,
}

struct Armed {
    app: usize,
    side: ReadySide,
    at_arm: Readiness,
}

/// A segment we emitted, from `local` to `remote`.
#[derive(Debug, Clone, Copy)]
struct Sent {
    local: IpEndpoint,
    remote: IpEndpoint,
    control: TcpControl,
    seq: TcpSeqNumber,
    ack: Option<TcpSeqNumber>,
    len: usize,
}

impl Sent {
    /// Sequence space the segment used.
    fn end(&self) -> TcpSeqNumber {
        self.seq + self.len + usize::from(matches!(self.control, TcpControl::Syn | TcpControl::Fin))
    }
}

#[derive(Debug, Clone)]
enum Event {
    Connect { app: usize, local: Ipv4Address, local_port: u16, remote: IpEndpoint },
    Reconnect { app: usize },
    Listen { app: usize, addr: Option<Ipv4Address>, port: u16 },
    Abort { app: usize },
    Close { app: usize },
    Orphan { app: usize },
    Send { app: usize, len: usize },
    Recv { app: usize },
    Replace { app: usize },
    Arm { app: usize, side: ReadySide, token: u32 },
    Disarm { app: usize, side: ReadySide },
    Segment {
        peer: usize,
        src: IpEndpoint,
        dst: IpEndpoint,
        control: TcpControl,
        seq: TcpSeqNumber,
        ack: Option<TcpSeqNumber>,
        len: usize,
    },
    ArpReply { peer: usize },
    Poll,
    PollIngressSingle,
    PollEgress,
    PollAt,
    Advance(Duration),
    Refuse(usize),
    NeighborFlush,
}

fn socket() -> tcp::Socket<'static> {
    tcp::Socket::test_socket(1024, 1024)
}

struct Walk {
    medium: Medium,
    iface: Interface,
    sockets: TcpSet<'static>,
    device: WalkDevice,
    apps: Vec<App>,
    armed: BTreeMap<u32, Armed>,
    next_token: u32,
    next_port: u16,
    sent: Vec<Sent>,
    now: Instant,
    last_egress: Option<Instant>,
}

impl Walk {
    fn new(medium: Medium) -> Self {
        let (mut iface, mut sockets, device) = crate::tests::setup(medium);
        iface.set_owned_ipv4(OwnedIpv4::new(&[Ipv4Cidr::new(Y, 32)], Ipv4Cidr::new(X, 24), None).unwrap());
        let apps = (0..APPS)
            .map(|_| App { handle: sockets.add(socket()), orphaned: false, last_tuple: None })
            .collect();
        Self {
            medium,
            iface,
            sockets,
            device: WalkDevice { inner: device, refuse: 0 },
            apps,
            armed: BTreeMap::new(),
            next_token: 0,
            next_port: 3000,
            sent: Vec::new(),
            now: Instant::ZERO,
            last_egress: None,
        }
    }

    fn live_app(&self, rng: &mut Rng) -> Option<usize> {
        let live: Vec<usize> = (0..APPS).filter(|&i| !self.apps[i].orphaned).collect();
        (!live.is_empty()).then(|| rng.pick(&live))
    }

    fn generate(&mut self, rng: &mut Rng) -> Event {
        let roll = rng.below(100);
        if roll < 35
            && let Some(app) = self.live_app(rng)
        {
            return match rng.below(20) {
                0..=2 => {
                    let local = rng.pick(&[X, X, Y, Y, Z]);
                    let local_port = if rng.below(3) == 0 {
                        self.next_port += 1;
                        self.next_port
                    } else {
                        rng.pick(&LOCAL_PORTS)
                    };
                    let remote = IpEndpoint::new(rng.pick(&PEERS).into(), rng.pick(&REMOTE_PORTS));
                    Event::Connect { app, local, local_port, remote }
                }
                3 => Event::Reconnect { app },
                4 | 5 => Event::Listen {
                    app,
                    addr: rng.pick(&[None, Some(X), Some(Y)]),
                    port: rng.pick(&LOCAL_PORTS),
                },
                6 => Event::Abort { app },
                7 | 8 => Event::Close { app },
                9 => Event::Orphan { app },
                10..=12 => Event::Send { app, len: 1 + rng.below(600) },
                13 | 14 => Event::Recv { app },
                15 => Event::Replace { app },
                16 | 17 => {
                    self.next_token += 1;
                    Event::Arm { app, side: rng.pick(&[ReadySide::Rx, ReadySide::Tx]), token: self.next_token }
                }
                _ => Event::Disarm { app, side: rng.pick(&[ReadySide::Rx, ReadySide::Tx]) },
            };
        }
        if roll < 75 {
            if self.medium == Medium::Ethernet && rng.below(8) == 0 {
                return Event::ArpReply { peer: rng.below(PEERS.len()) };
            }
            return self.peer_segment(rng);
        }
        match rng.below(20) {
            0..=5 => Event::Poll,
            6..=8 => Event::PollIngressSingle,
            9..=11 => Event::PollEgress,
            12 => Event::PollAt,
            13..=16 => Event::Advance(rng.pick(&STEP_TIMES)),
            17 => Event::Refuse(1 + rng.below(3)),
            18 if self.medium == Medium::Ethernet => Event::NeighborFlush,
            _ => Event::Poll,
        }
    }

    /// A peer segment: mostly the reply a peer would send to one of our recent segments, else a
    /// segment with a random control and sequence numbers near the ones the tuple expects, else a
    /// SYN to one of our ports.
    fn peer_segment(&mut self, rng: &mut Rng) -> Event {
        let roll = rng.below(10);
        if roll < 7 && !self.sent.is_empty() {
            let ours = self.sent[self.sent.len() - 1 - rng.below(self.sent.len().min(6))];
            let peer = PEERS.iter().position(|&p| IpAddress::from(p) == ours.remote.addr).unwrap();
            let expected = ours.ack.unwrap_or(TcpSeqNumber(rng.next() as i32));
            let (control, seq, ack, len) = if roll < 5 {
                match (ours.control, ours.ack) {
                    (TcpControl::Rst, _) => (TcpControl::None, expected, Some(ours.end()), 0),
                    (TcpControl::Syn, None) => {
                        (TcpControl::Syn, TcpSeqNumber(rng.next() as i32), Some(ours.end()), 0)
                    }
                    (TcpControl::Fin, _) => {
                        (rng.pick(&[TcpControl::None, TcpControl::Fin]), expected, Some(ours.end()), 0)
                    }
                    _ => (TcpControl::None, expected, Some(ours.end()), rng.pick(&[0, 0, 1, 200, 1400])),
                }
            } else {
                let control = rng.pick(&[
                    TcpControl::Syn,
                    TcpControl::SynFin,
                    TcpControl::None,
                    TcpControl::Fin,
                    TcpControl::Rst,
                ]);
                let offset = rng.pick(&[0i32, 0, 1, 3, 1000, -1, -100_000]);
                let seq = TcpSeqNumber(expected.0.wrapping_add(offset));
                let ack = match control {
                    TcpControl::Syn | TcpControl::SynFin if rng.below(2) == 0 => None,
                    _ => Some(TcpSeqNumber(ours.end().0.wrapping_add(rng.pick(&[0i32, 0, 1, -1])))),
                };
                (control, seq, ack, rng.pick(&[0, 0, 1, 100]))
            };
            return Event::Segment { peer, src: ours.remote, dst: ours.local, control, seq, ack, len };
        }
        let peer = rng.below(PEERS.len());
        Event::Segment {
            peer,
            src: IpEndpoint::new(PEERS[peer].into(), rng.pick(&REMOTE_PORTS)),
            dst: IpEndpoint::new(rng.pick(&LOCALS).into(), rng.pick(&LOCAL_PORTS)),
            control: rng.pick(&[TcpControl::Syn, TcpControl::Syn, TcpControl::SynFin]),
            seq: TcpSeqNumber(rng.next() as i32),
            ack: None,
            len: 0,
        }
    }

    fn apply(&mut self, event: &Event) {
        let now = self.now;
        match *event {
            Event::Connect { app, local, local_port, remote } => {
                let result = self.sockets.get_mut(self.apps[app].handle).connect(
                    self.iface.context(),
                    remote,
                    (IpAddress::from(local), local_port),
                );
                if result.is_ok() {
                    self.apps[app].last_tuple = Some((IpEndpoint::new(local.into(), local_port), remote));
                }
                assert!(result.is_err() || local != Z, "connect from unowned {local} succeeded");
            }
            Event::Reconnect { app } => {
                if let Some((local, remote)) = self.apps[app].last_tuple {
                    let _ = self.sockets.get_mut(self.apps[app].handle).connect(
                        self.iface.context(),
                        remote,
                        local,
                    );
                }
            }
            Event::Listen { app, addr, port } => {
                let endpoint = IpListenEndpoint { addr: addr.map(IpAddress::from), port };
                let _ = self.sockets.get_mut(self.apps[app].handle).listen(endpoint);
            }
            Event::Abort { app } => self.sockets.get_mut(self.apps[app].handle).abort(),
            Event::Close { app } => self.sockets.get_mut(self.apps[app].handle).close(),
            Event::Orphan { app } => {
                self.sockets.orphan(self.apps[app].handle, now);
                self.apps[app].orphaned = true;
                self.armed.retain(|_, armed| armed.app != app);
            }
            Event::Send { app, len } => {
                let data = std::vec![0xa5; len];
                let _ = self.sockets.get_mut(self.apps[app].handle).send_slice(&data);
            }
            Event::Recv { app } => {
                let _ = self.sockets.get_mut(self.apps[app].handle).recv();
            }
            Event::Replace { app } => self.replace(app),
            Event::Arm { app, side, token } => {
                self.armed.retain(|_, armed| armed.app != app || armed.side != side);
                let at_arm = Readiness::of(self.sockets.get(self.apps[app].handle));
                self.sockets.arm(self.apps[app].handle, side, token);
                self.armed.insert(token, Armed { app, side, at_arm });
            }
            Event::Disarm { app, side } => {
                let held = self.armed.iter().find(|(_, a)| a.app == app && a.side == side).map(|(&t, _)| t);
                let was_armed = self.sockets.disarm(self.apps[app].handle, side);
                assert_eq!(was_armed, held.is_some(), "disarm of an {}armed token", if held.is_some() { "" } else { "un" });
                if let Some(token) = held {
                    self.armed.remove(&token);
                }
            }
            Event::Segment { peer, src, dst, control, seq, ack, len } => {
                let payload = std::vec![0x5a; len];
                let repr = TcpRepr {
                    src_port: src.port,
                    dst_port: dst.port,
                    control,
                    seq_number: seq,
                    ack_number: ack,
                    window_len: 4096,
                    window_scale: None,
                    max_seg_size: None,
                    sack_permitted: false,
                    sack_ranges: [None, None, None],
                    timestamp: None,
                    payload: &payload,
                };
                let IpAddress::Ipv4(src_addr) = src.addr;
                let IpAddress::Ipv4(dst_addr) = dst.addr;
                let ip = Ipv4Repr {
                    src_addr,
                    dst_addr,
                    next_header: IpProtocol::Tcp,
                    payload_len: repr.buffer_len(),
                    hop_limit: 64,
                };
                let mut bytes = std::vec![0; ip.buffer_len() + repr.buffer_len()];
                ip.emit(&mut Ipv4Packet::new_unchecked(&mut bytes), &ChecksumCapabilities::default());
                repr.emit(
                    &mut TcpPacket::new_unchecked(&mut bytes[ip.buffer_len()..]),
                    &src.addr,
                    &dst.addr,
                    &ChecksumCapabilities::default(),
                );
                self.queue_rx(peer, EthernetProtocol::Ipv4, bytes);
            }
            Event::ArpReply { peer } => {
                let arp = ArpRepr::EthernetIpv4 {
                    operation: ArpOperation::Reply,
                    source_hardware_addr: PEER_MACS[peer],
                    source_protocol_addr: PEERS[peer],
                    target_hardware_addr: self.iface.inner.hardware_addr.ethernet_or_panic(),
                    target_protocol_addr: X,
                };
                let mut bytes = std::vec![0; arp.buffer_len()];
                arp.emit(&mut ArpPacket::new_unchecked(&mut bytes));
                self.queue_rx(peer, EthernetProtocol::Arp, bytes);
            }
            Event::Poll => {
                self.iface.poll(now, &mut self.device, &mut self.sockets);
                self.last_egress = Some(now);
                let mut reaped = Vec::new();
                self.sockets.take_reapable(|handle| reaped.push(handle));
                for handle in reaped {
                    let app = self.apps.iter().position(|a| a.handle == handle).expect("reaped socket has no app");
                    assert!(self.apps[app].orphaned, "reaped socket was not orphaned");
                    self.replace(app);
                }
            }
            Event::PollIngressSingle => {
                self.iface.poll_ingress_single(now, &mut self.device, &mut self.sockets);
            }
            Event::PollEgress => {
                self.iface.poll_egress(now, &mut self.device, &mut self.sockets);
                self.last_egress = Some(now);
            }
            Event::PollAt => {
                self.iface.poll_at(now, &mut self.sockets);
            }
            Event::Advance(step) => self.now += step,
            Event::Refuse(count) => self.device.refuse = count,
            Event::NeighborFlush => self.iface.inner.neighbor_cache.flush(),
        }
    }

    fn replace(&mut self, app: usize) {
        self.sockets.remove(self.apps[app].handle);
        self.armed.retain(|_, armed| armed.app != app);
        self.apps[app] = App { handle: self.sockets.add(socket()), orphaned: false, last_tuple: None };
    }

    fn queue_rx(&mut self, peer: usize, ethertype: EthernetProtocol, packet: Vec<u8>) {
        let frame = match self.medium {
            Medium::Ethernet => {
                let mut frame = std::vec![0; EthernetFrame::<&[u8]>::buffer_len(packet.len())];
                let mut ethernet = EthernetFrame::new_unchecked(&mut frame);
                ethernet.set_src_addr(PEER_MACS[peer]);
                ethernet.set_dst_addr(self.iface.inner.hardware_addr.ethernet_or_panic());
                ethernet.set_ethertype(ethertype);
                ethernet.payload_mut().copy_from_slice(&packet);
                frame
            }
            #[cfg(feature = "medium-ip")]
            Medium::Ip => packet,
        };
        self.device.inner.rx_queue.push_back(frame);
    }

    /// Every frame we sent leaves from X or Y; TCP segments are kept for peer replies.
    fn drain_sent(&mut self) {
        while let Some(frame) = self.device.inner.tx_queue.pop_front() {
            let ip_bytes = match self.medium {
                Medium::Ethernet => {
                    let ethernet = EthernetFrame::new_checked(&frame[..]).unwrap();
                    if ethernet.ethertype() == EthernetProtocol::Arp {
                        let arp = ArpRepr::parse(&ArpPacket::new_checked(ethernet.payload()).unwrap()).unwrap();
                        let ArpRepr::EthernetIpv4 { source_protocol_addr, .. } = arp else { unreachable!() };
                        assert!([X, Y].contains(&source_protocol_addr), "ARP from {source_protocol_addr}");
                        continue;
                    }
                    assert_eq!(ethernet.ethertype(), EthernetProtocol::Ipv4);
                    ethernet.payload().to_vec()
                }
                #[cfg(feature = "medium-ip")]
                Medium::Ip => frame,
            };
            let ip = Ipv4Packet::new_checked(&ip_bytes[..]).unwrap();
            let src = ip.src_addr();
            assert!([X, Y].contains(&src), "frame sent from {src}");
            if ip.next_header() != IpProtocol::Tcp {
                continue;
            }
            let tcp = TcpRepr::parse(
                &TcpPacket::new_checked(ip.payload()).unwrap(),
                &src.into(),
                &ip.dst_addr().into(),
                &ChecksumCapabilities::default(),
            )
            .unwrap();
            if self.sent.len() == 16 {
                self.sent.remove(0);
            }
            self.sent.push(Sent {
                local: IpEndpoint::new(src.into(), tcp.src_port),
                remote: IpEndpoint::new(ip.dst_addr().into(), tcp.dst_port),
                control: tcp.control,
                seq: tcp.seq_number,
                ack: tcp.ack_number,
                len: tcp.payload.len(),
            });
        }
    }

    fn check(&mut self) {
        self.drain_sent();
        self.sockets.check_flow_index();
        self.sockets.check_sched_invariants();
        for (_, socket) in self.sockets.iter() {
            socket.check_invariants();
        }
        assert_eq!(
            tcp::SOURCE_ADDRESS_RESETS.with(|resets| resets.get()),
            0,
            "a connection was reset for a source address the walk never removes"
        );
        while let Some(token) = self.sockets.pop_fired() {
            assert!(self.armed.remove(&token).is_some(), "token {token} fired but is not armed");
        }
        for (token, armed) in &self.armed {
            let now = Readiness::of(self.sockets.get(self.apps[armed.app].handle));
            assert!(
                !armed.at_arm.missed(&now),
                "token {token} ({:?} on app {}) did not fire: {:?} became {:?}",
                armed.side,
                armed.app,
                armed.at_arm,
                now
            );
        }
        if self.last_egress == Some(self.now) && self.iface.egress_idle(&self.sockets) {
            let before = self.sockets.sched_snapshot();
            self.iface.poll_egress(self.now, &mut self.device, &mut self.sockets);
            assert!(self.device.inner.tx_queue.is_empty(), "idle egress pass sent a frame");
            assert_eq!(self.sockets.sched_snapshot(), before, "idle egress pass changed the schedule");
        }
    }
}

fn run(seed: u64, log: &mut Vec<String>) {
    tcp::SOURCE_ADDRESS_RESETS.with(|resets| resets.set(0));
    let mut rng = Rng(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1);
    let medium = if seed.is_multiple_of(2) {
        Medium::Ethernet
    } else {
        #[cfg(feature = "medium-ip")]
        {
            Medium::Ip
        }
        #[cfg(not(feature = "medium-ip"))]
        {
            Medium::Ethernet
        }
    };
    let mut walk = Walk::new(medium);
    log.push(format!("{medium:?}"));
    for step in 0..STEPS {
        let event = walk.generate(&mut rng);
        log.push(format!("{step} @{}: {event:?}", walk.now));
        walk.apply(&event);
        walk.check();
    }
}

#[test]
fn tcp_walk() {
    let seeds = match std::env::var("TCP_WALK_SEED") {
        Ok(seed) => {
            let seed = seed.parse().expect("TCP_WALK_SEED is not a number");
            seed..seed + 1
        }
        Err(_) => 1..SEEDS + 1,
    };
    for seed in seeds {
        let mut log = Vec::new();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| run(seed, &mut log)));
        if let Err(panic) = result {
            std::eprintln!("tcp_walk failed at seed {seed} (TCP_WALK_SEED={seed}); events:");
            for line in &log {
                std::eprintln!("  {line}");
            }
            std::panic::resume_unwind(panic);
        }
    }
}
