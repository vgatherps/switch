#![allow(dead_code)]

use std::net::{Ipv4Addr, SocketAddrV4};
use std::os::unix::fs::DirBuilderExt;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::thread::{self, JoinHandle};
use std::time::{Duration as WallDuration, Instant as WallInstant};

use ceph_fifo::Fifo;
use ceph_proto::Retire;
use ceph_ring::{Config, Reader};
use dpdk_smoltcp::smoltcp::{socket::tcp, time::Instant, wire::Ipv4Address};
use dpdk_smoltcp::{Eal, Stack, StackConfig, TcpHandle, frame_reserve, rx_reserve};
use junction::control::{Event, ListenEndpoint, Op, SocketId, TcpConfig, UdpConfig};
use junction::host::{ControlChannels, ControlConfig, EndHow, FromNetwork, HostControl, ToNetwork};
use junction::session::Session;
use junctiond::network::{Network, NetworkConfig};
use zhida_test_kit::pair::{self, BUDGET, Side, T0, assert_lossless};
use zhida_test_kit::ring_port::RingPort;
use zhida_test_kit::wire::Link;

pub const PEER_PORT: u16 = 80;
pub const LOCAL_PORT: u16 = 49152;
pub const WAIT: WallDuration = WallDuration::from_secs(5);
static NEXT_PATH: AtomicU64 = AtomicU64::new(0);

pub fn tcp_config() -> TcpConfig {
    TcpConfig {
        rx_capacity: 6 * 1460,
        tx_capacity: 64 * 1024,
        mss_cap: None,
    }
}

pub fn peer_tcp_config() -> tcp::Config {
    tcp::Config {
        rx_capacity: 6 * 1460,
        tx_capacity: 64 * 1024,
        mss_cap: None,
    }
}

pub fn local(port: u16) -> ListenEndpoint {
    ListenEndpoint { addr: None, port }
}

pub fn udp_config(port: u16) -> UdpConfig {
    UdpConfig {
        local: local(port),
        rx_queue: 8,
        tx_checksum: true,
        broadcast: false,
    }
}

pub fn net_ip() -> Ipv4Address {
    Side::A.ip()
}
pub fn peer_endpoint(port: u16) -> SocketAddrV4 {
    SocketAddrV4::new(Ipv4Addr::new(10, 0, 0, 2), port)
}

pub struct Fixture {
    pub network: Network,
    pub peer: Stack,
    pub to_peer: Link,
    pub to_net: Link,
    pub now: Instant,
    pub socket_path: PathBuf,
    pub ingress: String,
    pub streams: Vec<u64>,
    pub retired: Vec<u64>,
    network_events: Receiver<FromNetwork>,
    forward_events: Sender<FromNetwork>,
    control: Receiver<ToNetwork>,
    forward: Sender<ToNetwork>,
    held_retire: Option<(Receiver<Retire>, Sender<Retire>)>,
    host: JoinHandle<std::io::Result<()>>,
    fixtures: [RingPort; 2],
    directory: PathBuf,
}

struct ControlOptions {
    batch: usize,
    max_sessions: u32,
    hold_retire: bool,
}

impl Default for ControlOptions {
    fn default() -> Self {
        Self {
            batch: 64,
            max_sessions: 16,
            hold_retire: false,
        }
    }
}

impl Fixture {
    pub fn new(eal: &Eal, names: [&str; 2], tcp: usize, udp: usize, fifo_batch: usize) -> Self {
        Self::with_control_batch(eal, names, tcp, udp, fifo_batch, 64)
    }

    pub fn with_control_batch(
        eal: &Eal,
        names: [&str; 2],
        tcp: usize,
        udp: usize,
        fifo_batch: usize,
        control_batch: usize,
    ) -> Self {
        Self::configured(
            eal,
            names,
            tcp,
            udp,
            fifo_batch,
            ControlOptions {
                batch: control_batch,
                ..ControlOptions::default()
            },
        )
    }

    pub fn with_held_retire(eal: &Eal, names: [&str; 2], tcp: usize, udp: usize) -> Self {
        Self::configured(
            eal,
            names,
            tcp,
            udp,
            64,
            ControlOptions {
                hold_retire: true,
                ..ControlOptions::default()
            },
        )
    }

    pub fn with_one_session(eal: &Eal, names: [&str; 2], tcp: usize) -> Self {
        Self::configured(
            eal,
            names,
            tcp,
            0,
            64,
            ControlOptions {
                max_sessions: 1,
                ..ControlOptions::default()
            },
        )
    }

    fn configured(
        eal: &Eal,
        names: [&str; 2],
        tcp: usize,
        udp: usize,
        fifo_batch: usize,
        options: ControlOptions,
    ) -> Self {
        let fixtures = names.map(|name| RingPort::new(name, 16, 16));
        let [stack, peer] = [Side::A, Side::B].map(|side| {
            let fixture = &fixtures[side.index()];
            let stack_config = StackConfig {
                max_sockets: tcp,
                max_udp_sockets: udp,
                ..pair::config(side)
            };
            let mut config = fixture.config();
            config.rx_pool.count = u32::try_from(
                rx_reserve(
                    config.rx_descriptors,
                    config.rx_burst,
                    stack_config.rx_staging,
                    config.rx_pool.cache_size,
                ) + 64 * tcp.max(1)
                    + udp * 8,
            )
            .unwrap();
            config.frame_pool.count = u32::try_from(
                frame_reserve(config.tx_descriptors, stack_config.tx_pending)
                    + tcp::tx_mbuf_budget(&peer_tcp_config(), config.mtu.into())
                    + 64,
            )
            .unwrap();
            match Stack::new(fixture.open_with(eal, config), stack_config, T0) {
                Ok(stack) => stack,
                Err((_, error)) => panic!("{error}"),
            }
        });
        let directory = std::env::temp_dir().join(format!(
            "junctiond-{}-{}",
            std::process::id(),
            NEXT_PATH.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::DirBuilder::new()
            .mode(0o700)
            .create(&directory)
            .unwrap();
        let socket_path = directory.join("control.sock");
        let reader = Reader::create(Config {
            ring_size: 64 * 1024,
            max_record: 256,
            dir: "/dev/shm".into(),
        })
        .unwrap();
        let ingress = reader.name().to_owned();
        let (to_network, control) = mpsc::channel();
        let (forward, operations) = mpsc::channel();
        let (events, network_events) = mpsc::channel();
        let (forward_events, from_network) = mpsc::channel();
        let (to_fifo, fifo_control) = mpsc::channel();
        let (retire, retired) = mpsc::channel();
        let (retired, held_retire) = if options.hold_retire {
            let (forward_retire, host_retired) = mpsc::channel();
            (host_retired, Some((retired, forward_retire)))
        } else {
            (retired, None)
        };
        let host = HostControl::bind(
            ControlConfig {
                socket_path: socket_path.clone(),
                ingress: ingress.clone(),
                max_record: 256,
                max_sessions: options.max_sessions,
                max_sockets: 64,
                poll_ms: 1,
                hello_timeout: WAIT,
            },
            ControlChannels {
                to_network,
                to_fifo,
                from_network,
                retire: retired,
            },
        )
        .unwrap();
        let host = thread::spawn(move || host.run());
        let fifo = Fifo::new(reader, options.max_sessions, fifo_control, retire);
        let network = Network::new(
            stack,
            fifo,
            options.max_sessions,
            NetworkConfig {
                ingress_budget: BUDGET,
                control_batch: options.batch,
                fifo_batch,
            },
            operations,
            events,
        );
        Self {
            network,
            peer,
            to_peer: fixtures[0].link_to(&fixtures[1]),
            to_net: fixtures[1].link_to(&fixtures[0]),
            now: T0,
            socket_path,
            ingress,
            streams: Vec::new(),
            retired: Vec::new(),
            network_events,
            forward_events,
            control,
            forward,
            held_retire,
            host,
            fixtures,
            directory,
        }
    }

    fn forward_message(&mut self, message: ToNetwork) {
        if let ToNetwork::Open { session, .. } = &message {
            self.streams.push(*session);
        }
        self.forward.send(message).unwrap();
    }

    pub fn client(&mut self, max_sockets: u16) -> Session {
        let client = Session::connect(&self.socket_path, max_sockets).unwrap();
        self.wait_open();
        client
    }

    pub fn wait_open(&mut self) -> u64 {
        loop {
            let message = self.control.recv_timeout(WAIT).unwrap();
            let opened = match &message {
                ToNetwork::Open { session, .. } => Some(*session),
                _ => None,
            };
            self.forward_message(message);
            if let Some(session) = opened {
                return session;
            }
        }
    }

    pub fn wait_op(&mut self, id: SocketId) -> Op {
        let deadline = WallInstant::now() + WAIT;
        loop {
            let message = self
                .control
                .recv_timeout(deadline.saturating_duration_since(WallInstant::now()))
                .unwrap();
            let op = match &message {
                ToNetwork::Op { op, .. } if op.id() == id => Some(*op),
                _ => None,
            };
            self.forward_message(message);
            if let Some(op) = op {
                return op;
            }
        }
    }
    /// Forward the real graceful terminal message, then reproduce the control EOF race.
    /// Only the trailing Lost is injected; retirement and all opens/ops use the host.
    pub fn forward_graceful_then_lost(&mut self, session: u64) {
        loop {
            let message = self.control.recv_timeout(WAIT).unwrap();
            let graceful = matches!(
                message, ToNetwork::End { session: id, how: EndHow::Graceful } if id == session
            );
            self.forward_message(message);
            if graceful {
                self.forward
                    .send(ToNetwork::End {
                        session,
                        how: EndHow::Lost,
                    })
                    .unwrap();
                return;
            }
        }
    }

    /// Replace the host's real graceful notification with the EOF race's Lost.
    /// The caller consumes the real FIFO end before forwarding this terminal message.
    pub fn forward_graceful_as_lost(&mut self, session: u64) {
        loop {
            let message = self.control.recv_timeout(WAIT).unwrap();
            if matches!(
                message, ToNetwork::End { session: id, how: EndHow::Graceful } if id == session
            ) {
                self.forward
                    .send(ToNetwork::End {
                        session,
                        how: EndHow::Lost,
                    })
                    .unwrap();
                return;
            }
            self.forward_message(message);
        }
    }

    /// Inject only the terminal control message to queue a disconnect behind a real Open.
    /// FIFO registration and the consumed end record still come from the real client.
    pub fn queue_lost(&self, session: u64) {
        self.forward
            .send(ToNetwork::End {
                session,
                how: EndHow::Lost,
            })
            .unwrap();
    }

    /// Inject a delayed terminal message from a previous occupant of a session slot.
    pub fn queue_stale_end(&self, session: u64, how: EndHow) {
        self.forward.send(ToNetwork::End { session, how }).unwrap();
    }

    /// Forward the host's real loss before opening the next session.
    pub fn forward_lost(&mut self, session: u64) {
        let message = self.control.recv_timeout(WAIT).unwrap();
        assert!(matches!(
            message, ToNetwork::End { session: id, how: EndHow::Lost } if id == session
        ));
        self.forward_message(message);
    }

    /// Hold the real FIFO completion until the test has queued its control bytes.
    pub fn wait_finished(&self, session: u64) -> Retire {
        let (retired, _) = self.held_retire.as_ref().unwrap();
        let message = retired.recv_timeout(WAIT).unwrap();
        assert!(matches!(
            message,
            Retire::Finished { slot, uid }
                if ceph_proto::pack(slot, uid) == session
        ));
        message
    }

    pub fn release_retire(&self, message: Retire) {
        self.held_retire.as_ref().unwrap().1.send(message).unwrap();
    }

    /// Assert and forward the host's actual terminal decision, without substituting it.
    pub fn forward_abort(&mut self, session: u64) {
        let message = self.control.recv_timeout(WAIT).unwrap();
        assert!(matches!(
            message,
            ToNetwork::End { session: id, how: EndHow::Abort } if id == session
        ));
        self.forward_message(message);
    }

    fn turn_network(&mut self) {
        while let Ok(message) = self.control.try_recv() {
            self.forward_message(message);
        }
        self.network.iteration(self.now).unwrap();
        while let Ok(message) = self.network_events.try_recv() {
            if let FromNetwork::Retired { session } = &message {
                self.retired.push(*session);
            }
            self.forward_events.send(message).unwrap();
        }
    }

    fn forward_links(&mut self) -> bool {
        let out = self.to_peer.forward();
        assert_lossless(&self.to_peer, "host to peer");
        self.peer.turn(self.now, BUDGET).unwrap();
        let back = self.to_net.forward();
        assert_lossless(&self.to_net, "peer to host");
        thread::yield_now();
        out + back > 0
    }

    pub fn round_links(&mut self) -> bool {
        self.turn_network();
        self.forward_links()
    }

    pub fn round(&mut self, client: &mut Session, events: &mut Vec<(SocketId, Event)>) -> bool {
        self.turn_network();
        client
            .poll_events(|id, event| events.push((id, event)))
            .unwrap();
        self.forward_links()
    }
    pub fn until(
        &mut self,
        client: &mut Session,
        events: &mut Vec<(SocketId, Event)>,
        mut done: impl FnMut(&mut Self, &[(SocketId, Event)]) -> bool,
    ) {
        let deadline = WallInstant::now() + WAIT;
        for _ in 0..100_000 {
            if done(self, events) {
                return;
            }
            assert!(
                WallInstant::now() < deadline,
                "network condition timed out at {}",
                self.now
            );
            if !self.round(client, events) && !done(self, events) {
                self.advance();
            }
        }
        panic!(
            "network condition not reached after 100000 rounds at {}",
            self.now
        );
    }

    pub fn advance(&mut self) {
        if let Some(at) = [self.network.poll_at(), self.peer.poll_at()]
            .into_iter()
            .flatten()
            .min()
        {
            self.now = self.now.max(at);
        }
    }

    pub fn close(self) {
        let Self {
            network,
            peer,
            mut to_peer,
            mut to_net,
            host,
            held_retire,
            fixtures,
            socket_path,
            directory,
            ..
        } = self;
        let stack = network.into_stack();
        drop(held_retire);
        host.join().unwrap().unwrap();
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
        std::fs::remove_file(socket_path).unwrap();
        std::fs::remove_dir(directory).unwrap();
    }
}

pub fn peer_listen(fixture: &mut Fixture) -> TcpHandle {
    let peer = fixture.peer.add_tcp(peer_tcp_config()).unwrap();
    fixture.peer.listen(&peer, PEER_PORT).unwrap();
    peer
}

pub fn connect(
    fixture: &mut Fixture,
    client: &mut Session,
    events: &mut Vec<(SocketId, Event)>,
    port: u16,
) -> (SocketId, TcpHandle) {
    let peer = peer_listen(fixture);
    let id = client
        .connect_tcp(tcp_config(), peer_endpoint(PEER_PORT), local(port))
        .unwrap();
    fixture.until(client, events, |f, events| {
        events.contains(&(id, Event::Connected))
            && f.peer.tcp(&peer).state() == tcp::State::Established
    });
    (id, peer)
}
