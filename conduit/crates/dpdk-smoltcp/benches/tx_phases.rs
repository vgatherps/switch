//! Sender-side TCP TX phase timings, Criterion group "tx_phases", on A of a
//! one-connection Pair. B only completes the handshake; afterwards A's frames
//! are taken off its TX ring and B's ACKs are built from B's handshake
//! sequence number and window scale and injected on A's RX ring. An untimed
//! first ACK sets A's view of B's window to the case's window (A's TX buffer;
//! 724 bytes for cut), which every later ACK repeats. One iteration moves
//! `depth` writes of `write` bytes through these phases:
//!
//! 1. allocate: `frame`: `Stack::tx_frame`. `slice` allocates inside
//!    `send_slice` (enqueue); no bench.
//! 2. fill: the application writes `write` bytes. `slice`: a copy of the
//!    payload into the application's own buffer. `frame`: the same copy into
//!    the frame's room, then `set_len`.
//! 3. enqueue: `send_slice` through `tcp_mut`, or `Stack::tcp_send_frame`.
//! 4. emit: one `Stack::send` per write, including its flush; each must
//!    send exactly one frame (mss536: one send for all writes).
//! 5. ack: one `Stack::receive` that takes all `depth / batch` injected ACKs,
//!    each acknowledging `batch` more segments, the last all of them.
//! 6. reclaim: the TX ring consumer takes every sent frame off A's TX ring
//!    and drops its reference (`Pair::drain_tx`). A frame the socket still
//!    holds returns to the frame pool when its ACK releases it (phase 5).
//!
//! Writes and emits alternate, write then emit, so `depth` segments are in
//! flight before the first ACK; reclaim follows the last emit, then the ACKs
//! are injected (untimed) and received. Each phase bench sums its phase over
//! the iteration. `total` runs the same iteration with no per-phase timers:
//! one stopwatch, paused only while ACK frames are built and injected. rto
//! has no `total`: its receive calls at early deadlines are waiting, not TX
//! work.
//!
//! Cases, each on its own Pair, so congestion state from one never reaches
//! another:
//! - `<api>/<write>/d<depth>/b<depth>/csum`: `slice` and `frame`, 64 and 1448
//!   B, depths 1, 8, 46, 256, one cumulative ACK.
//! - `slice/1448/d<depth>/b<batch>/csum`: ACK batches 1 and 2 at depth 8 and
//!   1, 2, 8 at depth 256.
//! - `slice/<write>/d<depth>/b<depth>/offload`: as csum with
//!   `Port::force_tx_tcp_checksum` (feature `bench-offload`) on A's net_ring
//!   Port before Stack creation: smoltcp skips the TCP checksum and flush sets
//!   the TX flags and lengths. net_ring fills nothing, so frames leave
//!   without a valid TCP checksum; this measures the software cost only. The
//!   handshake and the warmup frame checks fill the checksum first, as the
//!   NIC would (`fill_tcp_checksum`).
//! - `cut/slice/1448`: B's window stays 724 bytes, so one 1448-byte write
//!   leaves as two 724-byte segments, each a window-limited emit followed by
//!   reclaim and an ACK reopening 724 bytes. Throughput element: 2 segments.
//! - `mss536/frame/1460/d8`: both sockets cap the MSS at 536. 8 1460-byte
//!   frames are queued, then one send cuts them into 22 segments: 536 bytes
//!   each, some inside one frame and some spanning two, then 424; one ACK
//!   takes them all. Throughput element: one segment.
//! - `rto/slice/<write>`: depth 1; the emitted frame is freed unforwarded,
//!   then A runs `receive` at its `poll_at` deadlines until a `send` emits the
//!   retransmission. `retransmit` times that `send`; `send` calls at early
//!   deadlines that emit nothing are untimed. `emit` times the original send,
//!   reclaim both frees. Time advances by the RTO each iteration, so about
//!   once every 60 s of it A's retransmission comes with its ARP probe for B
//!   (neighbor lifetime); reclaim checks it and B's reply refreshes A, untimed.
//!
//! A's send buffer is `depth` × 1460 bytes (1460: the segment payload at IP
//! MTU 1500). It takes `depth` writes, each its own frame because an emit
//! follows it. Throughput element = one segment. Caller time is fixed except in rto.
//! Every iteration checks each `Sent` and `Turn`, the reclaimed frame count
//! and an empty send queue at the end. Warmup iterations after the first also
//! check every frame's sequence number, length, flags and payload, a stable
//! TX pool count and no A deadline after the ACKs (rto excepted); measured
//! iterations skip those, as emit.rs does, because they walk mempool caches
//! or change what the timed calls find in cache.
//! clock times an empty region with the same two Instant reads as emit.rs.

use std::hint::black_box;
use std::ops::Range;
use std::time::{Duration, Instant};

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use dpdk_smoltcp::smoltcp::socket::tcp::{self, State};
use dpdk_smoltcp::smoltcp::time::Instant as SmolInstant;
use dpdk_smoltcp::smoltcp::wire::{
    ArpOperation, ArpPacket, ArpRepr, IpEndpoint, TcpControl, TcpPacket, TcpRepr, TcpSeqNumber,
    ETHERNET_HEADER_LEN, IPV4_HEADER_LEN,
};
use dpdk_smoltcp::{
    frame_reserve, rx_reserve, Eal, Sent, TcpHandle, Turn,
};

use zhida_test_kit::pair::{
    self, establish_with, pattern, segment, tcp_frame, tcp_socket_sized, Dir, Pair, Side,
    BUDGET, CLIENT_PORT, IDLE_ROUNDS, MAX_ROUNDS, SERVER_PORT, T0,
};
use zhida_test_kit::{ring_port, with_eal};

const WARMUP: usize = 6;
const RING: u32 = 512;
const TX_POOL: u32 = 1023;
const BURST: u16 = 256;
const DATA_HEADERS: usize = ETHERNET_HEADER_LEN + IPV4_HEADER_LEN + 20;
const CUT_WINDOW: usize = 724;
const CUT_BUFFER: usize = 4096;
/// TCP payload per segment at IP MTU 1500: the socket's frame unit.
const SEGMENT: usize = 1460;
/// The MSS both sockets cap at in the mss536 cases.
const PEER_MSS: usize = 536;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Api {
    Slice,
    Frame,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Path {
    /// `depth` writes, `depth / batch` ACKs.
    Queue { depth: usize, batch: usize },
    /// One write, two window-limited segments.
    Cut,
    /// One write, one RTO retransmission.
    Rto,
    /// `depth` writes, one send cut at `PEER_MSS`, one ACK.
    Mss { depth: usize },
}

#[derive(Clone, Copy)]
struct Case {
    api: Api,
    write: usize,
    path: Path,
    offload: bool,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Phase {
    Allocate,
    Fill,
    Enqueue,
    Emit,
    Ack,
    Reclaim,
    Retransmit,
    Total,
}

const PHASES: [Phase; 7] = [
    Phase::Allocate,
    Phase::Fill,
    Phase::Enqueue,
    Phase::Emit,
    Phase::Ack,
    Phase::Reclaim,
    Phase::Retransmit,
];

impl Phase {
    fn name(self) -> &'static str {
        match self {
            Self::Allocate => "allocate",
            Self::Fill => "fill",
            Self::Enqueue => "enqueue",
            Self::Emit => "emit",
            Self::Ack => "ack",
            Self::Reclaim => "reclaim",
            Self::Retransmit => "retransmit",
            Self::Total => "total",
        }
    }
}

impl Case {
    fn name(self) -> String {
        let api = match self.api {
            Api::Slice => "slice",
            Api::Frame => "frame",
        };
        let csum = if self.offload { "offload" } else { "csum" };
        match self.path {
            Path::Queue { depth, batch } => {
                format!("{api}/{}/d{depth}/b{batch}/{csum}", self.write)
            }
            Path::Cut => format!("cut/{api}/{}", self.write),
            Path::Rto => format!("rto/{api}/{}", self.write),
            Path::Mss { depth } => format!("mss{PEER_MSS}/{api}/{}/d{depth}", self.write),
        }
    }

    fn depth(self) -> usize {
        match self.path {
            Path::Queue { depth, .. } | Path::Mss { depth } => depth,
            Path::Cut | Path::Rto => 1,
        }
    }

    /// Segments one iteration sends, retransmissions excluded.
    fn segments(self) -> usize {
        match self.path {
            Path::Cut => 2,
            Path::Mss { depth } => (self.write * depth).div_ceil(PEER_MSS),
            _ => self.depth(),
        }
    }

    fn buffer(self) -> usize {
        match self.path {
            Path::Cut => CUT_BUFFER,
            _ => self.write.max(SEGMENT).checked_mul(self.depth()).expect("socket buffer"),
        }
    }

    /// B's window in every ACK.
    fn window(self) -> usize {
        match self.path {
            Path::Cut => CUT_WINDOW,
            _ => self.buffer(),
        }
    }

    /// Both sockets' config.
    fn socket(self) -> tcp::Config {
        let mss_cap = matches!(self.path, Path::Mss { .. })
            .then(|| u16::try_from(PEER_MSS).expect("MSS fits u16"));
        tcp::Config { mss_cap, ..tcp_socket_sized(self.buffer()) }
    }

    fn phases(self) -> impl Iterator<Item = Phase> {
        let rto = self.path == Path::Rto;
        let frame = self.api == Api::Frame;
        PHASES
            .into_iter()
            .filter(move |&phase| rto || phase != Phase::Retransmit)
            .filter(move |&phase| frame || phase != Phase::Allocate)
            .chain((!rto).then_some(Phase::Total))
    }
}

/// Per-phase sums over one iteration. `Timing::Phases` times each phase;
/// `Timing::Total` runs one stopwatch into `Phase::Total` instead.
struct Times {
    timing: Timing,
    sums: [Duration; 8],
    running: Option<Instant>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Timing {
    Phases,
    Total,
}

impl Times {
    fn new(timing: Timing) -> Self {
        let running = (timing == Timing::Total).then(Instant::now);
        Self { timing, sums: [Duration::ZERO; 8], running }
    }

    fn add(&mut self, phase: Phase, elapsed: Duration) {
        let slot = &mut self.sums[phase as usize];
        *slot = slot.checked_add(elapsed).expect("phase duration");
    }

    fn phases(&self) -> bool {
        self.timing == Timing::Phases
    }

    /// `f`, timed into `phase` under `Timing::Phases`.
    fn time<R>(&mut self, phase: Phase, f: impl FnOnce() -> R) -> R {
        if !self.phases() {
            return f();
        }
        let start = Instant::now();
        let result = f();
        self.add(phase, start.elapsed());
        result
    }

    /// Stop the `Timing::Total` stopwatch.
    fn pause(&mut self) {
        if let Some(start) = self.running.take() {
            self.add(Phase::Total, start.elapsed());
        }
    }

    fn resume(&mut self) {
        if self.timing == Timing::Total {
            assert!(self.running.is_none(), "stopwatch already running");
            self.running = Some(Instant::now());
        }
    }

    fn get(&self, phase: Phase) -> Duration {
        assert!(self.running.is_none(), "stopwatch still running");
        self.sums[phase as usize]
    }
}

struct Bench {
    pair: Pair,
    client: TcpHandle,
    case: Case,
    payload: Vec<u8>,
    /// The bytes one iteration sends: `payload` once per write.
    stream: Vec<u8>,
    app: Vec<u8>,
    /// A's next data sequence number.
    a_next: TcpSeqNumber,
    /// B's sequence number, constant after the handshake.
    b_seq: TcpSeqNumber,
    /// B's window scale shift as A applies it.
    b_shift: u8,
    /// A's TX pool count after a checked iteration, from the first one.
    tx_in_use: Option<u32>,
    /// A's time: the Pair's until rto moves it past `Pair::now`.
    now: SmolInstant,
    /// The last retransmit send also emitted A's ARP probe for B; the next
    /// reclaim takes it and B's reply answers it.
    arp_probe: bool,
}

fn is_arp(frame: &[u8]) -> bool {
    frame[12..14] == [0x08, 0x06]
}

/// `frame` is A's ARP request for B's address.
fn assert_arp_probe(frame: &[u8]) {
    let arp = ArpRepr::parse(&ArpPacket::new_checked(&frame[ETHERNET_HEADER_LEN..]).unwrap())
        .unwrap();
    let ArpRepr::EthernetIpv4 { operation, source_hardware_addr, source_protocol_addr, target_protocol_addr, .. } = arp
    else {
        panic!("A sent a non-Ethernet/IPv4 ARP frame: {arp:?}")
    };
    assert_eq!(operation, ArpOperation::Request, "A ARP probe operation");
    assert_eq!((source_hardware_addr, source_protocol_addr), (Side::A.mac(), Side::A.ip()));
    assert_eq!(target_protocol_addr, Side::B.ip(), "A ARP probe target");
}

/// A's frame with its TCP checksum filled in, as a NIC with the TX TCP
/// checksum offload would; other frames unchanged.
fn fill_tcp_checksum(frame: &[u8]) -> Vec<u8> {
    let mut bytes = frame.to_vec();
    if bytes[12..14] == [0x08, 0x00] {
        TcpPacket::new_unchecked(&mut bytes[ETHERNET_HEADER_LEN + IPV4_HEADER_LEN..])
            .fill_checksum(&Side::A.ip().into(), &Side::B.ip().into());
    }
    bytes
}

/// `establish_with` (A connects, both sockets `socket`) for an A Port with
/// the forced TX TCP checksum profile: net_ring leaves A's TCP checksums
/// unfilled, so each A frame goes to B with `fill_tcp_checksum`. Rounds at
/// the Pair's time until both sockets are Established and a round moves
/// nothing. Returns the handles and every delivered frame, as
/// `establish_with` does.
fn establish_filled(pair: &mut Pair, socket: tcp::Config) -> ([TcpHandle; 2], Vec<(Dir, Vec<u8>)>) {
    let server = pair.b.add_tcp(socket).unwrap();
    pair.b.listen(&server, SERVER_PORT).unwrap();
    let client = pair.a.add_tcp(socket).unwrap();
    pair.a
        .connect(&client, IpEndpoint::new(Side::B.ip().into(), SERVER_PORT), CLIENT_PORT)
        .unwrap();
    let mut frames = Vec::new();
    for _ in 0..MAX_ROUNDS {
        let now = pair.now();
        pair.a.turn(now, BUDGET).unwrap();
        let mut sent = Vec::new();
        pair.drain_tx(Side::A, |frame| sent.push(fill_tcp_checksum(frame)));
        pair.inject(Side::B, &sent);
        let mut moved = !sent.is_empty();
        frames.extend(sent.into_iter().map(|frame| (Dir::AToB, frame)));
        pair.b.turn(now, BUDGET).unwrap();
        moved |= pair.b_to_a.forward_with(|frame| frames.push((Dir::BToA, frame.to_vec()))) > 0;
        let established = pair.a.tcp(&client).state() == State::Established
            && pair.b.tcp(&server).state() == State::Established;
        if established && !moved {
            return ([client, server], frames);
        }
    }
    panic!("offload handshake not done after {MAX_ROUNDS} rounds");
}

fn setup(eal: &Eal, case: Case, id: usize) -> Bench {
    let names = [format!("tx{id}a"), format!("tx{id}b")];
    let configs = [Side::A, Side::B].map(|side| dpdk_smoltcp::StackConfig {
        rx_staging: usize::from(BURST),
        ..pair::config(side)
    });
    let socket = case.socket();
    let mut pair = Pair::with_opened_ports(
        eal,
        [&names[0], &names[1]],
        configs,
        T0,
        RING,
        |side, port| {
            port.rx_burst = BURST;
            port.tx_burst = BURST;
            port.tx_pool.count = TX_POOL;
            let mtu = usize::from(port.mtu);
            // The frame reserve, the socket's frame limit and the one frame
            // the application holds between allocate and enqueue.
            port.frame_pool.count = zhida_test_kit::pair::pool_count(frame_reserve(port.tx_descriptors, configs[side.index()].tx_pending), tcp::tx_mbuf_budget(&socket, mtu)) + 1;
            // A's RX pool also holds up to RING - 1 injected ACKs.
            port.rx_pool.count = zhida_test_kit::pair::pool_count(rx_reserve(
                port.rx_descriptors,
                port.rx_burst,
                configs[side.index()].rx_staging,
                port.rx_pool.cache_size,
            ), zhida_test_kit::pair::windows(port.mtu, [case.buffer()])) + RING;
        },
        |side, port| {
            if side == Side::A && case.offload {
                port.force_tx_tcp_checksum();
            }
        },
    );
    let (handles, frames) = if case.offload {
        establish_filled(&mut pair, socket)
    } else {
        establish_with(&mut pair, Side::A, socket)
    };
    // establish can return with a B→A frame still on A's RX ring (seen with
    // 370688-byte buffers); deliver everything before priming the window.
    let settled = (0..IDLE_ROUNDS).any(|_| !pair.round(|_, _| {}).moved());
    assert!(settled, "Pair not quiet after the handshake");
    let [client, _] = handles;
    pair.a.tcp_mut(&client).set_ack_delay(None);
    // The handshake frames include ARP; ethertype IPv4 picks the TCP ones.
    let tcp_from =
        |dir: Dir| move |entry: &&(Dir, Vec<u8>)| entry.0 == dir && entry.1[12..14] == [0x08, 0x00];
    let syn_ack = frames
        .iter()
        .find(tcp_from(Dir::BToA))
        .map(|(_, frame)| segment(frame, Side::B))
        .expect("SYN-ACK");
    assert_eq!(syn_ack.control, TcpControl::Syn);
    let handshake_ack = frames
        .iter()
        .rev()
        .find(tcp_from(Dir::AToB))
        .map(|(_, frame)| segment(frame, Side::A))
        .expect("handshake ACK");
    let b_seq = syn_ack.seq_number + 1;
    assert_eq!(handshake_ack.control, TcpControl::None);
    assert_eq!(handshake_ack.ack_number, Some(b_seq));
    let mut bench = Bench {
        client,
        case,
        payload: pattern(case.write, 7),
        stream: pattern(case.write, 7).repeat(case.depth()),
        app: vec![0; case.write],
        a_next: handshake_ack.seq_number,
        b_seq,
        b_shift: syn_ack.window_scale.unwrap_or(0),
        tx_in_use: None,
        now: pair.now(),
        arp_probe: false,
        pair,
    };
    // A's view of B's window is B's SYN-ACK window, unscaled and at most
    // 65535 bytes; set it to the case's window before the first write.
    let ack = bench.ack_frame(bench.a_next, case.window());
    bench.pair.inject(Side::A, &[ack]);
    assert_receive(bench.pair.a.receive(bench.now, 1).unwrap(), 1, case, "window");
    bench
}

fn assert_receive(turn: Turn, received: usize, case: Case, step: &str) {
    assert_eq!(
        turn,
        Turn {
            rx_received: received,
            rx_rejected: 0,
            rx_processed: received,
            rx_staged: 0,
            tx_accepted: 0,
            tx_pending: 0,
        },
        "{} {step}",
        case.name()
    );
}

fn assert_sent(sent: Sent, count: usize, case: Case, step: &str) {
    assert_eq!(
        sent,
        Sent {
            tx_accepted: count,
            tx_pending: 0,
        },
        "{} {step}",
        case.name()
    );
}

impl Bench {
    /// B's pure ACK of A's data up to `ack`, advertising `window` bytes.
    fn ack_frame(&self, ack: TcpSeqNumber, window: usize) -> Vec<u8> {
        let window_len = u16::try_from(window >> self.b_shift).expect("window field");
        assert_eq!(usize::from(window_len) << self.b_shift, window, "window not scalable");
        tcp_frame(
            Side::B,
            &TcpRepr {
                src_port: SERVER_PORT,
                dst_port: CLIENT_PORT,
                control: TcpControl::None,
                seq_number: self.b_seq,
                ack_number: Some(ack),
                window_len,
                window_scale: None,
                max_seg_size: None,
                sack_permitted: false,
                sack_ranges: [None; 3],
                timestamp: None,
                payload: &[],
            },
        )
    }

    /// Phases 1 to 3: allocate (frame only), fill and enqueue one write.
    fn write(&mut self, times: &mut Times) {
        let Self { pair, client, payload, app, case, .. } = self;
        let len = payload.len();
        match case.api {
            Api::Slice => {
                times.time(Phase::Fill, || {
                    app.copy_from_slice(payload);
                    black_box(&mut *app);
                });
                let queued = times.time(Phase::Enqueue, || pair.a.tcp_mut(client).send_slice(app));
                assert_eq!(queued, Ok(len));
            }
            Api::Frame => {
                let frame = times.time(Phase::Allocate, || pair.a.tx_frame());
                let mut frame = frame.expect("frame pool holds the benchmark's writes");
                times.time(Phase::Fill, || {
                    frame.room_mut()[..len].copy_from_slice(payload);
                    frame.set_len(len);
                });
                let queued = times.time(Phase::Enqueue, || pair.a.tcp_send_frame(client, frame));
                if let Err((_, error)) = queued {
                    panic!("{} enqueue: {error}", case.name());
                }
            }
        }
    }

    /// Phase 4: one `Stack::send` that must send `count` frames.
    fn emit(&mut self, times: &mut Times, count: usize) {
        let sent = times.time(Phase::Emit, || self.pair.a.send());
        assert_sent(sent.unwrap(), count, self.case, "emit");
    }

    /// Phase 6: take A's sent frames off its TX ring and free them.
    /// `expected` holds each frame's sequence number, range of `stream` and
    /// flags; with `checks`, every frame is parsed against it, untimed. After a
    /// retransmit that also sent A's ARP probe, that frame is checked too and
    /// B's reply is delivered to A, untimed.
    fn reclaim(
        &mut self,
        times: &mut Times,
        expected: &[(TcpSeqNumber, Range<usize>, TcpControl)],
        checks: bool,
    ) {
        let probe = std::mem::take(&mut self.arp_probe);
        let mut probes = 0usize;
        let count = if checks {
            let (stream, b_seq, offload) = (&self.stream, self.b_seq, self.case.offload);
            let mut frames = expected.iter();
            self.pair.drain_tx(Side::A, |frame| {
                if is_arp(frame) {
                    assert_arp_probe(frame);
                    probes += 1;
                    return;
                }
                let (seq, range, control) = frames.next().expect("extra A frame");
                let filled = if offload { fill_tcp_checksum(frame) } else { frame.to_vec() };
                let tcp = segment(&filled, Side::A);
                assert_eq!(frame.len(), DATA_HEADERS + range.len(), "A frame length");
                assert_eq!((tcp.src_port, tcp.dst_port), (CLIENT_PORT, SERVER_PORT));
                assert_eq!(tcp.control, *control, "A frame control");
                assert_eq!(tcp.seq_number, *seq);
                assert_eq!(tcp.ack_number, Some(b_seq));
                assert_eq!(tcp.payload, &stream[range.clone()]);
            })
        } else if probe {
            let pair = &self.pair;
            times.time(Phase::Reclaim, || pair.drain_tx(Side::A, |frame| {
                if is_arp(frame) {
                    assert_arp_probe(frame);
                    probes += 1;
                }
            }))
        } else {
            let pair = &self.pair;
            times.time(Phase::Reclaim, || pair.drain_tx(Side::A, |_| {}))
        };
        assert_eq!(probes, usize::from(probe), "A ARP probes");
        assert_eq!(count, expected.len() + probes, "reclaimed frames");
        if probe {
            times.pause();
            let reply = ring_port::arp(
                ArpOperation::Reply,
                Side::A.mac(),
                (Side::B.mac(), Side::B.ip()),
                (Side::A.mac(), Side::A.ip()),
            );
            self.pair.inject(Side::A, &[reply]);
            assert_receive(self.pair.a.receive(self.now, 1).unwrap(), 1, self.case, "ARP reply");
            times.resume();
        }
    }

    /// Phase 5: inject `acks` (untimed), then one receive that takes them all.
    fn ack(&mut self, times: &mut Times, acks: &[Vec<u8>]) {
        times.pause();
        self.pair.inject(Side::A, acks);
        times.resume();
        let now = self.now;
        let pair = &mut self.pair;
        let turn = times.time(Phase::Ack, || pair.a.receive(now, acks.len()));
        assert_receive(turn.unwrap(), acks.len(), self.case, "ack");
    }

    /// The RTO retransmission: `receive` at each A deadline until `send`
    /// emits; only that `send` is timed.
    fn retransmit(&mut self, times: &mut Times) {
        let retransmits = self.pair.a.tcp(&self.client).retransmits();
        loop {
            let at = self.pair.a.poll_at().expect("RTO deadline");
            self.now = at;
            assert_receive(self.pair.a.receive(at, 1).unwrap(), 0, self.case, "rto wait");
            let start = Instant::now();
            let sent = self.pair.a.send().unwrap();
            let elapsed = start.elapsed();
            if sent.tx_accepted == 0 {
                assert_eq!(sent.tx_pending, 0);
                continue;
            }
            // A second frame is A's once-per-lifetime ARP probe of its entry for B; it lands
            // in this timed send about once every 60 s of virtual time.
            self.arp_probe = sent.tx_accepted == 2;
            assert_sent(sent, 1 + usize::from(self.arp_probe), self.case, "retransmit");
            if times.phases() {
                times.add(Phase::Retransmit, elapsed);
            }
            break;
        }
        assert_eq!(self.pair.a.tcp(&self.client).retransmits(), retransmits + 1);
    }

    /// Untimed under `Timing::Total`: build `f`'s ACK frames.
    fn build_acks(times: &mut Times, f: impl FnOnce() -> Vec<Vec<u8>>) -> Vec<Vec<u8>> {
        times.pause();
        let acks = f();
        times.resume();
        acks
    }

    fn iteration(&mut self, timing: Timing, checks: bool) -> Times {
        assert!(timing == Timing::Phases || self.case.path != Path::Rto);
        let mut times = Times::new(timing);
        let write = self.case.write;
        let start = self.a_next;
        // PSH marks the segment that empties the send queue.
        let last_acks = match self.case.path {
            Path::Queue { depth, batch } => {
                for _ in 0..depth {
                    self.write(&mut times);
                    self.emit(&mut times, 1);
                }
                let expected: Vec<_> = (0..depth)
                    .map(|i| (start + i * write, i * write..(i + 1) * write, TcpControl::Psh))
                    .collect();
                self.reclaim(&mut times, &expected, checks);
                Self::build_acks(&mut times, || {
                    (1..=depth / batch)
                        .map(|n| self.ack_frame(start + n * batch * write, self.case.window()))
                        .collect()
                })
            }
            Path::Cut => {
                let half = start + CUT_WINDOW;
                self.write(&mut times);
                self.emit(&mut times, 1);
                self.reclaim(&mut times, &[(start, 0..CUT_WINDOW, TcpControl::None)], checks);
                let first = Self::build_acks(&mut times, || vec![self.ack_frame(half, self.case.window())]);
                self.ack(&mut times, &first);
                self.emit(&mut times, 1);
                self.reclaim(&mut times, &[(half, CUT_WINDOW..write, TcpControl::Psh)], checks);
                Self::build_acks(&mut times, || vec![self.ack_frame(start + write, self.case.window())])
            }
            Path::Rto => {
                self.write(&mut times);
                self.emit(&mut times, 1);
                self.reclaim(&mut times, &[(start, 0..write, TcpControl::Psh)], checks);
                self.retransmit(&mut times);
                self.reclaim(&mut times, &[(start, 0..write, TcpControl::Psh)], checks);
                vec![self.ack_frame(start + write, self.case.window())]
            }
            Path::Mss { depth } => {
                for _ in 0..depth {
                    self.write(&mut times);
                }
                let segments = self.case.segments();
                self.emit(&mut times, segments);
                let total = depth * write;
                let expected: Vec<_> = (0..segments)
                    .map(|k| {
                        let range = k * PEER_MSS..total.min((k + 1) * PEER_MSS);
                        let control =
                            if range.end == total { TcpControl::Psh } else { TcpControl::None };
                        (start + range.start, range, control)
                    })
                    .collect();
                self.reclaim(&mut times, &expected, checks);
                Self::build_acks(&mut times, || vec![self.ack_frame(start + total, self.case.window())])
            }
        };
        self.ack(&mut times, &last_acks);
        times.pause();
        self.a_next = start + write * self.case.depth();
        assert_eq!(self.pair.a.tcp(&self.client).send_queue(), 0, "unacknowledged bytes");
        if checks {
            if self.case.path != Path::Rto {
                assert_eq!(self.pair.a.poll_at(), None, "A deadline after ACK");
            }
            let in_use = self.pair.in_use()[0].1;
            assert_eq!(*self.tx_in_use.get_or_insert(in_use), in_use, "A TX pool count");
        }
        times
    }

    fn close(self) {
        assert_eq!(self.pair.drain_tx(Side::B, |_| {}), 0, "B sent after the handshake");
        for link in [&self.pair.a_to_b, &self.pair.b_to_a] {
            assert_eq!((link.pool_empty, link.ring_full, link.dropped), (0, 0, 0));
        }
        assert_eq!(self.pair.close(), 0);
    }
}

fn cases() -> Vec<Case> {
    let mut cases = Vec::new();
    let queue = |api, write, depth, batch, offload| Case {
        api,
        write,
        path: Path::Queue { depth, batch },
        offload,
    };
    for api in [Api::Slice, Api::Frame] {
        for write in [64, 1448] {
            for depth in [1, 8, 46, 256] {
                cases.push(queue(api, write, depth, depth, false));
            }
        }
    }
    for (depth, batch) in [(8, 1), (8, 2), (256, 1), (256, 2), (256, 8)] {
        cases.push(queue(Api::Slice, 1448, depth, batch, false));
    }
    for write in [64, 1448] {
        for depth in [1, 8, 46, 256] {
            cases.push(queue(Api::Slice, write, depth, depth, true));
        }
    }
    cases.push(Case { api: Api::Slice, write: 2 * CUT_WINDOW, path: Path::Cut, offload: false });
    for write in [64, 1448] {
        cases.push(Case { api: Api::Slice, write, path: Path::Rto, offload: false });
    }
    cases.push(Case { api: Api::Frame, write: SEGMENT, path: Path::Mss { depth: 8 }, offload: false });
    cases
}

fn benches(c: &mut Criterion) {
    with_eal(|eal| {
        let mut group = c.benchmark_group("tx_phases");
        for (id, case) in cases().into_iter().enumerate() {
            let mut bench = setup(eal, case, id);
            bench.iteration(Timing::Phases, false);
            for _ in 1..WARMUP {
                bench.iteration(Timing::Phases, true);
            }
            group.throughput(Throughput::Elements(case.segments() as u64));
            for phase in case.phases() {
                let timing = if phase == Phase::Total { Timing::Total } else { Timing::Phases };
                group.bench_function(format!("{}/{}", case.name(), phase.name()), |b| {
                    b.iter_custom(|iters| {
                        let mut elapsed = Duration::ZERO;
                        for _ in 0..iters {
                            let times = bench.iteration(timing, false);
                            elapsed = elapsed.checked_add(times.get(phase)).expect("total duration");
                        }
                        elapsed
                    });
                });
            }
            bench.iteration(Timing::Phases, true);
            bench.close();
        }
        group.throughput(Throughput::Elements(1));
        group.bench_function("clock", |b| {
            b.iter_custom(|iters| {
                let mut elapsed = Duration::ZERO;
                for _ in 0..iters {
                    let start = Instant::now();
                    let sample = start.elapsed();
                    elapsed = elapsed.checked_add(sample).expect("total clock duration");
                }
                elapsed
            });
        });
        group.finish();
    });
}

criterion_group!(tx_phases, benches);
criterion_main!(tx_phases);
