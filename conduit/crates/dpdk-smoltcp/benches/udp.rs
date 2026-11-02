//! UDP datagram timings, Criterion group "udp", with one live Pair per case.
//! A sends to B with software UDP checksums (csum) or zero UDP checksums
//! (nocsum). Payloads are 64, 512 or 1472 bytes; bursts are 1, 8 or 32 frames.
//! send_copy times payload writes into the app's own buffer, k
//! `udp_send(&[u8])` calls that copy it into a frame-pool frame, and one flush.
//! send_frame times k `Stack::tx_frame` allocations, the same payload writes
//! into each frame, k `udp_send_frame` calls (unique frames, published in
//! place) and one flush. Both share the receive side and checks.
//! recv sends with send_frame's path, untimed, then times B's receive(now,
//! k), k socket recv calls and each datagram's drop. Each read touches only
//! the first payload byte and saves it for the untimed sequence check. Payloads have a one-byte sequence number, 0..k,
//! followed by a fixed pattern. Sequence numbers restart with each burst.
//! Setup and ARP resolution are untimed, and caller time stays fixed.
//! Forwarding and every frame's size, payload and checksum checks are untimed.
//! Every iteration checks k accepted frames, k received datagrams in order,
//! an empty receive queue and rx_dropped_full == 0. Send cases check the
//! received payloads in full; recv cases check the saved sequence bytes.
//! The first warmup iteration settles the pool counts. Five more warmup
//! iterations check both RX and TX pool counts return to that idle state.
//! Measured iterations skip pool checks because they walk mempool caches.
//! One throughput element is a datagram. Timings include the timer read;
//! clock times an empty region with the same two Instant reads as emit.rs.
//! The timer tick may be coarse in a VM; Criterion averages these samples.
//! send_frame/<size>/nocsum/paths4 is the nocsum frame send with four `StackConfig::paths`
//! rules on both Stacks (`PATHS4`), so each send's size check scans four entries.
//!
//! Offload turns on the TX UDP checksum profile on A's net_ring Port with
//! `Port::force_tx_udp_checksum` (feature `bench-offload`), so smoltcp writes
//! 0 and flush sets the TX flags and lengths. net_ring has no `tx_prepare` and
//! fills nothing, so frames arrive with checksum 0. It measures that software
//! cost, not NIC throughput. There is no recv offload mode because net_ring
//! never reports RX L4 checksum GOOD and refuses the RX checksum profile at
//! open.

use std::hint::black_box;
use std::time::{Duration, Instant};

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use dpdk_smoltcp::smoltcp::iface::PathMtu;
use dpdk_smoltcp::smoltcp::wire::{
    EthernetFrame, EthernetProtocol, IpEndpoint, IpProtocol, Ipv4Address, Ipv4Cidr, Ipv4Packet,
    UdpPacket,
};
use dpdk_smoltcp::{tx_reserve, Eal, Sent, Turn, UdpConfig, UdpHandle, UdpSendError};

use zhida_test_kit::pair::{self, Pair, Side, T0};
use zhida_test_kit::with_eal;

const PORT: u16 = 1234;
const BURST: usize = 32;
const WARMUP: usize = 6;
const UDP_HEADERS: usize = 14 + 20 + 8;
const PATTERN: u8 = 0xa5;

#[derive(Clone, Copy)]
enum Case {
    SendCopy,
    SendFrame,
    Recv,
}

impl Case {
    fn name(self) -> &'static str {
        match self {
            Case::SendCopy => "send_copy",
            Case::SendFrame => "send_frame",
            Case::Recv => "recv",
        }
    }
}

/// k payload writes into the app's buffer, each copied by `udp_send`, then
/// one flush, timed.
fn send_slices(
    pair: &mut Pair,
    handle: &UdpHandle,
    size: usize,
    sequences: &[u8],
) -> (Result<Sent, dpdk_smoltcp::StackError>, Duration) {
    let to = Some(endpoint(Side::B));
    let mut buffer = [0u8; 1472];
    let mut results = [Ok(()); BURST];
    let start = Instant::now();
    for (result, &sequence) in results.iter_mut().zip(sequences) {
        let payload = &mut buffer[..size];
        payload.fill(PATTERN);
        payload[0] = sequence;
        *result = pair.a.udp_send(handle, to, black_box(&*payload));
    }
    let sent = pair.a.flush();
    let send_elapsed = start.elapsed();
    for result in &results[..sequences.len()] {
        assert_eq!(*result, Ok(()));
    }
    (sent, send_elapsed)
}

/// k frame allocations, payload writes and frame sends, then one flush, timed.
fn send_frames(
    pair: &mut Pair,
    handle: &UdpHandle,
    size: usize,
    sequences: &[u8],
) -> (Result<Sent, dpdk_smoltcp::StackError>, Duration) {
    let to = Some(endpoint(Side::B));
    let mut sent_ok = [false; BURST];
    let start = Instant::now();
    for (ok, &sequence) in sent_ok.iter_mut().zip(sequences) {
        let mut frame = pair.a.tx_frame().expect("frame pool empty");
        let payload = &mut frame.room_mut()[..size];
        payload.fill(PATTERN);
        payload[0] = sequence;
        frame.set_len(size);
        *ok = pair.a.udp_send_frame(handle, to, frame).is_ok();
    }
    let sent = pair.a.flush();
    let send_elapsed = start.elapsed();
    assert!(sent_ok[..sequences.len()].iter().all(|&ok| ok), "udp_send_frame refused");
    (sent, send_elapsed)
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Mode {
    Csum,
    Nocsum,
    Offload,
}

impl Mode {
    fn name(self) -> &'static str {
        match self {
            Self::Csum => "csum",
            Self::Nocsum => "nocsum",
            Self::Offload => "offload",
        }
    }

    fn tx_checksum(self) -> bool {
        matches!(self, Self::Csum | Self::Offload)
    }
}

fn endpoint(side: Side) -> IpEndpoint {
    IpEndpoint::new(side.ip().into(), PORT)
}

/// Four path rules for the paths4 cases: B (on-subnet) matches the implicit subnet entry after
/// three /32 entries, so every send scans four entries. All are 1500, the Port MTU, so limits
/// stay as without a table.
const PATHS4: [PathMtu; 4] = [
    PathMtu { prefix: Ipv4Cidr::new(Ipv4Address::new(10, 0, 0, 97), 32), mtu: 1500 },
    PathMtu { prefix: Ipv4Cidr::new(Ipv4Address::new(10, 0, 0, 98), 32), mtu: 1500 },
    PathMtu { prefix: Ipv4Cidr::new(Ipv4Address::new(10, 0, 0, 99), 32), mtu: 1500 },
    PathMtu { prefix: Ipv4Cidr::new(Ipv4Address::UNSPECIFIED, 0), mtu: 1500 },
];

fn setup(eal: &Eal, mode: Mode, paths: &[PathMtu]) -> (Pair, [UdpHandle; 2]) {
    let configs = [Side::A, Side::B].map(|side| dpdk_smoltcp::StackConfig {
        max_udp_sockets: 1,
        paths,
        ..pair::config(side)
    });
    let mut pair = Pair::with_opened_ports(
        eal,
        ["udpa", "udpb"],
        configs,
        T0,
        128,
        |side, port| {
            port.rx_burst = u16::try_from(BURST).unwrap();
            port.tx_burst = u16::try_from(BURST).unwrap();
            port.rx_pool.count = 255;
            port.tx_pool.count =
                tx_reserve(port.tx_descriptors, configs[side.index()].tx_pending) as u32;
        },
        |side, port| {
            if side == Side::A && mode == Mode::Offload {
                port.force_tx_udp_checksum();
            }
        },
    );
    let handles = [&mut pair.a, &mut pair.b].map(|stack| {
        stack
            .add_udp(UdpConfig {
                port: PORT,
                local_addr: None,
                rx_queue: BURST,
                tx_checksum: mode.tx_checksum(),
                broadcast: false,
            })
            .unwrap()
    });
    assert_eq!(
        pair.a.udp_send(&handles[0], Some(endpoint(Side::B)), &[0]),
        Err(UdpSendError::NeighborPending)
    );
    assert_sent(pair.a.flush().unwrap(), 1);
    assert_eq!(pair.a_to_b.forward(), 1);
    assert_turn(pair.b.receive(T0, BURST).unwrap(), 1, 1);
    assert_eq!(pair.b_to_a.forward(), 1);
    assert_turn(pair.a.receive(T0, BURST).unwrap(), 1, 0);
    (pair, handles)
}

fn assert_sent(sent: Sent, k: usize) {
    assert_eq!(
        sent,
        Sent {
            tx_accepted: k,
            tx_pending: 0
        }
    );
}

fn assert_turn(turn: Turn, received: usize, sent: usize) {
    assert_eq!(
        turn,
        Turn {
            rx_received: received,
            rx_rejected: 0,
            rx_processed: received,
            rx_staged: 0,
            tx_accepted: sent,
            tx_pending: 0,
        }
    );
}

fn assert_payload(payload: &[u8], size: usize, sequence: u8) {
    assert_eq!(payload.len(), size);
    assert_eq!(payload[0], sequence);
    assert!(payload[1..].iter().all(|&byte| byte == PATTERN));
}

fn forward(pair: &mut Pair, size: usize, mode: Mode, k: usize) {
    let mut sequence = 0usize;
    let count = pair.a_to_b.forward_with(|frame| {
        assert_eq!(
            frame.len(),
            UDP_HEADERS.checked_add(size).expect("frame size")
        );
        let eth = EthernetFrame::new_checked(frame).unwrap();
        assert_eq!(eth.ethertype(), EthernetProtocol::Ipv4);
        let ip = Ipv4Packet::new_checked(eth.payload()).unwrap();
        assert_eq!((ip.src_addr(), ip.dst_addr()), (Side::A.ip(), Side::B.ip()));
        assert_eq!(ip.next_header(), IpProtocol::Udp);
        assert!(ip.verify_checksum());
        let udp = UdpPacket::new_checked(ip.payload()).unwrap();
        assert_eq!((udp.src_port(), udp.dst_port()), (PORT, PORT));
        assert_payload(udp.payload(), size, u8::try_from(sequence).unwrap());
        if mode == Mode::Csum {
            assert_ne!(udp.checksum(), 0);
            assert!(udp.verify_checksum(&Side::A.ip().into(), &Side::B.ip().into()));
        } else {
            assert_eq!(udp.checksum(), 0);
        }
        sequence = sequence.checked_add(1).expect("frame count");
    });
    assert_eq!(count, k, "A to B frame count");
    assert_eq!(sequence, k, "A to B delivered frame count");
}

fn iteration(
    pair: &mut Pair,
    handles: &[UdpHandle; 2],
    case: Case,
    size: usize,
    mode: Mode,
    sequences: &[u8],
) -> Duration {
    let k = sequences.len();
    let now = pair.now();
    let (sent, send_elapsed) = match case {
        Case::SendFrame | Case::Recv => send_frames(pair, &handles[0], size, sequences),
        Case::SendCopy => send_slices(pair, &handles[0], size, sequences),
    };
    assert_sent(sent.unwrap(), k);
    forward(pair, size, mode, k);
    let elapsed = match case {
        Case::SendCopy | Case::SendFrame => {
            assert_turn(pair.b.receive(now, k).unwrap(), k, 0);
            for &sequence in sequences {
                let datagram = pair.b.udp_mut(&handles[1]).recv().unwrap();
                assert_payload(datagram.payload(), size, sequence);
                drop(datagram);
            }
            send_elapsed
        }
        Case::Recv => {
            let mut received = [None; BURST];
            let start = Instant::now();
            let turn = pair.b.receive(now, k);
            for byte in &mut received[..k] {
                *byte = pair.b.udp_mut(&handles[1]).recv().map(|datagram| {
                    let sequence = black_box(datagram.payload()[0]);
                    drop(datagram);
                    sequence
                });
            }
            let elapsed = start.elapsed();
            assert_turn(turn.unwrap(), k, 0);
            for (byte, &sequence) in received.iter().zip(sequences) {
                assert_eq!(*byte, Some(sequence));
            }
            elapsed
        }
    };
    assert!(pair.b.udp_mut(&handles[1]).recv().is_none());
    assert_eq!(pair.b.udp(&handles[1]).stats().rx_dropped_full, 0);
    assert_eq!(pair.b_to_a.forward(), 0, "UDP must not generate a reply");
    elapsed
}

fn benches(c: &mut Criterion) {
    with_eal(|eal| {
        let mut group = c.benchmark_group("udp");
        for case in [Case::SendCopy, Case::SendFrame, Case::Recv] {
            for size in [64, 512, 1472] {
                let modes: &[Mode] = match case {
                    Case::SendCopy | Case::SendFrame => &[Mode::Csum, Mode::Nocsum, Mode::Offload],
                    Case::Recv => &[Mode::Csum, Mode::Nocsum],
                };
                for &mode in modes {
                    let tables: &[(&str, &[PathMtu])] =
                        if matches!(case, Case::SendFrame) && mode == Mode::Nocsum {
                            &[("", &[]), ("/paths4", &PATHS4)]
                        } else {
                            &[("", &[])]
                        };
                    let runs = tables.iter().flat_map(|&(table, paths)| {
                        [1usize, 8, 32].map(move |k| (table, paths, k))
                    });
                    for (table, paths, k) in runs {
                        let (mut pair, handles) = setup(eal, mode, paths);
                        let sequences: Vec<_> = (0..k).map(|n| u8::try_from(n).unwrap()).collect();
                        iteration(&mut pair, &handles, case, size, mode, &sequences);
                        let idle = pair.in_use();
                        for _ in 1..WARMUP {
                            iteration(&mut pair, &handles, case, size, mode, &sequences);
                            assert_eq!(pair.in_use(), idle, "pools after warmup burst");
                        }
                        let name = format!("{}/{size}/{}{table}", case.name(), mode.name());
                        group.throughput(Throughput::Elements(u64::try_from(k).unwrap()));
                        group.bench_function(BenchmarkId::new(name, k), |b| {
                            b.iter_custom(|iters| {
                                let mut elapsed = Duration::ZERO;
                                for _ in 0..iters {
                                    elapsed = elapsed
                                        .checked_add(iteration(
                                            &mut pair, &handles, case, size, mode, &sequences,
                                        ))
                                        .expect("total UDP duration");
                                }
                                elapsed
                            });
                        });
                        pair::assert_lossless(&pair.a_to_b, "A to B");
                        pair::assert_lossless(&pair.b_to_a, "B to A");
                        assert_eq!(pair.close(), 0);
                    }
                }
            }
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

criterion_group!(udp, benches);
criterion_main!(udp);
