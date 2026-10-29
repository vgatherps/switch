//! Stack turn timings, criterion group "stack".
//!
//! stack_arp_reply and stack_rx_no_reply share one Stack on one net_ring port
//! sized like ring.rs: 64-slot rings, RX and TX pools of 511 packets with
//! per-lcore cache 64, RX/TX burst `BATCH` = 32, RX staging and TX pending
//! `BATCH`. Untimed first: one turn on `BATCH` requests for the stack's IP
//! whose replies must all equal the expected ARP reply byte for byte. One
//! iteration, all timed: a test-only producer copies `BATCH` 42-byte ARP
//! request frames into the port's own RX pool and enqueues them on the RX
//! ring, then one `turn` with budget `BATCH` at a fixed time. Throughput
//! element = one received frame (`BATCH` per iteration).
//! - stack_arp_reply: every request asks for the stack's IP, so the turn
//!   sends `BATCH` ARP replies; the iteration ends with the ring peer
//!   dequeuing and freeing exactly those replies.
//! - stack_rx_no_reply: every request asks for another IP in the subnet, so
//!   nothing is sent. Nothing to drain.
//!
//! References in `benches/ring.rs`, same pool and burst sizes but 64-byte
//! frames: ring_rx/owned_rx_burst (the same producer, an RX burst, free) and
//! ring_tx/owned_tx_burst (`BATCH` TX allocs, a TX burst, the same peer
//! drain).
//! Approximate subtractions, each with its residual:
//! - stack_rx_no_reply - owned_rx_burst ~ ARP parse in smoltcp ingress, the
//!   reply buffer the device reserves per RX token and frees unused, and the
//!   per-turn fixed cost / `BATCH`. Residual: the producer copies 42-byte
//!   frames here and 64-byte frames in ring_rx.
//! - stack_arp_reply - owned_rx_burst - (owned_tx_burst - its TX alloc share)
//!   ~ ARP parse and reply emit, the reserve alloc (here used, not freed),
//!   and the per-turn fixed cost / `BATCH`. Residual: the same 42- vs
//!   64-byte producer copies; ring.rs does not time the TX alloc share apart
//!   from the burst, so that term is not measured.
//!
//! established_tcp_turn (the round and its setup are `established::Established`,
//! which benches/scaling.rs shares): two Stacks on the test Pair fixture (net_ring ports
//! with 16-slot rings holding 15, RX/TX burst 8, RX pools sized for the socket
//! budget and reserve, TX pools of 63 packets, per-lcore cache 8, RX staging
//! and TX pending 32) joined by
//! copying Links; A's client connected to B's listener, Cubic with the default
//! 1 MiB initial window, `TCP_BUFFER` bytes each way. Untimed: the handshake
//! must offer a 1460-byte MSS and
//! complete both ways; warmup drives rounds, moving the clock to a stack
//! deadline only when nothing moved (for example, a pending delayed ACK),
//! until one round carries a whole `CHUNK` write as `SEGMENTS`
//! segments with one ACK back; the largest segment payload seen must reach
//! the negotiated send MSS, and every received byte must equal the sent
//! stream. Then
//! `SETTLE_ROUNDS` steady rounds, the timed iteration's body, cover two laps
//! of both socket buffers, each read checked against the sent `CHUNK` and
//! each ending with B's `poll_at` None (no window update or ACK pending).
//! One iteration, all timed, is a full round on the clock fixed at the end
//! of warmup (no timer is needed in steady state; the timed round never
//! calls `poll_at`): A turn, Link A->B, B turn, Link B->A, then A writes
//! `CHUNK` bytes and B reads `CHUNK` bytes into a buffer passed through
//! `black_box` (the app I/O, timed inside the round). Every iteration
//! asserts A received one ACK and sent
//! `SEGMENTS` data frames and B received those and sent one ACK:
//! `SEGMENTS + 1` = 9 frames forwarded (the throughput element) and `CHUNK`
//! payload bytes per iteration. This per-frame figure is an average over 8
//! data frames and one ACK, whose sizes follow the negotiated options, and
//! is not comparable to the ARP per-frame figures.
//!
//! established_link_only, reference for established_tcp_turn: the same
//! fixture sizes and Links, no Stack. Untimed, the last full data frame and
//! the last ACK of established_tcp_turn's warmup are kept as templates. One
//! iteration, all timed, in the round's order: A allocates `SEGMENTS` TX
//! packets, copies the 1514-byte data template into each (the producer:
//! one TX alloc and one whole-frame copy per frame) and bursts them; Link
//! A->B; B's RX burst into a 32-slot queue, then every packet freed; B
//! allocates, copies and bursts one template ACK; Link B->A; A's RX burst and
//! free. Same 9 elements.
//!
//! Approximate: stack cost plus app I/O per round ~ established_tcp_turn -
//! established_link_only. In it: smoltcp ingress and egress processing,
//! header emit, per-turn fixed cost (two flushes, maintenance, egress
//! passes) twice per round, and the app's write and read. Residual: the
//! link-only producer's 9 whole-frame copies (8 full data templates and one
//! ACK) are subtracted, while the stack sends data frames in place after the
//! app's write.

use std::collections::VecDeque;
use std::ffi::{c_void, CStr};
use std::hint::black_box;
use std::ptr::null_mut;

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use dpdk_smoltcp::smoltcp::wire::{
    ArpOperation, EthernetAddress, Ipv4Address, Ipv4Cidr, ETHERNET_HEADER_LEN,
    IPV4_HEADER_LEN,
};
use dpdk_smoltcp::{frame_pool_count, frame_reserve, rx_pool_count, rx_reserve};
use dpdk_smoltcp::{Eal, Packet, Port, PortConfig, RxBurst, Stack, StackConfig, TxPacket};
use zhida_buffer::PoolConfig;
use zhida_dpdk_sys::*;

use zhida_test_kit::{pair, ring_port, wire, with_eal};
use established::{expected_turn, turn_counts, Established, SEGMENTS, TCP_BUFFER};
use pair::{config, segment, Dir, Pair, Side, T0};
use ring_port::RingPort;

mod established;

const BATCH: usize = 32;
const RING_SIZE: u32 = 64;

const STACK_MAC: EthernetAddress = EthernetAddress([0x02, 0, 0, 0, 0, 0x01]);
const PEER_MAC: EthernetAddress = EthernetAddress([0x02, 0, 0, 0, 0, 0x02]);
const STACK_IP: Ipv4Address = Ipv4Address::new(10, 0, 0, 1);
const PEER_IP: Ipv4Address = Ipv4Address::new(10, 0, 0, 2);
const OTHER_IP: Ipv4Address = Ipv4Address::new(10, 0, 0, 3);

fn pool(name: &CStr) -> PoolConfig<'_> {
    PoolConfig {
        name,
        count: 511,
        cache_size: 64,
        data_room_size: RTE_PKTMBUF_HEADROOM as u16 + 2048,
        socket_id: -1,
    }
}

/// Test-only producer: `BATCH` copies of `frame` from the port's own RX pool
/// onto its RX ring.
fn produce_rx(ring: *mut rte_ring, rx_pool: *mut rte_mempool, frame: &[u8]) {
    let mut raw = [null_mut::<c_void>(); BATCH];
    for slot in &mut raw {
        *slot = wire::copy_into(rx_pool, frame).cast();
    }
    let queued =
        unsafe { dpdk_raw_ring_enqueue_burst(ring, raw.as_ptr(), BATCH as u32, null_mut()) };
    assert_eq!(queued as usize, BATCH);
}

/// Ring peer: dequeue exactly one batch, returning each packet before it is
/// freed to `inspect`.
fn peer_drain_with(ring: *mut rte_ring, mut inspect: impl FnMut(*mut rte_mbuf)) {
    let mut raw = [null_mut::<c_void>(); BATCH];
    let count =
        unsafe { dpdk_raw_ring_dequeue_burst(ring, raw.as_mut_ptr(), BATCH as u32, null_mut()) };
    assert_eq!(count as usize, BATCH);
    for &packet in &raw {
        inspect(packet.cast());
        unsafe { dpdk_raw_pktmbuf_free(packet.cast()) };
    }
}

fn peer_drain(ring: *mut rte_ring) {
    peer_drain_with(ring, |_| {});
}

/// ring.rs sizes: 64 descriptors and bursts of `BATCH` each way, `pool` sizes.
fn bench_config(defaults: PortConfig<'_>) -> PortConfig<'_> {
    PortConfig {
        rx_descriptors: 64,
        tx_descriptors: 64,
        rx_burst: BATCH as u16,
        tx_burst: BATCH as u16,
        rx_pool: pool(defaults.rx_pool.name),
        tx_pool: pool(defaults.tx_pool.name),
        frame_pool: pool(defaults.frame_pool.name),
        ..defaults
    }
}

fn arp_benches(c: &mut Criterion, eal: &Eal) {
    let fixture = RingPort::new("stack_bench", RING_SIZE, RING_SIZE);
    let rings = [fixture.rx_ring(), fixture.tx_ring()];
    let port = fixture.open_with(eal, bench_config(fixture.config()));
    let rx_pool = fixture.rx_pool();
    let config = StackConfig {
        max_udp_sockets: 0,
        ethernet: STACK_MAC,
        ipv4: Ipv4Cidr::new(STACK_IP, 24),
        gateway: None,
        paths: &[],
        random_seed: 1,
        neighbor_capacity: dpdk_smoltcp::DEFAULT_NEIGHBOR_CAPACITY,
        segments_per_visit: 1,
        max_sockets: 1,
        rx_staging: BATCH,
        tx_pending: BATCH,
    };
    let mut stack = match Stack::new(port, config, T0) {
        Ok(stack) => stack,
        Err((_, error)) => panic!("{error}"),
    };
    let request = |target| {
        ring_port::arp(
            ArpOperation::Request,
            EthernetAddress::BROADCAST,
            (PEER_MAC, PEER_IP),
            (EthernetAddress([0; 6]), target),
        )
    };
    let (for_stack, for_other) = (request(STACK_IP), request(OTHER_IP));

    let reply = ring_port::arp(
        ArpOperation::Reply,
        PEER_MAC,
        (STACK_MAC, STACK_IP),
        (PEER_MAC, PEER_IP),
    );
    produce_rx(rings[0], rx_pool, &for_stack);
    let turn = stack.turn(T0, BATCH).unwrap();
    assert_eq!(turn_counts(&turn), expected_turn(BATCH, BATCH));
    peer_drain_with(rings[1], |packet| {
        let sent = unsafe {
            std::slice::from_raw_parts((*packet).data_ptr(), usize::from((*packet).data_len()))
        };
        assert_eq!(sent, reply);
    });

    let mut group = c.benchmark_group("stack");
    group.throughput(Throughput::Elements(BATCH as u64));
    group.bench_function("stack_arp_reply", |b| {
        b.iter(|| {
            produce_rx(rings[0], rx_pool, &for_stack);
            let turn = stack.turn(T0, BATCH).unwrap();
            assert_eq!(turn_counts(black_box(&turn)), expected_turn(BATCH, BATCH));
            peer_drain(rings[1]);
        });
    });
    group.bench_function("stack_rx_no_reply", |b| {
        b.iter(|| {
            produce_rx(rings[0], rx_pool, &for_other);
            let turn = stack.turn(T0, BATCH).unwrap();
            assert_eq!(turn_counts(black_box(&turn)), expected_turn(BATCH, 0));
        });
    });
    group.finish();

    stack.into_port().try_close().unwrap();
    fixture.close();
}

/// Steady-state frames as warmup last delivered them.
struct Templates {
    /// A full A->B data segment.
    data: Vec<u8>,
    /// A B->A pure ACK.
    ack: Vec<u8>,
}

fn tcp_bench(c: &mut Criterion, eal: &Eal) -> Templates {
    let configs = [config(Side::A), config(Side::B)];
    let mut pair = Pair::with_ports(eal, ["sbta", "sbtb"], configs, T0, 16, |side, port| {
        port.rx_pool.count = rx_pool_count(
            rx_reserve(
                port.rx_descriptors,
                configs[side.index()].rx_staging,
                port.rx_pool.cache_size,
            ),
            port.mtu,
            [TCP_BUFFER],
        );
        port.frame_pool.count = frame_pool_count(
            frame_reserve(port.tx_descriptors, configs[side.index()].tx_pending),
            port.mtu,
            [TCP_BUFFER],
        );
    });
    let (mut data, mut ack) = (Vec::new(), Vec::new());
    let mut established = Established::connect(&mut pair);
    let send_mss = established.send_mss;
    established.warm_up(&mut pair, |dir, frame| match dir {
        Dir::AToB if segment(frame, Side::A).payload.len() == send_mss => {
            data = frame.to_vec();
        }
        Dir::AToB => {}
        Dir::BToA => ack = frame.to_vec(),
    });
    assert_eq!(
        data.len(),
        ETHERNET_HEADER_LEN + IPV4_HEADER_LEN + segment(&data, Side::A).header_len() + send_mss
    );
    assert_eq!(
        ack.len(),
        ETHERNET_HEADER_LEN + IPV4_HEADER_LEN + segment(&ack, Side::B).header_len()
    );

    let mut group = c.benchmark_group("stack");
    group.throughput(Throughput::Elements(SEGMENTS as u64 + 1));
    group.bench_function("established_tcp_turn", |b| {
        b.iter(|| established.round(&mut pair));
    });
    group.finish();

    for link in [&pair.a_to_b, &pair.b_to_a] {
        assert_eq!((link.pool_empty, link.ring_full), (0, 0));
    }
    pair.close();
    Templates { data, ack }
}

/// Producer: `count` TX packets holding `frame`, burst whole.
fn send_copies(port: &mut Port, pending: &mut VecDeque<TxPacket>, frame: &[u8], count: usize) {
    for _ in 0..count {
        let mut packet = port.alloc_tx(frame.len() as u16).unwrap();
        packet.data_mut().copy_from_slice(frame);
        pending.push_back(packet.into());
    }
    assert_eq!(port.tx_burst(pending).unwrap(), count);
}

/// One RX burst of exactly `count` packets, then every packet freed.
fn receive_and_free(port: &mut Port, staged: &mut VecDeque<Packet>, count: usize) {
    let burst = port.rx_burst(staged).unwrap();
    assert_eq!(
        black_box(burst),
        RxBurst {
            accepted: count,
            rejected: 0,
        }
    );
    staged.clear();
}

fn link_only_bench(c: &mut Criterion, eal: &Eal, templates: &Templates) {
    let fixtures = [RingPort::new("sbla", 16, 16), RingPort::new("sblb", 16, 16)];
    let mut ports = fixtures.each_ref().map(|fixture| fixture.open(eal));
    let mut a_to_b = fixtures[0].link_to(&fixtures[1]);
    let mut b_to_a = fixtures[1].link_to(&fixtures[0]);
    let mut pending = VecDeque::with_capacity(BATCH);
    let mut staged = VecDeque::with_capacity(BATCH);

    let mut group = c.benchmark_group("stack");
    group.throughput(Throughput::Elements(SEGMENTS as u64 + 1));
    group.bench_function("established_link_only", |b| {
        b.iter(|| {
            let [port_a, port_b] = &mut ports;
            send_copies(port_a, &mut pending, &templates.data, SEGMENTS);
            assert_eq!(a_to_b.forward(), SEGMENTS);
            receive_and_free(port_b, &mut staged, SEGMENTS);
            send_copies(port_b, &mut pending, &templates.ack, 1);
            assert_eq!(b_to_a.forward(), 1);
            receive_and_free(port_a, &mut staged, 1);
        });
    });
    group.finish();

    for link in [&a_to_b, &b_to_a] {
        assert_eq!((link.pool_empty, link.ring_full), (0, 0));
    }
    for fixture in &fixtures {
        assert_eq!(fixture.in_use(), (0, 0));
    }
    for port in ports {
        port.try_close().unwrap();
    }
    for fixture in fixtures {
        fixture.close();
    }
}

fn stack_benches(c: &mut Criterion) {
    with_eal(|eal| {
        arp_benches(c, eal);
        let templates = tcp_bench(c, eal);
        link_only_bench(c, eal, &templates);
    });
}

criterion_group!(benches, stack_benches);
criterion_main!(benches);
