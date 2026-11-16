//! `Stack::udp_send_frame` on net_ring: wire bytes of unique and shared
//! frames, every refusal before the device, and broadcast of one frame to
//! many sockets. Mbuf metadata and refcounts are checked in-crate
//! (src/device_tests.rs, `udp_*frame*`).

use dpdk_smoltcp::smoltcp::iface::PathMtu;
use dpdk_smoltcp::smoltcp::phy::ImmutableFrame;
use dpdk_smoltcp::smoltcp::time::Instant;
use dpdk_smoltcp::smoltcp::wire::{
    ArpOperation, EthernetAddress, EthernetFrame, EthernetProtocol, IpEndpoint, Ipv4Address,
    Ipv4Cidr, Ipv4Packet, UdpPacket,
};
use dpdk_smoltcp::{
    Port, Stack, StackConfig, UdpConfig, UdpHandle, UdpSendError, UdpSendFrameError,
    DEFAULT_NEIGHBOR_CAPACITY,
};
use switch_test_kit::ring_port::{arp, RingPort};
use switch_test_kit::with_eal;

const T: Instant = Instant::from_millis_const(10);
const MAC: EthernetAddress = EthernetAddress([0x02, 0, 0, 0, 0, 0x01]);
const IP: Ipv4Address = Ipv4Address::new(10, 0, 0, 1);
const PEER_MAC: EthernetAddress = EthernetAddress([0x02, 0, 0, 0, 0, 0x02]);
const PEER: Ipv4Address = Ipv4Address::new(10, 0, 0, 2);
/// Path MTU 1000 to this on-link host: payloads above 972 are TooLarge.
const NARROW: Ipv4Address = Ipv4Address::new(10, 0, 0, 3);
const PATHS: [PathMtu; 1] = [PathMtu {
    prefix: Ipv4Cidr::new(NARROW, 32),
    mtu: 1000,
}];
const PORT: u16 = 49152;
const PEER_PORT: u16 = 80;

fn config(max_udp_sockets: usize) -> StackConfig<'static> {
    StackConfig {
        max_udp_sockets,
        ethernet: MAC,
        ipv4: Ipv4Cidr::new(IP, 24),
        gateway: None,
        paths: &PATHS,
        owned_ipv4: &[],
        random_seed: 1,
        neighbor_capacity: DEFAULT_NEIGHBOR_CAPACITY,
        max_sockets: 1,
        rx_staging: 8,
        tx_pending: 8,
    }
}

fn stack(port: Port, max_udp_sockets: usize) -> Stack {
    match Stack::new(port, config(max_udp_sockets), T) {
        Ok(stack) => stack,
        Err((_, error)) => panic!("{error}"),
    }
}

fn add_udp(stack: &mut Stack, port: u16) -> UdpHandle {
    stack
        .add_udp(UdpConfig {
            port,
            local_addr: None,
            rx_queue: 1,
            tx_checksum: true,
            broadcast: false,
        })
        .unwrap()
}

/// The peer's ARP reply, taken by `stack`.
fn resolve_peer(fixture: &RingPort, stack: &mut Stack) {
    fixture.inject(&[arp(ArpOperation::Reply, MAC, (PEER_MAC, PEER), (MAC, IP))]);
    let turn = stack.receive(T, 8).unwrap();
    assert_eq!((turn.rx_processed, turn.tx_accepted), (1, 0));
}

// ANCHOR: frame
fn frame(stack: &mut Stack, payload: &[u8]) -> ImmutableFrame {
    let mut frame = stack.tx_frame().unwrap();
    frame.as_mut()[..payload.len()].copy_from_slice(payload);
    frame.truncate(payload.len());
    frame.freeze()
}
// ANCHOR_END: frame

fn peer(port: u16) -> Option<IpEndpoint> {
    Some(IpEndpoint::new(PEER.into(), port))
}

/// A datagram from `src_port` to the peer carrying `payload`, with a valid UDP checksum.
fn assert_datagram(bytes: &[u8], src_port: u16, payload: &[u8]) {
    let eth = EthernetFrame::new_checked(bytes).unwrap();
    assert_eq!(
        (eth.src_addr(), eth.dst_addr(), eth.ethertype()),
        (MAC, PEER_MAC, EthernetProtocol::Ipv4)
    );
    let ip = Ipv4Packet::new_checked(eth.payload()).unwrap();
    assert!(ip.verify_checksum());
    assert_eq!((ip.src_addr(), ip.dst_addr()), (IP, PEER));
    let udp = UdpPacket::new_checked(ip.payload()).unwrap();
    assert_eq!((udp.src_port(), udp.dst_port()), (src_port, PEER_PORT));
    assert_eq!(udp.payload(), payload);
    assert_ne!(udp.checksum(), 0);
    assert!(udp.verify_checksum(&IP.into(), &PEER.into()));
}

fn sent(stack: &mut Stack) -> usize {
    stack.flush().unwrap().tx_accepted
}

/// The ring is empty; after the Stack releases its spare TX packet every pool
/// is back to zero and each object returned once.
fn finish(fixture: RingPort, stack: Stack) {
    assert!(fixture.take_tx().is_empty());
    assert_eq!(fixture.frame_in_use(), 0);
    let port = stack.into_port();
    assert_eq!(fixture.in_use(), (0, 0));
    fixture.assert_returned_exactly_once();
    port.try_close().unwrap();
    fixture.close();
}

/// Unique frames (empty, small, the full 1472-byte capacity) and a shared
/// frame reach the wire as the same datagram `udp_send` sends; each frame
/// returns to the pool once the ring consumer frees it, the shared frame's
/// clone when the app drops it.
#[switch_test_support::pinned_test]
fn udp_send_frame_reaches_wire_like_udp_send() {
    with_eal(|eal| {
        let fixture = RingPort::new("udpfr_wire", 16, 16);
        let mut stack = stack(fixture.open(eal), 1);
        let handle = add_udp(&mut stack, PORT);
        resolve_peer(&fixture, &mut stack);
        for len in [0, 1, 64, 1472] {
            let payload: Vec<u8> = (0..len).map(|i| (i % 253) as u8).collect();
            stack.udp_send(&handle, peer(PEER_PORT), &payload).unwrap();
            assert_eq!(sent(&mut stack), 1);
            let copied = fixture.take_tx();
            assert_datagram(&copied[0], PORT, &payload);
            for _ in 0..2 {
                let frame = frame(&mut stack, &payload);
                assert!(stack.udp_send_frame(&handle, peer(PEER_PORT), frame).is_ok());
                assert_eq!(fixture.frame_in_use(), 1);
                assert_eq!(sent(&mut stack), 1);
                assert_eq!(fixture.take_tx(), copied);
                assert_eq!(fixture.frame_in_use(), 0);
            }
            let kept = frame(&mut stack, &payload);
            assert!(stack.udp_send_frame(&handle, peer(PEER_PORT), kept.clone()).is_ok());
            assert_eq!(sent(&mut stack), 1);
            assert_eq!(fixture.take_tx(), copied);
            assert_eq!(fixture.frame_in_use(), 1);
            assert_eq!(kept.as_ref(), payload);
            drop(kept);
            assert_eq!(fixture.frame_in_use(), 0);
        }
        finish(fixture, stack);
    });
}

#[switch_test_support::pinned_test]
fn sliced_unique_udp_frame_reaches_wire() {
    with_eal(|eal| {
        let fixture = RingPort::new("udpfr_slice", 16, 16);
        let mut stack = stack(fixture.open(eal), 1);
        let handle = add_udp(&mut stack, PORT);
        resolve_peer(&fixture, &mut stack);
        // Exercise both the initial header build and the cached template.
        for _ in 0..2 {
            let sliced = frame(&mut stack, b"!slice").slice(1..6);
            assert!(stack.udp_send_frame(&handle, peer(PEER_PORT), sliced).is_ok());
            assert_eq!(sent(&mut stack), 1);
            let datagrams = fixture.take_tx();
            assert_eq!(datagrams.len(), 1);
            assert_datagram(&datagrams[0], PORT, b"slice");
            assert_eq!(fixture.frame_in_use(), 0);
        }
        finish(fixture, stack);
    });
}

#[track_caller]
fn refused(
    stack: &mut Stack,
    handle: &UdpHandle,
    to: Option<IpEndpoint>,
    frame: ImmutableFrame,
    error: UdpSendFrameError,
) -> ImmutableFrame {
    let payload = frame.as_ref().to_vec();
    match stack.udp_send_frame(handle, to, frame) {
        Ok(()) => panic!("send succeeded"),
        Err((frame, got)) => {
            assert_eq!(got, error);
            assert_eq!(frame.as_ref(), payload);
            frame
        }
    }
}

/// Refusals before the device return the frame untouched and send nothing:
/// a frame from another Stack's pool (`Foreign`, even to a valid
/// destination), then the destination errors in their order.
#[switch_test_support::pinned_test]
fn udp_send_frame_refusals_return_frame_and_send_nothing() {
    with_eal(|eal| {
        let fixture = RingPort::new("udpfr_ref", 16, 16);
        let other_fixture = RingPort::new("udpfr_oth", 16, 16);
        let mut stack = stack(fixture.open(eal), 2);
        let mut other = stack_other(other_fixture.open(eal));
        let handle = add_udp(&mut stack, PORT);
        let connected = add_udp(&mut stack, PORT + 1);
        stack
            .udp_mut(&connected)
            .connect(IpEndpoint::new(PEER.into(), PEER_PORT))
            .unwrap();
        resolve_peer(&fixture, &mut stack);

        let foreign = frame(&mut other, b"foreign");
        let foreign = refused(&mut stack, &handle, peer(PEER_PORT), foreign, UdpSendFrameError::Foreign);
        assert_eq!(other_fixture.frame_in_use(), 1);
        drop(foreign);

        let udp = UdpSendFrameError::Udp;
        let mut kept = frame(&mut stack, b"kept");
        kept = refused(&mut stack, &handle, None, kept, udp(UdpSendError::NoDestination));
        kept = refused(&mut stack, &connected, peer(PEER_PORT + 1), kept, udp(UdpSendError::PeerMismatch));
        kept = refused(&mut stack, &handle, Some(IpEndpoint::new(IP.into(), PEER_PORT)), kept, udp(UdpSendError::Unaddressable));
        kept = refused(&mut stack, &handle, peer(0), kept, udp(UdpSendError::Unaddressable));
        let narrow = Some(IpEndpoint::new(NARROW.into(), PEER_PORT));
        let mut big = frame(&mut stack, &[7; 973]);
        big = refused(&mut stack, &handle, narrow, big, udp(UdpSendError::TooLarge));
        let off_subnet = Some(IpEndpoint::new(Ipv4Address::new(192, 0, 2, 1).into(), PEER_PORT));
        kept = refused(&mut stack, &handle, off_subnet, kept, udp(UdpSendError::NoRoute));
        assert_eq!(stack.flush().unwrap().tx_accepted, 0);
        assert!(fixture.take_tx().is_empty());
        assert_eq!(fixture.frame_in_use(), 2);
        drop((kept, big));
        finish(fixture, stack);
        finish(other_fixture, other);
    });
}

/// `udp_send` copies into a frame-pool frame, as does `udp_send_frame` for a shared frame:
/// with every frame held by the app both report `WouldBlock` and send nothing, even toward an
/// unroutable destination, because the empty pool is checked before the neighbor lookup.
/// One frame released lets the copy go out.
#[switch_test_support::pinned_test]
fn udp_copy_with_frame_pool_empty_would_block() {
    with_eal(|eal| {
        let fixture = RingPort::new("udpfr_empty", 16, 16);
        let mut stack = stack(fixture.open(eal), 1);
        let handle = add_udp(&mut stack, PORT);
        resolve_peer(&fixture, &mut stack);
        let mut held: Vec<_> = std::iter::from_fn(|| stack.tx_frame()).map(|mut frame| {
            frame.truncate(0);
            frame.freeze()
        }).collect();
        assert!(!held.is_empty());
        assert_eq!(fixture.frame_in_use(), held.len() as u32);
        let off_subnet = Some(IpEndpoint::new(Ipv4Address::new(192, 0, 2, 1).into(), PEER_PORT));
        for to in [peer(PEER_PORT), off_subnet] {
            assert_eq!(stack.udp_send(&handle, to, b"copy"), Err(UdpSendError::WouldBlock));
            let shared = held[0].clone();
            drop(refused(&mut stack, &handle, to, shared, UdpSendFrameError::Udp(UdpSendError::WouldBlock)));
        }
        assert_eq!(sent(&mut stack), 0);
        assert!(fixture.take_tx().is_empty());
        drop(held.pop());
        stack.udp_send(&handle, peer(PEER_PORT), b"copy").unwrap();
        assert_eq!(sent(&mut stack), 1);
        assert_datagram(&fixture.take_tx()[0], PORT, b"copy");
        drop(held);
        finish(fixture, stack);
    });
}

fn stack_other(port: Port) -> Stack {
    let config = StackConfig {
        ethernet: EthernetAddress([0x02, 0, 0, 0, 0, 0x09]),
        ipv4: Ipv4Cidr::new(Ipv4Address::new(10, 0, 0, 9), 24),
        ..config(0)
    };
    match Stack::new(port, config, T) {
        Ok(stack) => stack,
        Err((_, error)) => panic!("{error}"),
    }
}

/// Broadcast of one payload: the app clones one frame for 8 sockets and
/// sends one clone on each. Each socket's datagram reaches the wire with its
/// own source port and the same payload, each from its own frame-pool copy
/// (frame pool in use rises from the app's 1 by 8, the TX pool is untouched);
/// the ring consumer frees every copy, and the frame returns when the app
/// drops it.
#[switch_test_support::pinned_test]
fn udp_broadcast_one_frame_to_eight_sockets() {
    const N: usize = 8;
    with_eal(|eal| {
        let fixture = RingPort::new("udpfr_bc", 16, 16);
        let mut stack = stack(fixture.open(eal), N);
        let handles: Vec<_> = (0..N).map(|i| add_udp(&mut stack, PORT + i as u16)).collect();
        resolve_peer(&fixture, &mut stack);
        let payload = b"one payload, eight sockets";
        let frame = frame(&mut stack, payload);
        let tx_before = fixture.in_use().1;
        for handle in &handles {
            assert!(stack.udp_send_frame(handle, peer(PEER_PORT), frame.clone()).is_ok());
        }
        assert_eq!(fixture.frame_in_use(), 1 + N as u32);
        assert_eq!(sent(&mut stack), N);
        assert_eq!(fixture.in_use().1, tx_before);
        let wire = fixture.take_tx();
        assert_eq!(wire.len(), N);
        for (i, bytes) in wire.iter().enumerate() {
            assert_datagram(bytes, PORT + i as u16, payload);
        }
        assert_eq!(fixture.frame_in_use(), 1);
        assert_eq!(fixture.in_use(), (0, tx_before));
        assert_eq!(frame.as_ref(), payload);
        drop(frame);
        finish(fixture, stack);
    });
}
