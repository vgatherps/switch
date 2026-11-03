use std::ffi::c_void;
use std::ptr::null_mut;

use zhida_test_kit::pair::{self, App, Pair, Side};
use zhida_test_kit::ring_port::RingPort;
use zhida_test_kit::{wire, with_eal};
use dpdk_smoltcp::smoltcp::phy::ChecksumCapabilities;
use dpdk_smoltcp::smoltcp::time::Instant;
use dpdk_smoltcp::smoltcp::wire::{
    EthernetFrame, EthernetProtocol, EthernetRepr, IpEndpoint, IpProtocol, Ipv4Address, Ipv4Cidr, Ipv4Packet,
    Ipv4Repr, UdpPacket, UdpRepr,
};
use dpdk_smoltcp::{
    rx_reserve, AddUdpError, Port, PortError, Stack, StackConfig, UdpConfig,
    DEFAULT_NEIGHBOR_CAPACITY,
};
use zhida_dpdk_sys::dpdk_raw_ring_enqueue_burst;

const T0: Instant = pair::T0;
const PORT: u16 = 1234;
const PEER_PORT: u16 = 4321;

fn config(max_udp_sockets: usize) -> StackConfig<'static> {
    StackConfig {
        ethernet: Side::A.mac(),
        ipv4: Ipv4Cidr::new(Side::A.ip(), 24),
        gateway: None,
        paths: &[],
        owned_ipv4: &[],
        random_seed: 1,
        neighbor_capacity: DEFAULT_NEIGHBOR_CAPACITY,
        max_sockets: 2,
        max_udp_sockets,
        rx_staging: 8,
        tx_pending: 8,
    }
}

fn new_stack(port: Port, max_udp_sockets: usize) -> Stack {
    match Stack::new(port, config(max_udp_sockets), T0) {
        Ok(stack) => stack,
        Err((_, error)) => panic!("{error}"),
    }
}

fn udp_config(port: u16, rx_queue: usize) -> UdpConfig {
    UdpConfig {
        port,
        local_addr: None,
        rx_queue,
        tx_checksum: true,
        broadcast: false,
    }
}

fn from_peer(src_port: u16, payload: &[u8]) -> Vec<u8> {
    let caps = ChecksumCapabilities::default();
    let udp = UdpRepr {
        src_port,
        dst_port: PORT,
    };
    let eth = EthernetRepr {
        src_addr: Side::B.mac(),
        dst_addr: Side::A.mac(),
        ethertype: EthernetProtocol::Ipv4,
    };
    let ip = Ipv4Repr {
        src_addr: Side::B.ip(),
        dst_addr: Side::A.ip(),
        next_header: IpProtocol::Udp,
        payload_len: udp.header_len() + payload.len(),
        hop_limit: 64,
    };
    let mut bytes = vec![0; eth.buffer_len() + ip.buffer_len() + ip.payload_len];
    let mut frame = EthernetFrame::new_unchecked(&mut bytes);
    eth.emit(&mut frame);
    let mut packet = Ipv4Packet::new_unchecked(frame.payload_mut());
    ip.emit(&mut packet, &caps);
    udp.emit(
        &mut UdpPacket::new_unchecked(packet.payload_mut()),
        &Side::B.ip().into(),
        &Side::A.ip().into(),
        payload.len(),
        |buffer| buffer.copy_from_slice(payload),
        &caps,
    );
    bytes
}

fn finish(fixture: RingPort, stack: Stack) {
    assert!(fixture.take_tx().is_empty());
    let port = stack.into_port();
    assert_eq!(fixture.in_use(), (0, 0));
    port.try_close().unwrap();
    fixture.close();
}

#[zhida_test_support::pinned_test]
fn udp_rx_payload_points_into_rx_mbuf() {
    with_eal(|eal| {
        let fixture = RingPort::new("udpaddr", 16, 16);
        let mut stack = new_stack(fixture.open(eal), 1);
        let handle = stack.add_udp(udp_config(PORT, 2)).unwrap();
        let bytes = from_peer(PEER_PORT, b"mbuf payload");
        let packet = wire::copy_into(fixture.rx_pool(), &bytes);
        let (start, end, payload_addr) = unsafe {
            let mbuf = &*packet;
            let start = mbuf.buf_addr as usize;
            let end = start.checked_add(usize::from(mbuf.buf_len())).unwrap();
            let payload_addr = (mbuf.data_ptr() as usize).checked_add(14 + 20 + 8).unwrap();
            (start, end, payload_addr)
        };
        let raw = [packet.cast::<c_void>()];
        let queued =
            unsafe { dpdk_raw_ring_enqueue_burst(fixture.rx_ring(), raw.as_ptr(), 1, null_mut()) };
        if queued != 1 {
            unsafe { zhida_dpdk_sys::dpdk_raw_pktmbuf_free(packet) };
        }
        assert_eq!(queued, 1);
        let turn = stack.turn(T0, 8).unwrap();
        assert_eq!((turn.rx_received, turn.rx_processed), (1, 1));
        let datagram = stack.udp_mut(&handle).recv().unwrap();
        let address = datagram.payload().as_ptr() as usize;
        assert!(address >= start);
        assert!(address.checked_add(datagram.payload().len()).unwrap() <= end);
        assert_eq!(address, payload_addr);
        assert_eq!(datagram.payload(), b"mbuf payload");
        assert_eq!(
            datagram.src(),
            IpEndpoint::new(Side::B.ip().into(), PEER_PORT)
        );
        assert_eq!(datagram.dst_addr(), Side::A.ip());
        assert_eq!(stack.udp(&handle).rx_queued(), 0);
        drop(datagram);
        finish(fixture, stack);
    });
}

#[zhida_test_support::pinned_test]
fn datagram_drop_returns_mbuf() {
    with_eal(|eal| {
        let fixture = RingPort::new("udpdrop", 16, 16);
        let mut stack = new_stack(fixture.open(eal), 1);
        let handle = stack.add_udp(udp_config(PORT, 2)).unwrap();
        assert_eq!(fixture.in_use().0, 0);
        fixture.inject(&[from_peer(PEER_PORT, b"held")]);
        stack.turn(T0, 8).unwrap();
        assert_eq!(fixture.in_use().0, 1);
        let datagram = stack.udp_mut(&handle).recv().unwrap();
        assert_eq!(fixture.in_use().0, 1);
        drop(datagram);
        assert_eq!(fixture.in_use().0, 0);
        finish(fixture, stack);
    });
}

#[zhida_test_support::pinned_test]
fn held_datagrams_make_try_close_refuse_then_succeed() {
    with_eal(|eal| {
        let fixture = RingPort::new("udpclose", 16, 16);
        let mut stack = new_stack(fixture.open(eal), 1);
        let handle = stack.add_udp(udp_config(PORT, 2)).unwrap();
        fixture.inject(&[
            from_peer(PEER_PORT, b"held"),
            from_peer(PEER_PORT, b"queued"),
        ]);
        stack.turn(T0, 8).unwrap();
        let datagram = stack.udp_mut(&handle).recv().unwrap();
        let port = stack.into_port();
        assert_eq!(fixture.in_use(), (1, 0));
        let (port, error) = port.try_close().unwrap_err();
        assert_eq!(error, PortError::OutstandingBuffers { rx: 1, tx: 0, frame: 0 });
        assert_eq!(datagram.payload(), b"held");
        drop(datagram);
        assert_eq!(fixture.in_use(), (0, 0));
        port.try_close().unwrap();
        fixture.close();
    });
}

#[zhida_test_support::pinned_test]
fn over_limit_dropped_counted_tcp_progresses() {
    with_eal(|eal| {
        let configs = [Side::A, Side::B].map(|side| StackConfig {
            max_udp_sockets: 1,
            ..pair::config(side)
        });
        let mut pair = Pair::with_pools(eal, ["udptcpa", "udptcpb"], configs, T0, 16, 127, 63, 63);
        let (handles, _) = pair::establish(&mut pair, Side::A);
        let udp = pair.a.add_udp(udp_config(PORT, 2)).unwrap();
        let before = pair.in_use()[0].0;
        pair.inject(
            Side::A,
            &[from_peer(PEER_PORT, b"one"), from_peer(PEER_PORT, b"two")],
        );
        pair.round(|_, _| {});
        let first = pair.a.udp_mut(&udp).recv().unwrap();
        let second = pair.a.udp_mut(&udp).recv().unwrap();
        assert_eq!(first.payload(), b"one");
        assert_eq!(second.payload(), b"two");
        assert_eq!(pair.a.udp(&udp).rx_queued(), 0);
        assert_eq!(pair.in_use()[0].0, before + 2);
        pair.inject(Side::A, &[from_peer(PEER_PORT, b"three")]);
        let round = pair.round(|_, _| {});
        assert_eq!(round.a.rx_processed, 1);
        assert_eq!(pair.a.udp(&udp).stats().rx_dropped_full, 1);
        assert!(pair.a.udp_mut(&udp).recv().is_none());
        assert_eq!(pair.in_use()[0].0, before + 2);

        assert_eq!(
            pair.a.tcp_mut(&handles[0]).send_slice(b"tcp still works"),
            Ok(15)
        );
        let mut received = Vec::new();
        pair.drive(
            |_, _| {},
            |_, b| {
                if b.tcp(&handles[1]).can_recv() {
                    let bytes = pair::drain_bytes(b.tcp_mut(&handles[1])).unwrap();
                    received.extend_from_slice(&bytes);
                    return App::Moved;
                }
                if received.len() == 15 {
                    App::Done
                } else {
                    App::Idle
                }
            },
        );
        assert_eq!(received, b"tcp still works");
        assert_eq!(pair.b.tcp_mut(&handles[1]).send_slice(b"reply"), Ok(5));
        let mut reply = Vec::new();
        pair.drive(
            |_, _| {},
            |a, _| {
                if a.tcp(&handles[0]).can_recv() {
                    let bytes = pair::drain_bytes(a.tcp_mut(&handles[0])).unwrap();
                    reply.extend_from_slice(&bytes);
                    return App::Moved;
                }
                if reply.len() == 5 {
                    App::Done
                } else {
                    App::Idle
                }
            },
        );
        assert_eq!(reply, b"reply");
        assert_eq!(pair.a.udp(&udp).stats().rx_dropped_full, 1);
        drop((first, second));
        assert_eq!(pair.in_use()[0].0, before);
        pair.close();
    });
}

/// The RX pool holds 11 mbufs beyond its reserve; a 4096-byte TCP socket (6 frames) and UDP
/// queues of 5 and 64 datagrams are all added.
#[zhida_test_support::pinned_test]
fn add_udp_admits_queues_beyond_the_shared_rx_pool() {
    with_eal(|eal| {
        let fixture = RingPort::new("udpcap", 16, 16);
        let mut port_config = fixture.config();
        let reserve = rx_reserve(
            port_config.rx_descriptors,
            port_config.rx_burst,
            8,
            port_config.rx_pool.cache_size,
        );
        port_config.rx_pool.count = u32::try_from(reserve + 6 + 5).unwrap();
        let mut stack = new_stack(fixture.open_with(eal, port_config), 2);
        let tcp = stack.add_tcp(pair::tcp_socket_sized(4096)).unwrap();
        let udp = stack.add_udp(udp_config(PORT, 5)).unwrap();
        let big = stack.add_udp(udp_config(PORT + 1, 64)).unwrap();
        assert_eq!(stack.udp(&udp).config(), &udp_config(PORT, 5));
        assert_eq!(stack.udp(&big).config(), &udp_config(PORT + 1, 64));
        stack.remove_udp(big);
        stack.remove_udp(udp);
        stack.remove_tcp(tcp);
        finish(fixture, stack);
    });
}

#[zhida_test_support::pinned_test]
fn removed_socket_held_datagrams_keep_rx_mbufs_until_dropped() {
    with_eal(|eal| {
        let fixture = RingPort::new("udprm", 16, 16);
        let mut port_config = fixture.config();
        let reserve = rx_reserve(
            port_config.rx_descriptors,
            port_config.rx_burst,
            8,
            port_config.rx_pool.cache_size,
        );
        port_config.rx_pool.count = u32::try_from(reserve + 6).unwrap();
        let mut stack = new_stack(fixture.open_with(eal, port_config), 1);
        let old = stack.add_udp(udp_config(PORT, 6)).unwrap();
        fixture.inject(&[
            from_peer(PEER_PORT, b"held"),
            from_peer(PEER_PORT, b"queued"),
        ]);
        stack.turn(T0, 8).unwrap();
        let first = stack.udp_mut(&old).recv().unwrap();
        stack.remove_udp(old);
        assert_eq!(fixture.in_use().0, 1);
        let replacement = stack.add_udp(udp_config(PORT, 5)).unwrap();
        fixture.inject(&[from_peer(PEER_PORT, b"second")]);
        stack.turn(T0, 8).unwrap();
        let second = stack.udp_mut(&replacement).recv().unwrap();
        stack.remove_udp(replacement);
        assert_eq!(fixture.in_use().0, 2);
        for (held, datagram) in [(2, first), (1, second)] {
            let udp = stack.add_udp(udp_config(PORT, 6)).unwrap();
            stack.remove_udp(udp);
            assert_eq!(fixture.in_use().0, held);
            drop(datagram);
            assert_eq!(fixture.in_use().0, held - 1);
        }
        let udp = stack.add_udp(udp_config(PORT, 6)).unwrap();
        stack.remove_udp(udp);
        let tcp = stack.add_tcp(pair::tcp_socket_sized(4096)).unwrap();
        stack.remove_tcp(tcp);
        finish(fixture, stack);
    });
}

/// A local address the Stack does not own is refused with nothing changed: no slot taken, so a
/// socket needing the only slot then fits on an owned address (also outside the subnet).
#[zhida_test_support::pinned_test]
fn unowned_local_address_refused_without_change() {
    const OWNED: Ipv4Address = Ipv4Address::new(10, 9, 0, 1);
    with_eal(|eal| {
        let fixture = RingPort::new("udpunown", 16, 16);
        let port_config = fixture.config();
        let owned = [Ipv4Cidr::new(OWNED, 32)];
        let config = StackConfig { owned_ipv4: &owned, ..config(1) };
        let mut stack = match Stack::new(fixture.open_with(eal, port_config), config, T0) {
            Ok(stack) => stack,
            Err((_, error)) => panic!("{error}"),
        };
        let at = |local_addr| UdpConfig { local_addr: Some(local_addr), ..udp_config(PORT, 6) };
        for unowned in [Ipv4Address::new(10, 0, 0, 9), Ipv4Address::new(10, 9, 0, 2), Ipv4Address::BROADCAST] {
            assert_eq!(stack.add_udp(at(unowned)).unwrap_err(), AddUdpError::Unaddressable);
        }
        let handle = stack.add_udp(at(OWNED)).unwrap();
        assert_eq!(stack.udp(&handle).config().local_addr, Some(OWNED));
        stack.remove_udp(handle);
        let handle = stack.add_udp(at(Side::A.ip())).unwrap();
        stack.remove_udp(handle);
        finish(fixture, stack);
    });
}

#[zhida_test_support::pinned_test]
fn port_in_use_and_full_refused() {
    with_eal(|eal| {
        let fixture = RingPort::new("udpfull", 16, 16);
        let mut stack = new_stack(fixture.open(eal), 0);
        assert_eq!(stack.add_udp(udp_config(PORT, 1)).unwrap_err(), AddUdpError::Full);
        finish(fixture, stack);

        let fixture = RingPort::new("udpbind", 16, 16);
        let mut stack = new_stack(fixture.open(eal), 1);
        assert!(matches!(
            stack.add_udp(udp_config(0, 1)),
            Err(AddUdpError::InvalidConfig(_))
        ));
        assert!(matches!(
            stack.add_udp(udp_config(PORT, 0)),
            Err(AddUdpError::InvalidConfig(_))
        ));
        let handle = stack.add_udp(udp_config(PORT, 2)).unwrap();
        assert_eq!(
            stack.add_udp(udp_config(PORT, 2)).unwrap_err(),
            AddUdpError::PortInUse
        );
        assert_eq!(
            stack.add_udp(udp_config(PORT + 1, 2)).unwrap_err(),
            AddUdpError::Full
        );
        assert_eq!(stack.udp(&handle).config(), &udp_config(PORT, 2));
        stack.remove_udp(handle);
        let _replacement = stack.add_udp(udp_config(PORT + 1, 2)).unwrap();
        finish(fixture, stack);
    });
}

#[zhida_test_support::pinned_test]
fn connected_socket_drops_other_peer() {
    with_eal(|eal| {
        let fixture = RingPort::new("udppeer", 16, 16);
        let mut stack = new_stack(fixture.open(eal), 1);
        let handle = stack.add_udp(udp_config(PORT, 2)).unwrap();
        let peer = IpEndpoint::new(Side::B.ip().into(), PEER_PORT);
        stack.udp_mut(&handle).connect(peer).unwrap();
        fixture.inject(&[
            from_peer(PEER_PORT + 1, b"other"),
            from_peer(PEER_PORT, b"peer"),
        ]);
        stack.turn(T0, 8).unwrap();
        assert_eq!(fixture.in_use().0, 1);
        let datagram = stack.udp_mut(&handle).recv().unwrap();
        assert_eq!(datagram.src(), peer);
        assert_eq!(datagram.payload(), b"peer");
        assert!(stack.udp_mut(&handle).recv().is_none());
        drop(datagram);
        finish(fixture, stack);
    });
}
