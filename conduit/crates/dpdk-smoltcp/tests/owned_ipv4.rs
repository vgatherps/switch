//! `StackConfig::owned_ipv4`: the Stack answers ARP and ICMP echo for every owned address, from
//! that address, and for no neighbour; `Stack::new` refuses an owned set holding a non-unicast
//! address, the gateway or the subnet broadcast.

use dpdk_smoltcp::smoltcp::phy::ChecksumCapabilities;
use dpdk_smoltcp::smoltcp::time::{Duration, Instant};
use dpdk_smoltcp::smoltcp::wire::{
    ArpOperation, ArpPacket, ArpRepr, EthernetAddress, EthernetFrame, EthernetProtocol,
    EthernetRepr, Icmpv4Packet, Icmpv4Repr, IpProtocol, Ipv4Address, Ipv4Cidr, Ipv4Packet,
    Ipv4Repr,
};
use dpdk_smoltcp::{Port, Stack, StackConfig, StackError, DEFAULT_NEIGHBOR_CAPACITY};

use zhida_test_kit::{ring_port, with_eal};
use ring_port::{arp, RingPort};

const OURS: EthernetAddress = EthernetAddress([0x02, 0, 0, 0, 0, 0x01]);
const OUR_IP: Ipv4Address = Ipv4Address::new(10, 0, 0, 1);
const PEER: EthernetAddress = EthernetAddress([0x02, 0, 0, 0, 0, 0x02]);
const PEER_IP: Ipv4Address = Ipv4Address::new(10, 0, 0, 2);
const GATEWAY: Ipv4Address = Ipv4Address::new(10, 0, 0, 254);
const T0: Instant = Instant::from_millis_const(5_000);
/// Requests per turn: the RX and TX rings hold 15.
const BATCH: usize = 8;

fn config(subnet: Ipv4Cidr, owned: &[Ipv4Cidr]) -> StackConfig<'_> {
    StackConfig {
        max_udp_sockets: 0,
        ethernet: OURS,
        ipv4: subnet,
        gateway: Some(GATEWAY),
        paths: &[],
        owned_ipv4: owned,
        random_seed: 1,
        neighbor_capacity: DEFAULT_NEIGHBOR_CAPACITY,
        max_sockets: 1,
        rx_staging: 16,
        tx_pending: 16,
    }
}

fn stack(port: Port, subnet: Ipv4Cidr, owned: &[Ipv4Cidr]) -> Stack {
    match Stack::new(port, config(subnet, owned), T0) {
        Ok(stack) => stack,
        Err((_, error)) => panic!("{error}"),
    }
}

fn finish(fixture: RingPort, stack: Stack) {
    assert!(fixture.take_tx().is_empty());
    let port = stack.into_port();
    assert_eq!(fixture.in_use(), (0, 0));
    port.try_close().unwrap();
    fixture.close();
}

fn arp_request(target: Ipv4Address) -> Vec<u8> {
    arp(
        ArpOperation::Request,
        EthernetAddress::BROADCAST,
        (PEER, PEER_IP),
        (EthernetAddress([0; 6]), target),
    )
}

/// The Ethernet and ARP fields of one ARP reply from us to the peer; its sender address.
fn arp_reply_sender(bytes: &[u8]) -> Ipv4Address {
    let eth = EthernetFrame::new_checked(bytes).unwrap();
    assert_eq!(
        (eth.src_addr(), eth.dst_addr(), eth.ethertype()),
        (OURS, PEER, EthernetProtocol::Arp)
    );
    let ArpRepr::EthernetIpv4 {
        operation,
        source_hardware_addr,
        source_protocol_addr,
        target_hardware_addr,
        target_protocol_addr,
    } = ArpRepr::parse(&ArpPacket::new_checked(eth.payload()).unwrap()).unwrap()
    else {
        panic!("not an Ethernet/IPv4 ARP packet");
    };
    assert_eq!(operation, ArpOperation::Reply);
    assert_eq!((source_hardware_addr, target_hardware_addr), (OURS, PEER));
    assert_eq!(target_protocol_addr, PEER_IP);
    source_protocol_addr
}

/// Ask for every target in batches; return the sender addresses of the replies, in order.
fn arp_replies(stack: &mut Stack, fixture: &RingPort, targets: &[Ipv4Address]) -> Vec<Ipv4Address> {
    let mut replied = Vec::new();
    for batch in targets.chunks(BATCH) {
        fixture.inject(&batch.iter().map(|&target| arp_request(target)).collect::<Vec<_>>());
        let turn = stack.turn(T0, BATCH).unwrap();
        assert_eq!((turn.rx_processed, turn.rx_staged), (batch.len(), 0));
        replied.extend(fixture.take_tx().iter().map(|bytes| arp_reply_sender(bytes)));
    }
    replied
}

/// `count` owned single addresses with a neighbour (not owned) after each: every owned address
/// gets one ARP reply sent from it, no neighbour gets one, at 1, 50, 500, 1000 and 4096 owned
/// addresses.
#[zhida_test_support::pinned_test]
fn arp_reply_for_every_owned_address_none_for_neighbours() {
    with_eal(|eal| {
        let subnet = Ipv4Cidr::new(OUR_IP, 16);
        let base = u32::from(Ipv4Address::new(10, 0, 16, 0));
        for count in [1u32, 50, 500, 1000, 4096] {
            let owned: Vec<_> = (0..count)
                .map(|index| Ipv4Cidr::new(Ipv4Address::from(base + 2 * index), 32))
                .collect();
            let fixture = RingPort::new(&format!("own{count}"), 16, 16);
            let mut stack = stack(fixture.open(eal), subnet, &owned);
            let targets: Vec<_> = (0..2 * count).map(|offset| Ipv4Address::from(base + offset)).collect();
            let expected: Vec<_> = owned.iter().map(|cidr| cidr.address()).collect();
            assert_eq!(arp_replies(&mut stack, &fixture, &targets), expected, "{count} owned");
            // The interface address still answers, and an address past the set does not.
            let edges = [OUR_IP, Ipv4Address::from(base - 1), Ipv4Address::from(base + 2 * count)];
            assert_eq!(arp_replies(&mut stack, &fixture, &edges), [OUR_IP]);
            finish(fixture, stack);
        }
    });
}

/// Duplicate, overlapping and adjacent entries merge into one set: both ends of each merged
/// range answer, the addresses just outside do not.
#[zhida_test_support::pinned_test]
fn merged_ranges_answer_at_their_edges_only() {
    with_eal(|eal| {
        let owned: Vec<Ipv4Cidr> = ["10.0.0.32/28", "10.0.0.40/32", "10.0.0.40/32", "10.0.0.48/28", "10.0.0.100/32"]
            .iter()
            .map(|text| text.parse().unwrap())
            .collect();
        let fixture = RingPort::new("ownmerge", 16, 16);
        let mut stack = stack(fixture.open(eal), Ipv4Cidr::new(OUR_IP, 24), &owned);
        let ip = |last| Ipv4Address::new(10, 0, 0, last);
        let targets = [ip(31), ip(32), ip(40), ip(47), ip(48), ip(63), ip(64), ip(99), ip(100), ip(101)];
        assert_eq!(
            arp_replies(&mut stack, &fixture, &targets),
            [ip(32), ip(40), ip(47), ip(48), ip(63), ip(100)]
        );
        finish(fixture, stack);
    });
}

fn echo_request(dst_addr: Ipv4Address) -> Vec<u8> {
    let caps = ChecksumCapabilities::default();
    let icmp = Icmpv4Repr::EchoRequest { ident: 7, seq_no: 9, data: b"owned" };
    let eth = EthernetRepr { src_addr: PEER, dst_addr: OURS, ethertype: EthernetProtocol::Ipv4 };
    let ip = Ipv4Repr {
        src_addr: PEER_IP,
        dst_addr,
        next_header: IpProtocol::Icmp,
        payload_len: icmp.buffer_len(),
        hop_limit: 64,
    };
    let mut bytes = vec![0; eth.buffer_len() + ip.buffer_len() + ip.payload_len];
    let mut frame = EthernetFrame::new_unchecked(&mut bytes);
    eth.emit(&mut frame);
    let mut packet = Ipv4Packet::new_unchecked(frame.payload_mut());
    ip.emit(&mut packet, &caps);
    icmp.emit(&mut Icmpv4Packet::new_unchecked(packet.payload_mut()), &caps);
    bytes
}

/// An echo request to an owned address, in or outside the subnet, is answered from that
/// address; one to a neighbour gets nothing.
#[zhida_test_support::pinned_test]
fn icmp_echo_reply_comes_from_the_owned_address() {
    with_eal(|eal| {
        let in_subnet = Ipv4Address::new(10, 0, 0, 77);
        let outside = Ipv4Address::new(192, 168, 5, 5);
        let owned = [Ipv4Cidr::new(in_subnet, 32), Ipv4Cidr::new(outside, 32)];
        let fixture = RingPort::new("ownicmp", 16, 16);
        let mut stack = stack(fixture.open(eal), Ipv4Cidr::new(OUR_IP, 24), &owned);
        assert_eq!(arp_replies(&mut stack, &fixture, &[OUR_IP]), [OUR_IP]);
        let caps = ChecksumCapabilities::default();
        let now = T0 + Duration::from_millis(1);
        for dst in [OUR_IP, in_subnet, outside] {
            fixture.inject(&[echo_request(dst)]);
            assert_eq!(stack.turn(now, 8).unwrap().tx_accepted, 1, "{dst}");
            let sent = fixture.take_tx();
            let eth = EthernetFrame::new_checked(&sent[0]).unwrap();
            assert_eq!((eth.src_addr(), eth.dst_addr()), (OURS, PEER));
            let ip = Ipv4Packet::new_checked(eth.payload()).unwrap();
            let repr = Ipv4Repr::parse(&ip, &caps).unwrap();
            assert_eq!((repr.src_addr, repr.dst_addr, repr.next_header), (dst, PEER_IP, IpProtocol::Icmp));
            let icmp = Icmpv4Packet::new_checked(ip.payload()).unwrap();
            assert_eq!(
                Icmpv4Repr::parse(&icmp, &caps).unwrap(),
                Icmpv4Repr::EchoReply { ident: 7, seq_no: 9, data: b"owned" }
            );
        }
        fixture.inject(&[echo_request(Ipv4Address::new(10, 0, 0, 78))]);
        assert_eq!(stack.turn(now, 8).unwrap().tx_accepted, 0);
        finish(fixture, stack);
    });
}

/// `Stack::new` refuses an owned set holding a non-unicast address, the gateway, the subnet
/// broadcast or a prefix with host bits set, and returns the Port untouched.
#[zhida_test_support::pinned_test]
fn stack_new_validates_owned_addresses() {
    with_eal(|eal| {
        let fixture = RingPort::new("ownvalid", 16, 16);
        let mut port = fixture.open(eal);
        let subnet = Ipv4Cidr::new(OUR_IP, 24);
        for (text, reason) in [
            ("0.0.0.0/32", "owned IPv4 addresses must be unicast"),
            ("224.0.0.5/32", "owned IPv4 addresses must be unicast"),
            ("255.255.255.255/32", "owned IPv4 addresses must be unicast"),
            ("10.0.0.254/32", "owned IPv4 addresses must not include the gateway"),
            ("10.0.0.240/28", "owned IPv4 addresses must not include the gateway"),
            ("10.0.0.255/32", "owned IPv4 addresses must not include the subnet's broadcast address"),
            ("10.0.1.7/24", "owned IPv4 prefix has host bits set"),
        ] {
            let owned = [text.parse::<Ipv4Cidr>().unwrap()];
            port = match Stack::new(port, config(subnet, &owned), T0) {
                Ok(_) => panic!("{text} accepted"),
                Err((port, error)) => {
                    assert_eq!(error, StackError::InvalidConfig(reason), "{text}");
                    port
                }
            };
        }
        let owned = [Ipv4Cidr::new(Ipv4Address::new(10, 0, 0, 253), 32)];
        let stack = stack(port, subnet, &owned);
        finish(fixture, stack);
    });
}

/// `Stack::listen` refuses an explicit local address we do not own with `Unaddressable` and
/// changes nothing: a closed socket stays closed with its endpoint, window target and transmit
/// capacity; a listening socket keeps listening on its owned address. A peer then connects to that
/// owned address and the connection's local endpoint is it.
#[zhida_test_support::pinned_test]
fn listen_refuses_unowned_address_and_accepts_on_owned() {
    use dpdk_smoltcp::smoltcp::socket::tcp::{ListenError, State};
    use dpdk_smoltcp::smoltcp::wire::{IpEndpoint, IpListenEndpoint};
    use zhida_test_kit::pair::{self, tcp_socket_sized, App, Pair, Side};

    with_eal(|eal| {
        let owned_ip = Ipv4Address::new(10, 0, 0, 77);
        let owned = [Ipv4Cidr::new(owned_ip, 32)];
        let configs = [pair::config(Side::A), StackConfig { owned_ipv4: &owned, ..pair::config(Side::B) }];
        let mut pair = Pair::new(eal, ["ownla", "ownlb"], configs, pair::T0);
        let server = pair.b.add_tcp(tcp_socket_sized(4096)).unwrap();
        let snapshot = |stack: &Stack| {
            let socket = stack.tcp(&server);
            (
                socket.state(),
                socket.listen_endpoint(),
                socket.local_endpoint(),
                socket.recv_capacity(),
                socket.send_capacity(),
            )
        };
        let before = snapshot(&pair.b);
        for foreign in [Side::A.ip(), Ipv4Address::new(10, 0, 0, 78), Ipv4Address::new(192, 0, 2, 1)] {
            assert_eq!(pair.b.listen(&server, (foreign, 80)), Err(ListenError::Unaddressable));
            assert_eq!(snapshot(&pair.b), before, "{foreign}");
        }
        pair.b.listen(&server, (owned_ip, 80)).unwrap();
        let listening = snapshot(&pair.b);
        assert_eq!((listening.0, listening.1), (State::Listen, IpListenEndpoint::from((owned_ip, 80))));
        assert_eq!(pair.b.listen(&server, (Side::A.ip(), 80)), Err(ListenError::Unaddressable));
        assert_eq!(snapshot(&pair.b), listening);
        pair.b.listen(&server, (Side::B.ip(), 81)).unwrap_err();
        assert_eq!(snapshot(&pair.b), listening);

        let client = pair.a.add_tcp(tcp_socket_sized(4096)).unwrap();
        pair.a.connect(&client, IpEndpoint::new(owned_ip.into(), 80), 49152).unwrap();
        pair.drive(|_, _| {}, |a, b| {
            if a.tcp(&client).state() == State::Established && b.tcp(&server).state() == State::Established {
                App::Done
            } else {
                App::Idle
            }
        });
        assert_eq!(pair.b.tcp(&server).local_endpoint(), Some(IpEndpoint::new(owned_ip.into(), 80)));
        assert_eq!(pair.a.tcp(&client).remote_endpoint(), Some(IpEndpoint::new(owned_ip.into(), 80)));
        pair.close();
    });
}
