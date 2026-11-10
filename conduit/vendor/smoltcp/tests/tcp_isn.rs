use smoltcp::iface::{Config, DropQueue, Interface, SocketHandle, TcpSet};
use smoltcp::phy::{Device, Loopback, Medium, RxToken, TxPool, TxSource, TxToken};
use smoltcp::socket::tcp;
use smoltcp::time::Instant;
use smoltcp::wire::{
    ArpOperation, ArpPacket, ArpRepr, EthernetAddress, EthernetFrame, EthernetProtocol,
    HardwareAddress, IpCidr, Ipv4Address, Ipv4Packet, TcpControl, TcpPacket,
};

const LOCAL: Ipv4Address = Ipv4Address::new(192, 0, 2, 1);
const REMOTE: Ipv4Address = Ipv4Address::new(192, 0, 2, 2);
const LOCAL_MAC: EthernetAddress = EthernetAddress([2, 0, 0, 0, 0, 1]);
const REMOTE_MAC: EthernetAddress = EthernetAddress([2, 0, 0, 0, 0, 2]);

/// An interface that has learned `REMOTE` from an ARP reply, so its first frame to the peer is
/// TCP, not an ARP request.
fn interface(seed: u64) -> (Interface, Loopback) {
    let mut device = Loopback::new(Medium::Ethernet, smoltcp::phy::TxSource::new(smoltcp::phy::TxPool::new(65535)));
    let mut config = Config::new(HardwareAddress::Ethernet(LOCAL_MAC));
    config.random_seed = seed;
    let mut iface = Interface::new(config, &mut device, Instant::ZERO);
    iface.update_ip_addrs(|addrs| addrs.push(IpCidr::new(LOCAL.into(), 24)).unwrap());
    let arp = ArpRepr::EthernetIpv4 {
        operation: ArpOperation::Reply,
        source_hardware_addr: REMOTE_MAC,
        source_protocol_addr: REMOTE,
        target_hardware_addr: LOCAL_MAC,
        target_protocol_addr: LOCAL,
    };
    inject(&mut device, Instant::ZERO, EthernetProtocol::Arp, arp.buffer_len(), |payload| {
        arp.emit(&mut ArpPacket::new_unchecked(payload))
    });
    iface.poll_ingress_single(Instant::ZERO, &mut device, &mut TcpSet::new(1));
    (iface, device)
}

/// Queue a frame from `REMOTE_MAC` to `LOCAL_MAC` whose payload `fill` writes.
fn inject(
    device: &mut Loopback,
    now: Instant,
    ethertype: EthernetProtocol,
    payload_len: usize,
    fill: impl FnOnce(&mut [u8]),
) {
    device
        .transmit(now)
        .unwrap()
        .consume(EthernetFrame::<&[u8]>::buffer_len(payload_len), |bytes| {
            let mut frame = EthernetFrame::new_unchecked(bytes);
            frame.set_src_addr(REMOTE_MAC);
            frame.set_dst_addr(LOCAL_MAC);
            frame.set_ethertype(ethertype);
            fill(frame.payload_mut());
        });
}

fn add_socket(sockets: &mut TcpSet<'static>) -> SocketHandle {
    let config = tcp::Config { rx_capacity: 64, tx_capacity: 64, mss_cap: None };
    let wake = DropQueue::with_capacity(1, 0);
    sockets.add_admitted(&config, 1500, TxSource::new(TxPool::new(1460)), wake)
}

fn take_syn(device: &mut Loopback) -> u32 {
    let frame = device.receive(Instant::ZERO).unwrap().0.into_buf();
    let ethernet = EthernetFrame::new_checked(frame.as_slice()).unwrap();
    assert_eq!(ethernet.ethertype(), EthernetProtocol::Ipv4);
    let ip = Ipv4Packet::new_checked(ethernet.payload()).unwrap();
    let tcp = TcpPacket::new_checked(ip.payload()).unwrap();
    assert!(tcp.syn());
    tcp.seq_number().0 as u32
}

#[test]
fn different_interface_seeds_change_active_syn_isn() {
    let syn = |seed| {
        let (mut iface, mut device) = interface(seed);
        let mut sockets = TcpSet::new(64);
        let handle = add_socket(&mut sockets);
        sockets.connect(handle, iface.context(), (REMOTE, 80), 49152)
            .unwrap();
        iface.poll_egress(Instant::ZERO, &mut device, &mut sockets);
        take_syn(&mut device)
    };
    assert_ne!(syn(1), syn(2));
}

#[test]
fn reconnect_same_tuple_advances_syn_isn_with_interface_time() {
    let (mut iface, mut device) = interface(1);
    let mut sockets = TcpSet::new(64);
    let handle = add_socket(&mut sockets);
    sockets.connect(handle, iface.context(), (REMOTE, 80), 49152)
        .unwrap();
    iface.poll_egress(Instant::ZERO, &mut device, &mut sockets);
    let first = take_syn(&mut device);
    sockets.abort(handle);
    iface.poll_maintenance(Instant::from_millis(1));
    sockets.connect(handle, iface.context(), (REMOTE, 80), 49152)
        .unwrap();
    iface.poll_egress(Instant::from_millis(1), &mut device, &mut sockets);
    let second = take_syn(&mut device);
    assert_eq!(second.wrapping_sub(first), 250);
}

#[test]
fn passive_syn_isn_uses_interface_seed_and_time() {
    use smoltcp::phy::ChecksumCapabilities;
    use smoltcp::wire::{IpProtocol, Ipv4Repr, TcpRepr, TcpSeqNumber};

    let syn = |seed, now| {
        let (mut iface, mut device) = interface(seed);
        let mut sockets = TcpSet::new(64);
        let handle = add_socket(&mut sockets);
        sockets.listen(handle, 80).unwrap();
        let tcp = TcpRepr {
            src_port: 49152,
            dst_port: 80,
            control: TcpControl::Syn,
            seq_number: TcpSeqNumber(123),
            ack_number: None,
            window_len: 64,
            window_scale: None,
            max_seg_size: None,
            sack_permitted: false,
            sack_ranges: [None; 3],
            timestamp: None,
            payload: &[],
        };
        let ip = Ipv4Repr {
            src_addr: REMOTE,
            dst_addr: LOCAL,
            next_header: IpProtocol::Tcp,
            payload_len: tcp.buffer_len(),
            hop_limit: 64,
        };
        let checksums = ChecksumCapabilities::default();
        let len = ip.buffer_len() + tcp.buffer_len();
        inject(&mut device, now, EthernetProtocol::Ipv4, len, |bytes| {
            ip.emit(&mut Ipv4Packet::new_unchecked(&mut *bytes), &checksums);
            tcp.emit(
                &mut TcpPacket::new_unchecked(&mut bytes[ip.buffer_len()..]),
                &REMOTE.into(),
                &LOCAL.into(),
                &checksums,
            );
        });
        iface.poll_ingress_single(now, &mut device, &mut sockets);
        iface.poll_egress(now, &mut device, &mut sockets);
        take_syn(&mut device)
    };
    assert_ne!(syn(1, Instant::ZERO), syn(2, Instant::ZERO));
    assert_eq!(
        syn(1, Instant::from_millis(1)).wrapping_sub(syn(1, Instant::ZERO)),
        250
    );
}
