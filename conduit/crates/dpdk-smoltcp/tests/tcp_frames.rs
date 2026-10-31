//! TCP frames on net_ring, with the test as the TCP peer: who frees a sent
//! frame and when, the mbuf and wire bytes of each zero-copy and copy case,
//! one frame broadcast over TCP sockets, a socket outliving its Stack, sockets
//! sharing an exhausted frame pool, and that no other TX packet is taken
//! for a frame send. Every test ends with every pool idle and each object
//! returned once.

use std::ffi::CString;
use std::panic::{catch_unwind, AssertUnwindSafe};

use dpdk_smoltcp::smoltcp::phy::{ChecksumCapabilities, TxFrame};
use dpdk_smoltcp::smoltcp::socket::tcp::{self, SendFrameError, State};
use dpdk_smoltcp::smoltcp::time::Instant;
use dpdk_smoltcp::smoltcp::wire::{
    ArpOperation, EthernetAddress, EthernetFrame, EthernetProtocol, EthernetRepr, IpAddress,
    IpEndpoint, IpProtocol, Ipv4Address, Ipv4Cidr, Ipv4Packet, Ipv4Repr, TcpControl, TcpPacket,
    TcpRepr, TcpSeqNumber,
};
use dpdk_smoltcp::{
    frame_reserve, rx_reserve, ReadySide, Stack, StackConfig, TcpHandle, Turn,
    DEFAULT_NEIGHBOR_CAPACITY,
};
use zhida_dpdk_sys::{
    dpdk_raw_pktmbuf_alloc, dpdk_raw_pktmbuf_free, dpdk_raw_ring_dequeue_burst,
    dpdk_raw_ring_enqueue_burst, rte_mbuf, rte_mempool, RTE_PKTMBUF_HEADROOM,
};
use zhida_test_kit::ring_port::{arp, RingPort};
use zhida_test_kit::{pair, wire, with_eal};

const OURS: EthernetAddress = EthernetAddress([0x02, 0, 0, 0, 0, 0x01]);
const OUR_IP: Ipv4Address = Ipv4Address::new(10, 0, 0, 1);
const PEER: EthernetAddress = EthernetAddress([0x02, 0, 0, 0, 0, 0x02]);
const PEER_IP: Ipv4Address = Ipv4Address::new(10, 0, 0, 2);
const OTHER: EthernetAddress = EthernetAddress([0x02, 0, 0, 0, 0, 0x03]);
const OTHER_IP: Ipv4Address = Ipv4Address::new(10, 0, 0, 3);
const REMOTE_PORT: u16 = 80;
const LOCAL_PORT: u16 = 49152;
const PEER_ISN: TcpSeqNumber = TcpSeqNumber(1000);
const T0: Instant = Instant::from_millis_const(5_000);
const RING: u32 = 16;
const PENDING: usize = 8;
/// Ethernet, IPv4 and TCP headers of a data segment (no TCP options).
const HEADERS: usize = 54;
/// A frame's payload offset in its mbuf buffer: headroom + 42.
const FRAME_DATA_OFF: u16 = RTE_PKTMBUF_HEADROOM as u16 + 42;
/// A published TCP frame starts its 54 header bytes this far into its buffer.
const PUBLISHED_DATA_OFF: u16 = FRAME_DATA_OFF - HEADERS as u16;
/// Frames of one 4 KiB socket: the six-frame floor.
const SMALL_FRAMES: usize = 6;

fn small_socket() -> tcp::Config {
    pair::tcp_socket_sized(4096)
}

/// One Stack on a net_ring fixture, driven at `now`; the test is its peer.
struct Host {
    fixture: RingPort,
    stack: Stack,
    now: Instant,
    frame_pool: *mut rte_mempool,
    tx_pool: *mut rte_mempool,
}

/// One established connection to the peer, which sends no data.
struct Conn {
    handle: TcpHandle,
    port: u16,
    /// The sequence number of our next new byte.
    seq: TcpSeqNumber,
    /// The peer's next sequence number.
    peer_seq: TcpSeqNumber,
}

/// One packet taken off the TX ring, owned until dropped.
struct Sent(*mut rte_mbuf);

impl Drop for Sent {
    fn drop(&mut self) {
        // SAFETY: this value owns the ring's reference to the mbuf.
        unsafe { dpdk_raw_pktmbuf_free(self.0) };
    }
}

impl Sent {
    fn mbuf(&self) -> &rte_mbuf {
        // SAFETY: the ring's reference keeps the mbuf alive while self lives.
        unsafe { &*self.0 }
    }

    fn bytes(&self) -> &[u8] {
        let m = self.mbuf();
        assert_eq!(m.nb_segs(), 1);
        // SAFETY: a single-segment mbuf's data_len bytes from data_ptr.
        unsafe { std::slice::from_raw_parts(m.data_ptr(), usize::from(m.data_len())) }
    }

    fn tcp(&self) -> TcpRepr<'_> {
        segment(self.bytes())
    }

    /// A frame-pool frame published in place: headers right before the
    /// payload, lengths set, no offload, `refcnt` references.
    fn assert_published(&self, frame_pool: *mut rte_mempool, payload: &[u8], refcnt: u16) {
        let m = self.mbuf();
        assert_eq!(m.pool, frame_pool, "not a frame-pool mbuf");
        assert_eq!(m.nb_segs(), 1);
        assert_eq!(m.data_off(), PUBLISHED_DATA_OFF);
        assert_eq!(usize::from(m.data_len()), HEADERS + payload.len());
        assert_eq!(m.pkt_len(), u32::from(m.data_len()));
        assert_eq!(metadata(m), (0, 0, 0, 0));
        assert_eq!(m.refcnt(), refcnt);
        assert_eq!(self.tcp().payload, payload);
    }

    /// A reserve (TX) pool copy carrying `payload`.
    fn assert_copied(&self, tx_pool: *mut rte_mempool, payload: &[u8]) {
        let m = self.mbuf();
        assert_eq!(m.pool, tx_pool, "not a TX-pool copy");
        assert_eq!(m.nb_segs(), 1);
        assert_eq!(m.refcnt(), 1);
        assert_eq!(self.tcp().payload, payload);
    }
}

/// (ol_flags, l2, l3, l4) of an mbuf.
fn metadata(m: &rte_mbuf) -> (u64, u64, u64, u64) {
    // SAFETY: reading the tx_offload bitfield union of a live mbuf.
    let lengths = unsafe { &m.__bindgen_anon_3.__bindgen_anon_1 };
    (m.ol_flags(), lengths.l2_len(), lengths.l3_len(), lengths.l4_len())
}

/// The mbuf behind a frame-pool frame: its buffer starts FRAME_DATA_OFF bytes
/// before the payload, after the mbuf header and the pool's 8-byte private
/// area (zhida pools keep a per-object cache slot there).
fn frame_mbuf(frame: &TxFrame, frame_pool: *mut rte_mempool) -> *const rte_mbuf {
    const PRIV_SIZE: usize = 8;
    let buf = frame.payload().as_ptr().wrapping_sub(usize::from(FRAME_DATA_OFF));
    let mbuf = buf
        .wrapping_sub(std::mem::size_of::<rte_mbuf>() + PRIV_SIZE)
        .cast::<rte_mbuf>();
    // SAFETY: a frame-pool frame's mbuf header precedes its private area and
    // buffer in the same pool object; checked against the buffer address,
    // private size and pool.
    unsafe {
        assert_eq!((*mbuf).buf_addr.cast::<u8>().cast_const(), buf);
        assert_eq!(usize::from((*mbuf).priv_size), PRIV_SIZE);
        assert_eq!((*mbuf).pool, frame_pool);
    }
    mbuf
}

/// (refcnt, data_off, data_len, ol_flags) of the frame's mbuf.
fn frame_state(frame: &TxFrame, frame_pool: *mut rte_mempool) -> (u16, u16, u16, u64) {
    // SAFETY: the frame's reference keeps its mbuf alive.
    let m = unsafe { &*frame_mbuf(frame, frame_pool) };
    (m.refcnt(), m.data_off(), m.data_len(), m.ol_flags())
}

fn segment(frame: &[u8]) -> TcpRepr<'_> {
    let eth = EthernetFrame::new_checked(frame).unwrap();
    assert_eq!(
        (eth.src_addr(), eth.dst_addr(), eth.ethertype()),
        (OURS, PEER, EthernetProtocol::Ipv4)
    );
    let ip = Ipv4Packet::new_checked(eth.payload()).unwrap();
    let ip_repr = Ipv4Repr::parse(&ip, &ChecksumCapabilities::default()).unwrap();
    assert_eq!(
        (ip_repr.src_addr, ip_repr.dst_addr, ip_repr.next_header),
        (OUR_IP, PEER_IP, IpProtocol::Tcp)
    );
    let tcp = TcpPacket::new_checked(ip.payload()).unwrap();
    TcpRepr::parse(
        &tcp,
        &IpAddress::Ipv4(OUR_IP),
        &IpAddress::Ipv4(PEER_IP),
        &ChecksumCapabilities::default(),
    )
    .unwrap()
}

fn from_peer(tcp: &TcpRepr<'_>) -> Vec<u8> {
    let caps = ChecksumCapabilities::default();
    let eth = EthernetRepr {
        src_addr: PEER,
        dst_addr: OURS,
        ethertype: EthernetProtocol::Ipv4,
    };
    let ip = Ipv4Repr {
        src_addr: PEER_IP,
        dst_addr: OUR_IP,
        next_header: IpProtocol::Tcp,
        payload_len: tcp.buffer_len(),
        hop_limit: 64,
    };
    let mut bytes = vec![0; eth.buffer_len() + ip.buffer_len() + tcp.buffer_len()];
    let mut frame = EthernetFrame::new_unchecked(&mut bytes);
    eth.emit(&mut frame);
    let mut packet = Ipv4Packet::new_unchecked(frame.payload_mut());
    ip.emit(&mut packet, &caps);
    tcp.emit(
        &mut TcpPacket::new_unchecked(packet.payload_mut()),
        &IpAddress::Ipv4(PEER_IP),
        &IpAddress::Ipv4(OUR_IP),
        &caps,
    );
    bytes
}

fn peer_segment(conn: &Conn, control: TcpControl, ack: TcpSeqNumber, window: u16) -> TcpRepr<'static> {
    TcpRepr {
        src_port: REMOTE_PORT,
        dst_port: conn.port,
        control,
        seq_number: conn.peer_seq,
        ack_number: Some(ack),
        window_len: window,
        window_scale: None,
        max_seg_size: None,
        sack_permitted: false,
        sack_ranges: [None; 3],
        timestamp: None,
        payload: &[],
    }
}

/// The peer's pure ACK of our bytes up to `ack`, advertising `window`.
fn ack(conn: &Conn, ack: TcpSeqNumber, window: u16) -> Vec<u8> {
    from_peer(&peer_segment(conn, TcpControl::None, ack, window))
}

fn pool_named(name: &str, suffix: &str) -> *mut rte_mempool {
    wire::pool_by_name(&CString::new(format!("{name}_{suffix}")).unwrap())
}

impl Host {
    /// IP MTU 1500, TX ring and descriptors 16, tx_pending 8, kit TX pool,
    /// an RX pool of its reserve plus six mbufs per socket; the frame pool holds
    /// `frame_pool` frames when given, else the kit's 63.
    fn new(eal: &dpdk_smoltcp::Eal, name: &str, max_sockets: usize, frame_pool: Option<u32>) -> Self {
        let fixture = RingPort::new(name, RING, RING);
        let mut config = fixture.config();
        let reserve = rx_reserve(config.rx_descriptors, config.rx_burst, 8, config.rx_pool.cache_size);
        config.rx_pool.count = u32::try_from(reserve + 6 * max_sockets).unwrap();
        if let Some(count) = frame_pool {
            config.frame_pool.count = count;
        }
        let port = fixture.open_with(eal, config);
        let config = StackConfig {
            max_udp_sockets: 0,
            ethernet: OURS,
            ipv4: Ipv4Cidr::new(OUR_IP, 24),
            gateway: None,
            paths: &[],
            owned_ipv4: &[],
            random_seed: 1,
            neighbor_capacity: DEFAULT_NEIGHBOR_CAPACITY,
            max_sockets,
            rx_staging: 8,
            tx_pending: PENDING,
        };
        let stack = match Stack::new(port, config, T0) {
            Ok(stack) => stack,
            Err((_, error)) => panic!("{error}"),
        };
        Self {
            frame_pool: pool_named(name, "frp"),
            tx_pool: pool_named(name, "txp"),
            fixture,
            stack,
            now: T0,
        }
    }

    fn turn(&mut self) -> Turn {
        self.stack.turn(self.now, 32).unwrap()
    }

    fn frame_in_use(&self) -> u32 {
        self.fixture.frame_in_use()
    }

    fn tx_in_use(&self) -> u32 {
        self.fixture.in_use().1
    }

    /// Every packet on the TX ring, in order, owned by the result.
    fn take(&self) -> Vec<Sent> {
        let mut sent = Vec::new();
        loop {
            let mut raw = [std::ptr::null_mut::<rte_mbuf>(); 16];
            // SAFETY: dequeues up to 16 mbuf pointers into `raw`.
            let count = unsafe {
                dpdk_raw_ring_dequeue_burst(
                    self.fixture.tx_ring(),
                    raw.as_mut_ptr().cast(),
                    16,
                    std::ptr::null_mut(),
                )
            } as usize;
            if count == 0 {
                return sent;
            }
            sent.extend(raw[..count].iter().map(|&m| Sent(m)));
        }
    }

    /// Put `count` 60-byte packets from the RX pool on the TX ring, as if the
    /// PMD still held earlier sends.
    fn occupy_tx_ring(&self, count: usize) {
        let rx_pool = self.fixture.rx_pool();
        for tag in 0..count {
            let m = wire::copy_into(rx_pool, &zhida_test_kit::ring_port::frame(tag as u8));
            let objects = [m.cast::<std::ffi::c_void>()];
            // SAFETY: enqueues one owned mbuf pointer.
            let queued = unsafe {
                dpdk_raw_ring_enqueue_burst(self.fixture.tx_ring(), objects.as_ptr(), 1, std::ptr::null_mut())
            };
            assert_eq!(queued, 1, "TX ring full");
        }
    }

    fn frame(&mut self, payload: &[u8]) -> TxFrame {
        let mut frame = self.stack.tx_frame().expect("frame pool has a frame");
        frame.room_mut()[..payload.len()].copy_from_slice(payload);
        frame.set_len(payload.len());
        frame
    }

    fn send_frame(&mut self, conn: &Conn, payload: &[u8]) {
        let frame = self.frame(payload);
        if let Err((_, error)) = self.stack.tcp_send_frame(&conn.handle, frame) {
            panic!("send_frame: {error}");
        }
    }

    /// Connect from `port` with `config`; the peer answers ARP and the
    /// handshake with MSS 1460 and a 65535-byte window without scaling.
    fn connect(&mut self, port: u16, config: tcp::Config) -> Conn {
        let handle = self.stack.add_tcp(config).unwrap();
        self.stack.tcp_mut(&handle).set_ack_delay(None);
        self.stack
            .connect(&handle, IpEndpoint::new(PEER_IP.into(), REMOTE_PORT), port)
            .unwrap();
        self.turn();
        let mut sent = self.take();
        if EthernetFrame::new_checked(sent[0].bytes()).unwrap().ethertype() == EthernetProtocol::Arp {
            drop(sent);
            self.fixture.inject(&[arp(ArpOperation::Reply, OURS, (PEER, PEER_IP), (OURS, OUR_IP))]);
            self.turn();
            sent = self.take();
        }
        assert_eq!(sent.len(), 1);
        let syn = sent[0].tcp();
        assert_eq!(syn.control, TcpControl::Syn);
        let seq = syn.seq_number + 1;
        drop(sent);
        let conn = Conn { handle, port, seq, peer_seq: PEER_ISN };
        let mut syn_ack = peer_segment(&conn, TcpControl::Syn, seq, 65535);
        syn_ack.max_seg_size = Some(1460);
        self.fixture.inject(&[from_peer(&syn_ack)]);
        self.turn();
        let sent = self.take();
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].tcp().ack_number, Some(PEER_ISN + 1));
        assert_eq!(self.stack.tcp(&conn.handle).state(), State::Established);
        Conn { peer_seq: PEER_ISN + 1, ..conn }
    }

    /// Deliver the peer's ACK of `conn` up to `to` with a 65535-byte window.
    fn ack(&mut self, conn: &Conn, to: TcpSeqNumber) {
        self.fixture.inject(&[ack(conn, to, 65535)]);
        let turn = self.stack.receive(self.now, 32).unwrap();
        assert_eq!(turn.rx_processed, 1);
    }

    /// Run until the socket's retransmission timer fires and something is sent.
    fn until_retransmit(&mut self, conn: &Conn) {
        let retransmits = self.stack.tcp(&conn.handle).retransmits();
        self.now = self.stack.poll_at().expect("retransmission deadline");
        while self.turn().tx_accepted == 0 {
            self.now = self.stack.poll_at().expect("retransmission deadline");
        }
        assert_eq!(self.stack.tcp(&conn.handle).retransmits(), retransmits + 1);
    }

    /// Every pool idle and each object returned once, then close.
    fn finish(self) {
        drop(self.take());
        let port = self.stack.into_port();
        assert_eq!(self.fixture.in_use(), (0, 0));
        assert_eq!(self.fixture.frame_in_use(), 0);
        self.fixture.assert_returned_exactly_once();
        port.try_close().unwrap();
        self.fixture.close();
    }
}

/// The TX ring consumer drops its reference first, then the ACK releases the
/// socket's: the frame returns to the pool at the ACK.
#[zhida_test_support::pinned_test]
fn tx_completion_before_ack_frees_on_ack() {
    with_eal(|eal| {
        let mut host = Host::new(eal, "tcbefore", 1, None);
        let conn = host.connect(LOCAL_PORT, small_socket());
        host.send_frame(&conn, b"completion first");
        host.turn();
        let sent = host.take();
        assert_eq!(sent.len(), 1);
        sent[0].assert_published(host.frame_pool, b"completion first", 2);
        drop(sent);
        assert_eq!(host.frame_in_use(), 1);
        host.ack(&conn, conn.seq + 16);
        assert_eq!(host.frame_in_use(), 0);
        assert_eq!(host.stack.tcp(&conn.handle).send_queue(), 0);
        host.finish();
    });
}

/// The ACK releases the socket's reference while the TX ring still holds the
/// frame: it returns to the pool when the ring consumer frees it.
#[zhida_test_support::pinned_test]
fn ack_before_tx_completion_frees_on_drain() {
    with_eal(|eal| {
        let mut host = Host::new(eal, "tcafter", 1, None);
        let conn = host.connect(LOCAL_PORT, small_socket());
        host.send_frame(&conn, b"ack first");
        host.turn();
        host.ack(&conn, conn.seq + 9);
        assert_eq!(host.stack.tcp(&conn.handle).send_queue(), 0);
        assert_eq!(host.frame_in_use(), 1);
        let sent = host.take();
        assert_eq!(sent.len(), 1);
        sent[0].assert_published(host.frame_pool, b"ack first", 1);
        drop(sent);
        assert_eq!(host.frame_in_use(), 0);
        host.finish();
    });
}

/// Three frames sent while the TX ring has one free slot: the PMD takes the
/// first, the other two stay pending.
fn frames_in_ring_and_pending(eal: &dpdk_smoltcp::Eal, name: &str) -> (Host, Conn) {
    let mut host = Host::new(eal, name, 1, None);
    let conn = host.connect(LOCAL_PORT, small_socket());
    // The ring holds RING − 1; leave one slot.
    host.occupy_tx_ring(RING as usize - 2);
    for payload in [b"one", b"two", b"six"] {
        host.send_frame(&conn, payload);
    }
    let sent = host.stack.send().unwrap();
    assert_eq!((sent.tx_accepted, sent.tx_pending), (1, 2));
    assert_eq!(host.frame_in_use(), 3);
    (host, conn)
}

/// The ring's and the pending queue's references outlive the socket's;
/// returns the frames' payloads as they reach the wire.
fn drain_after_release(host: &mut Host) -> Vec<Vec<u8>> {
    let mut wire = Vec::new();
    for sent in host.take() {
        if sent.mbuf().pool == host.frame_pool {
            assert_eq!(sent.mbuf().refcnt(), 1);
            wire.push(sent.tcp().payload.to_vec());
        }
    }
    assert_eq!(host.frame_in_use(), 2);
    let flushed = host.stack.flush().unwrap();
    assert_eq!((flushed.tx_accepted, flushed.tx_pending), (2, 0));
    for sent in host.take() {
        assert_eq!(sent.mbuf().pool, host.frame_pool);
        assert_eq!(sent.mbuf().refcnt(), 1);
        wire.push(sent.tcp().payload.to_vec());
    }
    assert_eq!(host.frame_in_use(), 0);
    wire
}

/// Reusing the socket resets it and releases its queue while one frame sits
/// in the TX ring and two are pending; each reaches the wire intact and is
/// freed once by its last holder.
#[zhida_test_support::pinned_test]
fn reset_with_frames_pending_and_in_ring() {
    with_eal(|eal| {
        let (mut host, conn) = frames_in_ring_and_pending(eal, "rstpend");
        host.stack.abort(&conn.handle);
        host.stack.listen(&conn.handle, 1234).unwrap();
        assert_eq!(host.stack.tcp(&conn.handle).send_queue(), 0);
        assert_eq!(host.frame_in_use(), 3);
        assert_eq!(drain_after_release(&mut host), [b"one", b"two", b"six"]);
        host.finish();
    });
}

/// `remove_tcp` drops the socket and its queue while one frame sits in the TX
/// ring and two are pending.
#[zhida_test_support::pinned_test]
fn remove_with_frames_pending_and_in_ring() {
    with_eal(|eal| {
        let (mut host, conn) = frames_in_ring_and_pending(eal, "rmpend");
        host.stack.remove_tcp(conn.handle);
        assert_eq!(host.frame_in_use(), 3);
        assert_eq!(drain_after_release(&mut host), [b"one", b"two", b"six"]);
        host.finish();
    });
}

/// When the PMD accepts a prefix of a burst, the unaccepted frames stay
/// shared between the socket and the pending queue and leave intact later.
#[zhida_test_support::pinned_test]
fn partial_tx_acceptance_keeps_suffix_shared() {
    with_eal(|eal| {
        let (mut host, conn) = frames_in_ring_and_pending(eal, "partial");
        let first: Vec<_> = host.take().into_iter().filter(|sent| sent.mbuf().pool == host.frame_pool).collect();
        assert_eq!(first.len(), 1);
        first[0].assert_published(host.frame_pool, b"one", 2);
        drop(first);
        assert_eq!(host.stack.flush().unwrap().tx_accepted, 2);
        let rest = host.take();
        assert_eq!(rest.len(), 2);
        rest[0].assert_published(host.frame_pool, b"two", 2);
        rest[1].assert_published(host.frame_pool, b"six", 2);
        assert_eq!(
            [rest[0].tcp().seq_number, rest[1].tcp().seq_number],
            [conn.seq + 3, conn.seq + 6]
        );
        drop(rest);
        assert_eq!(host.frame_in_use(), 3);
        host.ack(&conn, conn.seq + 9);
        assert_eq!(host.frame_in_use(), 0);
        host.finish();
    });
}

/// Case (a): a unique frame that starts the unsent data and fits goes out in
/// place, with no TX-pool packet.
#[zhida_test_support::pinned_test]
fn send_unique_in_place() {
    with_eal(|eal| {
        let mut host = Host::new(eal, "inplace", 1, None);
        let conn = host.connect(LOCAL_PORT, small_socket());
        let payload = pair::pattern(1460, 3);
        let tx_before = host.tx_in_use();
        host.send_frame(&conn, &payload);
        assert_eq!(host.turn().tx_accepted, 1);
        assert_eq!(host.tx_in_use(), tx_before);
        let sent = host.take();
        assert_eq!(sent.len(), 1);
        sent[0].assert_published(host.frame_pool, &payload, 2);
        let tcp = sent[0].tcp();
        assert_eq!((tcp.seq_number, tcp.ack_number, tcp.control), (conn.seq, Some(conn.peer_seq), TcpControl::Psh));
        drop(sent);
        host.ack(&conn, conn.seq + payload.len());
        host.finish();
    });
}

/// Case (b): after the ring consumer freed the first send, the RTO
/// retransmission is exactly that frame, unique again: its headers are
/// rewritten in place and no TX-pool packet is used.
#[zhida_test_support::pinned_test]
fn retransmit_single_frame_in_place() {
    with_eal(|eal| {
        let mut host = Host::new(eal, "rtxplace", 1, None);
        let conn = host.connect(LOCAL_PORT, small_socket());
        let payload = pair::pattern(700, 5);
        host.send_frame(&conn, &payload);
        host.turn();
        let first = host.take();
        first[0].assert_published(host.frame_pool, &payload, 2);
        drop(first);
        let tx_before = host.tx_in_use();
        host.until_retransmit(&conn);
        assert_eq!(host.tx_in_use(), tx_before);
        let again = host.take();
        assert_eq!(again.len(), 1);
        again[0].assert_published(host.frame_pool, &payload, 2);
        assert_eq!(again[0].tcp().seq_number, conn.seq);
        drop(again);
        host.ack(&conn, conn.seq + payload.len());
        host.finish();
    });
}

/// Case (c): two frames sent in separate egresses stay on the paused TX ring;
/// the RTO retransmission is one TX-pool copy spanning both, as the byte
/// ring would send, and the first sends keep their bytes and metadata.
#[zhida_test_support::pinned_test]
fn retransmit_copies_coalesced() {
    with_eal(|eal| {
        let mut host = Host::new(eal, "rtxcopy", 1, None);
        let conn = host.connect(LOCAL_PORT, small_socket());
        let (one, two) = (pair::pattern(64, 1), pair::pattern(64, 2));
        host.send_frame(&conn, &one);
        host.turn();
        host.send_frame(&conn, &two);
        host.turn();
        let first = host.take();
        assert_eq!(first.len(), 2);
        let tx_before = host.tx_in_use();
        host.until_retransmit(&conn);
        assert_eq!(host.tx_in_use(), tx_before + 1);
        let copy = host.take();
        assert_eq!(copy.len(), 1);
        let both = [one.as_slice(), two.as_slice()].concat();
        copy[0].assert_copied(host.tx_pool, &both);
        assert_eq!(copy[0].tcp().seq_number, conn.seq);
        first[0].assert_published(host.frame_pool, &one, 2);
        first[1].assert_published(host.frame_pool, &two, 2);
        drop((first, copy));
        host.ack(&conn, conn.seq + 128);
        host.finish();
    });
}

/// Case (c): a frame the application still shares is copied into a TX-pool
/// packet; the frame itself is never published.
#[zhida_test_support::pinned_test]
fn shared_frame_send_copies() {
    with_eal(|eal| {
        let mut host = Host::new(eal, "sharecp", 1, None);
        let conn = host.connect(LOCAL_PORT, small_socket());
        let frame = host.frame(b"shared payload");
        let before = frame_state(&frame, host.frame_pool);
        assert_eq!(before, (1, FRAME_DATA_OFF, 0, 0));
        if let Err((_, error)) = host.stack.tcp_send_frame(&conn.handle, frame.clone()) {
            panic!("{error}");
        }
        assert_eq!(host.turn().tx_accepted, 1);
        let sent = host.take();
        assert_eq!(sent.len(), 1);
        sent[0].assert_copied(host.tx_pool, b"shared payload");
        assert_eq!(frame_state(&frame, host.frame_pool), (2, FRAME_DATA_OFF, 0, 0));
        drop(sent);
        host.ack(&conn, conn.seq + 14);
        assert_eq!(frame_state(&frame, host.frame_pool), (1, FRAME_DATA_OFF, 0, 0));
        assert_eq!(frame.payload(), b"shared payload");
        drop(frame);
        host.finish();
    });
}

/// Case (c): a 1448-byte frame against a 724-byte window leaves as two
/// TX-pool copies of 724 bytes; the frame is never published.
#[zhida_test_support::pinned_test]
fn cut_copies_into_reserve_frame() {
    with_eal(|eal| {
        let mut host = Host::new(eal, "cutcopy", 1, None);
        let conn = host.connect(LOCAL_PORT, small_socket());
        host.fixture.inject(&[ack(&conn, conn.seq, 724)]);
        host.turn();
        let payload = pair::pattern(1448, 9);
        host.send_frame(&conn, &payload);
        assert_eq!(host.turn().tx_accepted, 1);
        let first = host.take();
        assert_eq!(first.len(), 1);
        first[0].assert_copied(host.tx_pool, &payload[..724]);
        drop(first);
        host.fixture.inject(&[ack(&conn, conn.seq + 724, 724)]);
        assert_eq!(host.turn().tx_accepted, 1);
        let second = host.take();
        assert_eq!(second.len(), 1);
        second[0].assert_copied(host.tx_pool, &payload[724..]);
        assert_eq!(second[0].tcp().seq_number, conn.seq + 724);
        drop(second);
        host.ack(&conn, conn.seq + 1448);
        host.finish();
    });
}

/// One frame cloned to 8 TCP sockets: every packet is a TX-pool copy with
/// its own socket's headers and the same payload; the frame is never
/// published; each socket releases its clone at its ACK.
#[zhida_test_support::pinned_test]
fn broadcast_frame_over_tcp() {
    with_eal(|eal| {
        const N: usize = 8;
        let pool = frame_reserve(RING as u16, PENDING) + N * SMALL_FRAMES + 1;
        let mut host = Host::new(eal, "bcasttcp", N, Some(pool as u32));
        let conns: Vec<_> = (0..N).map(|i| host.connect(LOCAL_PORT + i as u16, small_socket())).collect();
        let payload = pair::pattern(300, 4);
        let frame = host.frame(&payload);
        let tx_before = host.tx_in_use();
        for conn in &conns {
            if let Err((_, error)) = host.stack.tcp_send_frame(&conn.handle, frame.clone()) {
                panic!("{error}");
            }
        }
        assert_eq!(frame_state(&frame, host.frame_pool), (N as u16 + 1, FRAME_DATA_OFF, 0, 0));
        assert_eq!(host.turn().tx_accepted, N);
        assert_eq!(host.tx_in_use(), tx_before + N as u32);
        let sent = host.take();
        assert_eq!(sent.len(), N);
        for (sent, conn) in sent.iter().zip(&conns) {
            sent.assert_copied(host.tx_pool, &payload);
            let tcp = sent.tcp();
            assert_eq!((tcp.dst_port, tcp.src_port, tcp.seq_number), (REMOTE_PORT, conn.port, conn.seq));
        }
        drop(sent);
        assert_eq!(host.tx_in_use(), tx_before);
        assert_eq!(frame_state(&frame, host.frame_pool), (N as u16 + 1, FRAME_DATA_OFF, 0, 0));
        for (released, conn) in conns.iter().enumerate() {
            host.ack(conn, conn.seq + payload.len());
            let refs = (N - released) as u16;
            assert_eq!(frame_state(&frame, host.frame_pool), (refs, FRAME_DATA_OFF, 0, 0));
        }
        assert_eq!(host.frame_in_use(), 1);
        drop(frame);
        host.finish();
    });
}

/// A socket swapped into another Stack keeps its own Stack's frame pool alive
/// after that Stack and Port are gone, allocates from it, and cannot publish
/// through the other Stack's device.
#[zhida_test_support::pinned_test]
fn swapped_socket_outlives_source_stack() {
    with_eal(|eal| {
        let mut a = Host::new(eal, "swapa", 1, None);
        let mut b = Host::new(eal, "swapb", 1, None);
        let conn_a = a.connect(LOCAL_PORT, small_socket());
        let conn_b = b.connect(LOCAL_PORT, small_socket());
        std::mem::swap(a.stack.tcp_mut(&conn_a.handle), b.stack.tcp_mut(&conn_b.handle));

        let Host { fixture: fixture_a, stack: stack_a, .. } = a;
        let port_a = stack_a.into_port();
        assert_eq!(fixture_a.in_use(), (0, 0));
        port_a.try_close().unwrap();
        let a_frames = CString::new("swapa_frp").unwrap();
        assert!(wire::pool_exists(&a_frames), "the swapped socket keeps A's frame pool");

        assert_eq!(b.stack.tcp_mut(&conn_b.handle).send_slice(b"moved"), Ok(5));
        assert_eq!(wire::in_use(&a_frames), 1);
        assert_eq!(b.frame_in_use(), 0);
        let panic = catch_unwind(AssertUnwindSafe(|| b.stack.send())).unwrap_err();
        let message = panic.downcast_ref::<String>().expect("panic message");
        assert!(message.contains("TCP socket moved between Stacks"), "{message}");
        assert_eq!(wire::in_use(&a_frames), 0);

        let Host { fixture: fixture_b, stack: stack_b, .. } = b;
        let port_b = stack_b.into_port();
        assert!(!wire::pool_exists(&a_frames), "A's frame pool dies with the last socket");
        assert_eq!(fixture_b.in_use(), (0, 0));
        assert_eq!(fixture_b.frame_in_use(), 0);
        drop(fixture_b.take_tx());
        port_b.try_close().unwrap();
        fixture_b.close();
        fixture_a.close();
    });
}

/// A frame from another Stack's pool is refused with `Foreign` and returned
/// untouched.
#[zhida_test_support::pinned_test]
fn tcp_frame_from_other_stack_is_foreign() {
    with_eal(|eal| {
        let mut a = Host::new(eal, "forna", 1, None);
        let mut b = Host::new(eal, "fornb", 1, None);
        let conn = b.connect(LOCAL_PORT, small_socket());
        let frame = a.frame(b"from A");
        let (frame, error) = b.stack.tcp_send_frame(&conn.handle, frame).unwrap_err();
        assert_eq!(error, SendFrameError::Foreign);
        assert_eq!(frame.payload(), b"from A");
        assert_eq!((a.frame_in_use(), b.frame_in_use()), (1, 0));
        assert_eq!(b.stack.tcp(&conn.handle).send_queue(), 0);
        drop(frame);
        a.finish();
        b.finish();
    });
}

/// Sockets share the frame pool: three are added to a pool of `frame_reserve` plus one
/// socket's frame limit. With the application holding all but three frames, one socket's
/// 4096-byte send takes them; the second socket's send then queues nothing (`Ok(0)`) and
/// `tx_frame` has none, until the first socket's data is ACKed and its frames return.
#[zhida_test_support::pinned_test]
fn frame_pool_exhaustion_refuses_sends_until_acked_frames_return() {
    with_eal(|eal| {
        let pool = frame_reserve(RING as u16, PENDING) + SMALL_FRAMES;
        let mut host = Host::new(eal, "framshr", 3, Some(pool as u32));
        let first = host.connect(LOCAL_PORT, small_socket());
        let second = host.connect(LOCAL_PORT + 1, small_socket());
        let third = host.stack.add_tcp(small_socket()).unwrap();
        assert_eq!(host.stack.tcp(&third).tx_frame_cap(), SMALL_FRAMES);
        let mut held = Vec::new();
        while let Some(frame) = host.stack.tx_frame() {
            held.push(frame);
        }
        assert_eq!(held.len(), pool);
        held.truncate(pool - 3);

        let fill = pair::pattern(4096, 9);
        assert_eq!(host.stack.tcp_mut(&first.handle).send_slice(&fill), Ok(4096));
        assert_eq!(host.stack.tcp_mut(&second.handle).send_slice(b"waits"), Ok(0));
        assert_eq!(host.stack.tcp(&second.handle).send_queue(), 0);
        assert!(host.stack.tx_frame().is_none());
        host.turn();
        let sent = host.take();
        let bytes: Vec<u8> = sent.iter().flat_map(|sent| sent.tcp().payload.to_vec()).collect();
        assert_eq!(bytes, fill);
        drop(sent);
        // The TX ring released its references; the unacknowledged data still holds the frames.
        assert_eq!(host.stack.tcp_mut(&second.handle).send_slice(b"waits"), Ok(0));

        host.ack(&first, first.seq + 4096);
        assert_eq!(host.stack.tcp_mut(&second.handle).send_slice(b"waits"), Ok(5));
        host.turn();
        let sent = host.take();
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].tcp().payload, b"waits");
        drop(sent);
        host.ack(&second, second.seq + 5);
        drop(held);
        drop(third);
        host.finish();
    });
}

/// With the application holding the rest, sockets A and C take the shared frame pool's last
/// frames. B's `send_slice` returns `Ok(0)` and
/// B's application arms B's TX token; D is in the same wait and is removed. When the peer ACKs
/// A's data, the next turn finds free frames and fires B's token, with no retry from B; B's send
/// then goes out. Removing D took it off the wait.
#[zhida_test_support::pinned_test]
fn frame_pool_wait_fires_the_tx_token_when_frames_return() {
    with_eal(|eal| {
        let pool = frame_reserve(RING as u16, PENDING);
        let mut host = Host::new(eal, "framwake", 4, Some(pool as u32));
        let a = host.connect(LOCAL_PORT, small_socket());
        let c = host.connect(LOCAL_PORT + 1, small_socket());
        let b = host.connect(LOCAL_PORT + 2, small_socket());
        let d = host.connect(LOCAL_PORT + 3, small_socket());
        // The app holds all but 2 * 3 frames; A's and C's 4096-byte sends take those.
        let mut held = Vec::new();
        while let Some(frame) = host.stack.tx_frame() {
            held.push(frame);
        }
        held.truncate(pool - 6);
        let fill = pair::pattern(4096, 13);
        assert_eq!(host.stack.tcp_mut(&a.handle).send_slice(&fill), Ok(4096));
        assert_eq!(host.stack.tcp_mut(&c.handle).send_slice(&fill), Ok(4096));
        assert!(host.stack.tx_frame().is_none());
        assert_eq!(host.stack.tcp_mut(&b.handle).send_slice(b"b waits"), Ok(0));
        assert_eq!(host.stack.tcp_mut(&d.handle).send_slice(b"d waits"), Ok(0));
        let key = host.stack.tcp_ready_key(&b.handle);
        host.stack.arm_tcp(key, ReadySide::Tx, 7);
        host.stack.remove_tcp(d.handle);
        host.turn();
        drop(host.take());
        assert_eq!(host.stack.pop_fired(), None, "A's and C's frames are not acknowledged yet");

        host.ack(&a, a.seq + 4096);
        // The ACK freed A's frames; the next turn finds one and fires B's token.
        host.turn();
        assert_eq!(host.stack.pop_fired(), Some(7));
        assert_eq!(host.stack.pop_fired(), None);
        assert_eq!(host.stack.tcp_mut(&b.handle).send_slice(b"b waits"), Ok(7));
        host.turn();
        let sent = host.take();
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].tcp().payload, b"b waits");
        drop(sent);
        host.ack(&b, b.seq + 7);
        host.ack(&c, c.seq + 4096);
        drop(held);
        host.finish();
    });
}

/// The application holds every frame but the reserve and one socket's frame limit:
/// the peer's data is still ACKed, an ARP request is still answered, and the socket
/// still fills all its frame slots.
#[zhida_test_support::pinned_test]
fn app_held_saturation_leaves_ack_and_arp_working() {
    with_eal(|eal| {
        let mut host = Host::new(eal, "appfull", 1, None);
        let conn = host.connect(LOCAL_PORT, small_socket());
        let free = 63 - frame_reserve(RING as u16, PENDING) - SMALL_FRAMES;
        let held: Vec<_> = (0..free).map(|_| host.stack.tx_frame().unwrap()).collect();

        let mut data = peer_segment(&conn, TcpControl::Psh, conn.seq, 65535);
        data.payload = b"peer data";
        host.fixture.inject(&[from_peer(&data)]);
        host.fixture.inject(&[arp(
            ArpOperation::Request,
            EthernetAddress::BROADCAST,
            (OTHER, OTHER_IP),
            (EthernetAddress::BROADCAST, OUR_IP),
        )]);
        host.turn();
        let replies = host.take();
        assert_eq!(replies.len(), 2);
        let (arp_replies, acks): (Vec<_>, Vec<_>) = replies.iter().partition(|sent| {
            EthernetFrame::new_checked(sent.bytes()).unwrap().ethertype() == EthernetProtocol::Arp
        });
        assert_eq!(acks[0].tcp().ack_number, Some(conn.peer_seq + 9));
        let eth = EthernetFrame::new_checked(arp_replies[0].bytes()).unwrap();
        assert_eq!(eth.dst_addr(), OTHER);
        drop(replies);

        let fill = pair::pattern(4096, 6);
        assert_eq!(host.stack.tcp_mut(&conn.handle).send_slice(&fill), Ok(4096));
        assert_eq!(host.stack.tcp(&conn.handle).send_queue(), 4096);
        host.turn();
        let sent = host.take();
        let bytes: Vec<u8> = sent.iter().flat_map(|sent| sent.tcp().payload.to_vec()).collect();
        assert_eq!(bytes, fill);
        drop(sent);
        host.ack(&conn, conn.seq + 4096);
        drop(held);
        host.finish();
    });
}

/// A whole-frame send needs only a pending slot: with the TX pool empty and
/// no spare TX packet, it still goes out, on a template hit and on a template
/// miss with the neighbor known (a new neighbor changes the cache).
#[zhida_test_support::pinned_test]
fn frame_send_takes_no_tx_packet() {
    with_eal(|eal| {
        let mut host = Host::new(eal, "notoken", 1, None);
        let conn = host.connect(LOCAL_PORT, small_socket());
        let mut seq = conn.seq;
        for miss in [false, true] {
            if miss {
                host.fixture.inject(&[arp(
                    ArpOperation::Request,
                    EthernetAddress::BROADCAST,
                    (OTHER, OTHER_IP),
                    (EthernetAddress::BROADCAST, OUR_IP),
                )]);
                host.turn();
                drop(host.take());
            }
            // Nothing on the ring and no spare: the TX pool is idle.
            assert_eq!(host.tx_in_use(), 0);
            let mut exhausted = Vec::new();
            loop {
                // SAFETY: allocates one mbuf from the live TX pool.
                let m = unsafe { dpdk_raw_pktmbuf_alloc(host.tx_pool) };
                if m.is_null() {
                    break;
                }
                exhausted.push(Sent(m));
            }
            host.send_frame(&conn, b"no token");
            let sent = host.stack.send().unwrap();
            assert_eq!((sent.tx_accepted, sent.tx_pending), (1, 0), "miss {miss}");
            let wire = host.take();
            wire[0].assert_published(host.frame_pool, b"no token", 2);
            assert_eq!(wire[0].tcp().seq_number, seq);
            drop((wire, exhausted));
            seq += 8;
            host.ack(&conn, seq);
            drop(host.take());
        }
        host.finish();
    });
}

/// net_ring keeps sent frames on its TX ring, and the kit sets the TX
/// descriptors to the ring size: with the ring full of frames the Stack
/// published and the socket has since released, the frame pool still holds
/// the socket's frame limit and C + 2 more, and published frames use no TX
/// packet.
#[zhida_test_support::pinned_test]
fn kit_ring_descriptors_match_ring_size_with_published_frames() {
    with_eal(|eal| {
        const RING: u32 = 32;
        let fixture = RingPort::new("kitframes", 16, RING);
        let mut port_config = fixture.config();
        assert_eq!(port_config.tx_descriptors, RING as u16);
        let held = RING as usize - 1;
        let socket = tcp::Config { rx_capacity: 4096, tx_capacity: held * 1460, mss_cap: None };
        let socket_frames = tcp::tx_mbuf_budget(&socket, 1500);
        assert_eq!(socket_frames, held);
        let pool = frame_reserve(RING as u16, PENDING) + socket_frames;
        port_config.frame_pool.count = pool as u32;
        port_config.tx_pool.count = 63;
        let port = fixture.open_with(eal, port_config);
        let config = StackConfig {
            max_udp_sockets: 0,
            ethernet: OURS,
            ipv4: Ipv4Cidr::new(OUR_IP, 24),
            gateway: None,
            paths: &[],
            owned_ipv4: &[],
            random_seed: 1,
            neighbor_capacity: DEFAULT_NEIGHBOR_CAPACITY,
            max_sockets: 1,
            rx_staging: 8,
            tx_pending: PENDING,
        };
        let stack = match Stack::new(port, config, T0) {
            Ok(stack) => stack,
            Err((_, error)) => panic!("{error}"),
        };
        let mut host = Host {
            frame_pool: pool_named("kitframes", "frp"),
            tx_pool: pool_named("kitframes", "txp"),
            fixture,
            stack,
            now: T0,
        };
        let conn = host.connect(LOCAL_PORT, socket);
        for i in 0..held {
            host.send_frame(&conn, &[i as u8; 100]);
            assert_eq!(host.turn().tx_accepted, 1);
        }
        assert_eq!(host.tx_in_use(), 0);
        host.ack(&conn, conn.seq + held * 100);
        assert_eq!(host.frame_in_use(), held as u32);
        let all: Vec<_> = std::iter::from_fn(|| host.stack.tx_frame()).collect();
        assert_eq!(all.len(), pool - held);
        assert_eq!(all.len(), socket_frames + PENDING + 2);
        drop(all);
        let sent = host.take();
        assert_eq!(sent.len(), held);
        for (i, sent) in sent.iter().enumerate() {
            sent.assert_published(host.frame_pool, &[i as u8; 100], 1);
        }
        drop(sent);
        host.finish();
    });
}
