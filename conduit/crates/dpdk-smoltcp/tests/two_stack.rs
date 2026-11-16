use switch_test_kit::pair::drain_bytes;

use dpdk_smoltcp::TcpHandle;
use dpdk_smoltcp::smoltcp::socket::tcp::{self, RecvError, State};
use dpdk_smoltcp::smoltcp::time::Duration;
use dpdk_smoltcp::smoltcp::wire::{
    ArpOperation, EthernetAddress, EthernetFrame, EthernetProtocol, IpEndpoint, TcpControl,
    TcpRepr, TcpSeqNumber,
};

use switch_test_kit::{pair, ring_port, with_eal};
use pair::{
    establish, establish_sized, new_pair, pattern, segment, App, Dir, Pair, Side, CLIENT_PORT, MAX_ROUNDS,
    SERVER_PORT,
};
use ring_port::arp;

const ECHO_LEN: usize = 64 * 1024;
/// The measured RTO: RTT samples on a Pair at one instant are 0 µs, so the RTO is
/// smoltcp's 50 ms floor.
const RTO: Duration = Duration::from_millis(50);

fn exhaust_rx_pool(name: &std::ffi::CStr, count: u32) {
    use switch_test_kit::wire::{in_use, pool_by_name};
    use switch_dpdk_sys::{dpdk_raw_pktmbuf_alloc, dpdk_raw_pktmbuf_free};

    assert_eq!(in_use(name), 0);
    let pool = pool_by_name(name);
    for _ in 0..2 {
        let mut packets = Vec::new();
        for _ in 0..count {
            let packet = unsafe { dpdk_raw_pktmbuf_alloc(pool) };
            assert!(!packet.is_null());
            assert!(
                !packets.contains(&packet),
                "RX pool returned one mbuf twice"
            );
            packets.push(packet);
        }
        assert!(unsafe { dpdk_raw_pktmbuf_alloc(pool) }.is_null());
        assert_eq!(in_use(name), count);
        for packet in packets {
            unsafe { dpdk_raw_pktmbuf_free(packet) };
        }
        assert_eq!(in_use(name), 0);
    }
}

#[switch_test_support::pinned_test]
fn single_hole_fill_with_all_ooo_slots_progresses() {
    with_eal(|eal| {
        const SEGMENT: usize = 1448;
        const CAPACITY: usize = 6 * SEGMENT;
        const SEGMENTS: usize = CAPACITY / SEGMENT;
        let configs = [pair::config(Side::A), pair::config(Side::B)];
        let count = pair::pool_count(
            dpdk_smoltcp::rx_reserve(16, 8, configs[0].rx_staging, 8),
            pair::windows(1500, [CAPACITY]),
        );
        let mut pair = Pair::with_ports(
            eal,
            ["holefilla", "holefillb"],
            configs,
            pair::T0,
            16,
            |_, port| {
                port.rx_pool.count = count;
            },
        );
        let (handles, _) = establish_sized(&mut pair, Side::A, CAPACITY);
        pair.b.tcp_mut(&handles[1]).set_ack_delay(None);
        let warmup = pattern(CAPACITY, 91);
        assert_eq!(pair.a.tcp_mut(&handles[0]).send_slice(&warmup), Ok(CAPACITY));
        let mut warmed = Vec::new();
        pair.drive(
            |_, _| {},
            |a, b| {
                let bytes = drain_bytes(b.tcp_mut(&handles[1])).unwrap();
                let read = bytes.len();
                warmed.extend_from_slice(&bytes[..read]);
                if warmed.len() == CAPACITY && a.tcp(&handles[0]).send_queue() == 0 {
                    App::Done
                } else if read > 0 {
                    App::Moved
                } else {
                    App::Idle
                }
            },
        );
        assert_eq!(warmed, warmup);
        // The whole warmup leaves in one flight, so B's last ACKs can still be
        // on A's ring when the send queue empties: let them land.
        while pair.round(|_, _| {}).moved() {}
        assert_eq!(pair.in_use().map(|(rx, _)| rx), [0, 0]);
        let mut dropped = false;
        pair.a_to_b.impair(move |frame| {
            if !dropped && !segment(frame, Side::A).payload.is_empty() {
                dropped = true;
                switch_test_kit::wire::Verdict::Drop
            } else {
                switch_test_kit::wire::Verdict::Deliver
            }
        });
        pair.b_to_a.paused = true;
        let data = pattern(CAPACITY, 92);
        assert_eq!(data.len() / SEGMENT, SEGMENTS);
        for (index, bytes) in data.chunks(SEGMENT).enumerate() {
            assert_eq!(pair.a.tcp_mut(&handles[0]).send_slice(bytes), Ok(SEGMENT));
            let round = pair.round(|_, _| {});
            assert_eq!(round.a_to_b, 1);
            assert_eq!(pair.b.tcp(&handles[1]).recv_queue(), 0);
            assert_eq!(pair.in_use()[1].0, index as u32);
            assert_eq!(pair.b.tcp(&handles[1]).rx_refused_segments(), 0);
        }
        assert_eq!(
            pair.in_use()[1].0 as usize,
            SEGMENTS - 1
        );
        assert_eq!(pair.a_to_b.dropped, 1);
        pair.b_to_a.paused = false;
        pair.drive(
            |_, _| {},
            |a, b| {
                assert_eq!(b.tcp(&handles[1]).rx_refused_segments(), 0);
                if b.tcp(&handles[1]).recv_queue() == data.len()
                    && a.tcp(&handles[0]).send_queue() == 0
                {
                    App::Done
                } else {
                    App::Idle
                }
            },
        );
        assert_eq!(pair.in_use()[1].0 as usize, SEGMENTS);
        let received = drain_bytes(pair.b.tcp_mut(&handles[1])).unwrap();
        assert_eq!(received.len(), data.len());
        assert_eq!(received, data);
        pair.round(|_, _| {});
        pair.round(|_, _| {});
        assert_eq!(pair.in_use().map(|(rx, _)| rx), [0, 0]);
        assert_no_drops(&pair);
        exhaust_rx_pool(c"holefilla_rxp", count);
        exhaust_rx_pool(c"holefillb_rxp", count);
        assert_eq!(pair.close(), 0);
    });
}

/// Exactly: client's ARP request, server's ARP reply, SYN, SYN-ACK, ACK.
fn assert_arp_and_handshake(frames: &[(Dir, Vec<u8>)], client: Side) {
    let server = client.other();
    let dirs: Vec<Dir> = frames.iter().map(|(dir, _)| *dir).collect();
    assert_eq!(
        dirs,
        [client.dir(), server.dir(), client.dir(), server.dir(), client.dir()]
    );
    assert_eq!(
        frames[0].1,
        arp(
            ArpOperation::Request,
            EthernetAddress::BROADCAST,
            (client.mac(), client.ip()),
            (EthernetAddress::BROADCAST, server.ip()),
        )
    );
    assert_eq!(
        frames[1].1,
        arp(
            ArpOperation::Reply,
            client.mac(),
            (server.mac(), server.ip()),
            (client.mac(), client.ip()),
        )
    );

    let syn = segment(&frames[2].1, client);
    assert_eq!(
        (syn.src_port, syn.dst_port, syn.control, syn.ack_number, syn.payload.len()),
        (CLIENT_PORT, SERVER_PORT, TcpControl::Syn, None, 0)
    );
    let syn_ack = segment(&frames[3].1, server);
    assert_eq!(
        (syn_ack.src_port, syn_ack.dst_port, syn_ack.control, syn_ack.payload.len()),
        (SERVER_PORT, CLIENT_PORT, TcpControl::Syn, 0)
    );
    assert_eq!(syn_ack.ack_number, Some(syn.seq_number + 1));
    let ack = segment(&frames[4].1, client);
    assert_eq!(
        (ack.src_port, ack.dst_port, ack.control, ack.payload.len()),
        (CLIENT_PORT, SERVER_PORT, TcpControl::None, 0)
    );
    assert_eq!(ack.seq_number, syn.seq_number + 1);
    assert_eq!(ack.ack_number, Some(syn_ack.seq_number + 1));
}

fn assert_no_drops(pair: &Pair) {
    for link in [&pair.a_to_b, &pair.b_to_a] {
        assert_eq!((link.pool_empty, link.ring_full), (0, 0));
    }
}

#[switch_test_support::pinned_test]
fn arp_and_handshake_a_connects_b_listens() {
    with_eal(|eal| {
        let mut pair = new_pair(eal, ["hs1a", "hs1b"]);
        let (_, frames) = establish(&mut pair, Side::A);
        assert_arp_and_handshake(&frames, Side::A);
        assert_no_drops(&pair);
        assert_eq!(pair.close(), 0);
    });
}

#[switch_test_support::pinned_test]
fn two_connections_interleave_one_segment_per_visit() {
    with_eal(|eal| {
        {
            let mut a_config = pair::config(Side::A);
            a_config.max_sockets = 2;
            let mut b_config = pair::config(Side::B);
            b_config.max_sockets = 2;
            let mut pair = Pair::new(eal, ["bursta", "burstb"], [a_config, b_config], pair::T0);
            let handles = [0, 1].map(|index| {
                let server = pair.b.add_tcp(pair::tcp_socket_sized(8192)).unwrap();
                pair.b.tcp_mut(&server).set_ack_delay(None);
                pair.b.listen(&server, SERVER_PORT + index).unwrap();
                let client = pair.a.add_tcp(pair::tcp_socket_sized(8192)).unwrap();
                pair.a.tcp_mut(&client).set_ack_delay(None);
                pair.a
                    .connect(
                        &client,
                        IpEndpoint::new(Side::B.ip().into(), SERVER_PORT + index),
                        CLIENT_PORT + index,
                    )
                    .unwrap();
                [client, server]
            });
            let mut advertised = [None; 2];
            pair.drive(
                |dir, frame| {
                    if dir == Dir::BToA {
                        let ethernet = EthernetFrame::new_checked(frame).unwrap();
                        if ethernet.ethertype() != EthernetProtocol::Ipv4 {
                            return;
                        }
                        let tcp = segment(frame, Side::B);
                        if tcp.control == TcpControl::Syn {
                            assert_eq!(tcp.timestamp, None);
                            advertised[usize::from(tcp.dst_port - CLIENT_PORT)] =
                                Some((usize::from(tcp.max_seg_size.unwrap()), tcp.window_len));
                        }
                    }
                },
                |a, b| {
                    if handles.iter().all(|[client, server]| {
                        a.tcp(client).state() == State::Established
                            && b.tcp(server).state() == State::Established
                    }) {
                        App::Done
                    } else {
                        App::Idle
                    }
                },
            );
            // Two segments per connection: one full segment and a 300-byte
            // tail fit the peer window and Cubic initial window without an
            // ACK; the tail leaves immediately behind the first one.
            let mut sent = [Vec::new(), Vec::new()];
            for (index, [client, _]) in handles.iter().enumerate() {
                let (mss, window) = advertised[index].unwrap();
                let len = mss + 300;
                assert!(usize::from(window) >= len && len <= 2048);
                let data = vec![index as u8 + 1; len];
                assert_eq!(pair.a.tcp_mut(client).send_slice(&data), Ok(data.len()));
            }
            let now = pair.now();
            assert_eq!(pair.a.turn(now, pair::BUDGET).unwrap().tx_accepted, 4);
            let mut ports = Vec::new();
            assert_eq!(
                pair.a_to_b.forward_with(|frame| {
                    let tcp = segment(frame, Side::A);
                    let index = usize::from(tcp.src_port - CLIENT_PORT);
                    sent[index].extend_from_slice(tcp.payload);
                    ports.push(tcp.src_port);
                }),
                4
            );
            for (index, data) in sent.iter().enumerate() {
                let (mss, _) = advertised[index].unwrap();
                assert_eq!(*data, vec![index as u8 + 1; mss + 300]);
            }
            let c1 = CLIENT_PORT;
            let c2 = CLIENT_PORT + 1;
            assert_eq!(ports, [c1, c2, c1, c2]);
            assert_no_drops(&pair);
            assert_eq!(pair.close(), 4);
        }
    });
}

fn establish_small_segment_clients(pair: &mut Pair, count: u16) -> Vec<TcpHandle> {
    let handles: Vec<_> = (0..count)
        .map(|index| {
            let handle = pair.a.add_tcp(pair::tcp_socket_sized(8192)).unwrap();
            pair.a.tcp_mut(&handle).set_ack_delay(None);
            pair.a
                .connect(
                    &handle,
                    IpEndpoint::new(Side::B.ip().into(), SERVER_PORT + index),
                    CLIENT_PORT + index,
                )
                .unwrap();
            handle
        })
        .collect();
    let now = pair.now();
    assert_eq!(pair.a.turn(now, pair::BUDGET).unwrap().tx_accepted, 1);
    assert_eq!(
        pair.drain_tx(Side::A, |frame| {
            assert_eq!(
                EthernetFrame::new_checked(frame).unwrap().ethertype(),
                EthernetProtocol::Arp
            );
        }),
        1
    );
    pair.inject(
        Side::A,
        &[arp(
            ArpOperation::Reply,
            Side::A.mac(),
            (Side::B.mac(), Side::B.ip()),
            (Side::A.mac(), Side::A.ip()),
        )],
    );
    assert_eq!(
        pair.a.turn(now, pair::BUDGET).unwrap().tx_accepted,
        usize::from(count)
    );
    let mut replies = Vec::new();
    assert_eq!(
        pair.drain_tx(Side::A, |frame| {
            let syn = segment(frame, Side::A);
            assert_eq!(syn.control, TcpControl::Syn);
            let syn_ack = TcpRepr {
                src_port: syn.dst_port,
                dst_port: syn.src_port,
                seq_number: TcpSeqNumber(100),
                ack_number: Some(syn.seq_number + 1),
                window_len: 8192,
                window_scale: None,
                max_seg_size: Some(512),
                sack_permitted: false,
                timestamp: None,
                ..syn
            };
            replies.push(pair::tcp_frame(Side::B, &syn_ack));
        }),
        usize::from(count)
    );
    pair.inject(Side::A, &replies);
    assert_eq!(
        pair.a.turn(now, pair::BUDGET).unwrap().tx_accepted,
        usize::from(count)
    );
    assert_eq!(
        pair.drain_tx(Side::A, |frame| {
            let ack = segment(frame, Side::A);
            assert_eq!(ack.control, TcpControl::None);
            assert!(ack.payload.is_empty());
            assert_eq!(ack.ack_number, Some(TcpSeqNumber(101)));
        }),
        usize::from(count)
    );
    for handle in &handles {
        assert_eq!(pair.a.tcp(handle).state(), State::Established);
    }
    handles
}

#[switch_test_support::pinned_test]
fn lone_burst_keeps_poll_at_and_retransmits_at_deadline() {
    with_eal(|eal| {
        let mut pair = new_pair(eal, ["idlertoa", "idlertob"]);
        let handles = establish_small_segment_clients(&mut pair, 1);
        let handle = &handles[0];
        assert_eq!(
            pair.a.tcp_mut(handle).send_slice(&[1; 3 * 512]),
            Ok(3 * 512)
        );
        let now = pair.now();
        let turn = pair.a.turn(now, pair::BUDGET).unwrap();
        assert_eq!((turn.tx_accepted, turn.tx_pending), (3, 0));
        let mut first_seq = None;
        assert_eq!(
            pair.drain_tx(Side::A, |frame| {
                let tcp = segment(frame, Side::A);
                first_seq.get_or_insert(tcp.seq_number);
                assert_eq!(tcp.payload, &[1; 512]);
            }),
            3
        );
        let deadline = now + RTO;
        let mut previous = now;
        for _ in 0..MAX_ROUNDS {
            let at = pair
                .a
                .poll_at()
                .expect("burst must arm the retransmit timer");
            assert!(at > previous && at <= deadline);
            if at == deadline {
                break;
            }
            let turn = pair.a.turn(at, pair::BUDGET).unwrap();
            assert_eq!((turn.tx_accepted, turn.tx_pending), (0, 0));
            previous = at;
        }
        assert_eq!(pair.a.poll_at(), Some(deadline));
        let turn = pair.a.turn(deadline, pair::BUDGET).unwrap();
        assert!(turn.tx_accepted > 0);
        assert_eq!(turn.tx_pending, 0);
        let mut retransmits = 0;
        assert_eq!(
            pair.drain_tx(Side::A, |frame| {
                let tcp = segment(frame, Side::A);
                assert_eq!(tcp.src_port, CLIENT_PORT);
                assert_eq!(tcp.seq_number, first_seq.unwrap() + retransmits * 512);
                assert_eq!(tcp.payload, &[1; 512]);
                retransmits += 1;
            }),
            turn.tx_accepted
        );
        assert_eq!(pair.close(), 0);
    });
}

const WRITE_CHUNK: usize = 1000;
const READ_CHUNK: usize = 700;
const ECHO_BUFFER: usize = 1500;

/// Receive calls that moved fewer bytes than requested; empty requests are
/// not counted.
#[derive(Debug, Default)]
struct ShortReads {
    zero_recvs: usize,
    partial_recvs: usize,
    pending: Option<(tcp::TcpFrame, usize)>,
}

impl ShortReads {
    fn recv(&mut self, socket: &mut tcp::Socket<'static>, buffer: &mut [u8]) -> usize {
        let mut read = 0usize;
        while read < buffer.len() {
            if self.pending.is_none() {
                if !socket.can_recv() { break; }
                let Some(frame) = socket.recv().unwrap() else { break; };
                self.pending = Some((frame, 0));
            }
            let (frame, offset) = self.pending.as_mut().unwrap();
            let n = (buffer.len() - read).min(frame.payload().len() - *offset);
            buffer[read..read + n].copy_from_slice(&frame.payload()[*offset..*offset + n]);
            *offset = offset.checked_add(n).expect("frame offset exceeds usize");
            read = read.checked_add(n).expect("read count exceeds usize");
            if *offset == frame.payload().len() {
                self.pending = None;
            }
        }
        if read == 0 && !buffer.is_empty() {
            self.zero_recvs += 1;
        } else if read < buffer.len() {
            self.partial_recvs += 1;
        }
        read
    }

    fn assert_seen(&self) {
        let seen = [self.zero_recvs, self.partial_recvs];
        assert!(seen.iter().all(|&count| count > 0), "{self:?}");
    }
}

/// Writes `data` in short calls and reads the echo back.
struct Writer<'d> {
    data: &'d [u8],
    written: usize,
    echoed: Vec<u8>,
    short: ShortReads,
}

impl Writer<'_> {
    fn step(&mut self, socket: &mut tcp::Socket<'static>) -> bool {
        let end = self.data.len().min(self.written + WRITE_CHUNK);
        let sent = socket.send_slice(&self.data[self.written..end]).unwrap();
        self.written += sent;
        let mut buffer = [0; READ_CHUNK];
        let read = self.short.recv(socket, &mut buffer);
        self.echoed.extend_from_slice(&buffer[..read]);
        sent + read > 0
    }
}

/// Returns every byte it reads through a fixed buffer.
struct Echo {
    buffer: [u8; ECHO_BUFFER],
    held: usize,
    read: usize,
    written: usize,
    short: ShortReads,
}

impl Echo {
    fn new() -> Self {
        Self {
            buffer: [0; ECHO_BUFFER],
            held: 0,
            read: 0,
            written: 0,
            short: ShortReads::default(),
        }
    }

    fn step(&mut self, socket: &mut tcp::Socket<'static>) -> bool {
        let read = self.short.recv(socket, &mut self.buffer[self.held..]);
        self.held += read;
        self.read += read;
        let sent = socket.send_slice(&self.buffer[..self.held]).unwrap();
        self.buffer.copy_within(sent..self.held, 0);
        self.held -= sent;
        self.written += sent;
        read + sent > 0
    }
}

/// `writer` sends `data` over the connection and the other side echoes it.
fn echo_phase(pair: &mut Pair, handles: &[TcpHandle; 2], writer: Side, data: &[u8]) {
    let echoer = writer.other();
    let mut source = Writer {
        data,
        written: 0,
        echoed: Vec::with_capacity(data.len()),
        short: ShortReads::default(),
    };
    let mut echo = Echo::new();
    pair.drive(
        |_, _| {},
        |a, b| {
            let (writer_stack, echo_stack) = writer.pick(a, b);
            let wrote = source.step(writer_stack.tcp_mut(&handles[writer.index()]));
            let echoed = echo.step(echo_stack.tcp_mut(&handles[echoer.index()]));
            if source.echoed.len() >= data.len() {
                App::Done
            } else if wrote || echoed {
                App::Moved
            } else {
                App::Idle
            }
        },
    );
    assert_eq!(
        (source.written, echo.read, echo.written, source.echoed.len()),
        (data.len(), data.len(), data.len(), data.len())
    );
    assert!(source.echoed == data, "echoed bytes differ");
    source.short.assert_seen();
    echo.short.assert_seen();
}

/// A closes first; each side reads to `Finished`, B closes once it has.
fn close_both(pair: &mut Pair, handles: &[TcpHandle; 2]) {
    let mut a_closed = false;
    let mut finished = [false; 2];
    pair.drive(
        |_, _| {},
        |a, b| {
            let mut moved = false;
            if !a_closed {
                a.close(&handles[0]);
                a_closed = true;
                moved = true;
            }
            for (side, stack) in [(Side::A, &mut *a), (Side::B, &mut *b)] {
                if finished[side.index()] {
                    continue;
                }
                let socket = stack.tcp_mut(&handles[side.index()]);
                match socket.recv() {
                    Ok(frame) => assert!(frame.is_none()),
                    Err(RecvError::Finished) => {
                        finished[side.index()] = true;
                        if side == Side::B {
                            stack.close(&handles[side.index()]);
                        }
                        moved = true;
                    }
                    Err(error) => panic!("{error}"),
                }
            }
            let states = (
                a.tcp(&handles[0]).state(),
                b.tcp(&handles[1]).state(),
            );
            if finished == [true; 2] && states == (State::TimeWait, State::Closed) {
                App::Done
            } else if moved {
                App::Moved
            } else {
                App::Idle
            }
        },
    );
}

#[switch_test_support::pinned_test]
fn echo_64k_each_way_short_reads_fin_close_all_pools_zero() {
    with_eal(|eal| {
        let mut pair = new_pair(eal, ["echoa", "echob"]);
        let (handles, _) = establish(&mut pair, Side::A);
        echo_phase(&mut pair, &handles, Side::A, &pattern(ECHO_LEN, 1));
        echo_phase(&mut pair, &handles, Side::B, &pattern(ECHO_LEN, 2));
        close_both(&mut pair, &handles);
        assert_no_drops(&pair);
        assert_eq!(pair.close(), 0);
    });
}

#[switch_test_support::pinned_test]
fn two_stack_mixed_sizes_deliver_bytes() {
    with_eal(|eal| {
        const CAPACITY: usize = 65536;
        const SEGMENTS: usize = 120;
        let configs = [pair::config(Side::A), pair::config(Side::B)];
        let mut pair = Pair::with_ports(
            eal,
            ["mbufwina", "mbufwinb"],
            configs,
            pair::T0,
            16,
            |_, port| {
                port.rx_pool.count = pair::pool_count(
                    dpdk_smoltcp::rx_reserve(
                        port.rx_descriptors,
                        port.rx_burst,
                        configs[0].rx_staging,
                        port.rx_pool.cache_size,
                    ),
                    SEGMENTS,
                );
                port.frame_pool.count = pair::pool_count(
                    dpdk_smoltcp::frame_reserve(port.tx_descriptors, configs[0].tx_pending),
                    pair::windows(port.mtu, [CAPACITY]),
                );
            },
        );
        let (handles, _) = establish_sized(&mut pair, Side::A, CAPACITY);
        pair.a.tcp_mut(&handles[0]).set_ack_delay(None);
        pair.b.tcp_mut(&handles[1]).set_ack_delay(None);
        let lengths: Vec<_> = (0..SEGMENTS).map(|index| [200, 1100, 1448][index % 3]).collect();
        let data = pattern(lengths.iter().sum(), 91);
        let mut received = Vec::new();
        let mut written = 0;
        let mut chunk = 0;
        let mut reading = false;
        let mut reader = ShortReads::default();
        let mut at = pair.now();
        for round in 0..MAX_ROUNDS {
            let last = pair.round_at(at, |_, _| {});
            let receiver = pair.b.tcp_mut(&handles[1]);
            reading |= receiver.recv_queue() >= CAPACITY / 2;
            let mut read = 0;
            if reading {
                let mut buffer = [0; 8192];
                read = reader.recv(receiver, &mut buffer);
                received.extend_from_slice(&buffer[..read]);
            }
            let sender = pair.a.tcp_mut(&handles[0]);
            let mut sent = 0;
            if chunk < lengths.len() && sender.send_queue() == 0 {
                let end = written + lengths[chunk];
                sent = sender.send_slice(&data[written..end]).unwrap();
                assert_eq!(sent, lengths[chunk]);
                written = end;
                chunk += 1;
            }
            if received.len() == data.len() && sender.send_queue() == 0 {
                break;
            }
            if !last.moved() && read + sent == 0 {
                let next = [pair.a.poll_at(), pair.b.poll_at()]
                    .into_iter()
                    .flatten()
                    .min()
                    .expect("unfinished transfer has a deadline");
                at = next.max(pair.now());
            }
            assert!(round + 1 < MAX_ROUNDS, "mixed-size transfer did not complete");
        }
        assert!(reading);
        assert_eq!(written, data.len());
        assert_eq!(received, data);
        assert_eq!(pair.b.tcp(&handles[1]).rx_refused_segments(), 0);
        pair.round(|_, _| {});
        assert_eq!(pair.in_use().map(|(rx, _)| rx), [0, 0]);
        assert_no_drops(&pair);
        assert_eq!(pair.close(), 0);
    });
}

#[switch_test_support::pinned_test]
fn two_stacks_pipelined_messages_complete_without_ack_delay() {
    with_eal(|eal| {
        const MESSAGE_LEN: usize = 300;
        const MESSAGES: usize = 12;
        let mut pair = new_pair(eal, ["wrapa", "wrapb"]);
        let (handles, _) = establish_sized(&mut pair, Side::A, 1000);
        assert_eq!(
            pair.a.tcp(&handles[0]).ack_delay(),
            Some(Duration::from_millis(10))
        );
        assert_eq!(
            pair.b.tcp(&handles[1]).ack_delay(),
            Some(Duration::from_millis(10))
        );
        let messages: Vec<_> = (0..MESSAGES)
            .map(|i| pattern(MESSAGE_LEN, i as u32))
            .collect();
        let mut written_at = vec![pair.now()];
        assert_eq!(
            pair.a.tcp_mut(&handles[0]).send_slice(&messages[0]),
            Ok(MESSAGE_LEN)
        );
        let (mut received, mut replies) = (0, 0);
        let (mut request, mut reply) = ([0; MESSAGE_LEN], [0; MESSAGE_LEN]);
        let (mut request_len, mut reply_len) = (0, 0);
        let mut request_reader = ShortReads::default();
        let mut reply_reader = ShortReads::default();
        let mut at = pair.now();

        for round in 0..MAX_ROUNDS {
            let last = pair.round_at(at, |_, _| {});
            let now = pair.now();
            let server = pair.b.tcp_mut(&handles[1]);
            let read = request_reader.recv(server, &mut request[request_len..]);
            request_len += read;
            if request_len == MESSAGE_LEN {
                assert_eq!(request.as_slice(), messages[received]);
                assert_eq!(server.send_slice(&request), Ok(MESSAGE_LEN));
                received += 1;
                request_len = 0;
            }
            let client = pair.a.tcp_mut(&handles[0]);
            let echoed = reply_reader.recv(client, &mut reply[reply_len..]);
            reply_len += echoed;
            if reply_len == MESSAGE_LEN {
                assert_eq!(reply.as_slice(), messages[replies]);
                let elapsed = now - written_at[replies];
                assert!(
                    elapsed < Duration::from_millis(10),
                    "message {} reply took {elapsed}",
                    replies + 1
                );
                replies += 1;
                reply_len = 0;
            }
            if replies == MESSAGES {
                break;
            }
            let mut wrote = false;
            while written_at.len() < MESSAGES && written_at.len() - replies < 2 {
                assert_eq!(
                    client.send_slice(&messages[written_at.len()]),
                    Ok(MESSAGE_LEN)
                );
                written_at.push(now);
                wrote = true;
            }
            if !last.moved() && read + echoed == 0 && !wrote {
                let next = [pair.a.poll_at(), pair.b.poll_at()]
                    .into_iter()
                    .flatten()
                    .min()
                    .unwrap();
                at = next.max(now);
            }
            assert!(round + 1 < MAX_ROUNDS, "messages did not complete");
        }
        assert_eq!(
            (written_at.len(), received, replies),
            (MESSAGES, MESSAGES, MESSAGES)
        );
        assert_no_drops(&pair);
        assert_eq!(pair.close(), 0);
    });
}
