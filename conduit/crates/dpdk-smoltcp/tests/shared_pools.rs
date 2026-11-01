//! Every socket of a Stack draws on one RX pool and one frame pool; nothing is reserved per
//! socket. A socket's buffer sizes cap what it holds, like the kernel's per-socket buffer
//! limits. When the RX pool is empty the link drops arrivals and the peer retransmits. While
//! it is low, a TCP socket that receives a segment keeps no out-of-order data, so out-of-order
//! frames cannot hold the pool while the segments filling their gaps are dropped.

use std::cell::Cell;
use std::collections::HashMap;
use std::rc::Rc;

use zhida_test_kit::many::{self, Conn, CLIENT_BASE};
use zhida_test_kit::pair::{
    pattern, pool_count, segment, windows, App, Pair, Side, MAX_ROUNDS, T0,
};
use zhida_test_kit::wire::{Link, Verdict};
use zhida_test_kit::with_eal;
use dpdk_smoltcp::smoltcp::wire::{IpEndpoint, TcpSeqNumber};
use dpdk_smoltcp::{frame_reserve, rx_reserve, Eal, Stack, UdpConfig, UdpHandle, UdpSendError};

/// Each socket's receive and send buffer.
const WINDOW: usize = 256 * 1024;
/// RX mbufs each Stack's pool holds beyond its reserve. net_ring posts no RX mbufs, so the
/// sockets can also take the reserve's posted-buffer and low-mark room: each pool holds 140
/// mbufs in all, fewer than one window of frames.
const RX_SHARE: usize = 24;
const UDP_PORT: u16 = 7000;

/// `sockets` TCP and `udp` UDP slots per side; RX pools of the reserve plus `RX_SHARE`,
/// frame pools large enough for every send queue, so only RX is shared short.
fn shared_pair(eal: &Eal, names: [&str; 2], sockets: usize, udp: usize) -> Pair {
    let configs = [Side::A, Side::B].map(|side| dpdk_smoltcp::StackConfig {
        max_udp_sockets: udp,
        ..many::config(side, sockets)
    });
    Pair::with_ports(eal, names, configs, T0, 16, |side, port| {
        let stack = configs[side.index()];
        let reserve = rx_reserve(port.rx_descriptors, port.rx_burst, stack.rx_staging, port.rx_pool.cache_size);
        port.rx_pool.count = pool_count(reserve, RX_SHARE);
        let queues = windows(port.mtu, std::iter::repeat_n(WINDOW, sockets));
        port.frame_pool.count = pool_count(frame_reserve(port.tx_descriptors, stack.tx_pending), queues);
    })
}

/// B's RX pool count in a `shared_pair`: the kit's RingPort has 16 RX descriptors and an
/// RX cache of 8.
fn b_rx_pool() -> usize {
    rx_reserve(16, 8, many::config(Side::B, 1).rx_staging, 8) + RX_SHARE
}

/// The Stack's low mark on the kit's RingPort: its 16 RX descriptors floored at 32, plus the
/// RX burst of 8. The pool is low while fewer mbufs are free.
const LOW_MARK: usize = 32 + 8;

/// Fill B's RX pool with unread UDP datagrams until it is low.
fn hold_pool_low(pair: &mut Pair, udp: &[UdpHandle; 2]) {
    while b_rx_pool() - pair.in_use()[1].0 as usize >= LOW_MARK {
        send_datagram(pair, udp);
        pair.round(|_, _| {});
    }
}

/// Queue as much of `data[*sent..]` as the client socket takes.
fn send_more(a: &mut Stack, conn: &Conn, data: &[u8], sent: &mut usize) -> bool {
    let written = a.tcp_mut(&conn.client).send_slice(&data[*sent..]).unwrap();
    *sent += written;
    written != 0
}

/// Append every in-order byte the server socket holds to `received`.
fn read_all(b: &mut Stack, conn: &Conn, received: &mut Vec<u8>) -> bool {
    let before = received.len();
    while let Some(frame) = b.tcp_mut(&conn.server).recv().unwrap() {
        received.extend_from_slice(frame.payload());
    }
    received.len() != before
}

/// Four connections share 24 RX mbufs beyond the reserve: all four are added and each
/// delivers its whole 256 KiB intact while its reader
/// keeps up; the pool's mbufs all return.
#[zhida_test_support::pinned_test]
fn sockets_beyond_the_sum_of_windows_all_deliver() {
    with_eal(|eal| {
        const SOCKETS: u16 = 4;
        let mut pair = shared_pair(eal, ["shra", "shrb"], usize::from(SOCKETS), 0);
        let conns = many::open(&mut pair, 0..SOCKETS, WINDOW);
        let data: Vec<_> = conns.iter().map(|conn| pattern(WINDOW, u32::from(conn.id))).collect();
        let mut sent = vec![0; conns.len()];
        let mut received = vec![Vec::new(); conns.len()];
        pair.drive(|_, _| {}, |a, b| {
            let mut moved = false;
            for (index, conn) in conns.iter().enumerate() {
                moved |= send_more(a, conn, &data[index], &mut sent[index]);
                moved |= read_all(b, conn, &mut received[index]);
            }
            if received.iter().all(|bytes| bytes.len() == WINDOW) {
                App::Done
            } else if moved {
                App::Moved
            } else {
                App::Idle
            }
        });
        assert_eq!(received, data);
        // Every pool is empty once the links and rings are freed.
        pair.close();
    });
}

/// B's reader stops while A sends a whole 256 KiB window: its shared RX pool runs out
/// and the link drops the rest. Once B reads, the
/// mbufs return, A retransmits what was dropped, and every byte arrives in order.
#[zhida_test_support::pinned_test]
fn rx_pool_exhaustion_drops_until_the_app_reads() {
    with_eal(|eal| {
        let mut pair = shared_pair(eal, ["shxa", "shxb"], 1, 0);
        let conns = many::open(&mut pair, 0..1, WINDOW);
        let conn = &conns[0];
        pair.a.tcp_mut(&conn.client).set_ack_delay(None);
        pair.b.tcp_mut(&conn.server).set_ack_delay(None);
        let data = pattern(WINDOW, 3);
        let mut sent = 0;
        let mut rounds = 0;
        while pair.a_to_b.pool_empty == 0 {
            rounds += 1;
            assert!(rounds < MAX_ROUNDS, "B's RX pool never ran out");
            send_more(&mut pair.a, conn, &data, &mut sent);
            pair.round(|_, _| {});
        }
        let held = pair.b.tcp(&conn.server).recv_queue();
        assert!(held < WINDOW, "B took its whole window into {RX_SHARE} shared mbufs");

        let mut received = Vec::new();
        pair.drive(|_, _| {}, |a, b| {
            let moved = send_more(a, conn, &data, &mut sent) | read_all(b, conn, &mut received);
            if received.len() == WINDOW {
                App::Done
            } else if moved {
                App::Moved
            } else {
                App::Idle
            }
        });
        assert_eq!(received, data);
        assert!(pair.a.tcp(&conn.client).retransmits() > 0);
        pair.close();
    });
}

/// While `blocked` holds, drops every A→B TCP segment of a client port in `ports` that starts
/// at the first data sequence number seen on that port: the connection's first segment and
/// every resend of it, so B holds the rest out of order behind the gap.
fn block_gaps(link: &mut Link, ports: Vec<u16>, blocked: Rc<Cell<bool>>) {
    let mut gaps: HashMap<u16, TcpSeqNumber> = HashMap::new();
    link.impair(move |frame| {
        // IPv4 carrying TCP (protocol 6); ARP and UDP pass.
        if frame[12..14] != [0x08, 0x00] || frame[23] != 6 {
            return Verdict::Deliver;
        }
        let tcp = segment(frame, Side::A);
        if tcp.payload.is_empty() || !ports.contains(&tcp.src_port) {
            return Verdict::Deliver;
        }
        let gap = *gaps.entry(tcp.src_port).or_insert(tcp.seq_number);
        if blocked.get() && tcp.seq_number == gap {
            Verdict::Drop
        } else {
            Verdict::Deliver
        }
    });
}

fn client_port(conn: &Conn) -> u16 {
    CLIENT_BASE + conn.id
}

fn udp_sockets(pair: &mut Pair) -> [UdpHandle; 2] {
    let config = UdpConfig { port: UDP_PORT, local_addr: None, rx_queue: 256, tx_checksum: true, broadcast: false };
    [pair.a.add_udp(config).unwrap(), pair.b.add_udp(config).unwrap()]
}

/// Send one 64-byte datagram from A to B's UDP socket, unless A's pending queue is full.
fn send_datagram(pair: &mut Pair, udp: &[UdpHandle; 2]) {
    let to = Some(IpEndpoint::new(Side::B.ip().into(), UDP_PORT));
    match pair.a.udp_send(&udp[0], to, &[0x5a; 64]) {
        Ok(()) | Err(UdpSendError::WouldBlock) => {}
        Err(error) => panic!("UDP send: {error:?}"),
    }
}

fn drain_udp(b: &mut Stack, udp: &UdpHandle) -> bool {
    let mut moved = false;
    while let Some(datagram) = b.udp_mut(udp).recv() {
        drop(datagram);
        moved = true;
    }
    moved
}

/// The stall per-socket pruning prevents. Four connections each lose their first segment,
/// and every resend of it, while the rest arrive and are held out of order: out-of-order
/// frames fill B's RX pool until it is low. From then on each connection that receives a
/// segment drops its own out-of-order frames and refuses out-of-order segments, so mbufs come
/// back. Once the gap segments get through, every connection delivers its bytes in order.
/// Without pruning the out-of-order frames keep the pool empty, every resent gap segment is
/// dropped for lack of an mbuf, and no connection progresses.
#[zhida_test_support::pinned_test]
fn out_of_order_frames_filling_the_pool_are_pruned_and_gaps_recover() {
    with_eal(|eal| {
        const SOCKETS: u16 = 4;
        let mut pair = shared_pair(eal, ["shoa", "shob"], usize::from(SOCKETS), 0);
        let conns = many::open(&mut pair, 0..SOCKETS, WINDOW);
        let blocked = Rc::new(Cell::new(true));
        block_gaps(&mut pair.a_to_b, conns.iter().map(client_port).collect(), blocked.clone());
        let data: Vec<_> = conns.iter().map(|conn| pattern(WINDOW, 10 + u32::from(conn.id))).collect();
        let mut sent = vec![0; conns.len()];
        // 64 rounds of at most 15 frames each way: far more than B's 140-mbuf pool.
        for _ in 0..64 {
            for (index, conn) in conns.iter().enumerate() {
                send_more(&mut pair.a, conn, &data[index], &mut sent[index]);
            }
            pair.round(|_, _| {});
        }
        for conn in &conns {
            assert_eq!(pair.b.tcp(&conn.server).recv_queue(), 0, "the gaps are still open");
        }

        blocked.set(false);
        let mut received = vec![Vec::new(); conns.len()];
        pair.drive(|_, _| {}, |a, b| {
            let mut moved = false;
            for (index, conn) in conns.iter().enumerate() {
                moved |= send_more(a, conn, &data[index], &mut sent[index]);
                moved |= read_all(b, conn, &mut received[index]);
            }
            if received.iter().all(|bytes| bytes.len() == WINDOW) {
                App::Done
            } else if moved {
                App::Moved
            } else {
                App::Idle
            }
        });
        assert_eq!(received, data);
        let refused: u32 = conns.iter().map(|conn| pair.b.tcp(&conn.server).rx_refused_segments()).sum();
        assert!(refused > 0, "B's RX pool went low and out-of-order segments were refused");
        pair.close();
    });
}

/// UDP datagrams B's application has not read hold B's RX pool at its low mark. A segment
/// arriving out of order behind a lost one is refused, not held. Once the application reads
/// the datagrams the pool recovers, and the next out-of-order segment is held; when the lost
/// segment gets through, every byte arrives in order.
#[zhida_test_support::pinned_test]
fn low_rx_pool_refuses_out_of_order_until_it_recovers() {
    with_eal(|eal| {
        let mut pair = shared_pair(eal, ["shla", "shlb"], 1, 1);
        let conns = many::open(&mut pair, 0..1, WINDOW);
        let conn = &conns[0];
        pair.a.tcp_mut(&conn.client).set_ack_delay(None);
        pair.b.tcp_mut(&conn.server).set_ack_delay(None);
        let udp = udp_sockets(&mut pair);
        let blocked = Rc::new(Cell::new(true));
        block_gaps(&mut pair.a_to_b, vec![client_port(conn)], blocked.clone());
        let idle = pair.in_use()[1].0 as usize;
        hold_pool_low(&mut pair, &udp);
        let data = pattern(3 * 1460, 21);
        assert_eq!(pair.a.tcp_mut(&conn.client).send_slice(&data[..2 * 1460]), Ok(2 * 1460));
        pair.round(|_, _| {});
        assert_eq!(pair.a_to_b.dropped, 1, "the first segment is the gap");
        assert_eq!(pair.b.tcp(&conn.server).rx_refused_segments(), 1, "refused while low");

        drain_udp(&mut pair.b, &udp[1]);
        assert_eq!(pair.in_use()[1].0 as usize, idle);
        assert_eq!(pair.a.tcp_mut(&conn.client).send_slice(&data[2 * 1460..]), Ok(1460));
        pair.round(|_, _| {});
        assert_eq!(pair.b.tcp(&conn.server).rx_refused_segments(), 1, "held once the pool recovered");
        assert_eq!(pair.in_use()[1].0 as usize, idle + 1);

        blocked.set(false);
        let mut received = Vec::new();
        pair.drive(|_, _| {}, |_, b| {
            if read_all(b, conn, &mut received) && received.len() == data.len() {
                App::Done
            } else {
                App::Idle
            }
        });
        assert_eq!(received, data);
        pair.close();
    });
}

#[zhida_test_support::pinned_test]
fn empty_burst_rechecks_low_pool_before_processing_staged_segments() {
    with_eal(|eal| {
        let mut pair = shared_pair(eal, ["shea", "sheb"], 1, 1);
        let conns = many::open(&mut pair, 0..1, WINDOW);
        let conn = &conns[0];
        pair.a.tcp_mut(&conn.client).set_ack_delay(None);
        pair.b.tcp_mut(&conn.server).set_ack_delay(None);
        let udp = udp_sockets(&mut pair);
        let blocked = Rc::new(Cell::new(true));
        block_gaps(&mut pair.a_to_b, vec![client_port(conn)], blocked.clone());
        let idle = pair.in_use()[1].0 as usize;
        hold_pool_low(&mut pair, &udp);

        let data = pattern(3 * 1460, 21);
        assert_eq!(pair.a.tcp_mut(&conn.client).send_slice(&data[..2 * 1460]), Ok(2 * 1460));
        let now = pair.now();
        pair.a.turn(now, 32).unwrap();
        pair.a_to_b.forward();
        assert_eq!(pair.a_to_b.dropped, 1);
        let staged = pair.b.receive(now, 0).unwrap();
        assert_eq!((staged.rx_received, staged.rx_processed, staged.rx_staged), (1, 0, 1));
        let still_low = pair.b.receive(now, 32).unwrap();
        assert_eq!((still_low.rx_received, still_low.rx_processed, still_low.rx_staged), (0, 1, 0));
        assert_eq!(pair.b.tcp(&conn.server).rx_refused_segments(), 1);

        assert_eq!(pair.a.tcp_mut(&conn.client).send_slice(&data[2 * 1460..]), Ok(1460));
        pair.a.turn(now, 32).unwrap();
        pair.a_to_b.forward();
        let staged = pair.b.receive(now, 0).unwrap();
        assert_eq!((staged.rx_received, staged.rx_processed, staged.rx_staged), (1, 0, 1));
        assert!(drain_udp(&mut pair.b, &udp[1]));
        assert_eq!(pair.in_use()[1].0 as usize, idle + 1);
        let recovered = pair.b.receive(now, 32).unwrap();
        assert_eq!((recovered.rx_received, recovered.rx_processed, recovered.rx_staged), (0, 1, 0));
        assert_eq!(pair.b.tcp(&conn.server).rx_refused_segments(), 1);
        assert_eq!(pair.in_use()[1].0 as usize, idle + 1, "out-of-order segment retained");

        blocked.set(false);
        let mut received = Vec::new();
        pair.drive(|_, _| {}, |_, b| {
            if read_all(b, conn, &mut received) && received.len() == data.len() {
                App::Done
            } else {
                App::Idle
            }
        });
        assert_eq!(received, data);
        pair.close();
    });
}

/// S1 holds out-of-order frames behind a lost segment. While it waits, S2's in-order data and
/// UDP datagrams, unread, take B's RX pool down to empty: the link drops arrivals, S1's
/// resends among them. Once the application reads S2 and the datagrams, the pool refills, S1's
/// resent segment lands, and S1 delivers every byte in order, as does S2.
#[zhida_test_support::pinned_test]
fn pool_drained_by_other_sockets_recovers_once_apps_read() {
    with_eal(|eal| {
        let mut pair = shared_pair(eal, ["shda", "shdb"], 2, 1);
        let conns = many::open(&mut pair, 0..2, WINDOW);
        let (s1, s2) = (&conns[0], &conns[1]);
        let udp = udp_sockets(&mut pair);
        let blocked = Rc::new(Cell::new(true));
        block_gaps(&mut pair.a_to_b, vec![client_port(s1)], blocked.clone());
        let idle = pair.in_use()[1].0;
        let data = [pattern(8 * 1460, 31), pattern(WINDOW, 32)];
        let mut sent = [0, 0];
        send_more(&mut pair.a, s1, &data[0], &mut sent[0]);
        for _ in 0..4 {
            pair.round(|_, _| {});
        }
        assert_eq!(pair.b.tcp(&s1.server).recv_queue(), 0);
        assert_eq!(pair.in_use()[1].0, idle + 7, "S1 holds seven segments out of order");

        let mut rounds = 0;
        while pair.a_to_b.pool_empty == 0 {
            rounds += 1;
            assert!(rounds < MAX_ROUNDS, "B's RX pool never ran out");
            send_more(&mut pair.a, s2, &data[1], &mut sent[1]);
            send_datagram(&mut pair, &udp);
            pair.round(|_, _| {});
        }
        assert_eq!(pair.b.tcp(&s1.server).recv_queue(), 0, "S1's gap is still open");

        blocked.set(false);
        let mut received = [Vec::new(), Vec::new()];
        pair.drive(|_, _| {}, |a, b| {
            let mut moved = send_more(a, s2, &data[1], &mut sent[1]);
            for (index, conn) in [s1, s2].into_iter().enumerate() {
                moved |= read_all(b, conn, &mut received[index]);
            }
            moved |= drain_udp(b, &udp[1]);
            if received[0].len() == data[0].len() && received[1].len() == data[1].len() {
                App::Done
            } else if moved {
                App::Moved
            } else {
                App::Idle
            }
        });
        assert_eq!(received, data);
        pair.close();
    });
}

/// Pruning is per socket. S1 holds three segments out of order; unread datagrams hold B's
/// RX pool low. A segment for S2 processed under that pressure is stored in order and S1's
/// out-of-order frames stay held: B's RX mbufs in use grow by S2's one frame. Once the pool
/// recovers and S1's gap gets through, S1 delivers every byte.
#[zhida_test_support::pinned_test]
fn pressure_prunes_only_the_receiving_socket() {
    with_eal(|eal| {
        let mut pair = shared_pair(eal, ["shua", "shub"], 2, 1);
        let conns = many::open(&mut pair, 0..2, WINDOW);
        let (s1, s2) = (&conns[0], &conns[1]);
        let udp = udp_sockets(&mut pair);
        let blocked = Rc::new(Cell::new(true));
        block_gaps(&mut pair.a_to_b, vec![client_port(s1)], blocked.clone());
        let idle = pair.in_use()[1].0;
        let data = pattern(4 * 1460, 41);
        assert_eq!(pair.a.tcp_mut(&s1.client).send_slice(&data), Ok(data.len()));
        for _ in 0..4 {
            pair.round(|_, _| {});
        }
        assert_eq!(pair.in_use()[1].0, idle + 3, "S1 holds three segments out of order");

        hold_pool_low(&mut pair, &udp);
        let before = pair.in_use()[1].0;
        assert_eq!(pair.a.tcp_mut(&s2.client).send_slice(&[0x77; 100]), Ok(100));
        pair.round(|_, _| {});
        assert_eq!(pair.b.tcp(&s2.server).recv_queue(), 100);
        assert_eq!(pair.in_use()[1].0, before + 1, "S1's out-of-order frames are still held");
        assert_eq!(pair.b.tcp(&s1.server).rx_refused_segments(), 0);

        drain_udp(&mut pair.b, &udp[1]);
        blocked.set(false);
        let mut received = Vec::new();
        pair.drive(|_, _| {}, |_, b| {
            if read_all(b, s1, &mut received) && received.len() == data.len() {
                App::Done
            } else {
                App::Idle
            }
        });
        assert_eq!(received, data);
        pair.close();
    });
}
