//! Every socket of a Stack draws on one RX pool and one frame pool; nothing is reserved per
//! socket. A socket's buffer sizes cap what it holds, like the kernel's per-socket buffer
//! limits. When the RX pool is empty the link drops arrivals and the peer retransmits.

use zhida_test_kit::many::{self, Conn};
use zhida_test_kit::pair::{self, pattern, pool_count, windows, App, Pair, Side, MAX_ROUNDS, T0};
use zhida_test_kit::with_eal;
use dpdk_smoltcp::smoltcp::socket::tcp;
use dpdk_smoltcp::{frame_reserve, rx_reserve, Eal, Stack};

/// Each socket's receive and send buffer: 180 RX frames at IP MTU 1500.
const WINDOW: usize = 256 * 1024;
/// RX mbufs each Stack's pool holds beyond its reserve.
const RX_SHARE: usize = 64;

/// `sockets` slots per side; RX pools of the reserve plus `RX_SHARE`, frame pools large
/// enough for every send queue, so only RX is shared short.
fn shared_pair(eal: &Eal, names: [&str; 2], sockets: usize) -> Pair {
    let configs = [Side::A, Side::B].map(|side| many::config(side, sockets));
    Pair::with_ports(eal, names, configs, T0, 16, |side, port| {
        let stack = configs[side.index()];
        let reserve = rx_reserve(port.rx_descriptors, stack.rx_staging, port.rx_pool.cache_size);
        port.rx_pool.count = pool_count(reserve, RX_SHARE);
        let queues = windows(port.mtu, std::iter::repeat_n(WINDOW, sockets));
        port.frame_pool.count = pool_count(frame_reserve(port.tx_descriptors, stack.tx_pending), queues);
    })
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

/// Four connections whose receive caps total 720 frames share 64 RX mbufs beyond the
/// reserve: all four are added and each delivers its whole 256 KiB intact while its reader
/// keeps up; the pool's mbufs all return.
#[zhida_test_support::pinned_test]
fn sockets_beyond_the_sum_of_windows_all_deliver() {
    with_eal(|eal| {
        const SOCKETS: u16 = 4;
        let mut pair = shared_pair(eal, ["shra", "shrb"], usize::from(SOCKETS));
        let conns = many::open(&mut pair, 0..SOCKETS, WINDOW);
        let cap = tcp::rx_mbuf_budget(&pair::tcp_socket_sized(WINDOW), 1500);
        assert_eq!(cap, 180);
        for conn in &conns {
            assert_eq!(pair.b.tcp(&conn.server).rx_frame_cap(), cap);
        }
        assert!(usize::from(SOCKETS) * cap > RX_SHARE);
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

/// B's reader stops while A sends a whole 256 KiB window: B's socket would take 180 frames,
/// but its share of the RX pool runs out first and the link drops the rest. Once B reads, the
/// mbufs return, A retransmits what was dropped, and every byte arrives in order.
#[zhida_test_support::pinned_test]
fn rx_pool_exhaustion_drops_until_the_app_reads() {
    with_eal(|eal| {
        let mut pair = shared_pair(eal, ["shxa", "shxb"], 1);
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
        assert!(pair.b.tcp(&conn.server).recv_room() > 0, "the drops came from the pool, not the window");

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
