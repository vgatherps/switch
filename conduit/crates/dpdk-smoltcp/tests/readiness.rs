//! Readiness tokens on a real Stack pair: which calls fire them, and that idle
//! turns, removal and dropped handles fire nothing.

use dpdk_smoltcp::smoltcp::socket::tcp::State;
use dpdk_smoltcp::smoltcp::time::Duration;
use dpdk_smoltcp::smoltcp::wire::IpEndpoint;
use dpdk_smoltcp::{
    frame_pool_count, frame_reserve, ReadySide, Stack, StackConfig, UdpConfig, UdpSendError,
};
use zhida_test_kit::pair::{self, establish_sized, Pair, Side, BUDGET, MAX_ROUNDS};
use zhida_test_kit::with_eal;

/// Pair::new sizes pools for receive buffers up to this many bytes.
const SMALL: usize = 6 * (1500 - 52);

fn popped(stack: &mut Stack) -> Vec<u32> {
    let mut fired: Vec<u32> = std::iter::from_fn(|| stack.pop_fired()).collect();
    fired.sort_unstable();
    fired
}

/// Rounds at the earliest deadline until neither Stack has one.
fn settle(pair: &mut Pair) {
    for _ in 0..MAX_ROUNDS {
        let Some(at) = [pair.a.poll_at(), pair.b.poll_at()].into_iter().flatten().min() else {
            return;
        };
        pair.round_at(at.max(pair.now()), |_, _| {});
    }
    panic!("deadlines remain after {MAX_ROUNDS} rounds");
}

#[zhida_test_support::pinned_test]
fn tcp_tokens_fire_on_segments_acks_app_calls_and_time_wait_end_only() {
    with_eal(|eal| {
        let mut pair = pair::new_pair(eal, ["rdya", "rdyb"]);
        let (h, _) = establish_sized(&mut pair, Side::A, SMALL);
        let key = pair.a.tcp_ready_key(&h[0]);
        settle(&mut pair);
        assert_eq!(popped(&mut pair.a), [], "nothing was armed");

        // Idle turns fire nothing.
        pair.a.arm_tcp(key, ReadySide::Rx, 1);
        pair.a.arm_tcp(key, ReadySide::Tx, 2);
        for _ in 0..3 {
            let round = pair.round_at(pair.now() + Duration::from_millis(1), |_, _| {});
            assert!(!round.moved());
        }
        assert_eq!(popped(&mut pair.a), []);

        // A received segment fires both halves, once.
        assert_eq!(pair.b.tcp_mut(&h[1]).send_slice(b"data"), Ok(4));
        pair.round(|_, _| {});
        pair.round(|_, _| {});
        assert!(pair.a.tcp(&h[0]).can_recv());
        assert_eq!(popped(&mut pair.a), [1, 2]);

        // tcp_mut and tcp_send_frame fire at once, between turns.
        pair.a.arm_tcp(key, ReadySide::Rx, 3);
        let frame = pair.a.tcp_mut(&h[0]).recv().unwrap().unwrap();
        assert_eq!(frame.payload(), b"data");
        assert_eq!(popped(&mut pair.a), [3]);
        drop(frame);
        pair.a.arm_tcp(key, ReadySide::Tx, 4);
        let mut out = pair.a.tx_frame().unwrap();
        out.room_mut()[..1000].fill(7);
        out.set_len(1000);
        assert!(pair.a.tcp_send_frame(&h[0], out).is_ok());
        assert_eq!(popped(&mut pair.a), [4]);

        // Sending the data fires nothing; the peer's ACK that frees the room does.
        pair.a.arm_tcp(key, ReadySide::Tx, 5);
        pair.round(|_, _| {});
        assert_eq!(pair.a.tcp(&h[0]).send_queue(), 1000);
        assert_eq!(popped(&mut pair.a), []);
        settle(&mut pair);
        assert_eq!(pair.a.tcp(&h[0]).send_queue(), 0);
        assert_eq!(popped(&mut pair.a), [5]);

        // TIME-WAIT ends on a timer: turns of A alone fire nothing until it does.
        pair.a.tcp_mut(&h[0]).close();
        settle(&mut pair);
        pair.b.tcp_mut(&h[1]).close();
        pair.round(|_, _| {});
        pair.round(|_, _| {});
        assert_eq!(pair.a.tcp(&h[0]).state(), State::TimeWait);
        assert_eq!(pair.a.turn(pair.now(), BUDGET).unwrap().rx_received, 0);
        popped(&mut pair.a);
        pair.a.arm_tcp(key, ReadySide::Rx, 6);
        pair.a.arm_tcp(key, ReadySide::Tx, 7);
        let mut now = pair.now();
        for _ in 0..MAX_ROUNDS {
            if pair.a.tcp(&h[0]).state() == State::Closed {
                break;
            }
            assert_eq!(popped(&mut pair.a), []);
            now = pair.a.poll_at().expect("TIME-WAIT has a deadline").max(now);
            pair.a.turn(now, BUDGET).unwrap();
        }
        assert_eq!(pair.a.tcp(&h[0]).state(), State::Closed);
        assert_eq!(popped(&mut pair.a), [6, 7]);

        // disarm reports whether the token was still armed.
        pair.a.arm_tcp(key, ReadySide::Rx, 8);
        assert!(pair.a.disarm_tcp(key, ReadySide::Rx));
        assert!(!pair.a.disarm_tcp(key, ReadySide::Rx));
        pair.a.arm_tcp(key, ReadySide::Rx, 9);
        pair.a.tcp_mut(&h[0]);
        assert!(!pair.a.disarm_tcp(key, ReadySide::Rx), "fired, not yet popped");
        assert_eq!(popped(&mut pair.a), [9]);

        // A dropped handle's tokens never fire: the Stack disarms them when it
        // orphans the socket, which changes it.
        pair.a.arm_tcp(key, ReadySide::Rx, 10);
        let [ah, bh] = h;
        drop(ah);
        pair.a.turn(now, BUDGET).unwrap();
        assert_eq!(popped(&mut pair.a), []);
        drop(bh);
        pair.close();
    })
}

/// The app holds every received frame until the window shuts; releasing them
/// queues a receive wake, and the next turn fires the token with nothing
/// received.
#[zhida_test_support::pinned_test]
fn tcp_token_fires_when_released_frames_reopen_the_window() {
    with_eal(|eal| {
        let mut pair = pair::new_pair(eal, ["rdwa", "rdwb"]);
        let (h, _) = establish_sized(&mut pair, Side::A, SMALL);
        let data = vec![5u8; 64 * 1024];
        let (mut sent, mut held) = (0, Vec::new());
        for _ in 0..200 {
            sent += pair.b.tcp_mut(&h[1]).send_slice(&data[sent..]).unwrap();
            while let Ok(Some(frame)) = pair.a.tcp_mut(&h[0]).recv() {
                held.push(frame);
            }
            let at = [pair.a.poll_at(), pair.b.poll_at()].into_iter().flatten().min();
            pair.round_at(at.unwrap_or(pair.now()).max(pair.now()), |_, _| {});
        }
        while let Ok(Some(frame)) = pair.a.tcp_mut(&h[0]).recv() {
            held.push(frame);
        }
        assert!(held.len() >= 3, "only {} frames held", held.len());
        assert_eq!(pair.a.tcp(&h[0]).recv_room(), 0, "the window did not shut");
        pair.a.turn(pair.now(), BUDGET).unwrap();
        popped(&mut pair.a);
        let key = pair.a.tcp_ready_key(&h[0]);
        pair.a.arm_tcp(key, ReadySide::Rx, 1);
        held.clear();
        assert_eq!(popped(&mut pair.a), [], "a dropped frame fires nothing by itself");
        let turn = pair.a.turn(pair.now(), BUDGET).unwrap();
        assert_eq!(turn.rx_received, 0);
        assert_eq!(popped(&mut pair.a), [1]);
        drop(h);
        pair.close();
    })
}

const PORT: u16 = 1234;

fn udp_pair(eal: &dpdk_smoltcp::Eal) -> Pair {
    let configs = [Side::A, Side::B].map(|side| StackConfig { max_udp_sockets: 2, ..pair::config(side) });
    Pair::with_ports(eal, ["rdua", "rdub"], configs, pair::T0, 16, |side, port| {
        port.rx_pool.count = 255;
        port.frame_pool.count = frame_pool_count(
            frame_reserve(port.tx_descriptors, configs[side.index()].tx_pending),
            port.mtu,
            [SMALL],
        );
    })
}

fn udp_config(port: u16) -> UdpConfig {
    UdpConfig { port, local_addr: None, rx_queue: 8, tx_checksum: true, broadcast: false }
}

#[zhida_test_support::pinned_test]
fn udp_token_fires_on_a_queued_datagram_only() {
    with_eal(|eal| {
        let mut pair = udp_pair(eal);
        let ua = pair.a.add_udp(udp_config(PORT)).unwrap();
        let key = pair.a.udp_ready_key(&ua);
        let ub = pair.b.add_udp(udp_config(PORT)).unwrap();
        let to_a = Some(IpEndpoint::new(Side::A.ip().into(), PORT));
        assert_eq!(pair.b.udp_send(&ub, to_a, b"x"), Err(UdpSendError::NeighborPending));
        for _ in 0..3 {
            pair.round(|_, _| {});
        }

        pair.a.arm_udp(key, 1);
        for _ in 0..3 {
            pair.round_at(pair.now() + Duration::from_millis(1), |_, _| {});
        }
        assert_eq!(popped(&mut pair.a), [], "idle turns fire nothing");
        pair.b.udp_send(&ub, to_a, b"one").unwrap();
        pair.round(|_, _| {});
        pair.round(|_, _| {});
        assert_eq!(pair.a.udp(&ua).rx_queued(), 1);
        assert_eq!(popped(&mut pair.a), [1]);
        drop(pair.a.udp_mut(&ua).recv());
        assert_eq!(popped(&mut pair.a), [], "receiving and releasing fire nothing");

        pair.a.arm_udp(key, 2);
        assert!(pair.a.disarm_udp(key));
        pair.a.arm_udp(key, 3);
        pair.b.udp_send(&ub, to_a, b"two").unwrap();
        pair.round(|_, _| {});
        pair.round(|_, _| {});
        assert!(!pair.a.disarm_udp(key), "fired, not yet popped");
        assert_eq!(popped(&mut pair.a), [3]);
        drop(pair.a.udp_mut(&ua).recv());

        // A dropped handle's token never fires, also for a socket that reuses its slot.
        pair.a.arm_udp(key, 4);
        drop(ua);
        pair.b.udp_send(&ub, to_a, b"gone").unwrap();
        pair.round(|_, _| {});
        pair.round(|_, _| {});
        let reused = pair.a.add_udp(udp_config(PORT)).unwrap();
        pair.b.udp_send(&ub, to_a, b"new").unwrap();
        pair.round(|_, _| {});
        pair.round(|_, _| {});
        assert_eq!(pair.a.udp(&reused).rx_queued(), 1);
        assert_eq!(popped(&mut pair.a), []);
        drop(pair.a.udp_mut(&reused).recv());
        drop((reused, ub));
        pair.close();
    })
}
