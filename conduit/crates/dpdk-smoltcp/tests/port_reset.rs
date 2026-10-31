//! A device reset in the middle of a TCP exchange: the Stack recovers the Port at its next turn
//! and the connection carries on, including the bytes the reset destroyed.

use dpdk_smoltcp::{port_by_name, port_resets};
use zhida_dpdk_sys::rte_eth_dev_stop;
use zhida_test_kit::pair::{drain_bytes, establish, new_pair, pattern, App, Side};
use zhida_test_kit::with_eal;

/// Less than the 4 KiB socket buffers: each side queues all of it at once.
const DATA: usize = 3000;

/// B's port goes down the way an ENA reset leaves it: stopped (its bursts move nothing) and
/// the frames it had not sent gone. Then B's PMD reports the reset. B's next turn must reset,
/// configure and start the port again, once; both sides' data then arrives whole, B's by
/// retransmission, and every pool is idle at close.
#[zhida_test_support::pinned_test]
fn reset_mid_transfer_recovers_and_keeps_the_connection() {
    with_eal(|eal| {
        let mut pair = new_pair(eal, ["rsta", "rstb"]);
        let port_b = port_by_name(eal, "net_ring_rstb").expect("B's ethdev");
        let (handles, _) = establish(&mut pair, Side::A);
        let (a_data, b_data) = (pattern(DATA, 1), pattern(DATA, 2));
        assert_eq!(pair.a.tcp_mut(&handles[0]).send_slice(&a_data), Ok(DATA));
        assert_eq!(pair.b.tcp_mut(&handles[1]).send_slice(&b_data), Ok(DATA));
        let now = pair.now();
        // A's segments wait on A's TX ring and reach B's RX ring in the first round below.
        assert!(pair.a.turn(now, 32).unwrap().tx_accepted > 0);
        assert!(pair.b.turn(now, 32).unwrap().tx_accepted > 0);

        let resets = port_resets();
        assert!(pair.drain_tx(Side::B, |_| {}) > 0, "B's in-flight data is lost");
        assert_eq!(unsafe { rte_eth_dev_stop(port_b) }, 0);
        pair.b.raise_reset();

        let (mut at_a, mut at_b) = (Vec::new(), Vec::new());
        pair.drive(
            |_, _| {},
            |a, b| {
                at_a.extend(drain_bytes(a.tcp_mut(&handles[0])).unwrap());
                at_b.extend(drain_bytes(b.tcp_mut(&handles[1])).unwrap());
                if at_a.len() == DATA && at_b.len() == DATA {
                    App::Done
                } else {
                    App::Idle
                }
            },
        );
        assert_eq!(port_resets(), resets + 1);
        assert_eq!(at_b, a_data);
        assert_eq!(at_a, b_data);
        pair.close();
    });
}
