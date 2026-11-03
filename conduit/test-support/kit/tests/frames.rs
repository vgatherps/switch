//! Frames a test builds and moves itself: `Pair::inject` and `Pair::drain_tx`
//! reach a Stack's rings without the peer, and `Pair::with_ports` RX bursts
//! let one turn take more than the default 8 frames.

use zhida_test_kit::pair::{config, new_pair, Pair, Side, T0};
use zhida_test_kit::ring_port;
use zhida_test_kit::with_eal;
use dpdk_smoltcp::smoltcp::wire::{
    ArpOperation, ArpPacket, ArpRepr, EthernetAddress, EthernetFrame, EthernetProtocol,
};
use dpdk_smoltcp::{rx_reserve, Turn};

/// A's ARP request for B's address.
fn arp_request() -> Vec<u8> {
    ring_port::arp(
        ArpOperation::Request,
        EthernetAddress::BROADCAST,
        (Side::A.mac(), Side::A.ip()),
        (EthernetAddress([0; 6]), Side::B.ip()),
    )
}

/// B's reply to `arp_request`.
fn assert_arp_reply(frame: &[u8]) {
    let eth = EthernetFrame::new_checked(frame).unwrap();
    assert_eq!(
        (eth.src_addr(), eth.dst_addr(), eth.ethertype()),
        (Side::B.mac(), Side::A.mac(), EthernetProtocol::Arp)
    );
    let arp = ArpRepr::parse(&ArpPacket::new_checked(eth.payload()).unwrap()).unwrap();
    assert_eq!(
        arp,
        ArpRepr::EthernetIpv4 {
            operation: ArpOperation::Reply,
            source_hardware_addr: Side::B.mac(),
            source_protocol_addr: Side::B.ip(),
            target_hardware_addr: Side::A.mac(),
            target_protocol_addr: Side::A.ip(),
        }
    );
}

/// (received, processed, accepted), with nothing staged or pending.
fn counts(turn: &Turn) -> (usize, usize, usize) {
    assert_eq!(
        (turn.rx_rejected, turn.rx_staged, turn.tx_pending),
        (0, 0, 0),
        "{turn:?}"
    );
    (turn.rx_received, turn.rx_processed, turn.tx_accepted)
}

#[zhida_test_support::pinned_test]
fn with_ports_rx_burst_lets_one_turn_take_more_than_eight_frames() {
    with_eal(|eal| {
        // Default Ports: RX bursts of 8, so 12 requests take two turns.
        let mut pair = new_pair(eal, ["kfda", "kfdb"]);
        let now = pair.now();
        pair.inject(Side::B, &vec![arp_request(); 12]);
        for expected in [8, 4] {
            let turn = pair.b.turn(now, 32).unwrap();
            assert_eq!(
                (turn.rx_received, turn.rx_processed, turn.tx_accepted),
                (expected, expected, expected)
            );
            assert_eq!(pair.drain_tx(Side::B, assert_arp_reply), expected);
        }
        assert_eq!(pair.close(), 0);

        // RX bursts of 32: one turn takes all 20, and draining its replies
        // takes more than one 16-frame dequeue.
        let configs = [config(Side::A), config(Side::B)];
        let mut pair = Pair::with_ports(eal, ["kfwa", "kfwb"], configs, T0, 64, |side, port| {
            port.rx_burst = 32;
            port.rx_pool.count = u32::try_from(rx_reserve(
                port.rx_descriptors,
                port.rx_burst,
                configs[side.index()].rx_staging,
                port.rx_pool.cache_size,
            ))
            .unwrap();
        });
        let now = pair.now();
        pair.inject(Side::B, &vec![arp_request(); 20]);
        assert_eq!(counts(&pair.b.turn(now, 32).unwrap()), (20, 20, 20));
        assert_eq!(pair.drain_tx(Side::B, assert_arp_reply), 20);
        assert_eq!(pair.drain_tx(Side::B, |_| panic!("TX ring not drained")), 0);
        assert_eq!(pair.close(), 0);
    });
}
