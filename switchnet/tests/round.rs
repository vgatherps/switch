//! `round`: one Net turn and one Runtime poll, reporting whether anything moved.

use switchnet::{IpEndpoint, round};
use zhida_test_kit::pair::BUDGET;
use zhida_test_kit::with_eal;

mod support;
use support::{Mixed, counted, net_ip, peer_udp_send, udp_config};

#[zhida_test_support::pinned_test]
fn idle_rounds_report_nothing_moved() {
    with_eal(|eal| {
        let mut mixed = Mixed::new(eal, ["rdia", "rdib"]);
        assert_eq!(mixed.net.poll_at(), None, "the Stack has a timer");
        for _ in 0..1000 {
            let moved = round(&mut mixed.net, &mut mixed.rt, mixed.now, 0).unwrap();
            assert!(!moved, "an idle round moved something");
        }
        mixed.close();
    })
}

/// A datagram for a parked receiver: the round that takes it in wakes the task and reports it.
#[zhida_test_support::pinned_test]
fn a_round_that_wakes_a_task_reports_it_moved() {
    with_eal(|eal| {
        let mut mixed = Mixed::new(eal, ["rdwa", "rdwb"]);
        let peer = mixed.peer.add_udp(udp_config(7001)).unwrap();
        peer_udp_send(&mut mixed, &peer, 7009, b"resolve");
        mixed.rounds(5);
        let mut socket = mixed.handle().udp_bind(udp_config(7000)).unwrap();
        let (task, polls) = counted(async move {
            let datagram = socket.recv().await;
            (socket, datagram)
        });
        let slot = mixed.spawn(task);
        assert!(round(&mut mixed.net, &mut mixed.rt, mixed.now, 0).unwrap(), "the spawned task was polled");
        assert!(!round(&mut mixed.net, &mut mixed.rt, mixed.now, 0).unwrap());
        mixed.peer.udp_send(&peer, Some(IpEndpoint::new(net_ip().into(), 7000)), b"wake").unwrap();
        mixed.peer.turn(mixed.now, BUDGET).unwrap();
        assert_eq!(mixed.to_net.forward(), 1);
        assert!(round(&mut mixed.net, &mut mixed.rt, mixed.now, 0).unwrap());
        assert_eq!(polls.get(), 2);
        let (socket, datagram) = slot.take();
        assert_eq!(datagram.payload(), b"wake");
        drop((socket, datagram, peer));
        mixed.close();
    })
}
