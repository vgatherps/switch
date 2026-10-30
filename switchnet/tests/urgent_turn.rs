//! A Net with `NetConfig::turn_request`: once a poll's tasks hand the Stack one `tx_burst` of
//! frames, the poll stops before its next task; the Net turn then sends them and clears the
//! request, and the next poll goes on with the tasks left.

use switchnet::{IpEndpoint, NetHandle, TxFrame};
use zhida_test_kit::with_eal;

mod support;
use support::{Mixed, LOCAL_PORT, connect, counted, peer_drain, peer_ip, peer_udp_send, udp_config};

/// The fixture ports' `tx_burst`.
const TX_BURST: usize = 8;

async fn filled(handle: &NetHandle, data: &[u8]) -> TxFrame {
    let mut frame = handle.tx_frame().await;
    frame.room_mut()[..data.len()].copy_from_slice(data);
    frame.set_len(data.len());
    frame
}

#[zhida_test_support::pinned_test]
fn a_udp_burst_stops_the_poll_until_the_net_turns() {
    with_eal(|eal| {
        let mut mixed = Mixed::with_turn_request(eal, ["urua", "urub"]);
        let request = mixed.rt.turn_request();
        let socket = mixed.handle().udp_bind(udp_config(7000)).unwrap();
        let (mut rx, mut tx) = socket.split();
        let peer = mixed.peer.add_udp(udp_config(7001)).unwrap();
        let reader = mixed.spawn(async move { (rx.recv().await, rx) });
        peer_udp_send(&mut mixed, &peer, 7000, b"ping");
        let (ping, rx) = mixed.wait(&reader);
        assert!(!request.is_set(), "receiving set the request");
        let handle = mixed.handle();
        let to = IpEndpoint::new(peer_ip().into(), 7001);
        let writer = mixed.spawn(async move {
            for i in 0..TX_BURST {
                let frame = filled(&handle, &[i as u8; 16]).await;
                tx.send(Some(to), frame).map_err(|(_, error)| error).unwrap();
            }
            tx
        });
        let (later, polls) = counted(async {});
        let later = mixed.spawn(later);
        mixed.rt.poll(0).unwrap();
        assert!(writer.is_full());
        assert!(request.is_set());
        assert_eq!(polls.get(), 0, "a task after the burst was polled before the Net turned");
        mixed.net.turn(mixed.now).unwrap();
        assert!(!request.is_set());
        assert_eq!(mixed.to_peer.forward(), TX_BURST);
        mixed.rt.poll(0).unwrap();
        assert_eq!(polls.get(), 1);
        later.take();
        mixed.peer.turn(mixed.now, zhida_test_kit::pair::BUDGET).unwrap();
        for i in 0..TX_BURST {
            assert_eq!(mixed.peer.udp_mut(&peer).recv().unwrap().payload(), [i as u8; 16]);
        }
        drop((ping, rx, writer.take(), peer));
        mixed.close();
    })
}

/// TCP frames count from `try_send`, before egress makes segments of them.
#[zhida_test_support::pinned_test]
fn a_tcp_burst_stops_the_poll_until_the_net_turns() {
    with_eal(|eal| {
        let mut mixed = Mixed::with_turn_request(eal, ["urta", "urtb"]);
        let request = mixed.rt.turn_request();
        let (stream, peer) = connect(&mut mixed, LOCAL_PORT);
        mixed.net.turn(mixed.now).unwrap();
        assert!(!request.is_set());
        let (rx, mut tx) = stream.split();
        let handle = mixed.handle();
        let writer = mixed.spawn(async move {
            for i in 0..TX_BURST {
                let frame = filled(&handle, &[i as u8; 100]).await;
                tx.room(frame.len()).await.unwrap();
                tx.try_send(frame).map_err(|(_, error)| error).unwrap();
            }
            tx
        });
        let (later, polls) = counted(async {});
        let later = mixed.spawn(later);
        mixed.rt.poll(0).unwrap();
        assert!(writer.is_full());
        assert!(request.is_set());
        assert_eq!(polls.get(), 0, "a task after the burst was polled before the Net turned");
        mixed.net.turn(mixed.now).unwrap();
        assert!(!request.is_set());
        assert!(mixed.to_peer.forward() > 0);
        mixed.rt.poll(0).unwrap();
        assert_eq!(polls.get(), 1);
        later.take();
        let mut got = Vec::new();
        mixed.until(|m| {
            peer_drain(m, &peer, &mut got);
            got.len() >= TX_BURST * 100
        });
        let sent: Vec<u8> = (0..TX_BURST).flat_map(|i| [i as u8; 100]).collect();
        assert_eq!(got, sent);
        drop((rx, writer.take(), peer));
        mixed.close();
    })
}

/// Fewer frames than one burst leave the request unset and the poll whole.
#[zhida_test_support::pinned_test]
fn fewer_frames_than_a_burst_do_not_stop_the_poll() {
    with_eal(|eal| {
        let mut mixed = Mixed::with_turn_request(eal, ["urfa", "urfb"]);
        let request = mixed.rt.turn_request();
        let socket = mixed.handle().udp_bind(udp_config(7000)).unwrap();
        let (mut rx, mut tx) = socket.split();
        let peer = mixed.peer.add_udp(udp_config(7001)).unwrap();
        let reader = mixed.spawn(async move { (rx.recv().await, rx) });
        peer_udp_send(&mut mixed, &peer, 7000, b"ping");
        let (ping, rx) = mixed.wait(&reader);
        let handle = mixed.handle();
        let to = IpEndpoint::new(peer_ip().into(), 7001);
        let writer = mixed.spawn(async move {
            for i in 0..TX_BURST - 1 {
                let frame = filled(&handle, &[i as u8; 16]).await;
                tx.send(Some(to), frame).map_err(|(_, error)| error).unwrap();
            }
            tx
        });
        let (later, polls) = counted(async {});
        let later = mixed.spawn(later);
        mixed.rt.poll(0).unwrap();
        assert!(!request.is_set());
        assert_eq!(polls.get(), 1);
        later.take();
        drop((ping, rx, writer.take(), peer));
        mixed.close();
    })
}
