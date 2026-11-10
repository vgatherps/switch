//! Once a poll's tasks hand the Stack one `tx_burst` of frames, the poll ends before its next
//! task (`switch::request_turn`); the Net turn then sends them, and the next poll goes on
//! with the tasks left. Nothing to configure.

use switchnet::{IpEndpoint, NetHandle, ImmutableFrame};
use zhida_test_kit::with_eal;

mod support;
use support::{Mixed, LOCAL_PORT, bind, connect, counted, peer_drain, peer_ip, peer_udp_send, udp_config};

/// The fixture ports' `tx_burst`.
const TX_BURST: usize = 8;

async fn filled(handle: &NetHandle, data: &[u8]) -> ImmutableFrame {
    let mut frame = handle.tx_frame().await;
    frame.as_mut()[..data.len()].copy_from_slice(data);
    frame.truncate(data.len());
    frame.freeze()
}

#[zhida_test_support::pinned_test]
fn a_udp_burst_stops_the_poll_until_the_net_turns() {
    with_eal(|eal| {
        let mut mixed = Mixed::new(eal, ["urua", "urub"], support::Backend::Conduit);
        let socket = bind(&mut mixed, udp_config(7000));
        let (mut rx, mut tx) = socket.split();
        let peer = mixed.peer.add_udp(udp_config(7001)).unwrap();
        let reader = mixed.spawn(async move { (rx.recv().await, rx) });
        peer_udp_send(&mut mixed, &peer, 7000, b"ping");
        let (ping, rx) = mixed.wait(&reader);
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
        assert_eq!(polls.get(), 0, "a task after the burst was polled before the Net turned");
        mixed.net.turn(mixed.now).unwrap();
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
        let mut mixed = Mixed::new(eal, ["urta", "urtb"], support::Backend::Conduit);
        let (stream, peer) = connect(&mut mixed, LOCAL_PORT);
        mixed.net.turn(mixed.now).unwrap();
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
        assert_eq!(polls.get(), 0, "a task after the burst was polled before the Net turned");
        mixed.net.turn(mixed.now).unwrap();
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

/// Fewer frames than one burst leave the poll whole.
#[zhida_test_support::pinned_test]
fn fewer_frames_than_a_burst_do_not_stop_the_poll() {
    with_eal(|eal| {
        let mut mixed = Mixed::new(eal, ["urfa", "urfb"], support::Backend::Conduit);
        let socket = bind(&mut mixed, udp_config(7000));
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
        assert_eq!(polls.get(), 1);
        later.take();
        drop((ping, rx, writer.take(), peer));
        mixed.close();
    })
}

/// Frames sent outside any task, past one burst: no panic, and the next poll is whole.
#[zhida_test_support::pinned_test]
fn a_burst_sent_outside_any_task_does_not_stop_the_next_poll() {
    with_eal(|eal| {
        let mut mixed = Mixed::new(eal, ["uroa", "urob"], support::Backend::Conduit);
        let socket = bind(&mut mixed, udp_config(7000));
        let (mut rx, mut tx) = socket.split();
        let peer = mixed.peer.add_udp(udp_config(7001)).unwrap();
        let reader = mixed.spawn(async move { (rx.recv().await, rx) });
        peer_udp_send(&mut mixed, &peer, 7000, b"ping");
        let (ping, rx) = mixed.wait(&reader);
        let handle = mixed.handle();
        let frames = mixed.spawn(async move {
            let mut frames = Vec::new();
            for i in 0..=TX_BURST {
                frames.push(filled(&handle, &[i as u8; 16]).await);
            }
            frames
        });
        let frames = mixed.wait(&frames);
        let to = IpEndpoint::new(peer_ip().into(), 7001);
        for frame in frames {
            tx.send(Some(to), frame).map_err(|(_, error)| error).unwrap();
        }
        let (first, first_polls) = counted(async {});
        let (second, second_polls) = counted(async {});
        let (first, second) = (mixed.spawn(first), mixed.spawn(second));
        mixed.rt.poll(0).unwrap();
        assert_eq!((first_polls.get(), second_polls.get()), (1, 1));
        first.take();
        second.take();
        mixed.net.turn(mixed.now).unwrap();
        assert_eq!(mixed.to_peer.forward(), TX_BURST + 1);
        drop((ping, rx, tx, peer));
        mixed.close();
    })
}
