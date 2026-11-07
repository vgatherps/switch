use std::time::Instant as WallInstant;

use dpdk_smoltcp::smoltcp::{socket::tcp::State, time::Duration};
use dpdk_smoltcp::TcpHandle;
use junction::control::{Event, SocketId};
use junction::rx::OwnedFrame;
use junction::send::TCP_SEND_HEADER;
use junction::session::Session;
use zhida_test_kit::with_eal;

mod support;
use support::{Fixture, LOCAL_PORT, PEER_PORT, WAIT, connect, local, peer_endpoint, peer_tcp_config, tcp_config, udp_config};

// A jumbo frame exceeds the ring, so its remaining bytes live only in held_rx
// after the peer's send queue drains. Retaining the published chunk keeps it full.
fn hold_partial_tcp_frame(
    f: &mut Fixture,
    client: &mut Session,
    events: &mut Vec<(SocketId, Event)>,
    id: SocketId,
    peer: &TcpHandle,
) -> Vec<OwnedFrame> {
    assert_eq!(f.peer.tcp_mut(peer).send_slice(&[0x5a; 8000]), Ok(8000));
    let mut held = Vec::new();
    let deadline = WallInstant::now() + WAIT;
    while held.is_empty() || f.peer.tcp(peer).send_queue() != 0 {
        assert!(WallInstant::now() < deadline, "jumbo data was not acknowledged");
        f.round_receive(client, events, |received_id, _, frame| {
            assert_eq!(received_id, id);
            held.push(frame);
        });
    }
    assert!(held.iter().map(|frame| frame.len()).sum::<usize>() < 8000);
    for _ in 0..4 {
        f.round_receive(client, events, |_, _, _| panic!("held ring was reclaimed"));
    }
    held
}

#[zhida_test_support::pinned_test]
fn plain_close_resets_tcp_with_unpublished_held_bytes() {
    with_eal(|eal| {
        let mut f = Fixture::with_jumbo_rx(eal, ["jrhcla", "jrhclb"], 1, 0, 4096);
        let mut client = f.client(1);
        let mut events = Vec::new();
        let (id, peer) = connect(&mut f, &mut client, &mut events, LOCAL_PORT);
        let held = hold_partial_tcp_frame(&mut f, &mut client, &mut events, id, &peer);
        client.close(id, false);
        f.until(&mut client, &mut events, |f, events| {
            events.contains(&(id, Event::Freed)) && f.peer.tcp(&peer).reset_by_peer()
        });
        drop(held);
        f.peer.remove_tcp(peer);
        drop(client);
        f.close();
    });
}

#[zhida_test_support::pinned_test]
fn graceful_finish_resets_tcp_with_unpublished_held_bytes() {
    with_eal(|eal| {
        let mut f = Fixture::with_jumbo_rx(eal, ["jrhfna", "jrhfnb"], 1, 0, 4096);
        let mut client = f.client(1);
        let mut events = Vec::new();
        let (id, peer) = connect(&mut f, &mut client, &mut events, LOCAL_PORT);
        let held = hold_partial_tcp_frame(&mut f, &mut client, &mut events, id, &peer);
        let stream = f.streams[0];
        client.finish().unwrap();
        f.until(&mut client, &mut events, |f, _| {
            f.retired.contains(&stream) && f.peer.tcp(&peer).reset_by_peer()
        });
        drop(held);
        f.peer.remove_tcp(peer);
        drop(client);
        f.close();
    });
}

#[zhida_test_support::pinned_test]
fn reset_after_fin_discards_held_bytes_without_peer_fin() {
    with_eal(|eal| {
        let mut f = Fixture::with_jumbo_rx(eal, ["jrhrea", "jrhreb"], 1, 0, 4096);
        let mut client = f.client(1);
        let mut events = Vec::new();
        let (id, peer) = connect(&mut f, &mut client, &mut events, LOCAL_PORT);
        let held = hold_partial_tcp_frame(&mut f, &mut client, &mut events, id, &peer);
        f.peer.close(&peer);
        f.until(&mut client, &mut events, |f, _| {
            f.peer.tcp(&peer).state() == State::FinWait2
        });
        assert!(!events.contains(&(id, Event::PeerFin)));
        f.peer.abort(&peer);
        f.until(&mut client, &mut events, |_, events| {
            events.contains(&(id, Event::Closed { reset_by_peer: true }))
        });
        assert!(!events.contains(&(id, Event::PeerFin)));
        assert!(!events.contains(&(id, Event::Delivered)));
        drop(held);
        f.peer.remove_tcp(peer);
        drop(client);
        f.close();
    });
}

#[zhida_test_support::pinned_test]
fn shutdown_after_peer_fin_reports_delivered_before_closed() {
    with_eal(|eal| {
        let mut f = Fixture::new(eal, ["jrsdla", "jrsdlb"], 1, 0, 64);
        let mut client = f.client(1);
        let mut events = Vec::new();
        let (id, peer) = connect(&mut f, &mut client, &mut events, LOCAL_PORT);
        f.peer.close(&peer);
        f.until(&mut client, &mut events, |_, events| {
            events.contains(&(id, Event::PeerFin))
        });
        client.shutdown(id);
        f.until(&mut client, &mut events, |_, events| {
            events.contains(&(id, Event::Closed { reset_by_peer: false }))
        });
        assert_eq!(events, [
            (id, Event::Connected),
            (id, Event::PeerFin),
            (id, Event::Delivered),
            (id, Event::Closed { reset_by_peer: false }),
        ]);
        f.peer.remove_tcp(peer);
        drop(client);
        f.close();
    });
}

#[zhida_test_support::pinned_test]
fn graceful_finish_limits_retries_and_frees_silent_backlogged_socket() {
    with_eal(|eal| {
        let mut f = Fixture::new(eal, ["jrsfna", "jrsfnb"], 1, 1, 256);
        let mut client = f.client(1);
        let mut events = Vec::new();
        let peer = f.peer.add_tcp(peer_tcp_config()).unwrap();
        f.peer.listen(&peer, PEER_PORT).unwrap();
        let mut config = tcp_config();
        config.tx_capacity = 1;
        let id = client.connect_tcp(config, peer_endpoint(PEER_PORT), local(LOCAL_PORT)).unwrap();
        f.until(&mut client, &mut events, |f, events| {
            events.contains(&(id, Event::Connected)) && f.peer.tcp(&peer).state() == State::Established
        });
        let mut record = [0x33; TCP_SEND_HEADER + 240];
        for _ in 0..16 {
            client.send_tcp(id, &mut record).unwrap();
        }
        let stream = f.streams[0];
        client.finish().unwrap();
        f.until(&mut client, &mut events, |f, _| f.retired.contains(&stream));
        assert!(f.peer.tcp(&peer).recv_queue() < 16 * 240, "backlog already drained");
        // No peer turns: ACKs cannot drain the backlog, only bounded retries can.
        for _ in 0..12 {
            f.now += Duration::from_secs(120);
            f.network.iteration(f.now).unwrap();
        }
        f.peer.remove_tcp(peer);
        drop(client);
        let mut next = f.client(2);
        let next_id = next.connect_tcp(tcp_config(), peer_endpoint(PEER_PORT), local(LOCAL_PORT + 1)).unwrap();
        let fence = next.bind_udp(udp_config(9000)).unwrap();
        let mut next_events = Vec::new();
        let deadline = WallInstant::now() + WAIT;
        while !next_events.contains(&(fence, Event::Bound)) {
            assert!(WallInstant::now() < deadline, "UDP ordering fence was not processed");
            // Keep the peer silent: resuming ACKs could drain an unlimited backlog.
            f.turn_network();
            next.poll_events(|id, event| next_events.push((id, event))).unwrap();
            std::thread::yield_now();
        }
        assert!(!next_events.iter().any(|(id, event)| *id == next_id && matches!(event, Event::Rejected(_))),
            "silent retired socket retained the only TCP slot: {next_events:?}");
        drop(next);
        f.close();
    });
}
