use std::future::{Future, poll_fn};
use std::pin::pin;
use std::task::Poll;

use switchnet::{Error, IpEndpoint, tcp};
use zhida_test_kit::pair::BUDGET;
use zhida_test_kit::with_eal;

mod support;
use support::{Mixed, LOCAL_PORT, PEER_PORT, local, local_at, net_ip, peer_endpoint, tcp_config, udp_config};

async fn first_poll<F: Future>(future: F, backend: support::Backend) -> F::Output {
    let mut future = pin!(future);
    poll_fn(|cx| match future.as_mut().poll(cx) {
        Poll::Ready(value) => Poll::Ready(value),
        Poll::Pending if matches!(backend, support::Backend::Junction) => Poll::Pending,
        Poll::Pending => panic!("opening error did not resolve on first poll"),
    }).await
}

support::both_backends!(tcp_slots_full_on_first_poll_and_drop_releases_slot_conduit, tcp_slots_full_on_first_poll_and_drop_releases_slot_junction, |backend| {
    with_eal(|eal| {
        let mut mixed = Mixed::with_sockets(eal, ["ofsa", "ofsb"], 1, 1, backend);
        let held = mixed.handle().tcp_accept(tcp_config(), local(LOCAL_PORT));
        let failed = mixed.handle().tcp_connect(tcp_config(), peer_endpoint(PEER_PORT), local(LOCAL_PORT + 1));
        let slot = mixed.spawn(first_poll(failed, backend));
        mixed.rt.poll(0).unwrap();
        assert!(matches!(mixed.wait(&slot), Err(Error::SocketsFull)));
        drop(held);
        // Junction applies the drop asynchronously; a pending replacement proves no release.
        if matches!(backend, support::Backend::Conduit) {
            let replacement = mixed.handle().tcp_accept(tcp_config(), local(LOCAL_PORT));
            let slot = mixed.spawn(async move {
                let mut replacement = pin!(replacement);
                poll_fn(|cx| Poll::Ready(matches!(replacement.as_mut().poll(cx), Poll::Pending))).await
            });
            mixed.rt.poll(0).unwrap();
            assert!(slot.take(), "failed add leaked or drop retained the only TCP slot");
        }
        mixed.close();
    });
});

support::both_backends!(udp_port_in_use_on_first_poll_and_release_reopens_port_conduit, udp_port_in_use_on_first_poll_and_release_reopens_port_junction, |backend| {
    with_eal(|eal| {
        let mut mixed = Mixed::with_sockets(eal, ["ofua", "ofub"], 1, 2, backend);
        let held = support::bind(&mut mixed, udp_config(7000));
        let failed = mixed.handle().udp_bind(udp_config(7000));
        let slot = mixed.spawn(first_poll(failed, backend));
        mixed.rt.poll(0).unwrap();
        assert!(matches!(mixed.wait(&slot), Err(Error::PortInUse)));
        drop(held);
        // Immediate same-port rebind races the host's asynchronous close on junction.
        if matches!(backend, support::Backend::Conduit) {
            let first = support::bind(&mut mixed, udp_config(7000));
            let second = support::bind(&mut mixed, udp_config(7001));
            drop((first, second));
        }
        mixed.close();
    });
});

support::both_backends!(failed_connect_start_releases_only_socket_slot_conduit, failed_connect_start_releases_only_socket_slot_junction, |backend| {
    with_eal(|eal| {
        let mut mixed = Mixed::with_sockets(eal, ["ofca", "ofcb"], 1, 1, backend);
        let unowned = switchnet::Ipv4Address::new(10, 0, 0, 99);
        let failed = mixed.handle().tcp_connect(tcp_config(), peer_endpoint(PEER_PORT), local_at(unowned, LOCAL_PORT));
        let slot = mixed.spawn(first_poll(failed, backend));
        mixed.rt.poll(0).unwrap();
        assert!(matches!(mixed.wait(&slot), Err(Error::Unaddressable)));
        let accepting = mixed.handle().tcp_accept(tcp_config(), local(LOCAL_PORT));
        let slot = mixed.spawn(accepting);
        let peer = mixed.peer.add_tcp(tcp_config()).unwrap();
        mixed.peer.connect(&peer, IpEndpoint::new(net_ip().into(), LOCAL_PORT), PEER_PORT).unwrap();
        let stream = mixed.wait(&slot).expect("failed start leaked the only TCP slot");
        drop((stream, peer));
        mixed.close();
    });
});

support::both_backends!(accepting_error_is_ready_on_first_poll_and_releases_socket_conduit, accepting_error_is_ready_on_first_poll_and_releases_socket_junction, |backend| {
    with_eal(|eal| {
        let mut mixed = Mixed::with_sockets(eal, ["oaea", "oaeb"], 1, 1, backend);
        let failed = mixed.handle().tcp_accept(tcp_config(), local(0));
        let slot = mixed.spawn(first_poll(failed, backend));
        mixed.rt.poll(0).unwrap();
        assert!(matches!(mixed.wait(&slot), Err(Error::Unaddressable)));
        let accepting = mixed.handle().tcp_accept(tcp_config(), local(LOCAL_PORT));
        let slot = mixed.spawn(accepting);
        let peer = mixed.peer.add_tcp(tcp_config()).unwrap();
        mixed.peer.connect(&peer, IpEndpoint::new(net_ip().into(), LOCAL_PORT), PEER_PORT).unwrap();
        let stream = mixed.wait(&slot).expect("failed accept kept the only TCP slot");
        drop((stream, peer));
        mixed.close();
    });
});

// Conduit-only: immediate same-port rebind races junction's asynchronous host close.
#[zhida_test_support::pinned_test]
fn dropping_unpolled_binding_releases_udp_port() {
    let backend = support::Backend::Conduit;
    with_eal(|eal| {
        let mut mixed = Mixed::with_sockets(eal, ["odba", "odbb"], 1, 2, backend);
        let binding = mixed.handle().udp_bind(udp_config(7000));
        drop(binding);
        let socket = support::bind(&mut mixed, udp_config(7000));
        drop(socket);
        mixed.close();
    });
}

fn assert_cached_mss_after_reset(mixed: &mut Mixed, stream: switchnet::TcpStream, peer: dpdk_smoltcp::TcpHandle) {
    let (mut rx, tx) = stream.split();
    assert_eq!(tx.max_segment_payload(), Some(700));
    mixed.until(|m| m.peer.tcp(&peer).state() == tcp::State::Established);
    mixed.peer.abort(&peer);
    mixed.peer.turn(mixed.now, BUDGET).unwrap();
    mixed.to_net.forward();
    mixed.turn();
    let slot = mixed.spawn(async move {
        assert!(matches!(rx.recv().await, Err(Error::InvalidState)));
        rx
    });
    let rx = mixed.wait(&slot);
    assert_eq!(tx.max_segment_payload(), Some(700), "reset must not discard cached negotiated MSS");
    drop((rx, tx, peer));
}

support::both_backends!(connecting_caches_peer_negotiated_mss_across_reset_conduit, connecting_caches_peer_negotiated_mss_across_reset_junction, |backend| {
    with_eal(|eal| {
        let mut mixed = Mixed::new(eal, ["omca", "omcb"], backend);
        let peer = mixed.peer.add_tcp(tcp::Config { mss_cap: Some(700), ..tcp_config() }).unwrap();
        mixed.peer.listen(&peer, PEER_PORT).unwrap();
        let opening = mixed.handle().tcp_connect(tcp_config(), peer_endpoint(PEER_PORT), local(LOCAL_PORT));
        let slot = mixed.spawn(opening);
        let stream = mixed.wait(&slot).unwrap();
        assert_cached_mss_after_reset(&mut mixed, stream, peer);
        mixed.close();
    });
});

support::both_backends!(accepting_caches_peer_negotiated_mss_across_reset_conduit, accepting_caches_peer_negotiated_mss_across_reset_junction, |backend| {
    with_eal(|eal| {
        let mut mixed = Mixed::new(eal, ["omaa", "omab"], backend);
        let opening = mixed.handle().tcp_accept(tcp_config(), local(LOCAL_PORT));
        let slot = mixed.spawn(opening);
        let peer = mixed.peer.add_tcp(tcp::Config { mss_cap: Some(700), ..tcp_config() }).unwrap();
        mixed.peer.connect(&peer, IpEndpoint::new(net_ip().into(), LOCAL_PORT), PEER_PORT).unwrap();
        let stream = mixed.wait(&slot).unwrap();
        assert_cached_mss_after_reset(&mut mixed, stream, peer);
        mixed.close();
    });
});

support::both_backends!(accepting_already_closed_preserves_queued_data_without_mss_conduit, accepting_already_closed_preserves_queued_data_without_mss_junction, |backend| {
    with_eal(|eal| {
        let mut mixed = Mixed::new(eal, ["ocaa", "ocab"], backend);
        let opening = mixed.handle().tcp_accept(tcp_config(), local(LOCAL_PORT));
        let peer = mixed.peer.add_tcp(tcp_config()).unwrap();
        mixed.peer.connect(&peer, IpEndpoint::new(net_ip().into(), LOCAL_PORT), PEER_PORT).unwrap();
        // No accept task exists while the connection opens, receives data, and closes.
        mixed.until(|m| m.peer.tcp(&peer).state() == tcp::State::Established);
        support::peer_send(&mut mixed, &peer, b"before reset");
        mixed.until(|m| m.peer.tcp(&peer).send_queue() == 0);
        mixed.peer.abort(&peer);
        mixed.peer.turn(mixed.now, BUDGET).unwrap();
        assert!(mixed.to_net.forward() >= 1, "reset did not leave the peer");
        mixed.turn();
        if matches!(backend, support::Backend::Junction) {
            mixed.until(|m| m.net.tcp_reset_by_peer());
        }
        let slot = mixed.spawn(first_poll(opening, backend));
        mixed.rt.poll(0).unwrap();
        let stream = mixed.wait(&slot).expect("accept must preserve an already closed stream");
        let (mut rx, tx) = stream.split();
        assert_eq!(tx.max_segment_payload(), None);
        let slot = mixed.spawn(async move {
            let frame = rx.recv().await.expect("queued data survives reset");
            assert_eq!(frame.payload(), b"before reset");
            assert!(matches!(rx.recv().await, Err(Error::InvalidState)));
            (rx, frame)
        });
        let (rx, frame) = mixed.wait(&slot);
        drop((rx, tx, frame, peer));
        mixed.close();
    });
});

#[zhida_test_support::pinned_test]
fn remote_teardown_rejects_retained_tx_and_rx_frames() {
    with_eal(|eal| {
        let mut mixed = Mixed::new(eal, ["olka", "olkb"], support::Backend::Junction);
        let handle = mixed.handle();
        let slot = mixed.spawn(async move { handle.tx_frame().await });
        let frame = mixed.wait(&slot);
        assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            mixed.net.assert_remote_frames_returned();
        })).is_err(), "a retained TX frame escaped the teardown check");
        drop(frame);
        mixed.net.assert_remote_frames_returned();

        let (mut stream, peer) = support::connect(&mut mixed, LOCAL_PORT);
        support::peer_send(&mut mixed, &peer, b"retained");
        let slot = mixed.spawn(async move {
            let frame = stream.recv().await.unwrap();
            (stream, frame)
        });
        let (stream, frame) = mixed.wait(&slot);
        assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            mixed.net.assert_remote_frames_returned();
        })).is_err(), "a retained RX frame escaped the teardown check");
        drop(frame);
        mixed.net.assert_remote_frames_returned();
        drop((stream, peer));
        mixed.close();
    });
}
