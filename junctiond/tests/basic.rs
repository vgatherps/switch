use std::net::Ipv4Addr;
use std::time::Instant as WallInstant;

use ceph_ring::Writer;
use dpdk_smoltcp::smoltcp::{socket::tcp::State, wire::IpEndpoint};
use junction::control::{EndReason, Event, ListenEndpoint, Op, RejectCode};
use junction::session::Lost;
use zhida_test_kit::with_eal;

mod support;
use support::{
    Fixture, LOCAL_PORT, PEER_PORT, WAIT, connect, local, net_ip, peer_endpoint, peer_tcp_config,
    tcp_config, udp_config,
};

#[zhida_test_support::pinned_test]
fn peer_connects_to_client_listener() {
    with_eal(|eal| {
        let mut f = Fixture::new(eal, ["jnlna", "jnlnb"], 4, 4, 64);
        let mut client = f.client(4);
        let mut events = Vec::new();
        let id = client.listen_tcp(tcp_config(), local(LOCAL_PORT)).unwrap();
        assert!(matches!(f.wait_op(id), Op::ListenTcp { .. }));
        f.network.iteration(f.now).unwrap();
        let peer = f.peer.add_tcp(peer_tcp_config()).unwrap();
        f.peer
            .connect(
                &peer,
                IpEndpoint::new(net_ip().into(), LOCAL_PORT),
                PEER_PORT,
            )
            .unwrap();
        f.until(&mut client, &mut events, |f, events| {
            events.contains(&(id, Event::Connected))
                && f.peer.tcp(&peer).state() == State::Established
        });
        f.peer.remove_tcp(peer);
        drop(client);
        f.close();
    });
}

#[zhida_test_support::pinned_test]
fn refused_connect_reports_peer_reset() {
    with_eal(|eal| {
        let mut f = Fixture::new(eal, ["jnrfa", "jnrfb"], 4, 4, 64);
        let mut client = f.client(4);
        let mut events = Vec::new();
        let id = client
            .connect_tcp(tcp_config(), peer_endpoint(PEER_PORT), local(LOCAL_PORT))
            .unwrap();
        f.until(&mut client, &mut events, |_, events| {
            events.contains(&(
                id,
                Event::Closed {
                    reset_by_peer: true,
                },
            ))
        });
        assert!(!events.contains(&(id, Event::Connected)));
        client.close(id, false);
        f.until(&mut client, &mut events, |_, events| {
            events.contains(&(id, Event::Freed))
        });
        drop(client);
        f.close();
    });
}

#[zhida_test_support::pinned_test]
fn udp_port_collision_rejects_and_reuses_client_slot() {
    with_eal(|eal| {
        let mut f = Fixture::new(eal, ["jnupa", "jnupb"], 4, 4, 64);
        let mut client = f.client(2);
        let mut events = Vec::new();
        let first = client.bind_udp(udp_config(LOCAL_PORT)).unwrap();
        f.until(&mut client, &mut events, |_, events| {
            events.contains(&(first, Event::Bound))
        });
        let rejected = client.bind_udp(udp_config(LOCAL_PORT)).unwrap();
        f.until(&mut client, &mut events, |_, events| {
            events.contains(&(rejected, Event::Rejected(RejectCode::UdpPortInUse)))
        });
        let reused = client.bind_udp(udp_config(LOCAL_PORT + 1)).unwrap();
        assert_eq!(reused.slot, rejected.slot);
        assert_ne!(reused.generation, rejected.generation);
        f.until(&mut client, &mut events, |_, events| {
            events.contains(&(reused, Event::Bound))
        });
        drop(client);
        f.close();
    });
}

#[zhida_test_support::pinned_test]
fn unowned_listen_rejection_releases_host_and_client_slots() {
    with_eal(|eal| {
        let mut f = Fixture::new(eal, ["jnula", "jnulb"], 1, 1, 64);
        let mut client = f.client(1);
        let mut events = Vec::new();
        let rejected = client
            .listen_tcp(
                tcp_config(),
                ListenEndpoint {
                    addr: Some(Ipv4Addr::new(192, 0, 2, 1)),
                    port: LOCAL_PORT,
                },
            )
            .unwrap();
        f.until(&mut client, &mut events, |_, events| {
            events.contains(&(rejected, Event::Rejected(RejectCode::ListenUnaddressable)))
        });
        let (reused, peer) = connect(&mut f, &mut client, &mut events, LOCAL_PORT);
        assert_eq!(reused.slot, rejected.slot);
        assert_ne!(reused.generation, rejected.generation);
        f.peer.remove_tcp(peer);
        drop(client);
        f.close();
    });
}

#[zhida_test_support::pinned_test]
fn tcp_full_rejection_can_reuse_slot_after_capacity_frees() {
    with_eal(|eal| {
        let mut f = Fixture::new(eal, ["jntfa", "jntfb"], 1, 1, 64);
        let mut owner = f.client(1);
        let mut client = f.client(1);
        let mut events = Vec::new();
        let occupied = owner.listen_tcp(tcp_config(), local(LOCAL_PORT)).unwrap();
        f.wait_op(occupied);
        f.network.iteration(f.now).unwrap();
        let rejected = client
            .listen_tcp(tcp_config(), local(LOCAL_PORT + 1))
            .unwrap();
        f.until(&mut client, &mut events, |_, events| {
            events.contains(&(rejected, Event::Rejected(RejectCode::TcpSocketsFull)))
        });
        owner.abort(occupied);
        let mut owner_events = Vec::new();
        f.until(&mut owner, &mut owner_events, |_, events| {
            events.contains(&(occupied, Event::Freed))
        });
        let (reused, peer) = connect(&mut f, &mut client, &mut events, LOCAL_PORT + 1);
        assert_eq!(reused.slot, rejected.slot);
        assert_ne!(reused.generation, rejected.generation);
        f.peer.remove_tcp(peer);
        drop((client, owner));
        f.close();
    });
}

#[zhida_test_support::pinned_test]
fn plain_close_and_shutdown_wait_for_fifo_none_not_batch_exhaustion() {
    with_eal(|eal| {
        for shutdown in [false, true] {
            let mut f = Fixture::new(eal, ["jnhla", "jnhlb"], 4, 4, 1);
            let mut client = f.client(1);
            let mut events = Vec::new();
            let (id, peer) = connect(&mut f, &mut client, &mut events, LOCAL_PORT);
            let mut finished_a = f.client(1);
            let mut finished_b = f.client(1);
            finished_a.finish().unwrap();
            finished_b.finish().unwrap();
            if shutdown {
                client.shutdown(id);
            } else {
                client.close(id, false);
            }
            let op = f.wait_op(id);
            assert!(matches!(
                op,
                Op::ShutdownTcp { .. } | Op::Close { unread: false, .. }
            ));
            for _ in 0..2 {
                f.round(&mut client, &mut events);
                assert_eq!(
                    f.peer.tcp(&peer).state(),
                    State::Established,
                    "FIN escaped before FIFO returned None"
                );
                assert!(!events.contains(&(id, Event::Freed)));
            }
            f.round(&mut client, &mut events);
            assert_eq!(
                f.peer.tcp(&peer).state(),
                State::CloseWait,
                "third pass reached None but did not release FIN"
            );
            if !shutdown {
                f.until(&mut client, &mut events, |_, events| {
                    events.contains(&(id, Event::Freed))
                });
            }
            f.peer.remove_tcp(peer);
            drop((client, finished_a, finished_b));
            f.close();
        }
    });
}

#[zhida_test_support::pinned_test]
fn undefined_payload_ends_session_and_resets_peer() {
    with_eal(|eal| {
        let mut f = Fixture::new(eal, ["jnppa", "jnppb"], 4, 4, 64);
        let mut client = f.client(1);
        let mut events = Vec::new();
        let (_, peer) = connect(&mut f, &mut client, &mut events, LOCAL_PORT);
        let stream = f.streams[0];
        let writer = Writer::attach(&f.ingress).unwrap();
        writer
            .try_push(b"no packet kind exists", stream, 0)
            .unwrap();
        let deadline = WallInstant::now() + WAIT;
        loop {
            assert!(
                WallInstant::now() < deadline,
                "protocol error was not delivered"
            );
            f.round_links();
            match client.poll_events(|_, _| {}) {
                Ok(()) => {}
                Err(Lost::Ended(EndReason::ProtocolError)) => break,
                Err(error) => panic!("wrong session loss: {error:?}"),
            }
        }
        while f.peer.tcp(&peer).state() != State::Closed {
            assert!(
                WallInstant::now() < deadline,
                "protocol-error teardown did not reset peer"
            );
            f.round_links();
        }
        assert!(f.peer.tcp(&peer).reset_by_peer());
        f.peer.remove_tcp(peer);
        drop((writer, client));
        f.close();
    });
}

#[zhida_test_support::pinned_test]
fn unread_close_bypasses_fifo_and_resets_immediately() {
    with_eal(|eal| {
        let mut f = Fixture::new(eal, ["jnuca", "jnucb"], 4, 4, 1);
        let mut client = f.client(1);
        let mut events = Vec::new();
        let (id, peer) = connect(&mut f, &mut client, &mut events, LOCAL_PORT);
        let mut finished_a = f.client(1);
        let mut finished_b = f.client(1);
        finished_a.finish().unwrap();
        finished_b.finish().unwrap();
        client.close(id, true);
        assert!(matches!(f.wait_op(id), Op::Close { unread: true, .. }));
        f.round(&mut client, &mut events);
        assert_eq!(f.peer.tcp(&peer).state(), State::Closed);
        assert!(f.peer.tcp(&peer).reset_by_peer());
        f.until(&mut client, &mut events, |_, events| {
            events.contains(&(id, Event::Freed))
        });
        f.peer.remove_tcp(peer);
        drop((client, finished_a, finished_b));
        f.close();
    });
}

#[zhida_test_support::pinned_test]
fn close_racing_rejected_open_is_ignored_and_slot_reusable() {
    with_eal(|eal| {
        let mut f = Fixture::new(eal, ["jnrca", "jnrcb"], 1, 1, 64);
        let mut client = f.client(1);
        let mut events = Vec::new();
        let rejected = client
            .listen_tcp(
                tcp_config(),
                ListenEndpoint {
                    addr: Some(Ipv4Addr::new(192, 0, 2, 1)),
                    port: LOCAL_PORT,
                },
            )
            .unwrap();
        client.close(rejected, false);
        assert!(matches!(f.wait_op(rejected), Op::ListenTcp { .. }));
        assert!(matches!(
            f.wait_op(rejected),
            Op::Close { unread: false, .. }
        ));
        f.until(&mut client, &mut events, |_, events| {
            events.contains(&(rejected, Event::Rejected(RejectCode::ListenUnaddressable)))
        });
        assert!(!events.contains(&(rejected, Event::Freed)));
        let (reused, peer) = connect(&mut f, &mut client, &mut events, LOCAL_PORT);
        assert_eq!(reused.slot, rejected.slot);
        assert_ne!(reused.generation, rejected.generation);
        assert!(!events.contains(&(rejected, Event::Freed)));
        f.peer.remove_tcp(peer);
        drop(client);
        f.close();
    });
}
