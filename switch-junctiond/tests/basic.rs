use std::net::Ipv4Addr;
use std::time::Instant as WallInstant;

use ceph_ring::Writer;
use dpdk_smoltcp::smoltcp::{socket::tcp::State, wire::IpEndpoint};
use switch_junction::control::{EndReason, Event, ListenEndpoint, Op, RejectCode};
use switch_junction::session::Lost;
use switch_test_kit::with_eal;

mod support;
use support::{
    Fixture, LOCAL_PORT, PEER_PORT, WAIT, connect, local, net_ip, peer_endpoint, peer_tcp_config,
    tcp_config, udp_config,
};

#[switch_test_support::pinned_test]
fn connected_reports_peer_negotiated_mss() {
    with_eal(|eal| {
        for accept in [false, true] {
            let mut f = Fixture::new(eal, ["jnmsa", "jnmsb"], 4, 4, 64);
            let mut client = f.client(4);
            assert_eq!(client.frame_capacity(), 232);
            let mut config = peer_tcp_config();
            config.mss_cap = Some(536);
            let peer = f.peer.add_tcp(config).unwrap();
            let id = if accept {
                let id = client.listen_tcp(tcp_config(), local(LOCAL_PORT)).unwrap();
                f.wait_op(id);
                f.network.iteration(f.now).unwrap();
                f.peer.connect(&peer, IpEndpoint::new(net_ip().into(), LOCAL_PORT), PEER_PORT).unwrap();
                id
            } else {
                f.peer.listen(&peer, PEER_PORT).unwrap();
                client.connect_tcp(tcp_config(), peer_endpoint(PEER_PORT), local(LOCAL_PORT)).unwrap()
            };
            let mut events = Vec::new();
            f.until(&mut client, &mut events, |_, events| {
                events.iter().any(|(socket, event)| *socket == id && matches!(event, Event::Connected { .. }))
            });
            assert!(events.contains(&(id, Event::Connected { mss: 536 })), "{events:?}");
            f.peer.remove_tcp(peer);
            drop(client);
            f.close();
        }
    });
}

#[test]
fn bind_requires_room_for_frame_and_udp_header() {
    use std::{io, sync::mpsc, time::Duration};
    use switch_junction::host::{ControlChannels, ControlConfig, HostControl};
    let path = std::env::temp_dir().join(format!("junction-bound-{}.sock", std::process::id()));
    std::fs::write(&path, b"unchanged").unwrap();
    for (frame_capacity, max_record) in [(232, 255), (u32::MAX, u32::MAX), (u32::MAX - 23, u32::MAX)] {
        let (to_network, _network) = mpsc::channel();
        let (to_fifo, _fifo) = mpsc::channel();
        let (_events, from_network) = mpsc::channel();
        let (_retire, retire) = mpsc::channel();
        let result = HostControl::bind(ControlConfig {
            socket_path: path.clone(),
            ingress: String::new(),
            rx_dir: std::env::temp_dir(),
            rx_size: 64 * 1024,
            frame_capacity,
            max_record,
            max_sessions: 1,
            max_sockets: 1,
            poll_ms: 1,
            hello_timeout: Duration::from_secs(1),
        }, ControlChannels { to_network, to_fifo, from_network, retire });
        match result {
            Err(error) => assert_eq!(error.kind(), io::ErrorKind::InvalidInput),
            Ok(_) => panic!("accepted frame_capacity={frame_capacity}, max_record={max_record}"),
        }
        assert_eq!(std::fs::read(&path).unwrap(), b"unchanged");
    }
    std::fs::remove_file(path).unwrap();
}

#[switch_test_support::pinned_test]
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
            events.contains(&(id, Event::Connected { mss: 1460 }))
                && f.peer.tcp(&peer).state() == State::Established
        });
        f.peer.remove_tcp(peer);
        drop(client);
        f.close();
    });
}

#[switch_test_support::pinned_test]
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
        assert!(!events.iter().any(|(_, event)| matches!(event, Event::Connected { .. })));
        client.close(id, false);
        f.until(&mut client, &mut events, |_, events| {
            events.contains(&(id, Event::Freed))
        });
        drop(client);
        f.close();
    });
}

#[switch_test_support::pinned_test]
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
        assert_ne!(reused.generation, rejected.generation);
        f.until(&mut client, &mut events, |_, events| {
            events.contains(&(reused, Event::Bound))
        });
        drop(client);
        f.close();
    });
}

#[switch_test_support::pinned_test]
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
        assert_ne!(reused.generation, rejected.generation);
        f.peer.remove_tcp(peer);
        drop((client, owner));
        f.close();
    });
}

#[switch_test_support::pinned_test]
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

#[switch_test_support::pinned_test]
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

#[switch_test_support::pinned_test]
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
        assert_ne!(reused.generation, rejected.generation);
        assert!(!events.contains(&(rejected, Event::Freed)));
        f.peer.remove_tcp(peer);
        drop(client);
        f.close();
    });
}
