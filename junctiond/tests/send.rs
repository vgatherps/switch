use std::net::{Ipv4Addr, SocketAddrV4};
use std::time::Instant as WallInstant;

use dpdk_smoltcp::TcpHandle;
use dpdk_smoltcp::smoltcp::{socket::tcp::State, wire::IpEndpoint};
use junction::control::{Event, Op, SocketId};
use junction::send::{TCP_SEND_HEADER, UDP_SEND_HEADER};
use junction::session::Session;
use zhida_test_kit::with_eal;

mod support;
use support::{
    Fixture, LOCAL_PORT, PEER_PORT, WAIT, connect, local, net_ip, peer_endpoint, peer_tcp_config,
    tcp_config, udp_config,
};

fn queued_send(
    f: &mut Fixture,
    client: &mut Session,
    events: &mut Vec<(SocketId, Event)>,
) -> (SocketId, TcpHandle, Vec<u8>) {
    let peer = f.peer.add_tcp(peer_tcp_config()).unwrap();
    f.peer.listen(&peer, PEER_PORT).unwrap();
    let mut config = tcp_config();
    config.tx_capacity = 1;
    let id = client
        .connect_tcp(config, peer_endpoint(PEER_PORT), local(LOCAL_PORT))
        .unwrap();
    f.until(client, events, |f, events| {
        events.contains(&(id, Event::Connected)) && f.peer.tcp(&peer).state() == State::Established
    });

    // The first iteration can accept one byte; all remaining bytes must backlog.
    // Queue the terminal operation before that iteration, so ACKs cannot drain it first.
    let payload: Vec<u8> = (0..4096).map(|n| (n % 251) as u8).collect();
    let mut record = [0; TCP_SEND_HEADER + 240];
    for chunk in payload.chunks(240) {
        record[TCP_SEND_HEADER..TCP_SEND_HEADER + chunk.len()].copy_from_slice(chunk);
        client
            .send_tcp(id, &mut record[..TCP_SEND_HEADER + chunk.len()])
            .unwrap();
    }
    (id, peer, payload)
}

fn receive_all_then_fin(
    f: &mut Fixture,
    client: &mut Session,
    events: &mut Vec<(SocketId, Event)>,
    peer: &TcpHandle,
    expected: &[u8],
) {
    let mut received = Vec::new();
    f.until(client, events, |f, _| {
        assert!(
            !f.peer.tcp(peer).reset_by_peer(),
            "peer received RST, not FIN"
        );
        while f.peer.tcp(peer).can_recv() {
            let frame = f.peer.tcp_mut(peer).recv().unwrap().unwrap();
            received.extend_from_slice(frame.payload());
        }
        f.peer.tcp(peer).state() == State::CloseWait
    });
    assert_eq!(
        received, expected,
        "FIN arrived before every committed byte"
    );
    assert!(!f.peer.tcp(peer).reset_by_peer());
}

fn assert_another_bind_works(
    f: &mut Fixture,
    client: &mut Session,
    events: &mut Vec<(SocketId, Event)>,
) {
    assert!(f.protocol_errors.is_empty(), "send ended the session");
    let id = client.bind_udp(udp_config(LOCAL_PORT + 1)).unwrap();
    f.until(client, events, |_, events| {
        events.contains(&(id, Event::Bound))
    });
    assert!(f.protocol_errors.is_empty(), "send ended the session");
}

#[zhida_test_support::pinned_test]
fn tcp_plain_close_drains_backlog_before_fin_and_freed() {
    with_eal(|eal| {
        let mut f = Fixture::new(eal, ["jspca", "jspcb"], 2, 0, 256);
        let mut client = f.client(1);
        let mut events = Vec::new();
        let (id, peer, payload) = queued_send(&mut f, &mut client, &mut events);
        client.close(id, false);
        assert_eq!(f.wait_op(id), Op::Close { id, unread: false });
        f.round(&mut client, &mut events);
        assert!(!events.contains(&(id, Event::Freed)));
        assert_eq!(f.peer.tcp(&peer).state(), State::Established);

        receive_all_then_fin(&mut f, &mut client, &mut events, &peer, &payload);
        f.until(&mut client, &mut events, |_, events| {
            events.contains(&(id, Event::Freed))
        });
        f.peer.remove_tcp(peer);
        drop(client);
        f.close();
    });
}

#[zhida_test_support::pinned_test]
fn tcp_shutdown_drains_backlog_before_fin() {
    with_eal(|eal| {
        let mut f = Fixture::new(eal, ["jssda", "jssdb"], 2, 0, 256);
        let mut client = f.client(1);
        let mut events = Vec::new();
        let (id, peer, payload) = queued_send(&mut f, &mut client, &mut events);
        client.shutdown(id);
        assert_eq!(f.wait_op(id), Op::ShutdownTcp { id });
        f.round(&mut client, &mut events);
        assert_eq!(f.peer.tcp(&peer).state(), State::Established);
        assert!(!events.contains(&(id, Event::Delivered)));

        receive_all_then_fin(&mut f, &mut client, &mut events, &peer, &payload);
        f.until(&mut client, &mut events, |_, events| {
            events.contains(&(id, Event::Delivered))
        });
        client.close(id, false);
        f.until(&mut client, &mut events, |_, events| {
            events.contains(&(id, Event::Freed))
        });
        f.peer.remove_tcp(peer);
        drop(client);
        f.close();
    });
}

#[zhida_test_support::pinned_test]
fn tcp_send_after_peer_reset_drops_without_ending_session() {
    with_eal(|eal| {
        let mut f = Fixture::new(eal, ["jsrsa", "jsrsb"], 2, 1, 64);
        let mut client = f.client(2);
        let mut events = Vec::new();
        let (id, peer) = connect(&mut f, &mut client, &mut events, LOCAL_PORT);
        f.peer.abort(&peer);
        f.until(&mut client, &mut events, |_, events| {
            events.contains(&(
                id,
                Event::Closed {
                    reset_by_peer: true,
                },
            ))
        });
        let mut record = [0; TCP_SEND_HEADER + 5];
        record[TCP_SEND_HEADER..].copy_from_slice(b"reset");
        client.send_tcp(id, &mut record).unwrap();
        f.round(&mut client, &mut events);
        assert_another_bind_works(&mut f, &mut client, &mut events);
        f.peer.remove_tcp(peer);
        drop(client);
        f.close();
    });
}

#[zhida_test_support::pinned_test]
fn tcp_send_in_flight_after_abort_drops_without_ending_session() {
    with_eal(|eal| {
        let mut f = Fixture::new(eal, ["jsaba", "jsabb"], 2, 1, 64);
        let mut client = f.client(1);
        let mut events = Vec::new();
        let (id, peer) = connect(&mut f, &mut client, &mut events, LOCAL_PORT);
        let stream = f.streams[0];
        client.abort(id);
        assert_eq!(f.wait_op(id), Op::AbortTcp { id });
        // Abort is queued to apply before the FIFO; bypass Session's closing check
        // to reproduce a send committed while the abort is still in flight.
        f.raw_tcp_send(stream, id, 0, b"in flight");
        f.round(&mut client, &mut events);
        f.until(&mut client, &mut events, |_, events| {
            events.contains(&(id, Event::Freed))
        });
        assert_another_bind_works(&mut f, &mut client, &mut events);
        assert!(!f.retired.contains(&stream));
        f.peer.remove_tcp(peer);
        drop(client);
        f.close();
    });
}

#[zhida_test_support::pinned_test]
fn stale_send_after_abort_and_slot_reuse_does_not_end_session() {
    with_eal(|eal| {
        let mut f = Fixture::new(eal, ["jsgna", "jsgnb"], 2, 0, 1);
        let mut client = f.client(1);
        let mut events = Vec::new();
        let (id, peer) = connect(&mut f, &mut client, &mut events, LOCAL_PORT);
        let mut record = [0; TCP_SEND_HEADER + 3];
        record[TCP_SEND_HEADER..].copy_from_slice(b"old");
        client.send_tcp(id, &mut record).unwrap();
        client.send_tcp(id, &mut record).unwrap();
        client.abort(id);
        assert_eq!(f.wait_op(id), Op::AbortTcp { id });
        // Apply abort and consume only the first SEND. Wait for Freed without
        // another network iteration, leaving the second SEND behind the new open.
        f.round(&mut client, &mut events);
        let deadline = WallInstant::now() + WAIT;
        while !events.contains(&(id, Event::Freed)) {
            assert!(WallInstant::now() < deadline, "Freed did not arrive");
            client
                .poll_events(|id, event| events.push((id, event)))
                .unwrap();
            std::thread::yield_now();
        }
        let next_peer = f.peer.add_tcp(peer_tcp_config()).unwrap();
        f.peer.listen(&next_peer, PEER_PORT + 1).unwrap();
        let next = client
            .connect_tcp(
                tcp_config(),
                peer_endpoint(PEER_PORT + 1),
                local(LOCAL_PORT + 1),
            )
            .unwrap();
        assert_eq!(next.slot, id.slot);
        assert_eq!(next.generation, id.generation.wrapping_add(1));
        assert!(matches!(f.wait_op(next), Op::ConnectTcp { .. }));
        f.until(&mut client, &mut events, |f, events| {
            events.contains(&(next, Event::Connected))
                && f.peer.tcp(&next_peer).state() == State::Established
        });
        record[TCP_SEND_HEADER..].copy_from_slice(b"new");
        client.send_tcp(next, &mut record).unwrap();
        f.until(&mut client, &mut events, |f, _| {
            f.peer.tcp(&next_peer).can_recv()
        });
        let frame = f.peer.tcp_mut(&next_peer).recv().unwrap().unwrap();
        assert_eq!(frame.payload(), b"new");
        drop(frame);
        assert!(f.protocol_errors.is_empty());
        f.peer.remove_tcp(peer);
        f.peer.remove_tcp(next_peer);
        drop(client);
        f.close();
    });
}

#[zhida_test_support::pinned_test]
fn abort_discards_backlog_resets_peer_and_reuses_host_index() {
    with_eal(|eal| {
        let mut f = Fixture::new(eal, ["jsbga", "jsbgb"], 2, 0, 256);
        let mut client = f.client(1);
        let mut events = Vec::new();
        let (id, peer, payload) = queued_send(&mut f, &mut client, &mut events);
        // Fill smoltcp's one-byte TX queue and create the host backlog.
        f.round(&mut client, &mut events);
        client.abort(id);
        assert_eq!(f.wait_op(id), Op::AbortTcp { id });
        let mut received = Vec::new();
        f.until(&mut client, &mut events, |f, events| {
            while f.peer.tcp(&peer).can_recv() {
                let frame = f.peer.tcp_mut(&peer).recv().unwrap().unwrap();
                received.extend_from_slice(frame.payload());
            }
            f.peer.tcp(&peer).reset_by_peer() && events.contains(&(id, Event::Freed))
        });
        assert!(received.len() < payload.len());
        assert_eq!(received, payload[..received.len()]);
        assert_eq!(f.peer.tcp(&peer).state(), State::Closed);
        for _ in 0..4 {
            f.round(&mut client, &mut events);
            assert_eq!(
                f.peer.tcp(&peer).recv_queue(),
                0,
                "bytes arrived after reset"
            );
        }
        f.peer.remove_tcp(peer);
        let (next, next_peer) = connect(&mut f, &mut client, &mut events, LOCAL_PORT + 1);
        assert_eq!(next.slot, id.slot);
        let mut record = [0; TCP_SEND_HEADER + 5];
        record[TCP_SEND_HEADER..].copy_from_slice(b"alive");
        client.send_tcp(next, &mut record).unwrap();
        f.until(&mut client, &mut events, |f, _| {
            f.peer.tcp(&next_peer).can_recv()
        });
        let frame = f.peer.tcp_mut(&next_peer).recv().unwrap().unwrap();
        assert_eq!(frame.payload(), b"alive");
        drop(frame);
        assert!(f.protocol_errors.is_empty());
        f.peer.remove_tcp(next_peer);
        drop(client);
        f.close();
    });
}

#[zhida_test_support::pinned_test]
fn finish_acknowledges_before_backlog_drains_then_fin() {
    with_eal(|eal| {
        let mut f = Fixture::new(eal, ["jsfna", "jsfnb"], 2, 0, 256);
        let mut client = f.client(1);
        let mut events = Vec::new();
        let (_, peer, payload) = queued_send(&mut f, &mut client, &mut events);
        let stream = f.streams[0];
        client.finish().unwrap();
        let deadline = WallInstant::now() + WAIT;
        while !client.closed() {
            assert!(
                WallInstant::now() < deadline,
                "finish waited for the send backlog"
            );
            f.round(&mut client, &mut events);
        }
        assert!(f.retired.contains(&stream));
        assert_eq!(f.peer.tcp(&peer).state(), State::Established);
        assert!(f.peer.tcp(&peer).recv_queue() < payload.len());
        receive_all_then_fin(&mut f, &mut client, &mut events, &peer, &payload);
        f.peer.remove_tcp(peer);
        drop(client);
        f.close();
    });
}

#[zhida_test_support::pinned_test]
fn udp_send_delivers_destination_and_payload_and_counts_unresolved_arp_drop() {
    with_eal(|eal| {
        let mut f = Fixture::new(eal, ["jsuda", "jsudb"], 2, 2, 64);
        let mut client = f.client(2);
        let mut events = Vec::new();
        // Complete a real handshake so the successful UDP send has a resolved ARP entry.
        let (_, tcp_peer) = connect(&mut f, &mut client, &mut events, LOCAL_PORT);
        let peer_config = dpdk_smoltcp::UdpConfig {
            port: PEER_PORT + 1,
            local_addr: None,
            rx_queue: 8,
            tx_checksum: true,
            broadcast: false,
        };
        let peer = f.peer.add_udp(peer_config).unwrap();
        let decoy = f
            .peer
            .add_udp(dpdk_smoltcp::UdpConfig {
                port: PEER_PORT + 2,
                ..peer_config
            })
            .unwrap();
        let id = client.bind_udp(udp_config(LOCAL_PORT + 1)).unwrap();
        f.until(&mut client, &mut events, |_, events| {
            events.contains(&(id, Event::Bound))
        });
        let mut record = [0; UDP_SEND_HEADER + 8];
        record[UDP_SEND_HEADER..].copy_from_slice(b"datagram");
        client
            .send_udp(id, peer_endpoint(PEER_PORT + 1), &mut record)
            .unwrap();
        f.until(&mut client, &mut events, |f, _| {
            f.peer.udp(&peer).rx_queued() == 1
        });
        let datagram = f.peer.udp_mut(&peer).recv().unwrap();
        assert_eq!(
            datagram.src(),
            IpEndpoint::new(net_ip().into(), LOCAL_PORT + 1)
        );
        assert_eq!(datagram.dst_addr(), zhida_test_kit::pair::Side::B.ip());
        assert_eq!(datagram.payload(), b"datagram");
        drop(datagram);
        assert_eq!(f.peer.udp(&decoy).rx_queued(), 0);
        assert_eq!(f.network.udp_dropped(), 0);

        client
            .send_udp(
                id,
                SocketAddrV4::new(Ipv4Addr::new(10, 0, 0, 99), PEER_PORT + 1),
                &mut record,
            )
            .unwrap();
        f.round(&mut client, &mut events);
        assert_eq!(f.network.udp_dropped(), 1);
        for _ in 0..4 {
            f.round(&mut client, &mut events);
        }
        assert_eq!(f.network.udp_dropped(), 1);
        assert_eq!(f.peer.udp(&peer).rx_queued(), 0);
        assert_eq!(f.peer.udp(&decoy).rx_queued(), 0);
        f.peer.remove_udp(peer);
        f.peer.remove_udp(decoy);
        f.peer.remove_tcp(tcp_peer);
        drop(client);
        f.close();
    });
}
