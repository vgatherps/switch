use std::time::Instant as WallInstant;

use dpdk_smoltcp::TcpHandle;
use dpdk_smoltcp::smoltcp::{socket::tcp::State, wire::IpEndpoint};
use junction::control::{Event, Op, SocketId};
use junction::rx::{OwnedFrame, PacketMeta, TCP, UDP};
use junction::session::Session;
use zhida_test_kit::with_eal;

mod support;
use support::{
    Fixture, LOCAL_PORT, PEER_PORT, WAIT, local, net_ip, peer_endpoint, peer_listen, tcp_config,
    udp_config,
};

fn connect_receive(
    f: &mut Fixture,
    client: &mut Session,
    events: &mut Vec<(SocketId, Event)>,
) -> (SocketId, TcpHandle) {
    let peer = peer_listen(f);
    let mut config = tcp_config();
    // Keep the whole transfer inside the advertised window, independently of
    // whether dequeuing a native frame advertises additional receive capacity.
    config.rx_capacity = 64 * 1024;
    let id = client
        .connect_tcp(config, peer_endpoint(PEER_PORT), local(LOCAL_PORT))
        .unwrap();
    f.until(client, events, |f, events| {
        events.contains(&(id, Event::Connected { mss: 1460 })) && f.peer.tcp(&peer).state() == State::Established
    });
    (id, peer)
}

fn check_tcp(id: SocketId, expected_id: SocketId, meta: PacketMeta, frame: &OwnedFrame) {
    assert_eq!(id, expected_id);
    assert_eq!(meta.slot, expected_id.slot);
    assert_eq!(meta.generation, expected_id.generation);
    assert_eq!(meta.kind, TCP);
    assert!(!frame.is_empty());
}

fn hold_tcp_until_acked(
    f: &mut Fixture,
    client: &mut Session,
    events: &mut Vec<(SocketId, Event)>,
    id: SocketId,
    peer: &TcpHandle,
    payload: &[u8],
) -> Vec<OwnedFrame> {
    assert_eq!(f.peer.tcp_mut(peer).send_slice(payload), Ok(payload.len()));
    let mut held = Vec::new();
    let deadline = WallInstant::now() + WAIT;
    while f.peer.tcp(peer).send_queue() != 0 {
        assert!(
            WallInstant::now() < deadline,
            "peer data was not acknowledged"
        );
        if !f.round_receive(client, events, |packet_id, meta, frame| {
            check_tcp(packet_id, id, meta, &frame);
            held.push(frame);
        }) {
            f.advance();
        }
    }
    let marker = client.bind_udp(udp_config(LOCAL_PORT + 1)).unwrap();
    while !events.contains(&(marker, Event::Bound)) {
        assert!(WallInstant::now() < deadline, "full-ring barrier timed out");
        f.round_receive(client, events, |packet_id, meta, frame| {
            check_tcp(packet_id, id, meta, &frame);
            held.push(frame);
        });
    }
    let prefix: Vec<u8> = held
        .iter()
        .flat_map(|frame| frame.iter().copied())
        .collect();
    assert!(!prefix.is_empty());
    assert!(prefix.len() < payload.len(), "RX ring did not fill");
    assert_eq!(prefix, payload[..prefix.len()]);
    client.close(marker, false);
    f.until(client, events, |_, events| {
        events.contains(&(marker, Event::Freed))
    });
    held
}

#[zhida_test_support::pinned_test]
fn tcp_held_receive_frames_block_reclaim_and_fin_until_pending_bytes_publish() {
    with_eal(|eal| {
        let mut f = Fixture::with_rx_size(eal, ["jrhfa", "jrhfb"], 2, 1, 16 * 1024);
        let mut client = f.client(2);
        let mut events = Vec::new();
        let (id, peer) = connect_receive(&mut f, &mut client, &mut events);
        let payload: Vec<u8> = (0..32 * 1024).map(|n| (n % 239) as u8).collect();
        assert_eq!(
            f.peer.tcp_mut(&peer).send_slice(&payload),
            Ok(payload.len())
        );
        f.peer.close(&peer);

        let mut held = Vec::new();
        let deadline = WallInstant::now() + WAIT;
        while f.peer.tcp(&peer).state() != State::FinWait2 {
            assert!(
                WallInstant::now() < deadline,
                "peer FIN was not acknowledged"
            );
            if !f.round_receive(&mut client, &mut events, |packet_id, meta, frame| {
                check_tcp(packet_id, id, meta, &frame);
                held.push(frame);
            }) {
                f.advance();
            }
        }
        assert_eq!(f.peer.tcp(&peer).send_queue(), 0);

        // Bound travels through the same real network -> host -> client event
        // channel as PeerFin. It is a barrier, not a timed absence assertion.
        let marker = client.bind_udp(udp_config(LOCAL_PORT + 1)).unwrap();
        while !events.contains(&(marker, Event::Bound)) {
            assert!(WallInstant::now() < deadline, "event barrier timed out");
            f.round_receive(&mut client, &mut events, |packet_id, meta, frame| {
                check_tcp(packet_id, id, meta, &frame);
                held.push(frame);
            });
        }
        assert!(
            !events.contains(&(id, Event::PeerFin)),
            "FIN escaped a full RX ring"
        );
        let mut received = Vec::new();
        for frame in &held {
            received.extend_from_slice(frame);
        }
        assert!(!received.is_empty());
        assert!(
            received.len() < payload.len(),
            "held frames did not fill the RX ring"
        );
        assert_eq!(received, payload[..received.len()]);

        for _ in 0..32 {
            f.round_receive(&mut client, &mut events, |_, _, _| {
                panic!("producer reclaimed records while client frames were held")
            });
        }
        assert!(!events.contains(&(id, Event::PeerFin)));
        assert!(
            held.iter()
                .flat_map(|frame| frame.iter())
                .copied()
                .eq(received.iter().copied())
        );

        drop(held);
        let deadline = WallInstant::now() + WAIT;
        while received.len() < payload.len() || !events.contains(&(id, Event::PeerFin)) {
            assert!(
                WallInstant::now() < deadline,
                "pending RX bytes or FIN were lost"
            );
            if !f.round_receive(&mut client, &mut events, |packet_id, meta, frame| {
                check_tcp(packet_id, id, meta, &frame);
                received.extend_from_slice(&frame);
            }) {
                f.advance();
            }
            assert!(received.len() <= payload.len());
            if events.contains(&(id, Event::PeerFin)) {
                assert_eq!(
                    received.len(),
                    payload.len(),
                    "first poll reporting FIN did not deliver every pending byte"
                );
            }
        }
        assert_eq!(
            received, payload,
            "reserve-failed host frame was not retained"
        );
        f.peer.remove_tcp(peer);
        drop(client);
        f.close();
    });
}

#[zhida_test_support::pinned_test]
fn tcp_last_ack_closes_while_rx_is_held_then_publishes_bytes_before_fin_and_closed() {
    with_eal(|eal| {
        let mut f = Fixture::with_rx_size(eal, ["jrlaa", "jrlab"], 1, 1, 16 * 1024);
        let mut client = f.client(2);
        let mut events = Vec::new();
        let (id, peer) = connect_receive(&mut f, &mut client, &mut events);
        let payload: Vec<u8> = (0..32 * 1024).map(|n| (n % 233) as u8).collect();
        let held = hold_tcp_until_acked(&mut f, &mut client, &mut events, id, &peer, &payload);
        f.peer.close(&peer);
        let deadline = WallInstant::now() + WAIT;
        while f.peer.tcp(&peer).state() != State::FinWait2 {
            assert!(
                WallInstant::now() < deadline,
                "peer FIN was not acknowledged"
            );
            if !f.round_receive(&mut client, &mut events, |_, _, _| {
                panic!("full ring published another frame")
            }) {
                f.advance();
            }
        }
        assert!(!events.contains(&(id, Event::PeerFin)));
        client.shutdown(id);
        assert_eq!(f.wait_op(id), Op::ShutdownTcp { id });
        while f.peer.tcp(&peer).state() != State::TimeWait {
            assert!(WallInstant::now() < deadline, "peer did not ACK local FIN");
            if !f.round_receive(&mut client, &mut events, |_, _, _| {
                panic!("full ring published another frame")
            }) {
                f.advance();
            }
        }
        // TimeWait proves the peer received our FIN and forwarded its ACK. The
        // next network turn consumes that ACK before this ordered event fence.
        f.round_receive(&mut client, &mut events, |_, _, _| {
            panic!("full ring published another frame")
        });
        let marker = client.bind_udp(udp_config(LOCAL_PORT + 1)).unwrap();
        while !events.contains(&(marker, Event::Bound)) {
            assert!(WallInstant::now() < deadline, "ACK event barrier timed out");
            f.round_receive(&mut client, &mut events, |_, _, _| {
                panic!("full ring published another frame")
            });
        }
        assert!(!events.iter().any(|(socket, event)| {
            *socket == id && matches!(event, Event::PeerFin | Event::Closed { .. })
        }));
        let mut received: Vec<u8> = held
            .iter()
            .flat_map(|frame| frame.iter().copied())
            .collect();
        drop(held);
        let closed = Event::Closed {
            reset_by_peer: false,
        };
        let deadline = WallInstant::now() + WAIT;
        while !events.contains(&(id, closed)) {
            assert!(
                WallInstant::now() < deadline,
                "closed TCP lost pending RX or EOF"
            );
            f.round_receive(&mut client, &mut events, |packet_id, meta, frame| {
                check_tcp(packet_id, id, meta, &frame);
                received.extend_from_slice(&frame);
            });
            if events.contains(&(id, Event::PeerFin)) || events.contains(&(id, closed)) {
                assert_eq!(received, payload, "EOF preceded the last byte");
            }
        }
        assert_eq!(received, payload);
        assert_eq!(
            events
                .iter()
                .filter_map(|(socket, event)| {
                    (*socket == id && matches!(event, Event::PeerFin | Event::Closed { .. }))
                        .then_some(*event)
                })
                .collect::<Vec<_>>(),
            [Event::PeerFin, closed]
        );
        f.peer.remove_tcp(peer);
        drop(client);
        f.close();
    });
}

#[zhida_test_support::pinned_test]
fn tcp_peer_reset_closes_without_releasing_full_rx_ring_and_discards_pending_bytes() {
    with_eal(|eal| {
        let mut f = Fixture::with_rx_size(eal, ["jrrsa", "jrrsb"], 1, 1, 16 * 1024);
        let mut client = f.client(2);
        let mut events = Vec::new();
        let (id, peer) = connect_receive(&mut f, &mut client, &mut events);
        let payload: Vec<u8> = (0..32 * 1024).map(|n| (n % 229) as u8).collect();
        let held = hold_tcp_until_acked(&mut f, &mut client, &mut events, id, &peer, &payload);
        let held_bytes = held
            .iter()
            .map(|frame| frame.len())
            .sum::<usize>();
        f.peer.abort(&peer);
        let closed = Event::Closed {
            reset_by_peer: true,
        };
        let deadline = WallInstant::now() + WAIT;
        while !events.contains(&(id, closed)) {
            assert!(
                WallInstant::now() < deadline,
                "reset waited for client RX release"
            );
            if !f.round_receive(&mut client, &mut events, |_, _, _| {
                panic!("pending bytes published after peer reset")
            }) {
                f.advance();
            }
        }
        assert_eq!(
            held.iter()
                .flat_map(|frame| frame.iter().copied())
                .collect::<Vec<_>>(),
            payload[..held_bytes]
        );
        assert!(!events.contains(&(id, Event::PeerFin)));
        drop(held);
        client
            .poll(
                |socket, event| events.push((socket, event)),
                |_, _, _| panic!("packet delivered after reset Closed"),
            )
            .unwrap();
        let marker = client.bind_udp(udp_config(LOCAL_PORT + 1)).unwrap();
        while !events.contains(&(marker, Event::Bound)) {
            assert!(
                WallInstant::now() < deadline,
                "post-reset event barrier timed out"
            );
            f.round_receive(&mut client, &mut events, |_, _, _| {
                panic!("released ring received stale bytes after reset Closed")
            });
        }
        assert_eq!(
            events
                .iter()
                .filter(|event| **event == (id, closed))
                .count(),
            1
        );
        f.peer.remove_tcp(peer);
        drop(client);
        f.close();
    });
}

#[zhida_test_support::pinned_test]
fn udp_receive_reports_source_destination_kind_and_socket_metadata() {
    with_eal(|eal| {
        let mut f = Fixture::new(eal, ["jruda", "jrudb"], 2, 1, 64);
        let mut client = f.client(2);
        let mut events = Vec::new();
        // The real TCP handshake resolves ARP before the UDP send.
        let (_, tcp_peer) = connect_receive(&mut f, &mut client, &mut events);
        let peer = f
            .peer
            .add_udp(dpdk_smoltcp::UdpConfig {
                port: PEER_PORT + 1,
                local_addr: None,
                rx_queue: 8,
                tx_checksum: true,
                broadcast: false,
            })
            .unwrap();
        let id = client.bind_udp(udp_config(LOCAL_PORT + 1)).unwrap();
        f.until(&mut client, &mut events, |_, events| {
            events.contains(&(id, Event::Bound))
        });
        f.peer
            .udp_send(
                &peer,
                Some(IpEndpoint::new(net_ip().into(), LOCAL_PORT + 1)),
                b"receive datagram",
            )
            .unwrap();
        let mut received = false;
        let deadline = WallInstant::now() + WAIT;
        while !received {
            assert!(
                WallInstant::now() < deadline,
                "bound UDP socket was not armed for RX"
            );
            f.round_receive(&mut client, &mut events, |packet_id, meta, frame| {
                assert!(!received, "duplicate UDP delivery");
                assert_eq!(packet_id, id);
                assert_eq!(meta.slot, id.slot);
                assert_eq!(meta.generation, id.generation);
                assert_eq!(meta.kind, UDP);
                assert_eq!(meta.src_addr, [10, 0, 0, 2]);
                assert_eq!(meta.src_port, PEER_PORT + 1);
                assert_eq!(meta.dst_addr, [10, 0, 0, 1]);
                assert_eq!(&*frame, b"receive datagram");
                received = true;
            });
        }
        f.peer.remove_udp(peer);
        f.peer.remove_tcp(tcp_peer);
        drop(client);
        f.close();
    });
}
