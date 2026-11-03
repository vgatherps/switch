use std::time::Instant as WallInstant;

use dpdk_smoltcp::TcpHandle;
use dpdk_smoltcp::smoltcp::{socket::tcp::State, wire::IpEndpoint};
use junction::control::{Event, SocketId};
use junction::rx::{RxFrame, TCP, UDP};
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
        events.contains(&(id, Event::Connected)) && f.peer.tcp(&peer).state() == State::Established
    });
    (id, peer)
}

fn check_tcp(id: SocketId, expected_id: SocketId, frame: &RxFrame) {
    assert_eq!(id, expected_id);
    assert_eq!(frame.meta().slot, expected_id.slot);
    assert_eq!(frame.meta().generation, expected_id.generation);
    assert_eq!(frame.meta().kind, TCP);
    assert!(!frame.payload().is_empty());
}

#[zhida_test_support::pinned_test]
fn tcp_many_receive_frames_preserve_payload_order_and_socket_metadata() {
    with_eal(|eal| {
        let mut f = Fixture::new(eal, ["jrmna", "jrmnb"], 2, 0, 64);
        let mut client = f.client(1);
        let mut events = Vec::new();
        let (id, peer) = connect_receive(&mut f, &mut client, &mut events);
        let payload: Vec<u8> = (0..32 * 1024).map(|n| (n % 251) as u8).collect();
        assert_eq!(
            f.peer.tcp_mut(&peer).send_slice(&payload),
            Ok(payload.len())
        );
        let mut received = Vec::new();
        let mut frames = 0;
        let deadline = WallInstant::now() + WAIT;
        while received.len() < payload.len() || f.peer.tcp(&peer).send_queue() != 0 {
            assert!(WallInstant::now() < deadline, "TCP receive timed out");
            if !f.round_receive(&mut client, &mut events, |packet_id, frame| {
                check_tcp(packet_id, id, &frame);
                received.extend_from_slice(frame.payload());
                frames += 1;
            }) {
                f.advance();
            }
            assert!(received.len() <= payload.len());
        }
        assert!(frames > 16, "transfer did not exercise many RX records");
        assert_eq!(received, payload);
        f.peer.remove_tcp(peer);
        drop(client);
        f.close();
    });
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
            if !f.round_receive(&mut client, &mut events, |packet_id, frame| {
                check_tcp(packet_id, id, &frame);
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
            f.round_receive(&mut client, &mut events, |packet_id, frame| {
                check_tcp(packet_id, id, &frame);
                held.push(frame);
            });
        }
        assert!(
            !events.contains(&(id, Event::PeerFin)),
            "FIN escaped a full RX ring"
        );
        let mut received = Vec::new();
        for frame in &held {
            received.extend_from_slice(frame.payload());
        }
        assert!(!received.is_empty());
        assert!(
            received.len() < payload.len(),
            "held frames did not fill the RX ring"
        );
        assert_eq!(received, payload[..received.len()]);

        let held_bytes = received.len();
        for _ in 0..32 {
            f.round_receive(&mut client, &mut events, |_, _| {
                panic!("producer reclaimed records while client frames were held")
            });
        }
        assert!(!events.contains(&(id, Event::PeerFin)));
        assert_eq!(
            held.iter()
                .map(|frame| frame.payload().len())
                .sum::<usize>(),
            held_bytes
        );
        assert!(
            held.iter()
                .flat_map(|frame| frame.payload().iter())
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
            if !f.round_receive(&mut client, &mut events, |packet_id, frame| {
                check_tcp(packet_id, id, &frame);
                received.extend_from_slice(frame.payload());
            }) {
                f.advance();
            }
            assert!(received.len() <= payload.len());
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
            f.round_receive(&mut client, &mut events, |packet_id, frame| {
                assert!(!received, "duplicate UDP delivery");
                assert_eq!(packet_id, id);
                let meta = frame.meta();
                assert_eq!(meta.slot, id.slot);
                assert_eq!(meta.generation, id.generation);
                assert_eq!(meta.kind, UDP);
                assert_eq!(meta.src_addr, [10, 0, 0, 2]);
                assert_eq!(meta.src_port, PEER_PORT + 1);
                assert_eq!(meta.dst_addr, [10, 0, 0, 1]);
                assert_eq!(frame.payload(), b"receive datagram");
                received = true;
            });
        }
        f.peer.remove_udp(peer);
        f.peer.remove_tcp(tcp_peer);
        drop(client);
        f.close();
    });
}
