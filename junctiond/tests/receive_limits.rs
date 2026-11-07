use std::time::Instant;

use dpdk_smoltcp::smoltcp::wire::IpEndpoint;
use junction::control::Event;
use junction::rx::{TCP, UDP};
use zhida_test_kit::with_eal;

mod support;
use support::{Fixture, LOCAL_PORT, PEER_PORT, WAIT, connect, net_ip, udp_config};

#[zhida_test_support::pinned_test]
fn pending_udp_sockets_rotate_before_a_flood_can_refill_the_ring() {
    with_eal(|eal| {
        let mut f = Fixture::with_rx_size(eal, ["jrrota", "jrrotb"], 1, 2, 4096);
        let mut client = f.client(3);
        let mut events = Vec::new();
        let (_, tcp_peer) = connect(&mut f, &mut client, &mut events, LOCAL_PORT);
        let mut config = udp_config(LOCAL_PORT + 1);
        config.rx_queue = 32;
        let a = client.bind_udp(config).unwrap();
        config.local.port += 1;
        let b = client.bind_udp(config).unwrap();
        f.until(&mut client, &mut events, |_, events| {
            events.contains(&(b, Event::Bound))
        });
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
        let to_a = IpEndpoint::new(net_ip().into(), LOCAL_PORT + 1);
        let to_b = IpEndpoint::new(net_ip().into(), LOCAL_PORT + 2);
        let flood = [0xaa; 1200];
        for _ in 0..12 {
            f.peer.udp_send(&peer, Some(to_a), &flood).unwrap();
        }
        let mut held = Vec::new();
        let deadline = Instant::now() + WAIT;
        while held.len() < 3 {
            assert!(Instant::now() < deadline, "A did not fill the ring");
            f.round_receive(&mut client, &mut events, |id, _, frame| {
                assert_eq!(id, a);
                held.push(frame);
            });
        }
        f.peer.udp_send(&peer, Some(to_b), &[0xbb; 1200]).unwrap();
        for _ in 0..4 {
            f.round_receive(&mut client, &mut events, |_, _, _| {
                panic!("held ring was reclaimed")
            });
        }
        drop(held);
        client
            .poll(|id, event| events.push((id, event)), |_, _, _| {})
            .unwrap();
        let mut received_b = false;
        for _ in 0..4 {
            for _ in 0..6 {
                f.peer.udp_send(&peer, Some(to_a), &flood).unwrap();
            }
            f.round_receive(&mut client, &mut events, |id, meta, frame| {
                assert_eq!(meta.kind, UDP);
                if id == b {
                    assert_eq!(&*frame, &[0xbb; 1200]);
                    received_b = true;
                } else {
                    assert_eq!(id, a);
                }
            });
            if received_b {
                break;
            }
        }
        assert!(
            received_b,
            "flooded socket starved the pending UDP datagram for four polls"
        );
        f.peer.remove_udp(peer);
        f.peer.remove_tcp(tcp_peer);
        drop(client);
        f.close();
    });
}

#[zhida_test_support::pinned_test]
fn jumbo_tcp_frame_resumes_at_its_offset_and_oversized_udp_is_counted() {
    with_eal(|eal| {
        let mut f = Fixture::with_jumbo_rx(eal, ["jrjmba", "jrjmbb"], 1, 1, 4096);
        let mut client = f.client(2);
        let mut events = Vec::new();
        let (tcp_id, tcp_peer) = connect(&mut f, &mut client, &mut events, LOCAL_PORT);
        let payload: Vec<u8> = (0..8000).map(|i| (i % 251) as u8).collect();
        assert_eq!(
            f.peer.tcp_mut(&tcp_peer).send_slice(&payload),
            Ok(payload.len())
        );
        let mut held = None;
        let deadline = Instant::now() + WAIT;
        while held.is_none() {
            assert!(
                Instant::now() < deadline,
                "first jumbo chunk was not published"
            );
            f.round_receive(&mut client, &mut events, |id, meta, frame| {
                assert_eq!(id, tcp_id);
                assert_eq!(meta.kind, TCP);
                assert!(held.is_none());
                assert!(frame.len() < payload.len());
                held = Some(frame);
            });
        }
        for _ in 0..4 {
            f.round_receive(&mut client, &mut events, |_, _, _| {
                panic!("held jumbo chunk was reclaimed")
            });
        }
        let mut received = held.take().unwrap().to_vec();
        while received.len() < payload.len() {
            assert!(Instant::now() < deadline, "remaining jumbo chunk was lost");
            f.round_receive(&mut client, &mut events, |id, meta, frame| {
                assert_eq!(id, tcp_id);
                assert_eq!(meta.kind, TCP);
                received.extend_from_slice(&frame);
            });
        }
        assert_eq!(
            received, payload,
            "jumbo frame offset repeated or skipped bytes"
        );

        let id = client.bind_udp(udp_config(LOCAL_PORT + 1)).unwrap();
        f.until(&mut client, &mut events, |_, events| {
            events.contains(&(id, Event::Bound))
        });
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
        let to = IpEndpoint::new(net_ip().into(), LOCAL_PORT + 1);
        f.peer.udp_send(&peer, Some(to), &payload).unwrap();
        for _ in 0..4 {
            f.round_receive(&mut client, &mut events, |_, _, _| {
                panic!("oversized UDP was published")
            });
        }
        assert_eq!(
            f.network.udp_rx_dropped(),
            1,
            "oversized UDP drop was not counted"
        );
        f.peer.udp_send(&peer, Some(to), b"still live").unwrap();
        let mut live = false;
        let deadline = Instant::now() + WAIT;
        while !live {
            assert!(
                Instant::now() < deadline,
                "UDP socket did not survive oversized datagram"
            );
            f.round_receive(&mut client, &mut events, |packet_id, _, frame| {
                assert_eq!(packet_id, id);
                assert_eq!(&*frame, b"still live");
                live = true;
            });
        }
        f.peer.remove_udp(peer);
        f.peer.remove_tcp(tcp_peer);
        drop(client);
        f.close();
    });
}
