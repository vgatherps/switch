use std::io::{self, Read, Write};
use std::os::unix::net::UnixStream;
use std::time::Instant as WallInstant;

use dpdk_smoltcp::TcpHandle;
use dpdk_smoltcp::smoltcp::socket::tcp::State;
use dpdk_smoltcp::smoltcp::wire::IpEndpoint;
use junction::control::{
    ClientMessage, EndReason, Event, HostMessage, Op, RejectCode, SocketId, VERSION, WelcomeStatus,
};
use junction::host::EndHow;
use junction::session::{ConnectError, Session};
use zhida_test_kit::with_eal;

mod support;
use support::{
    Fixture, LOCAL_PORT, PEER_PORT, WAIT, connect, local, peer_endpoint, peer_listen, tcp_config,
};

#[zhida_test_support::pinned_test]
fn shutdown_ack_reports_delivered_then_peer_fin_before_clean_closed() {
    with_eal(|eal| {
        let mut fixture = Fixture::new(eal, ["lsda", "lsdb"], 2, 0, 64);
        let mut client = fixture.client(1);
        let mut events = Vec::new();
        let (id, peer) = connect(&mut fixture, &mut client, &mut events, LOCAL_PORT);
        client.shutdown(id);
        fixture.until(&mut client, &mut events, |f, events| {
            events.contains(&(id, Event::Delivered))
                && f.peer.tcp(&peer).state() == State::CloseWait
        });
        assert_eq!(events, [(id, Event::Connected), (id, Event::Delivered)]);
        assert!(!fixture.peer.tcp(&peer).reset_by_peer());

        fixture.peer.close(&peer);
        fixture.until(&mut client, &mut events, |f, events| {
            events.contains(&(id, Event::PeerFin)) && f.peer.tcp(&peer).state() == State::Closed
        });
        fixture.until(&mut client, &mut events, |f, events| {
            f.advance();
            events.contains(&(
                id,
                Event::Closed {
                    reset_by_peer: false,
                },
            ))
        });
        assert_eq!(
            events,
            [
                (id, Event::Connected),
                (id, Event::Delivered),
                (id, Event::PeerFin),
                (
                    id,
                    Event::Closed {
                        reset_by_peer: false
                    }
                ),
            ]
        );
        client.close(id, false);
        fixture.until(&mut client, &mut events, |_, events| {
            events.contains(&(id, Event::Freed))
        });
        drop((client, peer));
        fixture.close();
    });
}

#[zhida_test_support::pinned_test]
fn peer_fin_first_reports_peer_fin_without_closing_local_send_half() {
    with_eal(|eal| {
        let mut fixture = Fixture::new(eal, ["lpfa", "lpfb"], 2, 0, 64);
        let mut client = fixture.client(1);
        let mut events = Vec::new();
        let (id, peer) = connect(&mut fixture, &mut client, &mut events, LOCAL_PORT);
        fixture.peer.close(&peer);
        fixture.until(&mut client, &mut events, |f, events| {
            events.contains(&(id, Event::PeerFin)) && f.peer.tcp(&peer).state() == State::FinWait2
        });
        assert_eq!(events, [(id, Event::Connected), (id, Event::PeerFin)]);
        assert!(!fixture.peer.tcp(&peer).reset_by_peer());
        client.shutdown(id);
        fixture.until(&mut client, &mut events, |_, events| {
            events.contains(&(
                id,
                Event::Closed {
                    reset_by_peer: false,
                },
            ))
        });
        client.close(id, false);
        fixture.until(&mut client, &mut events, |_, events| {
            events.contains(&(id, Event::Freed))
        });
        drop((client, peer));
        fixture.close();
    });
}

#[zhida_test_support::pinned_test]
fn peer_reset_reports_closed_with_reset_flag() {
    with_eal(|eal| {
        let mut fixture = Fixture::new(eal, ["lrsa", "lrsb"], 2, 0, 64);
        let mut client = fixture.client(1);
        let mut events = Vec::new();
        let (id, peer) = connect(&mut fixture, &mut client, &mut events, LOCAL_PORT);
        fixture.peer.abort(&peer);
        fixture.until(&mut client, &mut events, |_, events| {
            events.contains(&(
                id,
                Event::Closed {
                    reset_by_peer: true,
                },
            ))
        });
        assert_eq!(
            events,
            [
                (id, Event::Connected),
                (
                    id,
                    Event::Closed {
                        reset_by_peer: true
                    }
                ),
            ]
        );
        client.close(id, false);
        fixture.until(&mut client, &mut events, |_, events| {
            events.contains(&(id, Event::Freed))
        });
        drop((client, peer));
        fixture.close();
    });
}

#[zhida_test_support::pinned_test]
fn close_during_syn_sent_frees_slot_and_next_generation_connects() {
    with_eal(|eal| {
        let mut fixture = Fixture::new(eal, ["lcsa", "lcsb"], 2, 0, 64);
        let mut client = fixture.client(1);
        let mut events = Vec::new();
        let peer = peer_listen(&mut fixture);
        let old = client
            .connect_tcp(tcp_config(), peer_endpoint(PEER_PORT), local(LOCAL_PORT))
            .unwrap();
        assert!(matches!(fixture.wait_op(old), Op::ConnectTcp { .. }));
        for round in 0..1000 {
            fixture.network.iteration(fixture.now).unwrap();
            fixture.to_peer.forward();
            zhida_test_kit::pair::assert_lossless(&fixture.to_peer, "host to peer");
            fixture
                .peer
                .turn(fixture.now, zhida_test_kit::pair::BUDGET)
                .unwrap();
            if fixture.peer.tcp(&peer).state() == State::SynReceived {
                break;
            }
            assert!(round < 999, "peer did not receive the SYN");
            fixture.to_net.forward();
            zhida_test_kit::pair::assert_lossless(&fixture.to_net, "peer to host");
        }
        // Leave the SYN-ACK queued, after allowing ARP to resolve in both directions.
        fixture.to_net.paused = true;
        client.close(old, false);
        assert_eq!(
            fixture.wait_op(old),
            Op::Close {
                id: old,
                unread: false
            }
        );
        let deadline = WallInstant::now() + WAIT;
        while !events.contains(&(old, Event::Freed)) {
            assert!(
                WallInstant::now() < deadline,
                "close did not free SynSent socket"
            );
            fixture.round(&mut client, &mut events);
        }
        assert_eq!(events, [(old, Event::Freed)]);
        fixture.to_net.paused = false;
        fixture.until(&mut client, &mut events, |f, _| {
            f.peer.tcp(&peer).state() == State::Listen
        });

        let new = client
            .connect_tcp(tcp_config(), peer_endpoint(PEER_PORT), local(LOCAL_PORT))
            .unwrap();
        assert_ne!(new.generation, old.generation);
        fixture.until(&mut client, &mut events, |f, events| {
            events.contains(&(new, Event::Connected))
                && f.peer.tcp(&peer).state() == State::Established
        });
        assert_eq!(events, [(old, Event::Freed), (new, Event::Connected)]);
        client.abort(new);
        fixture.until(&mut client, &mut events, |f, events| {
            events.contains(&(new, Event::Freed)) && f.peer.tcp(&peer).reset_by_peer()
        });
        drop((client, peer));
        fixture.close();
    });
}

#[zhida_test_support::pinned_test]
fn dropped_session_resets_established_tcp() {
    with_eal(|eal| {
        let mut fixture = Fixture::new(eal, ["ldsa", "ldsb"], 2, 0, 64);
        let mut client = fixture.client(1);
        let mut events = Vec::new();
        let (_, peer) = connect(&mut fixture, &mut client, &mut events, LOCAL_PORT);
        drop(client);
        let deadline = WallInstant::now() + WAIT;
        while fixture.peer.tcp(&peer).state() != State::Closed {
            assert!(
                WallInstant::now() < deadline,
                "client loss did not reset the peer"
            );
            fixture.round_links();
        }
        assert!(fixture.peer.tcp(&peer).reset_by_peer());
        drop(peer);
        fixture.close();
    });
}

#[zhida_test_support::pinned_test]
fn crashed_session_resets_tcp_before_releasing_held_plain_close() {
    with_eal(|eal| {
        let mut fixture = Fixture::new(eal, ["lcrsha", "lcrshb"], 1, 0, 64);
        let mut client = fixture.client(1);
        let mut events = Vec::new();
        let (id, peer) = connect(&mut fixture, &mut client, &mut events, LOCAL_PORT);
        let session = fixture.streams[0];
        client.close(id, false);
        assert_eq!(fixture.wait_op(id), Op::Close { id, unread: false });
        drop(client);
        fixture.forward_lost(session);
        // Both control messages precede this turn. Close is held until FIFO idle,
        // where the unended lost session must abort before that close is released.
        fixture.turn_network();
        let deadline = WallInstant::now() + WAIT;
        while !fixture.released.contains(&session)
            || !matches!(
                fixture.peer.tcp(&peer).state(),
                State::Closed | State::CloseWait
            )
        {
            assert!(
                WallInstant::now() < deadline,
                "crashed session did not close its peer"
            );
            fixture.round_links();
        }
        assert!(
            fixture.peer.tcp(&peer).reset_by_peer(),
            "crashed session sent FIN instead of resetting its held plain close"
        );
        assert_eq!(fixture.released, [session]);
        assert!(fixture.retired.is_empty());
        drop(peer);
        fixture.close();
    });
}

#[zhida_test_support::pinned_test]
fn finish_then_drop_with_control_loss_before_fifo_end_delivers_bytes_then_fin() {
    with_eal(|eal| {
        let mut fixture = Fixture::new(eal, ["lfda", "lfdb"], 2, 0, 1);
        let mut client = fixture.client(1);
        let mut events = Vec::new();
        let (id, peer) = connect(&mut fixture, &mut client, &mut events, LOCAL_PORT);
        let session = fixture.streams[0];
        let payload = b"bytes committed before finish";
        client.send_tcp(id, payload).unwrap();
        client.finish().unwrap();
        drop(client);
        fixture.forward_lost(session);
        // A one-record FIFO batch consumes only SEND. The real Lost is handled
        // before it, and the real FIFO end cannot be consumed until the next turn.
        fixture.turn_network();
        assert!(fixture.retired.is_empty());
        assert!(fixture.released.is_empty());
        assert!(fixture.protocol_errors.is_empty());
        let mut received = Vec::new();
        let deadline = WallInstant::now() + WAIT;
        while fixture.peer.tcp(&peer).state() != State::CloseWait
            || received.len() != payload.len()
            || !fixture.retired.contains(&session)
        {
            assert!(
                WallInstant::now() < deadline,
                "finish then drop did not deliver bytes and FIN"
            );
            fixture.round_links();
            while fixture.peer.tcp(&peer).can_recv() {
                let frame = fixture.peer.tcp_mut(&peer).recv().unwrap().unwrap();
                received.extend_from_slice(frame.payload());
            }
            assert!(!fixture.peer.tcp(&peer).reset_by_peer());
        }
        assert_eq!(received, payload);
        assert_eq!(fixture.retired, [session]);
        assert!(fixture.released.is_empty());
        drop(peer);
        fixture.close();
    });
}

#[zhida_test_support::pinned_test]
fn lost_session_rejects_hello_until_network_writer_handback() {
    with_eal(|eal| {
        let mut fixture = Fixture::with_one_session(eal, ["lhba", "lhbb"], 2);
        let mut client = fixture.client(1);
        let mut events = Vec::new();
        let (_, peer) = connect(&mut fixture, &mut client, &mut events, LOCAL_PORT);
        let old_session = fixture.streams[0];
        drop(client);
        fixture.forward_lost(old_session);
        // The control socket is gone, but the network still owns this session.
        assert!(matches!(
            Session::connect(&fixture.socket_path, 1),
            Err(ConnectError::Rejected(WelcomeStatus::SessionsFull))
        ));
        fixture.network.iteration(fixture.now).unwrap();
        // Keep Released in the fixture's channel: teardown alone must not free
        // the control-owned slot before writer handback.
        assert!(matches!(
            Session::connect(&fixture.socket_path, 1),
            Err(ConnectError::Rejected(WelcomeStatus::SessionsFull))
        ));
        fixture.wait_handback(old_session);
        assert_eq!(fixture.released, [old_session]);
        assert!(fixture.retired.is_empty());
        let admitted = fixture.client(1);
        let new_session = fixture.streams[1];
        assert_eq!(ceph_proto::slot(old_session), ceph_proto::slot(new_session));
        assert_ne!(ceph_proto::uid(old_session), ceph_proto::uid(new_session));
        drop((admitted, peer));
        fixture.close();
    });
}

#[zhida_test_support::pinned_test]
fn stale_session_end_after_slot_reuse_does_not_touch_new_connection() {
    with_eal(|eal| {
        for how in [EndHow::Lost, EndHow::Abort] {
            let mut fixture = Fixture::with_one_session(eal, ["lsea", "lseb"], 2);
            let old = fixture.client(1);
            let old_session = fixture.streams[0];
            drop(old);
            fixture.forward_lost(old_session);
            fixture.network.iteration(fixture.now).unwrap();
            fixture.wait_handback(old_session);

            let mut client = fixture.client(1);
            let new_session = fixture.streams[1];
            assert_eq!(ceph_proto::slot(old_session), ceph_proto::slot(new_session));
            assert_ne!(ceph_proto::uid(old_session), ceph_proto::uid(new_session));
            let mut events = Vec::new();
            let (id, peer) = connect(&mut fixture, &mut client, &mut events, LOCAL_PORT);
            // Only this late terminal message is injected; both sessions and the
            // original Lost use the real control thread.
            fixture.queue_stale_end(old_session, how);
            fixture.round(&mut client, &mut events);
            assert_eq!(
                fixture.peer.tcp(&peer).state(),
                State::Established,
                "{how:?}"
            );
            assert!(!fixture.peer.tcp(&peer).reset_by_peer());
            assert_eq!(events, [(id, Event::Connected)]);

            client.close(id, false);
            fixture.until(&mut client, &mut events, |f, events| {
                events.contains(&(id, Event::Freed))
                    && f.peer.tcp(&peer).state() == State::CloseWait
            });
            assert_eq!(events, [(id, Event::Connected), (id, Event::Freed)]);
            assert!(!fixture.peer.tcp(&peer).reset_by_peer(), "{how:?}");
            drop((client, peer));
            fixture.close();
        }
    });
}

#[zhida_test_support::pinned_test]
fn incoming_handshake_reset_keeps_listener_waiting_for_real_connection() {
    with_eal(|eal| {
        let mut fixture = Fixture::new(eal, ["lhra", "lhrb"], 2, 1, 64);
        let mut client = fixture.client(2);
        let mut events = Vec::new();
        // Resolve ARP with a refused connect, as in switchnet's accept reset test.
        let probe = fixture.peer.add_tcp(support::peer_tcp_config()).unwrap();
        fixture
            .peer
            .connect(
                &probe,
                IpEndpoint::new(support::net_ip().into(), 1),
                PEER_PORT + 1,
            )
            .unwrap();
        fixture.until(&mut client, &mut events, |f, _| {
            f.peer.tcp(&probe).state() == State::Closed
        });
        fixture.peer.remove_tcp(probe);

        let id = client.listen_tcp(tcp_config(), local(LOCAL_PORT)).unwrap();
        assert!(matches!(fixture.wait_op(id), Op::ListenTcp { .. }));
        fixture.round(&mut client, &mut events);
        let endpoint = IpEndpoint::new(support::net_ip().into(), LOCAL_PORT);
        let gone = fixture.peer.add_tcp(support::peer_tcp_config()).unwrap();
        fixture.peer.connect(&gone, endpoint, PEER_PORT).unwrap();
        fixture
            .peer
            .turn(fixture.now, zhida_test_kit::pair::BUDGET)
            .unwrap();
        assert_eq!(fixture.to_net.forward(), 1, "SYN");
        zhida_test_kit::pair::assert_lossless(&fixture.to_net, "SYN");
        fixture.network.iteration(fixture.now).unwrap();
        assert_eq!(fixture.to_peer.forward(), 1, "SYN-ACK");
        zhida_test_kit::pair::assert_lossless(&fixture.to_peer, "SYN-ACK");
        // Forget the initiating socket before delivering SYN-ACK. The peer stack
        // then generates a real RST, returning the host's SynReceived to Listen.
        fixture.peer.remove_tcp(gone);
        fixture
            .peer
            .turn(fixture.now, zhida_test_kit::pair::BUDGET)
            .unwrap();
        assert_eq!(fixture.to_net.forward(), 1, "RST");
        zhida_test_kit::pair::assert_lossless(&fixture.to_net, "RST");
        fixture.round(&mut client, &mut events);

        // Bound is an ordered control-event fence, not a timed absence assertion.
        let fence = client.bind_udp(support::udp_config(7000)).unwrap();
        fixture.until(&mut client, &mut events, |_, events| {
            events.contains(&(fence, Event::Bound))
        });
        assert_eq!(
            events,
            [(fence, Event::Bound)],
            "reset handshake emitted an event"
        );
        client.close(fence, false);
        fixture.until(&mut client, &mut events, |_, events| {
            events.contains(&(fence, Event::Freed))
        });
        events.clear();

        let peer = fixture.peer.add_tcp(support::peer_tcp_config()).unwrap();
        fixture
            .peer
            .connect(&peer, endpoint, PEER_PORT + 2)
            .unwrap();
        fixture.until(&mut client, &mut events, |f, events| {
            events.contains(&(id, Event::Connected))
                && f.peer.tcp(&peer).state() == State::Established
        });
        assert_eq!(events, [(id, Event::Connected)]);
        client.abort(id);
        fixture.until(&mut client, &mut events, |f, events| {
            events.contains(&(id, Event::Freed)) && f.peer.tcp(&peer).reset_by_peer()
        });
        drop((client, peer));
        fixture.close();
    });
}

#[zhida_test_support::pinned_test]
fn finish_with_open_tcp_sends_fin_and_closes_client() {
    with_eal(|eal| {
        let mut fixture = Fixture::new(eal, ["lfsa", "lfsb"], 2, 0, 64);
        let mut client = fixture.client(1);
        let mut events = Vec::new();
        let (_, peer) = connect(&mut fixture, &mut client, &mut events, LOCAL_PORT);
        client.finish().unwrap();
        assert!(!client.closed());
        let deadline = WallInstant::now() + WAIT;
        while !client.closed() || fixture.peer.tcp(&peer).state() != State::CloseWait {
            assert!(
                WallInstant::now() < deadline,
                "graceful session finish did not complete"
            );
            fixture.round(&mut client, &mut events);
        }
        assert!(!fixture.peer.tcp(&peer).reset_by_peer());
        drop((client, peer));
        fixture.close();
    });
}

struct RawClient {
    socket: UnixStream,
    input: Vec<u8>,
    messages: Vec<HostMessage>,
    closed: bool,
}

impl RawClient {
    fn connect(fixture: &Fixture) -> Self {
        let mut socket = UnixStream::connect(&fixture.socket_path).unwrap();
        socket.set_read_timeout(Some(WAIT)).unwrap();
        socket.set_write_timeout(Some(WAIT)).unwrap();
        let mut bytes = Vec::new();
        ClientMessage::Hello {
            version: VERSION,
            max_sockets: 1,
        }
        .encode(&mut bytes);
        socket.write_all(&bytes).unwrap();
        bytes.clear();
        loop {
            if let Some((message, used)) = HostMessage::decode(&bytes).unwrap() {
                let HostMessage::Welcome(welcome) = message else {
                    panic!("expected Welcome")
                };
                assert_eq!(welcome.status, WelcomeStatus::Ok);
                assert_eq!(welcome.version, VERSION);
                bytes.drain(..used);
                break;
            }
            let mut chunk = [0; 4096];
            let count = socket.read(&mut chunk).unwrap();
            assert_ne!(count, 0, "host closed before Welcome");
            bytes.extend_from_slice(&chunk[..count]);
        }
        socket.set_nonblocking(true).unwrap();
        Self {
            socket,
            input: bytes,
            messages: Vec::new(),
            closed: false,
        }
    }

    fn send(&mut self, ops: &[Op]) {
        let mut bytes = Vec::new();
        for op in ops {
            ClientMessage::Op(*op).encode(&mut bytes);
        }
        self.socket.write_all(&bytes).unwrap();
    }

    fn poll(&mut self) {
        loop {
            while let Some((message, used)) = HostMessage::decode(&self.input).unwrap() {
                self.input.drain(..used);
                self.messages.push(message);
            }
            if self.closed {
                return;
            }
            let mut chunk = [0; 4096];
            match self.socket.read(&mut chunk) {
                Ok(0) => self.closed = true,
                Ok(count) => self.input.extend_from_slice(&chunk[..count]),
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => return,
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(error) => panic!("raw control read failed: {error}"),
            }
        }
    }

    fn until(&mut self, fixture: &mut Fixture, mut done: impl FnMut(&Fixture, &Self) -> bool) {
        let deadline = WallInstant::now() + WAIT;
        while !done(fixture, self) {
            assert!(
                WallInstant::now() < deadline,
                "raw client condition timed out: {:?}",
                self.messages
            );
            fixture.round_links();
            self.poll();
        }
    }

    fn has(&self, id: SocketId, event: Event) -> bool {
        self.messages.contains(&HostMessage::Event { id, event })
    }
}

fn raw_connected(fixture: &mut Fixture) -> (RawClient, SocketId, TcpHandle) {
    let peer = peer_listen(fixture);
    let mut client = RawClient::connect(fixture);
    let id = SocketId {
        slot: 0,
        generation: 1,
    };
    client.send(&[Op::ConnectTcp {
        id,
        config: tcp_config(),
        remote: peer_endpoint(PEER_PORT),
        local: local(LOCAL_PORT),
    }]);
    client.until(fixture, |f, client| {
        client.has(id, Event::Connected) && f.peer.tcp(&peer).state() == State::Established
    });
    (client, id, peer)
}

#[zhida_test_support::pinned_test]
fn wrong_generation_on_occupied_socket_ends_only_offending_session() {
    with_eal(|eal| {
        let mut fixture = Fixture::new(eal, ["lwga", "lwgb"], 2, 1, 64);
        let mut unaffected = fixture.client(1);
        let (mut client, id, peer) = raw_connected(&mut fixture);
        client.send(&[Op::ShutdownTcp {
            id: SocketId {
                generation: id.generation + 1,
                ..id
            },
        }]);
        client.until(&mut fixture, |f, client| {
            client
                .messages
                .contains(&HostMessage::End(EndReason::ProtocolError))
                && f.peer.tcp(&peer).reset_by_peer()
        });
        let bound = unaffected.bind_udp(support::udp_config(7000)).unwrap();
        let mut events = Vec::new();
        fixture.until(&mut unaffected, &mut events, |_, events| {
            events.contains(&(bound, Event::Bound))
        });
        unaffected.close(bound, false);
        fixture.until(&mut unaffected, &mut events, |_, events| {
            events.contains(&(bound, Event::Freed))
        });
        drop((client, unaffected, peer));
        fixture.close();
    });
}

#[zhida_test_support::pinned_test]
fn open_on_occupied_slot_ends_session_with_protocol_error() {
    with_eal(|eal| {
        let mut fixture = Fixture::new(eal, ["losa", "losb"], 2, 0, 64);
        let (mut client, id, peer) = raw_connected(&mut fixture);
        client.send(&[Op::ListenTcp {
            id,
            config: tcp_config(),
            local: local(LOCAL_PORT + 1),
        }]);
        client.until(&mut fixture, |f, client| {
            client
                .messages
                .contains(&HostMessage::End(EndReason::ProtocolError))
                && f.peer.tcp(&peer).reset_by_peer()
        });
        drop((client, peer));
        fixture.close();
    });
}

#[zhida_test_support::pinned_test]
fn abort_after_held_close_in_same_control_batch_frees_once_without_protocol_error() {
    with_eal(|eal| {
        let mut fixture = Fixture::new(eal, ["lcaa", "lcab"], 2, 0, 64);
        let (mut client, id, peer) = raw_connected(&mut fixture);
        let close = Op::Close { id, unread: false };
        let abort = Op::AbortTcp { id };
        client.send(&[close, abort]);
        assert_eq!(fixture.wait_op(id), close);
        assert_eq!(fixture.wait_op(id), abort);
        client.until(&mut fixture, |f, client| {
            client.has(id, Event::Freed) && f.peer.tcp(&peer).reset_by_peer()
        });
        let bound = Op::ListenTcp {
            id: SocketId {
                generation: id.generation + 1,
                ..id
            },
            config: tcp_config(),
            local: local(LOCAL_PORT + 1),
        };
        client.send(&[bound]);
        assert_eq!(fixture.wait_op(bound.id()), bound);
        let abort_new = Op::AbortTcp { id: bound.id() };
        client.send(&[abort_new]);
        assert_eq!(fixture.wait_op(bound.id()), abort_new);
        client.until(&mut fixture, |_, client| {
            client.has(bound.id(), Event::Freed)
        });
        assert_eq!(
            client
                .messages
                .iter()
                .filter(|message| {
                    **message
                        == HostMessage::Event {
                            id,
                            event: Event::Freed,
                        }
                })
                .count(),
            1
        );
        assert!(
            !client
                .messages
                .iter()
                .any(|message| matches!(message, HostMessage::End(_)))
        );
        assert!(!client.closed);
        drop((client, peer));
        fixture.close();
    });
}

#[zhida_test_support::pinned_test]
fn lost_after_forwarded_graceful_end_retires_without_stopping_other_sessions() {
    with_eal(|eal| {
        let mut fixture = Fixture::new(eal, ["ltea", "lteb"], 2, 1, 64);
        let mut finished = fixture.client(1);
        let stream = fixture.streams[0];
        let mut unaffected = fixture.client(1);
        finished.finish().unwrap();
        // Consume the real FIFO end; its retire notification makes the host send Graceful.
        fixture.network.iteration(fixture.now).unwrap();
        fixture.forward_graceful_then_lost(stream);
        let bound = unaffected.bind_udp(support::udp_config(7000)).unwrap();
        let mut events = Vec::new();
        fixture.until(&mut unaffected, &mut events, |f, events| {
            events.contains(&(bound, Event::Bound)) && f.retired.contains(&stream)
        });
        let deadline = WallInstant::now() + WAIT;
        while !finished.closed() {
            assert!(
                WallInstant::now() < deadline,
                "finished session did not close"
            );
            fixture.round_links();
            finished.poll_events(|_, _| {}).unwrap();
        }
        unaffected.close(bound, false);
        fixture.until(&mut unaffected, &mut events, |_, events| {
            events.contains(&(bound, Event::Freed))
        });
        drop((finished, unaffected));
        fixture.close();
    });
}

#[zhida_test_support::pinned_test]
fn lost_after_consumed_fifo_end_sends_fin_instead_of_reset() {
    with_eal(|eal| {
        let mut fixture = Fixture::new(eal, ["lefa", "lefb"], 2, 0, 64);
        let mut client = fixture.client(1);
        let stream = fixture.streams[0];
        let mut events = Vec::new();
        let (_, peer) = connect(&mut fixture, &mut client, &mut events, LOCAL_PORT);
        client.finish().unwrap();
        // The FIFO consumes the real end before control reports a competing EOF.
        fixture.network.iteration(fixture.now).unwrap();
        fixture.forward_graceful_as_lost(stream);
        fixture.until(&mut client, &mut events, |f, _| {
            f.retired.contains(&stream) && f.peer.tcp(&peer).state() == State::CloseWait
        });
        assert!(!fixture.peer.tcp(&peer).reset_by_peer());
        drop((client, peer));
        fixture.close();
    });
}

#[zhida_test_support::pinned_test]
fn control_protocol_error_after_consumed_fifo_end_resets_established_tcp() {
    with_eal(|eal| {
        let mut fixture = Fixture::with_held_retire(eal, ["lpra", "lprb"], 2, 0);
        let (mut client, _, peer) = raw_connected(&mut fixture);
        let stream = fixture.streams[0];
        let writer = ceph_ring::Writer::attach(&fixture.ingress).unwrap();
        writer.try_finish(stream, 0).unwrap();
        fixture.network.iteration(fixture.now).unwrap();
        let finished = fixture.wait_finished(stream);
        assert_eq!(fixture.peer.tcp(&peer).state(), State::Established);

        // The host unit test covers malformed bytes already readable at Finished.
        // Here, hold the real Finished until the network has consumed the FIFO end,
        // then write the bytes: either host read path must still abort, never finish.
        client.socket.write_all(&[255; 8]).unwrap();
        fixture.release_retire(finished);
        fixture.forward_abort(stream);
        client.until(&mut fixture, |f, client| {
            client
                .messages
                .contains(&HostMessage::End(EndReason::ProtocolError))
                && f.peer.tcp(&peer).state() == State::Closed
        });
        assert!(fixture.peer.tcp(&peer).reset_by_peer());
        assert!(!fixture.retired.contains(&stream));
        drop((writer, client, peer));
        fixture.close();
    });
}

fn held_ops_do_not_touch_reopened_udp(shutdown: bool, wrap_generation: bool) {
    with_eal(|eal| {
        let mut fixture = Fixture::new(eal, ["lsga", "lsgb"], 2, 1, 64);
        let mut unaffected = fixture.client(1);
        let (mut client, id, peer) = raw_connected(&mut fixture);
        let held = if shutdown {
            Op::ShutdownTcp { id }
        } else {
            Op::Close { id, unread: false }
        };
        let reopen = Op::BindUdp {
            id,
            config: support::udp_config(7001),
        };
        // Multiple held operations must all disappear when their TCP socket is removed.
        let mut ops = vec![held, held, Op::AbortTcp { id }];
        if wrap_generation {
            let intermediate = SocketId {
                generation: id.generation + 1,
                ..id
            };
            ops.extend([
                Op::BindUdp {
                    id: intermediate,
                    config: support::udp_config(7001),
                },
                Op::Close {
                    id: intermediate,
                    unread: true,
                },
            ]);
        }
        ops.push(reopen);
        client.send(&ops);
        // Forward the entire sequence before one network iteration can release a held op.
        for op in ops {
            assert_eq!(fixture.wait_op(op.id()), op);
        }
        client.until(&mut fixture, |f, client| {
            client.has(id, Event::Bound) && f.peer.tcp(&peer).reset_by_peer()
        });
        // The replacement must still own its port after the FIFO-idle release.
        let collision = unaffected.bind_udp(support::udp_config(7001)).unwrap();
        let mut events = Vec::new();
        fixture.until(&mut unaffected, &mut events, |_, events| {
            events.contains(&(collision, Event::Rejected(RejectCode::UdpPortInUse)))
        });
        client.poll();
        let mut expected = vec![
            HostMessage::Event {
                id,
                event: Event::Connected,
            },
            HostMessage::Event {
                id,
                event: Event::Freed,
            },
        ];
        if wrap_generation {
            let intermediate = SocketId {
                generation: id.generation + 1,
                ..id
            };
            expected.extend([
                HostMessage::Event {
                    id: intermediate,
                    event: Event::Bound,
                },
                HostMessage::Event {
                    id: intermediate,
                    event: Event::Freed,
                },
            ]);
        }
        expected.push(HostMessage::Event {
            id,
            event: Event::Bound,
        });
        assert_eq!(client.messages, expected);
        assert!(!client.closed);
        client.send(&[Op::Close { id, unread: false }]);
        expected.push(HostMessage::Event {
            id,
            event: Event::Freed,
        });
        client.until(&mut fixture, |_, client| client.messages == expected);
        drop((client, unaffected, peer));
        fixture.close();
    });
}

#[zhida_test_support::pinned_test]
fn reopened_udp_discards_held_tcp_operations() {
    for (shutdown, wrap_generation) in [(true, false), (false, false), (true, true), (false, true)]
    {
        held_ops_do_not_touch_reopened_udp(shutdown, wrap_generation);
    }
}

#[zhida_test_support::pinned_test]
fn unknown_fifo_session_drains_past_control_batch_to_queued_open() {
    with_eal(|eal| {
        let mut fixture = Fixture::with_control_batch(eal, ["luqa", "luqb"], 2, 1, 64, 1);
        let mut other = fixture.client(1);
        let mut other_events = Vec::new();
        let listener = other.listen_tcp(tcp_config(), local(LOCAL_PORT)).unwrap();
        fixture.wait_op(listener);
        fixture.network.iteration(fixture.now).unwrap();
        fixture.network.iteration(fixture.now).unwrap();
        other.shutdown(listener);
        fixture.wait_op(listener);
        other.close(listener, false);
        fixture.wait_op(listener);
        let mut client = RawClient::connect(&fixture);
        let stream = fixture.wait_open();
        let writer = ceph_ring::Writer::attach(&fixture.ingress).unwrap();
        writer.try_push(b"undefined packet", stream, 0).unwrap();
        // The bounded drain takes Shutdown; retry must drain both Close and Open,
        // not another control_batch of one message.
        fixture.network.iteration(fixture.now).unwrap();
        client.until(&mut fixture, |_, client| {
            client
                .messages
                .contains(&HostMessage::End(EndReason::ProtocolError))
        });
        fixture.until(&mut other, &mut other_events, |_, events| {
            events.contains(&(listener, Event::Freed))
        });
        drop((writer, client, other));
        fixture.close();
    });
}

#[zhida_test_support::pinned_test]
fn fifo_end_before_queued_open_precedes_later_lost() {
    with_eal(|eal| {
        let mut fixture = Fixture::with_control_batch(eal, ["leqa", "leqb"], 2, 1, 64, 1);
        let mut other = fixture.client(1);
        let listener = other.listen_tcp(tcp_config(), local(LOCAL_PORT)).unwrap();
        fixture.wait_op(listener);
        fixture.network.iteration(fixture.now).unwrap();
        fixture.network.iteration(fixture.now).unwrap();
        other.shutdown(listener);
        fixture.wait_op(listener);
        let mut client = fixture.client(1);
        let stream = *fixture.streams.last().unwrap();
        client.finish().unwrap();
        fixture.queue_lost(stream);
        // Shutdown consumes step one's only message. The FIFO then yields this end;
        // registration catch-up must stop at Open, before processing the queued Lost.
        fixture.network.iteration(fixture.now).unwrap();
        let mut events = Vec::new();
        fixture.until(&mut other, &mut events, |f, _| f.retired.contains(&stream));
        drop((client, other));
        fixture.close();
    });
}
