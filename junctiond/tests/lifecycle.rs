use std::io::{self, Read, Write};
use std::os::unix::net::UnixStream;
use std::time::Instant as WallInstant;

use dpdk_smoltcp::TcpHandle;
use dpdk_smoltcp::smoltcp::socket::tcp::State;
use junction::control::{
    ClientMessage, EndReason, Event, HostMessage, Op, SocketId, VERSION, WelcomeStatus,
};
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
        let fin = events
            .iter()
            .position(|event| *event == (id, Event::PeerFin))
            .unwrap();
        let closed = events
            .iter()
            .position(|event| {
                *event
                    == (
                        id,
                        Event::Closed {
                            reset_by_peer: false,
                        },
                    )
            })
            .unwrap();
        assert!(fin < closed);
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
        assert!(events.is_empty());
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
        assert_ne!(fixture.peer.tcp(&peer).state(), State::Established);
        fixture.to_net.paused = false;
        fixture.until(&mut client, &mut events, |f, _| {
            f.peer.tcp(&peer).state() == State::Listen
        });

        let new = client
            .connect_tcp(tcp_config(), peer_endpoint(PEER_PORT), local(LOCAL_PORT))
            .unwrap();
        assert_eq!(new.slot, old.slot);
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

fn same_generation_reopen_after_abort(shutdown: bool) {
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
        client.send(&[held, Op::AbortTcp { id }, reopen]);
        assert_eq!(fixture.wait_op(id), held);
        assert_eq!(fixture.wait_op(id), Op::AbortTcp { id });
        assert_eq!(fixture.wait_op(id), reopen);
        client.until(&mut fixture, |f, client| {
            client
                .messages
                .contains(&HostMessage::End(EndReason::ProtocolError))
                && f.peer.tcp(&peer).reset_by_peer()
        });
        assert!(!client.has(id, Event::Bound));
        let bound = unaffected.bind_udp(support::udp_config(7002)).unwrap();
        let mut events = Vec::new();
        fixture.until(&mut unaffected, &mut events, |_, events| {
            events.contains(&(bound, Event::Bound))
        });
        drop((client, unaffected, peer));
        fixture.close();
    });
}

#[zhida_test_support::pinned_test]
fn same_generation_reopen_rejects_held_shutdown() {
    same_generation_reopen_after_abort(true);
}

#[zhida_test_support::pinned_test]
fn same_generation_reopen_rejects_held_close() {
    same_generation_reopen_after_abort(false);
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
