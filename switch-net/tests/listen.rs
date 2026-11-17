//! `NetHandle::tcp_listen` and `TcpListener::accept` against a raw network stack peer, owning
//! the card and sharing it; every future runs as a switch task.

use std::future::{poll_fn, Future};
use std::panic::{catch_unwind, resume_unwind, AssertUnwindSafe};
use std::pin::pin;
use std::task::Poll;

use dpdk_smoltcp::TcpHandle;
use dpdk_smoltcp::smoltcp::wire::Ipv4Cidr;
use switch_net::tcp::State;
use switch_net::{Error, IpEndpoint, Ipv4Address, TcpListener, TcpStream};
use switch_test_kit::pair::BUDGET;
use switch_test_kit::with_eal;

mod support;
use support::{
    cancel_after_one_poll, counted, local, local_at, net_ip, peer_send, tcp_config, Backend, Mixed, LOCAL_PORT,
    PEER_PORT,
};

/// `tcp_listen` on `LOCAL_PORT`, then one turn so that, sharing a card, the daemon opens the
/// sockets before a peer's SYN arrives.
fn listen(mixed: &mut Mixed, backlog: usize) -> TcpListener {
    let listener = mixed.handle().tcp_listen(tcp_config(), local(LOCAL_PORT), backlog).unwrap();
    mixed.turn();
    listener
}

/// A peer socket connecting from `peer_port` to the Net's `to`; not driven.
fn peer_connect_to(mixed: &mut Mixed, to: IpEndpoint, peer_port: u16) -> TcpHandle {
    let peer = mixed.peer.add_tcp(tcp_config()).unwrap();
    mixed.peer.connect(&peer, to, peer_port).unwrap();
    peer
}

/// `peer_connect_to` the Net's `LOCAL_PORT`.
fn peer_connect(mixed: &mut Mixed, peer_port: u16) -> TcpHandle {
    peer_connect_to(mixed, IpEndpoint::new(net_ip().into(), LOCAL_PORT), peer_port)
}

/// Drives until every peer is established, then one more round, in which the Net receives the
/// final ACK of the handshake.
fn established(mixed: &mut Mixed, peers: &[&TcpHandle]) {
    mixed.until(|m| peers.iter().all(|peer| m.peer.tcp(peer).state() == State::Established));
    mixed.round();
}

/// Drives until `peer` leaves its handshake or established state and asserts the Net reset
/// it, with no FIN first.
fn reset(mixed: &mut Mixed, peer: &TcpHandle, what: &str) {
    mixed.until(|m| matches!(m.peer.tcp(peer).state(), State::Closed | State::CloseWait));
    let socket = mixed.peer.tcp(peer);
    assert!(socket.state() == State::Closed && socket.reset_by_peer(), "{what}: {}", socket.state());
}

/// One poll of `future`: its output, or `None` if pending.
async fn poll_once<F: Future>(future: F) -> Option<F::Output> {
    let mut future = pin!(future);
    poll_fn(|cx| Poll::Ready(match future.as_mut().poll(cx) {
        Poll::Ready(output) => Some(output),
        Poll::Pending => None,
    }))
    .await
}

/// `accept` in a task, driven until it resolves.
fn accept(mixed: &mut Mixed, mut listener: TcpListener) -> (TcpListener, TcpStream) {
    let slot = mixed.spawn(async move {
        let stream = listener.accept().await;
        (listener, stream)
    });
    let (listener, stream) = mixed.wait(&slot);
    (listener, stream.expect("accept"))
}

/// One poll of `accept` in a task, then the future is dropped.
fn accept_once(mixed: &mut Mixed, mut listener: TcpListener) -> (TcpListener, Option<Result<TcpStream, Error>>) {
    let slot = mixed.spawn(async move {
        let polled = poll_once(listener.accept()).await;
        (listener, polled)
    });
    mixed.rt.poll(0).unwrap();
    slot.take()
}

/// Polls `accept` once per round until it no longer returns `SocketsFull`, so the listener has
/// sockets again. Owning the card, freed slots are reused at once; sharing a card, a slot is
/// free once the daemon's `Freed` arrives.
fn reopen(mixed: &mut Mixed, mut listener: TcpListener, backend: Backend) -> TcpListener {
    for attempt in 0..100 {
        let (back, polled) = accept_once(mixed, listener);
        listener = back;
        match polled {
            None => return listener,
            Some(Err(Error::SocketsFull)) => {
                assert!(matches!(backend, Backend::Junction), "owning the card, a freed slot was not reused");
                assert!(attempt < 99, "the listener's slots were never freed");
                mixed.round();
            }
            Some(other) => panic!("accept with no connection waiting gave {:?}", other.map(|_| ())),
        }
    }
    unreachable!()
}

/// The peer resets `peer` before any `accept`; drives until the Net has seen the reset, then
/// removes the peer socket. Only for a Net with no other TCP socket the peer reset.
fn peer_resets_unaccepted(mixed: &mut Mixed, peer: TcpHandle) {
    mixed.until(|m| m.peer.tcp(&peer).send_queue() == 0);
    mixed.peer.abort(&peer);
    mixed.peer.turn(mixed.now, BUDGET).unwrap();
    assert!(mixed.to_net.forward() >= 1, "the reset did not leave the peer");
    mixed.turn();
    mixed.until(|m| m.net.tcp_reset_by_peer());
    mixed.peer.remove_tcp(peer);
}

/// The peer resets `peer`; once `stream` reports it, drops the stream and removes the peer
/// socket.
fn peer_resets(mixed: &mut Mixed, peer: TcpHandle, mut stream: TcpStream) {
    mixed.peer.abort(&peer);
    let slot = mixed.spawn(async move {
        loop {
            if let Err(error) = stream.recv().await {
                break error;
            }
        }
    });
    assert_eq!(mixed.wait(&slot), Error::InvalidState);
    mixed.peer.remove_tcp(peer);
}

/// The payload of the stream's next frame.
fn recv(mixed: &mut Mixed, mut stream: TcpStream) -> (TcpStream, Vec<u8>) {
    let slot = mixed.spawn(async move {
        let frame = stream.recv().await.expect("recv");
        (stream, frame.payload().to_vec())
    });
    mixed.wait(&slot)
}

// Four peers connect in one round while no task is in `accept`; all four are established, and
// four `accept`s return the four connections.
support::both_backends!(backlog_n_takes_n_simultaneous_connects_conduit, backlog_n_takes_n_simultaneous_connects_junction, |backend| {
    with_eal(|eal| {
        let mut mixed = Mixed::with_sockets(eal, ["lbna", "lbnb"], 4, 0, backend);
        let mut listener = listen(&mut mixed, 4);
        let peers: Vec<_> = (0..4).map(|i| peer_connect(&mut mixed, PEER_PORT + i)).collect();
        established(&mut mixed, &peers.iter().collect::<Vec<_>>());
        for (i, peer) in peers.iter().enumerate() {
            peer_send(&mut mixed, peer, &[i as u8]);
        }
        let mut streams = Vec::new();
        let mut seen = Vec::new();
        for _ in 0..4 {
            let (back, stream) = accept(&mut mixed, listener);
            let (stream, payload) = recv(&mut mixed, stream);
            listener = back;
            streams.push(stream);
            seen.extend(payload);
        }
        seen.sort();
        assert_eq!(seen, [0, 1, 2, 3], "accept returned a connection twice or lost one");
        drop((listener, streams, peers));
        mixed.close();
    })
});

// With both sockets holding unaccepted connections, a third peer is reset. An `accept` opens a
// replacement, which takes the next peer.
support::both_backends!(connect_beyond_backlog_is_refused_conduit, connect_beyond_backlog_is_refused_junction, |backend| {
    with_eal(|eal| {
        let mut mixed = Mixed::with_sockets(eal, ["lcba", "lcbb"], 4, 0, backend);
        let listener = listen(&mut mixed, 2);
        let first = peer_connect(&mut mixed, PEER_PORT);
        let second = peer_connect(&mut mixed, PEER_PORT + 1);
        established(&mut mixed, &[&first, &second]);
        let refused = peer_connect(&mut mixed, PEER_PORT + 2);
        reset(&mut mixed, &refused, "a third connection got in with a backlog of 2");
        mixed.peer.remove_tcp(refused);
        let (listener, accepted) = accept(&mut mixed, listener);
        mixed.turn();
        let later = peer_connect(&mut mixed, PEER_PORT + 3);
        established(&mut mixed, &[&later]);
        let (listener, second_accepted) = accept(&mut mixed, listener);
        let (listener, third_accepted) = accept(&mut mixed, listener);
        drop((listener, accepted, second_accepted, third_accepted, first, second, later));
        mixed.close();
    })
});

// The peer sends bytes and a FIN before any `accept`; the accepted stream reads them, then
// `Finished`.
support::both_backends!(data_before_accept_is_readable_conduit, data_before_accept_is_readable_junction, |backend| {
    with_eal(|eal| {
        let mut mixed = Mixed::new(eal, ["ldba", "ldbb"], backend);
        let listener = listen(&mut mixed, 1);
        let peer = peer_connect(&mut mixed, PEER_PORT);
        established(&mut mixed, &[&peer]);
        peer_send(&mut mixed, &peer, b"early");
        mixed.peer.close(&peer);
        mixed.until(|m| m.peer.tcp(&peer).state() == State::FinWait2);
        let (listener, mut stream) = accept(&mut mixed, listener);
        let slot = mixed.spawn(async move {
            let frame = stream.recv().await.expect("data sent before accept");
            let end = stream.recv().await.unwrap_err();
            (stream, frame, end)
        });
        let (stream, frame, end) = mixed.wait(&slot);
        assert_eq!(frame.payload(), b"early");
        assert_eq!(end, Error::Finished);
        drop((listener, stream, frame, peer));
        mixed.close();
    })
});

/// Resolves ARP both ways (a refused connect from the peer), so later SYNs leave without
/// waiting for a neighbor.
fn resolve_neighbors(mixed: &mut Mixed) {
    let socket = mixed.peer.add_tcp(tcp_config()).unwrap();
    mixed.peer.connect(&socket, IpEndpoint::new(net_ip().into(), 1), PEER_PORT + 100).unwrap();
    mixed.until(|m| m.peer.tcp(&socket).state() == State::Closed);
    mixed.peer.remove_tcp(socket);
}

// The listener's one socket gets a SYN; the peer forgets its socket, so the SYN-ACK draws an
// RST. `accept` keeps waiting, and the socket takes the next connection.
support::both_backends!(reset_during_handshake_keeps_the_socket_conduit, reset_during_handshake_keeps_the_socket_junction, |backend| {
    with_eal(|eal| {
        let mut mixed = Mixed::new(eal, ["lrha", "lrhb"], backend);
        resolve_neighbors(&mut mixed);
        let mut listener = listen(&mut mixed, 1);
        let slot = mixed.spawn(async move {
            let stream = listener.accept().await;
            (listener, stream)
        });
        mixed.turn();
        let gone = peer_connect(&mut mixed, PEER_PORT);
        mixed.peer.turn(mixed.now, BUDGET).unwrap();
        assert_eq!(mixed.to_net.forward(), 1, "SYN");
        mixed.turn();
        assert_eq!(mixed.to_peer.forward(), 1, "SYN-ACK");
        mixed.peer.remove_tcp(gone);
        mixed.peer.turn(mixed.now, BUDGET).unwrap();
        assert_eq!(mixed.to_net.forward(), 1, "RST");
        mixed.turn();
        mixed.rounds(10);
        assert!(!slot.is_full(), "accept resolved on a reset handshake");
        let peer = peer_connect(&mut mixed, PEER_PORT + 1);
        let (listener, stream) = mixed.wait(&slot);
        let stream = stream.expect("the socket takes the next connection");
        established(&mut mixed, &[&peer]);
        drop((listener, stream, peer));
        mixed.close();
    })
});

// A connection that was established, received data and was reset by the peer before `accept`
// is still returned: its data is readable, then `recv` reports `InvalidState`, and it has no
// MSS (as `accept` documents).
support::both_backends!(closed_before_accept_is_returned_conduit, closed_before_accept_is_returned_junction, |backend| {
    with_eal(|eal| {
        let mut mixed = Mixed::new(eal, ["lcra", "lcrb"], backend);
        let listener = listen(&mut mixed, 1);
        let peer = peer_connect(&mut mixed, PEER_PORT);
        established(&mut mixed, &[&peer]);
        peer_send(&mut mixed, &peer, b"before reset");
        peer_resets_unaccepted(&mut mixed, peer);
        let (listener, stream) = accept(&mut mixed, listener);
        let (mut rx, tx) = stream.split();
        assert_eq!(tx.max_segment_payload(), None);
        let slot = mixed.spawn(async move {
            let frame = rx.recv().await.expect("data received before the reset");
            let end = rx.recv().await.unwrap_err();
            (rx, frame, end)
        });
        let (rx, frame, end) = mixed.wait(&slot);
        assert_eq!(frame.payload(), b"before reset");
        assert_eq!(end, Error::InvalidState);
        drop((listener, rx, tx, frame));
        mixed.close();
    })
});

// An `accept` future dropped after one poll leaves the listener's socket in place: a
// connection that completes afterwards is returned by the next `accept`.
support::both_backends!(dropping_accept_keeps_the_queue_conduit, dropping_accept_keeps_the_queue_junction, |backend| {
    with_eal(|eal| {
        let mut mixed = Mixed::new(eal, ["ldaa", "ldab"], backend);
        let mut listener = listen(&mut mixed, 1);
        let slot = mixed.spawn(async move {
            cancel_after_one_poll(listener.accept()).await;
            listener
        });
        let listener = mixed.wait(&slot);
        let peer = peer_connect(&mut mixed, PEER_PORT);
        established(&mut mixed, &[&peer]);
        peer_send(&mut mixed, &peer, b"after the drop");
        let (listener, stream) = accept(&mut mixed, listener);
        let (stream, payload) = recv(&mut mixed, stream);
        assert_eq!(payload, b"after the drop");
        drop((listener, stream, peer));
        mixed.close();
    })
});

/// Single-byte records the busy stream queues in the send ring. Sharing a card, the daemon
/// reads 64 records per iteration, so the ring stays non-empty for 64 rounds.
const RING_RECORDS: usize = 4096;

// Backlog 3 with two established connections nobody accepted, so one socket still listens;
// dropping the listener refuses a new connection to the port and resets both peers. Sharing a
// card, the listener's sockets must be aborted, not closed gracefully: the daemon holds a
// graceful close until every send ring is empty, and the listening socket would take the next
// SYN meanwhile. A busy stream keeps the send ring non-empty; an established stream dropped at
// the same time (a graceful close) shows the hold: its peer gets no FIN until after the
// listener's port refused a connection.
support::both_backends!(dropping_listener_resets_unaccepted_conduit, dropping_listener_resets_unaccepted_junction, |backend| {
    with_eal(|eal| {
        let mut mixed = Mixed::with_sockets(eal, ["ldla", "ldlb"], 6, 0, backend);
        let sharing = matches!(backend, Backend::Junction);
        let listener = listen(&mut mixed, 3);
        let first = peer_connect(&mut mixed, PEER_PORT + 10);
        let second = peer_connect(&mut mixed, PEER_PORT + 11);
        established(&mut mixed, &[&first, &second]);
        let mut busy = None;
        let mut witness = None;
        if sharing {
            let (stream, busy_peer) = support::connect(&mut mixed, LOCAL_PORT + 1);
            witness = Some(support::connect(&mut mixed, LOCAL_PORT + 2));
            let handle = mixed.handle();
            let slot = mixed.spawn(async move {
                let (rx, mut tx) = stream.split();
                for _ in 0..RING_RECORDS {
                    let mut frame = handle.tx_frame().await;
                    frame.as_mut()[0] = b'x';
                    frame.truncate(1);
                    tx.try_send(frame.freeze()).map_err(|(_, error)| error).expect("queue a record");
                }
                (rx, tx)
            });
            mixed.rt.poll(0).unwrap();
            busy = Some((slot.take(), busy_peer));
        }
        let witness_peer = witness.map(|(stream, peer)| {
            drop(stream);
            peer
        });
        drop(listener);
        let refused = peer_connect(&mut mixed, PEER_PORT + 12);
        reset(&mut mixed, &refused, "the dropped listener's port accepted a connection");
        reset(&mut mixed, &first, "an unaccepted connection was not reset");
        reset(&mut mixed, &second, "an unaccepted connection was not reset");
        if let Some(witness_peer) = &witness_peer {
            // The stream dropped with the listener is still open: its graceful close waits for
            // the send ring, which the listener's abort did not.
            assert_eq!(mixed.peer.tcp(witness_peer).state(), State::Established, "the send ring drained too early");
            // Once the ring drains, the held close sends its FIN.
            mixed.until(|m| m.peer.tcp(witness_peer).state() == State::CloseWait);
        }
        drop((busy, first, second, refused, witness_peer));
        mixed.close();
    })
});

// A table of two sockets, both taken by the listener: after an `accept`, no replacement can
// open, and the next `accept` waits on the remaining socket, which takes the next connection.
// Once both streams are closed and freed, `accept` reopens both sockets, and two simultaneous
// connects succeed.
support::both_backends!(short_listener_keeps_accepting_conduit, short_listener_keeps_accepting_junction, |backend| {
    with_eal(|eal| {
        let mut mixed = Mixed::with_sockets(eal, ["lsla", "lslb"], 2, 0, backend);
        let listener = listen(&mut mixed, 2);
        let first = peer_connect(&mut mixed, PEER_PORT);
        established(&mut mixed, &[&first]);
        let (mut listener, first_stream) = accept(&mut mixed, listener);
        let slot = mixed.spawn(async move {
            let stream = listener.accept().await;
            (listener, stream)
        });
        mixed.rounds(3);
        assert!(!slot.is_full(), "a listener short of sockets did not wait on the socket it has");
        let second = peer_connect(&mut mixed, PEER_PORT + 1);
        let (listener, second_stream) = mixed.wait(&slot);
        let second_stream = second_stream.expect("the remaining socket takes the connection");
        peer_resets(&mut mixed, first, first_stream);
        peer_resets(&mut mixed, second, second_stream);
        let listener = reopen(&mut mixed, listener, backend);
        // Sharing a card, a second `Freed` may arrive a round later; this poll opens its socket.
        mixed.rounds(2);
        let (listener, polled) = accept_once(&mut mixed, listener);
        assert!(polled.is_none(), "accept resolved with no connection waiting");
        let peers = [peer_connect(&mut mixed, PEER_PORT + 2), peer_connect(&mut mixed, PEER_PORT + 3)];
        established(&mut mixed, &[&peers[0], &peers[1]]);
        let (listener, third) = accept(&mut mixed, listener);
        let (listener, fourth) = accept(&mut mixed, listener);
        drop((listener, third, fourth, peers));
        mixed.close();
    })
});

// Backlog 1 in a table of 1: once the stream is handed out, the listener has no socket and
// `accept` returns `SocketsFull`; once the stream's socket is freed, `accept` works again.
support::both_backends!(listener_with_no_socket_returns_sockets_full_conduit, listener_with_no_socket_returns_sockets_full_junction, |backend| {
    with_eal(|eal| {
        let mut mixed = Mixed::with_sockets(eal, ["lnsa", "lnsb"], 1, 0, backend);
        let listener = listen(&mut mixed, 1);
        let first = peer_connect(&mut mixed, PEER_PORT);
        established(&mut mixed, &[&first]);
        let (listener, stream) = accept(&mut mixed, listener);
        let (listener, polled) = accept_once(&mut mixed, listener);
        assert!(matches!(polled, Some(Err(Error::SocketsFull))), "accept with no socket left did not fail");
        peer_resets(&mut mixed, first, stream);
        let listener = reopen(&mut mixed, listener, backend);
        let second = peer_connect(&mut mixed, PEER_PORT + 1);
        established(&mut mixed, &[&second]);
        let (listener, stream) = accept(&mut mixed, listener);
        drop((listener, stream, second));
        mixed.close();
    })
});

// A backlog larger than the free sockets fails with `SocketsFull` and keeps none of them: a
// backlog that fits then succeeds. Port 0 is `Unaddressable`: owning the card from
// `tcp_listen`; sharing a card the daemon rejects the open, and `accept` returns the error.
support::both_backends!(listen_fails_whole_conduit, listen_fails_whole_junction, |backend| {
    with_eal(|eal| {
        let mut mixed = Mixed::with_sockets(eal, ["llfa", "llfb"], 3, 0, backend);
        let handle = mixed.handle();
        match backend {
            Backend::Conduit => {
                assert!(matches!(handle.tcp_listen(tcp_config(), local(0), 1), Err(Error::Unaddressable)));
            }
            Backend::Junction => {
                let listener = handle.tcp_listen(tcp_config(), local(0), 1).expect("the session does not check the port");
                let slot = mixed.spawn(async move {
                    let mut listener = listener;
                    listener.accept().await.map(|_| ())
                });
                assert_eq!(mixed.wait(&slot), Err(Error::Unaddressable));
            }
        }
        assert!(matches!(handle.tcp_listen(tcp_config(), local(LOCAL_PORT), 4), Err(Error::SocketsFull)));
        let mut fitting = handle.tcp_listen(tcp_config(), local(LOCAL_PORT), 3);
        for _ in 0..100 {
            if fitting.is_ok() || matches!(backend, Backend::Conduit) {
                break;
            }
            // Sharing a card, the sockets of the failed listeners are free once `Freed` arrives.
            mixed.round();
            fitting = handle.tcp_listen(tcp_config(), local(LOCAL_PORT), 3);
        }
        let listener = fitting.expect("a failed tcp_listen kept sockets");
        mixed.turn();
        let peer = peer_connect(&mut mixed, PEER_PORT);
        established(&mut mixed, &[&peer]);
        let (listener, stream) = accept(&mut mixed, listener);
        drop((listener, stream, peer, handle));
        mixed.close();
    })
});

support::both_backends!(
    #[should_panic(expected = "backlog")]
    backlog_zero_panics_conduit, backlog_zero_panics_junction, |backend| {
    with_eal(|eal| {
        let mixed = Mixed::new(eal, ["lbza", "lbzb"], backend);
        let handle = mixed.handle();
        let result = catch_unwind(AssertUnwindSafe(|| handle.tcp_listen(tcp_config(), local(LOCAL_PORT), 0).map(drop)));
        drop(handle);
        mixed.close();
        resume_unwind(result.unwrap_err());
    })
});

const SECOND: Ipv4Address = Ipv4Address::new(10, 0, 0, 77);
const THIRD: Ipv4Address = Ipv4Address::new(10, 0, 0, 78);
const OWNED: &[Ipv4Cidr] = &[Ipv4Cidr::new(SECOND, 32), Ipv4Cidr::new(THIRD, 32)];

// The all-IP listener is opened first, yet a connection to the second address goes to the
// listener bound there without waking the all-IP listener's task; a connection to the third
// address, which has no listener of its own, goes to the all-IP one.
support::both_backends!(address_listener_beats_all_ip_listener_conduit, address_listener_beats_all_ip_listener_junction, |backend| {
    with_eal(|eal| {
        let mut mixed = Mixed::with_owned(eal, ["lala", "lalb"], OWNED, backend);
        let handle = mixed.handle();
        let mut all = handle.tcp_listen(tcp_config(), local(LOCAL_PORT), 1).unwrap();
        let mut exact = handle.tcp_listen(tcp_config(), local_at(SECOND, LOCAL_PORT), 1).unwrap();
        let (all, all_polls) = counted(async move {
            let stream = all.accept().await;
            (all, stream)
        });
        let all = mixed.spawn(all);
        let exact = mixed.spawn(async move {
            let stream = exact.accept().await;
            (exact, stream)
        });
        mixed.turn();
        let to_second = peer_connect_to(&mut mixed, IpEndpoint::new(SECOND.into(), LOCAL_PORT), PEER_PORT);
        established(&mut mixed, &[&to_second]);
        mixed.until(|_| exact.is_full());
        assert!(!all.is_full(), "the all-IP listener took the connection to the bound address");
        assert_eq!(all_polls.get(), 1, "the all-IP listener's task was woken");
        let to_third = peer_connect_to(&mut mixed, IpEndpoint::new(THIRD.into(), LOCAL_PORT), PEER_PORT + 1);
        established(&mut mixed, &[&to_third]);
        let (exact, exact_stream) = mixed.wait(&exact);
        let (all, all_stream) = mixed.wait(&all);
        drop((exact, exact_stream.unwrap(), all, all_stream.unwrap(), to_second, to_third, handle));
        mixed.close();
    })
});
