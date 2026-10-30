//! switchnet against a raw conduit Stack peer on net_ring; every future runs
//! as a switch task.

use std::cell::RefCell;
use std::future::pending;
use std::rc::Rc;

use dpdk_smoltcp::TcpHandle;
use dpdk_smoltcp::smoltcp::time::Duration;
use switchnet::tcp::State;
use switchnet::{IpEndpoint, RecvError, StackError, TcpStream};
use zhida_test_kit::pair::{BUDGET, pattern};
use zhida_test_kit::with_eal;

mod support;
use support::{
    cancel_after_one_poll, connect, counted, local, net_ip, peer_endpoint, peer_listen, peer_send, peer_udp_send,
    tcp_config, udp_config, Mixed, LOCAL_PORT, PEER_PORT,
};

/// The peer's queued segments all acknowledged, then a reset from the peer
/// that reaches the Net and is turned.
fn peer_resets_after(mixed: &mut Mixed, peer: &TcpHandle) {
    mixed.until(|m| m.peer.tcp(peer).send_queue() == 0);
    mixed.peer.abort(peer);
    mixed.peer.turn(mixed.now, BUDGET).unwrap();
    assert!(mixed.to_net.forward() >= 1, "the reset did not leave the peer");
    mixed.turn();
}

/// Two `recv`s in one task.
fn recv_twice(mixed: &mut Mixed, mut stream: TcpStream) -> (Result<switchnet::TcpFrame, RecvError>, RecvError) {
    let slot = mixed.spawn(async move {
        let first = stream.recv().await;
        let second = stream.recv().await.unwrap_err();
        (first, second)
    });
    mixed.wait(&slot)
}

/// Resolves ARP both ways (a refused connect from the peer), so later SYNs
/// leave without waiting for a neighbor.
fn resolve_neighbors(mixed: &mut Mixed) {
    let socket = mixed.peer.add_tcp(tcp_config()).unwrap();
    mixed.peer.connect(&socket, IpEndpoint::new(net_ip().into(), 1), PEER_PORT + 1).unwrap();
    mixed.until(|m| m.peer.tcp(&socket).state() == State::Closed);
    mixed.peer.remove_tcp(socket);
}

#[zhida_test_support::pinned_test]
fn connect_resolves_and_wakes_the_task_once() {
    with_eal(|eal| {
        let mut mixed = Mixed::new(eal, ["cona", "conb"]);
        let peer = peer_listen(&mut mixed);
        let connecting = mixed.handle().tcp_connect(tcp_config(), peer_endpoint(PEER_PORT), local(LOCAL_PORT)).unwrap();
        let (connecting, polls) = counted(connecting);
        let slot = mixed.spawn(connecting);
        let stream = mixed.wait(&slot).unwrap();
        assert_eq!(polls.get(), 2, "polled once to park, once after the wake");
        mixed.until(|m| m.peer.tcp(&peer).state() == State::Established);
        drop((stream, peer));
        mixed.close();
    })
}

#[zhida_test_support::pinned_test]
fn refused_connect_reports_reset_by_peer() {
    with_eal(|eal| {
        let mut mixed = Mixed::new(eal, ["refa", "refb"]);
        let connecting = mixed.handle().tcp_connect(tcp_config(), peer_endpoint(PEER_PORT), local(LOCAL_PORT)).unwrap();
        let slot = mixed.spawn(connecting);
        let Err(closed) = mixed.wait(&slot) else { panic!("expected Closed") };
        assert!(closed.reset_by_peer);
        mixed.close();
    })
}

#[zhida_test_support::pinned_test]
fn data_arrives_in_order_as_frames() {
    with_eal(|eal| {
        let mut mixed = Mixed::new(eal, ["orda", "ordb"]);
        let (mut stream, peer) = connect(&mut mixed, LOCAL_PORT);
        let data = pattern(256 * 1024, 7);
        let len = data.len();
        let slot = mixed.spawn(async move {
            let (mut got, mut frames) = (Vec::new(), 0);
            while got.len() < len {
                let frame = stream.recv().await.unwrap();
                assert!(!frame.payload().is_empty());
                assert_eq!(frame.rx_timestamp(), 0, "net_ring has no RX timestamp offload");
                got.extend_from_slice(frame.payload());
                frames += 1;
            }
            (stream, got, frames)
        });
        let mut sent = 0;
        mixed.until(|m| {
            let room = m.peer.tcp(&peer).send_capacity() - m.peer.tcp(&peer).send_queue();
            let n = room.min(len - sent);
            if n > 0 {
                peer_send(m, &peer, &data[sent..sent + n]);
                sent += n;
            }
            slot.is_full()
        });
        let (stream, got, frames) = slot.take();
        assert_eq!(got, data);
        assert!(frames > 1);
        drop((stream, peer));
        mixed.close();
    })
}

#[zhida_test_support::pinned_test]
fn parked_task_is_woken_once_by_data_and_not_polled_on_idle_turns() {
    with_eal(|eal| {
        let mut mixed = Mixed::new(eal, ["onca", "oncb"]);
        let (mut stream, peer) = connect(&mut mixed, LOCAL_PORT);
        let (task, polls) = counted(async move {
            let frame = stream.recv().await.unwrap();
            (stream, frame)
        });
        let slot = mixed.spawn(task);
        for _ in 0..100 {
            mixed.turn();
        }
        assert_eq!(polls.get(), 1, "idle turns polled the parked task");
        peer_send(&mut mixed, &peer, b"data");
        let (stream, frame) = mixed.wait(&slot);
        assert_eq!(frame.payload(), b"data");
        assert_eq!(polls.get(), 2);
        drop((stream, frame, peer));
        mixed.close();
    })
}

#[zhida_test_support::pinned_test]
fn queued_data_then_reset_gives_the_data_then_invalid_state() {
    with_eal(|eal| {
        let mut mixed = Mixed::new(eal, ["qrsa", "qrsb"]);
        let (stream, peer) = connect(&mut mixed, LOCAL_PORT);
        peer_send(&mut mixed, &peer, b"before reset");
        peer_resets_after(&mut mixed, &peer);
        let (frame, error) = recv_twice(&mut mixed, stream);
        assert_eq!(frame.unwrap().payload(), b"before reset");
        assert_eq!(error, RecvError::InvalidState);
        drop(peer);
        mixed.close();
    })
}

/// The peer's FIN alone: the data before it is delivered, then `Finished`.
#[zhida_test_support::pinned_test]
fn accepted_fin_gives_finished() {
    with_eal(|eal| {
        let mut mixed = Mixed::new(eal, ["fina", "finb"]);
        let (stream, peer) = connect(&mut mixed, LOCAL_PORT);
        peer_send(&mut mixed, &peer, b"last");
        mixed.peer.tcp_mut(&peer).close();
        mixed.until(|m| m.peer.tcp(&peer).state() == State::FinWait2);
        let (frame, error) = recv_twice(&mut mixed, stream);
        assert_eq!(frame.unwrap().payload(), b"last");
        assert_eq!(error, RecvError::Finished);
        drop(peer);
        mixed.close();
    })
}

/// The peer's FIN, then its reset: the data before them is delivered, then the reset
/// (`InvalidState`), not a clean end. (Before, `Finished`: a caller could not tell it from a
/// FIN alone.)
#[zhida_test_support::pinned_test]
fn accepted_fin_then_reset_gives_the_reset() {
    with_eal(|eal| {
        let mut mixed = Mixed::new(eal, ["frsa", "frsb"]);
        let (stream, peer) = connect(&mut mixed, LOCAL_PORT);
        peer_send(&mut mixed, &peer, b"last");
        mixed.peer.close(&peer);
        mixed.until(|m| m.peer.tcp(&peer).state() == State::FinWait2);
        peer_resets_after(&mut mixed, &peer);
        assert_eq!(mixed.peer.tcp(&peer).state(), State::Closed);
        let (frame, error) = recv_twice(&mut mixed, stream);
        assert_eq!(frame.unwrap().payload(), b"last");
        assert_eq!(error, RecvError::InvalidState);
        drop(peer);
        mixed.close();
    })
}

#[zhida_test_support::pinned_test]
fn udp_datagrams_arrive_with_source_address() {
    with_eal(|eal| {
        let mut mixed = Mixed::new(eal, ["udpa", "udpb"]);
        let mut socket = mixed.handle().udp_bind(udp_config(7000)).unwrap();
        let peer = mixed.peer.add_udp(udp_config(7001)).unwrap();
        let slot = mixed.spawn(async move {
            let first = socket.recv().await;
            let second = socket.recv().await;
            (socket, [first, second])
        });
        let payloads: [&[u8]; 2] = [b"one", b"two"];
        peer_udp_send(&mut mixed, &peer, 7000, payloads[0]);
        let to = Some(IpEndpoint::new(net_ip().into(), 7000));
        mixed.peer.udp_send(&peer, to, payloads[1]).unwrap();
        let (socket, datagrams) = mixed.wait(&slot);
        for (datagram, payload) in datagrams.iter().zip(payloads) {
            assert_eq!(datagram.payload(), payload);
            assert_eq!(datagram.src(), IpEndpoint::new(support::peer_ip().into(), 7001));
            assert_eq!(datagram.dst_addr(), net_ip());
            assert_eq!(datagram.rx_timestamp(), 0);
        }
        drop((datagrams, socket, peer));
        mixed.close();
    })
}

/// A task drops its parked `recv` and waits on nothing; data then arriving
/// must not poll it.
#[zhida_test_support::pinned_test]
fn canceled_recv_clears_its_parked_handle() {
    with_eal(|eal| {
        let mut mixed = Mixed::new(eal, ["crca", "crcb"]);
        let (mut stream, peer) = connect(&mut mixed, LOCAL_PORT);
        let (task, polls) = counted(async move {
            cancel_after_one_poll(stream.recv()).await;
            pending::<()>().await;
            drop(stream);
        });
        mixed.spawn(task);
        mixed.turn();
        assert_eq!(polls.get(), 1);
        peer_send(&mut mixed, &peer, b"after cancel");
        mixed.until(|m| m.peer.tcp(&peer).send_queue() == 0);
        mixed.rounds(10);
        assert_eq!(polls.get(), 1, "canceled recv was woken");
        mixed.rt.shutdown();
        drop(peer);
        mixed.close();
    })
}

#[zhida_test_support::pinned_test]
fn canceled_udp_recv_clears_its_parked_handle() {
    with_eal(|eal| {
        let mut mixed = Mixed::new(eal, ["cura", "curb"]);
        let mut socket = mixed.handle().udp_bind(udp_config(7000)).unwrap();
        let peer = mixed.peer.add_udp(udp_config(7001)).unwrap();
        let (task, polls) = counted(async move {
            cancel_after_one_poll(socket.recv()).await;
            pending::<()>().await;
            drop(socket);
        });
        mixed.spawn(task);
        mixed.turn();
        peer_udp_send(&mut mixed, &peer, 7000, b"after cancel");
        mixed.rounds(10);
        assert_eq!(polls.get(), 1, "canceled recv was woken");
        mixed.rt.shutdown();
        drop(peer);
        mixed.close();
    })
}

/// Dropping `Connecting` drops its socket: nothing connects, nothing wakes.
#[zhida_test_support::pinned_test]
fn canceled_connect_drops_the_socket_without_waking() {
    with_eal(|eal| {
        let mut mixed = Mixed::new(eal, ["ccna", "ccnb"]);
        let peer = peer_listen(&mut mixed);
        let connecting = mixed.handle().tcp_connect(tcp_config(), peer_endpoint(PEER_PORT), local(LOCAL_PORT)).unwrap();
        let (task, polls) = counted(async move {
            cancel_after_one_poll(connecting).await;
            pending::<()>().await;
        });
        mixed.spawn(task);
        mixed.rounds(10);
        assert_eq!(polls.get(), 1);
        assert_eq!(mixed.peer.tcp(&peer).state(), State::Listen, "the dropped socket still connected");
        mixed.rt.shutdown();
        drop(peer);
        mixed.close();
    })
}

/// Dropping `Accepting` drops its listener: a later SYN is refused.
#[zhida_test_support::pinned_test]
fn canceled_accept_drops_the_listener_without_waking() {
    with_eal(|eal| {
        let mut mixed = Mixed::new(eal, ["ccaa", "ccab"]);
        let accepting = mixed.handle().tcp_accept(tcp_config(), local(LOCAL_PORT)).unwrap();
        let (task, polls) = counted(async move {
            cancel_after_one_poll(accepting).await;
            pending::<()>().await;
        });
        mixed.spawn(task);
        mixed.turn();
        let peer = mixed.peer.add_tcp(tcp_config()).unwrap();
        mixed.peer.connect(&peer, IpEndpoint::new(net_ip().into(), LOCAL_PORT), PEER_PORT).unwrap();
        mixed.until(|m| m.peer.tcp(&peer).state() == State::Closed);
        assert!(mixed.peer.tcp(&peer).reset_by_peer(), "the dropped listener accepted");
        assert_eq!(polls.get(), 1);
        mixed.rt.shutdown();
        drop(peer);
        mixed.close();
    })
}

#[zhida_test_support::pinned_test]
fn connect_and_accept_resolve_established() {
    with_eal(|eal| {
        let mut mixed = Mixed::new(eal, ["acca", "accb"]);
        let accepting = mixed.handle().tcp_accept(tcp_config(), local(LOCAL_PORT)).unwrap();
        let (accepting, polls) = counted(accepting);
        let slot = mixed.spawn(async move {
            let mut stream = accepting.await;
            let frame = stream.recv().await.unwrap();
            (stream, frame)
        });
        mixed.turn();
        let peer = mixed.peer.add_tcp(tcp_config()).unwrap();
        mixed.peer.connect(&peer, IpEndpoint::new(net_ip().into(), LOCAL_PORT), PEER_PORT).unwrap();
        mixed.until(|m| m.peer.tcp(&peer).state() == State::Established);
        peer_send(&mut mixed, &peer, b"hello");
        let (stream, frame) = mixed.wait(&slot);
        assert_eq!(frame.payload(), b"hello");
        assert_eq!(polls.get(), 2, "woken in Listen or SynReceived");
        drop((stream, frame, peer));
        mixed.close();
    })
}

/// The Net's listener gets a SYN (SynReceived); the peer forgets its socket,
/// so the Net's SYN-ACK draws an RST that returns the listener to Listen. The
/// accept keeps waiting and resolves on the next connection.
#[zhida_test_support::pinned_test]
fn accept_survives_rst_in_syn_received() {
    with_eal(|eal| {
        let mut mixed = Mixed::new(eal, ["arsa", "arsb"]);
        resolve_neighbors(&mut mixed);
        let accepting = mixed.handle().tcp_accept(tcp_config(), local(LOCAL_PORT)).unwrap();
        let (accepting, polls) = counted(accepting);
        let slot = mixed.spawn(accepting);
        mixed.turn();
        let net_end = IpEndpoint::new(net_ip().into(), LOCAL_PORT);
        let gone = mixed.peer.add_tcp(tcp_config()).unwrap();
        mixed.peer.connect(&gone, net_end, PEER_PORT).unwrap();
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
        assert_eq!(polls.get(), 1, "woken by the reset in SynReceived");
        let peer = mixed.peer.add_tcp(tcp_config()).unwrap();
        mixed.peer.connect(&peer, net_end, PEER_PORT + 2).unwrap();
        let stream = mixed.wait(&slot);
        mixed.until(|m| m.peer.tcp(&peer).state() == State::Established);
        assert_eq!(polls.get(), 2);
        drop((stream, peer));
        mixed.close();
    })
}

/// Two accepts on one port serve two connections.
#[zhida_test_support::pinned_test]
fn two_accepts_on_one_port_serve_two_connections() {
    with_eal(|eal| {
        let mut mixed = Mixed::new(eal, ["a2pa", "a2pb"]);
        let slots = [0, 1].map(|_| {
            let accepting = mixed.handle().tcp_accept(tcp_config(), local(LOCAL_PORT)).unwrap();
            mixed.spawn(accepting)
        });
        mixed.turn();
        let net_end = IpEndpoint::new(net_ip().into(), LOCAL_PORT);
        let peers = [PEER_PORT, PEER_PORT + 1].map(|port| {
            let peer = mixed.peer.add_tcp(tcp_config()).unwrap();
            mixed.peer.connect(&peer, net_end, port).unwrap();
            peer
        });
        let streams = slots.map(|slot| mixed.wait(&slot));
        mixed.until(|m| peers.iter().all(|peer| m.peer.tcp(peer).state() == State::Established));
        drop((streams, peers));
        mixed.close();
    })
}

#[zhida_test_support::pinned_test]
fn frames_outlive_the_stack() {
    with_eal(|eal| {
        let mut mixed = Mixed::new(eal, ["outa", "outb"]);
        let (mut stream, peer) = connect(&mut mixed, LOCAL_PORT);
        let mut udp = mixed.handle().udp_bind(udp_config(7000)).unwrap();
        let udp_peer = mixed.peer.add_udp(udp_config(7001)).unwrap();
        peer_send(&mut mixed, &peer, b"tcp bytes");
        let slot = mixed.spawn(async move { stream.recv().await.unwrap() });
        let frame = mixed.wait(&slot);
        peer_udp_send(&mut mixed, &udp_peer, 7000, b"udp bytes");
        let slot = mixed.spawn(async move { udp.recv().await });
        let datagram = mixed.wait(&slot);
        drop((peer, udp_peer));
        mixed.close_after_stacks(move || {
            let (mut frame, mut datagram) = (frame, datagram);
            assert_eq!(frame.payload(), b"tcp bytes");
            assert_eq!(datagram.payload(), b"udp bytes");
            frame.payload_mut()[0] = b'T';
            datagram.payload_mut()[0] = b'U';
            assert_eq!(frame.payload(), b"Tcp bytes");
            assert_eq!(datagram.payload(), b"Udp bytes");
        });
    })
}

/// The app holds every frame it receives: the window shuts and the peer
/// stalls. Releasing a middle and the last frame, the head still held,
/// reopens it and data resumes in order; releasing all lets the rest in.
#[zhida_test_support::pinned_test]
fn held_frames_shut_the_window_and_releasing_them_reopens_it() {
    with_eal(|eal| {
        let mut mixed = Mixed::new(eal, ["wina", "winb"]);
        let (mut stream, peer) = connect(&mut mixed, LOCAL_PORT);
        let data = pattern(64 * 1024, 3);
        let len = data.len();
        peer_send(&mut mixed, &peer, &data);
        let held = Rc::new(RefCell::new(Vec::new()));
        let got = Rc::new(RefCell::new(Vec::new()));
        let slot = mixed.spawn({
            let (held, got) = (held.clone(), got.clone());
            async move {
                while got.borrow().len() < len {
                    let frame = stream.recv().await.unwrap();
                    got.borrow_mut().extend_from_slice(frame.payload());
                    held.borrow_mut().push(frame);
                }
                stream
            }
        });
        let mut received = 0;
        loop {
            mixed.rounds(50);
            let now = got.borrow().len();
            if now == received {
                break;
            }
            received = now;
        }
        assert!(held.borrow().len() >= 3, "only {} frames held", held.borrow().len());
        assert!(received < len, "the window never shut");
        assert!(mixed.peer.tcp(&peer).send_queue() > 0, "the peer is not stalled");
        assert_eq!(got.borrow()[..], data[..received]);
        let head = held.borrow_mut().remove(0);
        {
            let mut held = held.borrow_mut();
            let middle = held.len() / 2;
            held.remove(middle);
            held.pop();
        }
        mixed.until(|_| got.borrow().len() > received);
        assert_eq!(got.borrow()[..], data[..got.borrow().len()], "data after the reopened window is out of order");
        drop(head);
        mixed.until(|_| {
            held.borrow_mut().clear();
            slot.is_full()
        });
        let stream = slot.take();
        assert_eq!(*got.borrow(), data);
        drop((stream, peer));
        mixed.close();
    })
}

/// Both ends send a SYN. The Net's turn that takes the peer's SYN moves it
/// to SynReceived, which must not wake the connecting task.
#[zhida_test_support::pinned_test]
fn simultaneous_open_waits_through_syn_received() {
    with_eal(|eal| {
        let mut mixed = Mixed::new(eal, ["sima", "simb"]);
        resolve_neighbors(&mut mixed);
        let peer = mixed.peer.add_tcp(tcp_config()).unwrap();
        mixed.peer.connect(&peer, IpEndpoint::new(net_ip().into(), LOCAL_PORT), PEER_PORT).unwrap();
        let connecting = mixed.handle().tcp_connect(tcp_config(), peer_endpoint(PEER_PORT), local(LOCAL_PORT)).unwrap();
        let (connecting, polls) = counted(connecting);
        let slot = mixed.spawn(connecting);
        mixed.rt.poll(0).unwrap();
        assert_eq!(polls.get(), 1);
        // The peer's SYN reaches the Net while the Net is in SynSent and has sent nothing.
        mixed.peer.turn(mixed.now, BUDGET).unwrap();
        assert_eq!(mixed.peer.tcp(&peer).state(), State::SynSent);
        assert_eq!(mixed.to_net.forward(), 1);
        mixed.turn();
        assert_eq!(polls.get(), 1, "woken in SynReceived");
        assert_eq!(mixed.to_peer.forward(), 1);
        let stream = mixed.wait(&slot).unwrap();
        assert_eq!(polls.get(), 2);
        mixed.until(|m| m.peer.tcp(&peer).state() == State::Established);
        drop((stream, peer));
        mixed.close();
    })
}

/// A turn that fails before ingress (time went backwards) wakes nothing and
/// keeps the parked task; the next turn takes in the datagram and wakes it.
#[zhida_test_support::pinned_test]
fn error_turn_keeps_the_parked_task_for_the_next_turn() {
    with_eal(|eal| {
        let mut mixed = Mixed::new(eal, ["erra", "errb"]);
        let earlier = mixed.now;
        mixed.now = earlier + Duration::from_millis(1);
        let mut socket = mixed.handle().udp_bind(udp_config(7000)).unwrap();
        let peer = mixed.peer.add_udp(udp_config(7001)).unwrap();
        peer_udp_send(&mut mixed, &peer, 7000, b"warm");
        let (task, polls) = counted(async move {
            let warm = socket.recv().await;
            assert_eq!(warm.payload(), b"warm");
            let datagram = socket.recv().await;
            (socket, warm, datagram)
        });
        let slot = mixed.spawn(task);
        mixed.until(|_| polls.get() >= 2);
        mixed.rounds(5);
        let parked = polls.get();
        assert!(!slot.is_full());
        mixed.peer.udp_send(&peer, Some(IpEndpoint::new(net_ip().into(), 7000)), b"after error").unwrap();
        mixed.peer.turn(mixed.now, BUDGET).unwrap();
        assert_eq!(mixed.to_net.forward(), 1);
        let Err(StackError::TimeWentBackwards { .. }) = mixed.net.turn(earlier) else {
            panic!("turn at an earlier time did not fail")
        };
        mixed.rt.poll(0).unwrap();
        assert_eq!(polls.get(), parked, "the failed turn woke the task");
        mixed.turn();
        assert_eq!(polls.get(), parked + 1);
        let (socket, warm, datagram) = slot.take();
        assert_eq!(datagram.payload(), b"after error");
        drop((socket, warm, datagram, peer));
        mixed.close();
    })
}

/// A socket dropped while its receive token is still armed (no turn in
/// between) frees the index at once; a new socket that reuses it and parks is
/// woken by its own data.
#[zhida_test_support::pinned_test]
fn socket_reusing_a_just_freed_index_is_woken() {
    with_eal(|eal| {
        let mut mixed = Mixed::new(eal, ["reua", "reub"]);
        let peer = mixed.peer.add_udp(udp_config(7001)).unwrap();
        peer_udp_send(&mut mixed, &peer, 7009, b"resolve");
        let mut first = mixed.handle().udp_bind(udp_config(7000)).unwrap();
        let slot = mixed.spawn(async move { cancel_after_one_poll(first.recv()).await });
        mixed.rt.poll(0).unwrap();
        mixed.wait(&slot);
        let mut second = mixed.handle().udp_bind(udp_config(7000)).unwrap();
        let slot = mixed.spawn(async move {
            let datagram = second.recv().await;
            (second, datagram)
        });
        mixed.rt.poll(0).unwrap();
        mixed.peer.udp_send(&peer, Some(IpEndpoint::new(net_ip().into(), 7000)), b"second").unwrap();
        let (second, datagram) = mixed.wait(&slot);
        assert_eq!(datagram.payload(), b"second");
        drop((second, datagram, peer));
        mixed.close();
    })
}
