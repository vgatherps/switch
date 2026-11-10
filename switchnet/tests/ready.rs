//! Conduit-only: readiness-token internals, not the socket API.

//! Which parked tasks a `Net::turn` wakes: only those whose socket's readiness
//! token fired and whose socket is ready, also after their entry was
//! canceled, freed or reused. Every future runs as a switch task.

use std::cell::RefCell;
use std::future::poll_fn;
use std::rc::Rc;
use std::task::{Poll, Waker};

use switchnet::tcp;
use switchnet::{first, IpEndpoint, TcpStream};
use zhida_test_kit::pair::{BUDGET, pattern};
use zhida_test_kit::with_eal;

mod support;
use support::{
    bind,
    cancel_after_one_poll, connect, connect_with, counted, local, net_ip, peer_drain, peer_endpoint, peer_listen,
    peer_send, peer_udp_send, tcp_config, udp_config, Mixed, LOCAL_PORT, PEER_PORT,
};

/// Opened by the test; a task awaiting it is woken then.
#[derive(Clone, Default)]
struct Gate(Rc<RefCell<(bool, Option<Waker>)>>);

impl Gate {
    fn open(&self) {
        let mut state = self.0.borrow_mut();
        state.0 = true;
        if let Some(waker) = state.1.take() {
            waker.wake();
        }
    }

    async fn wait(&self) {
        poll_fn(|cx| {
            let mut state = self.0.borrow_mut();
            if state.0 {
                Poll::Ready(())
            } else {
                state.1 = Some(cx.waker().clone());
                Poll::Pending
            }
        })
        .await
    }
}

/// The peer sends `payload` to the Net's UDP `port`; one Net turn takes it in
/// and polls the woken tasks.
fn datagram_to(mixed: &mut Mixed, peer: &dpdk_smoltcp::UdpHandle, port: u16, payload: &[u8]) {
    mixed.peer.udp_send(peer, Some(IpEndpoint::new(net_ip().into(), port)), payload).unwrap();
    mixed.peer.turn(mixed.now, BUDGET).unwrap();
    assert_eq!(mixed.to_net.forward(), 1);
    mixed.turn();
}

const SOCKETS: usize = 1000;
const BASE: u16 = 20_000;

/// 1000 tasks parked on UDP receive: idle turns poll none of them, and one
/// datagram wakes exactly its receiver.
#[zhida_test_support::pinned_test]
fn one_of_a_thousand_parked_receivers_wakes() {
    with_eal(|eal| {
        let mut mixed = Mixed::with_sockets(eal, ["tha", "thb"], 1, SOCKETS + 1, support::Backend::Conduit);
        let peer = mixed.peer.add_udp(udp_config(7001)).unwrap();
        peer_udp_send(&mut mixed, &peer, 7009, b"resolve");
        mixed.rounds(2);
        let tasks: Vec<_> = (0..SOCKETS)
            .map(|i| {
                let mut socket = bind(&mut mixed, udp_config(BASE + i as u16));
                let (task, polls) = counted(async move {
                    let datagram = socket.recv().await;
                    (socket, datagram)
                });
                (mixed.spawn(task), polls)
            })
            .collect();
        mixed.rt.poll(0).unwrap();
        for _ in 0..20 {
            mixed.turn();
        }
        assert!(tasks.iter().all(|(_, polls)| polls.get() == 1), "an idle turn polled a parked task");
        let target = 617;
        datagram_to(&mut mixed, &peer, BASE + target as u16, b"one");
        for (i, (slot, polls)) in tasks.iter().enumerate() {
            if i == target {
                assert_eq!(polls.get(), 2);
                let (socket, datagram) = slot.take();
                assert_eq!(datagram.payload(), b"one");
                drop((socket, datagram));
            } else {
                assert_eq!(polls.get(), 1, "task {i} was polled");
                assert!(!slot.is_full());
            }
        }
        drop(peer);
        mixed.close();
    })
}

/// A reader and a writer parked on the two halves of one stream: data wakes
/// only the reader, the peer's ACKs only the writer.
#[zhida_test_support::pinned_test]
fn halves_of_one_stream_wake_independently() {
    with_eal(|eal| {
        let mut mixed = Mixed::new(eal, ["hlva", "hlvb"], support::Backend::Conduit);
        let stalling = tcp::Config { tx_capacity: 1500, ..tcp_config() };
        let (stream, peer) = connect_with(&mut mixed, LOCAL_PORT, stalling);
        let (mut rx, mut tx) = stream.split();
        let (reader, reads) = counted(async move {
            let frame = rx.recv().await.unwrap();
            (rx, frame)
        });
        let reader = mixed.spawn(reader);
        let data = pattern(2000, 4);
        let handle = mixed.handle();
        let sent = data.clone();
        let (writer, writes) = counted(async move {
            for chunk in sent.chunks(1000) {
                let mut frame = handle.tx_frame().await;
                frame.as_mut()[..chunk.len()].copy_from_slice(chunk);
                frame.truncate(chunk.len());
                let frame = frame.freeze();
                tx.room(chunk.len()).await.unwrap();
                tx.try_send(frame).map_err(|(_, error)| error).unwrap();
            }
            tx
        });
        let writer = mixed.spawn(writer);
        mixed.to_peer.paused = true;
        mixed.turn();
        let (parked_reads, parked_writes) = (reads.get(), writes.get());
        for _ in 0..10 {
            mixed.turn();
        }
        assert_eq!((reads.get(), writes.get()), (parked_reads, parked_writes), "parked tasks were polled");
        assert!(!writer.is_full());

        peer_send(&mut mixed, &peer, b"to the reader");
        mixed.peer.turn(mixed.now, BUDGET).unwrap();
        assert!(mixed.to_net.forward() > 0);
        mixed.turn();
        assert!(reader.is_full());
        assert_eq!(reads.get(), parked_reads + 1);
        assert_eq!(writes.get(), parked_writes, "data for the reader polled the writer");
        let (rx, frame) = reader.take();
        assert_eq!(frame.payload(), b"to the reader");

        mixed.to_peer.paused = false;
        let mut got = Vec::new();
        mixed.until(|m| {
            peer_drain(m, &peer, &mut got);
            writer.is_full()
        });
        assert!(writes.get() > parked_writes);
        mixed.until(|m| {
            peer_drain(m, &peer, &mut got);
            got.len() == data.len()
        });
        assert_eq!(got, data);
        let tx = writer.take();
        drop((rx, frame, tx, peer));
        mixed.close();
    })
}

/// A recv parked, its token fired by the send half (`shutdown` changes the
/// socket between turns), then the recv canceled and the whole stream dropped
/// before any turn. The freed entry is reused by a UDP socket whose task
/// parks: the next turn's check of the old token arms the new socket, and its
/// datagram wakes it once.
#[zhida_test_support::pinned_test]
fn token_fired_then_canceled_then_entry_reused_before_the_turn() {
    with_eal(|eal| {
        let mut mixed = Mixed::new(eal, ["fcra", "fcrb"], support::Backend::Conduit);
        let peer = mixed.peer.add_udp(udp_config(7001)).unwrap();
        peer_udp_send(&mut mixed, &peer, 7009, b"resolve");
        let (stream, tcp_peer) = connect(&mut mixed, LOCAL_PORT);
        let (mut rx, mut tx) = stream.split();
        let gate = Gate::default();
        let canceled = mixed.spawn({
            let gate = gate.clone();
            async move {
                assert!(first(rx.recv(), gate.wait()).await.is_none());
                drop(rx);
            }
        });
        mixed.rt.poll(0).unwrap();
        tx.shutdown();
        gate.open();
        mixed.rt.poll(0).unwrap();
        canceled.take();
        drop(tx);

        let mut socket = bind(&mut mixed, udp_config(7000));
        let (task, polls) = counted(async move {
            let datagram = socket.recv().await;
            (socket, datagram)
        });
        let slot = mixed.spawn(task);
        mixed.rt.poll(0).unwrap();
        mixed.turn();
        mixed.turn();
        assert_eq!(polls.get(), 1, "the old token woke the new socket's task");
        datagram_to(&mut mixed, &peer, 7000, b"reused");
        assert_eq!(polls.get(), 2);
        let (socket, datagram) = slot.take();
        assert_eq!(datagram.payload(), b"reused");
        drop((socket, datagram, peer, tcp_peer));
        mixed.close();
    })
}

/// As above, but the turn checks the fired token while its entry is free; a
/// socket opened afterwards in that entry arms its own token.
#[zhida_test_support::pinned_test]
fn token_fired_then_socket_dropped_then_turn() {
    with_eal(|eal| {
        let mut mixed = Mixed::new(eal, ["fsda", "fsdb"], support::Backend::Conduit);
        let peer = mixed.peer.add_udp(udp_config(7001)).unwrap();
        peer_udp_send(&mut mixed, &peer, 7009, b"resolve");
        let (stream, tcp_peer) = connect(&mut mixed, LOCAL_PORT);
        let (mut rx, mut tx) = stream.split();
        let gate = Gate::default();
        let canceled = mixed.spawn({
            let gate = gate.clone();
            async move {
                assert!(first(rx.recv(), gate.wait()).await.is_none());
                drop(rx);
            }
        });
        mixed.rt.poll(0).unwrap();
        tx.shutdown();
        gate.open();
        mixed.rt.poll(0).unwrap();
        canceled.take();
        drop(tx);
        mixed.turn();

        let mut socket = bind(&mut mixed, udp_config(7000));
        let (task, polls) = counted(async move {
            let datagram = socket.recv().await;
            (socket, datagram)
        });
        let slot = mixed.spawn(task);
        mixed.turn();
        assert_eq!(polls.get(), 1);
        datagram_to(&mut mixed, &peer, 7000, b"after");
        assert_eq!(polls.get(), 2);
        let (socket, datagram) = slot.take();
        assert_eq!(datagram.payload(), b"after");
        drop((socket, datagram, peer, tcp_peer));
        mixed.close();
    })
}

/// A recv canceled and a new recv parked on the same half between two turns:
/// the token armed by the first serves the second, which data wakes once.
#[zhida_test_support::pinned_test]
fn recv_canceled_and_parked_again_between_turns() {
    with_eal(|eal| {
        let mut mixed = Mixed::new(eal, ["crpa", "crpb"], support::Backend::Conduit);
        let (mut stream, peer) = connect(&mut mixed, LOCAL_PORT);
        let gate = Gate::default();
        let (task, polls) = counted({
            let gate = gate.clone();
            async move {
                assert!(first(stream.recv(), gate.wait()).await.is_none());
                let frame = stream.recv().await.unwrap();
                (stream, frame)
            }
        });
        let slot = mixed.spawn(task);
        mixed.turn();
        assert_eq!(polls.get(), 1);
        gate.open();
        mixed.rt.poll(0).unwrap();
        assert_eq!(polls.get(), 2);
        mixed.rounds(5);
        assert_eq!(polls.get(), 2);
        peer_send(&mut mixed, &peer, b"second");
        let (stream, frame) = mixed.wait(&slot);
        assert_eq!(polls.get(), 3);
        assert_eq!(frame.payload(), b"second");
        drop((stream, frame, peer));
        mixed.close();
    })
}

/// A parked reader's token fires on a send-half change between turns (a
/// writer task's `try_send` changes the socket): the next turn checks it,
/// finds no data and re-arms without polling the reader; data later wakes it
/// once.
#[zhida_test_support::pinned_test]
fn token_fired_by_the_other_half_rearms_without_a_wake() {
    with_eal(|eal| {
        let mut mixed = Mixed::new(eal, ["oha", "ohb"], support::Backend::Conduit);
        let (stream, peer) = connect(&mut mixed, LOCAL_PORT);
        let (mut rx, mut tx) = stream.split();
        let (task, polls) = counted(async move {
            let frame = rx.recv().await.unwrap();
            (rx, frame)
        });
        let slot = mixed.spawn(task);
        mixed.turn();
        assert_eq!(polls.get(), 1);
        let handle = mixed.handle();
        let writer = mixed.spawn(async move {
            let mut frame = handle.tx_frame().await;
            frame.as_mut()[..3].copy_from_slice(b"out");
            frame.truncate(3);
            let frame = frame.freeze();
            tx.try_send(frame).map_err(|(_, error)| error).unwrap();
            tx
        });
        mixed.rt.poll(0).unwrap();
        let tx = writer.take();
        mixed.turn();
        assert_eq!(polls.get(), 1, "the other half's change woke the reader");
        let mut got = Vec::new();
        mixed.until(|m| {
            peer_drain(m, &peer, &mut got);
            got.len() == 3
        });
        assert_eq!(polls.get(), 1);
        peer_send(&mut mixed, &peer, b"in");
        let (rx, frame) = mixed.wait(&slot);
        assert_eq!(polls.get(), 2);
        assert_eq!(frame.payload(), b"in");
        drop((rx, frame, tx, peer));
        mixed.close();
    })
}

/// Parks `delivered` on the stream's send side and cancels it, then changes the
/// socket (`shutdown` again), which fires the still armed send token; drops the
/// stream. No Net turn runs, so the fired token waits to be popped while the
/// entry is already free.
fn fire_send_token_and_drop(mixed: &mut Mixed, stream: TcpStream) {
    let (rx, mut tx) = stream.split();
    let slot = mixed.spawn(async move {
        tx.shutdown();
        cancel_after_one_poll(tx.delivered()).await;
        tx.shutdown();
        tx
    });
    mixed.rt.poll(0).unwrap();
    drop((rx, slot.take()));
}

/// A UDP socket takes the entry while the previous TCP socket's fired send
/// token is still out. Dropping the UDP send half, which never arms a token,
/// must leave the UDP receive token alone: the parked receiver is woken by
/// its datagram.
#[zhida_test_support::pinned_test]
fn udp_socket_reusing_an_entry_with_a_tcp_send_token_out_drops_its_send_half() {
    with_eal(|eal| {
        let mut mixed = Mixed::new(eal, ["xkua", "xkub"], support::Backend::Conduit);
        let (stream, tcp_peer) = connect(&mut mixed, LOCAL_PORT);
        fire_send_token_and_drop(&mut mixed, stream);
        let (mut rx, tx) = bind(&mut mixed, udp_config(7000)).split();
        let (task, polls) = counted(async move {
            let datagram = rx.recv().await;
            (rx, datagram)
        });
        let slot = mixed.spawn(task);
        mixed.rt.poll(0).unwrap();
        assert_eq!(polls.get(), 1);
        drop(tx);
        let peer = mixed.peer.add_udp(udp_config(7001)).unwrap();
        peer_udp_send(&mut mixed, &peer, 7000, b"after reuse");
        let (rx, datagram) = mixed.wait(&slot);
        assert_eq!(datagram.payload(), b"after reuse");
        assert_eq!(polls.get(), 2);
        drop((rx, datagram, peer, tcp_peer));
        mixed.close();
    })
}

/// A TCP socket takes the entry while the previous TCP socket's fired send
/// token is still out; it connects and, after `shutdown`, waits in
/// `delivered` until the peer acks the FIN.
#[zhida_test_support::pinned_test]
fn tcp_socket_reusing_an_entry_with_a_send_token_out_waits_and_wakes() {
    with_eal(|eal| {
        let mut mixed = Mixed::new(eal, ["xkta", "xktb"], support::Backend::Conduit);
        let (stream, old_peer) = connect(&mut mixed, LOCAL_PORT);
        fire_send_token_and_drop(&mut mixed, stream);
        let peer = peer_listen(&mut mixed);
        let connecting =
            mixed.handle().tcp_connect(tcp_config(), peer_endpoint(PEER_PORT), local(LOCAL_PORT + 1));
        let (task, polls) = counted(async move {
            let (rx, mut tx) = connecting.await.expect("connect").split();
            drop(rx);
            tx.shutdown();
            tx.delivered().await;
            tx
        });
        let slot = mixed.spawn(task);
        mixed.rt.poll(0).unwrap();
        let tx = mixed.wait(&slot);
        assert_eq!(polls.get(), 3, "parked in connect, then in delivered, each woken once");
        assert_eq!(mixed.peer.tcp(&peer).state(), tcp::State::CloseWait);
        drop((tx, peer, old_peer));
        mixed.close();
    })
}
