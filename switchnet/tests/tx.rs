//! switchnet send side against a raw conduit Stack peer on net_ring; every
//! future runs as a switch task.

use std::cell::{Cell, RefCell};
use std::future::{Future, pending, poll_fn};
use std::pin::pin;
use std::rc::Rc;
use std::task::Poll;

use switchnet::tcp::{self, State};
use switchnet::{IpEndpoint, NetHandle, RecvError, SendError, SendFrameError, TcpTx, TxFrame};
use zhida_test_kit::pair::{BUDGET, pattern};
use zhida_test_kit::with_eal;

mod support;
use support::{
    cancel_after_one_poll, connect, connect_with, counted, net_ip, peer_drain, peer_ip, peer_send, peer_udp_send,
    tcp_config, udp_config, Mixed, LOCAL_PORT,
};

/// Bytes per frame: below the 1460-byte segment payload, so frames go out whole.
const CHUNK: usize = 1000;

/// A send buffer larger than the peer's advertised receive window.
fn stalling_config() -> tcp::Config {
    tcp::Config { tx_capacity: 32 * 1024, ..tcp_config() }
}

/// `data` in a fresh frame from `handle`.
async fn filled(handle: &NetHandle, data: &[u8]) -> TxFrame {
    let mut frame = handle.tx_frame().await;
    frame.room_mut()[..data.len()].copy_from_slice(data);
    frame.set_len(data.len());
    frame
}

/// `data` in frames of `CHUNK` bytes, each sent after `room` resolves.
async fn send_all(handle: &NetHandle, tx: &mut TcpTx, data: &[u8]) {
    for chunk in data.chunks(CHUNK) {
        let frame = filled(handle, chunk).await;
        tx.room(chunk.len()).await.unwrap();
        tx.try_send(frame).map_err(|(_, error)| error).unwrap();
    }
}

/// One poll of `future`: its output, or `None` if pending.
async fn poll_once<F: Future>(future: F) -> Option<F::Output> {
    let mut future = pin!(future);
    poll_fn(|cx| Poll::Ready(match future.as_mut().poll(cx) {
        Poll::Ready(out) => Some(out),
        Poll::Pending => None,
    }))
    .await
}

/// Every free frame of the Net's pool, held, with no task left in its frame-wait queue.
/// The hoarder's last `tx_frame` stays queued when it finds the pool empty; one frame
/// returns so a receive wakes that entry off the queue, then a wait that succeeds at
/// once takes the frame back.
fn take_every_frame(mixed: &mut Mixed) -> Rc<RefCell<Vec<TxFrame>>> {
    let handle = mixed.handle();
    let held = Rc::new(RefCell::new(Vec::new()));
    let hoarder = mixed.spawn({
        let held = held.clone();
        async move {
            while let Some(frame) = poll_once(handle.tx_frame()).await {
                held.borrow_mut().push(frame);
            }
        }
    });
    mixed.wait(&hoarder);
    held.borrow_mut().pop();
    mixed.turn();
    let handle = mixed.handle();
    let last = mixed.spawn(async move { poll_once(handle.tx_frame()).await.expect("the returned frame") });
    let frame = mixed.wait(&last);
    held.borrow_mut().push(frame);
    held
}

/// Rounds, draining the peer, until it holds `len` bytes.
fn peer_receive(mixed: &mut Mixed, peer: &dpdk_smoltcp::TcpHandle, len: usize) -> Vec<u8> {
    let mut got = Vec::new();
    mixed.until(|m| {
        peer_drain(m, peer, &mut got);
        got.len() >= len
    });
    got
}

/// `try_send` with no room returns the frame it was given, untouched; after
/// the peer reads, `room` then `try_send` of that same frame succeed.
#[zhida_test_support::pinned_test]
fn try_send_full_returns_the_same_frame() {
    with_eal(|eal| {
        let mut mixed = Mixed::new(eal, ["tsfa", "tsfb"]);
        let (stream, peer) = connect_with(&mut mixed, LOCAL_PORT, stalling_config());
        let (_rx, mut tx) = stream.split();
        let handle = mixed.handle();
        let slot = mixed.spawn(async move {
            let mut sent = Vec::new();
            loop {
                let chunk = pattern(CHUNK, sent.len() as u32);
                let frame = filled(&handle, &chunk).await;
                let at = frame.payload().as_ptr();
                match tx.try_send(frame) {
                    Ok(()) => sent.extend_from_slice(&chunk),
                    Err((frame, error)) => {
                        assert_eq!(error, SendFrameError::Full);
                        assert_eq!(frame.payload().as_ptr(), at, "a different frame came back");
                        assert_eq!(frame.payload(), &chunk[..]);
                        tx.room(frame.len()).await.unwrap();
                        tx.try_send(frame).map_err(|(_, error)| error).unwrap();
                        sent.extend_from_slice(&chunk);
                        return (tx, sent);
                    }
                }
            }
        });
        let mut got = Vec::new();
        mixed.until(|m| {
            peer_drain(m, &peer, &mut got);
            slot.is_full()
        });
        let (tx, sent) = slot.take();
        got.extend(peer_receive(&mut mixed, &peer, sent.len() - got.len()));
        assert_eq!(got, sent);
        drop((tx, _rx, peer));
        mixed.close();
    })
}

/// With three tasks waiting for a frame, one frame returning wakes only the oldest
/// waiter: it takes the frame and the other two are not polled.
#[zhida_test_support::pinned_test]
fn one_free_frame_wakes_one_of_many_waiters() {
    with_eal(|eal| {
        let mut mixed = Mixed::new(eal, ["txoa", "txob"]);
        let held = take_every_frame(&mut mixed);
        let waiters: Vec<_> = (0..3)
            .map(|_| {
                let handle = mixed.handle();
                let (task, polls) = counted(async move { handle.tx_frame().await });
                (mixed.spawn(task), polls)
            })
            .collect();
        mixed.rounds(10);
        assert!(waiters.iter().all(|(slot, polls)| !slot.is_full() && polls.get() == 1));
        held.borrow_mut().pop();
        mixed.turn();
        let polls: Vec<_> = waiters.iter().map(|(_, polls)| polls.get()).collect();
        assert_eq!(polls, [2, 1, 1], "one free frame polled more than its one waiter");
        assert!(waiters[0].0.is_full());
        drop((waiters, held));
        mixed.close();
    })
}

/// A reader task and a writer task on the two halves of one stream; the peer
/// echoes every byte back.
#[zhida_test_support::pinned_test]
fn split_halves_echo_through_the_peer() {
    with_eal(|eal| {
        let mut mixed = Mixed::new(eal, ["echa", "echb"]);
        let (stream, peer) = connect(&mut mixed, LOCAL_PORT);
        let (mut rx, mut tx) = stream.split();
        let data = pattern(200 * 1024, 11);
        let len = data.len();
        let handle = mixed.handle();
        let sent = data.clone();
        let writer = mixed.spawn(async move {
            send_all(&handle, &mut tx, &sent).await;
            tx
        });
        let reader = mixed.spawn(async move {
            let mut got = Vec::new();
            while got.len() < len {
                got.extend_from_slice(rx.recv().await.unwrap().payload());
            }
            (rx, got)
        });
        let (mut echoed, mut pending) = (0, Vec::new());
        mixed.until(|m| {
            peer_drain(m, &peer, &mut pending);
            let n = m.peer.tcp_mut(&peer).send_slice(&pending[echoed..]).unwrap();
            echoed += n;
            writer.is_full() && reader.is_full()
        });
        assert_eq!(pending, data);
        let (rx, got) = reader.take();
        assert_eq!(got, data);
        drop((writer.take(), rx, peer));
        mixed.close();
    })
}

/// A writer parked on the full send buffer resolves with an error on reset.
#[zhida_test_support::pinned_test]
fn parked_room_resolves_with_an_error_on_reset() {
    with_eal(|eal| {
        let mut mixed = Mixed::new(eal, ["rrsa", "rrsb"]);
        let config = tcp::Config { tx_capacity: 1500, ..tcp_config() };
        let (stream, peer) = connect_with(&mut mixed, LOCAL_PORT, config);
        let (_rx, mut tx) = stream.split();
        let handle = mixed.handle();
        let (task, polls) = counted(async move {
            loop {
                let frame = filled(&handle, &[7; CHUNK]).await;
                if let Err(error) = tx.room(frame.len()).await {
                    return (tx, error);
                }
                tx.try_send(frame).map_err(|(_, error)| error).unwrap();
            }
        });
        let slot = mixed.spawn(task);
        mixed.to_peer.paused = true;
        mixed.turn();
        let parked = polls.get();
        for _ in 0..20 {
            mixed.turn();
        }
        assert_eq!(polls.get(), parked, "the writer is not parked");
        assert!(!slot.is_full());
        mixed.peer.abort(&peer);
        mixed.peer.turn(mixed.now, BUDGET).unwrap();
        assert!(mixed.to_net.forward() >= 1, "the reset did not leave the peer");
        mixed.turn();
        let (tx, error) = slot.take();
        assert_eq!(error, SendError::InvalidState);
        assert_eq!(polls.get(), parked + 1, "woken more than once by the reset");
        drop((tx, _rx, peer));
        mixed.close();
    })
}

/// `delivered` remains parked after the peer receives data and FIN until its
/// ACK is forwarded to the sender.
#[zhida_test_support::pinned_test]
fn delivered_waits_until_the_peer_acks_the_fin() {
    with_eal(|eal| {
        let mut mixed = Mixed::new(eal, ["dlva", "dlvb"]);
        let (stream, peer) = connect_with(&mut mixed, LOCAL_PORT, stalling_config());
        let (_rx, mut tx) = stream.split();
        let data = pattern(2 * CHUNK, 21);
        let handle = mixed.handle();
        let sent = data.clone();
        let (task, polls) = counted(async move {
            send_all(&handle, &mut tx, &sent).await;
            tx.shutdown();
            tx.delivered().await;
            tx
        });
        let slot = mixed.spawn(task);
        mixed.to_net.paused = true;
        mixed.until(|m| m.peer.tcp(&peer).state() == State::CloseWait);
        let parked = polls.get();
        for _ in 0..20 {
            mixed.turn();
        }
        assert_eq!(polls.get(), parked, "the waiter is not parked");
        assert!(!slot.is_full(), "delivered before the peer's ACK was forwarded");
        let got = peer_receive(&mut mixed, &peer, data.len());
        assert_eq!(got, data);
        assert_eq!(mixed.peer.tcp_mut(&peer).recv().map(|f| f.is_some()), Err(RecvError::Finished));
        mixed.to_net.paused = false;
        mixed.until(|_| slot.is_full());
        assert!(polls.get() > parked, "the ACK did not wake the waiter");
        drop((slot.take(), _rx, peer));
        mixed.close();
    })
}

/// After `shutdown` the peer reads every byte, then end of stream; the Net
/// side can still receive.
#[zhida_test_support::pinned_test]
fn shutdown_sends_fin_after_the_data() {
    with_eal(|eal| {
        let mut mixed = Mixed::new(eal, ["shda", "shdb"]);
        let (stream, peer) = connect(&mut mixed, LOCAL_PORT);
        let (mut rx, mut tx) = stream.split();
        let data = pattern(20 * CHUNK + 7, 13);
        let handle = mixed.handle();
        let sent = data.clone();
        let slot = mixed.spawn(async move {
            send_all(&handle, &mut tx, &sent).await;
            tx.shutdown();
            tx
        });
        let mut got = Vec::new();
        mixed.until(|m| loop {
            match m.peer.tcp_mut(&peer).recv() {
                Ok(Some(frame)) => got.extend_from_slice(frame.payload()),
                Ok(None) => break false,
                Err(error) => {
                    assert_eq!(error, RecvError::Finished);
                    break true;
                }
            }
        });
        assert_eq!(got, data);
        assert_eq!(mixed.peer.tcp(&peer).state(), State::CloseWait);
        assert_eq!(mixed.peer.tcp_mut(&peer).send_slice(b"after fin"), Ok(9));
        let reader = mixed.spawn(async move { (rx.recv().await.unwrap().payload().to_vec(), rx) });
        let (bytes, rx) = mixed.wait(&reader);
        assert_eq!(bytes, b"after fin");
        drop((slot.take(), rx, peer));
        mixed.close();
    })
}

/// A UDP socket's receive half gets the peer's datagram in one task; its
/// send half answers with a frame from another.
#[zhida_test_support::pinned_test]
fn udp_halves_receive_and_send_a_frame() {
    with_eal(|eal| {
        let mut mixed = Mixed::new(eal, ["udsa", "udsb"]);
        let socket = mixed.handle().udp_bind(udp_config(7000)).unwrap();
        let (mut rx, mut tx) = socket.split();
        let peer = mixed.peer.add_udp(udp_config(7001)).unwrap();
        let reader = mixed.spawn(async move { (rx.recv().await, rx) });
        peer_udp_send(&mut mixed, &peer, 7000, b"ping");
        let (ping, rx) = mixed.wait(&reader);
        assert_eq!(ping.payload(), b"ping");
        let handle = mixed.handle();
        let to = IpEndpoint::new(peer_ip().into(), 7001);
        let writer = mixed.spawn(async move {
            let frame = filled(&handle, b"pong").await;
            tx.send(Some(to), frame).map_err(|(_, error)| error).unwrap();
            tx
        });
        let tx = mixed.wait(&writer);
        let mut pong = None;
        mixed.until(|m| {
            pong = m.peer.udp_mut(&peer).recv();
            pong.is_some()
        });
        let pong = pong.unwrap();
        assert_eq!(pong.payload(), b"pong");
        assert_eq!(pong.src(), IpEndpoint::new(net_ip().into(), 7000));
        drop((ping, pong, rx, tx, peer));
        mixed.close();
    })
}

/// A `room` longer than the send buffer, or than u32, could never resolve: it fails at once with
/// `TooLarge`, and the socket still sends.
#[zhida_test_support::pinned_test]
fn room_beyond_the_send_capacity_fails() {
    with_eal(|eal| {
        let mut mixed = Mixed::new(eal, ["rbga", "rbgb"]);
        let config = tcp::Config { tx_capacity: 1500, ..tcp_config() };
        let (stream, peer) = connect_with(&mut mixed, LOCAL_PORT, config);
        let (_rx, mut tx) = stream.split();
        let handle = mixed.handle();
        let (task, _polls) = counted(async move {
            let capacity = tx.send_capacity();
            assert_eq!(capacity, 1500);
            let over = poll_once(tx.room(capacity + 1)).await;
            let huge = poll_once(tx.room(usize::MAX)).await;
            let fits = poll_once(tx.room(capacity)).await;
            let frame = filled(&handle, &[3; CHUNK]).await;
            tx.try_send(frame).map_err(|(_, error)| error).unwrap();
            (tx, over, huge, fits)
        });
        let slot = mixed.spawn(task);
        mixed.turn();
        let (tx, over, huge, fits) = slot.take();
        assert_eq!(over, Some(Err(SendError::TooLarge { len: 1501, capacity: 1500 })));
        assert_eq!(huge, Some(Err(SendError::TooLarge { len: usize::MAX, capacity: 1500 })));
        assert_eq!(fits, Some(Ok(())));
        assert_eq!(peer_receive(&mut mixed, &peer, CHUNK), [3; CHUNK]);
        drop((tx, _rx, peer));
        mixed.close();
    })
}

/// With `tx_capacity` 1500, one 1000-byte frame leaves 500 bytes, so
/// `room(400)` resolves and `room(1000)` parks. The peer's ACK frees the
/// bytes, and `try_send` of a 1000-byte frame then succeeds at once.
#[zhida_test_support::pinned_test]
fn room_waits_for_queued_bytes_to_be_acked() {
    with_eal(|eal| {
        let mut mixed = Mixed::new(eal, ["rbya", "rbyb"]);
        let config = tcp::Config { tx_capacity: 1500, ..tcp_config() };
        let (stream, peer) = connect_with(&mut mixed, LOCAL_PORT, config);
        let (_rx, mut tx) = stream.split();
        let data = pattern(2 * CHUNK, 17);
        let handle = mixed.handle();
        let sent = data.clone();
        let (task, polls) = counted(async move {
            let first = filled(&handle, &sent[..CHUNK]).await;
            tx.try_send(first).map_err(|(_, error)| error).unwrap();
            assert_eq!(poll_once(tx.room(400)).await, Some(Ok(())), "500 free bytes");
            let second = filled(&handle, &sent[CHUNK..]).await;
            tx.room(CHUNK).await.unwrap();
            tx.try_send(second).map_err(|(_, error)| error).unwrap();
            tx
        });
        let slot = mixed.spawn(task);
        mixed.turn();
        assert_eq!(polls.get(), 1);
        assert!(!slot.is_full(), "room(1000) resolved with 500 free bytes");
        let got = peer_receive(&mut mixed, &peer, data.len());
        assert_eq!(got, data);
        assert_eq!(polls.get(), 2, "woken more than once by the ACK");
        drop((slot.take(), _rx, peer));
        mixed.close();
    })
}

/// Dropping the send half sends no FIN: the receive half keeps receiving
/// and the peer stays Established.
#[zhida_test_support::pinned_test]
fn dropping_the_send_half_keeps_receiving() {
    with_eal(|eal| {
        let mut mixed = Mixed::new(eal, ["dtxa", "dtxb"]);
        let (stream, peer) = connect(&mut mixed, LOCAL_PORT);
        let (mut rx, tx) = stream.split();
        let reader = mixed.spawn(async move { (rx.recv().await.unwrap().payload().to_vec(), rx) });
        mixed.turn();
        assert!(!reader.is_full());
        drop(tx);
        peer_send(&mut mixed, &peer, b"still open");
        let (bytes, rx) = mixed.wait(&reader);
        assert_eq!(bytes, b"still open");
        mixed.rounds(10);
        assert_eq!(mixed.peer.tcp(&peer).state(), State::Established);
        drop((rx, peer));
        mixed.close();
    })
}

/// Dropping the receive half leaves a writer parked in `room` working: once
/// the peer ACKs the queued bytes, everything arrives.
#[zhida_test_support::pinned_test]
fn dropping_the_receive_half_keeps_a_parked_writer() {
    with_eal(|eal| {
        let mut mixed = Mixed::new(eal, ["drxa", "drxb"]);
        let config = tcp::Config { tx_capacity: 1500, ..tcp_config() };
        let (stream, peer) = connect_with(&mut mixed, LOCAL_PORT, config);
        let (rx, mut tx) = stream.split();
        let data = pattern(2 * CHUNK, 19);
        let handle = mixed.handle();
        let sent = data.clone();
        let (task, polls) = counted(async move {
            send_all(&handle, &mut tx, &sent).await;
            tx
        });
        let slot = mixed.spawn(task);
        mixed.to_peer.paused = true;
        mixed.turn();
        let parked = polls.get();
        for _ in 0..20 {
            mixed.turn();
        }
        assert_eq!(polls.get(), parked, "the writer is not parked");
        assert!(!slot.is_full(), "the writer completed before the receive half was dropped");
        drop(rx);
        mixed.to_peer.paused = false;
        let got = peer_receive(&mut mixed, &peer, data.len());
        assert_eq!(got, data);
        mixed.until(|_| slot.is_full());
        assert!(polls.get() > parked, "the ACK did not wake the writer");
        drop((slot.take(), peer));
        mixed.close();
    })
}

/// A `room` polled once with the send buffer full and then dropped leaves no
/// parked task: the ACKs that free room do not poll it.
#[zhida_test_support::pinned_test]
fn canceled_room_clears_its_parked_handle() {
    with_eal(|eal| {
        let mut mixed = Mixed::new(eal, ["crma", "crmb"]);
        let (stream, peer) = connect(&mut mixed, LOCAL_PORT);
        let (_rx, mut tx) = stream.split();
        let handle = mixed.handle();
        let queued = Rc::new(Cell::new(0));
        let (task, polls) = counted({
            let queued = queued.clone();
            async move {
                loop {
                    let frame = filled(&handle, &[3; CHUNK]).await;
                    match tx.try_send(frame) {
                        Ok(()) => queued.set(queued.get() + CHUNK),
                        Err((frame, error)) => {
                            assert_eq!(error, SendFrameError::Full);
                            cancel_after_one_poll(tx.room(frame.len())).await;
                            break;
                        }
                    }
                }
                pending::<()>().await;
                drop(tx);
            }
        });
        mixed.spawn(task);
        mixed.turn();
        assert_eq!(polls.get(), 1);
        let got = peer_receive(&mut mixed, &peer, queued.get());
        assert_eq!(got.len(), queued.get());
        mixed.rounds(10);
        assert_eq!(polls.get(), 1, "canceled room was woken");
        mixed.rt.shutdown();
        drop((_rx, peer));
        mixed.close();
    })
}

/// A task woken by a request replies in the round that polls it: the reply leaves in that
/// round's send, not after the next round's receive.
#[zhida_test_support::pinned_test]
fn a_reply_queued_by_a_woken_task_leaves_in_the_same_round() {
    with_eal(|eal| {
        let mut mixed = Mixed::new(eal, ["rpla", "rplb"]);
        let (stream, peer) = connect(&mut mixed, LOCAL_PORT);
        let handle = mixed.handle();
        let replier = mixed.spawn(async move {
            let (mut rx, mut tx) = stream.split();
            let request = rx.recv().await.unwrap().payload().to_vec();
            let reply = filled(&handle, &request).await;
            tx.room(reply.len()).await.unwrap();
            tx.try_send(reply).map_err(|(_, error)| error).unwrap();
            (rx, tx)
        });
        mixed.rounds(4);
        peer_send(&mut mixed, &peer, b"ping");
        mixed.peer.turn(mixed.now, BUDGET).unwrap();
        assert_eq!(mixed.to_net.forward(), 1);
        mixed.turn();
        assert!(replier.is_full());
        mixed.to_peer.forward();
        mixed.peer.turn(mixed.now, BUDGET).unwrap();
        let mut got = Vec::new();
        peer_drain(&mut mixed, &peer, &mut got);
        assert_eq!(got, b"ping");
        drop((replier.take(), peer));
        mixed.close();
    })
}
