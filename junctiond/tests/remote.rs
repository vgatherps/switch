use std::cell::{Cell, RefCell};
use std::future::Future;
use std::rc::Rc;
use std::task::{Poll, Waker};
use std::time::Instant as WallInstant;

use dpdk_smoltcp::{Eal, TcpHandle};
use switch::Runtime;
use switchnet::{Error, IpEndpoint, IpListenEndpoint, Net, NetHandle, RemoteConfig, TcpStream, tcp};
use zhida_test_kit::with_eal;

mod support;
use support::{Fixture, LOCAL_PORT, PEER_PORT, WAIT, net_ip, peer_tcp_config};

struct Remote {
    rt: Runtime,
    net: Net,
    fixture: Fixture,
}

struct Slot<T>(Rc<RefCell<Option<T>>>);

#[switch::task]
async fn collect<T: 'static, F: Future<Output = T> + 'static>(future: F, out: Rc<RefCell<Option<T>>>) {
    *out.borrow_mut() = Some(future.await);
}

impl<T> Slot<T> {
    fn ready(&self) -> bool { self.0.borrow().is_some() }
    fn take(&self) -> T { self.0.borrow_mut().take().expect("task did not finish") }
}

fn local(port: u16) -> IpListenEndpoint { IpListenEndpoint { addr: None, port } }
fn peer_endpoint(port: u16) -> IpEndpoint {
    IpEndpoint::new(switchnet::Ipv4Address::new(10, 0, 0, 2).into(), port)
}
fn udp_config(port: u16) -> switchnet::UdpConfig {
    switchnet::UdpConfig { port, local_addr: None, rx_queue: 8, tx_checksum: true, broadcast: false }
}

impl Remote {
    fn new(eal: &Eal, names: [&str; 2], frames: usize) -> Self {
        let mut fixture = Fixture::new(eal, names, 4, 4, 64);
        let session = fixture.client(16);
        let mut net = Net::remote(session, RemoteConfig { tx_frames: frames });
        assert_eq!(net.poll_at(), None);
        Self { rt: Runtime::new(0), net, fixture }
    }

    fn handle(&self) -> NetHandle { self.net.handle() }

    fn spawn<T: 'static>(&self, future: impl Future<Output = T> + 'static) -> Slot<T> {
        let out = Rc::new(RefCell::new(None));
        self.rt.spawner().spawn(collect(future, out.clone())).unwrap();
        Slot(out)
    }

    fn round(&mut self) {
        let progressed = self.fixture.round_links();
        self.net.turn(self.fixture.now).unwrap();
        self.rt.poll(0).unwrap();
        if !progressed { self.fixture.advance(); }
    }

    fn until(&mut self, mut done: impl FnMut(&mut Self) -> bool) {
        let deadline = WallInstant::now() + WAIT;
        while !done(self) {
            assert!(WallInstant::now() < deadline, "remote condition timed out");
            self.round();
        }
    }

    fn wait<T>(&mut self, slot: &Slot<T>) -> T {
        self.until(|_| slot.ready());
        slot.take()
    }

    fn connect(&mut self) -> (TcpStream, TcpHandle) {
        let peer = support::peer_listen(&mut self.fixture);
        let slot = self.spawn(self.handle().tcp_connect(peer_tcp_config(), peer_endpoint(PEER_PORT), local(LOCAL_PORT)));
        let stream = self.wait(&slot).unwrap();
        self.until(|m| m.fixture.peer.tcp(&peer).state() == tcp::State::Established);
        (stream, peer)
    }

    fn frame(&mut self, bytes: &[u8]) -> switchnet::ImmutableFrame {
        let handle = self.handle();
        let slot = self.spawn(async move { handle.tx_frame().await });
        let mut frame = self.wait(&slot);
        assert_eq!(frame.len(), 232);
        frame[..bytes.len()].copy_from_slice(bytes);
        frame.truncate(bytes.len());
        frame.freeze()
    }

    fn close(mut self) {
        self.rt.shutdown();
        let session = self.net.into_session().ok().expect("socket or NetHandle still alive");
        drop(session);
        self.fixture.close();
    }
}

// Mutations tcp_send_noop / tcp_rx_drop: skip Session::send_tcp or packet routing.
// Mutation fin_before_data: return Finished before draining queued payloads.
// Mutation connected_mss_discard: do not cache the host's Connected.mss.
#[zhida_test_support::pinned_test]
fn remote_connect_tcp_both_directions_fin_and_identity() {
    with_eal(|eal| {
        let mut m = Remote::new(eal, ["rcna", "rcnb"], 4);
        let (stream, peer) = m.connect();
        let (mut rx, mut tx) = stream.split();
        assert!(tx.is_on(&m.handle()));
        assert_eq!(tx.max_segment_payload(), Some(1460));
        assert_eq!(tx.send_capacity(), 240);
        let frame = m.frame(b"client bytes");
        tx.try_send(frame).unwrap();
        tx.shutdown();
        let mut received = Vec::new();
        m.until(|m| {
            while let Ok(Some(frame)) = m.fixture.peer.tcp_mut(&peer).recv() {
                received.extend_from_slice(frame.payload());
            }
            !m.fixture.peer.tcp(&peer).may_recv()
        });
        assert_eq!(received, b"client bytes");
        assert_eq!(m.fixture.peer.tcp_mut(&peer).send_slice(b"peer bytes"), Ok(10));
        m.fixture.peer.close(&peer);
        let slot = m.spawn(async move {
            let mut received = Vec::new();
            loop {
                match rx.recv().await {
                    Ok(frame) => received.extend_from_slice(frame.payload()),
                    Err(error) => return (received, error),
                }
            }
        });
        let (received, eof) = m.wait(&slot);
        assert_eq!(received, b"peer bytes");
        assert_eq!(eof, Error::Finished);
        let delivered = m.spawn(async move { tx.delivered().await });
        m.wait(&delivered);
        drop(peer);
        m.close();
    });
}

// Mutations accept_early_ready / accepted_packet_drop: complete before Connected,
// or skip routing an accepted socket's receive packets.
#[zhida_test_support::pinned_test]
fn remote_accept_waits_for_connection_and_receives() {
    with_eal(|eal| {
        let mut m = Remote::new(eal, ["raca", "racb"], 2);
        let slot = m.spawn(m.handle().tcp_accept(peer_tcp_config(), local(LOCAL_PORT)));
        m.rt.poll(0).unwrap();
        assert!(!slot.ready());
        let peer = m.fixture.peer.add_tcp(peer_tcp_config()).unwrap();
        m.fixture.peer.connect(&peer, IpEndpoint::new(net_ip().into(), LOCAL_PORT), PEER_PORT).unwrap();
        let (mut rx, tx) = m.wait(&slot).unwrap().split();
        assert_eq!(tx.max_segment_payload(), Some(1460));
        assert_eq!(m.fixture.peer.tcp_mut(&peer).send_slice(b"accepted"), Ok(8));
        let received = m.spawn(async move { rx.recv().await.unwrap() });
        assert_eq!(m.wait(&received).payload(), b"accepted");
        drop((tx, peer));
        m.close();
    });
}

// Mutation reset_clears_queue: clear queued frames while routing Closed(reset).
// Mutation reset_is_fin: map reset to Finished rather than InvalidState.
#[zhida_test_support::pinned_test]
fn remote_queued_data_precedes_reset_invalid_state() {
    with_eal(|eal| {
        let mut m = Remote::new(eal, ["rrsa", "rrsb"], 2);
        let (stream, peer) = m.connect();
        let (mut rx, mut tx) = stream.split();
        assert_eq!(m.fixture.peer.tcp_mut(&peer).send_slice(b"queued"), Ok(6));
        // Send onto the link, publish into the ring, then queue in Net without receiving.
        m.fixture.peer.turn(m.fixture.now, zhida_test_kit::pair::BUDGET).unwrap();
        assert!(m.fixture.to_net.forward() > 0);
        m.round();
        let closed = m.fixture.closed_events;
        m.fixture.peer.abort(&peer);
        m.until(|m| m.fixture.closed_events > closed);
        // Bound follows Closed on the ordered control channel, so Net has observed reset.
        let barrier = m.spawn(m.handle().udp_bind(udp_config(7000)));
        let barrier = m.wait(&barrier).unwrap();
        let received = m.spawn(async move {
            let frame = rx.recv().await.unwrap();
            assert_eq!(frame.payload(), b"queued");
            drop(frame);
            rx.recv().await.unwrap_err()
        });
        assert_eq!(m.wait(&received), Error::InvalidState);
        assert_eq!(tx.max_segment_payload(), Some(1460));
        let frame = m.frame(b"late");
        assert_eq!(tx.try_send(frame).unwrap_err().1, Error::InvalidState);
        drop((barrier, tx, peer));
        m.close();
    });
}

// Mutations udp_destination_ignored / udp_metadata_swapped: omit send destination
// or interchange source and destination when building the received Datagram.
#[zhida_test_support::pinned_test]
fn remote_udp_both_directions_preserves_addresses() {
    with_eal(|eal| {
        let mut m = Remote::new(eal, ["ruda", "rudb"], 2);
        let bound = m.spawn(m.handle().udp_bind(udp_config(7000)));
        let (mut rx, mut tx) = m.wait(&bound).unwrap().split();
        let peer = m.fixture.peer.add_udp(udp_config(7001)).unwrap();
        // Resolve the peer's neighbor before the client sends a datagram.
        m.until(|m| match m.fixture.peer.udp_send(&peer, Some(IpEndpoint::new(net_ip().into(), 7000)), b"peer udp") {
            Ok(()) => true,
            Err(error) => { assert_eq!(error, dpdk_smoltcp::UdpSendError::NeighborPending); false }
        });
        let received = m.spawn(async move { rx.recv().await });
        let datagram = m.wait(&received);
        assert_eq!(datagram.payload(), b"peer udp");
        assert_eq!(datagram.src(), peer_endpoint(7001));
        assert_eq!(datagram.dst_addr(), net_ip());
        drop(datagram);
        let frame = m.frame(b"client udp");
        tx.send(Some(peer_endpoint(7001)), frame).unwrap();
        let mut received = None;
        m.until(|m| { received = m.fixture.peer.udp_mut(&peer).recv(); received.is_some() });
        let datagram = received.unwrap();
        assert_eq!(datagram.payload(), b"client udp");
        assert_eq!(datagram.src(), IpEndpoint::new(net_ip().into(), 7000));
        assert_eq!(datagram.dst_addr(), switchnet::Ipv4Address::new(10, 0, 0, 2));
        drop((datagram, tx, peer));
        m.close();
    });
}

// Mutation pool_release_no_wake: do not wake allocators after the last clone drops.
#[zhida_test_support::pinned_test]
fn remote_tx_pool_exhaustion_parks_and_drop_wakes() {
    with_eal(|eal| {
        let mut m = Remote::new(eal, ["rpfa", "rpfb"], 1);
        let held = m.frame(b"held");
        let handle = m.handle();
        let waiting = m.spawn(async move { handle.tx_frame().await });
        m.rt.poll(0).unwrap();
        assert!(!waiting.ready());
        m.round();
        assert!(!waiting.ready());
        let clone = held.clone();
        drop(held);
        m.round();
        assert!(!waiting.ready(), "a clone still owns the only buffer");
        drop(clone);
        let frame = m.wait(&waiting);
        assert_eq!(frame.len(), 232);
        drop(frame);
        m.close();
    });
}

// Mutations full_consumes_frame / room_never_parks / room_retry_no_wake:
// discard Full's frame, return Ready after Full, or omit the next turn's wake.
#[zhida_test_support::pinned_test]
fn remote_full_returns_frame_and_room_wakes_after_fifo_drain() {
    with_eal(|eal| {
        let mut m = Remote::new(eal, ["rbpa", "rbpb"], 2);
        let (stream, peer) = m.connect();
        let (rx, mut tx) = stream.split();
        let handle = m.handle();
        let allocated = m.spawn(async move { handle.tx_frame().await });
        let mut frame = m.wait(&allocated);
        frame.fill(0x5a);
        let frame = frame.freeze();
        let mut sent = 0;
        let returned = loop {
            match tx.try_send(frame.clone()) {
                Ok(()) => sent += 1,
                Err((returned, error)) => { assert_eq!(error, Error::Full); break returned; }
            }
            assert!(sent < 10_000, "FIFO did not fill without network turns");
        };
        assert_eq!(&*returned, &*frame);
        let waiting = m.spawn(async move {
            let result = tx.room(returned.len()).await;
            (result, tx, returned)
        });
        m.rt.poll(0).unwrap();
        assert!(!waiting.ready());
        let (room, mut tx, returned) = m.wait(&waiting);
        assert_eq!(room, Ok(()));
        let sending = m.spawn(async move {
            let mut returned = returned;
            loop {
                match tx.try_send(returned) {
                    Ok(()) => return tx,
                    Err((frame, error)) => {
                        assert_eq!(error, Error::Full);
                        returned = frame;
                        tx.room(returned.len()).await.unwrap();
                    }
                }
            }
        });
        let mut tx = m.wait(&sending);
        let too_large = m.spawn(async move { let result = tx.room(241).await; (result, tx) });
        let (error, tx) = m.wait(&too_large);
        assert_eq!(error, Err(Error::TooLarge));
        drop((tx, rx, frame, peer));
        m.close();
    });
}

// Mutation opening_packet_drop: discard packets until Connected arrives.
#[zhida_test_support::pinned_test]
fn remote_packets_before_connected_survive_until_open_completes() {
    with_eal(|eal| {
        let mut m = Remote::new(eal, ["rpca", "rpcb"], 2);
        m.fixture.hold_connected();
        let peer = support::peer_listen(&mut m.fixture);
        let opening = m.spawn(m.handle().tcp_connect(peer_tcp_config(), peer_endpoint(PEER_PORT), local(LOCAL_PORT)));
        m.until(|m| m.fixture.connected_is_held()
            && m.fixture.peer.tcp(&peer).state() == tcp::State::Established);
        assert!(!opening.ready());
        assert_eq!(m.fixture.peer.tcp_mut(&peer).send_slice(b"before Connected"), Ok(16));
        m.fixture.peer.turn(m.fixture.now, zhida_test_kit::pair::BUDGET).unwrap();
        assert!(m.fixture.to_net.forward() > 0);
        m.round();
        assert!(!opening.ready());
        m.fixture.release_connected();
        let (mut rx, tx) = m.wait(&opening).unwrap().split();
        let received = m.spawn(async move { rx.recv().await.unwrap() });
        assert_eq!(m.wait(&received).payload(), b"before Connected");
        drop((tx, peer));
        m.close();
    });
}

// Mutations loss_not_reported / loss_no_wake / late_open_wrong_error:
// ignore poll loss, omit parked-task wakes, or map later opens to InvalidState.
// Mutation lost_socket_still_open: allow established socket I/O after session loss.
// Mutations bind_loss_no_wake / udp_send_after_loss / room_after_loss:
// omit the Bind wake, or leave established UDP send / TCP room usable after loss.
// Mutations udp_recv_fallible / allocator_lost: change Datagram output or disable
// the local frame allocator after losing the host.
#[zhida_test_support::pinned_test]
fn remote_session_loss_wakes_operations_and_turn_reports_host_lost() {
    with_eal(|eal| {
        let mut m = Remote::new(eal, ["rlsa", "rlsb"], 2);
        let (stream, peer) = m.connect();
        let (mut rx, mut tx) = stream.split();
        let bound = m.spawn(m.handle().udp_bind(udp_config(7000)));
        let (mut udp_rx, mut udp_tx) = m.wait(&bound).unwrap().split();
        let udp_receiving: Slot<switchnet::Datagram> = m.spawn(async move { udp_rx.recv().await });
        let receiving = m.spawn(async move { rx.recv().await.unwrap_err() });
        let accepting = m.spawn(m.handle().tcp_accept(peer_tcp_config(), local(LOCAL_PORT + 1)));
        let frame = m.frame(b"lost");
        let binding = m.spawn(m.handle().udp_bind(udp_config(7001)));
        m.rt.poll(0).unwrap();
        assert!(!receiving.ready());
        assert!(!accepting.ready());
        assert!(!binding.ready());
        assert!(!udp_receiving.ready());
        m.fixture.terminate_session(m.fixture.streams[0]);
        let deadline = WallInstant::now() + WAIT;
        loop {
            assert!(WallInstant::now() < deadline, "host loss did not reach Net");
            // Do not advance Network: Binding must stay pending until host loss,
            // rather than racing a real Bound event against the host's EOF.
            match m.net.turn(m.fixture.now) {
                Ok(_) => std::thread::yield_now(),
                Err(switchnet::TurnError::HostLost(_)) => break,
                Err(other) => panic!("wrong turn failure: {other:?}"),
            }
        }
        assert!(matches!(m.net.turn(m.fixture.now), Err(switchnet::TurnError::HostLost(_))));
        m.rt.poll(0).unwrap();
        assert_eq!(receiving.take(), Error::InvalidState);
        assert!(matches!(accepting.take(), Err(Error::HostLost)));
        assert!(matches!(binding.take(), Err(Error::HostLost)));
        assert!(!udp_receiving.ready(), "infallible UDP receive must remain parked");
        assert_eq!(udp_tx.send(Some(peer_endpoint(7001)), frame.clone()).unwrap_err().1, Error::InvalidState);
        assert_eq!(tx.try_send(frame).unwrap_err().1, Error::InvalidState);
        let room = m.spawn(async move { tx.room(1).await });
        let handle = m.handle();
        let allocated = m.spawn(async move { handle.tx_frame().await });
        let later_connect = m.spawn(m.handle().tcp_connect(peer_tcp_config(), peer_endpoint(PEER_PORT), local(LOCAL_PORT + 2)));
        let later_accept = m.spawn(m.handle().tcp_accept(peer_tcp_config(), local(LOCAL_PORT + 3)));
        let later_bind = m.spawn(m.handle().udp_bind(udp_config(7002)));
        m.rt.poll(0).unwrap();
        assert!(matches!(later_connect.take(), Err(Error::HostLost)));
        assert!(matches!(later_accept.take(), Err(Error::HostLost)));
        assert!(matches!(later_bind.take(), Err(Error::HostLost)));
        assert_eq!(room.take(), Err(Error::InvalidState));
        let frame = allocated.take();
        assert_eq!(frame.len(), 232);
        drop((frame, udp_tx, peer));
        m.close();
    });
}

// Mutations reject_wrong_error / stale_socket_id / raw_session_slot_state:
// map UdpPortInUse incorrectly, return the rejected socket's stale ID, or index
// future state by the reusable Session slot instead of keeping each open distinct.
#[zhida_test_support::pinned_test]
fn remote_rejected_bind_survives_session_slot_reuse() {
    with_eal(|eal| {
        let mut m = Remote::new(eal, ["rrba", "rrbb"], 2);
        let bound = m.spawn(m.handle().udp_bind(udp_config(7000)));
        let occupied = m.wait(&bound).unwrap();
        let mut binding = m.handle().udp_bind(udp_config(7000));
        let allow_completion = Rc::new(Cell::new(false));
        let rejection_woke = Rc::new(Cell::new(false));
        let parked_waker = Rc::new(RefCell::new(None::<Waker>));
        let allow = allow_completion.clone();
        let woke = rejection_woke.clone();
        let parked = parked_waker.clone();
        let mut started = false;
        let rejected = m.spawn(std::future::poll_fn(move |cx| {
            *parked.borrow_mut() = Some(cx.waker().clone());
            if started && !allow.get() {
                woke.set(true);
                return Poll::Pending;
            }
            started = true;
            std::pin::Pin::new(&mut binding).poll(cx)
        }));
        m.rt.poll(0).unwrap();
        assert!(!rejected.ready());
        m.until(|_| rejection_woke.get());

        // Session allocates the first free slot. Rejected released slot 1, while
        // occupied still owns slot 0; keep the rejected future alive and unpolled.
        let replacement = m.spawn(m.handle().udp_bind(udp_config(7001)));
        let (replacement_rx, mut replacement_tx) = m.wait(&replacement).unwrap().split();
        allow_completion.set(true);
        parked_waker.borrow().as_ref().unwrap().wake_by_ref();
        assert!(matches!(m.wait(&rejected), Err(Error::PortInUse)));

        // Dropping the rejected open must not close the replacement in its slot.
        let peer = m.fixture.peer.add_udp(udp_config(7002)).unwrap();
        m.until(|m| match m.fixture.peer.udp_send(&peer, Some(IpEndpoint::new(net_ip().into(), 7001)), b"resolve") {
            Ok(()) => true,
            Err(error) => { assert_eq!(error, dpdk_smoltcp::UdpSendError::NeighborPending); false }
        });
        let frame = m.frame(b"replacement");
        replacement_tx.send(Some(peer_endpoint(7002)), frame).unwrap();
        let mut received = None;
        m.until(|m| { received = m.fixture.peer.udp_mut(&peer).recv(); received.is_some() });
        assert_eq!(received.as_ref().unwrap().payload(), b"replacement");
        drop((received, replacement_rx, replacement_tx, occupied, peer));
        m.close();
    });
}

// Mutation clean_closed_is_reset: let clean Closed override an earlier PeerFin.
#[zhida_test_support::pinned_test]
fn remote_clean_closed_before_receive_preserves_data_then_finished() {
    with_eal(|eal| {
        let mut m = Remote::new(eal, ["rcfa", "rcfb"], 2);
        let (stream, peer) = m.connect();
        let (mut rx, mut tx) = stream.split();
        assert_eq!(m.fixture.peer.tcp_mut(&peer).send_slice(b"queued"), Ok(6));
        m.fixture.peer.close(&peer);
        m.until(|m| m.fixture.peer.tcp(&peer).state() == tcp::State::FinWait2);

        let closed = m.fixture.closed_events;
        tx.shutdown();
        m.until(|m| m.fixture.closed_events > closed);
        // Bound crosses the same ordered control stream after Closed, ensuring
        // Net has processed Closed before the first receive is polled.
        let barrier = m.spawn(m.handle().udp_bind(udp_config(7000)));
        let barrier = m.wait(&barrier).unwrap();
        let received = m.spawn(async move {
            let frame = rx.recv().await.unwrap();
            assert_eq!(frame.payload(), b"queued");
            drop(frame);
            rx.recv().await.unwrap_err()
        });
        assert_eq!(m.wait(&received), Error::Finished);
        drop((barrier, tx, peer));
        m.close();
    });
}

// Mutation fin_masks_reset: report Finished from PeerFin even after a later reset.
#[zhida_test_support::pinned_test]
fn remote_fin_then_reset_preserves_data_then_invalid_state() {
    with_eal(|eal| {
        let mut m = Remote::new(eal, ["rfra", "rfrb"], 2);
        let (stream, peer) = m.connect();
        let (mut rx, tx) = stream.split();
        assert_eq!(m.fixture.peer.tcp_mut(&peer).send_slice(b"queued"), Ok(6));
        m.fixture.peer.close(&peer);
        m.until(|m| m.fixture.peer.tcp(&peer).state() == tcp::State::FinWait2);
        // The host forwarded payload and PeerFin before acknowledging the FIN;
        // a later Bound ensures Net has consumed that PeerFin before the reset.
        let before_reset = m.spawn(m.handle().udp_bind(udp_config(7000)));
        let before_reset = m.wait(&before_reset).unwrap();
        let closed = m.fixture.closed_events;
        m.fixture.peer.abort(&peer);
        m.until(|m| m.fixture.closed_events > closed);
        let after_reset = m.spawn(m.handle().udp_bind(udp_config(7001)));
        let after_reset = m.wait(&after_reset).unwrap();
        let received = m.spawn(async move {
            let frame = rx.recv().await.unwrap();
            assert_eq!(frame.payload(), b"queued");
            drop(frame);
            rx.recv().await.unwrap_err()
        });
        assert_eq!(m.wait(&received), Error::InvalidState);
        drop((before_reset, after_reset, tx, peer));
        m.close();
    });
}

// Mutation close_unread_false: force unread=false when releasing the last half.
#[zhida_test_support::pinned_test]
fn remote_last_half_drop_resets_unread_data_without_losing_session() {
    with_eal(|eal| {
        let mut m = Remote::new(eal, ["rdra", "rdrb"], 2);
        let (stream, peer) = m.connect();
        let (rx, tx) = stream.split();
        assert_eq!(m.fixture.peer.tcp_mut(&peer).send_slice(b"unread"), Ok(6));
        m.until(|m| m.fixture.peer.tcp(&peer).send_queue() == 0);
        // Bound ensures Net has drained the payload published before the ACK.
        let barrier = m.spawn(m.handle().udp_bind(udp_config(7000)));
        let barrier = m.wait(&barrier).unwrap();
        drop(rx);
        let after_rx_drop = m.spawn(m.handle().udp_bind(udp_config(7001)));
        let after_rx_drop = m.wait(&after_rx_drop).unwrap();
        assert_eq!(m.fixture.peer.tcp(&peer).state(), tcp::State::Established);
        assert!(!m.fixture.peer.tcp(&peer).reset_by_peer());

        drop(tx);
        m.until(|m| m.fixture.peer.tcp(&peer).reset_by_peer());
        let still_alive = m.spawn(m.handle().udp_bind(udp_config(7002)));
        let still_alive = m.wait(&still_alive).unwrap();
        drop((barrier, after_rx_drop, still_alive, peer));
        m.close();
    });
}
