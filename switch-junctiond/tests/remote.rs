use std::cell::{Cell, RefCell};
use std::future::Future;
use std::rc::Rc;
use std::task::{Poll, Waker};
use std::time::Instant as WallInstant;

use dpdk_smoltcp::{Eal, TcpHandle};
use switch_runtime::Runtime;
use switch_net::{Error, IpEndpoint, IpListenEndpoint, Net, NetHandle, RemoteConfig, TcpStream, tcp};
use switch_test_kit::with_eal;

mod support;
use support::{Fixture, LOCAL_PORT, PEER_PORT, WAIT, net_ip, peer_tcp_config};

struct Remote {
    rt: Runtime,
    net: Net,
    fixture: Fixture,
}

struct Slot<T>(Rc<RefCell<Option<T>>>);

#[switch_runtime::task]
async fn collect<T: 'static, F: Future<Output = T> + 'static>(future: F, out: Rc<RefCell<Option<T>>>) {
    *out.borrow_mut() = Some(future.await);
}

impl<T> Slot<T> {
    fn ready(&self) -> bool { self.0.borrow().is_some() }
    fn take(&self) -> T { self.0.borrow_mut().take().expect("task did not finish") }
}

struct CompletionGate {
    allow: Rc<Cell<bool>>,
    waker: Rc<RefCell<Option<Waker>>>,
}

impl CompletionGate {
    fn release(self) {
        self.allow.set(true);
        self.waker.borrow().as_ref().unwrap().wake_by_ref();
    }
}

fn local(port: u16) -> IpListenEndpoint { IpListenEndpoint { addr: None, port } }
fn peer_endpoint(port: u16) -> IpEndpoint {
    IpEndpoint::new(switch_net::Ipv4Address::new(10, 0, 0, 2).into(), port)
}
fn udp_config(port: u16) -> switch_net::UdpConfig {
    switch_net::UdpConfig { port, local_addr: None, rx_queue: 8, tx_checksum: true, broadcast: false }
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

    /// A listener of one socket on `local` and its first `accept`; the listener drops with the
    /// future.
    fn accept_one(&self, local: IpListenEndpoint) -> impl Future<Output = Result<TcpStream, Error>> + 'static {
        let mut listener = self.handle().tcp_listen(peer_tcp_config(), local, 1).unwrap();
        async move { listener.accept().await }
    }

    fn spawn<T: 'static>(&self, future: impl Future<Output = T> + 'static) -> Slot<T> {
        let out = Rc::new(RefCell::new(None));
        self.rt.spawner().spawn(collect(future, out.clone())).unwrap();
        Slot(out)
    }

    fn spawn_gated<T: 'static>(
        &self,
        future: impl Future<Output = T> + Unpin + 'static,
    ) -> (Slot<T>, CompletionGate) {
        let gate = CompletionGate {
            allow: Rc::new(Cell::new(false)),
            waker: Rc::new(RefCell::new(None)),
        };
        let allow = gate.allow.clone();
        let waker = gate.waker.clone();
        let mut future = future;
        let mut started = false;
        let slot = self.spawn(std::future::poll_fn(move |cx| {
            *waker.borrow_mut() = Some(cx.waker().clone());
            if started && !allow.get() {
                return Poll::Pending;
            }
            started = true;
            std::pin::Pin::new(&mut future).poll(cx)
        }));
        (slot, gate)
    }

    fn fixture_until(&mut self, mut done: impl FnMut(&mut Fixture) -> bool) {
        let deadline = WallInstant::now() + WAIT;
        while !done(&mut self.fixture) {
            assert!(WallInstant::now() < deadline, "fixture condition timed out");
            if !self.fixture.round_links() { self.fixture.advance(); }
        }
    }

    fn publish_without_net_turn(&mut self, peer: &TcpHandle, bytes: &[u8]) {
        assert_eq!(self.fixture.peer.tcp_mut(peer).send_slice(bytes), Ok(bytes.len()));
        // The ACK reaches the peer only after Network has serviced RX and published
        // the payload. Deliberately do not turn Net or poll Runtime along this path.
        self.fixture_until(|f| f.peer.tcp(peer).send_queue() == 0);
    }

    fn control_barrier(&mut self, port: u16) -> switch_net::UdpSocket {
        let barrier = self.spawn(self.handle().udp_bind(udp_config(port)));
        self.wait(&barrier).unwrap()
    }

    fn round(&mut self) {
        let progressed = self.fixture.round_links();
        self.net.receive(self.fixture.now).unwrap();
        self.rt.poll(0).unwrap();
        self.net.send().unwrap();
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

    fn frame(&mut self, bytes: &[u8]) -> switch_net::ImmutableFrame {
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
#[switch_test_support::pinned_test]
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
#[switch_test_support::pinned_test]
fn remote_accept_waits_for_connection_and_receives() {
    with_eal(|eal| {
        let mut m = Remote::new(eal, ["raca", "racb"], 2);
        let slot = m.spawn(m.accept_one(local(LOCAL_PORT)));
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
#[switch_test_support::pinned_test]
fn remote_queued_data_precedes_reset_invalid_state() {
    with_eal(|eal| {
        let mut m = Remote::new(eal, ["rrsa", "rrsb"], 2);
        let (stream, peer) = m.connect();
        let (mut rx, mut tx) = stream.split();
        assert_eq!(m.fixture.peer.tcp_mut(&peer).send_slice(b"queued"), Ok(6));
        // Send onto the link, publish into the ring, then queue in Net without receiving.
        m.fixture.peer.turn(m.fixture.now, switch_test_kit::pair::BUDGET).unwrap();
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
#[switch_test_support::pinned_test]
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
        assert_eq!(datagram.dst_addr(), switch_net::Ipv4Address::new(10, 0, 0, 2));
        drop((datagram, tx, peer));
        m.close();
    });
}

// Mutation pool_release_no_wake: do not wake allocators after the last clone drops.
#[switch_test_support::pinned_test]
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
#[switch_test_support::pinned_test]
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
#[switch_test_support::pinned_test]
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
        m.fixture.peer.turn(m.fixture.now, switch_test_kit::pair::BUDGET).unwrap();
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
#[switch_test_support::pinned_test]
fn remote_session_loss_wakes_operations_and_turn_reports_host_lost() {
    with_eal(|eal| {
        let mut m = Remote::new(eal, ["rlsa", "rlsb"], 2);
        let (stream, peer) = m.connect();
        let (mut rx, mut tx) = stream.split();
        let bound = m.spawn(m.handle().udp_bind(udp_config(7000)));
        let (mut udp_rx, mut udp_tx) = m.wait(&bound).unwrap().split();
        let udp_receiving: Slot<switch_net::Datagram> = m.spawn(async move { udp_rx.recv().await });
        let receiving = m.spawn(async move { rx.recv().await.unwrap_err() });
        let accepting = m.spawn(m.accept_one(local(LOCAL_PORT + 1)));
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
            match m.net.receive(m.fixture.now) {
                Ok(_) => std::thread::yield_now(),
                Err(switch_net::TurnError::HostLost(_)) => break,
                Err(other) => panic!("wrong receive failure: {other:?}"),
            }
        }
        assert!(matches!(m.net.receive(m.fixture.now), Err(switch_net::TurnError::HostLost(_))));
        assert!(matches!(m.net.send(), Err(switch_net::TurnError::HostLost(_))));
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
        let later_listen = m.handle().tcp_listen(peer_tcp_config(), local(LOCAL_PORT + 3), 1);
        let later_bind = m.spawn(m.handle().udp_bind(udp_config(7002)));
        m.rt.poll(0).unwrap();
        assert!(matches!(later_connect.take(), Err(Error::HostLost)));
        assert!(matches!(later_listen, Err(Error::HostLost)));
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
#[switch_test_support::pinned_test]
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
#[switch_test_support::pinned_test]
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
#[switch_test_support::pinned_test]
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
#[switch_test_support::pinned_test]
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

// Mutations loss_skips_ring_drain / loss_discards_published_tcp: return the
// control failure before importing packets published since the last Net turn.
#[switch_test_support::pinned_test]
fn remote_ring_data_published_before_session_loss_is_received_first() {
    with_eal(|eal| {
        let mut m = Remote::new(eal, ["rlqa", "rlqb"], 2);
        let (stream, peer) = m.connect();
        let (mut rx, mut tx) = stream.split();
        m.publish_without_net_turn(&peer, b"before loss");
        let session = m.fixture.streams[0];
        m.fixture.terminate_session(session);
        m.fixture.forward_abort(session);
        // Host has dropped the Unix socket. A real control write records loss
        // synchronously, without a successful Net turn draining the ring first.
        tx.shutdown();
        assert!(matches!(m.net.receive(m.fixture.now), Err(switch_net::TurnError::HostLost(_))));
        let receiving = m.spawn(async move {
            let frame = rx.recv().await.unwrap();
            assert_eq!(frame.payload(), b"before loss");
            drop(frame);
            rx.recv().await.unwrap_err()
        });
        m.rt.poll(0).unwrap();
        assert_eq!(receiving.take(), Error::InvalidState);
        drop((tx, peer));
        m.close();
    });
}

// Mutations last_drop_skips_ring_drain / close_only_checks_local_queue:
// decide unread=false without collecting data published after the last turn.
#[switch_test_support::pinned_test]
fn remote_last_half_drop_resets_bytes_still_in_host_ring() {
    with_eal(|eal| {
        let mut m = Remote::new(eal, ["rdqa", "rdqb"], 2);
        let (stream, peer) = m.connect();
        let (rx, tx) = stream.split();
        m.publish_without_net_turn(&peer, b"unread ring");
        drop(rx);
        // One half must not close the connection, even with unread ring bytes.
        for _ in 0..32 { m.fixture.round_links(); }
        assert_eq!(m.fixture.peer.tcp(&peer).state(), tcp::State::Established);
        assert!(!m.fixture.peer.tcp(&peer).reset_by_peer());
        drop(tx);
        // The last-half drop must detect the ring bytes without a Net turn.
        m.fixture_until(|f| f.peer.tcp(&peer).reset_by_peer());
        let still_alive = m.control_barrier(7000);
        drop((still_alive, peer));
        m.close();
    });
}

fn closed_before_open_completion(eal: &Eal, accepting: bool, reset: bool, connected_first: bool) {
    let mut m = Remote::new(eal, ["roca", "rocb"], 2);
    m.fixture.hold_connected();
    m.fixture.hold_closed();
    let (opening, gate, peer) = if accepting {
        let (opening, gate) = m.spawn_gated(Box::pin(m.accept_one(local(LOCAL_PORT))));
        m.rt.poll(0).unwrap();
        let peer = m.fixture.peer.add_tcp(peer_tcp_config()).unwrap();
        m.fixture.peer.connect(&peer, IpEndpoint::new(net_ip().into(), LOCAL_PORT), PEER_PORT).unwrap();
        (opening, gate, peer)
    } else {
        let peer = support::peer_listen(&mut m.fixture);
        let (opening, gate) = m.spawn_gated(m.handle().tcp_connect(
            peer_tcp_config(), peer_endpoint(PEER_PORT), local(LOCAL_PORT),
        ));
        (opening, gate, peer)
    };
    m.until(|m| m.fixture.connected_is_held()
        && m.fixture.peer.tcp(&peer).state() == tcp::State::Established);
    assert!(!opening.ready());
    m.publish_without_net_turn(&peer, b"before close");
    if reset {
        m.fixture.peer.abort(&peer);
    } else {
        // Peer FIN first puts the host in CloseWait, so its real shutdown
        // completes via LastAck rather than waiting out TimeWait.
        m.fixture.peer.close(&peer);
        m.fixture_until(|f| f.peer.tcp(&peer).state() == tcp::State::FinWait2);
        m.fixture.shutdown_held_tcp();
    }
    m.until(|m| m.fixture.closed_is_held());
    assert_eq!(m.fixture.held_closed_reset(), reset);
    assert!(!opening.ready());
    if connected_first {
        m.fixture.release_connected();
    } else {
        m.fixture.release_closed();
    }
    let first = m.control_barrier(7000);
    assert!(!opening.ready(), "open completion must stay gated after the first event");
    if connected_first {
        m.fixture.release_closed();
    } else {
        m.fixture.release_connected();
    }
    let second = m.control_barrier(7001);
    assert!(!opening.ready(), "both events must precede the final future poll");
    gate.release();
    let result = m.wait(&opening);
    if accepting {
        let (mut rx, tx) = result.expect("an accepted, already-closed stream is still returned").split();
        assert_eq!(tx.max_segment_payload(), None);
        let receiving = m.spawn(async move {
            let frame = rx.recv().await.unwrap();
            assert_eq!(frame.payload(), b"before close");
            drop(frame);
            rx.recv().await.unwrap_err()
        });
        assert_eq!(m.wait(&receiving), if reset { Error::InvalidState } else { Error::Finished });
        drop(tx);
    } else {
        assert!(matches!(result, Err(error) if error == if reset { Error::Refused } else { Error::Closed }));
    }
    drop((first, second, peer));
    m.close();
}

// Mutations connect_uses_latched_connected / closed_before_connected_is_ignored:
// complete an opening connect successfully after either terminal-event order.
// Mutation connect_clean_closed_is_refused: lose the reset flag at completion.
#[switch_test_support::pinned_test]
fn remote_connect_closed_before_completion_rechecks_both_event_orders() {
    with_eal(|eal| {
        for reset in [false, true] {
            for connected_first in [false, true] {
                closed_before_open_completion(eal, false, reset, connected_first);
            }
        }
    });
}

// Mutations accept_closed_is_error / accept_closed_caches_mss / accept_closed_drops_rx:
// reject the accepted stream, retain its MSS, or discard its queued bytes when
// Closed and Connected both arrive before the opening future can complete.
#[switch_test_support::pinned_test]
fn remote_accept_closed_before_completion_returns_readable_stream_without_mss() {
    with_eal(|eal| {
        for reset in [false, true] {
            for connected_first in [false, true] {
                closed_before_open_completion(eal, true, reset, connected_first);
            }
        }
    });
}

// Mutations tcp_foreign_pool_accepted / udp_foreign_pool_accepted /
// foreign_replaces_frame: accept another producer or return replacement storage.
#[switch_test_support::pinned_test]
fn remote_tcp_and_udp_reject_other_remote_and_local_pool_frames_unchanged() {
    with_eal(|eal| {
        let mut m = Remote::new(eal, ["rfoa", "rfob"], 2);
        let mut producer = Remote::new(eal, ["rfpa", "rfpb"], 2);
        let (stream, peer) = m.connect();
        let (rx, mut tx) = stream.split();
        let bound = m.spawn(m.handle().udp_bind(udp_config(7000)));
        let (udp_rx, mut udp_tx) = m.wait(&bound).unwrap().split();
        let remote = producer.frame(b"foreign");
        let mut local = m.fixture.peer.tx_frame().unwrap();
        local[..7].copy_from_slice(b"foreign");
        local.truncate(7);
        for frame in [remote, local.freeze()] {
            let ptr = frame.data_ptr();
            let ctx = frame.ctx();
            let (frame, error) = tx.try_send(frame).unwrap_err();
            assert_eq!(error, Error::Foreign);
            assert_eq!(frame.data_ptr(), ptr);
            assert_eq!(frame.ctx(), ctx);
            assert_eq!(&*frame, b"foreign");
            let (frame, error) = udp_tx.send(Some(peer_endpoint(7001)), frame).unwrap_err();
            assert_eq!(error, Error::Foreign);
            assert_eq!(frame.data_ptr(), ptr);
            assert_eq!(frame.ctx(), ctx);
            assert_eq!(&*frame, b"foreign");
        }
        drop((rx, tx, udp_rx, udp_tx, peer));
        producer.close();
        m.close();
    });
}

// Mutations tcp_empty_accepted / tcp_empty_commits_record / empty_replaces_frame:
// allow an empty TCP write, commit it despite rejecting it, or replace its frame.
// Mutation udp_empty_rejected: apply TCP's empty restriction to UDP datagrams.
#[switch_test_support::pinned_test]
fn remote_empty_tcp_keeps_frame_and_fifo_room_while_empty_udp_is_delivered() {
    with_eal(|eal| {
        let mut m = Remote::new(eal, ["rema", "remb"], 2);
        let (stream, peer) = m.connect();
        let (rx, mut tx) = stream.split();
        let mut empty = m.frame(b"");
        let frame = m.frame(b"after empty");
        let ptr = empty.data_ptr();
        let ctx = empty.ctx();
        // More zero-payload records than the ingress ring can hold, without
        // any host turns: rejecting Empty must never consume FIFO space.
        for _ in 0..4096 {
            let (returned, error) = tx.try_send(empty).unwrap_err();
            assert_eq!(error, Error::Empty);
            assert_eq!(returned.data_ptr(), ptr);
            assert_eq!(returned.ctx(), ctx);
            assert!(returned.is_empty());
            empty = returned;
        }
        tx.try_send(frame).expect("empty TCP attempts must not fill the FIFO");
        let mut received = Vec::new();
        m.until(|m| {
            while let Ok(Some(frame)) = m.fixture.peer.tcp_mut(&peer).recv() {
                received.extend_from_slice(frame.payload());
            }
            received.len() == 11
        });
        assert_eq!(received, b"after empty");
        let bound = m.spawn(m.handle().udp_bind(udp_config(7000)));
        let (udp_rx, mut udp_tx) = m.wait(&bound).unwrap().split();
        let udp_peer = m.fixture.peer.add_udp(udp_config(7001)).unwrap();
        udp_tx.send(Some(peer_endpoint(7001)), empty).unwrap();
        let mut datagram = None;
        m.until(|m| { datagram = m.fixture.peer.udp_mut(&udp_peer).recv(); datagram.is_some() });
        let datagram = datagram.unwrap();
        assert!(datagram.payload().is_empty());
        assert_eq!(datagram.src(), IpEndpoint::new(net_ip().into(), 7000));
        drop((datagram, rx, tx, udp_rx, udp_tx, udp_peer, peer));
        m.close();
    });
}

// Mutations connect_reset_is_closed / tcp_slots_full_wrong_error: erase the
// actual RST/refusal distinction or mistranslate the host TCP rejection.
#[switch_test_support::pinned_test]
fn remote_real_refusal_and_tcp_slot_exhaustion_map_to_distinct_errors() {
    with_eal(|eal| {
        let mut m = Remote::new(eal, ["rrea", "rreb"], 2);
        // No listener exists at the peer: its real TCP stack answers SYN with RST.
        let refused = m.spawn(m.handle().tcp_connect(
            peer_tcp_config(), peer_endpoint(PEER_PORT), local(LOCAL_PORT),
        ));
        assert!(matches!(m.wait(&refused), Err(Error::Refused)));
        m.close();
        let mut m = Remote::new(eal, ["rtea", "rteb"], 2);
        let mut occupied = Vec::new();
        for port in 0..4 {
            let accepting = m.spawn(m.accept_one(local(LOCAL_PORT + port)));
            m.rt.poll(0).unwrap();
            occupied.push(accepting);
        }
        // Bound is generated after the four accepts, proving the host has
        // processed them; Session still has ample free client-side slots.
        let barrier = m.control_barrier(7000);
        assert!(occupied.iter().all(|slot| !slot.ready()));
        let rejected = m.spawn(m.accept_one(local(LOCAL_PORT + 4)));
        assert!(matches!(m.wait(&rejected), Err(Error::SocketsFull)));
        drop((barrier, occupied));
        m.close();
    });
}
