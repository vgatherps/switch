//! switchnet receive side against a raw conduit Stack peer on net_ring.

use std::cell::Cell;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::{Pin, pin};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Wake, Waker};

use dpdk_smoltcp::TcpHandle;
use dpdk_smoltcp::smoltcp::time::Duration;
use switchnet::tcp::State;
use switchnet::{IpEndpoint, IpListenEndpoint, NetHandle, RecvError, StackError, TcpStream, UdpConfig};
use zhida_test_kit::pair::pattern;
use zhida_test_kit::with_eal;

mod support;
use support::{drive, net_ip, peer_endpoint, peer_listen, tcp_config, Mixed, Probe, PEER_PORT, UDP_QUEUE};

const LOCAL_PORT: u16 = 49152;

fn local(port: u16) -> IpListenEndpoint {
    IpListenEndpoint { addr: None, port }
}

/// Net connects from `port` to the peer's listener; drives until the stream
/// is returned. Returns it with the peer's handle.
fn connect(mixed: &mut Mixed, port: u16) -> (TcpStream, TcpHandle) {
    let peer = peer_listen(mixed);
    let probe = Probe::new();
    let connecting = mixed.handle().tcp_connect(tcp_config(), peer_endpoint(PEER_PORT), local(port)).unwrap();
    let stream = drive(mixed, &probe, pin!(connecting)).unwrap();
    mixed.until(|m| m.peer.tcp(&peer).state() == State::Established);
    (stream, peer)
}

/// Peer queues `data`; drives until it is all acknowledged.
fn peer_send(mixed: &mut Mixed, peer: &TcpHandle, data: &[u8]) {
    assert_eq!(mixed.peer.tcp_mut(peer).send_slice(data), Ok(data.len()));
}

fn udp_config(port: u16) -> UdpConfig {
    UdpConfig { port, rx_queue: UDP_QUEUE, tx_checksum: true, broadcast: false }
}

#[zhida_test_support::pinned_test]
fn connect_completes_and_wakes_the_connecting_task() {
    with_eal(|eal| {
        let mut mixed = Mixed::new(eal, ["cona", "conb"]);
        let peer = peer_listen(&mut mixed);
        let probe = Probe::new();
        let mut connecting = pin!(mixed.handle().tcp_connect(tcp_config(), peer_endpoint(PEER_PORT), local(LOCAL_PORT)).unwrap());
        assert!(probe.poll(connecting.as_mut()).is_pending());
        assert_eq!(probe.wakes(), 0);
        mixed.until(|_| probe.wakes() > 0);
        assert_eq!(probe.wakes(), 1);
        let Poll::Ready(Ok(stream)) = probe.poll(connecting.as_mut()) else { panic!("woken but not connected") };
        assert_eq!(probe.clones(), 0, "completed future left a waker parked");
        mixed.until(|m| m.peer.tcp(&peer).state() == State::Established);
        drop(stream);
        drop(peer);
        mixed.close();
    })
}

#[zhida_test_support::pinned_test]
fn refused_connect_reports_reset_by_peer() {
    with_eal(|eal| {
        let mut mixed = Mixed::new(eal, ["refa", "refb"]);
        let probe = Probe::new();
        let mut connecting = pin!(mixed.handle().tcp_connect(tcp_config(), peer_endpoint(PEER_PORT), local(LOCAL_PORT)).unwrap());
        assert!(probe.poll(connecting.as_mut()).is_pending());
        mixed.until(|_| probe.wakes() > 0);
        let Poll::Ready(Err(closed)) = probe.poll(connecting.as_mut()) else { panic!("expected Closed") };
        assert!(closed.reset_by_peer);
        mixed.close();
    })
}

#[zhida_test_support::pinned_test]
fn simultaneous_open_waits_through_syn_received() {
    with_eal(|eal| {
        let mut mixed = Mixed::new(eal, ["sima", "simb"]);
        // Resolve ARP both ways first (a refused connect from the peer), so the
        // SYNs below leave without waiting for a neighbor.
        let probe_socket = mixed.peer.add_tcp(tcp_config()).unwrap();
        mixed.peer.connect(&probe_socket, IpEndpoint::new(net_ip().into(), 1), PEER_PORT + 1).unwrap();
        mixed.until(|m| m.peer.tcp(&probe_socket).state() == State::Closed);
        mixed.peer.remove_tcp(probe_socket);
        let peer = mixed.peer.add_tcp(tcp_config()).unwrap();
        let net_end = IpEndpoint::new(net_ip().into(), LOCAL_PORT);
        mixed.peer.connect(&peer, net_end, PEER_PORT).unwrap();
        let probe = Probe::new();
        let mut connecting = pin!(mixed.handle().tcp_connect(tcp_config(), peer_endpoint(PEER_PORT), local(LOCAL_PORT)).unwrap());
        assert!(probe.poll(connecting.as_mut()).is_pending());
        // The peer's SYN reaches the Net while the Net is in SynSent and has
        // sent nothing: the Net turn moves it to SynReceived, which must not wake.
        mixed.peer.turn(mixed.now, zhida_test_kit::pair::BUDGET).unwrap();
        assert_eq!(mixed.peer.tcp(&peer).state(), State::SynSent);
        assert_eq!(mixed.to_net.forward(), 1);
        mixed.net.turn(mixed.now).unwrap();
        assert_eq!(probe.wakes(), 0, "woken in SynReceived");
        assert_eq!(mixed.to_peer.forward(), 1);
        mixed.until(|_| probe.wakes() > 0);
        assert_eq!(probe.wakes(), 1);
        let Poll::Ready(Ok(stream)) = probe.poll(connecting.as_mut()) else { panic!("woken before established") };
        mixed.until(|m| m.peer.tcp(&peer).state() == State::Established);
        drop(stream);
        drop(peer);
        mixed.close();
    })
}

#[zhida_test_support::pinned_test]
fn data_arrives_in_order_as_frames_and_wakes_the_parked_receiver() {
    with_eal(|eal| {
        let mut mixed = Mixed::new(eal, ["orda", "ordb"]);
        let (mut stream, peer) = connect(&mut mixed, LOCAL_PORT);
        let probe = Probe::new();
        {
            let mut recv = pin!(stream.recv());
            assert!(probe.poll(recv.as_mut()).is_pending());
            mixed.rounds(20);
            assert_eq!(probe.wakes(), 0, "idle turns woke the receiver");
        }
        let data = pattern(256 * 1024, 7);
        let mut sent = 0;
        let mut got = Vec::new();
        let mut frames = 0;
        while got.len() < data.len() {
            let room = mixed.peer.tcp(&peer).send_capacity() - mixed.peer.tcp(&peer).send_queue();
            let n = room.min(data.len() - sent);
            if n > 0 {
                peer_send(&mut mixed, &peer, &data[sent..sent + n]);
                sent += n;
            }
            let frame = drive(&mut mixed, &probe, pin!(stream.recv())).unwrap();
            assert!(!frame.payload().is_empty());
            assert_eq!(frame.rx_timestamp(), 0, "net_ring has no RX timestamp offload");
            got.extend_from_slice(frame.payload());
            frames += 1;
        }
        assert_eq!(got, data);
        assert!(frames > 1);
        assert!(probe.wakes() > 0);
        drop(stream);
        drop(peer);
        mixed.close();
    })
}

#[zhida_test_support::pinned_test]
fn held_frames_dropped_out_of_order_reopen_the_window() {
    with_eal(|eal| {
        let mut mixed = Mixed::new(eal, ["wina", "winb"]);
        let (mut stream, peer) = connect(&mut mixed, LOCAL_PORT);
        let data = pattern(64 * 1024, 3);
        peer_send(&mut mixed, &peer, &data);
        let probe = Probe::new();
        let mut held = Vec::new();
        // Take frames until the window is shut: nothing more arrives while all are held.
        'take: loop {
            let mut recv = pin!(stream.recv());
            loop {
                match probe.poll(recv.as_mut()) {
                    Poll::Ready(frame) => {
                        held.push(frame.unwrap());
                        continue 'take;
                    }
                    Poll::Pending => {
                        let woken = probe.wakes();
                        mixed.rounds(50);
                        if probe.wakes() == woken {
                            break 'take;
                        }
                    }
                }
            }
        }
        assert!(held.len() >= 3, "only {} frames held", held.len());
        let received: usize = held.iter().map(|f| f.payload().len()).sum();
        assert!(received < data.len());
        let mut got: Vec<u8> = held.iter().flat_map(|f| f.payload().to_vec()).collect();
        assert_eq!(got, data[..received]);
        // Drop a middle frame then the last; the head stays alive.
        let head = held.remove(0);
        held.remove(held.len() / 2);
        held.pop();
        let frame = drive(&mut mixed, &probe, pin!(stream.recv())).unwrap();
        got.extend_from_slice(frame.payload());
        assert_eq!(got, data[..got.len()], "data after the reopened window is out of order");
        drop((head, held, frame));
        while got.len() < data.len() {
            let frame = drive(&mut mixed, &probe, pin!(stream.recv())).unwrap();
            got.extend_from_slice(frame.payload());
        }
        assert_eq!(got, data);
        drop(stream);
        drop(peer);
        mixed.close();
    })
}

#[zhida_test_support::pinned_test]
fn fin_gives_finished_after_the_last_frame() {
    with_eal(|eal| {
        let mut mixed = Mixed::new(eal, ["fina", "finb"]);
        let (mut stream, peer) = connect(&mut mixed, LOCAL_PORT);
        let probe = Probe::new();
        peer_send(&mut mixed, &peer, b"last bytes");
        mixed.peer.tcp_mut(&peer).close();
        let frame = drive(&mut mixed, &probe, pin!(stream.recv())).unwrap();
        assert_eq!(frame.payload(), b"last bytes");
        assert_eq!(drive(&mut mixed, &probe, pin!(stream.recv())).unwrap_err(), RecvError::Finished);
        drop((stream, frame, peer));
        mixed.close();
    })
}

#[zhida_test_support::pinned_test]
fn parked_receiver_is_woken_by_fin() {
    with_eal(|eal| {
        let mut mixed = Mixed::new(eal, ["pfna", "pfnb"]);
        let (mut stream, peer) = connect(&mut mixed, LOCAL_PORT);
        let probe = Probe::new();
        let mut recv = Box::pin(stream.recv());
        assert!(probe.poll(recv.as_mut()).is_pending());
        mixed.peer.tcp_mut(&peer).close();
        mixed.until(|_| probe.wakes() > 0);
        let Poll::Ready(Err(error)) = probe.poll(recv.as_mut()) else { panic!("FIN not reported") };
        assert_eq!(error, RecvError::Finished);
        drop(recv);
        drop((stream, peer));
        mixed.close();
    })
}

#[zhida_test_support::pinned_test]
fn reset_wakes_the_parked_receiver_and_reports() {
    with_eal(|eal| {
        let mut mixed = Mixed::new(eal, ["rsta", "rstb"]);
        let (mut stream, peer) = connect(&mut mixed, LOCAL_PORT);
        let probe = Probe::new();
        let mut recv = Box::pin(stream.recv());
        assert!(probe.poll(recv.as_mut()).is_pending());
        mixed.peer.tcp_mut(&peer).abort();
        mixed.until(|_| probe.wakes() > 0);
        let Poll::Ready(Err(error)) = probe.poll(recv.as_mut()) else { panic!("reset not reported") };
        assert_eq!(error, RecvError::InvalidState);
        drop(recv);
        drop((stream, peer));
        mixed.close();
    })
}

#[zhida_test_support::pinned_test]
fn udp_datagrams_arrive_with_source_address() {
    with_eal(|eal| {
        let mut mixed = Mixed::new(eal, ["udpa", "udpb"]);
        let mut socket = mixed.handle().udp_bind(udp_config(7000)).unwrap();
        let peer = mixed.peer.add_udp(udp_config(7001)).unwrap();
        let probe = Probe::new();
        let mut recv = Box::pin(socket.recv());
        assert!(probe.poll(recv.as_mut()).is_pending());
        let to = Some(IpEndpoint::new(net_ip().into(), 7000));
        let payloads: [&[u8]; 2] = [b"one", b"two"];
        // The first send resolves the Net's MAC.
        mixed.until(|m| match m.peer.udp_send(&peer, to, payloads[0]) {
            Ok(()) => true,
            Err(error) => {
                assert_eq!(error, dpdk_smoltcp::UdpSendError::NeighborPending);
                false
            }
        });
        mixed.peer.udp_send(&peer, to, payloads[1]).unwrap();
        mixed.until(|_| probe.wakes() > 0);
        assert_eq!(probe.wakes(), 1);
        let Poll::Ready(first) = probe.poll(recv.as_mut()) else { panic!("woken without a datagram") };
        drop(recv);
        let second = drive(&mut mixed, &probe, pin!(socket.recv()));
        for (datagram, payload) in [&first, &second].into_iter().zip(payloads) {
            assert_eq!(datagram.payload(), payload);
            assert_eq!(datagram.src(), IpEndpoint::new(support::peer_ip().into(), 7001));
            assert_eq!(datagram.dst_addr(), net_ip());
            assert_eq!(datagram.rx_timestamp(), 0);
        }
        drop((first, second, socket, peer));
        mixed.close();
    })
}

#[zhida_test_support::pinned_test]
fn task_parked_on_one_socket_is_not_woken_by_another() {
    with_eal(|eal| {
        let mut mixed = Mixed::new(eal, ["onea", "oneb"]);
        let (mut quiet, quiet_peer) = connect(&mut mixed, LOCAL_PORT);
        let (mut busy, busy_peer) = connect(&mut mixed, LOCAL_PORT + 1);
        let mut udp = mixed.handle().udp_bind(udp_config(7000)).unwrap();
        let [quiet_probe, udp_probe, busy_probe] = [Probe::new(), Probe::new(), Probe::new()];
        let mut quiet_recv = Box::pin(quiet.recv());
        let mut udp_recv = Box::pin(udp.recv());
        let mut busy_recv = Box::pin(busy.recv());
        assert!(quiet_probe.poll(quiet_recv.as_mut()).is_pending());
        assert!(udp_probe.poll(udp_recv.as_mut()).is_pending());
        assert!(busy_probe.poll(busy_recv.as_mut()).is_pending());
        peer_send(&mut mixed, &busy_peer, b"only for busy");
        mixed.until(|_| busy_probe.wakes() > 0);
        mixed.rounds(10);
        assert_eq!([quiet_probe.wakes(), udp_probe.wakes(), busy_probe.wakes()], [0, 0, 1]);
        let Poll::Ready(Ok(frame)) = busy_probe.poll(busy_recv.as_mut()) else { panic!("busy not ready") };
        assert_eq!(frame.payload(), b"only for busy");
        drop((busy_recv, quiet_recv, udp_recv));
        drop((frame, quiet, busy, udp, quiet_peer, busy_peer));
        mixed.close();
    })
}

#[zhida_test_support::pinned_test]
fn dropped_recv_future_clears_its_waker() {
    with_eal(|eal| {
        let mut mixed = Mixed::new(eal, ["drpa", "drpb"]);
        let (mut stream, peer) = connect(&mut mixed, LOCAL_PORT);
        let probe = Probe::new();
        {
            let mut recv = pin!(stream.recv());
            assert!(probe.poll(recv.as_mut()).is_pending());
            assert_eq!(probe.clones(), 1);
        }
        assert_eq!(probe.clones(), 0);
        peer_send(&mut mixed, &peer, b"data");
        mixed.rounds(10);
        assert_eq!(probe.wakes(), 0);
        let frame = drive(&mut mixed, &probe, pin!(stream.recv())).unwrap();
        assert_eq!(frame.payload(), b"data");
        drop((frame, stream, peer));
        mixed.close();
    })
}

#[zhida_test_support::pinned_test]
fn into_stack_refused_while_a_socket_or_handle_lives() {
    with_eal(|eal| {
        let mixed = Mixed::new(eal, ["inta", "intb"]);
        let socket = mixed.handle().udp_bind(udp_config(7000)).unwrap();
        let mixed = mixed.into_stack_refused();
        drop(socket);
        let handle = mixed.handle();
        let mixed = mixed.into_stack_refused();
        drop(handle);
        mixed.close();
    })
}

/// A waker whose clone and drop callbacks open and close a UDP socket through
/// a NetHandle: switchnet must not run them while it holds the stack.
struct Reenter {
    handle: NetHandle,
    clones: Cell<usize>,
    wakes: Cell<usize>,
    drops: Cell<usize>,
}

impl Reenter {
    fn touch(&self) {
        drop(self.handle.udp_bind(udp_config(9000)).unwrap());
    }
}

static REENTER_VTABLE: RawWakerVTable = RawWakerVTable::new(
    |data| {
        let reenter = unsafe { &*data.cast::<Reenter>() };
        reenter.clones.set(reenter.clones.get() + 1);
        reenter.touch();
        RawWaker::new(data, &REENTER_VTABLE)
    },
    |data| {
        let reenter = unsafe { &*data.cast::<Reenter>() };
        reenter.wakes.set(reenter.wakes.get() + 1);
        reenter.touch();
    },
    |data| {
        let reenter = unsafe { &*data.cast::<Reenter>() };
        reenter.wakes.set(reenter.wakes.get() + 1);
        reenter.touch();
    },
    |data| {
        let reenter = unsafe { &*data.cast::<Reenter>() };
        reenter.drops.set(reenter.drops.get() + 1);
        reenter.touch();
    },
);

#[zhida_test_support::pinned_test]
fn waker_clone_and_drop_may_reenter_the_net() {
    with_eal(|eal| {
        let mut mixed = Mixed::new(eal, ["reea", "reeb"]);
        let (mut stream, peer) = connect(&mut mixed, LOCAL_PORT);
        let new = || Box::new(Reenter { handle: mixed.handle(), clones: Cell::new(0), wakes: Cell::new(0), drops: Cell::new(0) });
        // Two distinct data pointers so the second poll replaces, not keeps, the parked waker.
        let (reenter, other) = (new(), new());
        let data = std::ptr::from_ref(&*reenter).cast();
        let other_data = std::ptr::from_ref(&*other).cast();
        {
            let first = std::mem::ManuallyDrop::new(unsafe { Waker::from_raw(RawWaker::new(data, &REENTER_VTABLE)) });
            let second = std::mem::ManuallyDrop::new(unsafe { Waker::from_raw(RawWaker::new(other_data, &REENTER_VTABLE)) });
            let mut recv = pin!(stream.recv());
            assert!(recv.as_mut().poll(&mut Context::from_waker(&first)).is_pending());
            assert_eq!(reenter.clones.get(), 1);
            assert!(recv.as_mut().poll(&mut Context::from_waker(&second)).is_pending());
            assert_eq!(other.clones.get(), 1);
            assert_eq!(reenter.drops.get(), 1, "replaced waker not dropped");
            peer_send(&mut mixed, &peer, b"x");
            mixed.until(|_| other.wakes.get() > 0);
            let Poll::Ready(Ok(frame)) = recv.as_mut().poll(&mut Context::from_waker(&second)) else {
                panic!("not ready after wake")
            };
            assert_eq!(frame.payload(), b"x");
        }
        // Every clone switchnet made was dropped or consumed by a wake.
        assert_eq!([reenter.clones.get(), reenter.wakes.get(), reenter.drops.get()], [1, 0, 1]);
        assert_eq!([other.clones.get(), other.wakes.get(), other.drops.get()], [1, 1, 0]);
        drop((stream, peer, reenter, other));
        mixed.close();
    })
}

/// Peer sends `payload` to the Net's `port`, turning until the Net's MAC is resolved.
fn peer_udp_send(mixed: &mut Mixed, peer: &dpdk_smoltcp::UdpHandle, port: u16, payload: &[u8]) {
    let to = Some(IpEndpoint::new(net_ip().into(), port));
    mixed.until(|m| match m.peer.udp_send(peer, to, payload) {
        Ok(()) => true,
        Err(error) => {
            assert_eq!(error, dpdk_smoltcp::UdpSendError::NeighborPending);
            false
        }
    });
}

struct PanickingWake(AtomicUsize);

impl Wake for PanickingWake {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::Relaxed);
        panic!("wake panicked");
    }
}

#[zhida_test_support::pinned_test]
fn panicking_wake_leaves_the_other_ready_task_for_the_next_turn() {
    with_eal(|eal| {
        let mut mixed = Mixed::new(eal, ["pwka", "pwkb"]);
        let mut quiet = mixed.handle().udp_bind(udp_config(7000)).unwrap();
        let mut loud = mixed.handle().udp_bind(udp_config(7001)).unwrap();
        let peer = mixed.peer.add_udp(udp_config(7002)).unwrap();
        peer_udp_send(&mut mixed, &peer, 7000, b"warm");
        let warm = drive(&mut mixed, &Probe::new(), pin!(quiet.recv()));
        assert_eq!(warm.payload(), b"warm");
        let probe = Probe::new();
        let panicking = Arc::new(PanickingWake(AtomicUsize::new(0)));
        let panicking_waker = Waker::from(panicking.clone());
        // Parked in this order, the scan collects quiet then loud and the
        // delivery wakes loud first.
        let mut quiet_recv = Box::pin(quiet.recv());
        let mut loud_recv = Box::pin(loud.recv());
        assert!(probe.poll(quiet_recv.as_mut()).is_pending());
        assert!(loud_recv.as_mut().poll(&mut Context::from_waker(&panicking_waker)).is_pending());
        let to = |port| Some(IpEndpoint::new(net_ip().into(), port));
        mixed.peer.udp_send(&peer, to(7000), b"quiet").unwrap();
        mixed.peer.udp_send(&peer, to(7001), b"loud").unwrap();
        mixed.peer.turn(mixed.now, zhida_test_kit::pair::BUDGET).unwrap();
        assert_eq!(mixed.to_net.forward(), 2);
        let now = mixed.now;
        let unwound = catch_unwind(AssertUnwindSafe(|| mixed.net.turn(now)));
        assert!(unwound.is_err());
        assert_eq!(panicking.0.load(Ordering::Relaxed), 1);
        assert_eq!(probe.wakes(), 0, "quiet woken before loud: the test lost its point");
        mixed.net.turn(now).unwrap();
        assert_eq!(probe.wakes(), 1, "the other ready task's wake was lost");
        mixed.rounds(5);
        assert_eq!([probe.wakes(), panicking.0.load(Ordering::Relaxed)], [1, 1]);
        let Poll::Ready(datagram) = probe.poll(quiet_recv.as_mut()) else { panic!("woken without a datagram") };
        assert_eq!(datagram.payload(), b"quiet");
        let Poll::Ready(datagram_loud) = loud_recv.as_mut().poll(&mut Context::from_waker(&panicking_waker))
        else {
            panic!("loud datagram lost")
        };
        assert_eq!(datagram_loud.payload(), b"loud");
        drop((quiet_recv, loud_recv, panicking_waker));
        assert_eq!(probe.clones(), 0);
        drop((warm, datagram, datagram_loud, quiet, loud, peer));
        mixed.close();
    })
}

#[zhida_test_support::pinned_test]
fn canceled_connect_drops_the_socket_without_waking() {
    with_eal(|eal| {
        let mut mixed = Mixed::new(eal, ["ccna", "ccnb"]);
        let peer = peer_listen(&mut mixed);
        let probe = Probe::new();
        {
            let mut connecting =
                pin!(mixed.handle().tcp_connect(tcp_config(), peer_endpoint(PEER_PORT), local(LOCAL_PORT)).unwrap());
            assert!(probe.poll(connecting.as_mut()).is_pending());
            assert_eq!(probe.clones(), 1);
        }
        assert_eq!(probe.clones(), 0, "canceled Connecting left its waker parked");
        mixed.rounds(10);
        assert_eq!(probe.wakes(), 0);
        assert_eq!(mixed.peer.tcp(&peer).state(), State::Listen, "the dropped socket still connected");
        drop(peer);
        mixed.close();
    })
}

#[zhida_test_support::pinned_test]
fn canceled_udp_recv_clears_its_waker() {
    with_eal(|eal| {
        let mut mixed = Mixed::new(eal, ["cura", "curb"]);
        let mut socket = mixed.handle().udp_bind(udp_config(7000)).unwrap();
        let peer = mixed.peer.add_udp(udp_config(7001)).unwrap();
        let probe = Probe::new();
        {
            let mut recv = pin!(socket.recv());
            assert!(probe.poll(recv.as_mut()).is_pending());
            assert_eq!(probe.clones(), 1);
        }
        assert_eq!(probe.clones(), 0);
        peer_udp_send(&mut mixed, &peer, 7000, b"after cancel");
        mixed.rounds(10);
        assert_eq!(probe.wakes(), 0, "canceled recv was woken");
        let datagram = drive(&mut mixed, &probe, pin!(socket.recv()));
        assert_eq!(datagram.payload(), b"after cancel");
        drop((datagram, socket, peer));
        mixed.close();
    })
}

/// A leaked recv future never runs its drop: its waker stays parked until
/// data wakes it once, or the stream drops it.
#[zhida_test_support::pinned_test]
fn forgotten_recv_future_is_woken_once_then_released() {
    with_eal(|eal| {
        let mut mixed = Mixed::new(eal, ["fgta", "fgtb"]);
        let (mut stream, peer) = connect(&mut mixed, LOCAL_PORT);
        let probe = Probe::new();
        let mut recv = stream.recv();
        assert!(probe.poll(Pin::new(&mut recv)).is_pending());
        std::mem::forget(recv);
        assert_eq!(probe.clones(), 1);
        peer_send(&mut mixed, &peer, b"data");
        mixed.until(|_| probe.wakes() > 0);
        mixed.rounds(5);
        assert_eq!([probe.wakes(), probe.clones()], [1, 0]);
        let frame = drive(&mut mixed, &probe, pin!(stream.recv())).unwrap();
        assert_eq!(frame.payload(), b"data");
        let mut recv = stream.recv();
        assert!(probe.poll(Pin::new(&mut recv)).is_pending());
        std::mem::forget(recv);
        assert_eq!(probe.clones(), 1);
        drop(stream);
        assert_eq!(probe.clones(), 0, "dropping the stream kept the forgotten future's waker");
        drop((frame, peer));
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
        let probe = Probe::new();
        peer_send(&mut mixed, &peer, b"tcp bytes");
        let frame = drive(&mut mixed, &probe, pin!(stream.recv())).unwrap();
        peer_udp_send(&mut mixed, &udp_peer, 7000, b"udp bytes");
        let datagram = drive(&mut mixed, &probe, pin!(udp.recv()));
        drop((stream, udp, peer, udp_peer));
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

/// A turn that returns an error (time went backwards: no work done) wakes
/// nothing and keeps the parked task; the next turn processes the waiting
/// datagram and wakes it.
#[zhida_test_support::pinned_test]
fn error_turn_keeps_the_parked_task_for_the_next_turn() {
    with_eal(|eal| {
        let mut mixed = Mixed::new(eal, ["erra", "errb"]);
        let earlier = mixed.now;
        mixed.now = earlier + Duration::from_millis(1);
        let mut socket = mixed.handle().udp_bind(udp_config(7000)).unwrap();
        let peer = mixed.peer.add_udp(udp_config(7001)).unwrap();
        peer_udp_send(&mut mixed, &peer, 7000, b"warm");
        let probe = Probe::new();
        let warm = drive(&mut mixed, &probe, pin!(socket.recv()));
        let mut recv = Box::pin(socket.recv());
        assert!(probe.poll(recv.as_mut()).is_pending());
        let woken = probe.wakes();
        mixed.peer.udp_send(&peer, Some(IpEndpoint::new(net_ip().into(), 7000)), b"after error").unwrap();
        mixed.peer.turn(mixed.now, zhida_test_kit::pair::BUDGET).unwrap();
        assert_eq!(mixed.to_net.forward(), 1);
        let Err(StackError::TimeWentBackwards { .. }) = mixed.net.turn(earlier) else {
            panic!("turn at an earlier time did not fail")
        };
        assert_eq!([probe.wakes() - woken, probe.clones()], [0, 1]);
        mixed.net.turn(mixed.now).unwrap();
        assert_eq!([probe.wakes() - woken, probe.clones()], [1, 0]);
        let Poll::Ready(datagram) = probe.poll(recv.as_mut()) else { panic!("woken without a datagram") };
        assert_eq!(datagram.payload(), b"after error");
        drop(recv);
        drop((warm, datagram, socket, peer));
        mixed.close();
    })
}
