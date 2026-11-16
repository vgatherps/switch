//! A Net owning two addresses besides its own (10.0.0.77 and 10.0.0.78, in the
//! peer's subnet so the peer reaches them by ARP): connect and accept on a
//! secondary address, UDP sockets bound per address, sockets bound to one
//! address taking precedence over all-IP ones, and tasks parked on secondary
//! address sockets woken only by their own traffic. Every future runs as a
//! switch task.

use dpdk_smoltcp::smoltcp::wire::Ipv4Cidr;
use switch_net::tcp::State;
use switch_net::{Error, Datagram, IpEndpoint, Ipv4Address, NetHandle, UdpRx, UdpSocket, UdpTx};
use switch_test_kit::with_eal;

mod support;
use support::{
    bind,
    counted, local, local_at, net_ip, peer_endpoint, peer_listen, peer_send, peer_udp_send_to, tcp_config,
    udp_config, udp_config_at, Mixed, LOCAL_PORT, PEER_PORT,
};

const SECOND: Ipv4Address = Ipv4Address::new(10, 0, 0, 77);
const THIRD: Ipv4Address = Ipv4Address::new(10, 0, 0, 78);
const OWNED: &[Ipv4Cidr] = &[Ipv4Cidr::new(SECOND, 32), Ipv4Cidr::new(THIRD, 32)];
const UDP_PORT: u16 = 7000;
const PEER_UDP: u16 = 7001;

fn at(addr: Ipv4Address, port: u16) -> IpEndpoint {
    IpEndpoint::new(addr.into(), port)
}

/// Peer socket connecting from `peer_port` to the Net's `to`; drives until established.
fn peer_connect(mixed: &mut Mixed, to: IpEndpoint, peer_port: u16) -> dpdk_smoltcp::TcpHandle {
    let peer = mixed.peer.add_tcp(tcp_config()).unwrap();
    mixed.peer.connect(&peer, to, peer_port).unwrap();
    mixed.until(|m| m.peer.tcp(&peer).state() == State::Established);
    peer
}

/// Receives one datagram and echoes it back to its sender.
async fn echo_once(handle: NetHandle, socket: UdpSocket) -> (UdpRx, UdpTx, Datagram) {
    let (mut rx, mut tx) = socket.split();
    let datagram = rx.recv().await;
    let mut frame = handle.tx_frame().await;
    frame.as_mut()[..datagram.payload().len()].copy_from_slice(datagram.payload());
    frame.truncate(datagram.payload().len());
    let frame = frame.freeze();
    tx.send(Some(datagram.src()), frame).map_err(|(_, error)| error).unwrap();
    (rx, tx, datagram)
}

support::both_backends!(connect_from_a_secondary_address_uses_it_conduit, connect_from_a_secondary_address_uses_it_junction, |backend| {
    with_eal(|eal| {
        let mut mixed = Mixed::with_owned(eal, ["mcna", "mcnb"], OWNED, backend);
        let unowned = Ipv4Address::new(10, 0, 0, 99);
        let refused = mixed.handle().tcp_connect(tcp_config(), peer_endpoint(PEER_PORT), local_at(unowned, LOCAL_PORT));
        let refused = mixed.spawn(refused);
        assert!(matches!(mixed.wait(&refused), Err(Error::Unaddressable)));
        let peer = peer_listen(&mut mixed);
        let connecting =
            mixed.handle().tcp_connect(tcp_config(), peer_endpoint(PEER_PORT), local_at(SECOND, LOCAL_PORT));
        let slot = mixed.spawn(async move {
            let mut stream = connecting.await.expect("connect");
            let frame = stream.recv().await.unwrap();
            (stream, frame)
        });
        mixed.until(|m| m.peer.tcp(&peer).state() == State::Established);
        assert_eq!(mixed.peer.tcp(&peer).remote_endpoint(), Some(at(SECOND, LOCAL_PORT)));
        peer_send(&mut mixed, &peer, b"to the second address");
        let (stream, frame) = mixed.wait(&slot);
        assert_eq!(frame.payload(), b"to the second address");
        drop((stream, frame, peer));
        mixed.close();
    })
});

// A listener bound to the second address refuses a connection to the
// primary address on its port and accepts one to its own.
support::both_backends!(accept_on_a_secondary_address_takes_only_its_connections_conduit, accept_on_a_secondary_address_takes_only_its_connections_junction, |backend| {
    with_eal(|eal| {
        let mut mixed = Mixed::with_owned(eal, ["maca", "macb"], OWNED, backend);
        let accepting = mixed.handle().tcp_accept(tcp_config(), local_at(SECOND, LOCAL_PORT));
        let slot = mixed.spawn(async move {
            let mut stream = accepting.await.unwrap();
            let frame = stream.recv().await.unwrap();
            (stream, frame)
        });
        mixed.turn();
        let refused = mixed.peer.add_tcp(tcp_config()).unwrap();
        mixed.peer.connect(&refused, at(net_ip(), LOCAL_PORT), PEER_PORT).unwrap();
        mixed.until(|m| m.peer.tcp(&refused).state() == State::Closed);
        assert!(mixed.peer.tcp(&refused).reset_by_peer());
        assert!(!slot.is_full());
        let peer = peer_connect(&mut mixed, at(SECOND, LOCAL_PORT), PEER_PORT + 1);
        peer_send(&mut mixed, &peer, b"accepted on .77");
        let (stream, frame) = mixed.wait(&slot);
        assert_eq!(frame.payload(), b"accepted on .77");
        drop((stream, frame, peer, refused));
        mixed.close();
    })
});

// The all-IP listener has the lower slot, yet a SYN to the second address
// goes to the listener bound there; a SYN to the third address, which has no
// listener of its own, goes to the all-IP one.
support::both_backends!(listener_bound_to_an_address_beats_the_all_ip_listener_conduit, listener_bound_to_an_address_beats_the_all_ip_listener_junction, |backend| {
    with_eal(|eal| {
        let mut mixed = Mixed::with_owned(eal, ["mlpa", "mlpb"], OWNED, backend);
        let (all, all_polls) = counted(mixed.handle().tcp_accept(tcp_config(), local(LOCAL_PORT)));
        let exact = mixed.handle().tcp_accept(tcp_config(), local_at(SECOND, LOCAL_PORT));
        let all = mixed.spawn(all);
        let exact = mixed.spawn(exact);
        mixed.turn();
        let to_second = peer_connect(&mut mixed, at(SECOND, LOCAL_PORT), PEER_PORT);
        mixed.until(|_| exact.is_full());
        assert!(!all.is_full(), "the all-IP listener took the connection to the bound address");
        assert_eq!(all_polls.get(), 1, "the all-IP listener's task was woken");
        let to_third = peer_connect(&mut mixed, at(THIRD, LOCAL_PORT), PEER_PORT + 1);
        let streams = [mixed.wait(&exact).unwrap(), mixed.wait(&all).unwrap()];
        drop((streams, to_second, to_third));
        mixed.close();
    })
});

// Two sockets on one port, each bound to its own address: a datagram reaches
// the socket bound to its destination, which replies from that address.
support::both_backends!(udp_sockets_bound_per_address_receive_and_reply_from_it_conduit, udp_sockets_bound_per_address_receive_and_reply_from_it_junction, |backend| {
    with_eal(|eal| {
        let mut mixed = Mixed::with_owned(eal, ["mupa", "mupb"], OWNED, backend);
        let handle = mixed.handle();
        let [second, third] = [SECOND, THIRD].map(|addr| {
            let socket = bind(&mut mixed, udp_config_at(addr, UDP_PORT));
            let (task, polls) = counted(echo_once(handle.clone(), socket));
            (mixed.spawn(task), polls)
        });
        mixed.turn();
        let peer = mixed.peer.add_udp(udp_config(PEER_UDP)).unwrap();
        // The second address's datagram first: the third address's task stays parked.
        for (addr, slot, polls, waiting) in [(SECOND, &second.0, &second.1, Some(&third.1)), (THIRD, &third.0, &third.1, None)] {
            peer_udp_send_to(&mut mixed, &peer, addr, UDP_PORT, b"ping");
            mixed.until(|_| slot.is_full());
            let (rx, tx, datagram) = slot.take();
            assert_eq!(datagram.dst_addr(), addr);
            assert_eq!(polls.get(), 2);
            if let Some(waiting) = waiting {
                assert_eq!(waiting.get(), 1, "the other address's socket was woken");
            }
            let mut reply = None;
            mixed.until(|m| {
                reply = m.peer.udp_mut(&peer).recv();
                reply.is_some()
            });
            assert_eq!(reply.as_ref().unwrap().src(), at(addr, UDP_PORT), "the reply left from another address");
            drop((rx, tx, datagram, reply));
        }
        drop((peer, handle));
        mixed.close();
    })
});

// The all-IP socket was bound first. A datagram to the second address goes
// to the socket bound there without waking the all-IP socket's task;
// datagrams to the third and the primary address go to the all-IP socket.
support::both_backends!(udp_socket_bound_to_an_address_beats_the_all_ip_socket_conduit, udp_socket_bound_to_an_address_beats_the_all_ip_socket_junction, |backend| {
    with_eal(|eal| {
        let mut mixed = Mixed::with_owned(eal, ["mupra", "muprb"], OWNED, backend);
        let mut all = bind(&mut mixed, udp_config(UDP_PORT));
        let mut exact = bind(&mut mixed, udp_config_at(SECOND, UDP_PORT));
        let (all_task, all_polls) = counted(async move {
            let first = all.recv().await;
            let second = all.recv().await;
            (all, [first, second])
        });
        let all_slot = mixed.spawn(all_task);
        let exact_slot = mixed.spawn(async move {
            let datagram = exact.recv().await;
            (exact, datagram)
        });
        mixed.turn();
        let peer = mixed.peer.add_udp(udp_config(PEER_UDP)).unwrap();
        peer_udp_send_to(&mut mixed, &peer, SECOND, UDP_PORT, b"bound");
        mixed.until(|_| exact_slot.is_full());
        assert_eq!(all_polls.get(), 1, "the all-IP socket's task was woken");
        let (exact, datagram) = exact_slot.take();
        assert_eq!((datagram.dst_addr(), datagram.payload()), (SECOND, &b"bound"[..]));
        peer_udp_send_to(&mut mixed, &peer, THIRD, UDP_PORT, b"third");
        peer_udp_send_to(&mut mixed, &peer, net_ip(), UDP_PORT, b"primary");
        mixed.until(|_| all_slot.is_full());
        let (all, [first, second]) = all_slot.take();
        assert_eq!((first.dst_addr(), first.payload()), (THIRD, &b"third"[..]));
        assert_eq!((second.dst_addr(), second.payload()), (net_ip(), &b"primary"[..]));
        drop((all, exact, datagram, first, second, peer));
        mixed.close();
    })
});

// Readiness tokens on secondary address sockets: a TCP reader on a
// connection accepted on the second address, a UDP reader bound there and an
// all-IP UDP reader on the same port all park; idle turns poll none, and each
// arrival wakes exactly its own reader once.
support::both_backends!(parked_tasks_on_secondary_address_sockets_wake_only_on_their_own_traffic_conduit, parked_tasks_on_secondary_address_sockets_wake_only_on_their_own_traffic_junction, |backend| {
    with_eal(|eal| {
        let mut mixed = Mixed::with_owned(eal, ["mtka", "mtkb"], OWNED, backend);
        let accepting = mixed.handle().tcp_accept(tcp_config(), local_at(SECOND, LOCAL_PORT));
        let accepted = mixed.spawn(accepting);
        mixed.turn();
        let tcp_peer = peer_connect(&mut mixed, at(SECOND, LOCAL_PORT), PEER_PORT);
        let mut stream = mixed.wait(&accepted).unwrap();
        let (tcp_task, tcp_polls) = counted(async move {
            let frame = stream.recv().await.unwrap();
            (stream, frame)
        });
        let mut exact = bind(&mut mixed, udp_config_at(SECOND, UDP_PORT));
        let (exact_task, exact_polls) = counted(async move {
            let datagram = exact.recv().await;
            (exact, datagram)
        });
        let mut all = bind(&mut mixed, udp_config(UDP_PORT));
        let (all_task, all_polls) = counted(async move {
            let datagram = all.recv().await;
            (all, datagram)
        });
        let slots = (mixed.spawn(tcp_task), mixed.spawn(exact_task), mixed.spawn(all_task));
        let udp_peer = mixed.peer.add_udp(udp_config(PEER_UDP)).unwrap();
        mixed.rounds(5);
        for _ in 0..20 {
            mixed.turn();
        }
        let polls = || [tcp_polls.get(), exact_polls.get(), all_polls.get()];
        assert_eq!(polls(), [1, 1, 1], "a task was polled while nothing arrived");

        peer_udp_send_to(&mut mixed, &udp_peer, SECOND, UDP_PORT, b"udp");
        mixed.until(|_| slots.1.is_full());
        assert_eq!(polls(), [1, 2, 1]);
        peer_send(&mut mixed, &tcp_peer, b"tcp");
        mixed.until(|_| slots.0.is_full());
        assert_eq!(polls(), [2, 2, 1]);
        peer_udp_send_to(&mut mixed, &udp_peer, THIRD, UDP_PORT, b"all");
        mixed.until(|_| slots.2.is_full());
        assert_eq!(polls(), [2, 2, 2]);
        let ((stream, frame), (exact, bound), (all, any)) = (slots.0.take(), slots.1.take(), slots.2.take());
        assert_eq!((frame.payload(), bound.payload(), any.payload()), (&b"tcp"[..], &b"udp"[..], &b"all"[..]));
        drop((stream, frame, exact, bound, all, any, tcp_peer, udp_peer));
        mixed.close();
    })
});
