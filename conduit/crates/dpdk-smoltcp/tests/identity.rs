//! Connect, listen, close and abort through the Stack: each takes effect in the call,
//! before any poll.

use dpdk_smoltcp::smoltcp::socket::tcp::{ConnectError, State};
use dpdk_smoltcp::smoltcp::wire::{EthernetFrame, EthernetProtocol, IpEndpoint, Ipv4Address, TcpControl};
use dpdk_smoltcp::Eal;

use zhida_test_kit::pair::{
    config, establish, segment, tcp_socket_sized, Dir, Pair, Side, CLIENT_PORT, SERVER_PORT, T0,
};
use zhida_test_kit::with_eal;

/// Two sockets per Stack.
fn pair(eal: &Eal, names: [&str; 2]) -> Pair {
    let configs = [Side::A, Side::B].map(|side| dpdk_smoltcp::StackConfig { max_sockets: 2, ..config(side) });
    Pair::new(eal, names, configs, T0)
}

/// Control, source port and destination port of every TCP segment one round delivers.
fn round(pair: &mut Pair) -> Vec<(Dir, TcpControl, u16, u16)> {
    let mut seen = Vec::new();
    pair.round(|dir, frame| {
        if EthernetFrame::new_checked(frame).unwrap().ethertype() != EthernetProtocol::Ipv4 {
            return;
        }
        let side = if dir == Dir::AToB { Side::A } else { Side::B };
        let tcp = segment(frame, side);
        seen.push((dir, tcp.control, tcp.src_port, tcp.dst_port));
    });
    seen
}

fn server() -> IpEndpoint {
    IpEndpoint::new(Side::B.ip().into(), SERVER_PORT)
}

/// A refused connect leaves the socket Closed with no endpoints and sends nothing: from an
/// address the Stack does not own, and on a tuple a live connection holds.
#[zhida_test_support::pinned_test]
fn failed_connect_changes_nothing() {
    with_eal(|eal| {
        let mut pair = pair(eal, ["ida", "idb"]);
        let (_handles, _) = establish(&mut pair, Side::A);
        let other = pair.a.add_tcp(tcp_socket_sized(4096)).unwrap();
        let poll_at = pair.a.poll_at();
        let unowned = (Ipv4Address::new(10, 9, 9, 9), CLIENT_PORT + 1);
        assert_eq!(pair.a.connect(&other, server(), unowned), Err(ConnectError::Unaddressable));
        assert_eq!(pair.a.connect(&other, server(), CLIENT_PORT), Err(ConnectError::AddressInUse));
        let socket = pair.a.tcp(&other);
        assert_eq!(socket.state(), State::Closed);
        assert_eq!((socket.local_endpoint(), socket.remote_endpoint()), (None, None));
        assert_eq!(pair.a.poll_at(), poll_at);
        assert_eq!(round(&mut pair), []);
        pair.close();
    });
}

/// After abort, the socket owes one RST: the Stack's next deadline is now, and the next
/// round sends it.
#[zhida_test_support::pinned_test]
fn abort_schedules_its_rst() {
    with_eal(|eal| {
        let mut pair = pair(eal, ["idc", "idd"]);
        let ([client, server], _) = establish(&mut pair, Side::A);
        assert_eq!(pair.a.poll_at(), None);
        pair.a.abort(&client);
        assert_eq!(pair.a.tcp(&client).state(), State::Closed);
        assert_eq!(pair.a.poll_at(), Some(pair.now()));
        assert_eq!(
            round(&mut pair),
            [(Dir::AToB, TcpControl::Rst, CLIENT_PORT, SERVER_PORT)]
        );
        assert_eq!(pair.b.tcp(&server).state(), State::Closed);
        assert_eq!(pair.a.poll_at(), None);
        pair.close();
    });
}

/// Abort, then connect the same socket on the same tuple, with no poll between. The
/// reconnect replaces the RST the abort owed, so the next round sends the new SYN, which the
/// peer, still Established on the tuple, answers with an ACK.
#[zhida_test_support::pinned_test]
fn abort_then_reconnect_same_tuple_before_any_poll() {
    with_eal(|eal| {
        let mut pair = pair(eal, ["ide", "idf"]);
        let ([client, _server], _) = establish(&mut pair, Side::A);
        pair.a.abort(&client);
        pair.a.connect(&client, server(), CLIENT_PORT).unwrap();
        assert_eq!(pair.a.tcp(&client).state(), State::SynSent);
        assert_eq!(
            round(&mut pair),
            [
                (Dir::AToB, TcpControl::Syn, CLIENT_PORT, SERVER_PORT),
                (Dir::BToA, TcpControl::None, SERVER_PORT, CLIENT_PORT),
            ]
        );
        assert_eq!(pair.a.tcp(&client).state(), State::SynSent);
        pair.close();
    });
}

/// Close on a listener removes it from the Stack at once: a SYN in the next round gets an
/// RST.
#[zhida_test_support::pinned_test]
fn closed_listener_takes_no_syn() {
    with_eal(|eal| {
        let mut pair = pair(eal, ["idg", "idh"]);
        let listener = pair.b.add_tcp(tcp_socket_sized(4096)).unwrap();
        pair.b.listen(&listener, SERVER_PORT).unwrap();
        pair.b.close(&listener);
        assert_eq!(pair.b.tcp(&listener).state(), State::Closed);
        let client = pair.a.add_tcp(tcp_socket_sized(4096)).unwrap();
        pair.a.connect(&client, server(), CLIENT_PORT).unwrap();
        let mut seen = Vec::new();
        for _ in 0..4 {
            seen.extend(round(&mut pair));
        }
        assert_eq!(
            seen,
            [
                (Dir::AToB, TcpControl::Syn, CLIENT_PORT, SERVER_PORT),
                (Dir::BToA, TcpControl::Rst, SERVER_PORT, CLIENT_PORT),
            ]
        );
        assert_eq!(pair.a.tcp(&client).state(), State::Closed);
        assert_eq!(pair.b.tcp(&listener).state(), State::Closed);
        pair.close();
    });
}
