/*! Network interface logic.

The `iface` module deals with the *network interfaces*. It filters incoming frames,
provides lookup and caching of hardware addresses, and handles management packets.
*/

mod header_template;
mod interface;
mod path_mtu;
mod owned_ipv4;
mod neighbor;
mod socket_meta;
mod tcp_set;
mod timer_wheel;

mod udp_set;
mod drop_queue;

mod packet;

/// A bind: a local port on one local address (`Some`), or on every address the interface owns
/// (`None`, all-IP). TCP listeners and UDP sockets are keyed by it; a lookup probes the exact
/// key, then the all-IP one.
pub(crate) type BindKey = (Option<crate::wire::Ipv4Address>, u16);

/// A slot index by key. Sized at construction for twice the slots it can hold, so removals
/// never force it to grow. Keys are packed into integers (`bind_word`, `tuple_word`): an
/// integer hashes in one multiply and compares in one instruction, so a probe inlines.
pub(crate) type SlotMap<K> = std::collections::HashMap<K, u32, rustc_hash::FxBuildHasher>;

/// `BindKey` as one integer; `None` and every `Some` address stay distinct.
#[inline]
pub(crate) fn bind_word((addr, port): BindKey) -> u64 {
    let addr = addr.map_or(0, |addr| (1 << 32) | u64::from(u32::from(addr)));
    (addr << 16) | u64::from(port)
}

/// A TCP tuple, local then remote endpoint, as one integer.
#[inline]
pub(crate) fn tuple_word(local: crate::wire::IpEndpoint, remote: crate::wire::IpEndpoint) -> u128 {
    let endpoint = |endpoint: crate::wire::IpEndpoint| {
        let crate::wire::IpAddress::Ipv4(addr) = endpoint.addr;
        (u64::from(endpoint.port) << 32) | u64::from(u32::from(addr))
    };
    (u128::from(endpoint(local)) << 48) | u128::from(endpoint(remote))
}

pub(crate) fn slot_map<K>(slot_count: usize) -> SlotMap<K> {
    assert!(u32::try_from(slot_count).is_ok(), "slot count exceeds u32");
    SlotMap::with_capacity_and_hasher(2 * slot_count, rustc_hash::FxBuildHasher)
}

pub use self::interface::{
    Config, Interface, InterfaceInner as Context, NoRoute, PollIngressSingleResult, PollResult,
};

pub use self::tcp_set::{ReadySide, SocketHandle, TcpSet};
pub use self::drop_queue::DropQueue;
pub use self::udp_set::{
    UdpAddError, UdpConfig, UdpConnectError, UdpDatagram, UdpHandle,
    UdpSendError, UdpSet, UdpSocket, UdpSocketStats, UdpTarget,
};

pub use self::owned_ipv4::OwnedIpv4;
pub use self::path_mtu::{PathMtu, PathMtus, MIN_IP_MTU};
