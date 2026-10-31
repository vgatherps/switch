use alloc::{collections::VecDeque, rc::Rc, vec::Vec};
use core::ops::Range;

use super::Interface;
use super::{bind_word, slot_map, SlotMap};
use crate::phy::{HeldFrames, RxBuf, RxShared, TxSource};
use crate::wire::{IpAddress, IpEndpoint, Ipv4Address, Ipv4AddressExt};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct UdpHandle {
    slot: usize,
    generation: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UdpConfig {
    pub port: u16,
    /// The one local address the socket receives on and sends from, which the interface must
    /// own; `None` binds every owned address (all-IP): it receives what no socket bound to the
    /// datagram's destination takes, and broadcasts, and sends from the interface's address.
    pub local_addr: Option<Ipv4Address>,
    pub rx_queue: usize,
    /// Compute the UDP checksum in software when true, or write 0 when false.
    /// With a device whose `checksum.udp` capability has no TX, smoltcp always
    /// writes 0; whether the checksum is filled in later depends on the device.
    pub tx_checksum: bool,
    pub broadcast: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UdpAddError {
    Full,
    /// A socket is already bound to this local address (or all-IP) and port.
    PortInUse,
    InvalidConfig(&'static str),
    /// `local_addr` is not a unicast address the interface owns.
    Unaddressable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UdpConnectError {
    Invalid,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UdpSendError {
    NoDestination,
    PeerMismatch,
    Unaddressable,
    TooLarge,
    NoRoute,
    NeighborPending,
    WouldBlock,
}

/// A validated UDP send: socket, destination and payload length.
/// `Interface::udp_target` makes one; pass it to `udp_send` of the same `Interface`, with the
/// same `UdpSet`, before the socket or the interface's addresses change.
#[derive(Debug, PartialEq, Eq)]
pub struct UdpTarget {
    pub(crate) handle: UdpHandle,
    pub(crate) to: IpEndpoint,
    pub(crate) dst_addr: Ipv4Address,
    pub(crate) len: usize,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct UdpSocketStats {
    pub rx_dropped_full: u64,
}

#[derive(Debug)]
pub struct UdpDatagram {
    frame: RxBuf,
    src: IpEndpoint,
    dst_addr: Ipv4Address,
    shared: Rc<RxShared>,
}

impl UdpDatagram {
    pub fn rx_timestamp(&self) -> u64 {
        self.frame.rx_timestamp()
    }

    pub fn payload(&self) -> &[u8] {
        self.frame.as_slice()
    }

    pub fn payload_mut(&mut self) -> &mut [u8] {
        self.frame.as_mut_slice()
    }

    pub fn src(&self) -> IpEndpoint {
        self.src
    }

    pub fn dst_addr(&self) -> Ipv4Address {
        self.dst_addr
    }
}

impl Drop for UdpDatagram {
    fn drop(&mut self) {
        self.shared.release();
    }
}

#[derive(Debug)]
pub struct UdpSocket {
    config: UdpConfig,
    peer: Option<IpEndpoint>,
    queue: VecDeque<UdpDatagram>,
    shared: Rc<RxShared>,
    pub(crate) stats: UdpSocketStats,
    pub(crate) template: Option<super::header_template::UdpTemplate>,
}

impl UdpSocket {
    pub fn config(&self) -> &UdpConfig {
        &self.config
    }

    pub fn connect(&mut self, peer: IpEndpoint) -> Result<(), UdpConnectError> {
        if !matches!(peer.addr, IpAddress::Ipv4(_)) || !peer.addr.is_unicast() || peer.port == 0 {
            return Err(UdpConnectError::Invalid);
        }
        self.peer = Some(peer);
        Ok(())
    }

    pub fn peer(&self) -> Option<IpEndpoint> {
        self.peer
    }

    pub fn recv(&mut self) -> Option<UdpDatagram> {
        self.queue.pop_front()
    }

    pub fn rx_queued(&self) -> usize {
        self.queue.len()
    }

    pub fn stats(&self) -> UdpSocketStats {
        self.stats
    }

    pub(crate) fn at_limit(&self) -> bool {
        self.shared.held.get() == self.config.rx_queue
    }
}

#[derive(Debug)]
struct Slot {
    socket: Option<UdpSocket>,
    generation: u64,
    /// Readiness token armed for the receive queue.
    rx_token: Option<u32>,
}

#[derive(Debug)]
pub(crate) struct PendingUdp {
    pub(crate) src: IpEndpoint,
    pub(crate) dst_addr: Ipv4Address,
    pub(crate) payload: Range<usize>,
}

/// Readiness tokens: [`arm_rx`](Self::arm_rx) stores a caller's `u32` for a socket's receive
/// queue. A queued datagram fires it: the token moves to a list that
/// [`pop_fired`](Self::pop_fired) drains, and is disarmed. The list holds one per slot without
/// growing while no token is re-armed before it was popped.
#[derive(Debug)]
pub struct UdpSet {
    slots: Vec<Slot>,
    binds: SlotMap<u64>,
    /// Sockets bound to one address. While none is, `lookup` probes only the all-IP key.
    bound: usize,
    pub(crate) pending_rx_attach: Option<(usize, PendingUdp)>,
    /// Frames `Interface::udp_send` copies payloads into.
    pub(crate) source: TxSource,
    fired: Vec<u32>,
}

impl UdpSet {
    /// A set of up to `max_sockets` sockets whose copied sends take frames from `source`.
    pub fn new(max_sockets: usize, source: TxSource) -> Self {
        Self {
            slots: (0..max_sockets)
                .map(|_| Slot {
                    socket: None,
                    generation: 0,
                    rx_token: None,
                })
                .collect(),
            binds: slot_map(max_sockets),
            bound: 0,
            pending_rx_attach: None,
            source,
            fired: Vec::with_capacity(max_sockets),
        }
    }

    /// Add a socket bound to `config`'s local address and port on `iface`. Errors, in this
    /// order, change nothing: `InvalidConfig` (port or receive queue zero), `Unaddressable` (a
    /// local address `iface` does not own), `PortInUse` (the same address, or all-IP, and port
    /// already bound; one port can carry one all-IP socket and one per address), `Full`.
    pub fn add(&mut self, iface: &Interface, config: UdpConfig) -> Result<UdpHandle, UdpAddError> {
        if config.port == 0 {
            return Err(UdpAddError::InvalidConfig("UDP port is zero"));
        }
        if config.rx_queue == 0 {
            return Err(UdpAddError::InvalidConfig("UDP receive queue is zero"));
        }
        if let Some(addr) = config.local_addr
            && (!addr.x_is_unicast() || !iface.inner.has_ip_addr(addr))
        {
            return Err(UdpAddError::Unaddressable);
        }
        let key = (config.local_addr, config.port);
        if self.binds.contains_key(&bind_word(key)) {
            return Err(UdpAddError::PortInUse);
        }
        let slot = self
            .slots
            .iter()
            .position(|slot| slot.socket.is_none())
            .ok_or(UdpAddError::Full)?;
        self.slots[slot].socket = Some(UdpSocket {
            config,
            peer: None,
            queue: VecDeque::with_capacity(config.rx_queue),
            shared: RxShared::new(),
            stats: UdpSocketStats::default(),
            template: None,
        });
        self.binds.insert(bind_word(key), slot as u32);
        self.bound += usize::from(config.local_addr.is_some());
        Ok(UdpHandle {
            slot,
            generation: self.slots[slot].generation,
        })
    }

    pub fn get(&self, handle: UdpHandle) -> &UdpSocket {
        let slot = &self.slots[handle.slot];
        assert_eq!(slot.generation, handle.generation, "stale UDP handle");
        slot.socket.as_ref().expect("removed UDP handle")
    }

    pub fn get_mut(&mut self, handle: UdpHandle) -> &mut UdpSocket {
        let slot = &mut self.slots[handle.slot];
        assert_eq!(slot.generation, handle.generation, "stale UDP handle");
        slot.socket.as_mut().expect("removed UDP handle")
    }

    /// Arm `token` for the socket's receive queue, replacing a token armed there. Removing
    /// the socket disarms it.
    ///
    /// Arming runs on every park: the caller checks `handle` once (as `get` does) and keeps it
    /// while the socket lives; only debug builds check it here. A stale handle misdirects the
    /// token to whatever socket the slot holds or holds next, where it fires spuriously.
    /// Panics if the handle's slot index is out of range.
    pub fn arm_rx(&mut self, handle: UdpHandle, token: u32) {
        let slot = &mut self.slots[handle.slot];
        debug_assert!(slot.generation == handle.generation && slot.socket.is_some(), "stale UDP handle");
        slot.rx_token = Some(token);
    }

    /// Disarm the socket's receive queue. Returns whether a token was armed; `false` when none
    /// was, or it fired and waits in the fired list until popped. Handle checks as in `arm_rx`.
    pub fn disarm_rx(&mut self, handle: UdpHandle) -> bool {
        let slot = &mut self.slots[handle.slot];
        debug_assert!(slot.generation == handle.generation && slot.socket.is_some(), "stale UDP handle");
        slot.rx_token.take().is_some()
    }

    /// One fired token, in no particular order; `None` once the list is empty.
    #[inline]
    pub fn pop_fired(&mut self) -> Option<u32> {
        self.fired.pop()
    }

    pub fn remove(&mut self, handle: UdpHandle) -> HeldFrames {
        let socket = self.get(handle);
        let key = (socket.config.local_addr, socket.config.port);
        let held = HeldFrames::new(socket.shared.clone());
        let slot = &mut self.slots[handle.slot];
        slot.generation = slot
            .generation
            .checked_add(1)
            .expect("UDP generation overflow");
        slot.socket = None;
        slot.rx_token = None;
        self.binds.remove(&bind_word(key));
        self.bound -= usize::from(key.0.is_some());
        held
    }

    /// The socket a datagram to `dst_addr:port` belongs to: one bound to `dst_addr`, else the
    /// all-IP one. A broadcast belongs only to the all-IP socket. Without address-bound sockets
    /// the exact probe, which could only miss, is skipped.
    #[inline]
    pub(crate) fn lookup(&self, dst_addr: Ipv4Address, port: u16, broadcast: bool) -> Option<usize> {
        if !broadcast
            && self.bound != 0
            && let Some(&slot) = self.binds.get(&bind_word((Some(dst_addr), port)))
        {
            return Some(slot as usize);
        }
        self.binds.get(&bind_word((None, port))).map(|&slot| slot as usize)
    }

    pub(crate) fn socket_at_mut(&mut self, slot: usize) -> &mut UdpSocket {
        self.slots[slot]
            .socket
            .as_mut()
            .expect("UDP socket is missing")
    }

    pub(crate) fn commit_rx(&mut self, slot: usize, mut frame: RxBuf, pending: PendingUdp) {
        let entry = &mut self.slots[slot];
        if let Some(token) = entry.rx_token {
            entry.rx_token = None;
            fire(&mut self.fired, token);
        }
        let socket = entry.socket.as_mut().expect("UDP socket is missing");
        assert!(!socket.at_limit(), "UDP receive credit is exhausted");
        frame.narrow(pending.payload);
        socket.shared.hold();
        socket.queue.push_back(UdpDatagram {
            frame,
            src: pending.src,
            dst_addr: pending.dst_addr,
            shared: socket.shared.clone(),
        });
    }
}

/// Out of line, so a datagram for an unarmed socket pays one load and branch.
#[inline(never)]
fn fire(fired: &mut Vec<u32>, token: u32) {
    fired.push(token);
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::cell::Cell;
    use crate::wire::IpAddress;
    use std::panic::{AssertUnwindSafe, catch_unwind};

    /// The interface's own address (crate::tests::setup).
    const LOCAL: Ipv4Address = Ipv4Address::new(192, 168, 1, 1);

    fn iface() -> Interface {
        crate::tests::setup(crate::phy::Medium::Ethernet).0
    }

    fn config(port: u16) -> UdpConfig {
        UdpConfig {
            port,
            local_addr: None,
            rx_queue: 2,
            tx_checksum: true,
            broadcast: false,
        }
    }

    #[test]
    fn duplicate_port_refused_without_change() {
        let mut set = UdpSet::new(2, crate::tests::tx_test_source(1472));
        let handle = set.add(&iface(), config(1234)).unwrap();
        let peer = IpEndpoint::new(IpAddress::v4(192, 168, 1, 2), 4321);
        set.get_mut(handle).connect(peer).unwrap();
        assert_eq!(set.add(&iface(), config(1234)), Err(UdpAddError::PortInUse));
        assert_eq!(set.get(handle).config(), &config(1234));
        assert_eq!(set.get(handle).peer(), Some(peer));
        assert_eq!(set.lookup(LOCAL, 1234, false), Some(handle.slot));
        assert!(set.add(&iface(), config(1235)).is_ok());
    }

    #[test]
    fn stale_handle_panics() {
        let mut set = UdpSet::new(1, crate::tests::tx_test_source(1472));
        let old = set.add(&iface(), config(1234)).unwrap();
        set.remove(old);
        assert!(catch_unwind(AssertUnwindSafe(|| set.get(old))).is_err());
        assert!(
            catch_unwind(AssertUnwindSafe(|| {
                set.get_mut(old);
            }))
            .is_err()
        );
        assert!(catch_unwind(AssertUnwindSafe(|| set.remove(old))).is_err());
        let new = set.add(&iface(), config(1234)).unwrap();
        assert_ne!(old, new);
        assert!(catch_unwind(AssertUnwindSafe(|| set.get(old))).is_err());
        assert!(
            catch_unwind(AssertUnwindSafe(|| {
                set.get_mut(old);
            }))
            .is_err()
        );
        assert!(catch_unwind(AssertUnwindSafe(|| set.remove(old))).is_err());
        assert_eq!(set.get(new).config().port, 1234);
    }

    #[test]
    fn rx_token_fires_once_on_a_queued_datagram_and_remove_disarms() {
        let mut set = UdpSet::new(2, crate::tests::tx_test_source(1472));
        let handle = set.add(&iface(), config(1234)).unwrap();
        let other = set.add(&iface(), config(1235)).unwrap();
        let releases = Rc::new(Cell::new(0));
        let enqueue = |set: &mut UdpSet, slot| {
            set.commit_rx(
                slot,
                RxBuf::counted_frame(alloc::vec![0, 1, 0], 0, releases.clone()),
                PendingUdp {
                    src: IpEndpoint::new(IpAddress::v4(192, 168, 1, 2), 4321),
                    dst_addr: Ipv4Address::new(192, 168, 1, 1),
                    payload: 1..2,
                },
            );
        };
        set.arm_rx(handle, 7);
        set.arm_rx(other, 8);
        enqueue(&mut set, handle.slot);
        assert_eq!(set.pop_fired(), Some(7));
        assert_eq!(set.pop_fired(), None);
        enqueue(&mut set, handle.slot);
        assert_eq!(set.pop_fired(), None, "a fired token is disarmed");
        assert!(!set.disarm_rx(handle));
        drop(set.get_mut(handle).recv());
        drop(set.get_mut(handle).recv());
        assert_eq!(set.pop_fired(), None, "receiving and releasing datagrams fire nothing");
        set.arm_rx(handle, 9);
        assert!(set.disarm_rx(handle));
        enqueue(&mut set, handle.slot);
        assert_eq!(set.pop_fired(), None);
        drop(set.get_mut(handle).recv());
        set.remove(other);
        let reused = set.add(&iface(), config(1236)).unwrap();
        assert_eq!(reused.slot, other.slot);
        enqueue(&mut set, reused.slot);
        assert_eq!(set.pop_fired(), None, "remove disarmed the old socket's token");
        drop(set.get_mut(reused).recv());
    }

    #[test]
    fn receive_queue_keeps_order_across_wraps() {
        let mut set = UdpSet::new(1, crate::tests::tx_test_source(1472));
        let handle = set
            .add(&iface(), UdpConfig {
                rx_queue: 3,
                ..config(1234)
            })
            .unwrap();
        let releases = Rc::new(Cell::new(0));
        let enqueue = |set: &mut UdpSet, byte| {
            set.commit_rx(
                handle.slot,
                RxBuf::counted_frame(alloc::vec![0, byte, 0], 0, releases.clone()),
                PendingUdp {
                    src: IpEndpoint::new(IpAddress::v4(192, 168, 1, 2), 4321),
                    dst_addr: Ipv4Address::new(192, 168, 1, 1),
                    payload: 1..2,
                },
            );
        };
        for cycle in 0..4 {
            let first = cycle * 5;
            for offset in 0..3 {
                enqueue(&mut set, first + offset);
                assert_eq!(set.get(handle).rx_queued(), usize::from(offset) + 1);
            }
            assert!(set.get(handle).at_limit());
            for offset in 0..2 {
                let datagram = set.get_mut(handle).recv().unwrap();
                assert_eq!(datagram.payload(), &[first + offset]);
                drop(datagram);
                assert_eq!(set.get(handle).rx_queued(), 2 - usize::from(offset));
            }
            for offset in 3..5 {
                enqueue(&mut set, first + offset);
                assert_eq!(set.get(handle).rx_queued(), usize::from(offset) - 1);
            }
            assert!(set.get(handle).at_limit());
            for offset in 2..5 {
                let datagram = set.get_mut(handle).recv().unwrap();
                assert_eq!(datagram.payload(), &[first + offset]);
                drop(datagram);
                assert_eq!(set.get(handle).rx_queued(), 4 - usize::from(offset));
            }
            assert!(set.get_mut(handle).recv().is_none());
            assert_eq!(set.get(handle).shared.held.get(), 0);
        }
        assert_eq!(releases.get(), 20);
        assert_eq!(set.get(handle).stats().rx_dropped_full, 0);
    }

    #[test]
    fn remove_frees_queued_held_stays_valid() {
        let mut set = UdpSet::new(1, crate::tests::tx_test_source(1472));
        let handle = set.add(&iface(), config(1234)).unwrap();
        let releases = Rc::new(Cell::new(0));
        for byte in [1, 2] {
            set.commit_rx(
                handle.slot,
                RxBuf::counted_frame(alloc::vec![0, byte, 0], 0, releases.clone()),
                PendingUdp {
                    src: IpEndpoint::new(IpAddress::v4(192, 168, 1, 2), 4321),
                    dst_addr: Ipv4Address::new(192, 168, 1, 1),
                    payload: 1..2,
                },
            );
        }
        let mut held = set.get_mut(handle).recv().unwrap();
        set.remove(handle);
        assert_eq!(set.lookup(LOCAL, 1234, false), None);
        assert_eq!(releases.get(), 1);
        assert_eq!(held.payload(), &[1]);
        held.payload_mut()[0] = 3;
        assert_eq!(held.payload(), &[3]);
        let replacement = set.add(&iface(), config(1234)).unwrap();
        assert_eq!(set.get(replacement).shared.held.get(), 0);
        drop(held);
        assert_eq!(releases.get(), 2);
        assert_eq!(set.get(replacement).shared.held.get(), 0);
    }

    #[test]
    fn remove_returns_held_count() {
        let mut set = UdpSet::new(1, crate::tests::tx_test_source(1472));
        let handle = set
            .add(&iface(), UdpConfig {
                rx_queue: 3,
                ..config(1234)
            })
            .unwrap();
        let releases = Rc::new(Cell::new(0));
        for byte in [1, 2, 3] {
            set.commit_rx(
                handle.slot,
                RxBuf::counted_frame(alloc::vec![byte], 0, releases.clone()),
                PendingUdp {
                    src: IpEndpoint::new(IpAddress::v4(192, 168, 1, 2), 4321),
                    dst_addr: Ipv4Address::new(192, 168, 1, 1),
                    payload: 0..1,
                },
            );
        }
        let first = set.get_mut(handle).recv().unwrap();
        let second = set.get_mut(handle).recv().unwrap();
        let held = set.remove(handle);
        assert_eq!(held.count(), 2);
        assert_eq!(releases.get(), 1);
        drop(first);
        assert_eq!(held.count(), 1);
        assert_eq!(releases.get(), 2);
        drop(second);
        assert_eq!(held.count(), 0);
        assert_eq!(releases.get(), 3);
    }

    #[test]
    fn removed_datagram_drop_clears_count_without_wake() {
        let mut set = UdpSet::new(1, crate::tests::tx_test_source(1472));
        let handle = set.add(&iface(), config(1234)).unwrap();
        let shared = set.get(handle).shared.clone();
        let releases = Rc::new(Cell::new(0));
        for byte in [1, 2] {
            set.commit_rx(
                handle.slot,
                RxBuf::counted_frame(alloc::vec![byte], 0, releases.clone()),
                PendingUdp {
                    src: IpEndpoint::new(IpAddress::v4(192, 168, 1, 2), 4321),
                    dst_addr: Ipv4Address::new(192, 168, 1, 1),
                    payload: 0..1,
                },
            );
            assert!(shared.wake.borrow().is_none());
            assert_eq!(shared.wake_at_held.get(), usize::MAX);
            assert!(!shared.wake_queued.get());
        }
        let datagram = set.get_mut(handle).recv().unwrap();
        let held = set.remove(handle);
        assert_eq!(held.count(), 1);
        assert_eq!(releases.get(), 1);
        assert!(shared.wake.borrow().is_none());
        assert_eq!(shared.wake_at_held.get(), usize::MAX);
        assert!(!shared.wake_queued.get());
        drop(datagram);
        assert_eq!(held.count(), 0);
        assert_eq!(releases.get(), 2);
        assert!(shared.wake.borrow().is_none());
        assert_eq!(shared.wake_at_held.get(), usize::MAX);
        assert!(!shared.wake_queued.get());
    }

    #[test]
    fn at_limit_counts_drained_datagrams_until_drop() {
        let mut set = UdpSet::new(1, crate::tests::tx_test_source(1472));
        let handle = set.add(&iface(), config(1234)).unwrap();
        let releases = Rc::new(Cell::new(0));
        assert!(!set.get(handle).at_limit());
        let mut datagrams = Vec::new();
        for byte in [1, 2] {
            set.commit_rx(
                handle.slot,
                RxBuf::counted_frame(alloc::vec![byte], 0, releases.clone()),
                PendingUdp {
                    src: IpEndpoint::new(IpAddress::v4(192, 168, 1, 2), 4321),
                    dst_addr: Ipv4Address::new(192, 168, 1, 1),
                    payload: 0..1,
                },
            );
            assert_eq!(set.get(handle).at_limit(), byte == 2);
            datagrams.push(set.get_mut(handle).recv().unwrap());
            assert_eq!(set.get(handle).rx_queued(), 0);
            assert_eq!(set.get(handle).at_limit(), byte == 2);
        }
        drop(datagrams.pop().unwrap());
        assert!(!set.get(handle).at_limit());
        assert_eq!(set.get(handle).shared.held.get(), 1);
        assert_eq!(releases.get(), 1);
        drop(datagrams);
        assert_eq!(set.get(handle).shared.held.get(), 0);
        assert_eq!(releases.get(), 2);
    }

    #[test]
    fn invalid_config_refused() {
        let mut set = UdpSet::new(1, crate::tests::tx_test_source(1472));
        assert!(matches!(
            set.add(&iface(), config(0)),
            Err(UdpAddError::InvalidConfig(_))
        ));
        assert!(matches!(
            set.add(&iface(), UdpConfig {
                rx_queue: 0,
                ..config(1234)
            }),
            Err(UdpAddError::InvalidConfig(_))
        ));
        let handle = set.add(&iface(), config(1234)).unwrap();
        let peer = IpEndpoint::new(IpAddress::v4(192, 168, 1, 2), 4321);
        set.get_mut(handle).connect(peer).unwrap();
        for invalid in [
            IpEndpoint::new(IpAddress::v4(0, 0, 0, 0), 4321),
            IpEndpoint::new(IpAddress::v4(224, 0, 0, 1), 4321),
            IpEndpoint::new(IpAddress::v4(255, 255, 255, 255), 4321),
            IpEndpoint::new(peer.addr, 0),
        ] {
            assert_eq!(
                set.get_mut(handle).connect(invalid),
                Err(UdpConnectError::Invalid)
            );
            assert_eq!(set.get(handle).peer(), Some(peer));
        }
    }

    #[test]
    fn full_set() {
        assert_eq!(UdpSet::new(0, crate::tests::tx_test_source(1472)).add(&iface(), config(1234)), Err(UdpAddError::Full));
        let mut set = UdpSet::new(1, crate::tests::tx_test_source(1472));
        let handle = set.add(&iface(), config(1234)).unwrap();
        assert_eq!(set.add(&iface(), config(1235)), Err(UdpAddError::Full));
        assert_eq!(set.lookup(LOCAL, 1235, false), None);
        assert_eq!(set.get(handle).config(), &config(1234));
        set.remove(handle);
        assert!(set.add(&iface(), config(1235)).is_ok());
        assert_eq!(set.slots.len(), 1);
    }

    fn bound(port: u16, addr: Ipv4Address) -> UdpConfig {
        UdpConfig {
            local_addr: Some(addr),
            ..config(port)
        }
    }

    /// The bound socket's key is probed first, whichever was added first; a broadcast and an
    /// address without a bound socket take the all-IP socket; a port without one takes nothing.
    #[test]
    fn lookup_prefers_the_bound_socket_in_either_order() {
        for bound_first in [true, false] {
            let mut set = UdpSet::new(2, crate::tests::tx_test_source(1472));
            let (bound_handle, all) = if bound_first {
                let b = set.add(&iface(), bound(1234, LOCAL)).unwrap();
                (b, set.add(&iface(), config(1234)).unwrap())
            } else {
                let a = set.add(&iface(), config(1234)).unwrap();
                (set.add(&iface(), bound(1234, LOCAL)).unwrap(), a)
            };
            assert_eq!(set.lookup(LOCAL, 1234, false), Some(bound_handle.slot));
            assert_eq!(set.lookup(LOCAL, 1234, true), Some(all.slot));
            assert_eq!(set.lookup(Ipv4Address::new(127, 0, 0, 1), 1234, false), Some(all.slot));
            assert_eq!(set.lookup(LOCAL, 1235, false), None);
            set.remove(all);
            assert_eq!(set.lookup(LOCAL, 1234, true), None);
            assert_eq!(set.lookup(Ipv4Address::new(127, 0, 0, 1), 1234, false), None);
        }
    }

    /// A local address the interface does not own, or one that is not unicast, is refused
    /// before the bind check and before taking a slot, and changes nothing: the next socket takes
    /// the same slot and generation. A bad port or queue comes first.
    #[test]
    fn unowned_local_address_refused_without_change() {
        let iface = iface();
        let mut set = UdpSet::new(2, crate::tests::tx_test_source(1472));
        let held = set.add(&iface, config(1234)).unwrap();
        for addr in [
            Ipv4Address::new(192, 168, 1, 9),
            Ipv4Address::UNSPECIFIED,
            Ipv4Address::BROADCAST,
            Ipv4Address::new(192, 168, 1, 255),
            Ipv4Address::new(224, 0, 0, 1),
        ] {
            assert_eq!(set.add(&iface, bound(1234, addr)), Err(UdpAddError::Unaddressable), "{addr}");
            assert_eq!(set.add(&iface, bound(1235, addr)), Err(UdpAddError::Unaddressable), "{addr}");
            assert!(matches!(set.add(&iface, bound(0, addr)), Err(UdpAddError::InvalidConfig(_))));
            assert_eq!(set.lookup(addr, 1234, false), Some(held.slot));
        }
        assert_eq!(set.slots[1].generation, 0);
        let next = set.add(&iface, bound(1234, LOCAL)).unwrap();
        assert_eq!((next.slot, next.generation), (1, 0));
        // an owned address from the owned set is accepted
        let mut iface = iface;
        let second = Ipv4Address::new(10, 9, 0, 1);
        iface.set_owned_ipv4(
            crate::iface::OwnedIpv4::new(
                &[crate::wire::Ipv4Cidr::new(second, 32)],
                crate::wire::Ipv4Cidr::new(LOCAL, 24),
                None,
            )
            .unwrap(),
        );
        set.remove(next);
        assert!(set.add(&iface, bound(1234, second)).is_ok());
    }

    /// One port carries one all-IP socket and one per address; an identical bind is refused.
    #[test]
    fn identical_bind_refused_others_on_the_port_allowed() {
        let iface = iface();
        let mut set = UdpSet::new(4, crate::tests::tx_test_source(1472));
        set.add(&iface, config(1234)).unwrap();
        set.add(&iface, bound(1234, LOCAL)).unwrap();
        set.add(&iface, bound(1234, Ipv4Address::new(127, 0, 0, 1))).unwrap();
        assert_eq!(set.add(&iface, config(1234)), Err(UdpAddError::PortInUse));
        assert_eq!(set.add(&iface, bound(1234, LOCAL)), Err(UdpAddError::PortInUse));
        assert!(set.add(&iface, bound(1235, LOCAL)).is_ok());
    }
}

