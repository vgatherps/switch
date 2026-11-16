use alloc::{rc::Rc, vec::Vec};
use core::cell::RefCell;

use super::SocketHandle;
use super::UdpHandle;

#[derive(Clone, Debug)]
pub struct DropQueue(Rc<RefCell<Dropped>>);

#[derive(Debug, Default)]
struct Dropped {
    tcp: Vec<SocketHandle>,
    udp: Vec<UdpHandle>,
}

impl DropQueue {
    /// Capacities are the maximum number of live TCP and UDP handles, so pushes never allocate.
    pub fn with_capacity(tcp: usize, udp: usize) -> Self {
        Self(Rc::new(RefCell::new(Dropped {
            tcp: Vec::with_capacity(tcp),
            udp: Vec::with_capacity(udp),
        })))
    }

    pub fn push_tcp(&self, handle: SocketHandle) {
        let mut dropped = self.0.borrow_mut();
        debug_assert!(dropped.tcp.len() < dropped.tcp.capacity());
        dropped.tcp.push(handle);
    }

    pub fn push_udp(&self, handle: UdpHandle) {
        let mut dropped = self.0.borrow_mut();
        debug_assert!(dropped.udp.len() < dropped.udp.capacity());
        dropped.udp.push(handle);
    }

    /// True when no dropped handles are queued.
    #[inline]
    pub fn is_empty(&self) -> bool {
        let dropped = self.0.borrow();
        dropped.tcp.is_empty() && dropped.udp.is_empty()
    }

    /// Append queued handles to the supplied lists, retaining the queue's capacity.
    pub fn drain_into(
        &self,
        tcp: &mut Vec<SocketHandle>,
        udp: &mut Vec<UdpHandle>,
    ) {
        let mut dropped = self.0.borrow_mut();
        tcp.append(&mut dropped.tcp);
        udp.append(&mut dropped.udp);
    }

    /// True when `self` and `other` are the same queue.
    pub fn same(&self, other: &Self) -> bool {
        Rc::ptr_eq(&self.0, &other.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::iface::{TcpSet, UdpConfig, UdpSet};
    use crate::socket::tcp;

    fn handles() -> ([SocketHandle; 3], [UdpHandle; 3]) {
        let mut tcp = TcpSet::new(64);
        let tcp = core::array::from_fn(|_| {
            tcp.add(tcp::Socket::test_socket(0, 0))
        });
        let iface = crate::tests::setup(crate::phy::Medium::Ethernet).0;
        let mut udp = UdpSet::new(3, crate::tests::tx_test_source(1472));
        let udp = core::array::from_fn(|index| {
            udp.add(&iface, UdpConfig {
                port: u16::try_from(1000 + index).unwrap(),
                local_addr: None,
                rx_queue: 1,
                tx_checksum: true,
                broadcast: false,
            }).unwrap()
        });
        (tcp, udp)
    }

    #[test]
    fn push_and_drain_append_in_order() {
        let (tcp_handles, udp_handles) = handles();
        let queue = DropQueue::with_capacity(2, 2);
        assert!(queue.is_empty());
        queue.push_tcp(tcp_handles[1]);
        assert!(!queue.is_empty());
        queue.push_udp(udp_handles[1]);
        queue.push_tcp(tcp_handles[2]);
        queue.push_udp(udp_handles[2]);
        let mut tcp = Vec::from([tcp_handles[0]]);
        let mut udp = Vec::from([udp_handles[0]]);
        queue.drain_into(&mut tcp, &mut udp);
        assert_eq!(tcp, tcp_handles);
        assert_eq!(udp, udp_handles);
        assert!(queue.is_empty());
        queue.drain_into(&mut tcp, &mut udp);
        assert_eq!(tcp, tcp_handles);
        assert_eq!(udp, udp_handles);
        queue.push_udp(udp_handles[0]);
        assert!(!queue.is_empty());
    }

    #[test]
    fn same_identifies_shared_queue() {
        let (tcp_handles, udp_handles) = handles();
        let queue = DropQueue::with_capacity(1, 1);
        let clone = queue.clone();
        let other = DropQueue::with_capacity(1, 1);
        assert!(queue.same(&queue));
        assert!(queue.same(&clone));
        assert!(clone.same(&queue));
        assert!(!queue.same(&other));
        clone.push_tcp(tcp_handles[0]);
        clone.push_udp(udp_handles[0]);
        assert!(!queue.is_empty());
        let mut tcp = Vec::new();
        let mut udp = Vec::new();
        queue.drain_into(&mut tcp, &mut udp);
        assert_eq!(tcp, [tcp_handles[0]]);
        assert_eq!(udp, [udp_handles[0]]);
        assert!(clone.is_empty());
        assert!(other.is_empty());
    }

}
