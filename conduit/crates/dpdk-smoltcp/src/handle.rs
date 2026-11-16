use std::fmt;
use std::mem::ManuallyDrop;
use std::ptr;

use smoltcp::iface::{DropQueue, SocketHandle};

/// Names one TCP socket in the [`Stack`](crate::Stack) that made it.
///
/// Pass `&handle` to the Stack's methods; using it on another Stack panics with
/// "TCP handle belongs to another Stack". It is not `Clone` and not `Send`.
/// [`Stack::remove_tcp`](crate::Stack::remove_tcp) consumes it and removes the socket at
/// once. Dropping it closes the socket like Linux `close()`: unread in-order data causes a
/// reset, otherwise a FIN; see [`Stack::close`](crate::Stack::close) for the full table.
/// A handle may outlive its Stack; dropping it afterwards does nothing.
pub struct TcpHandle {
    pub(crate) raw: SocketHandle,
    pub(crate) queue: DropQueue,
}

impl TcpHandle {
    pub(crate) fn into_raw(self) -> SocketHandle {
        let handle = ManuallyDrop::new(self);
        let raw = handle.raw;
        // SAFETY: ManuallyDrop prevents the handle's Drop. Read and drop the
        // queue once to release its Rc; raw is Copy and has no destructor.
        unsafe { drop(ptr::read(&handle.queue)) };
        raw
    }
}

impl fmt::Debug for TcpHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.raw, f)
    }
}

/// Displays the socket slot index.
impl fmt::Display for TcpHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.raw, f)
    }
}

impl Drop for TcpHandle {
    fn drop(&mut self) {
        self.queue.push_tcp(self.raw);
    }
}

/// Names one UDP socket (one bound local port) in the [`Stack`](crate::Stack) that made it.
///
/// Pass `&handle` to the Stack's methods; using it on another Stack panics with
/// "UDP handle belongs to another Stack". It is not `Clone` and not `Send`.
/// [`Stack::remove_udp`](crate::Stack::remove_udp) consumes it and removes the socket at
/// once. Dropping it does the same at the start of the Stack's next `turn`, `receive`,
/// `send`, `add_tcp` or `add_udp`, freeing the port binding. Datagrams the application
/// holds stay valid and keep their packet buffers until dropped.
pub struct UdpHandle {
    pub(crate) raw: smoltcp::iface::UdpHandle,
    pub(crate) queue: DropQueue,
}

impl UdpHandle {
    pub(crate) fn into_raw(self) -> smoltcp::iface::UdpHandle {
        let handle = ManuallyDrop::new(self);
        let raw = handle.raw;
        // SAFETY: ManuallyDrop prevents the handle's Drop. Read and drop the
        // queue once to release its Rc; raw is Copy and has no destructor.
        unsafe { drop(ptr::read(&handle.queue)) };
        raw
    }
}

impl fmt::Debug for UdpHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.raw, f)
    }
}

impl Drop for UdpHandle {
    fn drop(&mut self) {
        self.queue.push_udp(self.raw);
    }
}
