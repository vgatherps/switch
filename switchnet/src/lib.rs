//! Async sockets over one conduit `Stack`, receive side, for the `switch` runtime.
//!
//! The loop owner holds the [`Net`] and calls `net.turn(now)` (a `Stack::turn`,
//! then a wake of every task parked on a socket that became ready), then polls
//! the switch runtime. Tasks hold a [`NetHandle`], which opens sockets but
//! cannot turn the stack, and await owned received frames. One task may wait
//! per socket at a time (`recv` takes `&mut`). Futures here must be polled by
//! switch tasks: parking stores a `switch::StoredWaker`.
//! Everything is single-threaded (`Rc`), so no type here is `Send`.
//!
//! Received frames ([`TcpFrame`], [`Datagram`]) do not hold the Net: like
//! conduit's frames they stay valid after their socket, the Net and the Stack
//! are gone. Each held frame occupies its socket's receive budget until it drops.

use std::cell::RefCell;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};

pub use dpdk_smoltcp::smoltcp::socket::tcp;
pub use dpdk_smoltcp::smoltcp::socket::tcp::{ConnectError, ListenError, RecvError};
pub use dpdk_smoltcp::smoltcp::time::Instant;
pub use dpdk_smoltcp::smoltcp::wire::{IpEndpoint, IpListenEndpoint, Ipv4Address};
pub use dpdk_smoltcp::{AddTcpError, AddUdpError, Stack, StackError, Turn, UdpConfig};
use dpdk_smoltcp::{TcpHandle, UdpHandle};
use switch::StoredWaker;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NetConfig {
    /// Passed to `Stack::turn` each turn.
    pub ingress_budget: usize,
}

type Shared = Rc<RefCell<Inner>>;

enum Socket {
    Tcp(TcpHandle),
    Udp(UdpHandle),
}

#[derive(Clone, Copy)]
enum Interest {
    Connect,
    Accept,
    TcpRecv,
    UdpRecv,
}

struct Entry {
    socket: Option<Socket>,
    parked: Option<(StoredWaker, Interest)>,
    /// In `Inner::listed`.
    listed: bool,
}

struct Inner {
    stack: Stack,
    entries: Vec<Entry>,
    free: Vec<u32>,
    /// Entries parked since the last turn; may hold entries since unparked.
    listed: Vec<u32>,
    ingress_budget: usize,
}

impl Inner {
    fn insert(&mut self, socket: Socket) -> u32 {
        let index = self.free.pop().expect("more sockets than the Stack holds");
        self.entries[index as usize].socket = Some(socket);
        index
    }

    /// Clears the parked handle, removes the entry and returns its socket.
    fn remove(&mut self, index: u32) -> Option<Socket> {
        let entry = &mut self.entries[index as usize];
        entry.parked = None;
        let socket = entry.socket.take();
        if !entry.listed {
            self.free.push(index);
        }
        socket
    }

    /// The stack and the TCP handle of entry `index`.
    fn tcp(&mut self, index: u32) -> (&mut Stack, &TcpHandle) {
        match &self.entries[index as usize].socket {
            Some(Socket::Tcp(handle)) => (&mut self.stack, handle),
            _ => unreachable!("entry holds no TCP handle"),
        }
    }

    /// The stack and the UDP handle of entry `index`.
    fn udp(&mut self, index: u32) -> (&mut Stack, &UdpHandle) {
        match &self.entries[index as usize].socket {
            Some(Socket::Udp(handle)) => (&mut self.stack, handle),
            _ => unreachable!("entry holds no UDP handle"),
        }
    }

    fn park(&mut self, index: u32, interest: Interest, cx: &Context<'_>) {
        // SAFETY: `Net` is `!Send`, so the handle is stored and woken on the
        // executor thread. switch never frees task slabs, so a stale handle
        // is harmless; it is cleared anyway on every path that ends the
        // parked future: completion (`unpark`), drop (`unpark`) and socket
        // drop (`remove`).
        let waker = unsafe { StoredWaker::new(cx) };
        let entry = &mut self.entries[index as usize];
        entry.parked = Some((waker, interest));
        if !entry.listed {
            entry.listed = true;
            self.listed.push(index);
        }
    }

    fn unpark(&mut self, index: u32) {
        self.entries[index as usize].parked = None;
    }

    /// Wakes every listed task whose socket is ready; unlists woken,
    /// unparked and removed entries.
    #[inline(never)]
    fn wake_ready(&mut self) {
        let mut at = 0;
        while at < self.listed.len() {
            let index = self.listed[at];
            let entry = &mut self.entries[index as usize];
            if let (Some(socket), Some((waker, interest))) = (&entry.socket, entry.parked) {
                if !ready(&self.stack, socket, interest) {
                    at += 1;
                    continue;
                }
                entry.parked = None;
                waker.wake();
            }
            entry.listed = false;
            if entry.socket.is_none() {
                self.free.push(index);
            }
            self.listed.swap_remove(at);
        }
    }
}

fn ready(stack: &Stack, socket: &Socket, interest: Interest) -> bool {
    match (socket, interest) {
        (Socket::Tcp(handle), Interest::Connect) => !connecting(stack.tcp(handle).state()),
        (Socket::Tcp(handle), Interest::Accept) => !accepting(stack.tcp(handle).state()),
        (Socket::Tcp(handle), Interest::TcpRecv) => tcp_recv_ready(stack.tcp(handle)),
        (Socket::Udp(handle), Interest::UdpRecv) => stack.udp(handle).rx_queued() > 0,
        _ => unreachable!("interest does not match socket kind"),
    }
}

/// Whether TCP `recv` returns something other than `Ok(None)`.
fn tcp_recv_ready(socket: &tcp::Socket<'_>) -> bool {
    socket.can_recv() || !socket.may_recv()
}

/// SynSent, or SynReceived after a simultaneous open.
fn connecting(state: tcp::State) -> bool {
    matches!(state, tcp::State::SynSent | tcp::State::SynReceived)
}

/// An RST in SynReceived returns the socket to Listen.
fn accepting(state: tcp::State) -> bool {
    matches!(state, tcp::State::Listen | tcp::State::SynReceived)
}

/// Loop owner: owns the Stack. Not `Clone`.
///
/// Dropping the Net while sockets live keeps the Stack alive in them, but
/// nothing turns it any more: their parked tasks are never woken.
pub struct Net {
    shared: Shared,
}

impl Net {
    /// Sizes the registry once for every socket `stack` can hold.
    pub fn new(stack: Stack, config: NetConfig) -> Net {
        let slots = stack.max_sockets() + stack.max_udp_sockets();
        let entries = (0..slots).map(|_| Entry { socket: None, parked: None, listed: false }).collect();
        let inner = Inner {
            stack,
            entries,
            free: (0..slots as u32).rev().collect(),
            listed: Vec::with_capacity(slots),
            ingress_budget: config.ingress_budget,
        };
        Net { shared: Rc::new(RefCell::new(inner)) }
    }

    pub fn handle(&self) -> NetHandle {
        NetHandle { shared: self.shared.clone() }
    }

    /// `Stack::turn` at `now`, then wakes every task parked on a socket that
    /// became ready, also when the turn returns an error (egress can fail
    /// after ingress queued data). After a panic out of `turn`, drop the Net.
    #[inline]
    pub fn turn(&mut self, now: Instant) -> Result<Turn, StackError> {
        let mut inner = self.shared.borrow_mut();
        let inner = &mut *inner;
        let turn = inner.stack.turn(now, inner.ingress_budget);
        if !inner.listed.is_empty() {
            inner.wake_ready();
        }
        turn
    }

    /// `Stack::poll_at`.
    pub fn poll_at(&mut self) -> Option<Instant> {
        self.shared.borrow_mut().stack.poll_at()
    }

    /// The Stack back, for teardown. `Err(self)` while any `NetHandle` or
    /// socket is alive. Frames may still be held: they outlive the Stack.
    pub fn into_stack(self) -> Result<Stack, Net> {
        match Rc::try_unwrap(self.shared) {
            Ok(cell) => Ok(cell.into_inner().stack),
            Err(shared) => Err(Net { shared }),
        }
    }
}

/// Opens sockets on the Net's Stack; cannot turn it.
#[derive(Clone)]
pub struct NetHandle {
    shared: Shared,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpenError {
    Add(AddTcpError),
    Connect(ConnectError),
    Listen(ListenError),
}

impl NetHandle {
    /// Adds a TCP socket and connects it from `local`, whose port must be
    /// nonzero (conduit has no ephemeral ports). A refused add or connect
    /// removes the socket and returns the error.
    pub fn tcp_connect(
        &self,
        config: tcp::Config,
        remote: IpEndpoint,
        local: IpListenEndpoint,
    ) -> Result<Connecting, OpenError> {
        let stream = self.open_tcp(config, |stack, handle| {
            stack.connect(handle, remote, local).map_err(OpenError::Connect)
        })?;
        Ok(Connecting { stream: Some(stream) })
    }

    /// Adds a TCP socket listening on `local`; it becomes the one accepted
    /// connection (conduit has no accept queue). Several may listen on one
    /// port; the lowest slot takes the next SYN.
    pub fn tcp_accept(&self, config: tcp::Config, local: IpListenEndpoint) -> Result<Accepting, OpenError> {
        let stream = self.open_tcp(config, |stack, handle| stack.listen(handle, local).map_err(OpenError::Listen))?;
        Ok(Accepting { stream: Some(stream) })
    }

    fn open_tcp(
        &self,
        config: tcp::Config,
        start: impl FnOnce(&mut Stack, &TcpHandle) -> Result<(), OpenError>,
    ) -> Result<TcpStream, OpenError> {
        let mut inner = self.shared.borrow_mut();
        let handle = inner.stack.add_tcp(config).map_err(OpenError::Add)?;
        if let Err(error) = start(&mut inner.stack, &handle) {
            inner.stack.remove_tcp(handle);
            return Err(error);
        }
        let index = inner.insert(Socket::Tcp(handle));
        Ok(TcpStream { shared: self.shared.clone(), index })
    }

    pub fn udp_bind(&self, config: UdpConfig) -> Result<UdpSocket, AddUdpError> {
        let mut inner = self.shared.borrow_mut();
        let handle = inner.stack.add_udp(config)?;
        let index = inner.insert(Socket::Udp(handle));
        Ok(UdpSocket { shared: self.shared.clone(), index })
    }
}

/// Runs `op`; on `None` parks `cx`'s task on `index`, on `Some` clears the park.
fn poll_op<R>(
    shared: &Shared,
    index: u32,
    interest: Interest,
    cx: &Context<'_>,
    op: impl FnOnce(&mut Inner) -> Option<R>,
) -> Poll<R> {
    let mut inner = shared.borrow_mut();
    match op(&mut inner) {
        Some(out) => {
            inner.unpark(index);
            Poll::Ready(out)
        }
        None => {
            inner.park(index, interest, cx);
            Poll::Pending
        }
    }
}

/// The connection closed before it was established.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Closed {
    pub reset_by_peer: bool,
}

/// `Future<Output = Result<TcpStream, Closed>>`: waits while the socket is in
/// SynSent or SynReceived. Dropping it drops the socket (Linux `close()`).
pub struct Connecting {
    stream: Option<TcpStream>,
}

impl Future for Connecting {
    type Output = Result<TcpStream, Closed>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let stream = this.stream.as_ref().expect("Connecting polled after completion");
        let index = stream.index;
        let state = poll_op(&stream.shared, index, Interest::Connect, cx, |inner| {
            let (stack, handle) = inner.tcp(index);
            let socket = stack.tcp(handle);
            match socket.state() {
                state if connecting(state) => None,
                tcp::State::Closed => Some(Err(Closed { reset_by_peer: socket.reset_by_peer() })),
                _ => Some(Ok(())),
            }
        });
        state.map(|result| {
            let stream = this.stream.take().expect("stream present until completion");
            result.map(|()| stream)
        })
    }
}

/// `Future<Output = TcpStream>`: waits while the socket is in Listen or
/// SynReceived; an RST in SynReceived keeps it waiting. If the connection
/// already closed, `recv` on the stream reports it. Dropping it drops the
/// socket.
pub struct Accepting {
    stream: Option<TcpStream>,
}

impl Future for Accepting {
    type Output = TcpStream;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<TcpStream> {
        let this = self.get_mut();
        let stream = this.stream.as_ref().expect("Accepting polled after completion");
        let index = stream.index;
        let state = poll_op(&stream.shared, index, Interest::Accept, cx, |inner| {
            let (stack, handle) = inner.tcp(index);
            (!accepting(stack.tcp(handle).state())).then_some(())
        });
        state.map(|()| this.stream.take().expect("stream present until completion"))
    }
}

/// A connected TCP stream. Dropping it drops the conduit socket: Linux
/// `close()` semantics.
pub struct TcpStream {
    shared: Shared,
    index: u32,
}

impl TcpStream {
    /// `Future<Output = Result<TcpFrame, RecvError>>`: the next in-order frame.
    /// In-order data already received is delivered first, also after a reset.
    /// Then: after an accepted FIN, conduit's `RecvError::Finished`, even if a
    /// reset followed it; after a reset without one, or in a state without
    /// receive, `RecvError::InvalidState`.
    pub fn recv(&mut self) -> Recv<'_> {
        Recv { stream: self }
    }
}

impl Drop for TcpStream {
    fn drop(&mut self) {
        let socket = self.shared.borrow_mut().remove(self.index);
        drop(socket);
    }
}

pub struct Recv<'a> {
    stream: &'a mut TcpStream,
}

impl Future for Recv<'_> {
    type Output = Result<TcpFrame, RecvError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let stream = &*self.stream;
        let index = stream.index;
        poll_op(&stream.shared, index, Interest::TcpRecv, cx, |inner| {
            let (stack, handle) = inner.tcp(index);
            // `tcp_mut` marks the socket for egress; take it only to pop.
            if !tcp_recv_ready(stack.tcp(handle)) {
                return None;
            }
            match stack.tcp_mut(handle).recv() {
                Ok(None) => None,
                Ok(Some(frame)) => Some(Ok(TcpFrame(frame))),
                Err(error) => Some(Err(error)),
            }
        })
    }
}

impl Drop for Recv<'_> {
    fn drop(&mut self) {
        self.stream.shared.borrow_mut().unpark(self.stream.index);
    }
}

/// An owned in-order TCP frame.
#[derive(Debug)]
pub struct TcpFrame(dpdk_smoltcp::TcpFrame);

impl TcpFrame {
    pub fn payload(&self) -> &[u8] {
        self.0.payload()
    }

    pub fn payload_mut(&mut self) -> &mut [u8] {
        self.0.payload_mut()
    }

    /// Raw NIC stamp of the segment that created the frame; 0 = none.
    pub fn rx_timestamp(&self) -> u64 {
        self.0.rx_timestamp()
    }
}

/// A bound UDP socket. Dropping it unbinds the port.
pub struct UdpSocket {
    shared: Shared,
    index: u32,
}

impl UdpSocket {
    /// `Future<Output = Datagram>`: the next queued datagram.
    pub fn recv(&mut self) -> UdpRecv<'_> {
        UdpRecv { socket: self }
    }
}

impl Drop for UdpSocket {
    fn drop(&mut self) {
        let socket = self.shared.borrow_mut().remove(self.index);
        drop(socket);
    }
}

pub struct UdpRecv<'a> {
    socket: &'a mut UdpSocket,
}

impl Future for UdpRecv<'_> {
    type Output = Datagram;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Datagram> {
        let socket = &*self.socket;
        let index = socket.index;
        poll_op(&socket.shared, index, Interest::UdpRecv, cx, |inner| {
            let (stack, handle) = inner.udp(index);
            stack.udp_mut(handle).recv().map(Datagram)
        })
    }
}

impl Drop for UdpRecv<'_> {
    fn drop(&mut self) {
        self.socket.shared.borrow_mut().unpark(self.socket.index);
    }
}

/// An owned received UDP datagram.
#[derive(Debug)]
pub struct Datagram(dpdk_smoltcp::UdpDatagram);

impl Datagram {
    pub fn payload(&self) -> &[u8] {
        self.0.payload()
    }

    pub fn payload_mut(&mut self) -> &mut [u8] {
        self.0.payload_mut()
    }

    pub fn src(&self) -> IpEndpoint {
        self.0.src()
    }

    pub fn dst_addr(&self) -> Ipv4Address {
        self.0.dst_addr()
    }

    /// Raw NIC stamp; 0 = none.
    pub fn rx_timestamp(&self) -> u64 {
        self.0.rx_timestamp()
    }
}
