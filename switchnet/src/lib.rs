//! Async sockets over one conduit `Stack`, receive side.
//!
//! The loop owner holds the [`Net`] and calls `net.turn(now)` (a `Stack::turn`,
//! then a wake of every task parked on a socket that became ready), then polls
//! its executor. Tasks hold a [`NetHandle`], which opens sockets but cannot
//! turn the stack, and await owned received frames. Waking uses the std
//! `Waker`; one task may wait per socket at a time (`recv` takes `&mut`).
//! Everything is single-threaded (`Rc`), so no type here is `Send`.
//!
//! Received frames ([`TcpFrame`], [`Datagram`]) do not hold the Net: like
//! conduit's frames they stay valid after their socket, the Net and the Stack
//! are gone. Each held frame occupies its socket's receive budget until it drops.

use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

pub use dpdk_smoltcp::smoltcp::socket::tcp;
pub use dpdk_smoltcp::smoltcp::socket::tcp::{ConnectError, RecvError};
pub use dpdk_smoltcp::smoltcp::time::Instant;
pub use dpdk_smoltcp::smoltcp::wire::{IpEndpoint, IpListenEndpoint, Ipv4Address};
pub use dpdk_smoltcp::{AddTcpError, AddUdpError, Stack, StackError, Turn, UdpConfig};
use dpdk_smoltcp::{TcpHandle, UdpHandle};

use cell::StackCell;
use wait::{Interest, Registry};

mod cell;
mod wait;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NetConfig {
    /// Passed to `Stack::turn` each turn.
    pub ingress_budget: usize,
}

type Shared = StackCell<Inner>;

enum Socket {
    Tcp(TcpHandle),
    Udp(UdpHandle),
}

struct Inner {
    stack: Stack,
    registry: Registry<Socket>,
    ingress_budget: usize,
    /// Set while `Stack::turn` runs; left set if it unwinds.
    poisoned: bool,
}

impl Inner {
    #[inline]
    fn check(&self) {
        assert!(!self.poisoned, "Net poisoned: a Stack turn panicked");
    }

    fn tcp(&self, index: u32) -> &TcpHandle {
        match self.registry.socket(index) {
            Socket::Tcp(handle) => handle,
            Socket::Udp(_) => unreachable!("TCP entry holds a UDP handle"),
        }
    }
}

fn ready(stack: &Stack, socket: &Socket, interest: Interest) -> bool {
    match (socket, interest) {
        (Socket::Tcp(handle), Interest::Connect) => !handshaking(stack.tcp(handle).state()),
        (Socket::Tcp(handle), Interest::TcpRecv) => {
            let socket = stack.tcp(handle);
            socket.can_recv() || !socket.may_recv()
        }
        (Socket::Udp(handle), Interest::UdpRecv) => stack.udp(handle).rx_queued() > 0,
        _ => unreachable!("interest does not match socket kind"),
    }
}

/// SynSent, or SynReceived after a simultaneous open.
fn handshaking(state: tcp::State) -> bool {
    matches!(state, tcp::State::SynSent | tcp::State::SynReceived)
}

/// Loop owner: owns the Stack. Not `Clone`.
///
/// Dropping the Net while sockets live keeps the Stack alive in them, but
/// nothing turns it any more: their parked tasks are never woken.
pub struct Net {
    shared: Rc<Shared>,
}

impl Net {
    pub fn new(stack: Stack, config: NetConfig) -> Net {
        let inner = Inner {
            stack,
            registry: Registry::new(),
            ingress_budget: config.ingress_budget,
            poisoned: false,
        };
        Net { shared: Rc::new(StackCell::new(inner)) }
    }

    pub fn handle(&self) -> NetHandle {
        NetHandle { shared: self.shared.clone() }
    }

    /// `Stack::turn` at `now` with the configured ingress budget, then wakes
    /// every task parked on a socket that became ready. The check visits only
    /// sockets with a parked task. Wakers run after the stack is released.
    ///
    /// A panic out of `Stack::turn` poisons the Net: every later call except
    /// drops and `into_stack` panics.
    pub fn turn(&mut self, now: Instant) -> Result<Turn, StackError> {
        let (turn, mut wakes) = self.shared.with(|inner| {
            inner.check();
            inner.poisoned = true;
            let turn = inner.stack.turn(now, inner.ingress_budget);
            inner.poisoned = false;
            let Inner { stack, registry, .. } = inner;
            registry.scan(|socket, interest| ready(stack, socket, interest));
            (turn, registry.take_wakes())
        });
        if !wakes.is_empty() {
            wakes.drain(..).for_each(Waker::wake);
            self.shared.with(|inner| inner.registry.restore_wakes(wakes));
        }
        turn
    }

    /// `Stack::poll_at`.
    pub fn poll_at(&mut self) -> Option<Instant> {
        self.shared.with(|inner| {
            inner.check();
            inner.stack.poll_at()
        })
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
    shared: Rc<Shared>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpenError {
    Add(AddTcpError),
    Connect(ConnectError),
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
        let index = self.shared.with(|inner| {
            inner.check();
            let handle = inner.stack.add_tcp(config).map_err(OpenError::Add)?;
            if let Err(error) = inner.stack.connect(&handle, remote, local) {
                inner.stack.remove_tcp(handle);
                return Err(OpenError::Connect(error));
            }
            Ok(inner.registry.insert(Socket::Tcp(handle)))
        })?;
        let rx = TcpRx { shared: self.shared.clone(), index };
        Ok(Connecting { stream: Some(TcpStream { rx }), parked: false })
    }

    pub fn udp_bind(&self, config: UdpConfig) -> Result<UdpSocket, AddUdpError> {
        let index = self.shared.with(|inner| {
            inner.check();
            let handle = inner.stack.add_udp(config)?;
            Ok(inner.registry.insert(Socket::Udp(handle)))
        })?;
        Ok(UdpSocket { shared: self.shared.clone(), index })
    }
}

/// Polls `op` and parks `cx`'s task on `index` when it returns `None`. The
/// context waker is cloned, and replaced or unused wakers dropped, only
/// outside the borrow; the final check and the park happen in one borrow.
fn poll_op<R>(
    shared: &Shared,
    index: u32,
    interest: Interest,
    parked: &mut bool,
    cx: &mut Context<'_>,
    mut op: impl FnMut(&mut Inner, u32) -> Option<R>,
) -> Poll<R> {
    enum First<R> {
        Ready(R, Option<Waker>),
        Parked,
        Unparked,
    }
    let was_parked = *parked;
    let first = shared.with(|inner| {
        inner.check();
        match op(inner, index) {
            Some(out) => First::Ready(out, if was_parked { inner.registry.clear(index) } else { None }),
            None if was_parked && inner.registry.parked_as(index, cx.waker()) => First::Parked,
            None => First::Unparked,
        }
    });
    match first {
        First::Ready(out, stale) => {
            *parked = false;
            drop(stale);
            return Poll::Ready(out);
        }
        First::Parked => return Poll::Pending,
        First::Unparked => {}
    }
    let mut fresh = Some(cx.waker().clone());
    let (out, stale) = shared.with(|inner| {
        inner.check();
        match op(inner, index) {
            Some(out) => (Poll::Ready(out), inner.registry.clear(index)),
            None => {
                let waker = fresh.take().expect("fresh waker unused");
                (Poll::Pending, inner.registry.park(index, interest, waker))
            }
        }
    });
    *parked = out.is_pending();
    drop(stale);
    drop(fresh);
    out
}

/// Clears a dropped future's parked waker, dropping it outside the borrow.
fn unpark(shared: &Shared, index: u32) {
    let stale = shared.with(|inner| inner.registry.clear(index));
    drop(stale);
}

/// `Future<Output = Result<TcpStream, Closed>>`: waits while the socket is in
/// SynSent or SynReceived. Dropping it drops the socket (Linux `close()`).
pub struct Connecting {
    stream: Option<TcpStream>,
    parked: bool,
}

/// The connection closed before it was established.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Closed {
    pub reset_by_peer: bool,
}

impl Future for Connecting {
    type Output = Result<TcpStream, Closed>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let stream = this.stream.as_ref().expect("Connecting polled after completion");
        let shared = &*stream.rx.shared;
        let state = poll_op(shared, stream.rx.index, Interest::Connect, &mut this.parked, cx, |inner, index| {
            let socket = inner.stack.tcp(inner.tcp(index));
            match socket.state() {
                state if handshaking(state) => None,
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

/// A connected TCP stream. Dropping it drops the conduit socket: Linux
/// `close()` semantics.
pub struct TcpStream {
    rx: TcpRx,
}

/// The receive half: one waiter.
struct TcpRx {
    shared: Rc<Shared>,
    index: u32,
}

impl TcpStream {
    /// `Future<Output = Result<TcpFrame, RecvError>>`: the next in-order frame.
    /// After a FIN and the last frame, conduit's `RecvError::Finished`; after a
    /// reset or in a state without receive, `RecvError::InvalidState`.
    pub fn recv(&mut self) -> Recv<'_> {
        Recv { rx: &mut self.rx, parked: false }
    }
}

impl Drop for TcpStream {
    fn drop(&mut self) {
        let (socket, stale) = self.rx.shared.with(|inner| inner.registry.remove(self.rx.index));
        drop(stale);
        drop(socket);
    }
}

pub struct Recv<'a> {
    rx: &'a mut TcpRx,
    parked: bool,
}

impl Future for Recv<'_> {
    type Output = Result<TcpFrame, RecvError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        poll_op(&this.rx.shared, this.rx.index, Interest::TcpRecv, &mut this.parked, cx, |inner, index| {
            let handle = match inner.registry.socket(index) {
                Socket::Tcp(handle) => handle,
                Socket::Udp(_) => unreachable!("TCP entry holds a UDP handle"),
            };
            match inner.stack.tcp_mut(handle).recv() {
                Ok(None) => None,
                Ok(Some(frame)) => Some(Ok(TcpFrame(frame))),
                Err(error) => Some(Err(error)),
            }
        })
    }
}

impl Drop for Recv<'_> {
    fn drop(&mut self) {
        if self.parked {
            unpark(&self.rx.shared, self.rx.index);
        }
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
    shared: Rc<Shared>,
    index: u32,
}

impl UdpSocket {
    /// `Future<Output = Datagram>`: the next queued datagram.
    pub fn recv(&mut self) -> UdpRecv<'_> {
        UdpRecv { socket: self, parked: false }
    }
}

impl Drop for UdpSocket {
    fn drop(&mut self) {
        let (socket, stale) = self.shared.with(|inner| inner.registry.remove(self.index));
        drop(stale);
        drop(socket);
    }
}

pub struct UdpRecv<'a> {
    socket: &'a mut UdpSocket,
    parked: bool,
}

impl Future for UdpRecv<'_> {
    type Output = Datagram;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Datagram> {
        let this = self.get_mut();
        let socket = &*this.socket;
        poll_op(&socket.shared, socket.index, Interest::UdpRecv, &mut this.parked, cx, |inner, index| {
            let handle = match inner.registry.socket(index) {
                Socket::Udp(handle) => handle,
                Socket::Tcp(_) => unreachable!("UDP entry holds a TCP handle"),
            };
            inner.stack.udp_mut(handle).recv().map(Datagram)
        })
    }
}

impl Drop for UdpRecv<'_> {
    fn drop(&mut self) {
        if self.parked {
            unpark(&self.socket.shared, self.socket.index);
        }
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
