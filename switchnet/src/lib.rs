//! Async sockets over one conduit `Stack`, receive side.
//!
//! The loop owner holds the [`Net`] and calls `net.turn(now)` (a `Stack::turn`,
//! then a wake of every task parked on a socket that became ready), then polls
//! its executor. Tasks hold a [`NetHandle`], which opens sockets but cannot
//! turn the stack, and await owned received frames. Waking uses the std
//! `Waker`; one task may wait per socket at a time (`recv` takes `&mut`).
//! Parking clones the context's waker, and switch's waker cannot be cloned
//! yet, so a switch task that has to wait on a socket panics until switch's
//! cloneable waker lands.
//! Everything is single-threaded (`Rc`), so no type here is `Send`.
//!
//! Received frames ([`TcpFrame`], [`Datagram`]) do not hold the Net: like
//! conduit's frames they stay valid after their socket, the Net and the Stack
//! are gone. Each held frame occupies its socket's receive budget until it drops.

use std::mem;
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

#[cfg(test)]
mod alloc_count;
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
    /// Set if `Stack::turn` unwinds.
    poisoned: bool,
    /// Makes the next `stack_turn` panic where `Stack::turn` would run.
    #[cfg(test)]
    fail_turn: bool,
}

impl Inner {
    #[inline]
    fn check(&self) {
        assert!(!self.poisoned, "Net poisoned: a Stack turn panicked");
    }

    /// `Stack::turn` with the configured budget; poisons only on unwind.
    #[inline]
    fn stack_turn(&mut self, now: Instant) -> Result<Turn, StackError> {
        struct PoisonOnUnwind<'a>(&'a mut bool);
        impl Drop for PoisonOnUnwind<'_> {
            fn drop(&mut self) {
                *self.0 = true;
            }
        }
        self.check();
        let poison = PoisonOnUnwind(&mut self.poisoned);
        #[cfg(test)]
        assert!(!self.fail_turn, "injected Stack::turn panic");
        let turn = self.stack.turn(now, self.ingress_budget);
        mem::forget(poison);
        turn
    }

    /// After the Stack turn: what is left for outside the borrow.
    #[inline(never)]
    fn collect(&mut self) -> Wake {
        if self.registry.has_wakes() {
            Wake::Leftovers
        } else if self.scan() {
            Wake::Ready
        } else {
            Wake::Nothing
        }
    }

    /// Moves the wakers of parked tasks whose socket is ready into the
    /// (empty) scratch; whether it holds any.
    fn scan(&mut self) -> bool {
        let Inner { stack, registry, .. } = self;
        registry.scan(|socket, interest| ready(stack, socket, interest))
    }

    fn tcp(&self, index: u32) -> &TcpHandle {
        match self.registry.socket(index) {
            Socket::Tcp(handle) => handle,
            Socket::Udp(_) => unreachable!("TCP entry holds a UDP handle"),
        }
    }
}

/// What a turn must deliver once the stack is released.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Wake {
    Nothing,
    /// The scan collected wakers.
    Ready,
    /// A panicking wake left wakers undelivered: deliver them, then scan.
    Leftovers,
}

fn ready(stack: &Stack, socket: &Socket, interest: Interest) -> bool {
    match (socket, interest) {
        (Socket::Tcp(handle), Interest::Connect) => !handshaking(stack.tcp(handle).state()),
        (Socket::Tcp(handle), Interest::TcpRecv) => tcp_recv_ready(stack.tcp(handle)),
        (Socket::Udp(handle), Interest::UdpRecv) => stack.udp(handle).rx_queued() > 0,
        _ => unreachable!("interest does not match socket kind"),
    }
}

/// Whether TCP `recv` returns something other than `Ok(None)`: a frame or an
/// error. Takes `&Socket`, so checking marks nothing for egress.
fn tcp_recv_ready(socket: &tcp::Socket<'_>) -> bool {
    socket.can_recv() || !socket.may_recv()
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
    /// Sizes the wait registry once for every socket `stack` can hold.
    pub fn new(stack: Stack, config: NetConfig) -> Net {
        let slots = stack.max_sockets() + stack.max_udp_sockets();
        let inner = Inner {
            stack,
            registry: Registry::with_capacity(slots),
            ingress_budget: config.ingress_budget,
            poisoned: false,
            #[cfg(test)]
            fail_turn: false,
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
    /// The check and the wakes run also when `Stack::turn` returns an error:
    /// it can return `StackError::Port` from egress after ingress already
    /// queued data or applied a FIN or reset, and those tasks must not sleep.
    ///
    /// A panic out of `Stack::turn` poisons the Net: every later call except
    /// drops and `into_stack` panics. A panic out of a waker's `wake`
    /// propagates and discards this turn's result (the `Turn` or the error);
    /// the ready tasks not yet woken are woken by the next turn.
    #[inline]
    pub fn turn(&mut self, now: Instant) -> Result<Turn, StackError> {
        let mut wake = Wake::Nothing;
        let turn = self.shared.with(|inner| {
            let turn = inner.stack_turn(now);
            if inner.registry.pending() {
                wake = inner.collect();
            }
            turn
        });
        if wake != Wake::Nothing {
            self.wake_ready(wake);
        }
        turn
    }

    /// Delivers what the turn collected; after leftovers, scans (from an
    /// empty scratch) and delivers that too.
    #[inline(never)]
    fn wake_ready(&self, wake: Wake) {
        self.deliver();
        if wake == Wake::Leftovers && self.shared.with(Inner::scan) {
            self.deliver();
        }
    }

    /// Wakes the scratch's wakers outside the borrow. If a wake unwinds, the
    /// rest go back into the registry with the scratch.
    fn deliver(&self) {
        struct Deliver<'a> {
            shared: &'a Shared,
            wakes: Vec<Waker>,
        }
        impl Drop for Deliver<'_> {
            fn drop(&mut self) {
                let wakes = mem::take(&mut self.wakes);
                self.shared.with(|inner| inner.registry.restore_wakes(wakes));
            }
        }
        let wakes = self.shared.with(|inner| inner.registry.take_wakes());
        let mut deliver = Deliver { shared: &self.shared, wakes };
        while let Some(waker) = deliver.wakes.pop() {
            waker.wake();
        }
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
    /// From a poisoned Net the Stack is one whose turn panicked: drop it or
    /// call `into_port`, nothing else.
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
    /// In-order data already received is delivered first, also after a reset.
    /// Then: after an accepted FIN, conduit's `RecvError::Finished`, even if a
    /// reset followed it; after a reset without one, or in a state without
    /// receive, `RecvError::InvalidState`.
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
            // `tcp_mut` marks the socket for egress; take it only to pop.
            if !tcp_recv_ready(inner.stack.tcp(handle)) {
                return None;
            }
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

#[cfg(test)]
mod tests {
    use std::any::Any;
    use std::panic::{AssertUnwindSafe, catch_unwind};

    use dpdk_smoltcp::{StackConfig, rx_pool_count, rx_reserve};
    use zhida_test_kit::pair::{self, BUDGET, Side, T0};
    use zhida_test_kit::ring_port::RingPort;
    use zhida_test_kit::with_eal;

    use super::*;
    use crate::alloc_count::allocations;

    fn message(payload: Box<dyn Any + Send>) -> String {
        match payload.downcast::<String>() {
            Ok(message) => *message,
            Err(payload) => (*payload.downcast::<&str>().expect("panic message")).to_owned(),
        }
    }

    fn panic_of<R>(body: impl FnOnce() -> R) -> String {
        match catch_unwind(AssertUnwindSafe(body)) {
            Ok(_) => panic!("did not panic"),
            Err(payload) => message(payload),
        }
    }

    const RX_CAPACITY: usize = 6 * 1460;

    /// A Net alone on a net_ring port: nothing ever arrives.
    fn lone_net(eal: &dpdk_smoltcp::Eal, name: &str, max_tcp: usize, max_udp: usize) -> (RingPort, Net) {
        let fixture = RingPort::new(name, 16, 16);
        let stack_config =
            StackConfig { max_sockets: max_tcp, max_udp_sockets: max_udp, ..pair::config(Side::A) };
        let mut config = fixture.config();
        let reserve = rx_reserve(config.rx_descriptors, stack_config.rx_staging, config.rx_pool.cache_size);
        config.rx_pool.count = rx_pool_count(reserve, config.mtu, std::iter::repeat_n(RX_CAPACITY, max_tcp))
            + u32::try_from(4 * max_udp).unwrap();
        let stack = match Stack::new(fixture.open_with(eal, config), stack_config, T0) {
            Ok(stack) => stack,
            Err((_, error)) => panic!("{error}"),
        };
        (fixture, Net::new(stack, NetConfig { ingress_budget: BUDGET }))
    }

    fn close(fixture: RingPort, net: Net) {
        let port = net.into_stack().ok().expect("every socket and handle dropped").into_port();
        fixture.drain();
        assert_eq!(fixture.in_use(), (0, 0));
        port.try_close().unwrap();
        fixture.close();
    }

    fn udp(port: u16) -> UdpConfig {
        UdpConfig { port, rx_queue: 4, tx_checksum: true, broadcast: false }
    }

    fn connect(handle: &NetHandle, port: u16) -> Connecting {
        let config = tcp::Config { rx_capacity: RX_CAPACITY, tx_capacity: 4096, mss_cap: None };
        let remote = IpEndpoint::new(Side::B.ip().into(), 80);
        handle.tcp_connect(config, remote, IpListenEndpoint { addr: None, port }).unwrap()
    }

    /// The injected panic stands in for one unwinding out of `Stack::turn`
    /// (conduit has no deterministic trigger); it runs where `Stack::turn`
    /// would, under the same poison guard.
    #[zhida_test_support::pinned_test]
    fn stack_turn_panic_poisons_every_later_operation_but_drop_and_into_stack() {
        with_eal(|eal| {
            let (fixture, mut net) = lone_net(eal, "pois", 1, 2);
            let handle = net.handle();
            let mut socket = handle.udp_bind(udp(7000)).unwrap();
            let mut recv = Box::pin(socket.recv());
            let mut cx = Context::from_waker(Waker::noop());
            assert!(recv.as_mut().poll(&mut cx).is_pending());
            net.turn(T0).unwrap();

            net.shared.with(|inner| inner.fail_turn = true);
            assert_eq!(panic_of(|| net.turn(T0)), "injected Stack::turn panic");
            let poisoned = "Net poisoned: a Stack turn panicked";
            assert_eq!(panic_of(|| net.turn(T0)), poisoned);
            assert_eq!(panic_of(|| net.poll_at()), poisoned);
            assert_eq!(panic_of(|| handle.udp_bind(udp(7001))), poisoned);
            assert_eq!(panic_of(|| recv.as_mut().poll(&mut cx)), poisoned);

            drop(recv);
            drop(socket);
            drop(handle);
            close(fixture, net);
        })
    }

    #[zhida_test_support::pinned_test]
    fn registry_is_sized_once_for_every_stack_socket() {
        with_eal(|eal| {
            let (fixture, net) = lone_net(eal, "size", 3, 2);
            assert_eq!(net.shared.with(|inner| inner.registry.capacities()), [5; 4]);
            close(fixture, net);
        })
    }

    /// Every UDP and TCP socket parked, one canceled: turns allocate nothing.
    #[zhida_test_support::pinned_test]
    fn turns_with_parked_and_canceled_sockets_do_not_allocate() {
        with_eal(|eal| {
            let (fixture, mut net) = lone_net(eal, "talc", 2, 16);
            let handle = net.handle();
            let mut sockets: Vec<UdpSocket> = (0..16).map(|i| handle.udp_bind(udp(7000 + i)).unwrap()).collect();
            let mut connecting = [connect(&handle, 49152), connect(&handle, 49153)].map(Box::pin);
            let mut cx = Context::from_waker(Waker::noop());
            let mut recvs: Vec<_> = sockets.iter_mut().map(|socket| Box::pin(socket.recv())).collect();
            for recv in &mut recvs {
                assert!(recv.as_mut().poll(&mut cx).is_pending());
            }
            for connect in &mut connecting {
                assert!(connect.as_mut().poll(&mut cx).is_pending());
            }
            net.turn(T0).unwrap();
            let (turns, allocs) = allocations(|| (0..8).map(|_| net.turn(T0).is_ok()).filter(|&ok| ok).count());
            assert_eq!((turns, allocs), (8, 0));
            drop(recvs.pop());
            let (turn, allocs) = allocations(|| net.turn(T0));
            turn.unwrap();
            assert_eq!(allocs, 0, "the turn that unlists a canceled future allocated");
            drop(recvs);
            drop((connecting, sockets, handle));
            close(fixture, net);
        })
    }

    #[zhida_test_support::pinned_test]
    fn dropping_sockets_through_the_net_does_not_allocate() {
        with_eal(|eal| {
            let (fixture, net) = lone_net(eal, "dalc", 1, 1);
            let handle = net.handle();
            let udp_socket = handle.udp_bind(udp(7000)).unwrap();
            let ((), allocs) = allocations(|| drop(udp_socket));
            assert_eq!(allocs, 0, "dropping a UdpSocket allocated");
            // `Connecting` owns the TcpStream; dropping it runs the stream's drop.
            let connecting = connect(&handle, 49152);
            let ((), allocs) = allocations(|| drop(connecting));
            assert_eq!(allocs, 0, "dropping a TcpStream allocated");
            drop(handle);
            close(fixture, net);
        })
    }
}
