//! Async sockets over one conduit `Stack`, for the `switch` runtime.
//!
//! The loop owner holds the [`Net`] and calls `net.turn(now)` (a `Stack::turn`,
//! then a wake of every task parked on a socket that became ready), then polls
//! the switch runtime. Tasks hold a [`NetHandle`], which opens sockets and
//! hands out TX frames but cannot turn the stack. Futures here must be polled
//! by switch tasks: parking stores a `switch::StoredWaker`.
//! Everything is single-threaded (`Rc`), so no type here is `Send`.
//!
//! Each socket splits into a receive half and a send half; each half has at
//! most one waiting task (its futures take `&mut`). Sending never hands a
//! frame to a future: [`TcpTx::room`] waits until a frame of a given length
//! fits, then [`TcpTx::try_send`] queues it synchronously or returns it.
//!
//! Received frames ([`TcpFrame`], [`Datagram`]) do not hold the Net: like
//! conduit's frames they stay valid after their socket, the Net and the Stack
//! are gone. Each held frame occupies its socket's receive budget until it drops.

mod drive;
#[cfg(feature = "testkit")]
pub mod testkit;

pub use drive::{RoundError, boxed, first, round, within};

use std::cell::RefCell;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};

pub use dpdk_smoltcp::smoltcp::phy::TxFrame;
pub use dpdk_smoltcp::smoltcp::socket::tcp;
pub use dpdk_smoltcp::smoltcp::socket::tcp::{ConnectError, ListenError, RecvError, SendFrameError};
pub use dpdk_smoltcp::smoltcp::time::Instant;
pub use dpdk_smoltcp::smoltcp::wire::{IpEndpoint, IpListenEndpoint, Ipv4Address};
pub use dpdk_smoltcp::{
    AddTcpError, AddUdpError, Stack, StackError, Turn, UdpConfig, UdpSendError, UdpSendFrameError,
};
use dpdk_smoltcp::{ReadySide, TcpHandle, TcpReadyKey, UdpHandle, UdpReadyKey};
use switch::{StoredWaker, TurnRequest};

#[derive(Debug, Clone)]
pub struct NetConfig {
    /// Passed to `Stack::turn` each turn.
    pub ingress_budget: usize,
    /// The loop's `Runtime::turn_request()`, to end its poll early: once the
    /// frames handed to the Stack since the last turn (each `UdpTx::send` and
    /// `TcpTx::try_send` that succeeded) reach one `Stack::tx_burst`, the Net sets
    /// it, and a successful `turn` clears it. `None`: sends never set it.
    pub turn_request: Option<TurnRequest>,
}

type Shared = Rc<RefCell<Inner>>;

/// A handle and the readiness key taken from it at open, which arming uses.
enum Socket {
    Tcp(TcpHandle, TcpReadyKey),
    Udp(UdpHandle, UdpReadyKey),
}

/// Index of an entry's receive-side (recv, connect, accept) and send-side
/// (room) parked task.
const RX: usize = 0;
const TX: usize = 1;

#[derive(Clone, Copy)]
enum Interest {
    Connect,
    Accept,
    TcpRecv,
    UdpRecv,
    /// Room for one frame of this many bytes.
    TcpRoom(u32),
    /// Everything sent, FIN included, acked.
    TcpDelivered,
}

impl Interest {
    fn side(self) -> usize {
        match self {
            Interest::TcpRoom(_) | Interest::TcpDelivered => TX,
            _ => RX,
        }
    }
}

struct Entry {
    socket: Option<Socket>,
    parked: [Option<(StoredWaker, Interest)>; 2],
    /// Live halves; the socket drops with the last.
    halves: u8,
    /// Per side: a token for this entry and side is armed in the Stack or waits in its fired
    /// list. It may belong to the entry's previous socket, of either kind: a token that fired
    /// before the last half dropped stays out until `wake_fired` pops it, and the side does
    /// not arm until then, so a side never has two tokens out.
    token_out: [bool; 2],
}

struct Inner {
    stack: Stack,
    entries: Vec<Entry>,
    free: Vec<u32>,
    /// Tasks waiting for a TX frame; each listed once. Sized for one per
    /// socket slot; more simultaneous waiters than that grow it.
    frame_waiters: Vec<StoredWaker>,
    ingress_budget: usize,
    turn_request: Option<TurnRequest>,
    /// Frames handed to the Stack since the last turn; counted only with a `turn_request`.
    tx_handed: usize,
    /// `Stack::tx_burst`: `tx_handed` that sets the turn request.
    tx_burst: usize,
}

impl Inner {
    /// One frame handed to the Stack: at one TX burst since the last turn, request a turn.
    #[inline]
    fn handed_over(&mut self) {
        if let Some(request) = &self.turn_request {
            self.tx_handed += 1;
            if self.tx_handed >= self.tx_burst {
                request.set();
            }
        }
    }

    fn insert(&mut self, socket: Socket) -> u32 {
        let index = self.free.pop().expect("more sockets than the Stack holds");
        let entry = &mut self.entries[index as usize];
        entry.socket = Some(socket);
        entry.halves = 2;
        index
    }

    /// Drops one half: clears its parked handle and disarms its token; with the last half,
    /// removes the entry and returns its socket. The index is free at once; a token of the
    /// half that already fired is popped by the next turn, which checks whatever socket then
    /// holds the entry.
    fn release(&mut self, index: u32, side: usize) -> Option<Socket> {
        let entry = &mut self.entries[index as usize];
        entry.parked[side] = None;
        if entry.token_out[side] {
            let socket = entry.socket.as_ref().expect("a live half has a socket");
            entry.token_out[side] = !disarm(&mut self.stack, socket, side);
        }
        entry.halves -= 1;
        if entry.halves != 0 {
            return None;
        }
        self.free.push(index);
        entry.socket.take()
    }

    /// The stack and the TCP handle of entry `index`.
    fn tcp(&mut self, index: u32) -> (&mut Stack, &TcpHandle) {
        match &self.entries[index as usize].socket {
            Some(Socket::Tcp(handle, _)) => (&mut self.stack, handle),
            _ => unreachable!("entry holds no TCP handle"),
        }
    }

    /// The stack and the UDP handle of entry `index`.
    fn udp(&mut self, index: u32) -> (&mut Stack, &UdpHandle) {
        match &self.entries[index as usize].socket {
            Some(Socket::Udp(handle, _)) => (&mut self.stack, handle),
            _ => unreachable!("entry holds no UDP handle"),
        }
    }

    fn park(&mut self, index: u32, interest: Interest, cx: &Context<'_>) {
        // A stale handle (a leaked future's) is memory-safe: switch never frees task
        // blocks. Its wake polls whatever task then holds the slot: a spurious poll.
        let waker = StoredWaker::new(cx);
        let side = interest.side();
        let entry = &mut self.entries[index as usize];
        entry.parked[side] = Some((waker, interest));
        if !entry.token_out[side] {
            entry.token_out[side] = true;
            let socket = entry.socket.as_ref().expect("a parked entry has a socket");
            arm(&mut self.stack, socket, side, token(index, side));
        }
    }

    fn unpark(&mut self, index: u32, side: usize) {
        self.entries[index as usize].parked[side] = None;
    }
}

/// A side's readiness token: entry index and side.
fn token(index: u32, side: usize) -> u32 {
    (index << 1) | side as u32
}

fn arm(stack: &mut Stack, socket: &Socket, side: usize, token: u32) {
    match socket {
        Socket::Tcp(_, key) => stack.arm_tcp(*key, ready_side(side), token),
        Socket::Udp(_, key) => {
            debug_assert_eq!(side, RX, "nothing parks on UDP send");
            stack.arm_udp(*key, token);
        }
    }
}

/// Disarms the side's token on the entry's current socket; whether it was armed there. A
/// UDP socket arms only its receive side, so its send side has nothing to disarm: a send
/// token still out on its entry is the previous socket's and waits for `wake_fired`.
fn disarm(stack: &mut Stack, socket: &Socket, side: usize) -> bool {
    match socket {
        Socket::Tcp(_, key) => stack.disarm_tcp(*key, ready_side(side)),
        Socket::Udp(_, key) => side == RX && stack.disarm_udp(*key),
    }
}

fn ready_side(side: usize) -> ReadySide {
    if side == RX { ReadySide::Rx } else { ReadySide::Tx }
}

/// For `token` and every other fired token the Stack returns: wakes the side's parked task
/// if its socket is ready, else re-arms the token. A side with no parked task (canceled, its
/// half dropped, or the entry reused by a socket whose task has not parked) takes no action; the
/// token is only marked popped. A reused entry's task is woken only by its own socket's
/// readiness, since the check reads the socket the entry holds now.
#[inline(never)]
fn wake_fired(stack: &mut Stack, entries: &mut [Entry], mut token: u32) {
    loop {
        let side = (token & 1) as usize;
        let entry = &mut entries[(token >> 1) as usize];
        entry.token_out[side] = false;
        if let (Some(socket), Some((waker, interest))) = (&entry.socket, entry.parked[side]) {
            if ready(stack, socket, interest) {
                entry.parked[side] = None;
                waker.wake();
            } else {
                entry.token_out[side] = true;
                arm(stack, socket, side, token);
            }
        }
        match stack.pop_fired() {
            Some(next) => token = next,
            None => return,
        }
    }
}

/// Wakes every task waiting for a TX frame if the pool has one. The probe
/// frame goes straight back to the pool.
#[inline(never)]
fn wake_frame_waiters(stack: &mut Stack, waiters: &mut Vec<StoredWaker>) {
    if stack.tx_frame().is_some() {
        for waker in waiters.drain(..) {
            waker.wake();
        }
    }
}

/// Inlined into `wake_fired`.
#[inline(always)]
fn ready(stack: &Stack, socket: &Socket, interest: Interest) -> bool {
    match (socket, interest) {
        (Socket::Tcp(handle, _), Interest::Connect) => !connecting(stack.tcp(handle).state()),
        (Socket::Tcp(handle, _), Interest::Accept) => !accepting(stack.tcp(handle).state()),
        (Socket::Tcp(handle, _), Interest::TcpRecv) => tcp_recv_ready(stack.tcp(handle)),
        (Socket::Tcp(handle, _), Interest::TcpRoom(len)) => tcp_room_ready(stack.tcp(handle), len as usize),
        (Socket::Tcp(handle, _), Interest::TcpDelivered) => delivered(stack.tcp(handle).state()),
        (Socket::Udp(handle, _), Interest::UdpRecv) => stack.udp(handle).rx_queued() > 0,
        _ => unreachable!("interest does not match socket kind"),
    }
}

/// Whether TCP `recv` returns something other than `Ok(None)`.
fn tcp_recv_ready(socket: &tcp::Socket<'_>) -> bool {
    socket.can_recv() || !socket.may_recv()
}

/// Whether `room(len)` resolves: the socket can no longer send, or a frame of
/// `len` bytes fits (`send_frame` refuses with `Full` when it exceeds the free
/// bytes or no frame slot is free; `frame_send_room` is 0 without a slot).
fn tcp_room_ready(socket: &tcp::Socket<'_>, len: usize) -> bool {
    !socket.may_send()
        || (socket.send_capacity() - socket.send_queue() >= len && socket.frame_send_room() != 0)
}

/// Our FIN acked (FinWait2, TimeWait), or nothing more can be delivered (Closed).
fn delivered(state: tcp::State) -> bool {
    matches!(state, tcp::State::FinWait2 | tcp::State::TimeWait | tcp::State::Closed)
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
        assert!(u32::try_from(2 * slots).is_ok(), "socket slots exceed the readiness tokens");
        let entries =
            (0..slots).map(|_| Entry { socket: None, parked: [None; 2], halves: 0, token_out: [false; 2] }).collect();
        let tx_burst = stack.tx_burst();
        let inner = Inner {
            stack,
            entries,
            free: (0..slots as u32).rev().collect(),
            frame_waiters: Vec::with_capacity(slots),
            ingress_budget: config.ingress_budget,
            turn_request: config.turn_request,
            tx_handed: 0,
            tx_burst,
        };
        Net { shared: Rc::new(RefCell::new(inner)) }
    }

    pub fn handle(&self) -> NetHandle {
        NetHandle { shared: self.shared.clone() }
    }

    /// `Stack::turn` at `now`, then wakes every task parked on a socket that
    /// became ready, and every task waiting for a TX frame if the pool has
    /// one, also when the turn returns an error (egress can fail after
    /// ingress queued data). Only sockets whose readiness tokens fired since
    /// the last turn are checked, never every parked one. A successful turn
    /// (which flushed) clears the `NetConfig::turn_request` and restarts its frame
    /// count, also with frames still pending. After a panic out of `turn`, drop
    /// the Net.
    #[inline]
    pub fn turn(&mut self, now: Instant) -> Result<Turn, StackError> {
        let mut inner = self.shared.borrow_mut();
        let Inner { stack, entries, frame_waiters, ingress_budget, turn_request, tx_handed, .. } = &mut *inner;
        let turn = stack.turn(now, *ingress_budget);
        if turn.is_ok() {
            *tx_handed = 0;
            if let Some(request) = turn_request {
                request.clear();
            }
        }
        if let Some(token) = stack.pop_fired() {
            wake_fired(stack, entries, token);
        }
        if !frame_waiters.is_empty() {
            wake_frame_waiters(stack, frame_waiters);
        }
        turn
    }

    /// `Stack::poll_at`.
    pub fn poll_at(&mut self) -> Option<Instant> {
        self.shared.borrow_mut().stack.poll_at()
    }

    /// The Stack back, for teardown. `Err(self)` while any `NetHandle` or
    /// socket half is alive. Frames may still be held: they outlive the Stack.
    pub fn into_stack(self) -> Result<Stack, Net> {
        match Rc::try_unwrap(self.shared) {
            Ok(cell) => Ok(cell.into_inner().stack),
            Err(shared) => Err(Net { shared }),
        }
    }
}

/// Opens sockets on the Net's Stack and hands out TX frames; cannot turn it.
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
    /// port. A SYN goes to a listener bound to its destination address if one
    /// exists, whatever the slots; otherwise to an all-IP listener (`addr:
    /// None`) on the port. Among several listeners in the chosen group, the
    /// lowest slot takes it.
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
        let key = inner.stack.tcp_ready_key(&handle);
        let index = inner.insert(Socket::Tcp(handle, key));
        Ok(TcpStream {
            rx: TcpRx(Half { shared: self.shared.clone(), index, side: RX }),
            tx: TcpTx(Half { shared: self.shared.clone(), index, side: TX }),
        })
    }

    pub fn udp_bind(&self, config: UdpConfig) -> Result<UdpSocket, AddUdpError> {
        let mut inner = self.shared.borrow_mut();
        let handle = inner.stack.add_udp(config)?;
        let key = inner.stack.udp_ready_key(&handle);
        let index = inner.insert(Socket::Udp(handle, key));
        Ok(UdpSocket {
            rx: UdpRx(Half { shared: self.shared.clone(), index, side: RX }),
            tx: UdpTx(Half { shared: self.shared.clone(), index, side: TX }),
        })
    }

    /// `Future<Output = TxFrame>`: an empty frame from the Stack's frame pool
    /// (`capacity()` is the IP MTU − 28). While the pool is empty the task
    /// waits; it is woken by the first `Net::turn` that finds a free frame,
    /// and may wait again if another task took it first.
    pub fn tx_frame(&self) -> TxFrameWait<'_> {
        TxFrameWait { handle: self }
    }
}

pub struct TxFrameWait<'a> {
    handle: &'a NetHandle,
}

impl Future for TxFrameWait<'_> {
    type Output = TxFrame;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<TxFrame> {
        let mut inner = self.handle.shared.borrow_mut();
        if let Some(frame) = inner.stack.tx_frame() {
            return Poll::Ready(frame);
        }
        // Stale handles are memory-safe as in `Inner::park`. A dropped future's handle
        // stays until the next free frame wakes it, which spuriously polls its task, or
        // the task that reused the slot once its task finished. Entries are unique per task
        // slot (a reused slot has the same header), so the list holds at most one entry per
        // switch task slot however many waits are dropped.
        let waker = StoredWaker::new(cx);
        if !inner.frame_waiters.contains(&waker) {
            inner.frame_waiters.push(waker);
        }
        Poll::Pending
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
            inner.unpark(index, interest.side());
            Poll::Ready(out)
        }
        None => {
            inner.park(index, interest, cx);
            Poll::Pending
        }
    }
}

/// One half of a socket's entry. Dropping it clears the half's parked task;
/// dropping the last half drops the conduit socket.
struct Half {
    shared: Shared,
    index: u32,
    side: usize,
}

impl Drop for Half {
    fn drop(&mut self) {
        let socket = self.shared.borrow_mut().release(self.index, self.side);
        drop(socket);
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
        let half = &this.stream.as_ref().expect("Connecting polled after completion").rx.0;
        let index = half.index;
        let state = poll_op(&half.shared, index, Interest::Connect, cx, |inner| {
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
        let half = &this.stream.as_ref().expect("Accepting polled after completion").rx.0;
        let index = half.index;
        let state = poll_op(&half.shared, index, Interest::Accept, cx, |inner| {
            let (stack, handle) = inner.tcp(index);
            (!accepting(stack.tcp(handle).state())).then_some(())
        });
        state.map(|()| this.stream.take().expect("stream present until completion"))
    }
}

/// A connected TCP stream: a receive and a send half. Dropping both drops the
/// conduit socket: Linux `close()` semantics.
pub struct TcpStream {
    rx: TcpRx,
    tx: TcpTx,
}

impl TcpStream {
    /// The halves; sending needs the `TcpTx` half.
    pub fn split(self) -> (TcpRx, TcpTx) {
        (self.rx, self.tx)
    }

    /// [`TcpRx::recv`].
    pub fn recv(&mut self) -> Recv<'_> {
        self.rx.recv()
    }
}

/// The receive half of a TCP stream.
pub struct TcpRx(Half);

impl TcpRx {
    /// `Future<Output = Result<TcpFrame, RecvError>>`: the next in-order frame.
    /// In-order data already received is delivered first, also after a reset.
    /// Then: after an accepted FIN, conduit's `RecvError::Finished`, even if a
    /// reset followed it; after a reset without one, or in a state without
    /// receive, `RecvError::InvalidState`.
    pub fn recv(&mut self) -> Recv<'_> {
        Recv { rx: self }
    }
}

pub struct Recv<'a> {
    rx: &'a mut TcpRx,
}

impl Future for Recv<'_> {
    type Output = Result<TcpFrame, RecvError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let half = &self.rx.0;
        let index = half.index;
        poll_op(&half.shared, index, Interest::TcpRecv, cx, |inner| {
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
        self.rx.0.shared.borrow_mut().unpark(self.rx.0.index, RX);
    }
}

/// The send half of a TCP stream.
pub struct TcpTx(Half);

impl TcpTx {
    /// `Future<Output = Result<(), SendError>>`: `Ok` once a frame of `len`
    /// bytes fits (free bytes and a free frame slot), so `try_send` of such a
    /// frame succeeds until the next turn; `Err(InvalidState)` once the
    /// socket can no longer send (reset, closed, or after `shutdown`);
    /// `Err(TooLarge)` on the first poll if `len` exceeds the socket's send capacity
    /// (or u32): it would never fit.
    pub fn room(&mut self, len: usize) -> Room<'_> {
        Room { tx: self, len }
    }

    /// Queues `frame` (conduit `send_frame`); it leaves in a later turn. Every
    /// error returns the same frame: `Full` when it does not fit now,
    /// `InvalidState` when the socket cannot send, `Empty`, `Foreign`.
    pub fn try_send(&mut self, frame: TxFrame) -> Result<(), (TxFrame, SendFrameError)> {
        let mut inner = self.0.shared.borrow_mut();
        let (stack, handle) = inner.tcp(self.0.index);
        stack.tcp_send_frame(handle, frame)?;
        inner.handed_over();
        Ok(())
    }

    /// The socket's send buffer size: the largest `room` that can ever resolve.
    pub fn send_capacity(&self) -> usize {
        let mut inner = self.0.shared.borrow_mut();
        let (stack, handle) = inner.tcp(self.0.index);
        stack.tcp(handle).send_capacity()
    }

    /// Whether this stream is on `handle`'s Net, so frames from `handle` are its own.
    pub fn is_on(&self, handle: &NetHandle) -> bool {
        Rc::ptr_eq(&self.0.shared, &handle.shared)
    }

    /// Half-close: a FIN after the queued data. Receiving continues.
    pub fn shutdown(&mut self) {
        let mut inner = self.0.shared.borrow_mut();
        let (stack, handle) = inner.tcp(self.0.index);
        stack.close(handle);
    }

    /// `Future<Output = ()>`, after `shutdown`: resolves once the peer acked every byte and the
    /// FIN, or the connection closed (a reset: nothing more can be delivered). Until then the
    /// queued bytes exist only in this socket, and a Stack dropped now never sends them.
    pub fn delivered(&mut self) -> Delivered<'_> {
        Delivered { tx: self }
    }
}

pub struct Delivered<'a> {
    tx: &'a mut TcpTx,
}

impl Future for Delivered<'_> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let half = &self.tx.0;
        let index = half.index;
        poll_op(&half.shared, index, Interest::TcpDelivered, cx, |inner| {
            let (stack, handle) = inner.tcp(index);
            delivered(stack.tcp(handle).state()).then_some(())
        })
    }
}

impl Drop for Delivered<'_> {
    fn drop(&mut self) {
        self.tx.0.shared.borrow_mut().unpark(self.tx.0.index, TX);
    }
}

pub struct Room<'a> {
    tx: &'a mut TcpTx,
    len: usize,
}

/// Why a [`TcpTx::room`] failed.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum SendError {
    /// The socket can no longer send: reset, closed, or after `shutdown`.
    InvalidState,
    /// `len` exceeds the socket's send buffer, `capacity` bytes (or u32): it would never fit.
    TooLarge { len: usize, capacity: usize },
}

impl std::fmt::Display for SendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {
            Self::InvalidState => f.write_str("invalid state"),
            Self::TooLarge { len, capacity } => write!(f, "{len} B exceeds the {capacity} B send buffer"),
        }
    }
}

impl std::error::Error for SendError {}

impl Future for Room<'_> {
    type Output = Result<(), SendError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let (half, len) = (&self.tx.0, self.len);
        let index = half.index;
        let Ok(interest) = u32::try_from(len) else {
            let capacity = self.tx.send_capacity();
            return Poll::Ready(Err(SendError::TooLarge { len, capacity }));
        };
        poll_op(&half.shared, index, Interest::TcpRoom(interest), cx, |inner| {
            let (stack, handle) = inner.tcp(index);
            let socket = stack.tcp(handle);
            let capacity = socket.send_capacity();
            if len > capacity {
                Some(Err(SendError::TooLarge { len, capacity }))
            } else if !socket.may_send() {
                Some(Err(SendError::InvalidState))
            } else {
                tcp_room_ready(socket, len).then_some(Ok(()))
            }
        })
    }
}

impl Drop for Room<'_> {
    fn drop(&mut self) {
        self.tx.0.shared.borrow_mut().unpark(self.tx.0.index, TX);
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

/// A bound UDP socket: a receive and a send half. Dropping both unbinds the
/// port.
pub struct UdpSocket {
    rx: UdpRx,
    tx: UdpTx,
}

impl UdpSocket {
    /// The halves; sending needs the `UdpTx` half.
    pub fn split(self) -> (UdpRx, UdpTx) {
        (self.rx, self.tx)
    }

    /// [`UdpRx::recv`].
    pub fn recv(&mut self) -> UdpRecv<'_> {
        self.rx.recv()
    }
}

/// The receive half of a UDP socket.
pub struct UdpRx(Half);

impl UdpRx {
    /// `Future<Output = Datagram>`: the next queued datagram.
    pub fn recv(&mut self) -> UdpRecv<'_> {
        UdpRecv { rx: self }
    }
}

/// The send half of a UDP socket.
pub struct UdpTx(Half);

impl UdpTx {
    /// Sends `frame` as one datagram (conduit `udp_send_frame`); `to` may be
    /// `None` for a connected socket. Every error returns the same frame;
    /// `WouldBlock` (TX queue full) clears after a turn.
    pub fn send(&mut self, to: Option<IpEndpoint>, frame: TxFrame) -> Result<(), (TxFrame, UdpSendFrameError)> {
        let mut inner = self.0.shared.borrow_mut();
        let (stack, handle) = inner.udp(self.0.index);
        stack.udp_send_frame(handle, to, frame)?;
        inner.handed_over();
        Ok(())
    }
}

pub struct UdpRecv<'a> {
    rx: &'a mut UdpRx,
}

impl Future for UdpRecv<'_> {
    type Output = Datagram;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Datagram> {
        let half = &self.rx.0;
        let index = half.index;
        poll_op(&half.shared, index, Interest::UdpRecv, cx, |inner| {
            let (stack, handle) = inner.udp(index);
            stack.udp_mut(handle).recv().map(Datagram)
        })
    }
}

impl Drop for UdpRecv<'_> {
    fn drop(&mut self) {
        self.rx.0.shared.borrow_mut().unpark(self.rx.0.index, RX);
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
