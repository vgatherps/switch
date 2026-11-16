//! Async TCP and UDP sockets for tasks on the switch runtime.
//!
//! The loop that drives the program owns a [`Net`] and moves packets with [`round`]; tasks hold a
//! [`NetHandle`] to open sockets and get frames to send. A `Net` either runs a [`Stack`] in this
//! process ([`Net::new`]) or reaches the NIC through the junction daemon ([`Net::remote`]); task
//! code is the same for both. Applications use this crate as `switch::net`.

mod drive;
mod remote;
#[cfg(feature = "testkit")]
pub mod testkit;

pub use drive::{RoundError, first, round, within};

use std::cell::RefCell;
use std::collections::VecDeque;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};

pub use dpdk_smoltcp::smoltcp::phy::{ImmutableFrame, OwnedFrame};
pub use dpdk_smoltcp::smoltcp::socket::tcp;
pub use dpdk_smoltcp::smoltcp::time::Instant;
pub use dpdk_smoltcp::smoltcp::wire::{IpEndpoint, IpListenEndpoint, Ipv4Address};
pub use dpdk_smoltcp::{Sent, Stack, StackError, Turn, UdpConfig, UdpSocketStats};
use dpdk_smoltcp::{AddTcpError, AddUdpError, ReadySide, TcpHandle, TcpReadyKey, UdpHandle, UdpReadyKey, UdpSendError, UdpSendFrameError};
use switch_runtime::StoredWaker;
use remote::Remote;

/// Settings for [`Net::remote`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RemoteConfig {
    /// Number of buffers in the Net's own pool for [`NetHandle::tx_frame`], each
    /// [`Session::frame_capacity`](switch_junction::session::Session::frame_capacity) bytes. There
    /// is no default. A sent frame returns to the pool as soon as `try_send` or `UdpTx::send`
    /// copies it, so the pool only needs to cover frames tasks hold at once.
    pub tx_frames: usize,
}

/// Error from [`Net::receive`] and [`Net::send`]. `Display` prints the inner error; `source`
/// returns it.
#[derive(Debug)]
pub enum TurnError {
    /// Local backend: the [`Stack`] failed, for example `StackError::TimeWentBackwards` when
    /// `now` is earlier than the previous receive's, or a NIC port error.
    Stack(StackError),
    /// Junction backend: the session with the daemon ended. The first error carries the reason;
    /// every later `receive` and `send` returns
    /// [`Lost::Disconnected`](switch_junction::session::Lost::Disconnected). See [`Error::HostLost`]
    /// for what sockets do afterwards.
    HostLost(switch_junction::session::Lost),
}

impl std::fmt::Display for TurnError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Stack(error) => error.fmt(f),
            Self::HostLost(error) => error.fmt(f),
        }
    }
}

impl std::error::Error for TurnError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(match self {
            Self::Stack(error) => error,
            Self::HostLost(error) => error,
        })
    }
}

/// Settings for [`Net::new`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NetConfig {
    /// The most received packets one [`Net::receive`] processes ([`Stack::receive`]'s
    /// `ingress_budget`). Packets left over are reported in `Turn::rx_staged`. There is no
    /// default.
    pub ingress_budget: usize,
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
    Bind,
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

struct Local {
    stack: Stack,
    entries: Vec<Entry>,
    free: Vec<u32>,
    /// Tasks waiting for a TX frame, oldest first; each listed once. A wait
    /// dropped while queued leaves its entry: a free frame's wake then goes to it,
    /// and the waiters behind it are woken one `receive` later.
    frame_waiters: VecDeque<StoredWaker>,
    ingress_budget: usize,
    /// Frames handed to the Stack since the last successful send.
    tx_handed: usize,
    /// `Stack::tx_burst`: from this many `tx_handed` on, each handed frame requests a turn.
    tx_burst: usize,
}

impl Local {
    /// One frame handed to the Stack: at one TX burst since the last send, end the
    /// running `Runtime::poll` after this task (`switch_runtime::request_turn`), so the loop
    /// sends and the frames leave sooner.
    #[inline]
    fn handed_over(&mut self) {
        self.tx_handed += 1;
        if self.tx_handed >= self.tx_burst {
            switch_runtime::request_turn();
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
    /// half that already fired is popped by the next `Net::receive`, which checks whatever socket then
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

// One Inner per Net; boxing adds a pointer chase to every local socket operation.
#[allow(clippy::large_enum_variant)]
enum Inner {
    Local(Local),
    Remote(Remote),
}

impl Inner {
    fn park(&mut self, index: u32, interest: Interest, cx: &Context<'_>) {
        match self {
            Self::Local(local) => local.park(index, interest, cx),
            Self::Remote(remote) => remote.park(index, interest, cx),
        }
    }

    fn unpark(&mut self, index: u32, side: usize) {
        match self {
            Self::Local(local) => local.unpark(index, side),
            Self::Remote(remote) => remote.unpark(index, side),
        }
    }

    fn release(&mut self, index: u32, side: usize) -> Option<Socket> {
        match self {
            Self::Local(local) => local.release(index, side),
            Self::Remote(remote) => {
                remote.release(index, side);
                None
            }
        }
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

/// Wakes as many of the oldest tasks waiting for a TX frame as the pool has free
/// frames. A woken task that finds none (another took it) queues again at the back.
#[inline(never)]
fn wake_frame_waiters(stack: &Stack, waiters: &mut VecDeque<StoredWaker>) {
    let woken = stack.free_frames().min(waiters.len());
    for waker in waiters.drain(..woken) {
        waker.wake();
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
/// `len` bytes fits in the socket's free send bytes.
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

/// The driving loop's side of the network, and the only value that moves packets. Not `Clone`.
///
/// Each round calls [`receive`](Net::receive), polls the runtime, then calls
/// [`send`](Net::send); [`round`] does all three. Tasks get a [`NetHandle`] from
/// [`handle`](Net::handle). The constructor chooses the backend:
///
/// | | [`Net::new`] (local) | [`Net::remote`] (junction) |
/// |---|---|---|
/// | NIC owned by | this process; the `Net` holds the [`Stack`] | the junction daemon; the `Net` holds a [`Session`](switch_junction::session::Session) |
/// | Socket table | `max_sockets` TCP plus `max_udp_sockets` UDP sockets of the Stack | `Session::max_sockets` sockets of either kind |
/// | Teardown | [`Net::into_stack`] | [`Net::into_session`] |
///
/// Behavior that differs by backend is described on [`Net::receive`], [`Net::send`],
/// [`Net::poll_at`], [`NetHandle::tx_frame`], [`TcpTx::room`], [`TcpTx::try_send`],
/// [`TcpTx::send_capacity`], [`UdpTx::send`], [`UdpTx::stats`] and [`Error`].
///
/// No type in this module is `Send`. Every future it returns must be polled by a task of the
/// switch runtime: under any other executor, the first poll that returns `Pending` panics with
/// "StoredWaker needs a switch task waker".
///
/// Dropping the `Net` while handles or sockets are alive keeps its backend alive in them, but
/// nothing receives or sends any more: tasks waiting on those sockets are never woken.
pub struct Net {
    shared: Shared,
}

impl Net {
    /// A `Net` over a [`Stack`] running in this process.
    ///
    /// The socket table has room for the Stack's `max_sockets` TCP and `max_udp_sockets` UDP
    /// sockets.
    ///
    /// # Panics
    ///
    /// If twice the total socket count does not fit in `u32`.
    pub fn new(stack: Stack, config: NetConfig) -> Net {
        let slots = stack.max_sockets() + stack.max_udp_sockets();
        assert!(u32::try_from(2 * slots).is_ok(), "socket slots exceed the readiness tokens");
        let entries =
            (0..slots).map(|_| Entry { socket: None, parked: [None; 2], halves: 0, token_out: [false; 2] }).collect();
        let tx_burst = stack.tx_burst();
        let inner = Local {
            stack,
            entries,
            free: (0..slots as u32).rev().collect(),
            frame_waiters: VecDeque::new(),
            ingress_budget: config.ingress_budget,
            tx_handed: 0,
            tx_burst,
        };
        Net { shared: Rc::new(RefCell::new(Inner::Local(inner))) }
    }

    /// A `Net` over a session with the junction daemon, which runs the Stack and owns the NIC.
    ///
    /// The socket table has `session.max_sockets()` slots, shared by TCP and UDP. The frame pool
    /// for [`NetHandle::tx_frame`] is allocated here: `config.tx_frames` buffers of
    /// `session.frame_capacity()` bytes.
    ///
    /// # Panics
    ///
    /// If `session.frame_capacity()` exceeds `u32::MAX - 2`.
    pub fn remote(session: switch_junction::session::Session, config: RemoteConfig) -> Net {
        Net { shared: Rc::new(RefCell::new(Inner::Remote(Remote::new(session, config)))) }
    }

    /// A new handle for tasks. A live handle keeps [`Net::into_stack`] and
    /// [`Net::into_session`] from succeeding.
    pub fn handle(&self) -> NetHandle {
        NetHandle { shared: self.shared.clone() }
    }

    /// The receive step: takes in packets and wakes the tasks whose sockets became ready.
    ///
    /// It also wakes tasks waiting in [`NetHandle::tx_frame`], oldest first, as many as the pool
    /// has free frames. Tasks are woken even when this returns an error, because the error can
    /// come after packets were taken in.
    ///
    /// | Backend | What it does | Errors |
    /// |---|---|---|
    /// | local | [`Stack::receive`] at `now` with [`NetConfig::ingress_budget`]; answers what needs an immediate answer (ACKs, resets, ARP) | [`TurnError::Stack`] |
    /// | junction | polls the session once: sends pending control messages, applies the daemon's events, collects received packets; `now` is unused | [`TurnError::HostLost`] |
    ///
    /// The returned [`Turn`] counts:
    ///
    /// | Field | Local | Junction |
    /// |---|---|---|
    /// | `rx_received` | packets the RX burst staged | packets delivered to an open socket |
    /// | `rx_rejected` | received packets dropped as unsupported or with a bad IPv4 header checksum | 0 |
    /// | `rx_processed` | staged packets handed to TCP/IP processing | daemon events plus packets delivered |
    /// | `rx_staged` | packets still staged because the budget ran out; receive again promptly | 0 |
    /// | `tx_accepted` | packets the NIC accepted during this call | 0 |
    /// | `tx_pending` | packets still waiting for the NIC after this call | 0 |
    ///
    /// If a panic unwinds out of `receive`, drop the `Net`.
    #[inline]
    pub fn receive(&mut self, now: Instant) -> Result<Turn, TurnError> {
        let mut inner = self.shared.borrow_mut();
        let Inner::Local(Local { stack, entries, frame_waiters, ingress_budget, .. }) = &mut *inner else {
            let Inner::Remote(remote) = &mut *inner else { unreachable!() };
            return remote.receive().map_err(TurnError::HostLost);
        };
        let turn = stack.receive(now, *ingress_budget);
        if let Some(token) = stack.pop_fired() {
            wake_fired(stack, entries, token);
        }
        if !frame_waiters.is_empty() {
            wake_frame_waiters(stack, frame_waiters);
        }
        turn.map_err(TurnError::Stack)
    }

    /// The send step: what tasks queued since [`receive`](Net::receive) leaves.
    ///
    /// | Backend | What it does | Returned [`Sent`] | Errors |
    /// |---|---|---|---|
    /// | local | [`Stack::send`]: TCP sends queued data, retransmissions and window updates; packets go to the NIC | `tx_accepted`: packets the NIC accepted in the final flush; `tx_pending`: packets still waiting for it | [`TurnError::Stack`] |
    /// | junction | nothing left to push: [`TcpTx::try_send`] and [`UdpTx::send`] already copied into the send ring | `tx_accepted`: records committed since the last `send`; `tx_pending`: 0 | [`TurnError::HostLost`] once the session is lost |
    ///
    /// On the local backend, frames handed to the Stack (successful `try_send` and `UdpTx::send`)
    /// are counted from the last successful `send`. Once the count reaches the NIC port's TX
    /// burst size ([`Stack::tx_burst`]), each further one asks the runtime to end the current
    /// poll after that task, so the send step comes sooner.
    ///
    /// If a panic unwinds out of `send`, drop the `Net`.
    #[inline]
    pub fn send(&mut self) -> Result<Sent, TurnError> {
        match &mut *self.shared.borrow_mut() {
            Inner::Local(local) => {
                let sent = local.stack.send().map_err(TurnError::Stack)?;
                local.tx_handed = 0;
                Ok(sent)
            }
            Inner::Remote(remote) => remote.send().map_err(TurnError::HostLost),
        }
    }

    /// When the local Stack's next TCP timer is due ([`Stack::poll_at`]); a loop that sleeps
    /// while idle should wake by then. Always `None` on the junction backend, where timers run
    /// in the daemon.
    pub fn poll_at(&mut self) -> Option<Instant> {
        match &mut *self.shared.borrow_mut() {
            Inner::Local(local) => local.stack.poll_at(),
            Inner::Remote(_) => None,
        }
    }

    /// The [`Stack`] back, for teardown. Returns this `Net` as the error while any
    /// [`NetHandle`], socket half or unfinished opening future is alive, and always on the
    /// junction backend. Received frames do not hold the `Net`; they stay valid after the Stack
    /// is gone.
    pub fn into_stack(self) -> Result<Stack, Net> {
        match Rc::try_unwrap(self.shared) {
            Ok(cell) => match cell.into_inner() {
                Inner::Local(local) => Ok(local.stack),
                inner => Err(Net { shared: Rc::new(RefCell::new(inner)) }),
            },
            Err(shared) => Err(Net { shared }),
        }
    }

    /// The [`Session`](switch_junction::session::Session) back, for teardown. Returns this `Net`
    /// as the error while any [`NetHandle`], socket half or unfinished opening future is alive,
    /// and always on the local backend. The caller then finishes the session
    /// ([`Session::finish`](switch_junction::session::Session::finish)) and polls it until
    /// [`Session::closed`](switch_junction::session::Session::closed).
    pub fn into_session(self) -> Result<switch_junction::session::Session, Net> {
        match Rc::try_unwrap(self.shared) {
            Ok(cell) => match cell.into_inner() {
                Inner::Remote(remote) => Ok(remote.session),
                inner => Err(Net { shared: Rc::new(RefCell::new(inner)) }),
            },
            Err(shared) => Err(Net { shared }),
        }
    }
}

/// A task's access to a [`Net`]: opens sockets and hands out frames to send. Cheap to clone.
/// It cannot receive or send packets; only the `Net` does that.
///
/// The opening calls ([`tcp_connect`](NetHandle::tcp_connect),
/// [`tcp_accept`](NetHandle::tcp_accept), [`udp_bind`](NetHandle::udp_bind)) create the socket
/// when called, not at the first poll. The returned future waits for the outcome; dropping it
/// before it resolves drops the socket. Each socket takes one slot in the Net's socket table
/// until both its halves are dropped.
///
/// On the local backend every opening error resolves on the first poll, with no socket left
/// behind. On the junction backend the first poll returns `SocketsFull` (no free session slot),
/// `InvalidConfig` and `HostLost`; errors the daemon decides (`SocketsFull`, `PortInUse`,
/// `Unaddressable`, `InvalidState`) arrive after a round trip, in a later receive step.
#[derive(Clone)]
pub struct NetHandle {
    shared: Shared,
}

/// A socket operation failed. The same type on both backends.
///
/// `Copy`, with `Display` and `std::error::Error`. The enum is `#[repr(u32)]` with fixed
/// discriminants, so `error as u32` is a stable code.
///
/// | Code | Variant | `Display` |
/// |---|---|---|
/// | 1 | [`SocketsFull`](Error::SocketsFull) | no free socket slot |
/// | 2 | [`PortInUse`](Error::PortInUse) | port in use |
/// | 3 | [`InvalidConfig`](Error::InvalidConfig) | invalid socket configuration |
/// | 4 | [`Unaddressable`](Error::Unaddressable) | unaddressable endpoint |
/// | 5 | [`Refused`](Error::Refused) | connection refused |
/// | 6 | [`Closed`](Error::Closed) | connection closed |
/// | 7 | [`Finished`](Error::Finished) | receive finished |
/// | 8 | [`InvalidState`](Error::InvalidState) | invalid socket state |
/// | 9 | [`TooLarge`](Error::TooLarge) | payload too large |
/// | 10 | [`Full`](Error::Full) | send queue full |
/// | 11 | [`Empty`](Error::Empty) | empty frame |
/// | 12 | [`Foreign`](Error::Foreign) | frame belongs to another Net |
/// | 13 | [`NoDestination`](Error::NoDestination) | no destination |
/// | 14 | [`PeerMismatch`](Error::PeerMismatch) | destination differs from connected peer |
/// | 15 | [`NoRoute`](Error::NoRoute) | no route |
/// | 16 | [`NeighborPending`](Error::NeighborPending) | neighbor resolution pending |
/// | 17 | [`HostLost`](Error::HostLost) | junction host session lost |
#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    /// No free slot in the socket table, from any opening call. On the local backend a dropped
    /// TCP connection keeps its slot until it finishes closing, so a program that closes and
    /// reconnects quickly can see this below the limit. On the junction backend it comes from
    /// the session at once or, when the daemon has no room, after a round trip.
    SocketsFull = 1,
    /// The local endpoint is taken: `tcp_connect` when a connection already uses the same local
    /// and remote endpoints, `udp_bind` when the address (or all-IP) and port are already bound.
    /// On the junction backend the daemon closes dropped sockets asynchronously, so binding a
    /// port right after dropping its socket can return this.
    PortInUse = 2,
    /// `udp_bind` with port 0 or `rx_queue` 0, on both backends. On the junction backend also
    /// a `tcp::Config` with `mss_cap: Some(0)`, `rx_capacity` above 1 GiB, `tx_capacity` 0 or a
    /// size that does not fit in `u32`, and a `UdpConfig::rx_queue` that does not fit in `u32`.
    /// The local backend panics on those TCP settings instead (see
    /// [`NetHandle::tcp_connect`]).
    InvalidConfig = 3,
    /// An endpoint cannot be used: a local or remote port of 0, an unspecified remote address,
    /// or a local address the Net does not own, from the opening calls. Also from a local
    /// `UdpTx::send` to an unusable destination. On the junction backend opening errors of this
    /// kind come from the daemon after a round trip.
    Unaddressable = 4,
    /// `tcp_connect`: the peer reset the connection attempt.
    Refused = 5,
    /// `tcp_connect`: the attempt ended without a reset from the peer.
    Closed = 6,
    /// `TcpRx::recv`: the peer sent FIN and did not reset the connection, and all data before it
    /// was received. Sending may continue.
    Finished = 7,
    /// The socket is in no state for the operation: `TcpRx::recv` after a reset (even one that
    /// followed a FIN) or in a state without receive; `TcpTx::room` and `try_send` after a reset,
    /// close or `shutdown`; an opening call the Stack refuses. On the junction backend also
    /// `UdpTx::send` on a closed socket, and TCP `recv` (after queued data), `room`, `try_send`
    /// and UDP `send` once the session is lost.
    InvalidState = 8,
    /// The data can never fit: `TcpTx::room` with `len` above `send_capacity()` or `u32::MAX`,
    /// `UdpTx::send` of a datagram larger than the path allows, or on the junction backend a
    /// frame larger than the session's largest record.
    TooLarge = 9,
    /// No room now; the frame comes back with the error. TCP: wait for `room` and retry. UDP:
    /// retry after a later send step. Locally, UDP returns it when the queue of outgoing packets
    /// is full or a shared frame needs a copy and the frame pool is empty. On the junction
    /// backend it means the send ring is full, and `room` on every socket of the `Net` then
    /// waits until the next receive step.
    Full = 10,
    /// `TcpTx::try_send` of a zero-length frame. An empty UDP datagram is valid.
    Empty = 11,
    /// `try_send` or `UdpTx::send` of a frame from another `Net`'s pool; checked before any
    /// other error. [`TcpTx::is_on`] tells whether a handle's frames belong to a stream.
    Foreign = 12,
    /// `UdpTx::send` with `to: None`: UDP sockets here have no default destination.
    NoDestination = 13,
    /// Not returned: this module has no connected UDP sockets. The code stays reserved.
    PeerMismatch = 14,
    /// Local `UdpTx::send`: the Stack has no route to the destination. On the junction backend
    /// the daemon drops such datagrams without reporting.
    NoRoute = 15,
    /// Local `UdpTx::send`: the destination's hardware address is not known yet. Keep the frame
    /// and retry after a later receive step has taken in the ARP reply. On the junction backend
    /// the daemon drops such datagrams without reporting.
    NeighborPending = 16,
    /// Junction backend: the session with the daemon is gone. Returned by opens that were
    /// waiting and by every later `tcp_connect`, `tcp_accept` and `udp_bind`. After the loss,
    /// TCP `recv` returns queued data and then `InvalidState`; `room`, `try_send` and UDP `send`
    /// return `InvalidState`; `delivered` resolves; UDP `recv` waits forever; `tx_frame` still
    /// works.
    HostLost = 17,
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::SocketsFull => "no free socket slot",
            Self::PortInUse => "port in use",
            Self::InvalidConfig => "invalid socket configuration",
            Self::Unaddressable => "unaddressable endpoint",
            Self::Refused => "connection refused",
            Self::Closed => "connection closed",
            Self::Finished => "receive finished",
            Self::InvalidState => "invalid socket state",
            Self::TooLarge => "payload too large",
            Self::Full => "send queue full",
            Self::Empty => "empty frame",
            Self::Foreign => "frame belongs to another Net",
            Self::NoDestination => "no destination",
            Self::PeerMismatch => "destination differs from connected peer",
            Self::NoRoute => "no route",
            Self::NeighborPending => "neighbor resolution pending",
            Self::HostLost => "junction host session lost",
        })
    }
}

impl std::error::Error for Error {}

impl From<AddTcpError> for Error {
    fn from(error: AddTcpError) -> Self {
        match error {
            AddTcpError::SocketsFull(_) => Self::SocketsFull,
        }
    }
}

impl From<AddUdpError> for Error {
    fn from(error: AddUdpError) -> Self {
        match error {
            AddUdpError::Full => Self::SocketsFull,
            AddUdpError::PortInUse => Self::PortInUse,
            AddUdpError::InvalidConfig(_) => Self::InvalidConfig,
            AddUdpError::Unaddressable => Self::Unaddressable,
        }
    }
}

impl From<tcp::ConnectError> for Error {
    fn from(error: tcp::ConnectError) -> Self {
        match error {
            tcp::ConnectError::InvalidState => Self::InvalidState,
            tcp::ConnectError::Unaddressable => Self::Unaddressable,
            tcp::ConnectError::AddressInUse => Self::PortInUse,
        }
    }
}

impl From<tcp::ListenError> for Error {
    fn from(error: tcp::ListenError) -> Self {
        match error {
            tcp::ListenError::InvalidState => Self::InvalidState,
            tcp::ListenError::Unaddressable => Self::Unaddressable,
        }
    }
}

impl From<tcp::RecvError> for Error {
    fn from(error: tcp::RecvError) -> Self {
        match error {
            tcp::RecvError::InvalidState => Self::InvalidState,
            tcp::RecvError::Finished => Self::Finished,
        }
    }
}

impl From<tcp::SendFrameError> for Error {
    fn from(error: tcp::SendFrameError) -> Self {
        match error {
            tcp::SendFrameError::InvalidState => Self::InvalidState,
            tcp::SendFrameError::Foreign => Self::Foreign,
            tcp::SendFrameError::Empty => Self::Empty,
            tcp::SendFrameError::Full => Self::Full,
        }
    }
}

impl From<UdpSendError> for Error {
    fn from(error: UdpSendError) -> Self {
        match error {
            UdpSendError::NoDestination => Self::NoDestination,
            UdpSendError::PeerMismatch => Self::PeerMismatch,
            UdpSendError::Unaddressable => Self::Unaddressable,
            UdpSendError::TooLarge => Self::TooLarge,
            UdpSendError::NoRoute => Self::NoRoute,
            UdpSendError::NeighborPending => Self::NeighborPending,
            UdpSendError::WouldBlock => Self::Full,
        }
    }
}

impl From<UdpSendFrameError> for Error {
    fn from(error: UdpSendFrameError) -> Self {
        match error {
            UdpSendFrameError::Udp(error) => error.into(),
            UdpSendFrameError::Foreign => Self::Foreign,
        }
    }
}

impl NetHandle {
    /// Opens a TCP connection from `local` to `remote`.
    ///
    /// The future resolves to the [`TcpStream`] once the handshake completes, to
    /// [`Error::Refused`] if the peer reset the attempt, or to [`Error::Closed`] if it ended
    /// another way. Other errors: `SocketsFull`, `PortInUse`, `Unaddressable`, `InvalidState`,
    /// and on the junction backend `InvalidConfig` and `HostLost`.
    ///
    /// `local.port` must be nonzero: there is no ephemeral port allocator. `local.addr: None`
    /// uses any of the Net's addresses. A port of 0 on either side, an unspecified remote
    /// address or a local address the Net does not own gives `Unaddressable`.
    ///
    /// # Panics
    ///
    /// On the local backend, at the call, if `config.rx_capacity` exceeds 1 GiB or
    /// `config.mss_cap` is `Some(0)`. The junction backend returns `InvalidConfig` instead.
    pub fn tcp_connect(
        &self,
        config: tcp::Config,
        remote: IpEndpoint,
        local: IpListenEndpoint,
    ) -> Connecting {
        let index = {
            let mut inner = self.shared.borrow_mut();
            match &mut *inner {
                Inner::Remote(backend) => Some(backend.connect(config, remote, local)),
                Inner::Local(_) => None,
            }
        };
        if let Some(index) = index {
            return Connecting { opening: Some(index.map(|index| self.opening(index))) };
        }
        Connecting {
            opening: Some(self.open_tcp(config, |stack, handle| {
                stack.connect(handle, remote, local).map_err(Error::from)
            })),
        }
    }

    /// Opens a TCP socket listening on `local`; it becomes one accepted connection.
    ///
    /// There is no accept queue. To serve many connections on a port, call `tcp_accept` again
    /// as soon as one completes. Several sockets may listen on one port. On the local backend a
    /// SYN goes to a listener bound to its destination address if one exists, otherwise to an
    /// all-IP listener (`addr: None`) on the port; among several in the chosen group, the one in
    /// the lowest slot takes it.
    ///
    /// A reset during the handshake returns the socket to listening and the future keeps
    /// waiting. If the connection already closed when the future completes, it still resolves
    /// to a stream: data the peer sent is readable, then `recv` reports the end, and
    /// [`TcpTx::max_segment_payload`] is `None`.
    ///
    /// Errors: `Unaddressable` for port 0, `SocketsFull`, `InvalidState`, and on the junction
    /// backend `InvalidConfig` and `HostLost`.
    ///
    /// # Panics
    ///
    /// As [`NetHandle::tcp_connect`].
    pub fn tcp_accept(&self, config: tcp::Config, local: IpListenEndpoint) -> Accepting {
        let index = {
            let mut inner = self.shared.borrow_mut();
            match &mut *inner {
                Inner::Remote(remote) => Some(remote.accept(config, local)),
                Inner::Local(_) => None,
            }
        };
        if let Some(index) = index {
            return Accepting { opening: Some(index.map(|index| self.opening(index))) };
        }
        Accepting {
            opening: Some(self.open_tcp(config, |stack, handle| {
                stack.listen(handle, local).map_err(Error::from)
            })),
        }
    }

    fn open_tcp(
        &self,
        config: tcp::Config,
        start: impl FnOnce(&mut Stack, &TcpHandle) -> Result<(), Error>,
    ) -> Result<Opening, Error> {
        let mut inner = self.shared.borrow_mut();
        let Inner::Local(inner) = &mut *inner else { unreachable!() };
        let handle = inner.stack.add_tcp(config).map_err(Error::from)?;
        if let Err(error) = start(&mut inner.stack, &handle) {
            inner.stack.remove_tcp(handle);
            return Err(error);
        }
        let key = inner.stack.tcp_ready_key(&handle);
        let index = inner.insert(Socket::Tcp(handle, key));
        Ok(self.opening(index))
    }

    fn opening(&self, index: u32) -> Opening {
        Opening {
            rx: TcpRx(Half { shared: self.shared.clone(), index, side: RX }),
            tx: Half { shared: self.shared.clone(), index, side: TX },
        }
    }

    /// Binds a UDP socket as `config` says. The future resolves on its first poll on the local
    /// backend, and once the daemon confirms on the junction backend.
    ///
    /// Errors, local order: `InvalidConfig` (port 0 or `rx_queue` 0), `Unaddressable`
    /// (`local_addr` is not a unicast address the Net owns), `PortInUse` (that address, or
    /// all-IP, already bound on the port), `SocketsFull`. The junction backend returns the same
    /// kinds, plus `HostLost`.
    pub fn udp_bind(&self, config: UdpConfig) -> Binding {
        Binding { socket: Some(self.open_udp(config)) }
    }

    fn open_udp(&self, config: UdpConfig) -> Result<UdpSocket, Error> {
        let mut inner = self.shared.borrow_mut();
        let index = match &mut *inner {
            Inner::Remote(remote) => remote.bind(config)?,
            Inner::Local(inner) => {
                let handle = inner.stack.add_udp(config).map_err(Error::from)?;
                let key = inner.stack.udp_ready_key(&handle);
                inner.insert(Socket::Udp(handle, key))
            }
        };
        Ok(UdpSocket {
            rx: UdpRx(Half { shared: self.shared.clone(), index, side: RX }),
            tx: UdpTx(Half { shared: self.shared.clone(), index, side: TX }),
        })
    }

    /// Waits for an empty frame to fill and send: write through `as_mut()`, `truncate` to the
    /// length, then `freeze` for [`TcpTx::try_send`] or [`UdpTx::send`]. Dropping a frame
    /// returns it to its pool.
    ///
    /// | Backend | Pool | `len()` |
    /// |---|---|---|
    /// | local | the Stack's frame pool, shared by every socket; a sent TCP frame stays in its socket until the peer acknowledges it | IP MTU − 28, the largest UDP payload |
    /// | junction | the Net's own [`RemoteConfig::tx_frames`] buffers; a sent frame is copied and returns at once | `Session::frame_capacity()` |
    ///
    /// While the pool is empty the task waits in line. Each [`Net::receive`] wakes as many
    /// waiting tasks, oldest first, as the pool has free frames; one that finds its frame taken
    /// by another task waits again at the back.
    pub fn tx_frame(&self) -> TxFrameWait<'_> {
        TxFrameWait { handle: self }
    }
}

/// Future from [`NetHandle::tx_frame`]; resolves to an [`OwnedFrame`].
pub struct TxFrameWait<'a> {
    handle: &'a NetHandle,
}

impl Future for TxFrameWait<'_> {
    type Output = OwnedFrame;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<OwnedFrame> {
        let mut inner = self.handle.shared.borrow_mut();
        let Inner::Local(inner) = &mut *inner else {
            let Inner::Remote(remote) = &mut *inner else { unreachable!() };
            return remote.tx_frame(cx);
        };
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
            inner.frame_waiters.push_back(waker);
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

/// A TCP socket not yet handed to its task.
struct Opening {
    rx: TcpRx,
    tx: Half,
}

impl Opening {
    fn finish(self, mss: Option<usize>) -> TcpStream {
        TcpStream { rx: self.rx, tx: TcpTx(self.tx, mss) }
    }
}

/// Future from [`NetHandle::tcp_connect`]; resolves to `Result<TcpStream, Error>`. Dropping it
/// before it resolves drops the socket.
///
/// # Panics
///
/// If polled again after it resolved.
pub struct Connecting {
    opening: Option<Result<Opening, Error>>,
}

impl Future for Connecting {
    type Output = Result<TcpStream, Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let opening = match this.opening.as_ref().expect("Connecting polled after completion") {
            Ok(opening) => opening,
            Err(error) => {
                let error = *error;
                this.opening.take();
                return Poll::Ready(Err(error));
            }
        };
        let half = &opening.rx.0;
        let index = half.index;
        let state = poll_op(&half.shared, index, Interest::Connect, cx, |inner| {
            let Inner::Local(inner) = inner else {
                let Inner::Remote(remote) = inner else { unreachable!() };
                return remote.opened(index, false);
            };
            let (stack, handle) = inner.tcp(index);
            let socket = stack.tcp(handle);
            match socket.state() {
                state if connecting(state) => None,
                tcp::State::Closed => Some(Err(if socket.reset_by_peer() { Error::Refused } else { Error::Closed })),
                _ => Some(Ok(socket.max_segment_payload())),
            }
        });
        state.map(|result| {
            let opening = this.opening.take().expect("opening present until completion");
            result.map(|mss| opening.expect("socket opened").finish(mss))
        })
    }
}

/// Future from [`NetHandle::tcp_accept`]; resolves to `Result<TcpStream, Error>`. Dropping it
/// before it resolves drops the socket.
///
/// # Panics
///
/// If polled again after it resolved.
pub struct Accepting {
    opening: Option<Result<Opening, Error>>,
}

impl Future for Accepting {
    type Output = Result<TcpStream, Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let opening = match this.opening.as_ref().expect("Accepting polled after completion") {
            Ok(opening) => opening,
            Err(error) => {
                let error = *error;
                this.opening.take();
                return Poll::Ready(Err(error));
            }
        };
        let half = &opening.rx.0;
        let index = half.index;
        let state = poll_op(&half.shared, index, Interest::Accept, cx, |inner| {
            let Inner::Local(inner) = inner else {
                let Inner::Remote(remote) = inner else { unreachable!() };
                return remote.opened(index, true);
            };
            let (stack, handle) = inner.tcp(index);
            let socket = stack.tcp(handle);
            (!accepting(socket.state())).then(|| Ok(socket.max_segment_payload()))
        });
        state.map(|result| {
            let opening = this.opening.take().expect("opening present until completion");
            result.map(|mss| opening.expect("socket opened").finish(mss))
        })
    }
}

/// Future from [`NetHandle::udp_bind`]; resolves to `Result<UdpSocket, Error>`. Dropping it
/// before it resolves drops the socket.
///
/// # Panics
///
/// If polled again after it resolved.
pub struct Binding {
    socket: Option<Result<UdpSocket, Error>>,
}

impl Future for Binding {
    type Output = Result<UdpSocket, Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if let Some(Ok(socket)) = &this.socket {
            let half = &socket.rx.0;
            let index = half.index;
            let bound = poll_op(&half.shared, index, Interest::Bind, cx, |inner| {
                match inner {
                    Inner::Local(_) => Some(Ok(())),
                    Inner::Remote(remote) => remote.bound(index),
                }
            });
            return bound.map(|result| {
                let socket = this.socket.take().expect("binding present until completion");
                result.and(socket)
            });
        }
        Poll::Ready(this.socket.take().expect("Binding polled after completion"))
    }
}

/// An open TCP connection. [`split`](TcpStream::split) gives a receive half and a send half;
/// each half allows one waiting operation at a time.
///
/// Dropping both halves closes the socket like Linux `close()`: a reset if received data is
/// unread, otherwise a FIN. Dropping only one half leaves the other working. On the local
/// backend the slot stays taken until the connection finishes closing; on the junction backend
/// the daemon closes it asynchronously.
pub struct TcpStream {
    rx: TcpRx,
    tx: TcpTx,
}

impl TcpStream {
    /// The receive and send halves.
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
    /// Waits for the next in-order [`TcpFrame`].
    ///
    /// Data already received is delivered first, also after a reset or a lost session. Then:
    ///
    /// - [`Error::Finished`] after the peer's FIN, with no reset.
    /// - [`Error::InvalidState`] after the peer's reset (even one that followed a FIN), in a
    ///   state without receive, or on the junction backend once the session is lost.
    ///
    /// Dropping the future loses no data.
    pub fn recv(&mut self) -> Recv<'_> {
        Recv { rx: self }
    }
}

/// Future from [`TcpRx::recv`]; resolves to `Result<TcpFrame, Error>`.
pub struct Recv<'a> {
    rx: &'a mut TcpRx,
}

impl Future for Recv<'_> {
    type Output = Result<TcpFrame, Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let half = &self.rx.0;
        let index = half.index;
        poll_op(&half.shared, index, Interest::TcpRecv, cx, |inner| {
            let Inner::Local(inner) = inner else {
                let Inner::Remote(remote) = inner else { unreachable!() };
                return remote.recv(index);
            };
            let (stack, handle) = inner.tcp(index);
            // `tcp_mut` marks the socket for egress; take it only to pop.
            if !tcp_recv_ready(stack.tcp(handle)) {
                return None;
            }
            let socket = stack.tcp_mut(handle);
            match socket.recv() {
                Ok(None) => None,
                Ok(Some(frame)) => Some(Ok(TcpFrame::local(frame))),
                // a FIN followed by the peer's reset ends in the reset: every caller sees the
                // connection's death, not a clean end of the stream
                Err(tcp::RecvError::Finished) if socket.reset_by_peer() => Some(Err(Error::InvalidState)),
                Err(error) => Some(Err(error.into())),
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
pub struct TcpTx(Half, Option<usize>);

impl TcpTx {
    /// Waits until a frame of `len` bytes can be queued with [`try_send`](TcpTx::try_send).
    ///
    /// Resolves to:
    ///
    /// - `Err(TooLarge)` on the first poll if `len` exceeds [`send_capacity`](TcpTx::send_capacity)
    ///   or `u32::MAX`; it would never fit. Checked before the socket state.
    /// - `Err(InvalidState)` once the socket cannot send: reset, closed, after
    ///   [`shutdown`](TcpTx::shutdown), or on the junction backend once the session is lost.
    /// - `Ok(())` otherwise, with a different promise per backend. Local: `len` bytes fit in the
    ///   socket's free send buffer, and a `try_send` of a frame that size succeeds until the next
    ///   [`Net::receive`]. Junction: a hint only. It waits only after some socket of this `Net`
    ///   got [`Error::Full`] since the last receive step, and `try_send` can still return `Full`.
    pub fn room(&mut self, len: usize) -> Room<'_> {
        Room { tx: self, len }
    }

    /// Queues `frame` without waiting; it leaves in the next [`Net::send`].
    ///
    /// Errors, checked in this order, each returning the same frame: `Foreign` (the frame is
    /// from another `Net`), `InvalidState` (the socket cannot send), `Empty` (zero length),
    /// `Full` (no room now; wait for [`room`](TcpTx::room) and retry). On the junction backend
    /// `Full` means the send ring is full, and `TooLarge` that the frame exceeds the session's
    /// largest record.
    ///
    /// Local backend: the socket holds the frame until the peer acknowledges its last byte. A
    /// frame that fits into the end of the previous unsent frame within one segment is copied
    /// there and freed at once. A frame longer than the segment size goes out in several
    /// segments. Junction backend: the bytes are copied into the send ring and the frame returns
    /// to the pool at once.
    pub fn try_send(&mut self, frame: ImmutableFrame) -> Result<(), (ImmutableFrame, Error)> {
        let mut inner = self.0.shared.borrow_mut();
        let Inner::Local(inner) = &mut *inner else {
            let Inner::Remote(remote) = &mut *inner else { unreachable!() };
            return remote.send_tcp(self.0.index, frame);
        };
        let (stack, handle) = inner.tcp(self.0.index);
        stack.tcp_send_frame(handle, frame).map_err(|(frame, error)| (frame, error.into()))?;
        inner.handed_over();
        Ok(())
    }

    /// The largest `len` [`room`](TcpTx::room) accepts. Local: the socket's send buffer,
    /// `tcp::Config::tx_capacity`. Junction: the session's largest record minus 16 bytes.
    pub fn send_capacity(&self) -> usize {
        let mut inner = self.0.shared.borrow_mut();
        let Inner::Local(inner) = &mut *inner else {
            let Inner::Remote(remote) = &mut *inner else { unreachable!() };
            return remote.send_capacity();
        };
        let (stack, handle) = inner.tcp(self.0.index);
        stack.tcp(handle).send_capacity()
    }

    /// The segment size the connection settled on, read when the stream was handed to the task.
    /// `None` if an accepted connection had already closed by then; otherwise it stays `Some`
    /// after close or reset. On the local backend, filling a frame with at most this many
    /// bytes keeps it to one segment.
    pub fn max_segment_payload(&self) -> Option<usize> {
        self.1
    }

    /// Whether this stream is on `handle`'s `Net`, so frames from `handle` are its own and do
    /// not give [`Error::Foreign`].
    pub fn is_on(&self, handle: &NetHandle) -> bool {
        Rc::ptr_eq(&self.0.shared, &handle.shared)
    }

    /// Half-close: a FIN after the queued data. Receiving continues; [`room`](TcpTx::room) and
    /// [`try_send`](TcpTx::try_send) return `InvalidState` from now on. Calling it again does
    /// nothing.
    pub fn shutdown(&mut self) {
        let mut inner = self.0.shared.borrow_mut();
        let Inner::Local(inner) = &mut *inner else {
            let Inner::Remote(remote) = &mut *inner else { unreachable!() };
            remote.shutdown(self.0.index);
            return;
        };
        let (stack, handle) = inner.tcp(self.0.index);
        stack.close(handle);
    }

    /// Waits until the peer has acknowledged every byte and the FIN after
    /// [`shutdown`](TcpTx::shutdown), or the connection closed and nothing more can be delivered;
    /// on the junction backend also once the session is lost. Until then the queued bytes exist
    /// only in the socket: a local Stack torn down earlier never sends them.
    pub fn delivered(&mut self) -> Delivered<'_> {
        Delivered { tx: self }
    }
}

/// Future from [`TcpTx::delivered`].
pub struct Delivered<'a> {
    tx: &'a mut TcpTx,
}

impl Future for Delivered<'_> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let half = &self.tx.0;
        let index = half.index;
        poll_op(&half.shared, index, Interest::TcpDelivered, cx, |inner| {
            let Inner::Local(inner) = inner else {
                let Inner::Remote(remote) = inner else { unreachable!() };
                return remote.delivered(index);
            };
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

/// Future from [`TcpTx::room`]; resolves to `Result<(), Error>`.
pub struct Room<'a> {
    tx: &'a mut TcpTx,
    len: usize,
}

impl Future for Room<'_> {
    type Output = Result<(), Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let (half, len) = (&self.tx.0, self.len);
        let index = half.index;
        let Ok(interest) = u32::try_from(len) else {
            return Poll::Ready(Err(Error::TooLarge));
        };
        poll_op(&half.shared, index, Interest::TcpRoom(interest), cx, |inner| {
            let Inner::Local(inner) = inner else {
                let Inner::Remote(remote) = inner else { unreachable!() };
                return remote.room(index, len);
            };
            let (stack, handle) = inner.tcp(index);
            let socket = stack.tcp(handle);
            let capacity = socket.send_capacity();
            if len > capacity {
                Some(Err(Error::TooLarge))
            } else if !socket.may_send() {
                Some(Err(Error::InvalidState))
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

/// Received in-order TCP data.
///
/// The frame stays valid after its socket, the `Net` and the Stack are gone. While held it
/// keeps its buffer: on the local backend a buffer of the Stack's RX pool (the TCP window does
/// not shrink, so later data still arrives); on the junction backend space in the session's
/// receive ring, shared by every socket of the session.
#[derive(Debug)]
pub struct TcpFrame {
    frame: OwnedFrame,
    rx_timestamp: u64,
}

impl TcpFrame {
    fn local(frame: dpdk_smoltcp::TcpFrame) -> Self {
        Self { rx_timestamp: frame.rx_timestamp(), frame: frame.into_frame() }
    }

    /// The received bytes.
    pub fn payload(&self) -> &[u8] {
        &self.frame
    }

    /// The received bytes, writable.
    pub fn payload_mut(&mut self) -> &mut [u8] {
        &mut self.frame
    }

    /// The NIC's receive timestamp of the packet that carried the data, raw in the driver's
    /// units; 0 when there is none.
    pub fn rx_timestamp(&self) -> u64 {
        self.rx_timestamp
    }

    /// The payload as a plain [`OwnedFrame`], which can be frozen and cloned without copying.
    /// Read [`rx_timestamp`](TcpFrame::rx_timestamp) first; the frame does not carry it.
    pub fn into_frame(self) -> OwnedFrame {
        self.frame
    }
}

/// A bound UDP socket. [`split`](UdpSocket::split) gives a receive half and a send half.
/// Dropping both unbinds the port; on the junction backend the daemon does that
/// asynchronously.
pub struct UdpSocket {
    rx: UdpRx,
    tx: UdpTx,
}

impl UdpSocket {
    /// The receive and send halves.
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
    /// Waits for the next queued [`Datagram`]; never fails. On the junction backend it keeps
    /// waiting after the session is lost.
    pub fn recv(&mut self) -> UdpRecv<'_> {
        UdpRecv { rx: self }
    }
}

/// The send half of a UDP socket.
pub struct UdpTx(Half);

impl UdpTx {
    /// Sends `frame` as one datagram to `to`; it leaves in the next [`Net::send`]. A zero-length
    /// frame is an empty datagram. Every error returns the same frame.
    ///
    /// | Backend | Errors |
    /// |---|---|
    /// | local | `Foreign`, then `NoDestination`, `Unaddressable`, `TooLarge`, `Full` (no free frame to copy a shared frame into), `NoRoute`, `NeighborPending`, `Full` (outgoing queue full) |
    /// | junction | `Foreign`, `InvalidState` (socket closed or session lost), `NoDestination`, `Full` (send ring full), `TooLarge` |
    ///
    /// `to: None` always gives `NoDestination`. Retry `Full` after a later send step and
    /// `NeighborPending` after a later receive step. On the local backend a frame held only
    /// here is sent in place; a cloned frame is copied into a new pool frame first. On the
    /// junction backend every send is copied into the send ring, and the daemon drops a datagram
    /// it cannot route, resolve or address without reporting it.
    pub fn send(&mut self, to: Option<IpEndpoint>, frame: ImmutableFrame) -> Result<(), (ImmutableFrame, Error)> {
        let mut inner = self.0.shared.borrow_mut();
        let Inner::Local(inner) = &mut *inner else {
            let Inner::Remote(remote) = &mut *inner else { unreachable!() };
            return remote.send_udp(self.0.index, to, frame);
        };
        let (stack, handle) = inner.udp(self.0.index);
        stack.udp_send_frame(handle, to, frame).map_err(|(frame, error)| (frame, error.into()))?;
        inner.handed_over();
        Ok(())
    }

    /// The socket's receive drop counters; `rx_dropped_full` counts datagrams dropped because
    /// `UdpConfig::rx_queue` was full. On the local backend that queue counts datagrams waiting
    /// for `recv` and those the program still holds. On the junction backend the daemon applies
    /// `rx_queue` itself and this always returns zeros. Borrows the whole `Net`: keep it off a
    /// hot path.
    pub fn stats(&self) -> UdpSocketStats {
        let mut inner = self.0.shared.borrow_mut();
        let Inner::Local(inner) = &mut *inner else {
            let Inner::Remote(remote) = &mut *inner else { unreachable!() };
            return remote.stats(self.0.index);
        };
        let (stack, handle) = inner.udp(self.0.index);
        stack.udp(handle).stats()
    }
}

/// Future from [`UdpRx::recv`]; resolves to a [`Datagram`].
pub struct UdpRecv<'a> {
    rx: &'a mut UdpRx,
}

impl Future for UdpRecv<'_> {
    type Output = Datagram;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Datagram> {
        let half = &self.rx.0;
        let index = half.index;
        poll_op(&half.shared, index, Interest::UdpRecv, cx, |inner| {
            let Inner::Local(inner) = inner else {
                let Inner::Remote(remote) = inner else { unreachable!() };
                return remote.recv_udp(index);
            };
            let (stack, handle) = inner.udp(index);
            stack.udp_mut(handle).recv().map(Datagram::local)
        })
    }
}

impl Drop for UdpRecv<'_> {
    fn drop(&mut self) {
        self.rx.0.shared.borrow_mut().unpark(self.rx.0.index, RX);
    }
}

/// A received UDP datagram.
///
/// The frame stays valid after its socket, the `Net` and the Stack are gone. While held it
/// keeps its buffer: on the local backend a buffer of the Stack's RX pool and one place in the
/// socket's `rx_queue`; on the junction backend space in the session's receive ring.
#[derive(Debug)]
pub struct Datagram {
    frame: OwnedFrame,
    src: IpEndpoint,
    dst_addr: Ipv4Address,
    rx_timestamp: u64,
}

impl Datagram {
    fn local(frame: dpdk_smoltcp::UdpDatagram) -> Self {
        Self { src: frame.src(), dst_addr: frame.dst_addr(), rx_timestamp: frame.rx_timestamp(), frame: frame.into_frame() }
    }

    /// The datagram's payload.
    pub fn payload(&self) -> &[u8] {
        &self.frame
    }

    /// The datagram's payload, writable.
    pub fn payload_mut(&mut self) -> &mut [u8] {
        &mut self.frame
    }

    /// The sender's endpoint.
    pub fn src(&self) -> IpEndpoint {
        self.src
    }

    /// The address the datagram was sent to.
    pub fn dst_addr(&self) -> Ipv4Address {
        self.dst_addr
    }

    /// The NIC's receive timestamp, raw in the driver's units; 0 when there is none.
    pub fn rx_timestamp(&self) -> u64 {
        self.rx_timestamp
    }

    /// The payload as a plain [`OwnedFrame`]. Read the addresses and timestamp first; the frame
    /// does not carry them.
    pub fn into_frame(self) -> OwnedFrame {
        self.frame
    }
}
