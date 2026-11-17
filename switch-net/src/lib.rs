//! Async TCP and UDP sockets for tasks on the switch runtime.
//!
//! The loop that drives the program owns a [`Net`] and moves packets with [`round`]; tasks hold a
//! [`NetHandle`] to open sockets and get frames to send. A `Net` either owns the card
//! ([`Net::new`]) or shares a card through the daemon ([`Net::remote`]); task code is the same
//! for both. Applications use this crate as `switch::net`.

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
/// TCP socket configuration and protocol types. See [`TcpStream`] for TCP timer behavior.
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
    /// When owning the card: the network stack failed, for example
    /// `StackError::TimeWentBackwards` when `now` is earlier than the previous receive's,
    /// or a card error.
    Stack(StackError),
    /// When sharing a card: the session with the daemon ended. The first error carries the reason;
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
    /// Frames handed to the network stack since the last successful send.
    tx_handed: usize,
    /// `Stack::tx_burst`: from this many `tx_handed` on, each handed frame requests a turn.
    tx_burst: usize,
}

impl Local {
    /// One frame handed to the network stack: at one TX burst since the last send, end the
    /// running `Runtime::poll` after this task (`switch_runtime::request_turn`), so the loop
    /// attempts transmission sooner.
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

    /// Resets the TCP socket of entry `index` now, before its halves drop. Sharing a card,
    /// dropping the halves then sends no `Close`, which the daemon would hold until every
    /// send ring is empty.
    fn abort(&mut self, index: u32) {
        match self {
            Self::Local(local) => {
                let (stack, handle) = local.tcp(index);
                stack.abort(handle);
            }
            Self::Remote(remote) => remote.abort(index),
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
/// [`handle`](Net::handle). Choose how the program gets its card:
///
/// | | Owning the card | Sharing a card |
/// |---|---|---|
/// | Constructor | [`Net::new`] | [`Net::remote`] |
/// | Card owned by | this process; the `Net` holds the network stack | the daemon; the `Net` holds a [`Session`](switch_junction::session::Session) |
/// | Socket table | `max_sockets` TCP plus `max_udp_sockets` UDP sockets of the network stack | `Session::max_sockets` sockets of either kind |
/// | TCP configuration | `rx_capacity` above 1 GiB or `mss_cap: Some(0)` panics | those settings, zero `tx_capacity`, or capacities that do not fit in `u32` return `InvalidConfig` |
/// | Send copies | see [`TcpTx::try_send`] and [`UdpTx::send`] | into shared memory, then into the daemon's send buffers; TCP backlog can add another copy |
/// | Teardown | [`Net::into_stack`] | [`Net::into_session`] |
///
/// Other differences are described on [`Net::receive`], [`Net::send`],
/// [`Net::poll_at`], [`NetHandle::tx_frame`], [`TcpTx::room`], [`TcpTx::try_send`],
/// [`TcpTx::send_capacity`], [`UdpTx::send`], [`UdpTx::stats`] and [`Error`].
///
/// `Net`, its handles, sockets and frames are not `Send`; keep them on the thread running
/// the loop. Every future it returns must be polled by a task of the switch runtime: under
/// any other executor, the first poll that returns `Pending` panics with
/// "StoredWaker needs a switch task waker".
///
/// Dropping the `Net` while handles or sockets are alive keeps the network stack or session
/// alive in them, but nothing receives or sends any more: waiting tasks are never woken.
pub struct Net {
    shared: Shared,
}

impl Net {
    /// A `Net` that owns its card through the supplied network stack.
    ///
    /// The socket table has room for the network stack's `max_sockets` TCP and
    /// `max_udp_sockets` UDP sockets.
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

    /// A `Net` that shares a card through a session with the daemon.
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
    /// | | Owning the card | Sharing a card |
    /// |---|---|---|
    /// | Work | [`Stack::receive`] at `now` with [`NetConfig::ingress_budget`]; offers immediate replies: resets, immediate and challenge ACKs, and ARP replies; delayed ACKs wait for `send` | polls the session once: sends pending control messages, applies the daemon's events, collects received packets; `now` is unused |
    /// | Errors | [`TurnError::Stack`] | [`TurnError::HostLost`] |
    ///
    /// The returned [`Turn`] counts:
    ///
    /// | Field | Owning the card | Sharing a card |
    /// |---|---|---|
    /// | `rx_received` | packets the RX burst staged | packets delivered to an open socket |
    /// | `rx_rejected` | received packets dropped as unsupported or with a bad IPv4 header checksum | 0 |
    /// | `rx_processed` | staged packets handed to TCP/IP processing | daemon events plus packets delivered |
    /// | `rx_staged` | packets still staged because the budget ran out; receive again promptly | 0 |
    /// | `tx_accepted` | packets the card accepted during this call | 0 |
    /// | `tx_pending` | packets still waiting for the card after this call | 0 |
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

    /// The send step: attempts to transmit queued data and flush pending packets.
    ///
    /// | | Owning the card | Sharing a card |
    /// |---|---|---|
    /// | Work | [`Stack::send`]: attempts queued TCP data, retransmissions and window updates, then flushes to the card | [`TcpTx::try_send`] and [`UdpTx::send`] already published their send-ring records |
    /// | Returned [`Sent`] | `tx_accepted`: packets the card accepted in the final flush; `tx_pending`: packets still waiting for it | `tx_accepted`: records committed since the last `send`; `tx_pending`: 0 |
    /// | Errors | [`TurnError::Stack`] | [`TurnError::HostLost`] once the session is lost |
    ///
    /// Transmission can wait for the peer's window, congestion control or card capacity.
    /// When owning the card, frames handed over by all tasks are counted since the last
    /// successful `send`. The frame that brings the count to [`Stack::tx_burst`], and every
    /// later one, asks the runtime to end the running poll after its task so the send step
    /// can run sooner.
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

    /// When the network stack's sockets next need a turn ([`Stack::poll_at`]): TCP timers,
    /// segments ready to send and dropped sockets to free. Arriving, staged and pending
    /// packets and ARP probes are not covered, so cap the sleep even when this returns `None`.
    /// A loop that sleeps while idle should wake by this deadline. Always `None` when
    /// sharing a card: the daemon runs TCP's timers.
    pub fn poll_at(&mut self) -> Option<Instant> {
        match &mut *self.shared.borrow_mut() {
            Inner::Local(local) => local.stack.poll_at(),
            Inner::Remote(_) => None,
        }
    }

    /// The [`Stack`] back, for teardown. Returns this `Net` as the error while any
    /// [`NetHandle`], socket half or unfinished opening future is alive, and always when
    /// sharing a card. Received frames do not hold the `Net`; they stay valid after the
    /// network stack is gone.
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
    /// and always when owning the card. The caller then finishes the session
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
#[derive(Clone)]
pub struct NetHandle {
    shared: Shared,
}

/// A socket operation failed.
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
    /// No free slot in the socket table, from any opening call. When owning the card, a dropped
    /// TCP connection keeps its slot until it finishes closing, so a program that closes and
    /// reconnects quickly can see this below the limit. When sharing a card it comes from
    /// the session at once or, when the daemon has no room, after a round trip.
    SocketsFull = 1,
    /// The local endpoint is taken: `tcp_connect` when a connection already uses the same local
    /// and remote endpoints, `udp_bind` when the address (or all-IP) and port are already bound.
    /// When sharing a card, the daemon closes dropped sockets asynchronously, so binding a
    /// port right after dropping its socket can return this.
    PortInUse = 2,
    /// `udp_bind` with port 0 or `rx_queue` 0. When sharing a card, also a `tcp::Config`
    /// with `mss_cap: Some(0)`, `rx_capacity` above 1 GiB, `tx_capacity` 0 or a capacity that
    /// does not fit in `u32`, and a `UdpConfig::rx_queue` that does not fit in `u32`.
    /// When owning the card, `rx_capacity` above 1 GiB and `mss_cap: Some(0)` panic;
    /// a `tx_capacity` of 0 or above `u32::MAX` is accepted.
    InvalidConfig = 3,
    /// An endpoint cannot be used: a local or remote port of 0, an unspecified remote address,
    /// or a local address the Net does not own, from the opening calls. Also from
    /// `UdpTx::send` to an unusable destination when owning the card. When sharing a card,
    /// opening errors of this kind come from the daemon after a round trip.
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
    /// close or `shutdown`; an opening call the network stack refuses. When sharing a card,
    /// also `UdpTx::send` on a closed socket, and TCP `recv` (after queued data), `room`, `try_send`
    /// and UDP `send` once the session is lost.
    InvalidState = 8,
    /// The data can never fit: `TcpTx::room` with `len` above `send_capacity()` or `u32::MAX`,
    /// `UdpTx::send` of a datagram larger than the path allows, or when sharing a card a
    /// frame larger than the session's largest record.
    TooLarge = 9,
    /// No room now; the frame comes back with the error. TCP: wait for `room` and retry.
    /// UDP: retry after later rounds free capacity. When owning the card, UDP returns it when
    /// the outgoing packet queue is full, or a shared or off-start frame needs a copy and the
    /// frame pool is empty. When sharing a card it means the send ring is full, and `room`
    /// on every socket of the `Net` then waits until the next receive step.
    Full = 10,
    /// `TcpTx::try_send` of a zero-length frame. An empty UDP datagram is valid.
    Empty = 11,
    /// `try_send` or `UdpTx::send` of a frame not from this `Net`'s TX-frame pool, including
    /// a received frame from this same `Net`. Checked before any other error.
    /// [`TcpTx::is_on`] checks which `Net` a stream uses.
    Foreign = 12,
    /// `UdpTx::send` with `to: None`: UDP sockets here have no default destination.
    NoDestination = 13,
    /// Reserved; never returned.
    PeerMismatch = 14,
    /// `UdpTx::send` when owning the card: the network stack has no route to the destination.
    /// When sharing a card, the daemon drops such datagrams without reporting.
    NoRoute = 15,
    /// `UdpTx::send` when owning the card: the destination's hardware address is not known
    /// yet. Keep the frame and retry after later receive steps can take in an ARP reply.
    /// A reply is not guaranteed. When sharing a card, the daemon drops such datagrams
    /// without reporting.
    NeighborPending = 16,
    /// When sharing a card: the session with the daemon is gone. Returned by opens that were
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
    /// and when sharing a card `InvalidConfig` and `HostLost`.
    ///
    /// `local.port` must be nonzero: there is no ephemeral port allocator. `local.addr: None`
    /// uses any of the Net's addresses. A port of 0 on either side, an unspecified remote
    /// address or a local address the Net does not own gives `Unaddressable`.
    ///
    /// # Panics
    ///
    /// When owning the card, at the call, if `config.rx_capacity` exceeds 1 GiB or
    /// `config.mss_cap` is `Some(0)`. Sharing a card returns `InvalidConfig` instead.
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
    /// as soon as one completes. Several sockets may listen on one port. When owning the card,
    /// a SYN goes to a listener bound to its destination address if one exists, otherwise to an
    /// all-IP listener (`addr: None`) on the port.
    ///
    /// A reset during the handshake returns the socket to listening and the future keeps
    /// waiting. A handshake the peer does not complete is given up after five SYN-ACK
    /// retransmissions, and the socket listens again. If the connection already closed when
    /// the future completes, it still resolves to a stream: data the peer sent is readable,
    /// then `recv` reports the end, and [`TcpTx::max_segment_payload`] is `None`.
    ///
    /// Errors: `Unaddressable` for port 0, `SocketsFull`, `InvalidState`, and when sharing
    /// a card `InvalidConfig` and `HostLost`.
    ///
    /// # Panics
    ///
    /// As [`NetHandle::tcp_connect`].
    pub fn tcp_accept(&self, config: tcp::Config, local: IpListenEndpoint) -> Accepting {
        Accepting { opening: Some(self.open_listening(config, local)) }
    }

    /// Opens `backlog` TCP sockets listening on `local`, whose connections
    /// [`TcpListener::accept`] returns.
    ///
    /// Each socket takes a slot in the Net's socket table. If one cannot be opened, the
    /// sockets already opened are closed and its error is returned: `Unaddressable` for port 0
    /// or an address the Net does not own, `SocketsFull`, `InvalidState`, and when sharing a
    /// card `InvalidConfig` and `HostLost`. When sharing a card, the daemon checks the endpoint
    /// and its own socket table after the call, and an open it rejects is returned by `accept`.
    ///
    /// # Panics
    ///
    /// If `backlog` is 0, and as [`NetHandle::tcp_connect`].
    pub fn tcp_listen(
        &self,
        config: tcp::Config,
        local: IpListenEndpoint,
        backlog: usize,
    ) -> Result<TcpListener, Error> {
        assert!(backlog != 0, "tcp_listen backlog must be at least 1");
        let mut listener = TcpListener { handle: self.clone(), config, local, backlog, queue: VecDeque::new() };
        for _ in 0..backlog {
            let accepting = listener.open()?;
            listener.queue.push_back(accepting);
        }
        Ok(listener)
    }

    /// One TCP socket listening on `local`.
    fn open_listening(&self, config: tcp::Config, local: IpListenEndpoint) -> Result<Opening, Error> {
        let index = {
            let mut inner = self.shared.borrow_mut();
            match &mut *inner {
                Inner::Remote(remote) => Some(remote.accept(config, local)),
                Inner::Local(_) => None,
            }
        };
        if let Some(index) = index {
            return index.map(|index| self.opening(index));
        }
        self.open_tcp(config, |stack, handle| stack.listen(handle, local).map_err(Error::from))
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

    /// Binds a UDP socket as `config` says. When owning the card, the future resolves on its
    /// first poll; when sharing a card, once the daemon confirms.
    ///
    /// Errors when owning the card, in order: `InvalidConfig` (port 0 or `rx_queue` 0), `Unaddressable`
    /// (`local_addr` is not a unicast address the Net owns), `PortInUse` (that address, or
    /// all-IP, already bound on the port), `SocketsFull`. Sharing a card returns the same
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
    /// | | Owning the card | Sharing a card |
    /// |---|---|---|
    /// | Pool | the network stack's frame pool, shared by every socket; with `switch::nic::open`, `Sizing::frame_share` adds buffers beyond its reserve; see [`TcpTx::try_send`] for retention and copying | the Net's own [`RemoteConfig::tx_frames`] buffers; a sent frame is copied and returns at once |
    /// | `len()` | IP MTU − 28, the largest UDP payload | `Session::frame_capacity()` |
    ///
    /// While the pool is empty the task waits in line. Each [`Net::receive`] wakes as many
    /// waiting tasks, oldest first, as the pool has free frames.
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
/// dropping the last half drops the socket.
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

/// TCP sockets listening on one endpoint, from [`NetHandle::tcp_listen`];
/// [`accept`](TcpListener::accept) returns their connections.
///
/// The listener keeps up to `backlog` sockets, which bounds the connections waiting for
/// `accept`, those still in the handshake included. A connection completes the handshake and
/// receives data while it waits, with no task in `accept`. A handshake the peer does not
/// complete is given up after five SYN-ACK retransmissions, about 19 s, and the socket listens
/// again. A SYN that arrives while every socket holds a connection is reset, unless another
/// socket listens on the port. The listener takes `backlog` slots of the Net's socket table,
/// and each accepted stream keeps the slot of its socket.
///
/// Dropping the listener resets the connections not yet accepted and closes its listening
/// sockets, so a SYN to the port is then refused unless another socket listens on it. When
/// sharing a card, the daemon does this asynchronously.
pub struct TcpListener {
    handle: NetHandle,
    config: tcp::Config,
    local: IpListenEndpoint,
    backlog: usize,
    /// Each holds an open socket; an accept that resolves is removed.
    queue: VecDeque<Accepting>,
}

impl TcpListener {
    /// Resolves to the next connection.
    ///
    /// Each call first opens sockets until the listener holds `backlog` again, and the socket
    /// of the returned connection is replaced. A socket that cannot be opened, for example
    /// because the socket table is full, is tried again by the next call; its error is
    /// returned only when the listener has no socket left. When sharing a card, the daemon
    /// can reject an open after the call that made it returned, and one call of `accept`
    /// returns that error. When several connections are waiting, which one is returned is
    /// unspecified. A connection that closed before `accept` is still returned, as for
    /// [`NetHandle::tcp_accept`].
    ///
    /// Dropping the future leaves the waiting connections in the listener.
    pub fn accept(&mut self) -> Accept<'_> {
        Accept { listener: self }
    }

    fn open(&self) -> Result<Accepting, Error> {
        let opening = self.handle.open_listening(self.config, self.local)?;
        Ok(Accepting { opening: Some(Ok(opening)) })
    }
}

impl Drop for TcpListener {
    fn drop(&mut self) {
        let mut inner = self.handle.shared.borrow_mut();
        for accepting in &self.queue {
            let Some(Ok(opening)) = &accepting.opening else {
                unreachable!("a queued accept holds an open socket")
            };
            inner.abort(opening.rx.0.index);
        }
    }
}

/// Future from [`TcpListener::accept`]; resolves to `Result<TcpStream, Error>`.
pub struct Accept<'a> {
    listener: &'a mut TcpListener,
}

impl Future for Accept<'_> {
    type Output = Result<TcpStream, Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let listener = &mut *self.get_mut().listener;
        let mut failed = None;
        while listener.queue.len() < listener.backlog {
            match listener.open() {
                Ok(accepting) => listener.queue.push_back(accepting),
                Err(error) => {
                    failed = Some(error);
                    break;
                }
            }
        }
        for position in 0..listener.queue.len() {
            if let Poll::Ready(result) = Pin::new(&mut listener.queue[position]).poll(cx) {
                listener.queue.remove(position);
                if let Ok(accepting) = listener.open() {
                    listener.queue.push_back(accepting);
                }
                return Poll::Ready(result);
            }
        }
        match failed {
            Some(error) if listener.queue.is_empty() => Poll::Ready(Err(error)),
            _ => Poll::Pending,
        }
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
/// unread, otherwise a FIN. Dropping only one half leaves the other working. When owning the
/// card, the slot stays taken until the connection finishes closing; when sharing a card,
/// the daemon closes it asynchronously.
///
/// TCP timers run when the network stack is driven; when sharing a card, the daemon drives it.
///
/// | Behavior | Timing |
/// |---|---|
/// | Retransmission timeout | 300 ms before the first round-trip sample; then the smoothed round-trip time plus the larger of four times its variation and 50 ms. Each timeout doubles the wait, up to 60 s. |
/// | Giving up | No retry limit while the program keeps the connection open. Gracefully dropped connections reset at 8 retransmission back-offs. Once dropped and waiting for the peer's FIN after our FIN was acknowledged, a connection has a 60 s timeout. |
/// | Delayed acknowledgement | 10 ms; an acknowledgement can be sent sooner. |
/// | Keep-alive | None; an idle connection sends nothing. |
/// | TIME-WAIT | 10 s after both sides complete an active or simultaneous close; further packets can extend it. This is not a countdown from [`TcpTx::shutdown`]. |
/// | Reusing endpoints | The same local-address/port and remote-address/port pair stays in use until closing and TIME-WAIT finish. A connection still held while waiting for the peer's FIN can wait indefinitely. |
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
    ///   state without receive, or when sharing a card once the session is lost.
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
    ///   [`shutdown`](TcpTx::shutdown), or when sharing a card once the session is lost.
    /// - `Ok(())` otherwise. When owning the card, `len` bytes fit beside all queued and
    ///   unacknowledged bytes. A nonempty frame of that length from this Net's TX-frame pool
    ///   will not return `Full` before the next [`Net::receive`]. No frame is reserved.
    ///   When sharing a card, this is a hint: it waits only after some socket of this `Net`
    ///   got [`Error::Full`] since the last receive step; `try_send` can still return `Full`.
    pub fn room(&mut self, len: usize) -> Room<'_> {
        Room { tx: self, len }
    }

    /// Queues `frame` without waiting; [`Net::send`] attempts to transmit queued data.
    ///
    /// Errors, checked in this order, each returning the same frame: `Foreign` (not from this
    /// Net's TX-frame pool), `InvalidState` (the socket cannot send), `Empty` (zero length),
    /// `Full` (no room now; wait for [`room`](TcpTx::room) and retry). When sharing a card,
    /// `Full` means the send ring is full, and `TooLarge` that the frame exceeds the session's
    /// largest record.
    ///
    /// When owning the card, the socket holds the frame until its last byte is acknowledged.
    /// If the previous frame is still unsent, has no other handle, and has room for this one
    /// within one segment, this frame is copied there and freed at once. Otherwise, a
    /// transmission sends it in place only if it has one
    /// remaining handle, retains its original payload start, starts at that segment's stream
    /// offset and fits the segment allowed by the peer's window and congestion control.
    /// Other transmissions copy the segment payload; retransmissions can copy it again.
    /// When sharing a card, bytes are copied into the send ring and the frame returns to its
    /// pool at once. The daemon copies them into its frame pool, possibly through its backlog first.
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

    /// The largest `len` [`room`](TcpTx::room) accepts. When owning the card: the socket's
    /// `tcp::Config::tx_capacity`. When sharing a card: the session's largest record minus 16 bytes.
    pub fn send_capacity(&self) -> usize {
        let mut inner = self.0.shared.borrow_mut();
        let Inner::Local(inner) = &mut *inner else {
            let Inner::Remote(remote) = &mut *inner else { unreachable!() };
            return remote.send_capacity();
        };
        let (stack, handle) = inner.tcp(self.0.index);
        stack.tcp(handle).send_capacity()
    }

    /// The connection's maximum segment payload, read when the stream was handed to the task.
    /// `None` if an accepted connection had already closed by then; otherwise it stays `Some`
    /// after close or reset. This is a ceiling: the peer's window and congestion control can
    /// split a smaller frame too.
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
    /// when sharing a card also once the session is lost. Until then queued bytes can remain
    /// unsent: tearing down an owned network stack earlier loses them.
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
/// The frame stays valid after its socket, the `Net` and the network stack are gone. While
/// held it keeps its buffer: when owning the card, a buffer of the network stack's RX pool
/// (the TCP window does not shrink, so later data still arrives); when sharing a card, space
/// in the session's receive ring, shared by every socket of the session.
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

    /// Borrows the received bytes mutably to edit them in place.
    pub fn payload_mut(&mut self) -> &mut [u8] {
        &mut self.frame
    }

    /// The card's receive timestamp of the packet that carried the data, raw in the driver's
    /// units; 0 when there is none.
    pub fn rx_timestamp(&self) -> u64 {
        self.rx_timestamp
    }

    /// Gives up this wrapper for an [`OwnedFrame`] that can be frozen and cloned without copying.
    /// Read [`rx_timestamp`](TcpFrame::rx_timestamp) first; the frame does not carry it.
    pub fn into_frame(self) -> OwnedFrame {
        self.frame
    }
}

/// A bound UDP socket.
///
/// [`split`](UdpSocket::split) gives a receive half and a send half. Dropping both unbinds the
/// port; when sharing a card, the daemon does that asynchronously.
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
    /// Waits for the next queued [`Datagram`]; never fails. When sharing a card it keeps
    /// waiting after the session is lost.
    pub fn recv(&mut self) -> UdpRecv<'_> {
        UdpRecv { rx: self }
    }
}

/// The send half of a UDP socket.
pub struct UdpTx(Half);

impl UdpTx {
    /// Queues `frame` as one datagram to `to`; transmission can wait for card capacity.
    /// A zero-length frame is an empty datagram. Every error returns the same frame.
    ///
    /// | | Owning the card | Sharing a card |
    /// |---|---|---|
    /// | Errors, in order | `Foreign`, `NoDestination`, `Unaddressable`, `TooLarge`, `Full` (no frame for a copy), `NoRoute`, `NeighborPending`, `Full` (outgoing queue full) | `Foreign`, `InvalidState` (socket closed or session lost), `NoDestination`, `Full` (send ring full), `TooLarge` |
    ///
    /// `to: None` gives `NoDestination` after the preceding checks. Retry `Full` after later
    /// rounds free capacity and `NeighborPending` after later receive steps can take in an ARP
    /// reply; neither recovery is guaranteed.
    ///
    /// When owning the card, a frame with one remaining handle and its original payload start
    /// is sent in place. A shared frame, or one sliced away from that start, is copied into
    /// another pool frame. When sharing a card, every send is copied into the send ring, then
    /// into the daemon's frame pool. The daemon drops a datagram it cannot route, resolve or
    /// address without reporting it.
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
    /// `UdpConfig::rx_queue` was full. When owning the card that queue counts datagrams waiting
    /// for `recv` and those the program still holds. When sharing a card the daemon applies
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
/// The frame stays valid after its socket, the `Net` and the network stack are gone. While
/// held it keeps its buffer: when owning the card, a buffer of the network stack's RX pool and
/// one place in the socket's `rx_queue`; when sharing a card, space in the session's receive ring.
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

    /// Borrows the datagram's payload mutably to edit it in place.
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

    /// The card's receive timestamp, raw in the driver's units; 0 when there is none.
    pub fn rx_timestamp(&self) -> u64 {
        self.rx_timestamp
    }

    /// Gives up this wrapper for an [`OwnedFrame`] that can be frozen and cloned without copying.
    /// Read the addresses and timestamp first; the frame does not carry them.
    pub fn into_frame(self) -> OwnedFrame {
        self.frame
    }
}
