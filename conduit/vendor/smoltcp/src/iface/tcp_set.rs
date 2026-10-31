use core::fmt;

use alloc::collections::VecDeque;
use alloc::vec::Vec;

use super::socket_meta::Meta;
use super::interface::InterfaceInner;
use crate::socket::tcp;

use super::{bind_word, slot_map, tuple_word, BindKey, SlotMap};
use super::timer_wheel::{
    InsertOutcome, IntrusiveLinks, Linked, Timed, TimerWheel, WheelEntry, WheelPosition,
};
use crate::socket::PollAt;
use crate::time::Instant;
use crate::wire::{IpAddress, IpEndpoint, IpListenEndpoint};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FlowKey {
    None,
    /// A listener's (local address or all-IP, port).
    Listener(BindKey),
    Exact {
        local: IpEndpoint,
        remote: IpEndpoint,
    },
}

pub(crate) fn flow_key(socket: &tcp::Socket) -> FlowKey {
    let message = match socket.state() {
        tcp::State::Closed => return FlowKey::None,
        tcp::State::Listen => {
            debug_assert!(socket.local_endpoint().is_none());
            debug_assert!(socket.remote_endpoint().is_none());
            let endpoint = socket.listen_endpoint();
            let addr = endpoint.addr.map(|addr| match addr {
                IpAddress::Ipv4(addr) => addr,
            });
            return FlowKey::Listener((addr, endpoint.port));
        }
        tcp::State::SynSent => "TCP SynSent requires local and remote endpoints",
        tcp::State::SynReceived => "TCP SynReceived requires local and remote endpoints",
        tcp::State::Established => "TCP Established requires local and remote endpoints",
        tcp::State::FinWait1 => "TCP FinWait1 requires local and remote endpoints",
        tcp::State::FinWait2 => "TCP FinWait2 requires local and remote endpoints",
        tcp::State::CloseWait => "TCP CloseWait requires local and remote endpoints",
        tcp::State::Closing => "TCP Closing requires local and remote endpoints",
        tcp::State::LastAck => "TCP LastAck requires local and remote endpoints",
        tcp::State::TimeWait => "TCP TimeWait requires local and remote endpoints",
    };
    FlowKey::Exact {
        local: socket.local_endpoint().expect(message),
        remote: socket.remote_endpoint().expect(message),
    }
}

#[derive(Debug, Clone, Copy)]
#[repr(align(64))]
struct FlowSlot {
    registered: FlowKey,
    /// The next slot in a listener chain (ascending), or `usize::MAX`.
    next: usize,
    dirty: bool,
    deadline: PollAt,
    /// Position in `TcpSet::waiting`, or `UNLISTED`.
    waiting_at: u32,
    queued: bool,
    timer_links: IntrusiveLinks,
    timer_position: WheelPosition,
    orphaned: bool,
    /// Position in `TcpSet::reapable`; `REPORTED` once `take_reapable` returned the slot,
    /// until it is removed; `UNLISTED` otherwise.
    reap_at: u32,
    /// `TcpStorage::tokens` holds an armed token: the one check `mark_dirty` makes.
    armed: bool,
}

impl FlowSlot {
    const EMPTY: Self = Self {
        registered: FlowKey::None,
        next: usize::MAX,
        dirty: false,
        deadline: PollAt::Ingress,
        waiting_at: UNLISTED,
        queued: false,
        timer_links: IntrusiveLinks::new(),
        timer_position: WheelPosition::Unlinked,
        orphaned: false,
        reap_at: UNLISTED,
        armed: false,
    };

    fn is_waiting(&self) -> bool {
        self.waiting_at != UNLISTED
    }
}

/// A list position meaning "not in the list".
const UNLISTED: u32 = u32::MAX;
/// `FlowSlot::reap_at` for a slot `take_reapable` returned.
const REPORTED: u32 = u32::MAX - 1;

fn deadline_tick(instant: Instant) -> u64 {
    let micros = instant.total_micros();
    if micros < 0 {
        0
    } else {
        (micros as u64).div_ceil(1000)
    }
}

impl Linked for FlowSlot {
    fn links(&self) -> &IntrusiveLinks {
        &self.timer_links
    }

    fn links_mut(&mut self) -> &mut IntrusiveLinks {
        &mut self.timer_links
    }
}

impl Timed for FlowSlot {
    fn deadline(&self) -> u64 {
        match self.deadline {
            PollAt::Time(instant) => deadline_tick(instant),
            _ => unreachable!("TCP wheel entry has no timed deadline"),
        }
    }
}

impl WheelEntry for FlowSlot {
    fn wheel_position(&self) -> WheelPosition {
        self.timer_position
    }

    fn set_wheel_position(&mut self, position: WheelPosition) {
        self.timer_position = position;
    }
}

/// One socket slot.
#[derive(Debug, Default)]
#[repr(C, align(64))]
pub(crate) struct TcpStorage<'a> {
    /// Bumped on every remove, so handles to a removed socket stop matching.
    generation: u64,
    inner: Option<Item<'a>>,
    /// Readiness tokens armed for the receive and send halves ([`ReadySide`]). In the last
    /// line's padding, so the slot keeps its size and line map.
    tokens: [Option<u32>; 2],
}

impl<'a> TcpStorage<'a> {
    pub const EMPTY: Self = Self {
        inner: None,
        generation: 0,
        tokens: [None; 2],
    };
}

/// The half of a TCP socket a readiness token watches; an index into its two tokens.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadySide {
    Rx = 0,
    Tx = 1,
}

/// An item of a socket set.
#[derive(Debug)]
#[repr(C)]
pub(crate) struct Item<'a> {
    pub(crate) meta: Meta,
    pub(crate) socket: tcp::Socket<'a>,
}

/// A handle, identifying a socket in an Interface.
///
/// A handle names one socket: once that socket is removed, the handle
/// stops matching even if a new socket reuses the slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default, Hash)]
pub struct SocketHandle {
    index: usize,
    generation: u64,
}

impl fmt::Display for SocketHandle {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "#{}", self.index)
    }
}

/// A fixed-size set of TCP sockets.
///
/// A set is used with one Interface. Cached deadlines depend on its IP MTU.
///
/// Every socket's flow key (None for Closed, the listen endpoint for Listen, the 4-tuple
/// otherwise) is in the flow index from the call that changed it: `connect`, `listen`,
/// `close`, `abort`, `orphan`, and the interface's processing and egress. `get_mut` gives the
/// socket for data and settings, which cannot change the key, and marks it dirty.
///
/// Readiness tokens: [`arm`](Self::arm) stores a caller's `u32` for one half of a socket.
/// Every change that marks the socket dirty (ingress, `get_mut`, a receive wake, timer and
/// neighbor rescans) and every state change in egress fires each armed token of the socket:
/// it moves to a list that [`pop_fired`](Self::pop_fired) drains, and is disarmed. A token
/// fires at most once per arm; the list holds two per slot without growing while no token is
/// re-armed before it was popped.
///
/// If a socket method unwinds out of `Interface::poll`, the set must not be polled again.
#[derive(Debug)]
pub struct TcpSet<'a> {
    sockets: Vec<TcpStorage<'a>>,
    pub(crate) pending_rx_attach: Option<(usize, tcp::PendingRxCommit)>,
    pub(crate) pending_tcp_reply: Option<usize>,
    /// Number of occupied slots.
    len: usize,
    /// Bit `i` is set when slot `i` holds a socket.
    occupied: Vec<u64>,
    flow_slots: Vec<FlowSlot>,
    exact: SlotMap<u128>,
    /// Listener chains by (local address or all-IP, port), each ascending by slot.
    listeners: SlotMap<u64>,
    /// The slot the last non-SYN exact lookup found.
    last_exact: Option<usize>,
    pub(crate) dirty_list: Vec<usize>,
    wheel: TimerWheel,
    /// Slots the wheel expired in the current advance; empty between calls.
    expired: Vec<u32>,
    pub(crate) waiting: Vec<usize>,
    pub(crate) neighbor_changes: u64,
    ready: VecDeque<usize>,
    reapable: Vec<SocketHandle>,
    fired: Vec<u32>,
}

impl<'a> TcpSet<'a> {
    /// Create an empty set with room for `slot_count` sockets.
    pub fn new(slot_count: usize) -> TcpSet<'a> {
        let mut sockets = Vec::with_capacity(slot_count);
        sockets.resize_with(slot_count, || TcpStorage::EMPTY);
        TcpSet {
            sockets,
            pending_rx_attach: None,
            pending_tcp_reply: None,
            len: 0,
            occupied: alloc::vec![0u64; slot_count.div_ceil(64)],
            flow_slots: alloc::vec![FlowSlot::EMPTY; slot_count],
            exact: slot_map(slot_count),
            listeners: slot_map(slot_count),
            last_exact: None,
            dirty_list: Vec::with_capacity(slot_count),
            wheel: TimerWheel::default(),
            expired: Vec::with_capacity(slot_count),
            waiting: Vec::with_capacity(slot_count),
            neighbor_changes: 0,
            ready: VecDeque::with_capacity(slot_count),
            reapable: Vec::with_capacity(slot_count),
            fired: Vec::with_capacity(2 * slot_count),
        }
    }

    /// Return the number of sockets in the set.
    pub fn len(&self) -> usize {
        self.len
    }

    fn lowest_free(&self) -> Option<usize> {
        let (word, bits) = self
            .occupied
            .iter()
            .enumerate()
            .find(|(_, bits)| **bits != u64::MAX)?;
        let index = word * 64 + bits.trailing_ones() as usize;
        (index < self.sockets.len()).then_some(index)
    }

    fn set_occupied(&mut self, index: usize, occupied: bool) {
        if occupied {
            self.len += 1;
        } else {
            self.len -= 1;
        }
        let bit = 1u64 << (index % 64);
        if occupied {
            self.occupied[index / 64] |= bit;
        } else {
            self.occupied[index / 64] &= !bit;
        }
    }

    /// Add a socket to the set without fixing its RX frame count, register its flow, and return
    /// its handle.
    ///
    /// # Panics
    /// This function panics if the set is full.
    pub(crate) fn add(&mut self, socket: tcp::Socket<'a>) -> SocketHandle {
        fn put<'a>(
            index: usize,
            slot: &mut TcpStorage<'a>,
            socket: tcp::Socket<'a>,
        ) -> SocketHandle {
            net_trace!("[{}]: adding", index);
            let handle = SocketHandle {
                index,
                generation: slot.generation,
            };
            let meta = Meta::default();
            slot.inner = Some(Item { meta, socket });
            handle
        }

        let index = self.lowest_free().expect("adding a socket to a full TcpSet");
        self.set_occupied(index, true);
        let idle = socket.state() == tcp::State::Closed && socket.remote_endpoint().is_none();
        let handle = put(index, &mut self.sockets[index], socket);
        self.update_identity(index);
        // A Closed socket with no peer has nothing to send; `connect` or `listen` marks it.
        if !idle {
            self.mark_dirty(index);
        }
        handle
    }

    /// Build a socket from `config`, fix its RX frame count to
    /// [`tcp::rx_mbuf_budget`]`(config, ip_mtu)` and add it. Its TX frames come from
    /// `tx_source`, at most [`tcp::tx_mbuf_budget`]`(config, ip_mtu)` queued at a time. Both
    /// are per-socket limits; the frames come from pools shared by every socket. Outside this
    /// crate, sockets are created only here and [`TcpSet::remove`] drops them, so no owned
    /// socket leaves the set.
    ///
    /// # Panics
    /// This function panics if `ip_mtu` is below 68, `config.rx_capacity` exceeds 1 GiB,
    /// `config.mss_cap` is `Some(0)`, the frame count exceeds u32, or the set is full.
    pub fn add_admitted(
        &mut self,
        config: &tcp::Config,
        ip_mtu: usize,
        tx_source: crate::phy::TxSource,
        wake: super::DropQueue,
    ) -> SocketHandle {
        let mut socket = tcp::Socket::from_config(config, ip_mtu, tx_source);
        socket.set_rx_mbuf_budget(ip_mtu);
        let handle = self.add(socket);
        self.item_mut(handle).socket.set_rx_wake(wake, handle);
        handle
    }

    /// Get a socket from the set by its handle.
    ///
    /// # Panics
    /// This function may panic if the handle does not belong to this socket set,
    /// and panics if the socket it named was removed.
    pub fn get(&self, handle: SocketHandle) -> &tcp::Socket<'a> {
        &self.item(handle).socket
    }

    /// Get a mutable socket from the set by its handle, for data and settings, and schedule its
    /// readiness check.
    ///
    /// # Panics
    /// This function may panic if the handle does not belong to this socket set,
    /// and panics if the socket it named was removed.
    pub fn get_mut(&mut self, handle: SocketHandle) -> &mut tcp::Socket<'a> {
        self.item(handle);
        self.mark_dirty(handle.index);
        &mut self.item_mut(handle).socket
    }

    /// [`tcp::Socket::connect`] through the set, with `cx` the context of the interface that
    /// drives it: the new flow is registered, and the socket scheduled, in this call. Refuses
    /// `AddressInUse` when a socket outside CLOSED and LISTEN holds the tuple. A failed connect
    /// changes nothing.
    pub fn connect<T, U>(
        &mut self,
        handle: SocketHandle,
        cx: &mut InterfaceInner,
        remote_endpoint: T,
        local_endpoint: U,
    ) -> Result<(), tcp::ConnectError>
    where
        T: Into<IpEndpoint>,
        U: Into<IpListenEndpoint>,
    {
        let remote_endpoint = remote_endpoint.into();
        let local_endpoint = self.item(handle).socket.connect_local_endpoint(
            cx,
            remote_endpoint,
            local_endpoint.into(),
        )?;
        if self.exact.contains_key(&tuple_word(local_endpoint, remote_endpoint)) {
            return Err(tcp::ConnectError::AddressInUse);
        }
        self.item_mut(handle).socket.connect(cx, remote_endpoint, local_endpoint)?;
        self.update_identity(handle.index);
        self.identity_changed(handle.index);
        Ok(())
    }

    /// [`tcp::Socket::listen`] through the set: the listener is registered, and the socket
    /// scheduled, in this call. A failed listen changes nothing.
    pub fn listen<T>(&mut self, handle: SocketHandle, local_endpoint: T) -> Result<(), tcp::ListenError>
    where
        T: Into<IpListenEndpoint>,
    {
        self.item_mut(handle).socket.listen(local_endpoint)?;
        self.update_identity(handle.index);
        self.identity_changed(handle.index);
        Ok(())
    }

    /// [`tcp::Socket::close`] through the set: a listener, or a connection still in SYN-SENT or
    /// SYN-RECEIVED, leaves the flow index in this call.
    pub fn close(&mut self, handle: SocketHandle) {
        self.item_mut(handle).socket.close();
        self.update_identity(handle.index);
        self.identity_changed(handle.index);
    }

    /// [`tcp::Socket::abort`] through the set: the socket leaves the flow index in this call
    /// and is scheduled to send its reset.
    pub fn abort(&mut self, handle: SocketHandle) {
        self.item_mut(handle).socket.abort();
        self.update_identity(handle.index);
        self.identity_changed(handle.index);
    }

    /// After an identity operation registered the slot's flow: queue an orphan that closed for
    /// reaping and schedule the socket's readiness check.
    fn identity_changed(&mut self, index: usize) {
        self.check_reapable(index);
        self.mark_dirty(index);
    }

    /// Arm `token` for `side` of the socket, replacing a token armed there. Removing the
    /// socket or orphaning it (its handle dropped) disarms both halves.
    ///
    /// Arming runs on every park: the caller checks `handle` once (as `get` does) and keeps it
    /// while the socket lives; only debug builds check it here. A stale handle misdirects the
    /// token to whatever socket the slot holds or holds next, where it fires spuriously.
    ///
    /// # Panics
    /// If the handle's slot index is out of range; in debug builds, as `get`.
    pub fn arm(&mut self, handle: SocketHandle, side: ReadySide, token: u32) {
        self.debug_assert_live(handle);
        self.sockets[handle.index].tokens[side as usize] = Some(token);
        self.flow_slots[handle.index].armed = true;
    }

    /// Disarm `side` of the socket. Returns whether a token was armed there; `false` when none
    /// was, or it fired and waits in the fired list until popped. Handle checks as in `arm`.
    pub fn disarm(&mut self, handle: SocketHandle, side: ReadySide) -> bool {
        self.debug_assert_live(handle);
        let tokens = &mut self.sockets[handle.index].tokens;
        let armed = tokens[side as usize].take().is_some();
        self.flow_slots[handle.index].armed = tokens.iter().any(Option::is_some);
        armed
    }

    fn debug_assert_live(&self, handle: SocketHandle) {
        debug_assert!(
            self.sockets.get(handle.index).is_some_and(|slot| {
                slot.inner.is_some() && slot.generation == handle.generation
            }),
            "handle does not refer to a valid socket"
        );
    }

    /// One fired token, in no particular order; `None` once the list is empty.
    #[inline]
    pub fn pop_fired(&mut self) -> Option<u32> {
        self.fired.pop()
    }

    /// Move every token armed on slot `index` to the fired list.
    #[inline]
    pub(crate) fn fire(&mut self, index: usize) {
        if self.flow_slots[index].armed {
            self.fire_armed(index);
        }
    }

    /// Out of line, so `mark_dirty` stays small enough to inline.
    #[inline(never)]
    fn fire_armed(&mut self, index: usize) {
        self.flow_slots[index].armed = false;
        for token in &mut self.sockets[index].tokens {
            if let Some(token) = token.take() {
                self.fired.push(token);
            }
        }
    }

    /// Complete a queued receive wake and schedule the socket's readiness check.
    pub fn rx_woken(&mut self, handle: SocketHandle) {
        let socket = &mut self.sockets.get_mut(handle.index)
            .filter(|slot| slot.generation == handle.generation)
            .and_then(|slot| slot.inner.as_mut())
            .expect("receive wake does not name a live TCP socket").socket;
        socket.clear_rx_wake_queued();
        self.mark_dirty(handle.index);
    }

    /// Complete a frame wait: the frame pool has a free frame again, so fire the socket's
    /// readiness tokens and schedule its readiness check. Removal takes a socket off the
    /// wait list, so `handle` names a live socket.
    pub fn frame_woken(&mut self, handle: SocketHandle) {
        let socket = &mut self.sockets.get_mut(handle.index)
            .filter(|slot| slot.generation == handle.generation)
            .and_then(|slot| slot.inner.as_mut())
            .expect("frame wake does not name a live TCP socket").socket;
        socket.clear_frame_wait_queued();
        self.mark_dirty(handle.index);
    }

    /// Close the socket after its application handle is dropped, keeping it in the set.
    pub fn orphan(&mut self, handle: SocketHandle, now: Instant) {
        self.item(handle);
        self.sockets[handle.index].tokens = [None; 2];
        self.flow_slots[handle.index].armed = false;
        self.get_mut(handle).orphan(now);
        self.update_identity(handle.index);
        self.flow_slots[handle.index].orphaned = true;
        self.check_reapable(handle.index);
    }

    pub(crate) fn check_reapable(&mut self, index: usize) {
        let flow = &mut self.flow_slots[index];
        if !flow.orphaned || flow.reap_at != UNLISTED {
            return;
        }
        let storage = &self.sockets[index];
        let socket = &storage.inner.as_ref().expect("orphan slot is empty").socket;
        if socket.state() == tcp::State::Closed && socket.remote_endpoint().is_none() {
            debug_assert!(self.reapable.len() < self.reapable.capacity());
            flow.reap_at = self.reapable.len() as u32;
            self.reapable.push(SocketHandle { index, generation: storage.generation });
        }
    }

    /// Whether an orphan has become CLOSED with no remote endpoint.
    pub fn has_reapable(&self) -> bool {
        !self.reapable.is_empty()
    }

    /// Drain reapable orphan handles, in no particular order, retaining the list's allocation.
    /// Each handle leaves the list before `f` sees it, so a panic in `f` leaves the handles
    /// not yet reported queued.
    pub fn take_reapable(&mut self, mut f: impl FnMut(SocketHandle)) {
        while let Some(handle) = self.reapable.pop() {
            self.flow_slots[handle.index].reap_at = REPORTED;
            f(handle);
        }
    }

    /// Removes a socket from the set without changing its state, discarding unread received
    /// bytes and clearing receive wakes. Returns the frames the application still holds. The
    /// socket itself is dropped, releasing its queued TX frames: handing it out would let a
    /// caller write it over another socket through `get_mut`.
    ///
    /// # Panics
    /// This function may panic if the handle does not belong to this socket set,
    /// and panics if the socket it named was already removed.
    pub fn remove(&mut self, handle: SocketHandle) -> crate::phy::HeldFrames {
        self.take(handle).held_frames()
    }

    /// `remove`, but returns the socket with an empty receive buffer.
    pub(crate) fn take(&mut self, handle: SocketHandle) -> tcp::Socket<'a> {
        net_trace!("[{}]: removing", handle.index);
        self.item(handle);
        let slot = &mut self.sockets[handle.index];
        slot.generation = slot
            .generation
            .checked_add(1)
            .expect("socket slot generation overflow");
        slot.tokens = [None; 2];
        self.flow_slots[handle.index].armed = false;
        let reap_at = self.flow_slots[handle.index].reap_at;
        if reap_at < REPORTED {
            self.reapable.swap_remove(reap_at as usize);
            if let Some(moved) = self.reapable.get(reap_at as usize) {
                self.flow_slots[moved.index].reap_at = reap_at;
            }
        }
        self.flow_slots[handle.index].orphaned = false;
        self.flow_slots[handle.index].reap_at = UNLISTED;
        self.unlink_identity(handle.index);
        self.set_deadline(handle.index, PollAt::Ingress, false);
        if self.flow_slots[handle.index].queued {
            let position = self
                .ready
                .iter()
                .position(|&index| index == handle.index)
                .expect("queued TCP slot is not in the ready queue");
            self.ready.remove(position);
            self.flow_slots[handle.index].queued = false;
        }
        // Before the take, so the socket is moved out once, straight to the caller.
        self.set_occupied(handle.index, false);
        match self.sockets[handle.index].inner.take() {
            Some(mut item) => {
                item.socket.clear_rx_wake();
                item.socket.discard_received();
                item.socket
            }
            None => unreachable!(),
        }
    }

    /// The item `handle` names; panics unless it is the socket currently in its slot.
    fn item(&self, handle: SocketHandle) -> &Item<'a> {
        match &self.sockets[handle.index] {
            TcpStorage {
                inner: Some(item),
                generation,
                ..
            } if *generation == handle.generation => item,
            _ => panic!("handle does not refer to a valid socket"),
        }
    }

    /// The item `handle` names; panics unless it is the socket currently in its slot.
    fn item_mut(&mut self, handle: SocketHandle) -> &mut Item<'a> {
        match &mut self.sockets[handle.index] {
            TcpStorage {
                inner: Some(item),
                generation,
                ..
            } if *generation == handle.generation => item,
            _ => panic!("handle does not refer to a valid socket"),
        }
    }

    /// Get an iterator to the inner sockets.
    pub fn iter(&self) -> impl Iterator<Item = (SocketHandle, &tcp::Socket<'a>)> {
        self.sockets.iter().enumerate().filter_map(|(index, storage)| {
            let item = storage.inner.as_ref()?;
            let handle = SocketHandle {
                index,
                generation: storage.generation,
            };
            Some((handle, &item.socket))
        })
    }

    /// Get a mutable iterator to the inner sockets.
    #[cfg(test)]
    pub(crate) fn iter_mut(&mut self) -> impl Iterator<Item = (SocketHandle, &mut tcp::Socket<'a>)> {
        let dirty_list = &mut self.dirty_list;
        let flow_slots = &mut self.flow_slots;
        let wheel = &mut self.wheel;
        self.sockets
            .iter_mut()
            .enumerate()
            .filter_map(move |(index, storage)| {
                let item = storage.inner.as_mut()?;
                if !flow_slots[index].dirty {
                    let timer_index = u32::try_from(index).expect("TCP timer index overflow");
                    wheel.remove(flow_slots, timer_index);
                    flow_slots[index].dirty = true;
                    dirty_list.push(index);
                }
                let handle = SocketHandle {
                    index,
                    generation: storage.generation,
                };
                Some((handle, &mut item.socket))
            })
    }

    /// Iterate every socket in this set.
    pub(crate) fn items(&self) -> impl Iterator<Item = &Item<'a>> + '_ {
        self.sockets.iter().filter_map(|x| x.inner.as_ref())
    }

    pub(crate) fn slot_count(&self) -> usize {
        self.sockets.len()
    }

    pub(crate) fn item_at_mut(&mut self, index: usize) -> Option<&mut Item<'a>> {
        self.sockets[index].inner.as_mut()
    }

    pub(crate) fn handle_at(&self, index: usize) -> SocketHandle {
        SocketHandle {
            index,
            generation: self.sockets[index].generation,
        }
    }
}

impl TcpSet<'_> {
    fn unlink_identity(&mut self, index: usize) {
        let slot = self.flow_slots[index];
        match slot.registered {
            FlowKey::None => return,
            FlowKey::Exact { local, remote } => {
                self.exact.remove(&tuple_word(local, remote));
            }
            FlowKey::Listener(bind) => {
                let head = *self
                    .listeners
                    .get(&bind_word(bind))
                    .expect("registered TCP listener has no chain head") as usize;
                if head == index {
                    if slot.next == usize::MAX {
                        self.listeners.remove(&bind_word(bind));
                    } else {
                        self.listeners.insert(bind_word(bind), slot.next as u32);
                    }
                } else {
                    let mut previous = head;
                    while self.flow_slots[previous].next != index {
                        previous = self.flow_slots[previous].next;
                        assert_ne!(
                            previous,
                            usize::MAX,
                            "registered TCP listener is not in its chain"
                        );
                    }
                    self.flow_slots[previous].next = slot.next;
                }
            }
        }
        self.flow_slots[index].registered = FlowKey::None;
        self.flow_slots[index].next = usize::MAX;
    }

    /// Registers the slot's current flow key if it changed: called by every operation that can
    /// change it, so the flow index is never stale. A connection that keeps its registered
    /// tuple, the case on every segment, is checked inline.
    #[inline(always)]
    pub(crate) fn update_identity(&mut self, index: usize) {
        if let FlowKey::Exact { local, remote } = self.flow_slots[index].registered
            && let Some(item) = &self.sockets[index].inner
            && !matches!(item.socket.state(), tcp::State::Closed | tcp::State::Listen)
            && item.socket.local_endpoint() == Some(local)
            && item.socket.remote_endpoint() == Some(remote)
        {
            return;
        }
        self.update_identity_slow(index);
    }

    #[inline(never)]
    fn update_identity_slow(&mut self, index: usize) {
        let key = self.slot_flow_key(index);
        if key == self.flow_slots[index].registered {
            return;
        }
        self.register_identity(index, key);
    }

    fn slot_flow_key(&self, index: usize) -> FlowKey {
        self.sockets[index]
            .inner
            .as_ref()
            .map_or(FlowKey::None, |item| flow_key(&item.socket))
    }

    fn register_identity(&mut self, index: usize, key: FlowKey) {
        self.set_deadline(index, PollAt::Ingress, false);
        if let Some(item) = &mut self.sockets[index].inner {
            item.meta = Meta::default();
        }
        self.unlink_identity(index);
        self.flow_slots[index].registered = key;
        match key {
            FlowKey::None => {}
            FlowKey::Exact { local, remote } => {
                let previous = self.exact.insert(tuple_word(local, remote), index as u32);
                assert!(previous.is_none(), "TCP tuple {local} -> {remote} is already registered");
            }
            FlowKey::Listener(bind) => {
                // Lowest slot first: the accept pool hands a SYN to the lowest listener.
                let head = self
                    .listeners
                    .get(&bind_word(bind))
                    .map_or(usize::MAX, |&head| head as usize);
                if index < head {
                    self.flow_slots[index].next = head;
                    self.listeners.insert(bind_word(bind), index as u32);
                } else {
                    let mut previous = head;
                    while self.flow_slots[previous].next < index {
                        previous = self.flow_slots[previous].next;
                    }
                    self.flow_slots[index].next = self.flow_slots[previous].next;
                    self.flow_slots[previous].next = index;
                }
            }
        }
    }

    fn remove_deadline(&mut self, index: usize) {
        if self.flow_slots[index].timer_position != WheelPosition::Unlinked {
            let timer_index = u32::try_from(index).expect("TCP timer index overflow");
            self.wheel.remove(&mut self.flow_slots, timer_index);
        }
    }

    /// Also fires the slot's tokens; the armed check shares the dirty check's slot load.
    /// Always inlined: the out-of-line `fire_armed` call otherwise moves it out of `get_mut`.
    #[inline(always)]
    pub(crate) fn mark_dirty(&mut self, index: usize) {
        let slot = &self.flow_slots[index];
        let (armed, dirty) = (slot.armed, slot.dirty);
        if armed {
            self.fire_armed(index);
        }
        if !dirty {
            self.remove_deadline(index);
            self.flow_slots[index].dirty = true;
            self.dirty_list.push(index);
        }
    }

    /// Clear the dirty list; each slot not already queued is queued if due, else timed on the
    /// wheel. Returns the ready queue length.
    pub(crate) fn clear_dirty(&mut self) -> usize {
        for position in 0..self.dirty_list.len() {
            let index = self.dirty_list[position];
            self.flow_slots[index].dirty = false;
            if !self.flow_slots[index].queued {
                self.include_deadline(index);
            }
        }
        self.dirty_list.clear();
        self.ready.len()
    }

    /// Store the slot's deadline, unlinking an old wheel entry. The caller schedules it.
    pub(crate) fn set_deadline(&mut self, index: usize, deadline: PollAt, waiting: bool) {
        if self.flow_slots[index].deadline != deadline {
            self.remove_deadline(index);
            self.flow_slots[index].deadline = deadline;
        }
        self.set_waiting(index, waiting);
    }

    #[inline]
    pub(crate) fn set_waiting(&mut self, index: usize, waiting: bool) {
        if waiting != self.flow_slots[index].is_waiting() {
            self.move_waiting(index, waiting);
        }
    }

    #[inline(never)]
    fn move_waiting(&mut self, index: usize, waiting: bool) {
        let slot = &mut self.flow_slots[index];
        if waiting {
            slot.waiting_at = self.waiting.len() as u32;
            self.waiting.push(index);
        } else {
            let position = slot.waiting_at as usize;
            slot.waiting_at = UNLISTED;
            self.waiting.swap_remove(position);
            if let Some(&moved) = self.waiting.get(position) {
                self.flow_slots[moved].waiting_at = position as u32;
            }
        }
    }

    pub(crate) fn poll_at(&mut self) -> Option<Instant> {
        let mut earliest = self.wheel.next_boundary().map(|tick| {
            Instant::from_micros(
                i64::try_from(tick.checked_mul(1000).expect("TCP timer boundary overflow"))
                    .expect("TCP timer boundary exceeds Instant"),
            )
        });
        // A queued slot can have gained a future deadline, or none, since it was queued.
        for &index in &self.ready {
            let deadline = match self.flow_slots[index].deadline {
                PollAt::Ingress => continue,
                PollAt::Now => Instant::ZERO,
                PollAt::Time(instant) => instant,
            };
            earliest = Some(earliest.map_or(deadline, |previous| previous.min(deadline)));
        }
        earliest
    }

    pub(crate) fn deadline_due(&self, index: usize, timestamp: Instant) -> bool {
        match self.flow_slots[index].deadline {
            PollAt::Ingress => false,
            PollAt::Now => true,
            PollAt::Time(instant) if instant.total_micros() < 0 => instant <= timestamp,
            PollAt::Time(instant) => {
                timestamp.total_micros() >= 0
                    && deadline_tick(instant) <= timestamp.total_micros() as u64 / 1000
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn stored_deadline(&self, index: usize) -> PollAt {
        self.flow_slots[index].deadline
    }

    /// Advance the wheel to `timestamp`. Expired slots are queued in expiry order; one waiting
    /// for a neighbor is marked dirty instead.
    pub(crate) fn advance_wheel(&mut self, timestamp: Instant) {
        debug_assert!(self.expired.is_empty());
        if timestamp.total_micros() >= 0 {
            let now_tick = timestamp.total_micros() as u64 / 1000;
            if now_tick >= self.wheel.current_tick() {
                self.wheel
                    .advance_to(&mut self.flow_slots, now_tick, &mut self.expired)
                    .expect("TCP wheel time regressed");
            }
        }
        for position in 0..self.expired.len() {
            let index = self.expired[position] as usize;
            if self.flow_slots[index].is_waiting() {
                self.mark_dirty(index);
            } else {
                self.push_ready(index, false);
            }
        }
        self.expired.clear();
    }

    pub(crate) fn ready_len(&self) -> usize {
        self.ready.len()
    }

    pub(crate) fn egress_idle(&self) -> bool {
        self.ready.is_empty() && self.dirty_list.is_empty() && self.waiting.is_empty()
    }

    pub(crate) fn pop_ready(&mut self) -> usize {
        let index = self.ready.pop_front().expect("TCP ready queue is empty");
        self.flow_slots[index].queued = false;
        index
    }

    /// Queue the slot if its deadline is due by the wheel's clock, else link it on the wheel.
    pub(crate) fn include_deadline(&mut self, index: usize) {
        let slot = &self.flow_slots[index];
        debug_assert!(!slot.dirty && !slot.queued);
        if self.sockets[index].inner.is_none() || slot.timer_position != WheelPosition::Unlinked {
            return;
        }
        let timer_index = u32::try_from(index).expect("TCP timer index overflow");
        let due = match slot.deadline {
            PollAt::Ingress => false,
            PollAt::Now => true,
            PollAt::Time(_) => {
                self.wheel.insert(&mut self.flow_slots, timer_index) == InsertOutcome::Due
            }
        };
        if due {
            self.push_ready(index, false);
        }
    }

    pub(crate) fn push_ready(&mut self, index: usize, first: bool) {
        if !self.flow_slots[index].queued {
            self.remove_deadline(index);
            if first {
                self.ready.push_front(index);
            } else {
                self.ready.push_back(index);
            }
            self.flow_slots[index].queued = true;
        }
    }

    #[cfg(feature = "diagnostics")]
    pub(crate) fn debug_assert_ready<F>(&self, timestamp: Instant, has_neighbor: F)
    where
        F: Fn(crate::wire::IpAddress) -> bool,
    {
        for (index, storage) in self.sockets.iter().enumerate() {
            let queued = self.flow_slots[index].queued;
            match &storage.inner {
                Some(item) => {
                    if !self.flow_slots[index].dirty
                        && self.deadline_due(index, timestamp)
                        && item
                            .meta
                            .poll_at(crate::socket::PollAt::Now, &has_neighbor, timestamp)
                            == crate::socket::PollAt::Now
                    {
                        assert!(queued, "TCP slot {index} needs egress but is not queued");
                    }
                }
                None => assert!(!queued, "empty TCP slot {index} is queued"),
            }
        }
        for &index in &self.ready {
            assert!(self.flow_slots[index].queued);
            assert!(self.sockets[index].inner.is_some());
        }
    }

    /// The exact flow for the segment, else for a SYN-like segment the listener.
    #[cfg(test)]
    pub(crate) fn tcp_lookup(
        &mut self,
        dst: IpEndpoint,
        src: IpEndpoint,
        syn_like: bool,
    ) -> Option<usize> {
        let exact = self.tcp_exact_lookup(dst, src, syn_like);
        if exact.is_none() && syn_like {
            self.tcp_listener_lookup(dst)
        } else {
            exact
        }
    }

    /// The exact (4-tuple) flow for a segment from `src` to `dst`, without the listener
    /// fallback. A non-SYN segment's hit is cached for the next lookup.
    #[inline]
    pub(crate) fn tcp_exact_lookup(
        &mut self,
        dst: IpEndpoint,
        src: IpEndpoint,
        syn_like: bool,
    ) -> Option<usize> {
        if !syn_like
            && let Some(index) = self.last_exact
            && self.flow_slots[index].registered
                == (FlowKey::Exact {
                    local: dst,
                    remote: src,
                })
        {
            return Some(index);
        }
        let e = self.exact.get(&tuple_word(dst, src)).map(|&index| index as usize);
        if !syn_like && let Some(index) = e {
            self.last_exact = Some(index);
        }
        e
    }

    /// The listener a SYN to `dst` goes to: the lowest slot listening on `dst`'s address and
    /// port, else the lowest slot listening on the port on every address (Linux's rule: an exact
    /// bind beats all-IP, whatever the creation order).
    pub(crate) fn tcp_listener_lookup(&self, dst: IpEndpoint) -> Option<usize> {
        let IpAddress::Ipv4(addr) = dst.addr;
        self.listeners
            .get(&bind_word((Some(addr), dst.port)))
            .or_else(|| self.listeners.get(&bind_word((None, dst.port))))
            .map(|&index| index as usize)
    }

    /// Every slot in `waiting` and `reapable` stores its position there, and no other slot
    /// claims one.
    #[cfg(test)]
    pub(crate) fn check_list_positions(&self) {
        for (index, slot) in self.flow_slots.iter().enumerate() {
            match self.waiting.iter().position(|&listed| listed == index) {
                Some(position) => assert_eq!(slot.waiting_at, position as u32, "slot {index}"),
                None => assert_eq!(slot.waiting_at, UNLISTED, "slot {index}"),
            }
            match self.reapable.iter().position(|listed| listed.index == index) {
                Some(position) => assert_eq!(slot.reap_at, position as u32, "slot {index}"),
                None => assert!(slot.reap_at >= REPORTED, "slot {index}"),
            }
        }
    }

    #[cfg(any(test, feature = "diagnostics"))]
    pub(crate) fn check_flow_index(&self) {
        assert_eq!(self.flow_slots.len(), self.sockets.len());
        let mut seen = alloc::vec![false; self.sockets.len()];
        for (&word, &index) in &self.exact {
            let index = index as usize;
            assert!(!seen[index], "TCP slot holds two flows");
            seen[index] = true;
            assert!(self.sockets[index].inner.is_some());
            let FlowKey::Exact { local, remote } = self.flow_slots[index].registered else {
                panic!("TCP slot {index} is in the exact table without a tuple");
            };
            assert_eq!(tuple_word(local, remote), word);
            assert_eq!(self.flow_slots[index].next, usize::MAX);
        }
        for (&word, &head) in &self.listeners {
            let head = head as usize;
            let FlowKey::Listener(bind) = self.flow_slots[head].registered else {
                panic!("TCP slot {head} heads a listener chain without a bind");
            };
            assert_eq!(bind_word(bind), word);
            assert_eq!(
                self.flow_slots
                    .iter()
                    .position(|slot| slot.registered == FlowKey::Listener(bind)),
                Some(head)
            );
            let mut index = head;
            while index != usize::MAX {
                assert!(!seen[index], "TCP slot occurs in more than one chain");
                seen[index] = true;
                assert!(self.sockets[index].inner.is_some());
                let slot = &self.flow_slots[index];
                assert_eq!(slot.registered, FlowKey::Listener(bind));
                assert!(slot.next > index, "TCP listener chain is not ascending");
                index = slot.next;
            }
        }
        let mut dirty = alloc::vec![false; self.sockets.len()];
        for &index in &self.dirty_list {
            assert!(!dirty[index], "TCP dirty list contains a duplicate");
            dirty[index] = true;
        }
        for (index, (storage, slot)) in self.sockets.iter().zip(&self.flow_slots).enumerate() {
            assert_eq!(slot.dirty, dirty[index]);
            assert_eq!(seen[index], slot.registered != FlowKey::None);
            if let Some(item) = &storage.inner {
                assert_eq!(slot.registered, flow_key(&item.socket), "TCP slot {index} flow key");
            } else {
                assert_eq!(slot.registered, FlowKey::None);
            }
            if slot.registered == FlowKey::None {
                assert_eq!(slot.next, usize::MAX);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::string::ToString;
    use alloc::vec;

    fn socket() -> tcp::Socket<'static> {
        tcp::Socket::test_socket(0, 0)
    }

    fn at(index: usize, generation: u64) -> SocketHandle {
        SocketHandle { index, generation }
    }

    const INVALID: &str = "handle does not refer to a valid socket";

    fn assert_reapable(set: &mut TcpSet, expected: &[SocketHandle]) {
        let mut found = Vec::new();
        set.take_reapable(|handle| found.push(handle));
        assert_eq!(found, expected);
        assert!(!set.has_reapable());
        set.take_reapable(|_| panic!("orphan reported twice"));
    }

    /// Removing a queued orphan before it is reported keeps the other queued orphans' stored
    /// positions right; each is reported once, and a reported orphan is not queued again.
    #[test]
    fn removing_a_queued_orphan_keeps_the_others_reported_once() {
        let mut set = TcpSet::new(4);
        let handles: Vec<_> = (0..4)
            .map(|port| {
                let handle = set.add(socket());
                set.listen(handle, 80 + port).unwrap();
                set.orphan(handle, Instant::ZERO);
                handle
            })
            .collect();
        set.check_list_positions();
        set.remove(handles[1]);
        set.check_list_positions();
        let mut found = Vec::new();
        set.take_reapable(|handle| found.push(handle));
        found.sort();
        assert_eq!(found, [handles[0], handles[2], handles[3]]);
        set.orphan(handles[0], Instant::ZERO);
        assert!(!set.has_reapable());
        set.remove(handles[0]);
        set.check_list_positions();
    }

    /// A panic in the `take_reapable` callback leaves the orphans not yet reported queued: the
    /// next call reports them, and each can still be removed.
    #[test]
    fn panic_in_take_reapable_keeps_the_rest_queued() {
        let mut set = TcpSet::new(2);
        let handles: Vec<_> = (0..2)
            .map(|port| {
                let handle = set.add(socket());
                set.listen(handle, 80 + port).unwrap();
                set.orphan(handle, Instant::ZERO);
                handle
            })
            .collect();
        let mut panicked = None;
        let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            set.take_reapable(|handle| {
                panicked = Some(handle);
                panic!("callback panics");
            })
        }));
        assert!(caught.is_err());
        set.check_list_positions();
        let panicked = panicked.unwrap();
        let rest = handles.iter().copied().find(|&handle| handle != panicked).unwrap();
        assert_reapable(&mut set, &[rest]);
        set.remove(panicked);
        set.remove(rest);
        set.check_list_positions();
    }

    #[cfg(feature = "medium-ip")]
    #[test]
    fn reapable_immediate_orphans_and_slot_reuse() {
        let (mut iface, _, _) = crate::tests::setup(crate::phy::Medium::Ip);
        let mut set = TcpSet::new(2);
        for _ in 0..2 {
            let handle = set.add(socket());
            set.listen(handle, 80).unwrap();
            set.orphan(handle, Instant::ZERO);
            set.orphan(handle, Instant::ZERO);
            assert!(set.has_reapable());
            assert_reapable(&mut set, &[handle]);
            set.orphan(handle, Instant::ZERO);
            assert_reapable(&mut set, &[]);
            set.remove(handle);
        }
        let owned = set.add(socket());
        let sent = set.add(socket());
        set.connect(sent, iface.context(), (crate::wire::IpAddress::v4(192, 168, 1, 2), 80), 1234).unwrap();
        set.orphan(sent, Instant::ZERO);
        assert_reapable(&mut set, &[sent]);
        set.orphan(owned, Instant::ZERO);
        set.remove(owned);
        assert_reapable(&mut set, &[]);
    }

    #[cfg(feature = "medium-ip")]
    #[test]
    fn reapable_abort_rst_dispatch() {
        let (mut iface, mut set, mut device) = crate::tests::setup(crate::phy::Medium::Ip);
        let handle = set.add(socket());
        set.connect(handle, iface.context(), (crate::wire::IpAddress::v4(192, 168, 1, 2), 80), 1234).unwrap();
        set.abort(handle);
        set.orphan(handle, Instant::ZERO);
        assert_eq!(set.get(handle).state(), tcp::State::Closed);
        assert!(set.get(handle).remote_endpoint().is_some());
        assert_reapable(&mut set, &[]);
        iface.poll_egress(Instant::ZERO, &mut device, &mut set);
        assert_eq!(set.get(handle).remote_endpoint(), None);
        assert_reapable(&mut set, &[handle]);
        iface.poll_egress(Instant::ZERO, &mut device, &mut set);
        assert_reapable(&mut set, &[]);
        let owned = set.add(socket());
        set.connect(owned, iface.context(), (crate::wire::IpAddress::v4(192, 168, 1, 2), 81), 1235).unwrap();
        set.abort(owned);
        iface.poll_egress(Instant::ZERO, &mut device, &mut set);
        assert_eq!(set.get(owned).remote_endpoint(), None);
        assert_reapable(&mut set, &[]);
    }

    #[cfg(feature = "medium-ip")]
    #[test]
    fn owned_time_wait_reopen_keeps_received_data_and_eof() {
        use crate::phy::Medium;
        use crate::wire::{IpAddress, IpCidr};

        let (mut a, mut a_set, mut a_device) = crate::tests::setup(Medium::Ip);
        let (mut b, mut b_set, mut b_device) = crate::tests::setup(Medium::Ip);
        let remote = IpAddress::v4(192, 168, 1, 2);
        b.update_ip_addrs(|addrs| {
            addrs.clear();
            addrs.push(IpCidr::new(remote, 24)).unwrap();
        });
        let buffered = || tcp::Socket::test_socket(64, 64);
        let ah = a_set.add(buffered());
        let bh = b_set.add(buffered());
        a_set.get_mut(ah).set_ack_delay(None);
        b_set.get_mut(bh).set_ack_delay(None);
        b_set.listen(bh, 80).unwrap();
        a_set.connect(ah, a.context(), (remote, 80), 1234).unwrap();
        for _ in 0..8 {
            a.poll(Instant::ZERO, &mut a_device, &mut a_set);
            b_device.rx_queue.append(&mut a_device.tx_queue);
            b.poll(Instant::ZERO, &mut b_device, &mut b_set);
            a_device.rx_queue.append(&mut b_device.tx_queue);
        }
        assert_eq!(a_set.get(ah).state(), tcp::State::Established);
        a_set.close(ah);
        a.poll(Instant::ZERO, &mut a_device, &mut a_set);
        b_device.rx_queue.append(&mut a_device.tx_queue);
        b.poll(Instant::ZERO, &mut b_device, &mut b_set);
        b_set.get_mut(bh).send_slice(b"abc").unwrap();
        b_set.close(bh);
        for _ in 0..8 {
            b.poll(Instant::ZERO, &mut b_device, &mut b_set);
            a_device.rx_queue.append(&mut b_device.tx_queue);
            a.poll(Instant::ZERO, &mut a_device, &mut a_set);
            b_device.rx_queue.append(&mut a_device.tx_queue);
        }
        assert_eq!(b_set.get(bh).state(), tcp::State::Closed);
        assert_eq!(a_set.get(ah).state(), tcp::State::TimeWait);
        assert_eq!(a_set.get(ah).recv_queue(), 3);
        let listener = a_set.add(buffered());
        a_set.listen(listener, 1234).unwrap();
        b.poll_maintenance(Instant::from_millis(1));
        b_set.connect(bh, b.context(), (IpAddress::v4(192, 168, 1, 1), 1234), 80).unwrap();
        b.poll_egress(Instant::from_millis(1), &mut b_device, &mut b_set);
        let bytes = b_device.tx_queue.front_mut().unwrap();
        let mut ip = crate::wire::Ipv4Packet::new_unchecked(bytes.as_mut_slice());
        let (src, dst) = (ip.src_addr().into(), ip.dst_addr().into());
        let mut syn = crate::wire::TcpPacket::new_unchecked(ip.payload_mut());
        syn.set_seq_number(syn.seq_number() + 6);
        syn.fill_checksum(&src, &dst);
        a_device.rx_queue.append(&mut b_device.tx_queue);
        a.poll(Instant::from_millis(1), &mut a_device, &mut a_set);
        assert_eq!(a_set.get(listener).state(), tcp::State::SynReceived);
        assert_eq!(a_set.get(ah).state(), tcp::State::Closed);
        assert_eq!(a_set.get(ah).remote_endpoint(), None);
        assert_reapable(&mut a_set, &[]);
        assert_eq!(a_set.get(ah).recv_queue(), 3);
        let frame = a_set.get_mut(ah).recv().unwrap().unwrap();
        assert_eq!(frame.payload(), b"abc");
        drop(frame);
        assert!(matches!(a_set.get_mut(ah).recv(), Err(tcp::RecvError::Finished)));
    }

    #[cfg(feature = "medium-ip")]
    #[test]
    fn reapable_ingress_and_time_wait_expiry() {
        use crate::phy::Medium;
        use crate::wire::{IpAddress, IpCidr};

        for (orphaned, reset_with_syn) in [(true, false), (false, false), (true, true)] {
            let (mut a, mut a_set, mut a_device) = crate::tests::setup(Medium::Ip);
            let (mut b, mut b_set, mut b_device) = crate::tests::setup(Medium::Ip);
            let remote = IpAddress::v4(192, 168, 1, 2);
            b.update_ip_addrs(|addrs| {
                addrs.clear();
                addrs.push(IpCidr::new(remote, 24)).unwrap();
            });
            let buffered = || tcp::Socket::test_socket(64, 64);
            let ah = a_set.add(buffered());
            let bh = b_set.add(buffered());
            b_set.listen(bh, 80).unwrap();
            a_set.connect(ah, a.context(), (remote, 80), 1234).unwrap();
            for _ in 0..8 {
                a.poll(Instant::ZERO, &mut a_device, &mut a_set);
                b_device.rx_queue.append(&mut a_device.tx_queue);
                b.poll(Instant::ZERO, &mut b_device, &mut b_set);
                a_device.rx_queue.append(&mut b_device.tx_queue);
            }
            assert_eq!(a_set.get(ah).state(), tcp::State::Established);
            assert_eq!(b_set.get(bh).state(), tcp::State::Established);
            a_set.close(ah);
            if orphaned { a_set.orphan(ah, Instant::ZERO); }
            a.poll(Instant::ZERO, &mut a_device, &mut a_set);
            b_device.rx_queue.append(&mut a_device.tx_queue);
            b.poll(Instant::ZERO, &mut b_device, &mut b_set);
            b_set.close(bh);
            if orphaned { b_set.orphan(bh, Instant::ZERO); }
            for _ in 0..8 {
                b.poll(Instant::ZERO, &mut b_device, &mut b_set);
                a_device.rx_queue.append(&mut b_device.tx_queue);
                a.poll(Instant::ZERO, &mut a_device, &mut a_set);
                b_device.rx_queue.append(&mut a_device.tx_queue);
            }
            assert_eq!(b_set.get(bh).state(), tcp::State::Closed);
            let expected_b = [bh];
            assert_reapable(&mut b_set, if orphaned { &expected_b } else { &[] });
            assert_eq!(a_set.get(ah).state(), tcp::State::TimeWait);
            assert_reapable(&mut a_set, &[]);
            if reset_with_syn {
                let listener = a_set.add(buffered());
                a_set.listen(listener, 1234).unwrap();
                b.poll_maintenance(Instant::from_millis(1));
                b_set.connect(bh, b.context(), (crate::wire::IpAddress::v4(192, 168, 1, 1), 1234), 80).unwrap();
                b.poll_egress(Instant::from_millis(1), &mut b_device, &mut b_set);
                let bytes = b_device.tx_queue.front_mut().unwrap();
                let mut ip = crate::wire::Ipv4Packet::new_unchecked(bytes.as_mut_slice());
                let (src, dst) = (ip.src_addr().into(), ip.dst_addr().into());
                let mut syn = crate::wire::TcpPacket::new_unchecked(ip.payload_mut());
                syn.set_seq_number(syn.seq_number() + 3);
                syn.fill_checksum(&src, &dst);
                a_device.rx_queue.append(&mut b_device.tx_queue);
                a.poll(Instant::from_millis(1), &mut a_device, &mut a_set);
                assert_eq!(a_set.get(listener).state(), tcp::State::SynReceived);
            } else {
                a.poll_egress(Instant::from_secs(10), &mut a_device, &mut a_set);
            }
            assert_eq!(a_set.get(ah).state(), tcp::State::Closed);
            assert_eq!(a_set.get(ah).remote_endpoint(), None);
            let expected_a = [ah];
            assert_reapable(&mut a_set, if orphaned { &expected_a } else { &[] });
            a.poll_egress(Instant::from_secs(11), &mut a_device, &mut a_set);
            assert_reapable(&mut a_set, &[]);
        }
    }

    #[cfg(feature = "medium-ip")]
    #[test]
    fn reapable_retry_limit_and_ingress_rst() {
        use crate::phy::Medium;
        use crate::wire::{IpAddress, IpCidr};

        for (orphaned, rst) in [(true, false), (false, false), (true, true), (false, true)] {
            let (mut a, mut a_set, mut a_device) = crate::tests::setup(Medium::Ip);
            let (mut b, mut b_set, mut b_device) = crate::tests::setup(Medium::Ip);
            let remote = IpAddress::v4(192, 168, 1, 2);
            b.update_ip_addrs(|addrs| {
                addrs.clear();
                addrs.push(IpCidr::new(remote, 24)).unwrap();
            });
            let buffered = || tcp::Socket::test_socket(64, 64);
            let ah = a_set.add(buffered());
            let bh = b_set.add(buffered());
            b_set.listen(bh, 80).unwrap();
            a_set.connect(ah, a.context(), (remote, 80), 1234).unwrap();
            for _ in 0..8 {
                a.poll(Instant::ZERO, &mut a_device, &mut a_set);
                b_device.rx_queue.append(&mut a_device.tx_queue);
                b.poll(Instant::ZERO, &mut b_device, &mut b_set);
                a_device.rx_queue.append(&mut b_device.tx_queue);
            }
            assert_eq!(a_set.get(ah).state(), tcp::State::Established);
            a_set.close(ah);
            if orphaned { a_set.orphan(ah, Instant::ZERO); }
            a.poll_egress(Instant::ZERO, &mut a_device, &mut a_set);
            if rst {
                b_set.abort(bh);
                b.poll_egress(Instant::from_millis(1), &mut b_device, &mut b_set);
                a_device.rx_queue.append(&mut b_device.tx_queue);
                a.poll(Instant::from_millis(1), &mut a_device, &mut a_set);
            } else {
                let mut now = Instant::ZERO;
                for _ in 0..10_000 {
                    if a_set.get(ah).state() == tcp::State::Closed
                        || (!orphaned && a_device.tx_queue.len() == 10) {
                        break;
                    }
                    now = a.poll_at(now, &mut a_set).unwrap();
                    a.poll_egress(now, &mut a_device, &mut a_set);
                }
                assert_eq!(a_device.tx_queue.len(), if orphaned { 9 } else { 10 });
            }
            if orphaned || rst {
                assert_eq!(a_set.get(ah).state(), tcp::State::Closed);
                assert_eq!(a_set.get(ah).remote_endpoint(), None);
            } else {
                assert_eq!(a_set.get(ah).state(), tcp::State::FinWait1);
                assert!(a_set.get(ah).remote_endpoint().is_some());
            }
            let expected = [ah];
            assert_reapable(&mut a_set, if orphaned { &expected } else { &[] });
            assert_reapable(&mut b_set, &[]);
        }
    }

    #[test]
    fn orphan_stale_handle_panics() {
        let mut set = TcpSet::new(64);
        let stale = set.add(socket());
        set.remove(stale);
        let current = set.add(socket());
        assert_panics(INVALID, || set.orphan(stale, Instant::ZERO));
        assert_eq!(set.get(current).state(), tcp::State::Closed);
    }

    #[cfg(feature = "medium-ip")]
    #[test]
    fn orphan_established_schedules_fin_and_close_deadline() {
        use crate::phy::Medium;
        use crate::wire::{IpAddress, IpCidr, Ipv4Packet, TcpPacket};

        let (mut a, mut a_set, mut a_device) = crate::tests::setup(Medium::Ip);
        let (mut b, mut b_set, mut b_device) = crate::tests::setup(Medium::Ip);
        let remote = IpAddress::v4(192, 168, 1, 2);
        b.update_ip_addrs(|addrs| {
            addrs.clear();
            addrs.push(IpCidr::new(remote, 24)).unwrap();
        });
        let buffered = || tcp::Socket::test_socket(64, 64);
        let a_handle = a_set.add(buffered());
        let b_handle = b_set.add(buffered());
        b_set.listen(b_handle, 80).unwrap();
        a_set.connect(a_handle, a.context(), (remote, 80), 1234).unwrap();
        for _ in 0..8 {
            a.poll(Instant::ZERO, &mut a_device, &mut a_set);
            b_device.rx_queue.append(&mut a_device.tx_queue);
            b.poll(Instant::ZERO, &mut b_device, &mut b_set);
            a_device.rx_queue.append(&mut b_device.tx_queue);
        }
        assert_eq!(a_set.get(a_handle).state(), tcp::State::Established);
        assert_eq!(b_set.get(b_handle).state(), tcp::State::Established);
        assert_eq!(a.poll_at(Instant::ZERO, &mut a_set), None);
        assert!(a_device.tx_queue.is_empty());
        a_set.orphan(a_handle, Instant::from_millis(10));
        assert_eq!(a_set.get(a_handle).state(), tcp::State::FinWait1);
        a.poll_egress(Instant::from_millis(10), &mut a_device, &mut a_set);
        assert_eq!(a_device.tx_queue.len(), 1);
        let ip = Ipv4Packet::new_checked(a_device.tx_queue.front().unwrap().as_slice()).unwrap();
        let segment = TcpPacket::new_checked(ip.payload()).unwrap();
        assert!(segment.fin());
        assert!(!segment.rst());
        b_device.rx_queue.append(&mut a_device.tx_queue);
        b.poll(Instant::from_millis(20), &mut b_device, &mut b_set);
        assert_eq!(b_set.get(b_handle).state(), tcp::State::CloseWait);
        a_device.rx_queue.append(&mut b_device.tx_queue);
        a.poll(Instant::from_millis(20), &mut a_device, &mut a_set);
        assert_eq!(a_set.get(a_handle).state(), tcp::State::FinWait2);
        // The timer wheel reports a boundary no later than the 60 s deadline.
        let at = a.poll_at(Instant::from_millis(20), &mut a_set).unwrap();
        assert!(at > Instant::from_millis(20) && at <= Instant::from_millis(60_020));
        a.poll_egress(Instant::from_millis(60_019), &mut a_device, &mut a_set);
        assert_eq!(a_set.get(a_handle).state(), tcp::State::FinWait2);
        a.poll_egress(Instant::from_millis(60_020), &mut a_device, &mut a_set);
        assert_eq!(a_set.get(a_handle).state(), tcp::State::Closed);
        assert_eq!(a_set.get(a_handle).remote_endpoint(), None);
        assert!(a_device.tx_queue.is_empty());
        assert_reapable(&mut a_set, &[a_handle]);
        a.poll_egress(Instant::from_millis(60_021), &mut a_device, &mut a_set);
        assert_reapable(&mut a_set, &[]);
        assert_reapable(&mut b_set, &[]);
    }

    #[cfg(feature = "medium-ip")]
    #[test]
    fn readiness_tokens_fire_on_ingress_app_changes_and_egress_state_changes() {
        use crate::phy::Medium;
        use crate::wire::{IpAddress, IpCidr};
        use ReadySide::{Rx, Tx};

        fn fired(set: &mut TcpSet) -> alloc::vec::Vec<u32> {
            let mut fired: alloc::vec::Vec<u32> = core::iter::from_fn(|| set.pop_fired()).collect();
            fired.sort_unstable();
            fired
        }

        let (mut a, mut a_set, mut a_device) = crate::tests::setup(Medium::Ip);
        let (mut b, mut b_set, mut b_device) = crate::tests::setup(Medium::Ip);
        let remote = IpAddress::v4(192, 168, 1, 2);
        b.update_ip_addrs(|addrs| {
            addrs.clear();
            addrs.push(IpCidr::new(remote, 24)).unwrap();
        });
        let buffered = || tcp::Socket::test_socket(64, 64);
        macro_rules! exchange {
            () => {
                for _ in 0..8 {
                    a.poll(Instant::ZERO, &mut a_device, &mut a_set);
                    b_device.rx_queue.append(&mut a_device.tx_queue);
                    b.poll(Instant::ZERO, &mut b_device, &mut b_set);
                    a_device.rx_queue.append(&mut b_device.tx_queue);
                }
            };
        }
        let ah = a_set.add(buffered());
        let bh = b_set.add(buffered());
        b_set.listen(bh, 80).unwrap();
        a_set.connect(ah, a.context(), (remote, 80), 1234).unwrap();
        exchange!();
        assert_eq!(a_set.get(ah).state(), tcp::State::Established);
        assert_eq!(a.poll_at(Instant::ZERO, &mut a_set), None);
        assert_eq!(fired(&mut a_set), [], "nothing was armed");

        // An idle poll and a read-only access fire nothing.
        a_set.arm(ah, Rx, 10);
        a_set.arm(ah, Tx, 11);
        a.poll(Instant::ZERO, &mut a_device, &mut a_set);
        a_set.get(ah);
        assert_eq!(fired(&mut a_set), []);

        // A received segment fires both halves, once.
        b_set.get_mut(bh).send_slice(b"data").unwrap();
        b.poll(Instant::ZERO, &mut b_device, &mut b_set);
        a_device.rx_queue.append(&mut b_device.tx_queue);
        a.poll(Instant::ZERO, &mut a_device, &mut a_set);
        assert!(a_set.get(ah).can_recv());
        assert_eq!(fired(&mut a_set), [10, 11]);
        a_set.get_mut(ah);
        assert_eq!(fired(&mut a_set), [], "a fired token is disarmed");

        // App changes and receive wakes fire; a re-arm replaces the token.
        a_set.arm(ah, Rx, 12);
        a_set.arm(ah, Rx, 13);
        a_set.get_mut(ah);
        assert_eq!(fired(&mut a_set), [13]);
        a_set.arm(ah, Tx, 14);
        a_set.rx_woken(ah);
        assert_eq!(fired(&mut a_set), [14]);

        // disarm reports whether the token was still armed.
        a_set.arm(ah, Rx, 15);
        assert!(a_set.disarm(ah, Rx));
        assert!(!a_set.disarm(ah, Rx));
        a_set.arm(ah, Rx, 16);
        a_set.get_mut(ah);
        assert!(!a_set.disarm(ah, Rx), "fired, not yet popped");
        assert_eq!(fired(&mut a_set), [16]);

        // TIME-WAIT ends on a timer in egress: no segment, no app call.
        a_set.close(ah);
        exchange!();
        b_set.close(bh);
        exchange!();
        assert_eq!(a_set.get(ah).state(), tcp::State::TimeWait);
        assert_eq!(fired(&mut a_set), []);
        a_set.arm(ah, Rx, 17);
        a_set.arm(ah, Tx, 18);
        a.poll_egress(Instant::from_secs(10), &mut a_device, &mut a_set);
        assert_eq!(a_set.get(ah).state(), tcp::State::Closed);
        assert_eq!(fired(&mut a_set), [17, 18]);

        // Orphaning (handle dropped) and removing disarm; the next socket in a slot starts
        // unarmed.
        a_set.arm(ah, Rx, 19);
        a_set.orphan(ah, Instant::from_secs(10));
        assert_eq!(fired(&mut a_set), []);
        let ch = a_set.add(buffered());
        a_set.arm(ch, Tx, 20);
        a_set.remove(ch);
        let dh = a_set.add(buffered());
        assert_eq!(dh.index, ch.index);
        a_set.get_mut(dh);
        assert_eq!(fired(&mut a_set), []);
    }

    fn assert_panics(message: &str, f: impl FnOnce()) {
        let payload = std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)).unwrap_err();
        let actual = payload.downcast_ref::<&str>().copied().or_else(|| {
            payload
                .downcast_ref::<std::string::String>()
                .map(|s| s.as_str())
        });
        assert_eq!(actual, Some(message));
    }

    #[test]
    fn template_rejects_zero_source_port() {
        use crate::iface::header_template::TcpTemplate;
        use crate::wire::{EthernetAddress, IpProtocol, Ipv4Address, Ipv4Repr};

        let ip = Ipv4Repr {
            src_addr: Ipv4Address::new(192, 168, 1, 1),
            dst_addr: Ipv4Address::new(192, 168, 1, 2),
            next_header: IpProtocol::Tcp,
            payload_len: 20,
            hop_limit: 64,
        };
        assert_panics("TCP template source port must be nonzero", || {
            TcpTemplate::new(
                EthernetAddress([2, 0, 0, 0, 0, 1]),
                EthernetAddress([2, 0, 0, 0, 0, 2]),
                &ip,
                0,
                80,
                0,
            );
        });
    }

    fn handles(set: &TcpSet) -> Vec<usize> {
        set.iter().map(|(h, _)| h.index).collect()
    }

    #[cfg(feature = "medium-ip")]
    #[test]
    fn remove_discards_unread_received_bytes() {
        use crate::phy::Medium;
        use crate::wire::IpAddress;

        let (mut a, mut a_set, mut a_device) = crate::tests::setup(Medium::Ip);
        let (mut b, mut b_set, mut b_device) = crate::tests::setup(Medium::Ip);
        let remote = IpAddress::v4(192, 168, 1, 2);
        b.update_ip_addrs(|addrs| {
            addrs.clear();
            addrs.push(crate::wire::IpCidr::new(remote, 24)).unwrap();
        });
        let buffered = || tcp::Socket::test_socket(64, 64);
        let a_handle = a_set.add(buffered());
        let b_handle = b_set.add(buffered());
        b_set.listen(b_handle, 80).unwrap();
        a_set.connect(a_handle, a.context(), (remote, 80), 1234)
            .unwrap();
        let drive = |a: &mut crate::iface::Interface,
                     a_set: &mut TcpSet,
                     a_device: &mut crate::tests::TestingDevice,
                     b: &mut crate::iface::Interface,
                     b_set: &mut TcpSet,
                     b_device: &mut crate::tests::TestingDevice| {
            for _ in 0..8 {
                a.poll(Instant::ZERO, a_device, a_set);
                b_device.rx_queue.append(&mut a_device.tx_queue);
                b.poll(Instant::ZERO, b_device, b_set);
                a_device.rx_queue.append(&mut b_device.tx_queue);
            }
        };
        drive(
            &mut a,
            &mut a_set,
            &mut a_device,
            &mut b,
            &mut b_set,
            &mut b_device,
        );
        assert_eq!(a_set.get(a_handle).state(), tcp::State::Established);
        assert_eq!(b_set.get(b_handle).state(), tcp::State::Established);
        assert_eq!(a_set.get_mut(a_handle).send_slice(b"unread"), Ok(6));
        drive(
            &mut a,
            &mut a_set,
            &mut a_device,
            &mut b,
            &mut b_set,
            &mut b_device,
        );
        assert_eq!(b_set.get(b_handle).recv_queue(), 6);
        assert_eq!(b_set.get_mut(b_handle).send_slice(b"queued"), Ok(6));
        let removed = b_set.take(b_handle);
        assert_eq!(removed.recv_queue(), 0);
        assert_eq!(removed.recv_capacity(), 64);
        assert_eq!(removed.send_queue(), 6);
        assert_eq!(removed.state(), tcp::State::Established);
        let b_handle = b_set.add(removed);
        assert_eq!(a_set.get_mut(a_handle).send_slice(b"next"), Ok(4));
        drive(
            &mut a,
            &mut a_set,
            &mut a_device,
            &mut b,
            &mut b_set,
            &mut b_device,
        );
        let mut bytes = Vec::new();
        while let Some(frame) = b_set.get_mut(b_handle).recv().unwrap() {
            bytes.extend_from_slice(frame.payload());
        }
        assert_eq!(bytes.len(), 4);
        assert_eq!(&bytes, b"next");
    }

    #[cfg(feature = "medium-ip")]
    #[test]
    fn add_admitted_preserves_configured_rx_budget() {
        use crate::phy::Medium;
        use crate::wire::IpAddress;

        let mut set = TcpSet::new(64);
        let config = tcp::Config { rx_capacity: 4096, tx_capacity: 64, mss_cap: Some(100) };
        let budget = tcp::rx_mbuf_budget(&config, 1500);
        let handle = set.add_admitted(
            &config,
            1500,
            crate::tests::tx_test_source(1460),
            super::super::DropQueue::with_capacity(1, 0),
        );
        // Raising the cap after admission must not change the admitted frame count.
        let (mut iface, _, _) = crate::tests::setup(Medium::Ip);
        let socket = set.get_mut(handle);
        socket.set_mss_cap(Some(1000));
        socket
            .connect(iface.context(), (IpAddress::v4(192, 168, 1, 2), 80), 1234)
            .unwrap();
        assert_eq!(socket.state(), tcp::State::SynSent);
        // 4096 bytes / 100-byte frames = 41; a 1000-byte cap would give the six-frame floor.
        assert_eq!(budget, 41);
        assert_eq!(socket.rx_frame_cap(), budget);
    }

    #[test]
    fn add_reuses_lowest_free_slot() {
        let mut set = TcpSet::new(131);
        let h: Vec<_> = (0..130).map(|_| set.add(socket())).collect();
        set.remove(h[70]);
        set.remove(h[3]);
        set.remove(h[129]);
        assert_eq!(set.add(socket()), at(3, 1));
        assert_eq!(set.add(socket()), at(70, 1));
        assert_eq!(set.add(socket()), at(129, 1));
        assert_eq!(set.add(socket()), at(130, 0));
    }

    #[test]
    fn add_takes_lowest_empty_slot() {
        let mut set = TcpSet::new(65);
        for i in 0..65 {
            assert_eq!(set.add(socket()), at(i, 0));
        }
        set.remove(at(64, 0));
        assert_eq!(set.add(socket()), at(64, 1));
        assert_eq!(set.sockets.len(), 65);
    }

    #[test]
    #[should_panic(expected = "adding a socket to a full TcpSet")]
    fn add_to_full_set_panics() {
        let mut set = TcpSet::new(65);
        for _ in 0..65 {
            set.add(socket());
        }
        set.add(socket());
    }

    #[test]
    fn capacity_count_tracks_interleaved_add_remove() {
        let mut set = TcpSet::new(64);
        assert_eq!(set.len(), 0);
        let a = set.add(socket());
        let b = set.add(socket());
        let c = set.add(socket());
        assert_eq!(set.len(), 3);
        set.remove(b);
        assert_eq!(set.len(), 2);
        assert_eq!(handles(&set), vec![a.index, c.index]);
        let b2 = set.add(socket());
        set.remove(a);
        set.remove(c);
        assert_eq!(set.len(), 1);
        assert_eq!(handles(&set), vec![b.index]);
        set.remove(b2);
        assert_eq!(set.len(), 0);
        assert_eq!(set.len(), set.iter().count());
    }

    #[test]
    fn remove_of_empty_slot_panics_and_changes_nothing() {
        let mut set = TcpSet::new(64);
        let a = set.add(socket());
        let b = set.add(socket());
        set.remove(a);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| set.remove(a)));
        assert!(result.is_err());
        assert_eq!(set.len(), 1);
        assert_eq!(handles(&set), vec![b.index]);
        assert_eq!(set.add(socket()), at(a.index, 1));
    }

    #[test]
    fn stale_handle_after_reuse_panics() {
        let mut set = TcpSet::new(64);
        let a = set.add(socket());
        set.remove(a);
        let b = set.add(socket());
        assert_eq!(b.to_string(), a.to_string());
        assert_ne!(b, a);
        assert_panics(INVALID, || {
            set.get_mut(a);
        });
        assert_panics(INVALID, || {
            set.remove(a);
        });
        assert_eq!(set.len(), 1);
        assert_eq!(set.iter().map(|(h, _)| h).collect::<Vec<_>>(), vec![b]);
        set.get_mut(b);
    }

    #[test]
    #[should_panic(expected = "handle does not refer to a valid socket")]
    fn stale_handle_after_remove_panics() {
        let mut set = TcpSet::new(64);
        let a = set.add(socket());
        set.remove(a);
        set.get(a);
    }

    #[test]
    fn generation_survives_reuse_past_the_first_word() {
        let mut set = TcpSet::new(71);
        let h: Vec<_> = (0..70).map(|_| set.add(socket())).collect();
        for (i, handle) in h.iter().enumerate() {
            assert_eq!(*handle, at(i, 0));
        }
        set.remove(h[66]);
        let reused = set.add(socket());
        assert_eq!(reused, at(66, 1));
        set.remove(reused);
        assert_eq!(set.add(socket()), at(66, 2));
        assert_eq!(set.add(socket()), at(70, 0));
    }

    #[test]
    #[should_panic(expected = "handle does not refer to a valid socket")]
    fn stale_handle_get_after_reuse_panics() {
        let mut set = TcpSet::new(64);
        let a = set.add(socket());
        set.remove(a);
        set.add(socket());
        set.get(a);
    }

    #[test]
    fn generation_overflow_panics_and_changes_nothing() {
        let mut set = TcpSet::new(64);
        set.add(socket());
        set.sockets[0].generation = u64::MAX;
        let a = set.iter().next().unwrap().0;
        assert_panics("socket slot generation overflow", || {
            set.remove(a);
        });
        assert_eq!(set.len(), 1);
        assert_eq!(set.iter().map(|(h, _)| h).collect::<Vec<_>>(), vec![a]);
        set.get_mut(a);
        assert_eq!(set.add(socket()), at(1, 0));
    }

    #[test]
    fn iter_handles_match_add_after_reuse() {
        let mut set = TcpSet::new(64);
        let old = set.add(socket());
        set.remove(old);
        let reused = set.add(socket());
        assert_eq!(reused, at(old.index, old.generation + 1));
        assert_eq!(set.iter().map(|(h, _)| h).collect::<Vec<_>>(), vec![reused]);
        assert_eq!(
            set.iter_mut().map(|(h, _)| h).collect::<Vec<_>>(),
            vec![reused],
        );
        assert_panics(INVALID, || {
            set.get(old);
        });
    }

    #[test]
    fn tcp_set_iter_yields_tcp_sockets_with_handles() {
        let mut set = TcpSet::new(64);
        let a = set.add(socket());
        let b = set.add(socket());
        let c = set.add(socket());
        set.remove(b);
        let b2 = set.add(socket());
        assert_eq!(b2, at(b.index, 1));
        assert_ne!(b2, b);

        assert_eq!(
            set.iter().map(|(h, _)| h).collect::<Vec<_>>(),
            vec![a, b2, c]
        );
        for (handle, socket) in set.iter() {
            assert!(core::ptr::eq(socket, set.get(handle)));
        }
    }

    #[cfg(feature = "medium-ip")]
    mod flow_index {
        use super::*;
        use crate::phy::Medium;
        use crate::wire::IpAddress;

        fn endpoints() -> (IpEndpoint, IpEndpoint) {
            (
                IpEndpoint::new(IpAddress::v4(192, 168, 1, 1), 1234),
                IpEndpoint::new(IpAddress::v4(192, 168, 1, 2), 65000),
            )
        }

        fn connected(local: IpEndpoint, remote: IpEndpoint) -> tcp::Socket<'static> {
            let (mut iface, _, _) = crate::tests::setup(Medium::Ip);
            let mut socket = socket();
            socket.connect(iface.context(), remote, local).unwrap();
            assert_eq!(socket.state(), tcp::State::SynSent);
            socket
        }

        fn listener(port: u16) -> tcp::Socket<'static> {
            let mut socket = socket();
            socket.listen(port).unwrap();
            socket
        }

        /// Each identity operation updates the flow index in its call, before any poll: a
        /// connect's tuple and a listener's port are found at once, and close and abort remove
        /// them at once.
        #[test]
        fn identity_operations_update_the_index_in_the_call() {
            let (mut iface, mut set, _) = crate::tests::setup(Medium::Ip);
            let (local, remote) = endpoints();
            let client = set.add(socket());
            let server = set.add(socket());
            set.connect(client, iface.context(), remote, local).unwrap();
            set.listen(server, local.port + 1).unwrap();
            set.check_flow_index();
            let syn_to_server = IpEndpoint { port: local.port + 1, ..local };
            assert_eq!(set.tcp_lookup(local, remote, false), Some(client.index));
            assert_eq!(set.tcp_lookup(syn_to_server, remote, true), Some(server.index));
            set.abort(client);
            set.close(server);
            set.check_flow_index();
            assert_eq!(set.tcp_lookup(local, remote, false), None);
            assert_eq!(set.tcp_lookup(syn_to_server, remote, true), None);
        }

        /// A connect on a tuple a live socket holds is refused and changes nothing; once that
        /// socket is closed by a reset (it keeps the tuple only to send its RST), the tuple is
        /// free again.
        #[test]
        fn connect_on_a_live_tuple_is_refused() {
            let (mut iface, mut set, _) = crate::tests::setup(Medium::Ip);
            let (local, remote) = endpoints();
            let first = set.add(socket());
            let second = set.add(socket());
            set.connect(first, iface.context(), remote, local).unwrap();
            assert_eq!(
                set.connect(second, iface.context(), remote, local),
                Err(tcp::ConnectError::AddressInUse)
            );
            assert_eq!(set.get(second).state(), tcp::State::Closed);
            assert_eq!(set.get(second).local_endpoint(), None);
            set.check_flow_index();
            set.abort(first);
            set.connect(second, iface.context(), remote, local).unwrap();
            set.check_flow_index();
            assert_eq!(set.tcp_lookup(local, remote, false), Some(second.index));
        }

        #[test]
        fn mutable_access_unlinks_tcp_timer() {
            let (mut iface, mut set, mut device) = crate::tests::setup(Medium::Ip);
            let (local, remote) = endpoints();
            let handle = set.add(connected(local, remote));
            iface.poll_egress(Instant::ZERO, &mut device, &mut set);
            iface.poll_at(Instant::ZERO, &mut set);
            assert!(set.wheel.has_pending());
            assert_ne!(
                set.flow_slots[handle.index].timer_position,
                WheelPosition::Unlinked
            );
            set.get_mut(handle);
            assert!(!set.wheel.has_pending());
            assert_eq!(
                set.flow_slots[handle.index].timer_position,
                WheelPosition::Unlinked
            );
            assert!(set.flow_slots[handle.index].dirty);
            iface.poll_at(Instant::ZERO, &mut set);
            assert!(set.wheel.has_pending());
            set.iter_mut().next().unwrap();
            assert!(!set.wheel.has_pending());
            iface.poll_at(Instant::ZERO, &mut set);
            assert!(set.wheel.has_pending());
            set.remove(handle);
            assert!(!set.wheel.has_pending());
            assert_eq!(iface.poll_at(Instant::ZERO, &mut set), None);
        }

        #[test]
        fn flow_index_tracks_add_remove_and_listen() {
            let (mut iface, _, _) = crate::tests::setup(Medium::Ip);
            let (local, remote) = endpoints();
            let mut set = TcpSet::new(64);
            let first = set.add(socket());
            set.check_flow_index();
            assert_eq!(set.tcp_lookup(local, remote, true), None);
            set.listen(first, local.port).unwrap();
            set.check_flow_index();
            iface.classify_dirty(&mut set, Instant::ZERO);
            set.check_flow_index();
            assert_eq!(set.tcp_lookup(local, remote, true), Some(first.index));
            assert_eq!(set.tcp_lookup(local, remote, false), None);
            let second = set.add(listener(local.port));
            iface.classify_dirty(&mut set, Instant::ZERO);
            set.check_flow_index();
            assert_eq!(set.tcp_lookup(local, remote, true), Some(first.index));
            assert_eq!(set.tcp_lookup(local, remote, false), None);
            set.remove(first);
            set.check_flow_index();
            assert_eq!(set.tcp_lookup(local, remote, true), Some(second.index));
            assert_eq!(set.tcp_lookup(local, remote, false), None);
            set.close(second);
            iface.classify_dirty(&mut set, Instant::ZERO);
            set.check_flow_index();
            assert_eq!(set.tcp_lookup(local, remote, true), None);
            set.listen(second, local.port).unwrap();
            iface.classify_dirty(&mut set, Instant::ZERO);
            set.remove(second);
            set.check_flow_index();
            assert_eq!(set.tcp_lookup(local, remote, true), None);
        }

        #[test]
        fn flow_index_filters_bound_listeners_and_keeps_lowest_match() {
            let (mut iface, _, _) = crate::tests::setup(Medium::Ip);
            let (local, remote) = endpoints();
            for exact_first in [false, true] {
                let mut set = TcpSet::new(64);
                let mut bound = socket();
                bound.listen((remote.addr, local.port)).unwrap();
                let bound = set.add(bound);
                let (first, second) = if exact_first {
                    (
                        set.add(connected(local, remote)),
                        set.add(listener(local.port)),
                    )
                } else {
                    (
                        set.add(listener(local.port)),
                        set.add(connected(local, remote)),
                    )
                };
                iface.classify_dirty(&mut set, Instant::ZERO);
                set.check_flow_index();
                let exact = if exact_first { first } else { second };
                assert_eq!(set.tcp_lookup(local, remote, true), Some(exact.index));
                assert_eq!(set.tcp_lookup(local, remote, false), Some(exact.index));
                let dst = IpEndpoint {
                    addr: remote.addr,
                    ..local
                };
                assert_eq!(set.tcp_lookup(dst, remote, true), Some(bound.index));
                set.remove(bound);
                set.check_flow_index();
                assert_eq!(set.tcp_lookup(local, remote, true), Some(exact.index));
            }
        }

        #[test]
        fn flow_index_marks_only_mutably_yielded_slots() {
            let (mut iface, _, _) = crate::tests::setup(Medium::Ip);
            let (local, remote) = endpoints();
            let mut set = TcpSet::new(64);
            let handles: Vec<_> = (0..3).map(|_| set.add(socket())).collect();
            iface.classify_dirty(&mut set, Instant::ZERO);
            set.get(handles[0]);
            assert_eq!(set.iter().count(), 3);
            assert!(set.dirty_list.is_empty());
            drop(set.iter_mut());
            assert!(set.dirty_list.is_empty());
            set.listen(handles[0], local.port).unwrap();
            set.get_mut(handles[0]);
            set.get_mut(handles[0]);
            assert_eq!(set.dirty_list, vec![0]);
            set.check_flow_index();
            assert_eq!(set.tcp_lookup(local, remote, true), Some(0));
            for &handle in &handles {
                set.abort(handle);
            }
            assert_eq!(set.dirty_list, vec![0, 1, 2]);
            set.check_flow_index();
            assert_eq!(set.tcp_lookup(local, remote, true), None);
        }

    }
}
