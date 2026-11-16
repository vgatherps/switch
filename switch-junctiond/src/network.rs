//! Applies control operations and reports TCP transitions without control syscalls.
//!
//! Opens, aborts and unread closes apply immediately. Shutdown and plain close validate
//! immediately but wait for a subsequent FIFO poll to find every ring empty. Removing a
//! socket discards all its held operations, including before same-generation reuse.
//! Idle iterations skip held-operation scans when none are queued. A consumed FIFO end
//! makes later session loss graceful unless the session has a protocol error. Explicit
//! aborts always reset sockets, even after a FIFO end. SEND records borrow their FIFO
//! message through dispatch; an unknown session first drains control through its queued
//! Open. Sends from older socket generations are dropped; unopened or future generations
//! are protocol errors. TCP backlogs delay shutdown and close, and outlive graceful retirement.
//! Closed sockets with backlogs use the Stack's orphan retry limit even while held here.
//! TCP receive frames split at the session ring's payload limit; oversized UDP datagrams
//! are dropped and counted. A full ring retains one owned frame and TCP offset per socket,
//! retrying before the next Stack turn and delaying receive EOF. Close and graceful
//! retirement reset TCP sockets with held receive bytes the application never received.
//! Control loss waits for a FIFO end or global FIFO idle before deciding graceful/reset.
//! Lost sessions without an end are reset before other sessions' held ops are released.

use std::collections::VecDeque;
use std::sync::mpsc::{Receiver, Sender};

use ceph_fifo::Fifo;
use ceph_proto::{slot, uid};
use dpdk_smoltcp::smoltcp::time::Instant;
use dpdk_smoltcp::{Stack, StackError, Turn};
use switch_junction::control::{Event, Op};
use switch_junction::host::{EndHow, FromNetwork, SessionId, ToNetwork};
use switch_junction::send::{self, SendRecord};
use slice_ringbuffer::RingWriter;

use crate::sockets::{self, HostSocket, Socket};

/// Per-iteration work limits; exhausting the FIFO batch does not release held operations.
pub struct NetworkConfig {
    pub ingress_budget: usize,
    pub control_batch: usize,
    pub fifo_batch: usize,
}

#[derive(Default)]
struct Slot {
    host: Option<u32>,
    generation: Option<u32>,
}

struct Session {
    uid: u32,
    rx: RingWriter,
    slots: Vec<Slot>,
    held: VecDeque<Op>,
    errored: bool,
    ended: bool,
    lost: bool,
}

/// Owns the Stack, session socket handles and FIFO on the network thread.
pub struct Network {
    fifo: Fifo,
    state: NetworkState,
    config: NetworkConfig,
    from_control: Receiver<ToNetwork>,
}

// Kept separate from the FIFO so control catch-up and SEND dispatch can run while
// a Message holds the FIFO's reader borrow.
struct NetworkState {
    stack: Stack,
    sessions: Vec<Option<Session>>,
    sockets: Vec<Option<HostSocket>>,
    free: Vec<u32>,
    backlogged: Vec<u32>,
    rx_pending: Vec<u32>,
    rx_cursor: usize,
    held_count: usize,
    lost_count: usize,
    udp_dropped: u64,
    udp_rx_dropped: u64,
    to_control: Sender<FromNetwork>,
}

impl Network {
    /// `max_sessions` must equal the Fifo's max_streams and the control thread's max_sessions.
    pub fn new(
        stack: Stack,
        fifo: Fifo,
        max_sessions: u32,
        config: NetworkConfig,
        from_control: Receiver<ToNetwork>,
        to_control: Sender<FromNetwork>,
    ) -> Network {
        let capacity = stack.max_sockets() + stack.max_udp_sockets();
        let capacity = u32::try_from(capacity).expect("host socket slots exceed readiness tokens");
        Network {
            fifo,
            state: NetworkState {
                stack,
                sessions: (0..max_sessions).map(|_| None).collect(),
                sockets: (0..capacity).map(|_| None).collect(),
                free: (0..capacity).rev().collect(),
                backlogged: Vec::new(),
                rx_pending: Vec::new(),
                rx_cursor: 0,
                held_count: 0,
                lost_count: 0,
                udp_dropped: 0,
                udp_rx_dropped: 0,
                to_control,
            },
            config,
            from_control,
        }
    }

    /// Drains bounded control and FIFO work, releases held operations on FIFO idle, then
    /// turns the Stack and reports every fired TCP transition, even if the turn failed.
    pub fn iteration(&mut self, now: Instant) -> Result<Turn, StackError> {
        let state = &mut self.state;
        for _ in 0..self.config.control_batch {
            let Ok(message) = self.from_control.try_recv() else {
                break;
            };
            state.receive_control(message);
        }

        let mut idle = false;
        for _ in 0..self.config.fifo_batch {
            let Some(message) = self.fifo.next_message() else {
                idle = true;
                break;
            };
            let session = message.stream();
            if state.live(session).is_none() {
                // FIFO registration can overtake the bounded control drain.
                while let Ok(control) = self.from_control.try_recv() {
                    state.receive_control(control);
                    if state.live(session).is_some() {
                        // Handle this record before later ops or terminal control messages.
                        break;
                    }
                }
            }
            if message.end() {
                if let Some(Some(entry)) = state.sessions.get_mut(slot(session) as usize)
                    && entry.uid == uid(session)
                {
                    entry.ended = true;
                    if entry.lost {
                        state.end(session, EndHow::Lost);
                    }
                }
            } else if state.live(session).is_some_and(|entry| !entry.errored) {
                state.receive_send(session, message.payload());
            }
        }
        if idle && state.lost_count != 0 {
            for index in 0..state.sessions.len() {
                if let Some(session) = &state.sessions[index]
                    && session.lost
                {
                    let id = ceph_proto::pack(index as u32, session.uid);
                    state.end(id, EndHow::Abort);
                }
            }
        }
        if idle && state.held_count != 0 {
            for index in 0..state.sessions.len() {
                if let Some(session) = &state.sessions[index]
                    && !session.errored
                {
                    let id = ceph_proto::pack(index as u32, session.uid);
                    state.release(id, true);
                }
            }
        }
        state.drain_backlogs();
        state.retry_rx();
        // ANCHOR: fired
        let turn = state.stack.turn(now, self.config.ingress_budget);
        while let Some(token) = state.stack.pop_fired() {
            state.service_rx(token);
        }
        // ANCHOR_END: fired
        turn
    }

    pub fn poll_at(&mut self) -> Option<Instant> {
        self.state.stack.poll_at()
    }

    /// Datagrams rejected by the Stack after their SEND records were committed.
    pub fn udp_dropped(&self) -> u64 {
        self.state.udp_dropped
    }

    /// Received datagrams exceeding their session ring's maximum payload.
    pub fn udp_rx_dropped(&self) -> u64 {
        self.state.udp_rx_dropped
    }

    /// Drops every remaining socket handle and returns the Stack (test teardown).
    pub fn into_stack(self) -> Stack {
        let NetworkState { stack, sockets, .. } = self.state;
        drop(sockets);
        stack
    }
}

impl NetworkState {
    fn receive_control(&mut self, message: ToNetwork) {
        match message {
            ToNetwork::Open {
                session,
                max_sockets,
                rx,
            } => {
                let entry = self
                    .sessions
                    .get_mut(slot(session) as usize)
                    .expect("control thread's max_sessions exceeds the network's");
                assert!(
                    entry.is_none(),
                    "opening an already registered session slot: {session}"
                );
                *entry = Some(Session {
                    uid: uid(session),
                    rx,
                    slots: (0..max_sockets).map(|_| Slot::default()).collect(),
                    held: VecDeque::new(),
                    errored: false,
                    ended: false,
                    lost: false,
                });
            }
            ToNetwork::Op { session, op } => self.receive_op(session, op),
            ToNetwork::End { session, how } => self.end(session, how),
        }
    }

    fn live(&self, session: SessionId) -> Option<&Session> {
        self.sessions
            .get(slot(session) as usize)?
            .as_ref()
            .filter(|entry| entry.uid == uid(session))
    }

    fn registered(&self, session: SessionId) -> &Session {
        self.live(session)
            .unwrap_or_else(|| panic!("operation for unregistered session: {session}"))
    }

    fn protocol_error(&mut self, session: SessionId) {
        self.rx_pending.retain(|index| {
            let socket = self.sockets[*index as usize].as_mut().unwrap();
            if socket.session == Some(session) {
                socket.held_rx = None;
                false
            } else {
                true
            }
        });
        let entry = self.sessions[slot(session) as usize].as_mut().unwrap();
        if !entry.errored {
            entry.errored = true;
            let _ = self.to_control.send(FromNetwork::ProtocolError { session });
        }
    }

    fn receive_send(&mut self, session: SessionId, payload: &[u8]) {
        let Some(record) = send::decode(payload) else {
            self.protocol_error(session);
            return;
        };
        let id = match &record {
            SendRecord::Tcp { id, .. } | SendRecord::Udp { id, .. } => *id,
        };
        let entry = self.registered(session);
        let Some(entry_slot) = entry.slots.get(id.slot as usize) else {
            self.protocol_error(session);
            return;
        };
        let Some(current) = entry_slot.generation else {
            self.protocol_error(session);
            return;
        };
        if current != id.generation {
            if (current.wrapping_sub(id.generation) as i32) <= 0 {
                self.protocol_error(session);
            }
            return;
        }
        let Some(index) = entry_slot.host else {
            return;
        };
        let socket = self.sockets[index as usize].as_mut().unwrap();
        match (record, &socket.socket) {
            (SendRecord::Tcp { payload, .. }, Socket::Tcp { .. }) => {
                let empty = socket.backlog.is_empty();
                socket.send_tcp(&mut self.stack, index, payload);
                if empty && !socket.backlog.is_empty() {
                    self.backlogged.push(index);
                }
            }
            (SendRecord::Udp { to, payload, .. }, Socket::Udp { handle, .. }) => {
                if self
                    .stack
                    .udp_send(handle, Some(to.into()), payload)
                    .is_err()
                {
                    self.udp_dropped += 1;
                }
            }
            _ => self.protocol_error(session),
        }
    }

    /// Services only attached, non-errored sessions; pending entries exactly name held frames.
    // ANCHOR: service_rx
    fn service_rx(&mut self, index: u32) {
        let Some(socket) = self.sockets[index as usize].as_mut() else {
            return;
        };
        let Some(session) = socket.session else {
            return;
        };
        let entry = self.sessions[slot(session) as usize]
            .as_mut()
            .expect("host socket belongs to an unregistered session");
        if entry.errored {
            return;
        }
        let was_pending = socket.held_rx.is_some();
        // A pending retry may run while armed; tcp_mut would fire another token.
        socket.socket.disarm(&mut self.stack);
        socket.receive(&mut self.stack, &mut entry.rx, &mut self.udp_rx_dropped);
        socket.fired(&mut self.stack, &self.to_control);
        socket.socket.arm(&mut self.stack, index);
        if socket.held_rx.is_some() {
            if !was_pending {
                self.rx_pending.push(index);
            }
        } else if was_pending {
            self.rx_pending.retain(|pending| *pending != index);
        }
    }
    // ANCHOR_END: service_rx

    fn retry_rx(&mut self) {
        let count = self.rx_pending.len();
        if count == 0 {
            return;
        }
        let mut position = self.rx_cursor % count;
        self.rx_cursor = position + 1;
        // Removal preserves order, so the next unvisited entry shifts into this position.
        // Visit the original count exactly once, wrapping after the original tail.
        for _ in 0..count {
            let index = self.rx_pending[position];
            self.service_rx(index);
            if self.rx_pending.get(position) == Some(&index) {
                position += 1;
            }
            if self.rx_pending.is_empty() {
                break;
            }
            position %= self.rx_pending.len();
        }
    }

    fn drain_backlogs(&mut self) {
        let mut position = 0;
        while position < self.backlogged.len() {
            let index = self.backlogged[position];
            let socket = self.sockets[index as usize].as_mut().unwrap();
            if let Some(session) = socket.session
                && self.sessions[slot(session) as usize]
                    .as_ref()
                    .unwrap()
                    .errored
            {
                position += 1;
                continue;
            }
            socket.drain_backlog(&mut self.stack, index);
            if !socket.backlog.is_empty() {
                position += 1;
                continue;
            }
            self.backlogged.swap_remove(position);
            let deferred = socket.deferred.take();
            let session = socket.session;
            if let Some(session) = session {
                if let Some(op) = deferred {
                    self.apply(session, op, true);
                }
            } else {
                self.remove_socket(index, false);
            }
        }
    }

    fn receive_op(&mut self, session: SessionId, op: Op) {
        let entry = self.registered(session);
        if entry.errored {
            return;
        }
        let id = op.id();
        let entry_slot = entry
            .slots
            .get(id.slot as usize)
            .expect("control thread forwarded an out-of-range socket slot");
        let open = matches!(
            op,
            Op::ConnectTcp { .. } | Op::ListenTcp { .. } | Op::BindUdp { .. }
        );
        if open {
            if entry_slot.host.is_some() {
                self.protocol_error(session);
                return;
            }
            self.sessions[slot(session) as usize]
                .as_mut()
                .unwrap()
                .slots[id.slot as usize]
                .generation = Some(id.generation);
            match Socket::open(&mut self.stack, op) {
                Ok(socket) => {
                    let bound = !socket.is_tcp();
                    let index = self
                        .free
                        .pop()
                        .expect("Stack admitted more sockets than the host table holds");
                    socket.arm(&mut self.stack, index);
                    self.sockets[index as usize] = Some(HostSocket {
                        session: Some(session),
                        id,
                        socket,
                        backlog: VecDeque::new(),
                        deferred: None,
                        held_rx: None,
                    });
                    self.sessions[slot(session) as usize]
                        .as_mut()
                        .unwrap()
                        .slots[id.slot as usize]
                        .host = Some(index);
                    if bound {
                        sockets::event(&self.to_control, session, id, Event::Bound);
                    }
                }
                Err(reason) => {
                    sockets::event(&self.to_control, session, id, Event::Rejected(reason))
                }
            }
            return;
        }
        if entry_slot.generation != Some(id.generation) {
            self.protocol_error(session);
            return;
        }
        let Some(index) = entry_slot.host else {
            return;
        };
        if matches!(op, Op::ShutdownTcp { .. } | Op::AbortTcp { .. })
            && !self.sockets[index as usize]
                .as_ref()
                .unwrap()
                .socket
                .is_tcp()
        {
            self.protocol_error(session);
            return;
        }
        if matches!(op, Op::ShutdownTcp { .. } | Op::Close { unread: false, .. }) {
            // Validate now; socket removal discards this operation before slot reuse.
            self.sessions[slot(session) as usize]
                .as_mut()
                .unwrap()
                .held
                .push_back(op);
            self.held_count += 1;
        } else {
            self.apply(session, op, true);
        }
    }

    fn apply(&mut self, session: SessionId, op: Op, events: bool) {
        let id = op.id();
        let entry_slot = &self.sessions[slot(session) as usize]
            .as_ref()
            .unwrap()
            .slots[id.slot as usize];
        if entry_slot.generation != Some(id.generation) {
            return;
        }
        let Some(index) = entry_slot.host else {
            return;
        };
        let socket = self.sockets[index as usize].as_mut().unwrap();
        if !socket.backlog.is_empty()
            && !(matches!(op, Op::Close { .. }) && socket.has_unread_rx())
            && matches!(op, Op::ShutdownTcp { .. } | Op::Close { unread: false, .. })
        {
            if matches!(op, Op::Close { unread: false, .. })
                && let Socket::Tcp { handle, .. } = &socket.socket
            {
                self.stack.tcp_mut(handle).limit_retries();
            }
            if socket.deferred.is_none() || matches!(op, Op::Close { .. }) {
                socket.deferred = Some(op);
            }
            return;
        }
        if matches!(op, Op::ShutdownTcp { .. }) {
            self.sockets[index as usize]
                .as_mut()
                .unwrap()
                .socket
                .shutdown(&mut self.stack);
        } else {
            let abort = matches!(op, Op::AbortTcp { .. } | Op::Close { unread: true, .. });
            self.remove_socket(index, abort);
            let entry = self.sessions[slot(session) as usize].as_mut().unwrap();
            entry.slots[id.slot as usize].host = None;
            let held_before = entry.held.len();
            entry.held.retain(|held| held.id() != id);
            self.held_count -= held_before - entry.held.len();
            if events {
                sockets::event(&self.to_control, session, id, Event::Freed);
            }
        }
    }

    fn remove_socket(&mut self, index: u32, abort: bool) {
        let socket = self.sockets[index as usize].take().unwrap();
        self.rx_pending.retain(|pending| *pending != index);
        if !socket.backlog.is_empty() {
            self.backlogged.retain(|pending| *pending != index);
        }
        if abort || socket.has_unread_rx() {
            socket.socket.abort(&mut self.stack);
        }
        socket.socket.drop_handle();
        self.free.push(index);
    }

    fn release(&mut self, session: SessionId, events: bool) {
        while let Some(op) = self.sessions[slot(session) as usize]
            .as_mut()
            .unwrap()
            .held
            .pop_front()
        {
            self.held_count -= 1;
            self.apply(session, op, events);
        }
    }

    /// Ignore stale terminal messages: control EOF can race an already-applied graceful
    /// retirement, and the session slot may already belong to a different uid.
    fn end(&mut self, session: SessionId, how: EndHow) {
        let Some(entry) = self.live(session) else {
            return;
        };
        // We sent ProtocolError; control answers End{Abort} to perform teardown.
        if how == EndHow::Graceful && entry.errored {
            return;
        }
        if how == EndHow::Lost && !entry.errored && !entry.ended {
            let entry = self.sessions[slot(session) as usize].as_mut().unwrap();
            if !entry.lost {
                entry.lost = true;
                self.lost_count += 1;
            }
            return;
        }
        let graceful = !entry.errored
            && match how {
                EndHow::Graceful => true,
                EndHow::Lost => entry.ended,
                EndHow::Abort => false,
            };
        if graceful {
            self.release(session, false);
        }
        let entry = self.sessions[slot(session) as usize].take().unwrap();
        self.lost_count -= usize::from(entry.lost);
        self.held_count -= entry.held.len();
        for slot in entry.slots {
            if let Some(index) = slot.host {
                let socket = self.sockets[index as usize].as_mut().unwrap();
                if graceful && !socket.backlog.is_empty() && !socket.has_unread_rx() {
                    if let Socket::Tcp { handle, .. } = &socket.socket {
                        self.stack.tcp_mut(handle).limit_retries();
                    }
                    socket.session = None;
                    socket.held_rx = None;
                    self.rx_pending.retain(|pending| *pending != index);
                } else {
                    self.remove_socket(index, !graceful);
                }
            }
        }
        let message = if graceful {
            FromNetwork::Retired {
                session,
                rx: entry.rx,
            }
        } else {
            FromNetwork::Released {
                session,
                rx: entry.rx,
            }
        };
        let _ = self.to_control.send(message);
    }
}
