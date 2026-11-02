//! Applies control operations and reports TCP transitions without control syscalls.
//!
//! Opens and aborts apply immediately. Shutdown and plain close validate immediately but
//! wait for a subsequent FIFO poll to find every ring empty. A later abort cancels a held
//! operation by removing its socket. Until packet records are defined, every non-end
//! FIFO record is a session protocol error.

use std::collections::VecDeque;
use std::sync::mpsc::{Receiver, Sender};

use ceph_fifo::Fifo;
use ceph_proto::{slot, uid};
use dpdk_smoltcp::smoltcp::time::Instant;
use dpdk_smoltcp::{Stack, StackError, Turn};
use junction::control::{Event, Op};
use junction::host::{EndHow, FromNetwork, SessionId, ToNetwork};

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
    slots: Vec<Slot>,
    held: VecDeque<Op>,
    errored: bool,
}

/// Owns the Stack, session socket handles and FIFO on the network thread.
pub struct Network {
    stack: Stack,
    fifo: Fifo,
    sessions: Vec<Option<Session>>,
    sockets: Vec<Option<HostSocket>>,
    free: Vec<u32>,
    config: NetworkConfig,
    from_control: Receiver<ToNetwork>,
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
            stack,
            fifo,
            sessions: (0..max_sessions).map(|_| None).collect(),
            sockets: (0..capacity).map(|_| None).collect(),
            free: (0..capacity).rev().collect(),
            config,
            from_control,
            to_control,
        }
    }

    /// Drains bounded control and FIFO work, releases held operations on FIFO idle, then
    /// turns the Stack and reports every fired TCP transition, even if the turn failed.
    pub fn iteration(&mut self, now: Instant) -> Result<Turn, StackError> {
        for _ in 0..self.config.control_batch {
            let Ok(message) = self.from_control.try_recv() else {
                break;
            };
            match message {
                ToNetwork::Open {
                    session,
                    max_sockets,
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
                        slots: (0..max_sockets).map(|_| Slot::default()).collect(),
                        held: VecDeque::new(),
                        errored: false,
                    });
                }
                ToNetwork::Op { session, op } => self.receive_op(session, op),
                ToNetwork::End { session, how } => self.end(session, how),
            }
        }

        let mut idle = false;
        for _ in 0..self.config.fifo_batch {
            let Some(message) = self.fifo.next_message() else {
                idle = true;
                break;
            };
            let session = message.stream();
            let end = message.end();
            drop(message);
            if !end && self.live(session).is_some_and(|session| !session.errored) {
                self.protocol_error(session);
            }
        }
        if idle {
            for index in 0..self.sessions.len() {
                if let Some(session) = &self.sessions[index]
                    && !session.errored
                {
                    let id = ceph_proto::pack(index as u32, session.uid);
                    self.release(id, true);
                }
            }
        }
        let turn = self.stack.turn(now, self.config.ingress_budget);
        while let Some(token) = self.stack.pop_fired() {
            if let Some(socket) = &mut self.sockets[token as usize] {
                let session = self.sessions[slot(socket.session) as usize]
                    .as_ref()
                    .expect("host socket belongs to an unregistered session");
                if !session.errored {
                    socket.fired(&mut self.stack, &self.to_control, token);
                }
            }
        }
        turn
    }

    pub fn poll_at(&mut self) -> Option<Instant> {
        self.stack.poll_at()
    }

    /// Drops every remaining socket handle and returns the Stack (test teardown).
    pub fn into_stack(self) -> Stack {
        let Self { stack, sockets, .. } = self;
        drop(sockets);
        stack
    }

    fn live(&self, session: SessionId) -> Option<&Session> {
        self.sessions
            .get(slot(session) as usize)?
            .as_ref()
            .filter(|entry| entry.uid == uid(session))
    }

    fn registered(&self, session: SessionId) -> &Session {
        self.live(session)
            .unwrap_or_else(|| panic!("operation or end for unregistered session: {session}"))
    }

    fn protocol_error(&mut self, session: SessionId) {
        let entry = self.sessions[slot(session) as usize].as_mut().unwrap();
        if !entry.errored {
            entry.errored = true;
            let _ = self.to_control.send(FromNetwork::ProtocolError { session });
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
                        session,
                        id,
                        socket,
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
            // Validate now; a later abort may remove this socket before the FIFO is idle.
            self.sessions[slot(session) as usize]
                .as_mut()
                .unwrap()
                .held
                .push_back(op);
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
        if matches!(op, Op::ShutdownTcp { .. }) {
            self.sockets[index as usize]
                .as_mut()
                .unwrap()
                .socket
                .shutdown(&mut self.stack);
        } else {
            let socket = self.sockets[index as usize].take().unwrap();
            if matches!(op, Op::AbortTcp { .. } | Op::Close { unread: true, .. }) {
                socket.socket.abort(&mut self.stack);
            }
            socket.socket.drop_handle();
            self.free.push(index);
            self.sessions[slot(session) as usize]
                .as_mut()
                .unwrap()
                .slots[id.slot as usize]
                .host = None;
            if events {
                sockets::event(&self.to_control, session, id, Event::Freed);
            }
        }
    }

    fn release(&mut self, session: SessionId, events: bool) {
        while let Some(op) = self.sessions[slot(session) as usize]
            .as_mut()
            .unwrap()
            .held
            .pop_front()
        {
            self.apply(session, op, events);
        }
    }

    fn end(&mut self, session: SessionId, how: EndHow) {
        let entry = self.registered(session);
        if how == EndHow::Graceful {
            // ProtocolError has already requested End{Lost}; it owns this teardown.
            if entry.errored {
                return;
            }
            self.release(session, false);
        }
        let entry = self.sessions[slot(session) as usize].take().unwrap();
        for slot in entry.slots {
            if let Some(index) = slot.host {
                let socket = self.sockets[index as usize].take().unwrap();
                if how == EndHow::Lost {
                    socket.socket.abort(&mut self.stack);
                }
                socket.socket.drop_handle();
                self.free.push(index);
            }
        }
        if how == EndHow::Graceful {
            let _ = self.to_control.send(FromNetwork::Retired { session });
        }
    }
}
