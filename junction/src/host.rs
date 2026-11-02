//! Unix control thread. The network thread owns sockets in the Stack, never these descriptors.

use std::io::{self, Read, Write};
use std::os::fd::AsRawFd;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::PathBuf;
use std::sync::mpsc::{Receiver, Sender, TryRecvError};
use std::time::{Duration, Instant};

use ceph_proto::{Control, Retire, pack, slot, uid};

use crate::control::{
    ClientMessage, EndReason, Event, HostMessage, Op, SocketId, VERSION, Welcome, WelcomeStatus,
};

pub type SessionId = u64;

#[derive(Debug)]
pub enum ToNetwork {
    Open {
        session: SessionId,
        max_sockets: u16,
    },
    Op {
        session: SessionId,
        op: Op,
    },
    End {
        session: SessionId,
        how: EndHow,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EndHow {
    Graceful,
    Lost,
}

#[derive(Debug)]
pub enum FromNetwork {
    Event {
        session: SessionId,
        id: SocketId,
        event: Event,
    },
    Retired {
        session: SessionId,
    },
    ProtocolError {
        session: SessionId,
    },
}

pub struct ControlConfig {
    pub socket_path: PathBuf,
    pub ingress: String,
    pub max_record: u32,
    pub max_sessions: u32,
    pub max_sockets: u16,
    pub poll_ms: u16,
    pub hello_timeout: Duration,
}

pub struct ControlChannels {
    pub to_network: Sender<ToNetwork>,
    pub to_fifo: Sender<Control>,
    pub from_network: Receiver<FromNetwork>,
    pub retire: Receiver<Retire>,
}

enum Phase {
    Pending(Instant),
    Rejected,
    Live {
        session: SessionId,
        max_sockets: u16,
        draining: bool,
    },
}

struct Connection {
    socket: UnixStream,
    phase: Phase,
    input: Vec<u8>,
    output: Vec<u8>,
    written: usize,
}

impl Connection {
    fn queue(&mut self, message: HostMessage) {
        message.encode(&mut self.output);
    }

    fn flush(&mut self) -> io::Result<()> {
        while self.written < self.output.len() {
            match self.socket.write(&self.output[self.written..]) {
                Ok(0) => return Err(io::ErrorKind::WriteZero.into()),
                Ok(n) => self.written += n,
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => return Ok(()),
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(error) => return Err(error),
            }
        }
        self.output.clear();
        self.written = 0;
        Ok(())
    }

    // Terminal messages get exactly one nonblocking write attempt, including queued events.
    fn end(&mut self, reason: EndReason) {
        self.queue(HostMessage::End(reason));
        let _ = self.socket.write(&self.output[self.written..]);
    }
}

pub struct HostControl {
    listener: UnixListener,
    config: ControlConfig,
    channels: ControlChannels,
    connections: Vec<Connection>,
    free: Vec<u32>,
    next_uid: u32,
    shutdown: bool,
}

impl HostControl {
    pub fn bind(config: ControlConfig, channels: ControlChannels) -> io::Result<Self> {
        if config.poll_ms == 0 || config.ingress.len() > usize::from(u16::MAX) - 22 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "poll_ms must be at least 1 and ingress must fit in Welcome",
            ));
        }
        match std::fs::remove_file(&config.socket_path) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        let listener = UnixListener::bind(&config.socket_path)?;
        listener.set_nonblocking(true)?;
        let free = (0..config.max_sessions).rev().collect();
        Ok(Self {
            listener,
            config,
            channels,
            connections: Vec::new(),
            free,
            next_uid: 1,
            shutdown: false,
        })
    }

    pub fn run(mut self) -> io::Result<()> {
        let mut indices = Vec::new();
        let mut pollfds = Vec::new();
        loop {
            self.drain_network();
            self.drain_retire();
            if self.shutdown {
                for connection in &mut self.connections {
                    if matches!(connection.phase, Phase::Live { .. }) {
                        connection.end(EndReason::HostShutdown);
                    }
                }
                return Ok(());
            }

            indices.clear();
            pollfds.clear();
            pollfds.push(libc::pollfd {
                fd: self.listener.as_raw_fd(),
                events: libc::POLLIN,
                revents: 0,
            });
            for (index, connection) in self.connections.iter().enumerate() {
                let draining = matches!(connection.phase, Phase::Live { draining: true, .. });
                let reading = !draining && !matches!(connection.phase, Phase::Rejected);
                let output = connection.written < connection.output.len();
                // A draining socket's HUP must not spin the loop while the network retires it.
                if draining && !output {
                    continue;
                }
                indices.push(index);
                pollfds.push(libc::pollfd {
                    fd: connection.socket.as_raw_fd(),
                    events: if reading { libc::POLLIN } else { 0 }
                        | if output { libc::POLLOUT } else { 0 },
                    revents: 0,
                });
            }
            // SAFETY: pollfds is writable for the provided count, and all descriptors are owned here.
            let result = unsafe {
                libc::poll(
                    pollfds.as_mut_ptr(),
                    pollfds.len() as libc::nfds_t,
                    i32::from(self.config.poll_ms),
                )
            };
            if result < 0 {
                let error = io::Error::last_os_error();
                if error.kind() == io::ErrorKind::Interrupted {
                    continue;
                }
                return Err(error);
            }
            if pollfds[0].revents & libc::POLLIN != 0 {
                self.accept()?;
            }
            // Removing in reverse keeps the earlier poll indices valid, even with swap_remove.
            for (position, &index) in indices.iter().enumerate().rev() {
                if self.shutdown {
                    break;
                }
                if matches!(self.connections[index].phase, Phase::Pending(since) if since.elapsed() >= self.config.hello_timeout)
                {
                    self.remove(index, true);
                    continue;
                }
                let revents = pollfds[position + 1].revents;
                let draining = matches!(
                    self.connections[index].phase,
                    Phase::Live { draining: true, .. }
                );
                let reading =
                    !draining && !matches!(self.connections[index].phase, Phase::Rejected);
                if !draining && revents & (libc::POLLHUP | libc::POLLERR | libc::POLLNVAL) != 0 {
                    self.remove(index, true);
                    continue;
                }
                if reading && revents & libc::POLLIN != 0 && !self.read(index)? {
                    self.remove(index, true);
                    continue;
                }
                if self.shutdown {
                    break;
                }
                if self.connections[index].flush().is_err()
                    || (matches!(self.connections[index].phase, Phase::Rejected)
                        && self.connections[index].output.is_empty())
                {
                    self.remove(index, true);
                }
            }
            for index in (0..self.connections.len()).rev() {
                if matches!(self.connections[index].phase, Phase::Pending(since) if since.elapsed() >= self.config.hello_timeout)
                {
                    self.remove(index, true);
                }
            }
        }
    }

    fn accept(&mut self) -> io::Result<()> {
        for _ in 0..64 {
            let (socket, _) = match self.listener.accept() {
                Ok(pair) => pair,
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
                Err(error)
                    if matches!(
                        error.kind(),
                        io::ErrorKind::Interrupted | io::ErrorKind::ConnectionAborted
                    ) =>
                {
                    continue;
                }
                Err(error) => return Err(error),
            };
            socket.set_nonblocking(true)?;
            self.connections.push(Connection {
                socket,
                phase: Phase::Pending(Instant::now()),
                input: Vec::new(),
                output: Vec::new(),
                written: 0,
            });
        }
        Ok(())
    }

    fn read(&mut self, index: usize) -> io::Result<bool> {
        loop {
            let mut chunk = [0; 4096];
            match self.connections[index].socket.read(&mut chunk) {
                Ok(0) => return Ok(false),
                Ok(n) => self.connections[index].input.extend_from_slice(&chunk[..n]),
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => return Ok(true),
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(_) => return Ok(false),
            }
            loop {
                let (message, used) = match ClientMessage::decode(&self.connections[index].input) {
                    Ok(Some(message)) => message,
                    Ok(None) => break,
                    Err(_) => {
                        self.connections[index].end(EndReason::ProtocolError);
                        return Ok(false);
                    }
                };
                self.connections[index].input.drain(..used);
                match (&self.connections[index].phase, message) {
                    (
                        Phase::Pending(_),
                        ClientMessage::Hello {
                            version,
                            max_sockets,
                        },
                    ) => {
                        self.hello(index, version, max_sockets)?;
                        if self.shutdown || matches!(self.connections[index].phase, Phase::Rejected)
                        {
                            return Ok(true);
                        }
                    }
                    (
                        Phase::Live {
                            session,
                            max_sockets,
                            draining: false,
                        },
                        ClientMessage::Op(op),
                    ) if op.id().slot < u32::from(*max_sockets) => {
                        if self
                            .channels
                            .to_network
                            .send(ToNetwork::Op {
                                session: *session,
                                op,
                            })
                            .is_err()
                        {
                            self.shutdown = true;
                            return Ok(true);
                        }
                    }
                    _ => {
                        self.connections[index].end(EndReason::ProtocolError);
                        return Ok(false);
                    }
                }
            }
        }
    }

    fn hello(&mut self, index: usize, version: u16, max_sockets: u16) -> io::Result<()> {
        let status = if version != VERSION {
            WelcomeStatus::BadVersion
        } else if max_sockets == 0 || max_sockets > self.config.max_sockets {
            WelcomeStatus::BadMaxSockets
        } else if self.free.is_empty() {
            WelcomeStatus::SessionsFull
        } else {
            WelcomeStatus::Ok
        };
        let mut stream = 0;
        if status == WelcomeStatus::Ok {
            let next_uid = self
                .next_uid
                .checked_add(1)
                .ok_or_else(|| io::Error::other("stream uid space exhausted"))?;
            let slot = self.free.pop().expect("free slot checked above");
            let uid = self.next_uid;
            self.next_uid = next_uid;
            stream = pack(slot, uid);
            self.connections[index].phase = Phase::Live {
                session: stream,
                max_sockets,
                draining: false,
            };
            if self
                .channels
                .to_network
                .send(ToNetwork::Open {
                    session: stream,
                    max_sockets,
                })
                .is_err()
                || self
                    .channels
                    .to_fifo
                    .send(Control::Open { slot, uid })
                    .is_err()
            {
                self.shutdown = true;
                return Ok(());
            }
        } else {
            self.connections[index].phase = Phase::Rejected;
        }
        self.connections[index].queue(HostMessage::Welcome(Welcome {
            version: VERSION,
            status,
            stream,
            max_record: self.config.max_record,
            ingress: self.config.ingress.clone(),
        }));
        Ok(())
    }

    fn find(&self, session: SessionId) -> Option<usize> {
        self.connections.iter().position(|connection| {
            matches!(connection.phase, Phase::Live { session: current, .. } if current == session)
        })
    }

    fn remove(&mut self, index: usize, lost: bool) {
        let connection = self.connections.swap_remove(index);
        if let Phase::Live { session, .. } = connection.phase {
            drop(connection.socket);
            let slot = slot(session);
            let uid = uid(session);
            if self
                .channels
                .to_fifo
                .send(Control::Close { slot, uid })
                .is_err()
            {
                self.shutdown = true;
            }
            if lost
                && self
                    .channels
                    .to_network
                    .send(ToNetwork::End {
                        session,
                        how: EndHow::Lost,
                    })
                    .is_err()
            {
                self.shutdown = true;
            }
            self.free.push(slot);
        }
    }

    fn drain_network(&mut self) {
        while !self.shutdown {
            let message = match self.channels.from_network.try_recv() {
                Ok(message) => message,
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    self.shutdown = true;
                    break;
                }
            };
            match message {
                FromNetwork::Event { session, id, event } => {
                    if let Some(index) = self.find(session) {
                        self.connections[index].queue(HostMessage::Event { id, event });
                    }
                }
                FromNetwork::ProtocolError { session } => {
                    if let Some(index) = self.find(session) {
                        self.connections[index].end(EndReason::ProtocolError);
                        self.remove(index, true);
                    }
                }
                FromNetwork::Retired { session } => {
                    if let Some(index) = self.find(session)
                        && matches!(
                            self.connections[index].phase,
                            Phase::Live { draining: true, .. }
                        )
                    {
                        let _ = self.connections[index].flush();
                        self.remove(index, false);
                    }
                }
            }
        }
    }

    fn drain_retire(&mut self) {
        while !self.shutdown {
            let message = match self.channels.retire.try_recv() {
                Ok(message) => message,
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    self.shutdown = true;
                    break;
                }
            };
            let (slot, uid) = match message {
                Retire::Kicked { slot, uid } | Retire::Finished { slot, uid } => (slot, uid),
            };
            let session = pack(slot, uid);
            let Some(index) = self.find(session) else {
                continue;
            };
            match message {
                Retire::Kicked { .. } => {
                    self.connections[index].end(EndReason::Kicked);
                    self.remove(index, true);
                }
                Retire::Finished { .. } => {
                    if let Phase::Live { draining, .. } = &mut self.connections[index].phase
                        && !*draining
                    {
                        *draining = true;
                        if self
                            .channels
                            .to_network
                            .send(ToNetwork::End {
                                session,
                                how: EndHow::Graceful,
                            })
                            .is_err()
                        {
                            self.shutdown = true;
                        }
                    }
                }
            }
        }
    }
}
