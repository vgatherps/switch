//! Unix control thread. The network thread owns sockets in the Stack, never these descriptors.
//!
//! Poll passes read one chunk per connection. Terminal handling snapshots and drains only
//! already-readable control bytes. FIFO completion survives control disconnects, and network
//! retirement keeps flushing queued events and End(Finished) before closing.

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
    Retiring {
        session: SessionId,
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
            self.drain_retire()?;
            if self.shutdown {
                for connection in &mut self.connections {
                    if matches!(
                        connection.phase,
                        Phase::Live { .. } | Phase::Retiring { .. }
                    ) {
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
                let draining = matches!(
                    connection.phase,
                    Phase::Live { draining: true, .. } | Phase::Retiring { .. }
                );
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
                let revents = pollfds[position + 1].revents;
                let draining = matches!(
                    self.connections[index].phase,
                    Phase::Live { draining: true, .. } | Phase::Retiring { .. }
                );
                let reading =
                    !draining && !matches!(self.connections[index].phase, Phase::Rejected);
                if !draining && revents & (libc::POLLHUP | libc::POLLERR | libc::POLLNVAL) != 0 {
                    self.disconnect(index);
                    continue;
                }
                if reading && revents & libc::POLLIN != 0 && !self.read(index, false)? {
                    self.remove(index, true);
                    continue;
                }
                if self.shutdown {
                    break;
                }
                self.flush(index);
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

    // Terminal handling snapshots readable bytes; later writes cannot prolong the drain.
    fn read(&mut self, index: usize, drain: bool) -> io::Result<bool> {
        let mut remaining = if drain {
            let mut available: libc::c_int = 0;
            // SAFETY: the descriptor is owned and available is writable for one integer.
            if unsafe {
                libc::ioctl(
                    self.connections[index].socket.as_raw_fd(),
                    libc::FIONREAD,
                    &mut available,
                )
            } < 0
            {
                return Err(io::Error::last_os_error());
            }
            usize::try_from(available).expect("FIONREAD returned a negative byte count")
        } else {
            4096
        };
        while remaining != 0 {
            let mut chunk = [0; 4096];
            let length = remaining.min(chunk.len());
            match self.connections[index].socket.read(&mut chunk[..length]) {
                // Finished already committed the drain; losing control cannot abort it.
                Ok(0) => {
                    if !drain {
                        self.read(index, true)?;
                    }
                    return Ok(drain);
                }
                Ok(n) => {
                    remaining -= n;
                    self.connections[index].input.extend_from_slice(&chunk[..n]);
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => return Ok(true),
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {
                    if drain {
                        continue;
                    }
                    return Ok(true);
                }
                Err(_) => {
                    if !drain {
                        self.read(index, true)?;
                    }
                    return Ok(drain);
                }
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
            if !drain {
                return Ok(true);
            }
        }
        Ok(true)
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
            matches!(connection.phase, Phase::Live { session: current, .. } | Phase::Retiring { session: current } if current == session)
        })
    }

    fn remove(&mut self, index: usize, lost: bool) {
        let connection = self.connections.swap_remove(index);
        if let Phase::Live { session, .. } | Phase::Retiring { session } = connection.phase {
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

    fn disconnect(&mut self, index: usize) {
        if matches!(
            self.connections[index].phase,
            Phase::Live {
                draining: false,
                ..
            }
        ) {
            // Preserve operations written before the control connection was lost.
            let _ = self.read(index, true);
        }
        self.remove(index, true);
    }

    fn flush(&mut self, index: usize) {
        let retiring = matches!(self.connections[index].phase, Phase::Retiring { .. });
        if self.connections[index].flush().is_err()
            || (matches!(
                self.connections[index].phase,
                Phase::Rejected | Phase::Retiring { .. }
            ) && self.connections[index].output.is_empty())
        {
            self.remove(index, !retiring);
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
                        self.connections[index].queue(HostMessage::End(EndReason::Finished));
                        self.connections[index].phase = Phase::Retiring { session };
                        self.flush(index);
                    }
                }
            }
        }
    }

    fn drain_retire(&mut self) -> io::Result<()> {
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
                    if matches!(
                        self.connections[index].phase,
                        Phase::Live {
                            draining: false,
                            ..
                        }
                    ) {
                        // The client wrote these bytes before pushing the end record, and
                        // the network thread consumed that record before sending Finished.
                        // Its preceding control bytes are therefore already readable.
                        if !self.read(index, true)? {
                            self.remove(index, true);
                            continue;
                        }
                        if self.shutdown {
                            break;
                        }
                        let Phase::Live { draining, .. } = &mut self.connections[index].phase
                        else {
                            unreachable!("Finished belongs to a live connection");
                        };
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
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::mpsc;

    static NEXT_PATH: AtomicU64 = AtomicU64::new(0);

    struct Fixture {
        host: HostControl,
        client: Option<UnixStream>,
        network: Receiver<ToNetwork>,
        retire: Sender<Retire>,
        _events: Sender<FromNetwork>,
        _fifo: Receiver<Control>,
    }

    impl Fixture {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!(
                "jnct-unit-{}-{}.sock",
                std::process::id(),
                NEXT_PATH.fetch_add(1, Ordering::Relaxed)
            ));
            let (to_network, network) = mpsc::channel();
            let (events, from_network) = mpsc::channel();
            let (to_fifo, fifo) = mpsc::channel();
            let (retire, from_fifo) = mpsc::channel();
            let mut host = HostControl::bind(
                ControlConfig {
                    socket_path: path,
                    ingress: String::new(),
                    max_record: 256,
                    max_sessions: 1,
                    max_sockets: 1,
                    poll_ms: 1,
                    hello_timeout: Duration::from_secs(2),
                },
                ControlChannels {
                    to_network,
                    to_fifo,
                    from_network,
                    retire: from_fifo,
                },
            )
            .unwrap();
            let (client, socket) = UnixStream::pair().unwrap();
            socket.set_nonblocking(true).unwrap();
            host.free.clear();
            host.connections.push(Connection {
                socket,
                phase: Phase::Live {
                    session: pack(0, 1),
                    max_sockets: 1,
                    draining: false,
                },
                input: Vec::new(),
                output: Vec::new(),
                written: 0,
            });
            Self {
                host,
                client: Some(client),
                network,
                retire,
                _events: events,
                _fifo: fifo,
            }
        }

        fn write_ops(&mut self) -> usize {
            let mut bytes = Vec::new();
            for generation in 0..300 {
                ClientMessage::Op(Op::ShutdownTcp {
                    id: SocketId {
                        slot: 0,
                        generation,
                    },
                })
                .encode(&mut bytes);
            }
            assert!(bytes.len() > 4096);
            self.client.as_mut().unwrap().write_all(&bytes).unwrap();
            300
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.host.config.socket_path);
        }
    }

    #[test]
    fn finished_forwards_all_readable_ops_before_graceful_end() {
        let mut fixture = Fixture::new();
        let count = fixture.write_ops();
        fixture
            .retire
            .send(Retire::Finished { slot: 0, uid: 1 })
            .unwrap();
        fixture.host.drain_retire().unwrap();
        for generation in 0..count as u32 {
            assert!(matches!(
                fixture.network.try_recv().unwrap(),
                ToNetwork::Op {
                    session,
                    op: Op::ShutdownTcp { id: SocketId { slot: 0, generation: actual } },
                } if session == pack(0, 1) && actual == generation
            ));
        }
        assert!(matches!(
            fixture.network.try_recv().unwrap(),
            ToNetwork::End { session, how: EndHow::Graceful } if session == pack(0, 1)
        ));
        assert!(fixture.network.try_recv().is_err());
    }

    #[test]
    fn finished_after_control_eof_forwards_op_then_retires_gracefully() {
        let mut fixture = Fixture::new();
        let id = SocketId {
            slot: 0,
            generation: 1,
        };
        let mut bytes = Vec::new();
        ClientMessage::Op(Op::ShutdownTcp { id }).encode(&mut bytes);
        fixture.client.as_mut().unwrap().write_all(&bytes).unwrap();
        drop(fixture.client.take());
        fixture
            .retire
            .send(Retire::Finished { slot: 0, uid: 1 })
            .unwrap();
        fixture.host.drain_retire().unwrap();
        assert!(matches!(
            fixture.network.try_recv().unwrap(),
            ToNetwork::Op { session, op: Op::ShutdownTcp { id: actual } }
                if session == pack(0, 1) && actual == id
        ));
        assert!(matches!(
            fixture.network.try_recv().unwrap(),
            ToNetwork::End { session, how: EndHow::Graceful } if session == pack(0, 1)
        ));
        assert!(
            fixture.network.try_recv().is_err(),
            "must not send End Lost"
        );
        assert!(matches!(
            fixture.host.connections[0].phase,
            Phase::Live { draining: true, .. }
        ));
    }

    #[test]
    fn finished_with_readable_protocol_error_never_sends_graceful_end() {
        let mut fixture = Fixture::new();
        fixture
            .client
            .as_mut()
            .unwrap()
            .write_all(&[255; 8])
            .unwrap();
        fixture
            .retire
            .send(Retire::Finished { slot: 0, uid: 1 })
            .unwrap();
        fixture.host.drain_retire().unwrap();
        assert!(matches!(
            fixture.network.try_recv().unwrap(),
            ToNetwork::End { session, how: EndHow::Lost } if session == pack(0, 1)
        ));
        assert!(fixture.host.connections.is_empty());
        let mut bytes = [0; 9];
        fixture
            .client
            .as_mut()
            .unwrap()
            .read_exact(&mut bytes)
            .unwrap();
        assert!(matches!(
            HostMessage::decode(&bytes).unwrap().unwrap().0,
            HostMessage::End(EndReason::ProtocolError)
        ));
    }

    #[test]
    fn normal_read_handles_only_one_chunk() {
        let mut fixture = Fixture::new();
        let count = fixture.write_ops();
        assert!(fixture.host.read(0, false).unwrap());
        let first = fixture.network.try_iter().count();
        assert!(
            first > 0 && first < count,
            "one pass must not drain the socket"
        );
        assert!(fixture.host.read(0, false).unwrap());
        assert_eq!(first + fixture.network.try_iter().count(), count);
    }

    #[test]
    fn hangup_forwards_readable_ops_before_lost_end() {
        let mut fixture = Fixture::new();
        let count = fixture.write_ops();
        drop(fixture.client.take());
        fixture.host.disconnect(0);
        for generation in 0..count as u32 {
            assert!(matches!(
                fixture.network.try_recv().unwrap(),
                ToNetwork::Op {
                    session,
                    op: Op::ShutdownTcp { id: SocketId { slot: 0, generation: actual } },
                } if session == pack(0, 1) && actual == generation
            ));
        }
        assert!(matches!(
            fixture.network.try_recv().unwrap(),
            ToNetwork::End { session, how: EndHow::Lost } if session == pack(0, 1)
        ));
        assert!(fixture.network.try_recv().is_err());
        assert!(fixture.host.connections.is_empty());
    }
}
