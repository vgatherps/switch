//! Unix control thread. The network thread owns sockets in the Stack, never these descriptors.
//!
//! Poll passes read one chunk per connection. Terminal handling snapshots and drains only
//! already-readable control bytes, including after write failures. FIFO completion survives
//! control disconnects, and network retirement keeps flushing queued events and End(Finished)
//! before closing.
//! Protocol errors and kicks always abort, even after FIFO completion.
//! The control thread creates each session's receive ring and owns its backing until the
//! connection is removed. The network thread returns the writer here for destruction.
//! Session slots and FIFO registrations remain owned until writer handback, even after
//! control disconnect. A new Hello can report SessionsFull until that handback arrives.

use std::io::{self, Read, Write};
use std::os::fd::AsRawFd;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::PathBuf;
use std::sync::mpsc::{Receiver, Sender, TryRecvError};
use std::time::{Duration, Instant};

use ceph_proto::{Control, Retire, pack, slot, uid};
use shared_memory::{Backing, page_size_in, unique_name};
use slice_ringbuffer::RingWriter;

use crate::control::{
    ClientMessage, EndReason, Event, HostMessage, Op, SocketId, VERSION, Welcome, WelcomeStatus,
    write_queued,
};

pub type SessionId = u64;

const RX_NAME_PREFIX: &str = "jnctrx";

pub enum ToNetwork {
    Open {
        session: SessionId,
        max_sockets: u16,
        rx: RingWriter,
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
    Abort,
}

pub enum FromNetwork {
    Event {
        session: SessionId,
        id: SocketId,
        event: Event,
    },
    Retired {
        session: SessionId,
        rx: RingWriter,
    },
    Released {
        session: SessionId,
        rx: RingWriter,
    },
    ProtocolError {
        session: SessionId,
    },
}

pub struct ControlConfig {
    pub socket_path: PathBuf,
    pub ingress: String,
    pub rx_dir: PathBuf,
    pub rx_size: usize,
    pub max_record: u32,
    /// Host TX frame payload capacity (IP MTU minus 28).
    pub frame_capacity: u32,
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
        rx_backing: Backing,
    },
    Retiring {
        session: SessionId,
        rx_backing: Backing,
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
        write_queued(&mut self.socket, &mut self.output, &mut self.written)
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
    sessions: Vec<Option<u32>>,
    next_uid: u32,
    shutdown: bool,
}

impl HostControl {
    pub fn bind(config: ControlConfig, channels: ControlChannels) -> io::Result<Self> {
        if config.frame_capacity.checked_add(24).is_none_or(|minimum| config.max_record < minimum) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "max_record must fit frame_capacity plus the 24-byte UDP send header",
            ));
        }
        #[cfg(target_os = "linux")]
        let rx_name_len = config
            .rx_dir
            .join(RX_NAME_PREFIX)
            .to_str()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "rx_dir is not UTF-8"))?
            .len();
        #[cfg(target_os = "macos")]
        let rx_name_len = RX_NAME_PREFIX.len();
        let welcome_len = 28usize
            .checked_add(config.ingress.len())
            .and_then(|len| len.checked_add(rx_name_len))
            .and_then(|len| len.checked_add(16));
        if config.poll_ms == 0 || welcome_len.is_none_or(|len| len > usize::from(u16::MAX)) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "poll_ms must be at least 1 and ingress plus rx must fit in Welcome",
            ));
        }
        let page = page_size_in(&config.rx_dir)?;
        if !config.rx_size.is_power_of_two()
            || config.rx_size > (1usize << 31)
            || !config.rx_size.is_multiple_of(page)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "rx_size must be a power of two, at most 2 GiB, and a backing-page multiple",
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
        let sessions = vec![None; config.max_sessions as usize];
        Ok(Self {
            listener,
            config,
            channels,
            connections: Vec::new(),
            free,
            sessions,
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
                if reading
                    && revents & libc::POLLIN != 0
                    && let Some(how) = self.read(index, false)?
                {
                    self.remove(index, Some(how));
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
                    self.remove(index, Some(EndHow::Lost));
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
                Err(error)
                    if matches!(
                        error.raw_os_error(),
                        Some(libc::EMFILE | libc::ENFILE | libc::ENOBUFS | libc::ENOMEM)
                    ) => break,
                Err(error) => return Err(error),
            };
            if socket.set_nonblocking(true).is_err() {
                continue;
            }
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
    fn read(&mut self, index: usize, drain: bool) -> io::Result<Option<EndHow>> {
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
                Ok(0) => return Ok(if drain { None } else { Some(EndHow::Lost) }),
                Ok(n) => {
                    remaining -= n;
                    self.connections[index].input.extend_from_slice(&chunk[..n]);
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => return Ok(None),
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {
                    if drain {
                        continue;
                    }
                    return Ok(None);
                }
                Err(_) => return Ok(Some(EndHow::Lost)),
            }
            loop {
                let (message, used) = match ClientMessage::decode(&self.connections[index].input) {
                    Ok(Some(message)) => message,
                    Ok(None) => break,
                    Err(_) => {
                        self.connections[index].end(EndReason::ProtocolError);
                        return Ok(Some(EndHow::Abort));
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
                            return Ok(None);
                        }
                    }
                    (
                        Phase::Live {
                            session,
                            max_sockets,
                            draining: false,
                            ..
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
                            return Ok(None);
                        }
                    }
                    _ => {
                        self.connections[index].end(EndReason::ProtocolError);
                        return Ok(Some(EndHow::Abort));
                    }
                }
            }
            if !drain {
                return Ok(None);
            }
        }
        Ok(None)
    }

    fn hello(&mut self, index: usize, version: u16, max_sockets: u16) -> io::Result<()> {
        let mut status = if version != VERSION {
            WelcomeStatus::BadVersion
        } else if max_sockets == 0 || max_sockets > self.config.max_sockets {
            WelcomeStatus::BadMaxSockets
        } else if self.free.is_empty() {
            WelcomeStatus::SessionsFull
        } else {
            WelcomeStatus::Ok
        };
        let mut stream = 0;
        let mut rx_name = String::new();
        if status == WelcomeStatus::Ok {
            match self.create_rx() {
                Ok((rx_backing, rx)) => {
                    let next_uid = self
                        .next_uid
                        .checked_add(1)
                        .ok_or_else(|| io::Error::other("stream uid space exhausted"))?;
                    let slot = self.free.pop().expect("free slot checked above");
                    let uid = self.next_uid;
                    self.next_uid = next_uid;
                    self.sessions[slot as usize] = Some(uid);
                    stream = pack(slot, uid);
                    rx_name = rx_backing.name().to_owned();
                    self.connections[index].phase = Phase::Live {
                        session: stream,
                        max_sockets,
                        draining: false,
                        rx_backing,
                    };
                    if self
                        .channels
                        .to_network
                        .send(ToNetwork::Open {
                            session: stream,
                            max_sockets,
                            rx,
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
                }
                Err(_) => status = WelcomeStatus::HostResources,
            }
        }
        if status != WelcomeStatus::Ok {
            self.connections[index].phase = Phase::Rejected;
        }
        self.connections[index].queue(HostMessage::Welcome(Welcome {
            version: VERSION,
            status,
            stream,
            max_record: self.config.max_record,
            frame_capacity: self.config.frame_capacity,
            ingress: self.config.ingress.clone(),
            rx: rx_name,
        }));
        Ok(())
    }

    fn create_rx(&self) -> io::Result<(Backing, RingWriter)> {
        let name = unique_name(RX_NAME_PREFIX)?;
        #[cfg(target_os = "linux")]
        let mut backing = Backing::create_file(&self.config.rx_dir.join(name))?;
        #[cfg(target_os = "macos")]
        let mut backing = Backing::create_shm(&name)?;
        let rx = RingWriter::create(&mut backing, self.config.rx_size)?;
        Ok((backing, rx))
    }

    fn find(&self, session: SessionId) -> Option<usize> {
        self.connections.iter().position(|connection| {
            matches!(connection.phase, Phase::Live { session: current, .. } | Phase::Retiring { session: current, .. } if current == session)
        })
    }

    fn remove(&mut self, index: usize, how: Option<EndHow>) {
        let connection = self.connections.swap_remove(index);
        if let Phase::Live {
            session,
            rx_backing,
            ..
        }
        | Phase::Retiring {
            session,
            rx_backing,
        } = connection.phase
        {
            drop(connection.socket);
            drop(rx_backing);
            if let Some(how) = how
                && self
                    .channels
                    .to_network
                    .send(ToNetwork::End { session, how })
                    .is_err()
            {
                self.shutdown = true;
            }
        }
    }

    fn handback(&mut self, session: SessionId, rx: RingWriter) {
        drop(rx);
        let slot = slot(session);
        let uid = uid(session);
        let Some(entry) = self.sessions.get_mut(slot as usize) else {
            return;
        };
        if *entry != Some(uid) {
            return;
        }
        *entry = None;
        self.free.push(slot);
        if self.channels.to_fifo.send(Control::Close { slot, uid }).is_err() {
            self.shutdown = true;
        }
    }

    fn disconnect(&mut self, index: usize) {
        let mut how = EndHow::Lost;
        if matches!(
            self.connections[index].phase,
            Phase::Live {
                draining: false,
                ..
            }
        ) {
            // A decoded protocol error must abort even if FIFO completion raced this HUP.
            if let Ok(Some(EndHow::Abort)) = self.read(index, true) {
                how = EndHow::Abort;
            }
        }
        self.remove(index, Some(how));
    }

    fn flush(&mut self, index: usize) {
        let retiring = matches!(self.connections[index].phase, Phase::Retiring { .. });
        if self.connections[index].flush().is_err() {
            if matches!(
                self.connections[index].phase,
                Phase::Live {
                    draining: false,
                    ..
                }
            ) {
                self.disconnect(index);
            } else {
                self.remove(index, (!retiring).then_some(EndHow::Lost));
            }
        } else if matches!(
            self.connections[index].phase,
            Phase::Rejected | Phase::Retiring { .. }
        ) && self.connections[index].output.is_empty()
        {
            self.remove(index, (!retiring).then_some(EndHow::Lost));
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
                        self.remove(index, Some(EndHow::Abort));
                    }
                }
                FromNetwork::Retired { session, rx } => {
                    self.handback(session, rx);
                    if let Some(index) = self.find(session)
                        && matches!(
                            self.connections[index].phase,
                            Phase::Live { draining: true, .. }
                        )
                    {
                        let connection = &mut self.connections[index];
                        let Phase::Live { rx_backing, .. } =
                            std::mem::replace(&mut connection.phase, Phase::Rejected)
                        else {
                            unreachable!("Retired belongs to a draining live connection");
                        };
                        connection.queue(HostMessage::End(EndReason::Finished));
                        connection.phase = Phase::Retiring {
                            session,
                            rx_backing,
                        };
                        self.flush(index);
                    }
                }
                FromNetwork::Released { session, rx } => self.handback(session, rx),
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
                    self.remove(index, Some(EndHow::Abort));
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
                        match self.read(index, true) {
                            Ok(None) => {}
                            Ok(Some(how)) => {
                                self.remove(index, Some(how));
                                continue;
                            }
                            Err(_) => {
                                self.remove(index, Some(EndHow::Lost));
                                continue;
                            }
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
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::mpsc;

    static NEXT_PATH: AtomicU64 = AtomicU64::new(0);

    fn config(socket_path: PathBuf) -> ControlConfig {
        let rx_dir = std::env::temp_dir();
        let rx_size = page_size_in(&rx_dir).unwrap();
        ControlConfig {
            socket_path,
            ingress: String::new(),
            rx_dir,
            rx_size,
            max_record: 256,
            frame_capacity: 232,
            max_sessions: 1,
            max_sockets: 1,
            poll_ms: 1,
            hello_timeout: Duration::from_secs(2),
        }
    }

    struct Fixture {
        host: HostControl,
        client: Option<UnixStream>,
        network: Receiver<ToNetwork>,
        retire: Sender<Retire>,
        events: Sender<FromNetwork>,
        rx: Option<RingWriter>,
        fifo: Receiver<Control>,
        _path: FixturePath,
    }

    impl Fixture {
        fn new() -> Self {
            let path = FixturePath::new();
            let (to_network, network) = mpsc::channel();
            let (events, from_network) = mpsc::channel();
            let (to_fifo, fifo) = mpsc::channel();
            let (retire, from_fifo) = mpsc::channel();
            let mut host = HostControl::bind(
                config(path.0.clone()),
                ControlChannels {
                    to_network,
                    to_fifo,
                    from_network,
                    retire: from_fifo,
                },
            )
            .unwrap();
            let (mut client, socket) = UnixStream::pair().unwrap();
            socket.set_nonblocking(true).unwrap();
            host.connections.push(Connection {
                socket,
                phase: Phase::Pending(Instant::now()),
                input: Vec::new(),
                output: Vec::new(),
                written: 0,
            });
            host.hello(0, VERSION, 1).unwrap();
            let rx = match network.try_recv().unwrap() {
                ToNetwork::Open {
                    session,
                    max_sockets: 1,
                    rx,
                } if session == pack(0, 1) => rx,
                _ => panic!("expected network Open"),
            };
            let mut welcome = vec![0; host.connections[0].output.len()];
            host.flush(0);
            client.read_exact(&mut welcome).unwrap();
            assert!(matches!(
                HostMessage::decode(&welcome).unwrap().unwrap().0,
                HostMessage::Welcome(Welcome {
                    status: WelcomeStatus::Ok,
                    ..
                })
            ));
            Self {
                host,
                client: Some(client),
                network,
                retire,
                events,
                rx: Some(rx),
                fifo,
                _path: path,
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
            if let Some(rx) = self.rx.take() {
                self.events
                    .send(FromNetwork::Released {
                        session: pack(0, 1),
                        rx,
                    })
                    .unwrap_or_else(|_| panic!("control event channel closed"));
                self.host.drain_network();
            }
        }
    }

    #[test]
    fn accept_fd_exhaustion_preserves_sessions_and_recovers() {
        const CHILD: &str = "JUNCTION_ACCEPT_FD_EXHAUSTION_CHILD";
        if std::env::var_os(CHILD).is_none() {
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "host::tests::accept_fd_exhaustion_preserves_sessions_and_recovers",
                    "--exact",
                    "--nocapture",
                ])
                .env(CHILD, "1")
                .status()
                .unwrap();
            assert!(status.success(), "descriptor-exhaustion child failed");
            return;
        }

        let mut fixture = Fixture::new();
        let mut pending = UnixStream::connect(&fixture._path.0).unwrap();
        pending.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        let mut original = libc::rlimit { rlim_cur: 0, rlim_max: 0 };
        // SAFETY: getrlimit writes to a valid local rlimit.
        assert_eq!(unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut original) }, 0);
        let limit = libc::rlimit {
            rlim_cur: original.rlim_cur.min(128),
            rlim_max: original.rlim_max,
        };
        // SAFETY: the limit is valid and affects only this isolated child process.
        assert_eq!(unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &limit) }, 0);
        let mut descriptors = Vec::new();
        loop {
            match std::fs::File::open("/dev/null") {
                Ok(file) => descriptors.push(file),
                Err(error) => {
                    assert_eq!(error.raw_os_error(), Some(libc::EMFILE));
                    break;
                }
            }
        }

        fixture.host.accept().unwrap();
        let mut request = Vec::new();
        ClientMessage::Op(Op::ShutdownTcp {
            id: SocketId { slot: 0, generation: 0 },
        }).encode(&mut request);
        fixture.client.as_mut().unwrap().write_all(&request).unwrap();
        assert!(fixture.host.read(0, false).unwrap().is_none());
        assert!(matches!(
            fixture.network.try_recv().unwrap(),
            ToNetwork::Op {
                session,
                op: Op::ShutdownTcp { id: SocketId { slot: 0, generation: 0 } },
            } if session == pack(0, 1)
        ));

        drop(descriptors);
        // SAFETY: restore this child's previously valid limit.
        assert_eq!(unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &original) }, 0);
        fixture.host.accept().unwrap();
        let mut hello = Vec::new();
        ClientMessage::Hello { version: VERSION, max_sockets: 1 }.encode(&mut hello);
        pending.write_all(&hello).unwrap();
        assert!(fixture.host.read(1, false).unwrap().is_none());
        let mut reply = vec![0; fixture.host.connections[1].output.len()];
        fixture.host.flush(1);
        pending.read_exact(&mut reply).unwrap();
        assert!(matches!(
            HostMessage::decode(&reply).unwrap().unwrap().0,
            HostMessage::Welcome(Welcome { status: WelcomeStatus::SessionsFull, .. })
        ));
    }

    #[test]
    fn lost_session_slot_stays_full_until_writer_handback() {
        for graceful in [false, true] {
            let mut fixture = Fixture::new();
            assert_eq!(fixture.fifo.try_recv().unwrap(), Control::Open { slot: 0, uid: 1 });
            drop(fixture.client.take());
            fixture.host.disconnect(0);
            assert!(matches!(
                fixture.network.try_recv().unwrap(),
                ToNetwork::End { session, how: EndHow::Lost } if session == pack(0, 1)
            ));
            assert!(fixture.fifo.try_recv().is_err());

            let (_client, socket) = UnixStream::pair().unwrap();
            socket.set_nonblocking(true).unwrap();
            fixture.host.connections.push(Connection {
                socket,
                phase: Phase::Pending(Instant::now()),
                input: Vec::new(),
                output: Vec::new(),
                written: 0,
            });
            fixture.host.hello(0, VERSION, 1).unwrap();
            assert!(matches!(
                HostMessage::decode(&fixture.host.connections[0].output).unwrap().unwrap().0,
                HostMessage::Welcome(Welcome { status: WelcomeStatus::SessionsFull, .. })
            ));
            assert!(fixture.network.try_recv().is_err());

            let rx = fixture.rx.take().unwrap();
            let session = pack(0, 1);
            fixture.events.send(if graceful {
                FromNetwork::Retired { session, rx }
            } else {
                FromNetwork::Released { session, rx }
            }).unwrap();
            fixture.host.drain_network();
            assert_eq!(fixture.fifo.try_recv().unwrap(), Control::Close { slot: 0, uid: 1 });
            assert!(fixture.fifo.try_recv().is_err());
            fixture.host.remove(0, None);
            let (_next_client, socket) = UnixStream::pair().unwrap();
            socket.set_nonblocking(true).unwrap();
            fixture.host.connections.push(Connection {
                socket,
                phase: Phase::Pending(Instant::now()),
                input: Vec::new(),
                output: Vec::new(),
                written: 0,
            });
            fixture.host.hello(0, VERSION, 1).unwrap();
            assert!(matches!(
                HostMessage::decode(&fixture.host.connections[0].output).unwrap().unwrap().0,
                HostMessage::Welcome(Welcome { status: WelcomeStatus::Ok, .. })
            ));
            assert!(matches!(
                fixture.network.try_recv().unwrap(),
                ToNetwork::Open { session, .. } if session == pack(0, 2)
            ));
            assert_eq!(fixture.fifo.try_recv().unwrap(), Control::Open { slot: 0, uid: 2 });
            assert!(fixture.fifo.try_recv().is_err());
        }
    }

    struct FixturePath(PathBuf);

    impl FixturePath {
        fn new() -> Self {
            Self(std::env::temp_dir().join(format!(
                "jnct-unit-{}-{}.sock",
                std::process::id(),
                NEXT_PATH.fetch_add(1, Ordering::Relaxed)
            )))
        }
    }

    impl Drop for FixturePath {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }

    #[test]
    fn bind_rejects_invalid_rx_sizes_before_touching_socket_path() {
        let path = FixturePath::new();
        std::fs::write(&path.0, b"unchanged").unwrap();
        let page = page_size_in(&std::env::temp_dir()).unwrap();
        for rx_size in [0, page + 1, page / 2, 1usize << 32] {
            let mut config = config(path.0.clone());
            config.rx_size = rx_size;
            let (to_network, _network) = mpsc::channel();
            let (_events, from_network) = mpsc::channel();
            let (to_fifo, _fifo) = mpsc::channel();
            let (_retire, retire) = mpsc::channel();
            match HostControl::bind(
                config,
                ControlChannels {
                    to_network,
                    to_fifo,
                    from_network,
                    retire,
                },
            ) {
                Err(error) => assert_eq!(error.kind(), io::ErrorKind::InvalidInput),
                Ok(_) => panic!("accepted invalid rx_size {rx_size}"),
            }
            assert_eq!(std::fs::read(&path.0).unwrap(), b"unchanged");
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
        fixture.host.drain_retire();
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
        fixture.host.drain_retire();
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
        fixture.host.drain_retire();
        assert!(matches!(
            fixture.network.try_recv().unwrap(),
            ToNetwork::End { session, how: EndHow::Abort } if session == pack(0, 1)
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
    fn hangup_with_readable_protocol_error_aborts_instead_of_lost() {
        let mut fixture = Fixture::new();
        fixture
            .client
            .as_mut()
            .unwrap()
            .write_all(&[255; 8])
            .unwrap();
        drop(fixture.client.take());
        fixture.host.disconnect(0);
        assert!(matches!(
            fixture.network.try_recv().unwrap(),
            ToNetwork::End { session, how: EndHow::Abort } if session == pack(0, 1)
        ));
        assert!(
            fixture.network.try_recv().is_err(),
            "must not also send Lost"
        );
        assert!(fixture.host.connections.is_empty());
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

    #[cfg(target_os = "linux")]
    #[test]
    fn write_failure_forwards_readable_ops_before_lost_end() {
        let mut fixture = Fixture::new();
        let count = fixture.write_ops();
        fixture.host.connections[0]
            .socket
            .shutdown(std::net::Shutdown::Write)
            .unwrap();
        fixture.host.connections[0].queue(HostMessage::Event {
            id: SocketId {
                slot: 0,
                generation: 0,
            },
            event: Event::Connected { mss: 1460 },
        });
        fixture.host.flush(0);
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

    #[cfg(target_os = "linux")]
    #[test]
    fn write_failure_with_readable_protocol_error_aborts_instead_of_lost() {
        let mut fixture = Fixture::new();
        fixture
            .client
            .as_mut()
            .unwrap()
            .write_all(&[255; 8])
            .unwrap();
        fixture.host.connections[0]
            .socket
            .shutdown(std::net::Shutdown::Write)
            .unwrap();
        fixture.host.connections[0].queue(HostMessage::Event {
            id: SocketId {
                slot: 0,
                generation: 0,
            },
            event: Event::Connected { mss: 1460 },
        });
        fixture.host.flush(0);
        assert!(matches!(
            fixture.network.try_recv().unwrap(),
            ToNetwork::End { session, how: EndHow::Abort } if session == pack(0, 1)
        ));
        assert!(fixture.network.try_recv().is_err());
        assert!(fixture.host.connections.is_empty());
    }
}
