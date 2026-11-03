//! Client-side control socket and socket-slot ownership for one producer thread.
//!
//! `Session::finish` first flushes queued control operations, then commits the
//! cephalopod end record. Retry backpressure errors while polling events.
//! Only the host's `EndReason::Finished` acknowledgement makes `Session::closed`
//! true; a disconnect without that acknowledgement is session loss.
//! After a successful `finish`, opening sockets, sending and shutting down TCP panic.
//! `close` and `abort` become no-ops so Drop cleanup remains safe, even while
//! unwinding. Repeated `finish` calls and polling for the host acknowledgement
//! remain valid.

use std::cell::Cell;
use std::fmt;
use std::io::{self, Read, Write};
use std::marker::PhantomData;
use std::net::SocketAddrV4;
use std::os::unix::net::UnixStream;
use std::path::Path;

use ceph_ring::Writer;
use shared_memory::Backing;
use slice_ringbuffer::RingReader;

use crate::control::{
    ClientMessage, ConfigError, DecodeError, EndReason, Event, HostMessage, ListenEndpoint, Op,
    SocketId, TcpConfig, UdpConfig, VERSION, WelcomeStatus,
};
use crate::send::{self, TCP_SEND_HEADER, UDP_SEND_HEADER};

#[derive(Debug)]
pub enum ConnectError {
    Io(io::Error),
    Rejected(WelcomeStatus),
    Version { host: u16 },
    Protocol(DecodeError),
}

#[derive(Debug)]
pub enum OpenError {
    NoFreeSlot,
    Config(ConfigError),
}

/// Backpressure preventing the session's end record from being committed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FinishError {
    /// Control operations still await writing to the host. Poll events and retry.
    ControlPending,
    /// The cephalopod ingress has no room for the end record. Retry later.
    StreamFull,
}

impl fmt::Display for FinishError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::ControlPending => "session control output is still pending",
            Self::StreamFull => "session ingress stream is full",
        })
    }
}

impl std::error::Error for FinishError {}

/// A send record could not be committed to the session stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SendError {
    Full,
    TooLarge,
    NotOpen,
}

impl fmt::Display for SendError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Full => "session ingress stream is full",
            Self::TooLarge => "send record exceeds ingress max_record",
            Self::NotOpen => "socket is not open for sending",
        })
    }
}

impl std::error::Error for SendError {}

#[derive(Debug)]
pub enum Lost {
    Ended(EndReason),
    Disconnected,
    Io(io::Error),
    Protocol(DecodeError),
}

impl fmt::Display for ConnectError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(f, "session connection failed: {error}"),
            Self::Rejected(status) => write!(f, "session rejected: {status:?}"),
            Self::Version { host } => write!(f, "host control version {host}, expected {VERSION}"),
            Self::Protocol(error) => write!(f, "invalid host handshake: {error}"),
        }
    }
}

impl std::error::Error for ConnectError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Protocol(error) => Some(error),
            _ => None,
        }
    }
}

impl fmt::Display for OpenError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoFreeSlot => f.write_str("no free session socket slot"),
            Self::Config(error) => write!(f, "invalid socket config: {error}"),
        }
    }
}

impl std::error::Error for OpenError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Config(error) => Some(error),
            Self::NoFreeSlot => None,
        }
    }
}

impl fmt::Display for Lost {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Ended(reason) => write!(f, "host ended session: {reason:?}"),
            Self::Disconnected => f.write_str("host disconnected"),
            Self::Io(error) => write!(f, "session I/O failed: {error}"),
            Self::Protocol(error) => write!(f, "invalid host message: {error}"),
        }
    }
}

impl std::error::Error for Lost {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Protocol(error) => Some(error),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum State {
    Free,
    Open,
    Closing,
}

struct Slot {
    state: State,
    generation: u32,
    tcp: bool,
    shut: bool,
}

pub struct Session {
    socket: UnixStream,
    writer: Writer,
    _rx: RingReader,
    stream: u64,
    next_seq: u64,
    slots: Vec<Slot>,
    input: Vec<u8>,
    output: Vec<u8>,
    written: usize,
    write_error: Option<io::Error>,
    finished: bool,
    closed: bool,
    not_sync: PhantomData<Cell<()>>,
}

impl Session {
    pub fn connect(path: &Path, max_sockets: u16) -> Result<Self, ConnectError> {
        let mut socket = UnixStream::connect(path).map_err(ConnectError::Io)?;
        let mut bytes = Vec::new();
        ClientMessage::Hello {
            version: VERSION,
            max_sockets,
        }
        .encode(&mut bytes);
        socket.write_all(&bytes).map_err(ConnectError::Io)?;
        bytes.clear();
        let welcome = loop {
            if let Some((message, used)) =
                HostMessage::decode(&bytes).map_err(ConnectError::Protocol)?
            {
                bytes.drain(..used);
                match message {
                    HostMessage::Welcome(welcome) => break welcome,
                    _ => {
                        return Err(ConnectError::Io(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "first host message is not Welcome",
                        )));
                    }
                }
            }
            let mut chunk = [0; 4096];
            match socket.read(&mut chunk) {
                Ok(0) => {
                    return Err(ConnectError::Io(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "host disconnected before Welcome",
                    )));
                }
                Ok(n) => bytes.extend_from_slice(&chunk[..n]),
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(error) => return Err(ConnectError::Io(error)),
            }
        };
        if welcome.version != VERSION {
            return Err(ConnectError::Version {
                host: welcome.version,
            });
        }
        if welcome.status != WelcomeStatus::Ok {
            return Err(ConnectError::Rejected(welcome.status));
        }
        let writer = Writer::attach(&welcome.ingress).map_err(ConnectError::Io)?;
        #[cfg(target_os = "linux")]
        let backing = Backing::open_file(Path::new(&welcome.rx)).map_err(ConnectError::Io)?;
        #[cfg(target_os = "macos")]
        let backing = Backing::open_shm(&welcome.rx).map_err(ConnectError::Io)?;
        let rx = RingReader::open(&backing).map_err(ConnectError::Io)?;
        drop(backing);
        socket.set_nonblocking(true).map_err(ConnectError::Io)?;
        Ok(Self {
            socket,
            writer,
            _rx: rx,
            stream: welcome.stream,
            next_seq: 0,
            slots: (0..max_sockets)
                .map(|_| Slot {
                    state: State::Free,
                    generation: 0,
                    tcp: false,
                    shut: false,
                })
                .collect(),
            input: bytes,
            output: Vec::new(),
            written: 0,
            write_error: None,
            finished: false,
            closed: false,
            not_sync: PhantomData,
        })
    }

    fn allocate(&mut self, tcp: bool) -> Result<SocketId, OpenError> {
        let (index, slot) = self
            .slots
            .iter_mut()
            .enumerate()
            .find(|(_, slot)| slot.state == State::Free)
            .ok_or(OpenError::NoFreeSlot)?;
        slot.generation = slot.generation.wrapping_add(1);
        slot.state = State::Open;
        slot.tcp = tcp;
        slot.shut = false;
        Ok(SocketId {
            slot: index as u32,
            generation: slot.generation,
        })
    }

    /// # Panics
    /// Panics if `finish()` has succeeded, before validating the configuration.
    pub fn connect_tcp(
        &mut self,
        config: TcpConfig,
        remote: SocketAddrV4,
        local: ListenEndpoint,
    ) -> Result<SocketId, OpenError> {
        self.assert_unfinished();
        config.check().map_err(OpenError::Config)?;
        let id = self.allocate(true)?;
        self.queue(Op::ConnectTcp {
            id,
            config,
            remote,
            local,
        });
        Ok(id)
    }

    /// # Panics
    /// Panics if `finish()` has succeeded, before validating the configuration.
    pub fn listen_tcp(
        &mut self,
        config: TcpConfig,
        local: ListenEndpoint,
    ) -> Result<SocketId, OpenError> {
        self.assert_unfinished();
        config.check().map_err(OpenError::Config)?;
        let id = self.allocate(true)?;
        self.queue(Op::ListenTcp { id, config, local });
        Ok(id)
    }

    /// # Panics
    /// Panics if `finish()` has succeeded, before validating the configuration.
    pub fn bind_udp(&mut self, config: UdpConfig) -> Result<SocketId, OpenError> {
        self.assert_unfinished();
        config.check().map_err(OpenError::Config)?;
        let id = self.allocate(false)?;
        self.queue(Op::BindUdp { id, config });
        Ok(id)
    }

    /// Overwrites `record[..TCP_SEND_HEADER]` with the header and commits one record.
    /// `Full` consumes no sequence number; retry after space becomes available.
    ///
    /// Valid only after this socket's `Connected` event. An earlier send can reach
    /// the host before the open because the stream and control socket are unordered,
    /// ending the session as a protocol error. Session does not enforce this ordering.
    ///
    /// # Panics
    /// Panics after successful `finish()`, for insufficient header headroom, an
    /// invalid or stale socket ID, or a UDP socket.
    pub fn send_tcp(&mut self, id: SocketId, record: &mut [u8]) -> Result<(), SendError> {
        self.assert_unfinished();
        assert!(
            record.len() >= TCP_SEND_HEADER,
            "TCP send requires header headroom"
        );
        let slot = self.slot_for(id);
        assert!(slot.tcp, "TCP send requires a TCP socket: {id:?}");
        if slot.state != State::Open || slot.shut {
            return Err(SendError::NotOpen);
        }
        send::write_tcp(record, id);
        self.push(record)
    }

    /// Overwrites `record[..UDP_SEND_HEADER]` with the header and commits one record.
    /// `Full` consumes no sequence number; retry after space becomes available.
    ///
    /// Valid only after this socket's `Bound` event. An earlier send can reach
    /// the host before the open because the stream and control socket are unordered,
    /// ending the session as a protocol error. Session does not enforce this ordering.
    ///
    /// # Panics
    /// Panics after successful `finish()`, for insufficient header headroom, an
    /// invalid or stale socket ID, or a TCP socket.
    pub fn send_udp(
        &mut self,
        id: SocketId,
        to: SocketAddrV4,
        record: &mut [u8],
    ) -> Result<(), SendError> {
        self.assert_unfinished();
        assert!(
            record.len() >= UDP_SEND_HEADER,
            "UDP send requires header headroom"
        );
        let slot = self.slot_for(id);
        assert!(!slot.tcp, "UDP send requires a UDP socket: {id:?}");
        if slot.state != State::Open {
            return Err(SendError::NotOpen);
        }
        send::write_udp(record, id, to);
        self.push(record)
    }

    fn push(&mut self, record: &[u8]) -> Result<(), SendError> {
        match self.writer.try_push(record, self.stream, self.next_seq) {
            Ok(()) => {
                self.next_seq += 1;
                Ok(())
            }
            Err(ceph_ring::PushError::Full) => Err(SendError::Full),
            Err(ceph_ring::PushError::TooLarge) => Err(SendError::TooLarge),
        }
    }

    /// # Panics
    /// Panics if `finish()` has succeeded, or the socket is not open.
    pub fn shutdown(&mut self, id: SocketId) {
        self.assert_unfinished();
        let slot = self.slot_for(id);
        assert!(
            slot.state == State::Open,
            "shutdown requires an open socket: {id:?}"
        );
        slot.shut = true;
        self.queue(Op::ShutdownTcp { id });
    }

    /// After successful `finish()`, does nothing, without validating the ID.
    /// This permits socket cleanup from Drop, including during unwinding.
    ///
    /// # Panics
    /// Before successful `finish()`, panics if the socket ID is invalid or closing.
    pub fn close(&mut self, id: SocketId, unread: bool) {
        if self.finished {
            return;
        }
        if self.begin_close(id) {
            self.queue(Op::Close { id, unread });
        }
    }

    /// After successful `finish()`, does nothing, without validating the ID.
    /// This permits socket cleanup from Drop, including during unwinding.
    ///
    /// # Panics
    /// Before successful `finish()`, panics if the socket ID is invalid or closing.
    pub fn abort(&mut self, id: SocketId) {
        if self.finished {
            return;
        }
        if self.begin_close(id) {
            self.queue(Op::AbortTcp { id });
        }
    }

    fn assert_unfinished(&self) {
        assert!(
            !self.finished,
            "socket operation after successful Session::finish()"
        );
    }

    fn slot_for(&mut self, id: SocketId) -> &mut Slot {
        let slot = self.slots.get_mut(id.slot as usize).unwrap_or_else(|| {
            panic!("socket slot outside session capacity: {id:?}");
        });
        assert_eq!(slot.generation, id.generation, "stale socket id: {id:?}");
        slot
    }

    fn begin_close(&mut self, id: SocketId) -> bool {
        let slot = self.slot_for(id);
        match slot.state {
            State::Free => false,
            State::Open => {
                slot.state = State::Closing;
                true
            }
            State::Closing => panic!("socket is already closing: {id:?}"),
        }
    }

    fn queue(&mut self, op: Op) {
        ClientMessage::Op(op).encode(&mut self.output);
        self.flush();
    }

    fn flush(&mut self) {
        if self.write_error.is_some() {
            return;
        }
        while self.written < self.output.len() {
            match self.socket.write(&self.output[self.written..]) {
                Ok(0) => {
                    self.write_error = Some(io::Error::new(
                        io::ErrorKind::WriteZero,
                        "control socket write returned zero",
                    ));
                    return;
                }
                Ok(n) => self.written += n,
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => return,
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(error) => {
                    self.write_error = Some(error);
                    return;
                }
            }
        }
        self.output.clear();
        self.written = 0;
    }

    /// Flushes control output and handles available host events without blocking.
    ///
    /// A successful finish acknowledgement sets `closed()` and returns `Ok(())`.
    /// EOF without that acknowledgement returns `Lost::Disconnected`, even after
    /// `finish()` succeeded. An unsolicited finish acknowledgement is invalid data.
    pub fn poll_events(&mut self, mut on_event: impl FnMut(SocketId, Event)) -> Result<(), Lost> {
        if self.closed {
            return Ok(());
        }
        self.flush();
        if let Some(error) = self.write_error.take() {
            return Err(lost_io(error));
        }
        loop {
            while let Some((message, used)) =
                HostMessage::decode(&self.input).map_err(Lost::Protocol)?
            {
                self.input.drain(..used);
                match message {
                    HostMessage::Event { id, event } => {
                        self.event(id, event)?;
                        on_event(id, event);
                    }
                    HostMessage::End(EndReason::Finished) => {
                        if !self.finished {
                            return Err(invalid_host("Finished before session finish"));
                        }
                        self.closed = true;
                        return Ok(());
                    }
                    HostMessage::End(reason) => return Err(Lost::Ended(reason)),
                    HostMessage::Welcome(_) => {
                        return Err(invalid_host("second Welcome on live session"));
                    }
                }
            }
            let mut chunk = [0; 4096];
            match self.socket.read(&mut chunk) {
                Ok(0) => return Err(Lost::Disconnected),
                Ok(n) => self.input.extend_from_slice(&chunk[..n]),
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => return Ok(()),
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(error) => return Err(lost_io(error)),
            }
        }
    }

    fn event(&mut self, id: SocketId, event: Event) -> Result<(), Lost> {
        let Some(slot) = self.slots.get_mut(id.slot as usize) else {
            return Err(invalid_host(format!(
                "event for slot {} generation {} outside session capacity {}",
                id.slot,
                id.generation,
                self.slots.len()
            )));
        };
        if slot.generation != id.generation {
            return Err(invalid_host(format!(
                "event for slot {} generation {}, current generation {}",
                id.slot, id.generation, slot.generation
            )));
        }
        match (event, slot.state) {
            (Event::Rejected(_), State::Open | State::Closing) | (Event::Freed, State::Closing) => {
                slot.state = State::Free;
            }
            _ => {}
        }
        Ok(())
    }

    /// Commits the session's end record after all queued control operations are written.
    ///
    /// `ControlPending` means control output is still unsent; no end record was
    /// committed. `StreamFull` means ingress has no room for the end record.
    /// Poll events and retry either error. A successful call is idempotent, but
    /// does not mean retirement is complete: keep polling until `closed()` is true.
    /// Control I/O failures are reported by `poll_events`.
    /// After success, `connect_tcp`, `listen_tcp`, `bind_udp`, `send_tcp`,
    /// `send_udp`, and `shutdown` panic before validating arguments or changing state.
    /// `close` and `abort` instead do nothing, even for invalid IDs, so Drop
    /// cleanup remains safe during unwinding and sends no further operations.
    pub fn finish(&mut self) -> Result<(), FinishError> {
        if self.finished {
            return Ok(());
        }
        self.flush();
        if self.written < self.output.len() {
            return Err(FinishError::ControlPending);
        }
        match self.writer.try_finish(self.stream, self.next_seq) {
            Ok(()) => self.finished = true,
            Err(ceph_ring::PushError::Full) => return Err(FinishError::StreamFull),
            Err(ceph_ring::PushError::TooLarge) => {
                panic!("empty session end record exceeds ingress max_record");
            }
        }
        Ok(())
    }

    /// Whether the host acknowledged successful retirement with `EndReason::Finished`.
    pub fn closed(&self) -> bool {
        self.closed
    }
}

fn invalid_host(message: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> Lost {
    Lost::Io(io::Error::new(io::ErrorKind::InvalidData, message))
}

fn lost_io(error: io::Error) -> Lost {
    match error.kind() {
        io::ErrorKind::BrokenPipe | io::ErrorKind::ConnectionReset => Lost::Disconnected,
        _ => Lost::Io(error),
    }
}
