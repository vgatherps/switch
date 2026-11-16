//! [`Session`], one application's connection to the daemon, and its errors.

use std::cell::Cell;
use std::fmt;
use std::io::{self, Read, Write};
use std::marker::PhantomData;
use std::net::{Shutdown, SocketAddrV4};
use std::os::fd::AsRawFd;
use std::os::unix::net::UnixStream;
use std::path::Path;
use std::sync::mpsc::{self, Receiver, Sender, TryRecvError};
use std::thread::{self, JoinHandle};

use ceph_ring::Writer;
use shared_memory::Backing;
use slice_ringbuffer::RingReader;

use crate::rx::{OwnedFrame, PacketMeta};

use crate::control::{
    ClientMessage, ConfigError, DecodeError, EndReason, Event, HostMessage, ListenEndpoint, Op,
    SocketId, TcpConfig, UdpConfig, VERSION, WelcomeStatus, write_queued,
};
use crate::send::{self, TCP_SEND_HEADER, UDP_SEND_HEADER};

/// Why [`Session::connect`] failed.
#[derive(Debug)]
pub enum ConnectError {
    /// Connecting to the control socket, the handshake I/O, opening either ring or starting
    /// the reader thread failed. The daemon closing the connection before its `Welcome` is
    /// `UnexpectedEof`; a first message other than `Welcome` is `InvalidData`. On Linux,
    /// attaching to the send ring fails with `Unsupported` when glibc registered no
    /// restartable sequences for the process.
    Io(io::Error),
    /// The daemon refused the session.
    Rejected(WelcomeStatus),
    /// The daemon speaks protocol version `host`, not [`VERSION`].
    Version { host: u16 },
    /// The daemon sent bytes that are not a valid control message.
    Protocol(DecodeError),
}

/// Why opening a socket failed. Nothing was sent to the daemon and no slot was taken.
#[derive(Debug)]
pub enum OpenError {
    /// Every socket slot is open or closing.
    NoFreeSlot,
    /// The configuration fails its `check`.
    Config(ConfigError),
}

/// Why [`Session::finish`] could not commit the end record. Nothing was committed; poll and
/// call `finish` again.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FinishError {
    /// Control messages are still waiting to be written to the daemon.
    ControlPending,
    /// The send ring of the current CPU has no room for the end record.
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

/// Why a send was not committed. Nothing was committed and no sequence number was used.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SendError {
    /// The send ring of the CPU the thread is running on has no room. Every session sending
    /// from that CPU writes into the same ring, so another application can fill it. Space
    /// returns as the daemon takes records; poll and retry.
    Full,
    /// Send header plus payload exceed [`Session::max_record`]: a TCP payload is at most
    /// `max_record() - 16` bytes, a UDP payload `max_record() - 24`.
    TooLarge,
    /// The socket is closing, its open was rejected, it was freed, or it is a TCP socket after
    /// [`Session::shutdown`].
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

/// How a session was lost: the error of [`Session::poll`] and [`Session::poll_events`].
///
/// | How the session ended | What a poll returns | What the daemon does with open TCP connections |
/// |---|---|---|
/// | the daemon is stopped (SIGINT or SIGTERM) | `Ended(HostShutdown)`, or `Disconnected` | closes the network card without closing them; peers see no FIN or reset |
/// | the daemon is killed or crashes | `Disconnected` | nothing |
/// | the daemon kicked the session | `Ended(Kicked)` | resets them |
/// | the session broke the protocol, such as a send before `Connected` | `Ended(ProtocolError)` | resets them |
/// | writing to the control socket failed | `Io(..)` or `Disconnected` | resets them, as for a dropped session |
/// | the daemon sent bytes the session cannot decode | `Protocol(..)` | nothing until the session is dropped; then resets them |
///
/// The first `Err` carries the reason; later polls may return `Ok` or a different `Lost`.
/// A lost session cannot be resumed: connect a new one and open new sockets.
#[derive(Debug)]
pub enum Lost {
    /// The daemon ended the session.
    Ended(EndReason),
    /// The control connection closed without the daemon's [`EndReason::Finished`].
    Disconnected,
    /// Reading or writing the control socket failed, or the daemon sent a message that is not
    /// valid at this point (kind `InvalidData`).
    Io(io::Error),
    /// The daemon sent bytes that are not a valid control message.
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

/// One application's connection to the junction daemon: TCP and UDP sockets on the network
/// card the daemon owns, with packet data passed through shared memory.
///
/// Most programs do not use a `Session` directly: they connect one and hand it to
/// `Net::remote` in `switch::net`, and their tasks then run against the daemon. A program
/// with its own loop and no async runtime uses the methods here.
///
/// Only [`connect`](Self::connect) waits. Opening, sending and polling return at once, and
/// the program calls them in its own loop: open sockets, poll for events and packets, send.
/// One background thread, `junction-ctl`, reads the control socket. A `Session` is neither
/// `Send` nor `Sync`; it stays on the thread that connected it.
///
/// # Socket ids and slots
///
/// A session has a table of [`max_sockets`](Self::max_sockets) socket slots. Opening a socket
/// takes a free slot and returns its [`SocketId`]. A slot moves through these states:
///
/// ```text
///             connect_tcp / listen_tcp / bind_udp
///    free -----------------------------------------> open
///     ^  ^                                            |  |
///     |  |              Rejected(code)                |  |
///     |  +--------------------------------------------+  |
///     |                                                  | close / abort
///     |                 Freed or Rejected(code)          v
///     +--------------------------------------------- closing
///
///    (the next open of a freed slot gets generation + 1)
/// ```
///
/// Other events leave the slot's state alone. A closing socket still occupies its slot, so
/// [`OpenError::NoFreeSlot`] means every slot is open or closing. An id is stale when its slot
/// has been opened again since; methods panic on a stale id, and packets for one are dropped.
///
/// # Opening sockets
///
/// [`connect_tcp`](Self::connect_tcp), [`listen_tcp`](Self::listen_tcp) and
/// [`bind_udp`](Self::bind_udp) check the configuration, take a slot and return the id at
/// once; the daemon answers later with [`Event::Connected`], [`Event::Bound`] or
/// [`Event::Rejected`]. Wait for `Connected` or `Bound` before sending on the socket: opens
/// travel over the control socket and sends through the send ring, which are not ordered, and
/// a send that reaches the daemon before its open ends the session with
/// [`EndReason::ProtocolError`]. The session does not check this. Addresses are IPv4 only.
///
/// A TCP listener is one connection, not an accept queue: when a peer connects, the
/// listening socket becomes that connection and gets `Connected`. To serve several
/// connections, keep several listeners open on the same port and open a new one when one is
/// taken. Listeners of different sessions on the same port all wait for connections on it.
///
/// # Closing sockets
///
/// | Call | What the daemon does | Then |
/// |---|---|---|
/// | [`shutdown(id)`](Self::shutdown) | TCP only. Sends a FIN after the data already sent; the socket can still receive. | [`Event::Delivered`] once the peer acknowledges |
/// | [`close(id, false)`](Self::close) | Waits until every send ring is empty and the socket's queued bytes are written, then closes the connection with a FIN and removes the socket. Resets instead if it holds received bytes the program never read. For UDP, waits the same way, then unbinds. | [`Event::Freed`] |
/// | [`close(id, true)`](Self::close) | Resets the connection at once and discards queued bytes; unbinds a UDP socket at once. | [`Event::Freed`] |
/// | [`abort(id)`](Self::abort) | TCP only. Resets the connection at once and discards queued bytes. | [`Event::Freed`] |
///
/// A graceful close waits for the send rings of all sessions, so it can wait while another
/// application keeps sending. Until `Freed`, the socket still exists in the daemon, and opening
/// another socket on its port can be refused. Pass `unread = true` when the program dropped
/// received data without reading it.
///
/// # Polling
///
/// [`poll_events`](Self::poll_events) writes control messages still waiting to be sent and
/// calls back once per event that has arrived. [`poll_packets`](Self::poll_packets) calls back
/// once per received packet, then reclaims receive-ring space. [`poll`](Self::poll) does both,
/// events first. Polling is the only way to learn that the session is lost ([`Lost`]); the
/// send methods do not look, so a program that only sends must still poll.
///
/// Events and packets travel on different channels. Within one `poll`, every event callback
/// runs before any packet callback. A socket's first packet can arrive before its
/// `Connected`; a program that needs the event first holds such packets per socket until it
/// arrives. By the time the program handles [`Event::PeerFin`], every byte the peer sent
/// before its FIN is in the ring, and the same `poll`, or an earlier one, delivers it. Packets
/// for freed sockets or earlier generations of a slot are dropped.
///
/// # Received packets and ring space
///
/// Each packet comes as a [`PacketMeta`] and an [`OwnedFrame`] that points into the session's
/// receive ring, which is `--rx-size` bytes for all of the session's sockets. A TCP chunk
/// larger than the ring can hold arrives in several frames; a UDP datagram larger than
/// `--rx-size` minus 40 bytes is dropped by the daemon. A frame's space returns to the daemon
/// only after it is dropped and a later poll reclaims it, and reclaiming stops at the oldest
/// frame still held: one long-held frame blocks the reuse of everything received after it, on
/// every socket. While the ring is full the daemon keeps the next pending data for each socket
/// and retries; TCP flow control slows the peer.
///
/// # Ending a session
///
/// [`finish`](Self::finish) asks the daemon to end the session after everything already
/// sent; keep polling until [`closed`](Self::closed) is true. The daemon then applies every
/// close and shutdown already sent and closes the remaining TCP connections with a FIN. A
/// connection that still has bytes queued closes after they are sent; if the peer stops
/// acknowledging, the daemon gives up after 8 retransmission back-offs and resets it. A
/// connection with received data the program never read is reset.
///
/// Dropping a `Session` without a successful `finish` is treated as a crash: the daemon resets
/// every socket the session still had. Dropping it after a successful `finish` but before
/// `closed()` still ends it gracefully. `Drop` closes the control socket and joins the reader
/// thread.
///
/// # Panics
///
/// | Method | Panics when |
/// |---|---|
/// | `connect_tcp`, `listen_tcp`, `bind_udp` | `finish` has succeeded |
/// | `send_tcp`, `send_udp` | `finish` has succeeded; the id's slot is out of range or the id is stale; the socket is of the other kind |
/// | `shutdown` | `finish` has succeeded; out-of-range or stale id; a UDP socket; the socket is not open |
/// | `close` | before a successful `finish`: out-of-range or stale id; the socket is already closing |
/// | `abort` | before a successful `finish`: out-of-range or stale id; a UDP socket; the socket is already closing |
///
/// After a successful `finish`, `close` and `abort` do nothing, so cleanup in destructors
/// stays safe. On an id whose socket was rejected or freed and whose slot has not been opened
/// again, sends return [`SendError::NotOpen`] and `close` and `abort` do nothing.
pub struct Session {
    socket: UnixStream,
    writer: Writer,
    rx: RingReader,
    stream: u64,
    frame_capacity: u32,
    max_record: u32,
    next_seq: u64,
    slots: Vec<Slot>,
    messages: Receiver<Result<HostMessage, Lost>>,
    reader: Option<JoinHandle<()>>,
    read_error: Option<Lost>,
    output: Vec<u8>,
    written: usize,
    write_error: Option<io::Error>,
    finished: bool,
    closed: bool,
    not_sync: PhantomData<Cell<()>>,
}

impl Session {
    /// Connects to the daemon's control socket at `path` (`control.sock` in its directory),
    /// asking for `max_sockets` socket slots.
    ///
    /// Blocks, with no timeout, until the daemon answers. A daemon that is still opening its
    /// network card answers once it has finished. Then starts the `junction-ctl` reader thread,
    /// which inherits the calling thread's CPU affinity.
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
        let read_socket = socket.try_clone().map_err(ConnectError::Io)?;
        let (sender, messages) = mpsc::channel();
        let reader = thread::Builder::new()
            .name("junction-ctl".into())
            .spawn(move || read_control(read_socket, bytes, sender))
            .map_err(ConnectError::Io)?;
        Ok(Self {
            socket,
            writer,
            rx,
            stream: welcome.stream,
            frame_capacity: welcome.frame_capacity,
            max_record: welcome.max_record,
            next_seq: 0,
            slots: (0..max_sockets)
                .map(|_| Slot {
                    state: State::Free,
                    generation: 0,
                    tcp: false,
                    shut: false,
                })
                .collect(),
            messages,
            reader: Some(reader),
            read_error: None,
            output: Vec::new(),
            written: 0,
            write_error: None,
            finished: false,
            closed: false,
            not_sync: PhantomData,
        })
    }

    /// The daemon's IP MTU minus 28: the largest UDP payload that fits one frame.
    pub fn frame_capacity(&self) -> u32 {
        self.frame_capacity
    }

    /// The largest send record in bytes, send header included. See [`SendError::TooLarge`].
    pub fn max_record(&self) -> usize {
        self.max_record as usize
    }

    /// The number of socket slots, as passed to [`connect`](Self::connect).
    pub fn max_sockets(&self) -> usize {
        self.slots.len()
    }

    /// Received records still retained by frames, excluding unread ring records.
    #[cfg(feature = "testkit")]
    pub fn outstanding_rx_frames(&self) -> usize {
        self.rx.outstanding_frames()
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

    /// Opens a TCP connection to `remote` from `local`. The answer is [`Event::Connected`],
    /// [`Event::Rejected`], or, when the peer refuses, [`Event::Closed`] without `Connected`.
    /// The local port must be nonzero.
    ///
    /// # Panics
    /// Panics if [`finish`](Self::finish) has succeeded.
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

    /// Opens a TCP socket that waits for one connection on `local`. The answer is
    /// [`Event::Rejected`], or [`Event::Connected`] when a peer connects.
    ///
    /// # Panics
    /// Panics if [`finish`](Self::finish) has succeeded.
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

    /// Binds a UDP socket. The answer is [`Event::Bound`] or [`Event::Rejected`].
    ///
    /// # Panics
    /// Panics if [`finish`](Self::finish) has succeeded.
    pub fn bind_udp(&mut self, config: UdpConfig) -> Result<SocketId, OpenError> {
        self.assert_unfinished();
        config.check().map_err(OpenError::Config)?;
        let id = self.allocate(false)?;
        self.queue(Op::BindUdp { id, config });
        Ok(id)
    }

    /// Commits `payload` to the send ring as one record for the TCP socket `id`.
    ///
    /// `Ok` means the record is in the send ring, nothing more: the daemon has not taken it and
    /// the peer has not received it. The daemon passes the bytes to its network stack as fast as
    /// the stack accepts them and queues the rest without limit, so a peer that does not read
    /// never makes this return `Full`. Received data chunks need not match these records. Send
    /// only after the socket's [`Event::Connected`].
    ///
    /// # Panics
    /// Panics if [`finish`](Self::finish) has succeeded, for an out-of-range or stale id, or
    /// for a UDP socket.
    pub fn send_tcp(&mut self, id: SocketId, payload: &[u8]) -> Result<(), SendError> {
        self.assert_unfinished();
        let slot = self.slot_for(id);
        assert!(slot.tcp, "TCP send requires a TCP socket: {id:?}");
        if slot.state != State::Open || slot.shut {
            return Err(SendError::NotOpen);
        }
        let mut header = [0; TCP_SEND_HEADER];
        send::write_tcp(&mut header, id);
        self.push(&header, payload)
    }

    /// Commits `payload` to the send ring as one datagram for the UDP socket `id`, to `to`.
    ///
    /// `Ok` means the record is in the send ring. A datagram the daemon's network stack cannot
    /// send, for example because the destination's hardware address is not resolved yet, is
    /// dropped after `Ok`. Send only after the socket's [`Event::Bound`].
    ///
    /// # Panics
    /// Panics if [`finish`](Self::finish) has succeeded, for an out-of-range or stale id, or
    /// for a TCP socket.
    pub fn send_udp(
        &mut self,
        id: SocketId,
        to: SocketAddrV4,
        payload: &[u8],
    ) -> Result<(), SendError> {
        self.assert_unfinished();
        let slot = self.slot_for(id);
        assert!(!slot.tcp, "UDP send requires a UDP socket: {id:?}");
        if slot.state != State::Open {
            return Err(SendError::NotOpen);
        }
        let mut header = [0; UDP_SEND_HEADER];
        send::write_udp(&mut header, id, to);
        self.push(&header, payload)
    }

    fn push(&mut self, header: &[u8], payload: &[u8]) -> Result<(), SendError> {
        match self
            .writer
            .try_push_with_header(header, payload, self.stream, self.next_seq)
        {
            Ok(()) => {
                self.next_seq += 1;
                Ok(())
            }
            Err(ceph_ring::PushError::Full) => Err(SendError::Full),
            Err(ceph_ring::PushError::TooLarge) => Err(SendError::TooLarge),
        }
    }

    /// Half-closes the TCP socket `id`: the daemon sends a FIN after the data already sent.
    /// [`Event::Delivered`] follows once the peer acknowledges it. Later sends return
    /// [`SendError::NotOpen`].
    ///
    /// # Panics
    /// Panics if [`finish`](Self::finish) has succeeded, for an out-of-range or stale id, for
    /// a UDP socket, or if the socket is not open.
    pub fn shutdown(&mut self, id: SocketId) {
        self.assert_unfinished();
        let slot = self.slot_for(id);
        assert!(slot.tcp, "shutdown requires a TCP socket: {id:?}");
        assert!(
            slot.state == State::Open,
            "shutdown requires an open socket: {id:?}"
        );
        slot.shut = true;
        self.queue(Op::ShutdownTcp { id });
    }

    /// Closes the socket `id`; [`Event::Freed`] follows. `unread` says the program dropped
    /// received data without reading it. See [Closing sockets](Self#closing-sockets).
    /// Does nothing if the socket's open was rejected or it was already freed.
    ///
    /// After a successful [`finish`](Self::finish), does nothing, without checking `id`.
    ///
    /// # Panics
    /// Before a successful `finish`, panics for an out-of-range or stale id, or if the socket
    /// is already closing.
    pub fn close(&mut self, id: SocketId, unread: bool) {
        if self.finished {
            return;
        }
        if self.begin_close(id) {
            self.queue(Op::Close { id, unread });
        }
    }

    /// Resets the TCP connection `id` and removes the socket; [`Event::Freed`] follows. Does
    /// nothing if the socket's open was rejected or it was already freed.
    ///
    /// After a successful [`finish`](Self::finish), does nothing, without checking `id`.
    ///
    /// # Panics
    /// Before a successful `finish`, panics for an out-of-range or stale id, for a UDP socket,
    /// or if the socket is already closing.
    pub fn abort(&mut self, id: SocketId) {
        if self.finished {
            return;
        }
        assert!(self.slot_for(id).tcp, "abort requires a TCP socket: {id:?}");
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
        if let Err(error) = write_queued(&mut self.socket, &mut self.output, &mut self.written) {
            self.write_error = Some(error);
        }
    }

    /// Writes control messages still waiting to be sent, then calls `on_event` for each event
    /// that has arrived. Never waits.
    ///
    /// Returns `Ok` and sets [`closed`](Self::closed) when the daemon confirms the end of a
    /// finished session. A control connection that closes without that confirmation is
    /// [`Lost::Disconnected`], even after `finish` succeeded. After `closed()`, returns `Ok`
    /// and does nothing.
    pub fn poll_events(&mut self, mut on_event: impl FnMut(SocketId, Event)) -> Result<(), Lost> {
        if self.closed {
            return Ok(());
        }
        if self.written < self.output.len() {
            self.flush();
        }
        if let Some(error) = self.write_error.take() {
            return Err(lost_io(error));
        }
        if let Some(error) = &self.read_error {
            return Err(repeat_lost(error));
        }
        loop {
            match self.messages.try_recv() {
                Ok(Ok(HostMessage::Event { id, event })) => {
                    self.event(id, event)?;
                    on_event(id, event);
                }
                Ok(Ok(HostMessage::End(EndReason::Finished))) => {
                    if !self.finished {
                        return Err(invalid_host("Finished before session finish"));
                    }
                    self.closed = true;
                    return Ok(());
                }
                Ok(Ok(HostMessage::End(reason))) => return Err(Lost::Ended(reason)),
                Ok(Ok(HostMessage::Welcome(_))) => {
                    return Err(invalid_host("second Welcome on live session"));
                }
                Ok(Err(error)) => {
                    self.read_error = Some(error);
                    return Err(repeat_lost(self.read_error.as_ref().unwrap()));
                }
                Err(TryRecvError::Empty) => return Ok(()),
                Err(TryRecvError::Disconnected) => return Err(Lost::Disconnected),
            }
        }
    }

    /// [`poll_events`](Self::poll_events), then [`poll_packets`](Self::poll_packets). Never
    /// waits.
    ///
    /// When `poll_events` reports loss, the packets already in the ring are still delivered
    /// before the error is returned. See [Polling](Self#polling) for ordering.
    pub fn poll(
        &mut self,
        on_event: impl FnMut(SocketId, Event),
        on_packet: impl FnMut(SocketId, PacketMeta, OwnedFrame),
    ) -> Result<(), Lost> {
        let result = self.poll_events(on_event);
        self.poll_packets(on_packet);
        result
    }

    /// Calls `on_packet` for each packet in the receive ring, then reclaims ring space.
    /// Never waits, and keeps working after the session is lost.
    ///
    /// Packets for freed sockets or earlier generations of a slot are dropped.
    pub fn poll_packets(&mut self, mut on_packet: impl FnMut(SocketId, PacketMeta, OwnedFrame)) {
        while let Some((meta, frame)) = self.rx.next() {
            let id = SocketId {
                slot: meta.slot,
                generation: meta.generation,
            };
            if self
                .slots
                .get(id.slot as usize)
                .is_some_and(|slot| slot.state != State::Free && slot.generation == id.generation)
            {
                on_packet(id, meta, frame);
            }
        }
        self.rx.reclaim();
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

    /// Ends the session once the daemon has taken everything already sent: commits the end
    /// record to the send ring after every control message is written.
    ///
    /// A successful call does not mean the session is over: keep polling until
    /// [`closed`](Self::closed) is true. Further calls return `Ok` and do nothing. After
    /// success, opening, sending and `shutdown` panic; `close` and `abort` do nothing.
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

    /// Whether the daemon confirmed the end of the session after [`finish`](Self::finish).
    pub fn closed(&self) -> bool {
        self.closed
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        let _ = self.socket.shutdown(Shutdown::Both);
        if let Some(reader) = self.reader.take() {
            let _ = reader.join();
        }
    }
}

fn read_control(
    mut socket: UnixStream,
    mut input: Vec<u8>,
    messages: Sender<Result<HostMessage, Lost>>,
) {
    // The cloned descriptors share O_NONBLOCK. Wait for readability rather than
    // changing the reader to blocking mode and blocking application writes too.
    let mut readable = false;
    let error = loop {
        match HostMessage::decode(&input) {
            Ok(Some((message, used))) => {
                input.drain(..used);
                if messages.send(Ok(message)).is_err() {
                    return;
                }
                continue;
            }
            Ok(None) => {}
            Err(error) => break Lost::Protocol(error),
        }
        if !readable {
            let mut descriptor = libc::pollfd {
                fd: socket.as_raw_fd(),
                events: libc::POLLIN,
                revents: 0,
            };
            // SAFETY: descriptor is one initialized pollfd, valid for this call.
            if unsafe { libc::poll(&mut descriptor, 1, -1) } < 0 {
                let error = io::Error::last_os_error();
                if error.kind() == io::ErrorKind::Interrupted {
                    continue;
                }
                break lost_io(error);
            }
            readable = true;
        }
        let mut chunk = [0; 4096];
        match socket.read(&mut chunk) {
            Ok(0) => break Lost::Disconnected,
            Ok(n) => input.extend_from_slice(&chunk[..n]),
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => readable = false,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => break lost_io(error),
        }
    };
    let _ = messages.send(Err(error));
}

fn repeat_lost(error: &Lost) -> Lost {
    match error {
        Lost::Ended(reason) => Lost::Ended(*reason),
        Lost::Disconnected => Lost::Disconnected,
        Lost::Io(error) => Lost::Io(match error.raw_os_error() {
            Some(code) => io::Error::from_raw_os_error(code),
            None => io::Error::new(error.kind(), error.to_string()),
        }),
        Lost::Protocol(error) => Lost::Protocol(*error),
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
