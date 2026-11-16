//! The control protocol between a [`Session`](crate::session::Session) and the daemon.
//!
//! [`Session`](crate::session::Session) implements all of it. The types here are its
//! vocabulary ([`SocketId`], the socket configurations, [`Event`], [`RejectCode`],
//! [`EndReason`]) and the encoder and decoder of the control socket, for reading a capture of
//! that socket or writing another client.
//!
//! # Channels
//!
//! A session uses three channels:
//!
//! | Channel | Direction | Carries |
//! |---|---|---|
//! | control socket (`control.sock` in the daemon's directory, a Unix stream socket) | both | handshake, socket operations, events, the end-of-session notice |
//! | send ring (one shared file with one ring per CPU, named in [`Welcome::ingress`]) | client to daemon | send records ([`crate::send`]) and the session's end record |
//! | receive ring (one per session, named in [`Welcome::rx`]) | daemon to client | received TCP chunks and UDP datagrams |
//!
//! The control socket and the two rings are not ordered against each other. A send record
//! can reach the daemon before an open sent earlier on the control socket, and a socket's
//! first received packet can reach the client before its [`Event::Connected`].
//!
//! # Handshake
//!
//! ```text
//!  client                                         daemon
//!    | connect(control.sock)                        |
//!    |--- Hello { version, max_sockets } ---------->|
//!    |                                              | checks the version, max_sockets and
//!    |                                              | for a free session; creates the
//!    |                                              | session's receive ring
//!    |<-- Welcome { version, status, stream,        |
//!    |              max_record, frame_capacity,     |
//!    |              ingress, rx } ------------------|
//!    | opens the send ring (ingress) and            |
//!    | the receive ring (rx)                        |
//!    |--- operations ------------------------------>|
//!    |<-- events -----------------------------------|
//! ```
//!
//! The client sends [`ClientMessage::Hello`] first; the daemon drops a connection that has
//! not sent it within 5 seconds. The daemon answers every `Hello` with a [`Welcome`] carrying
//! its own [`VERSION`], whatever the client sent.
//!
//! # Message framing
//!
//! Every message is a [`HEADER_LEN`]-byte header followed by a body. Integers are
//! little-endian; IPv4 addresses are 4 bytes in network order.
//!
//! ```text
//!  offset  size  field
//!  0       1     kind
//!  1       1     flags: bit 0 for kinds 6 (Close: unread) and 70 (Closed: reset_by_peer); otherwise 0
//!  2       2     body length in bytes
//!  4       4     zero
//!  8       ...   body
//! ```
//!
//! A socket id in a body is 8 bytes: slot u32, then generation u32. A TCP config is 12 bytes:
//! `rx_capacity` u32, `tx_capacity` u32, `mss_cap` u16 (0 for none), 2 zero bytes. A local
//! endpoint is 8 bytes: address (all zero when absent), port u16, has-address u8 (0 or 1),
//! 1 zero byte.
//!
//! Client to daemon ([`ClientMessage`]):
//!
//! | Kind | Message | Body length | Body |
//! |---|---|---|---|
//! | 1 | [`Hello`](ClientMessage::Hello) | 8 | magic `JNCT`, version u16, max_sockets u16 |
//! | 2 | [`ConnectTcp`](Op::ConnectTcp) | 36 | id, TCP config, remote address, remote port u16, 2 zero bytes, local endpoint |
//! | 3 | [`ListenTcp`](Op::ListenTcp) | 28 | id, TCP config, local endpoint |
//! | 4 | [`BindUdp`](Op::BindUdp) | 24 | id, local endpoint, rx_queue u32, tx_checksum u8, broadcast u8, 2 zero bytes |
//! | 5 | [`ShutdownTcp`](Op::ShutdownTcp) | 8 | id |
//! | 6 | [`Close`](Op::Close) | 8 | id; flag bit 0 = unread |
//! | 7 | [`AbortTcp`](Op::AbortTcp) | 8 | id |
//!
//! Daemon to client ([`HostMessage`]):
//!
//! | Kind | Message | Body length | Body |
//! |---|---|---|---|
//! | 64 | [`Welcome`] | 28 + both path lengths | magic `JNCT`, version u16, status u8, zero u8, stream u64, max_record u32, frame_capacity u32, ingress length u16, ingress path, rx length u16, rx path |
//! | 65 | [`Connected`](Event::Connected) | 10 | id, mss u16 |
//! | 66 | [`Bound`](Event::Bound) | 8 | id |
//! | 67 | [`Rejected`](Event::Rejected) | 9 | id, [`RejectCode`] u8 |
//! | 68 | [`PeerFin`](Event::PeerFin) | 8 | id |
//! | 69 | [`Delivered`](Event::Delivered) | 8 | id |
//! | 70 | [`Closed`](Event::Closed) | 8 | id; flag bit 0 = reset by peer |
//! | 71 | [`Freed`](Event::Freed) | 8 | id |
//! | 72 | [`End`](HostMessage::End) | 1 | [`EndReason`] u8 |
//!
//! Both decoders reject unknown kinds, wrong body lengths, nonzero padding, unknown flags,
//! booleans other than 0 or 1, an absent address with nonzero bytes, and socket configs that
//! fail [`TcpConfig::check`] or [`UdpConfig::check`] ([`DecodeError`]). For example, the end
//! notice of a finished session is these 9 bytes:
//!
//! ```text
//! 72, 0, 1, 0, 0, 0, 0, 0, 4
//! ```
//!
//! # Rules the daemon enforces
//!
//! The daemon ends a session with [`EndReason::ProtocolError`] when the client:
//!
//! - sends bytes it cannot decode;
//! - sends an operation before `Hello`, or a second `Hello`;
//! - uses a socket slot at or above the `max_sockets` it asked for;
//! - opens a socket on a slot whose socket still exists in the daemon;
//! - sends an operation other than an open with a generation that is not the slot's last opened one;
//! - sends `ShutdownTcp` or `AbortTcp` for a UDP socket;
//! - commits a send record that is malformed, names a slot at or above `max_sockets`, a slot
//!   never opened, a later generation than the slot's last open, or a socket of the other kind.
//!
//! A send record for an earlier generation of a slot is dropped. An operation or send record
//! whose socket the daemon already removed (rejected, or freed after a close) is ignored.

use std::fmt;
use std::io::{self, Write};
use std::net::{Ipv4Addr, SocketAddrV4};
use std::num::NonZeroU16;

/// The 4-byte magic that starts the bodies of `Hello` and `Welcome`: `JNCT` in byte order.
pub const MAGIC: u32 = u32::from_le_bytes(*b"JNCT");
/// The protocol version. Client and daemon must use the same one: the daemon answers any
/// other version with [`WelcomeStatus::BadVersion`].
pub const VERSION: u16 = 3;
/// Length of every message header.
pub const HEADER_LEN: usize = 8;

/// One socket of a session.
///
/// A session has a table of `max_sockets` socket slots. `slot` is the index in that table.
/// `generation` counts the opens of that slot: each open of a free slot gets the previous
/// generation plus one, so an id from an earlier use of the slot differs from the current one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SocketId {
    pub slot: u32,
    pub generation: u32,
}

/// Configuration of a TCP socket in the daemon's network stack.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TcpConfig {
    /// Target receive window advertised to the peer, in bytes; at most 1 GiB.
    pub rx_capacity: u32,
    /// Bytes the daemon's stack queues for sending and keeps until acknowledged; nonzero.
    pub tx_capacity: u32,
    /// Cap on the advertised MSS and on outgoing segment payloads.
    pub mss_cap: Option<NonZeroU16>,
}

/// A local IPv4 address and port.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ListenEndpoint {
    /// One address the daemon owns, or `None` for the daemon's address: for a listener or
    /// UDP socket, every address the daemon owns; for a connect, the source address for the remote.
    pub addr: Option<Ipv4Addr>,
    pub port: u16,
}

/// Configuration of a UDP socket in the daemon's network stack.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UdpConfig {
    /// Local address and port; the port is nonzero.
    pub local: ListenEndpoint,
    /// Received datagrams the daemon's stack holds for this socket before it drops more; nonzero.
    pub rx_queue: u32,
    /// Compute the UDP checksum of sent datagrams in software, or write 0.
    pub tx_checksum: bool,
    /// Allow sending to the broadcast address.
    pub broadcast: bool,
}

/// A message from client to daemon.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientMessage {
    /// The first message on a connection: the protocol version and the size of the session's
    /// socket table.
    Hello { version: u16, max_sockets: u16 },
    Op(Op),
}

/// A socket operation. Opens carry the [`SocketId`] the client chose for a free slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Op {
    /// Connect to `remote` from `local`. The local port is nonzero.
    ConnectTcp {
        id: SocketId,
        config: TcpConfig,
        remote: SocketAddrV4,
        local: ListenEndpoint,
    },
    /// Wait for one connection on `local`. The socket itself becomes that connection.
    ListenTcp {
        id: SocketId,
        config: TcpConfig,
        local: ListenEndpoint,
    },
    BindUdp {
        id: SocketId,
        config: UdpConfig,
    },
    /// Send a FIN after the data already sent.
    ShutdownTcp {
        id: SocketId,
    },
    /// Remove the socket. `unread` says the client dropped received data without reading it;
    /// a TCP connection is then reset.
    Close {
        id: SocketId,
        unread: bool,
    },
    /// Reset the connection and remove the socket.
    AbortTcp {
        id: SocketId,
    },
}

impl Op {
    /// The socket this operation names.
    pub fn id(&self) -> SocketId {
        match *self {
            Self::ConnectTcp { id, .. }
            | Self::ListenTcp { id, .. }
            | Self::BindUdp { id, .. }
            | Self::ShutdownTcp { id }
            | Self::Close { id, .. }
            | Self::AbortTcp { id } => id,
        }
    }
}

/// A message from daemon to client.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HostMessage {
    /// The answer to `Hello`; exactly one per connection, first.
    Welcome(Welcome),
    Event { id: SocketId, event: Event },
    /// The daemon ended the session; the last message on the connection.
    End(EndReason),
}

/// The daemon's answer to `Hello`.
///
/// A rejected `Welcome` still carries the daemon's `version`, `max_record`, `frame_capacity`
/// and `ingress`; its `stream` is 0 and its `rx` is empty.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Welcome {
    /// The daemon's protocol version.
    pub version: u16,
    pub status: WelcomeStatus,
    /// The session's id in the send ring: the daemon's session slot in the high 32 bits, a
    /// number unique for the daemon's lifetime in the low 32 bits.
    pub stream: u64,
    /// The largest send record in bytes, send header included: the IP MTU minus 4.
    pub max_record: u32,
    /// The largest UDP payload the daemon sends in one frame: the IP MTU minus 28.
    pub frame_capacity: u32,
    /// Path of the send-ring file (on macOS, a POSIX shared-memory object name).
    pub ingress: String,
    /// Path of the session's receive-ring file (on macOS, a POSIX shared-memory object name).
    pub rx: String,
}

/// Whether the daemon accepted a `Hello`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WelcomeStatus {
    Ok = 0,
    /// The client's version differs from the daemon's.
    BadVersion = 1,
    /// Every session slot (`--max-sessions`) is taken. A session that has ended keeps its slot
    /// until the daemon has released its rings.
    SessionsFull = 2,
    /// `max_sockets` is 0 or larger than the daemon's `--max-sockets`.
    BadMaxSockets = 3,
    /// The daemon could not create the session's receive ring.
    HostResources = 4,
}

/// What happened to one socket.
///
/// `Connected`, `Bound`, `PeerFin`, `Delivered` and `Closed` leave the socket in place;
/// `Rejected` and `Freed` mean the daemon no longer has it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Event {
    /// A TCP socket is connected: for `ConnectTcp` the handshake finished, for `ListenTcp` a
    /// peer connected to it. `mss` is the segment payload limit agreed with the peer.
    Connected { mss: u16 },
    /// A UDP socket is bound.
    Bound,
    /// The open failed. The socket never existed in the daemon; no close is needed.
    Rejected(RejectCode),
    /// The peer finished sending, and every byte it sent before its FIN is already in the
    /// receive ring. The socket can still send.
    PeerFin,
    /// After `ShutdownTcp`, the peer acknowledged the FIN and all data before it.
    Delivered,
    /// The connection is fully closed, by FINs in both directions or by a reset, and the
    /// daemon holds no unread data for it. The socket still exists until it is closed.
    Closed { reset_by_peer: bool },
    /// The daemon removed the socket after `Close` or `AbortTcp`.
    Freed,
}

/// Why the daemon refused an open.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RejectCode {
    /// The daemon's TCP sockets (`--tcp-sockets`, shared by all sessions) are all in use.
    TcpSocketsFull = 1,
    /// The daemon's stack reported the new socket in a state that cannot connect.
    ConnectInvalidState = 2,
    /// The remote address is unspecified, the remote or local port is 0, the local address
    /// is not one the daemon owns, or the daemon has no source address for the remote.
    ConnectUnaddressable = 3,
    /// A connection with the same local and remote address and port already exists.
    ConnectAddressInUse = 4,
    /// The daemon's stack reported the new socket in a state that cannot listen.
    ListenInvalidState = 5,
    /// The port is 0, or the address is not one the daemon owns.
    ListenUnaddressable = 6,
    /// The daemon's UDP sockets (`--udp-sockets`, shared by all sessions) are all in use.
    UdpSocketsFull = 7,
    /// A UDP socket of any session is already bound to this address (or to every address) and port.
    UdpPortInUse = 8,
    /// The address is not a unicast address the daemon owns.
    UdpUnaddressable = 9,
}

/// Why the daemon ended a session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EndReason {
    /// The session's send stream had a gap the daemon could not fill: a record was committed
    /// with a sequence number ahead of one that never arrived.
    Kicked = 1,
    /// The client broke a rule of the protocol (see the module documentation).
    ProtocolError = 2,
    /// The daemon is stopping.
    HostShutdown = 3,
    /// The daemon took every record the session committed before its end record and
    /// completed the session's close.
    Finished = 4,
}

/// A socket configuration that [`TcpConfig::check`] or [`UdpConfig::check`] rejects.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigError {
    /// `rx_capacity` is above 1 GiB.
    TcpRxCapacityTooLarge,
    TcpTxCapacityZero,
    UdpPortZero,
    UdpRxQueueZero,
}

impl TcpConfig {
    /// Rejects an `rx_capacity` above 1 GiB and a `tx_capacity` of 0.
    pub fn check(&self) -> Result<(), ConfigError> {
        if self.rx_capacity > 1 << 30 {
            return Err(ConfigError::TcpRxCapacityTooLarge);
        }
        if self.tx_capacity == 0 {
            return Err(ConfigError::TcpTxCapacityZero);
        }
        Ok(())
    }
}

impl UdpConfig {
    /// Rejects a local port of 0 and an `rx_queue` of 0.
    pub fn check(&self) -> Result<(), ConfigError> {
        if self.local.port == 0 {
            return Err(ConfigError::UdpPortZero);
        }
        if self.rx_queue == 0 {
            return Err(ConfigError::UdpRxQueueZero);
        }
        Ok(())
    }
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::TcpRxCapacityTooLarge => "TCP receive capacity exceeds 1 GiB",
            Self::TcpTxCapacityZero => "TCP transmit capacity is zero",
            Self::UdpPortZero => "UDP local port is zero",
            Self::UdpRxQueueZero => "UDP receive queue is zero",
        })
    }
}

impl std::error::Error for ConfigError {}

/// A control message that does not follow the protocol.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecodeError {
    /// The kind is not one this side receives.
    UnknownKind(u8),
    /// The body length is not the one the kind requires (`Welcome`: less than 28, or not
    /// matching its two path lengths).
    WrongLength { kind: u8, len: u16 },
    NonzeroPad,
    /// A flag other than bit 0 of `Close` or `Closed` is set.
    UnknownFlags { kind: u8, flags: u8 },
    BadMagic(u32),
    /// A boolean byte other than 0 or 1.
    InvalidBool(u8),
    InvalidWelcomeStatus(u8),
    InvalidRejectCode(u8),
    InvalidEndReason(u8),
    /// A `Welcome` path is not UTF-8.
    InvalidUtf8(std::str::Utf8Error),
    /// A local endpoint without an address has nonzero address bytes.
    NonzeroAbsentAddress,
    /// A socket configuration fails [`TcpConfig::check`] or [`UdpConfig::check`].
    InvalidConfig(ConfigError),
}

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownKind(kind) => write!(f, "unknown message kind {kind}"),
            Self::WrongLength { kind, len } => {
                write!(f, "invalid body length {len} for kind {kind}")
            }
            Self::NonzeroPad => f.write_str("nonzero message padding"),
            Self::UnknownFlags { kind, flags } => {
                write!(f, "unknown flags {flags:#04x} for kind {kind}")
            }
            Self::BadMagic(magic) => write!(f, "invalid control magic {magic:#010x}"),
            Self::InvalidBool(value) => write!(f, "invalid boolean byte {value}"),
            Self::InvalidWelcomeStatus(value) => write!(f, "invalid welcome status {value}"),
            Self::InvalidRejectCode(value) => write!(f, "invalid rejection code {value}"),
            Self::InvalidEndReason(value) => write!(f, "invalid end reason {value}"),
            Self::InvalidUtf8(error) => write!(f, "invalid ingress path UTF-8: {error}"),
            Self::NonzeroAbsentAddress => f.write_str("absent listen address has nonzero bytes"),
            Self::InvalidConfig(error) => write!(f, "invalid config: {error}"),
        }
    }
}

impl std::error::Error for DecodeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::InvalidUtf8(error) => Some(error),
            Self::InvalidConfig(error) => Some(error),
            _ => None,
        }
    }
}

fn put_header(out: &mut Vec<u8>, kind: u8, flags: u8, len: u16) {
    out.reserve(HEADER_LEN + usize::from(len));
    out.extend_from_slice(&[kind, flags]);
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(&[0; 4]);
}

fn put_id(out: &mut Vec<u8>, id: SocketId) {
    out.extend_from_slice(&id.slot.to_le_bytes());
    out.extend_from_slice(&id.generation.to_le_bytes());
}

fn put_tcp(out: &mut Vec<u8>, config: TcpConfig) {
    out.extend_from_slice(&config.rx_capacity.to_le_bytes());
    out.extend_from_slice(&config.tx_capacity.to_le_bytes());
    out.extend_from_slice(&config.mss_cap.map_or(0, NonZeroU16::get).to_le_bytes());
    out.extend_from_slice(&[0; 2]);
}

fn put_listen(out: &mut Vec<u8>, endpoint: ListenEndpoint) {
    out.extend_from_slice(&endpoint.addr.unwrap_or(Ipv4Addr::UNSPECIFIED).octets());
    out.extend_from_slice(&endpoint.port.to_le_bytes());
    out.extend_from_slice(&[u8::from(endpoint.addr.is_some()), 0]);
}

impl ClientMessage {
    /// Appends one complete control message to `out`.
    pub fn encode(&self, out: &mut Vec<u8>) {
        let op = match *self {
            Self::Hello {
                version,
                max_sockets,
            } => {
                put_header(out, 1, 0, 8);
                out.extend_from_slice(&MAGIC.to_le_bytes());
                out.extend_from_slice(&version.to_le_bytes());
                out.extend_from_slice(&max_sockets.to_le_bytes());
                return;
            }
            Self::Op(op) => op,
        };
        let (kind, flags, len) = match op {
            Op::ConnectTcp { .. } => (2, 0, 36),
            Op::ListenTcp { .. } => (3, 0, 28),
            Op::BindUdp { .. } => (4, 0, 24),
            Op::ShutdownTcp { .. } => (5, 0, 8),
            Op::Close { unread, .. } => (6, u8::from(unread), 8),
            Op::AbortTcp { .. } => (7, 0, 8),
        };
        put_header(out, kind, flags, len);
        put_id(out, op.id());
        match op {
            Op::ConnectTcp {
                config,
                remote,
                local,
                ..
            } => {
                put_tcp(out, config);
                out.extend_from_slice(&remote.ip().octets());
                out.extend_from_slice(&remote.port().to_le_bytes());
                out.extend_from_slice(&[0; 2]);
                put_listen(out, local);
            }
            Op::ListenTcp { config, local, .. } => {
                put_tcp(out, config);
                put_listen(out, local);
            }
            Op::BindUdp { config, .. } => {
                put_listen(out, config.local);
                out.extend_from_slice(&config.rx_queue.to_le_bytes());
                out.extend_from_slice(&[u8::from(config.tx_checksum), u8::from(config.broadcast)]);
                out.extend_from_slice(&[0; 2]);
            }
            Op::ShutdownTcp { .. } | Op::Close { .. } | Op::AbortTcp { .. } => {}
        }
    }
}

impl HostMessage {
    /// Appends one complete control message to `out`.
    ///
    /// Panics if the ingress and RX paths plus their 28-byte prefix exceed a `u16` body length.
    pub fn encode(&self, out: &mut Vec<u8>) {
        match self {
            Self::Welcome(welcome) => {
                let len = welcome
                    .ingress
                    .len()
                    .checked_add(welcome.rx.len())
                    .and_then(|len| len.checked_add(28))
                    .and_then(|len| u16::try_from(len).ok())
                    .expect("Welcome paths exceed the control body length");
                put_header(out, 64, 0, len);
                out.extend_from_slice(&MAGIC.to_le_bytes());
                out.extend_from_slice(&welcome.version.to_le_bytes());
                out.extend_from_slice(&[welcome.status as u8, 0]);
                out.extend_from_slice(&welcome.stream.to_le_bytes());
                out.extend_from_slice(&welcome.max_record.to_le_bytes());
                out.extend_from_slice(&welcome.frame_capacity.to_le_bytes());
                out.extend_from_slice(&(welcome.ingress.len() as u16).to_le_bytes());
                out.extend_from_slice(welcome.ingress.as_bytes());
                out.extend_from_slice(&(welcome.rx.len() as u16).to_le_bytes());
                out.extend_from_slice(welcome.rx.as_bytes());
            }
            Self::Event { id, event } => {
                let (kind, flags, len) = match *event {
                    Event::Connected { .. } => (65, 0, 10),
                    Event::Bound => (66, 0, 8),
                    Event::Rejected(_) => (67, 0, 9),
                    Event::PeerFin => (68, 0, 8),
                    Event::Delivered => (69, 0, 8),
                    Event::Closed { reset_by_peer } => (70, u8::from(reset_by_peer), 8),
                    Event::Freed => (71, 0, 8),
                };
                put_header(out, kind, flags, len);
                put_id(out, *id);
                if let Event::Connected { mss } = event {
                    out.extend_from_slice(&mss.to_le_bytes());
                }
                if let Event::Rejected(code) = event {
                    out.push(*code as u8);
                }
            }
            Self::End(reason) => {
                put_header(out, 72, 0, 1);
                out.push(*reason as u8);
            }
        }
    }
}

fn header(buf: &[u8], client: bool) -> Result<Option<(u8, u8, usize)>, DecodeError> {
    if buf.len() < HEADER_LEN {
        return Ok(None);
    }
    let kind = buf[0];
    let flags = buf[1];
    let len = u16::from_le_bytes([buf[2], buf[3]]);
    let expected = match (client, kind) {
        (true, 1 | 5..=7) | (false, 66 | 68..=71) => Some(8),
        (false, 65) => Some(10),
        (true, 2) => Some(36),
        (true, 3) => Some(28),
        (true, 4) => Some(24),
        (false, 64) => None,
        (false, 67) => Some(9),
        (false, 72) => Some(1),
        _ => return Err(DecodeError::UnknownKind(kind)),
    };
    if expected.is_some_and(|expected| len != expected) || (kind == 64 && len < 28) {
        return Err(DecodeError::WrongLength { kind, len });
    }
    if buf[4..8] != [0; 4] {
        return Err(DecodeError::NonzeroPad);
    }
    let allowed_flags = u8::from((client && kind == 6) || (!client && kind == 70));
    if flags & !allowed_flags != 0 {
        return Err(DecodeError::UnknownFlags { kind, flags });
    }
    let total = HEADER_LEN + usize::from(len);
    if buf.len() < total {
        return Ok(None);
    }
    Ok(Some((kind, flags, total)))
}

// Only constructed after the header establishes a complete, correctly sized body.
struct Body<'a>(&'a [u8]);

impl<'a> Body<'a> {
    fn take<const N: usize>(&mut self) -> [u8; N] {
        let (bytes, rest) = self.0.split_at(N);
        self.0 = rest;
        bytes.try_into().unwrap()
    }

    fn byte(&mut self) -> u8 {
        self.take::<1>()[0]
    }

    fn u16(&mut self) -> u16 {
        u16::from_le_bytes(self.take())
    }

    fn u32(&mut self) -> u32 {
        u32::from_le_bytes(self.take())
    }

    fn pad<const N: usize>(&mut self) -> Result<(), DecodeError> {
        if self.take::<N>() != [0; N] {
            return Err(DecodeError::NonzeroPad);
        }
        Ok(())
    }

    fn boolean(&mut self) -> Result<bool, DecodeError> {
        match self.byte() {
            0 => Ok(false),
            1 => Ok(true),
            value => Err(DecodeError::InvalidBool(value)),
        }
    }

    fn magic(&mut self) -> Result<(), DecodeError> {
        let magic = self.u32();
        if magic != MAGIC {
            return Err(DecodeError::BadMagic(magic));
        }
        Ok(())
    }

    fn id(&mut self) -> SocketId {
        SocketId {
            slot: self.u32(),
            generation: self.u32(),
        }
    }

    fn tcp(&mut self) -> Result<TcpConfig, DecodeError> {
        let config = TcpConfig {
            rx_capacity: self.u32(),
            tx_capacity: self.u32(),
            mss_cap: NonZeroU16::new(self.u16()),
        };
        self.pad::<2>()?;
        config.check().map_err(DecodeError::InvalidConfig)?;
        Ok(config)
    }

    fn listen(&mut self) -> Result<ListenEndpoint, DecodeError> {
        let addr = self.take::<4>();
        let port = self.u16();
        let has_addr = self.boolean()?;
        self.pad::<1>()?;
        if !has_addr && addr != [0; 4] {
            return Err(DecodeError::NonzeroAbsentAddress);
        }
        Ok(ListenEndpoint {
            addr: has_addr.then(|| Ipv4Addr::from(addr)),
            port,
        })
    }

    fn udp(&mut self) -> Result<UdpConfig, DecodeError> {
        let config = UdpConfig {
            local: self.listen()?,
            rx_queue: self.u32(),
            tx_checksum: self.boolean()?,
            broadcast: self.boolean()?,
        };
        self.pad::<2>()?;
        config.check().map_err(DecodeError::InvalidConfig)?;
        Ok(config)
    }
}

impl ClientMessage {
    /// Decodes one message, returning `None` if its header or body is incomplete.
    pub fn decode(buf: &[u8]) -> Result<Option<(Self, usize)>, DecodeError> {
        let Some((kind, flags, total)) = header(buf, true)? else {
            return Ok(None);
        };
        let mut body = Body(&buf[HEADER_LEN..total]);
        let message = if kind == 1 {
            body.magic()?;
            Self::Hello {
                version: body.u16(),
                max_sockets: body.u16(),
            }
        } else {
            let id = body.id();
            Self::Op(match kind {
                2 => {
                    let config = body.tcp()?;
                    let addr = Ipv4Addr::from(body.take::<4>());
                    let remote = SocketAddrV4::new(addr, body.u16());
                    body.pad::<2>()?;
                    Op::ConnectTcp {
                        id,
                        config,
                        remote,
                        local: body.listen()?,
                    }
                }
                3 => Op::ListenTcp {
                    id,
                    config: body.tcp()?,
                    local: body.listen()?,
                },
                4 => Op::BindUdp {
                    id,
                    config: body.udp()?,
                },
                5 => Op::ShutdownTcp { id },
                6 => Op::Close {
                    id,
                    unread: flags & 1 != 0,
                },
                7 => Op::AbortTcp { id },
                _ => unreachable!("kind validated by header"),
            })
        };
        Ok(Some((message, total)))
    }
}

impl HostMessage {
    /// Decodes one message, returning `None` if its header or body is incomplete.
    pub fn decode(buf: &[u8]) -> Result<Option<(Self, usize)>, DecodeError> {
        let Some((kind, flags, total)) = header(buf, false)? else {
            return Ok(None);
        };
        let mut body = Body(&buf[HEADER_LEN..total]);
        let message = match kind {
            64 => {
                body.magic()?;
                let version = body.u16();
                let status = match body.byte() {
                    0 => WelcomeStatus::Ok,
                    1 => WelcomeStatus::BadVersion,
                    2 => WelcomeStatus::SessionsFull,
                    3 => WelcomeStatus::BadMaxSockets,
                    4 => WelcomeStatus::HostResources,
                    value => return Err(DecodeError::InvalidWelcomeStatus(value)),
                };
                body.pad::<1>()?;
                let stream = u64::from_le_bytes(body.take());
                let max_record = body.u32();
                let frame_capacity = body.u32();
                let ingress_len = usize::from(body.u16());
                if ingress_len > body.0.len() - 2 {
                    return Err(DecodeError::WrongLength {
                        kind,
                        len: (total - HEADER_LEN) as u16,
                    });
                }
                let (ingress, rest) = body.0.split_at(ingress_len);
                let ingress = std::str::from_utf8(ingress)
                    .map_err(DecodeError::InvalidUtf8)?
                    .to_owned();
                body.0 = rest;
                let rx_len = usize::from(body.u16());
                if body.0.len() != rx_len {
                    return Err(DecodeError::WrongLength {
                        kind,
                        len: (total - HEADER_LEN) as u16,
                    });
                }
                let rx = std::str::from_utf8(body.0)
                    .map_err(DecodeError::InvalidUtf8)?
                    .to_owned();
                Self::Welcome(Welcome {
                    version,
                    status,
                    stream,
                    max_record,
                    frame_capacity,
                    ingress,
                    rx,
                })
            }
            72 => Self::End(match body.byte() {
                1 => EndReason::Kicked,
                2 => EndReason::ProtocolError,
                3 => EndReason::HostShutdown,
                4 => EndReason::Finished,
                value => return Err(DecodeError::InvalidEndReason(value)),
            }),
            _ => {
                let id = body.id();
                let event = match kind {
                    65 => Event::Connected { mss: body.u16() },
                    66 => Event::Bound,
                    67 => Event::Rejected(match body.byte() {
                        1 => RejectCode::TcpSocketsFull,
                        2 => RejectCode::ConnectInvalidState,
                        3 => RejectCode::ConnectUnaddressable,
                        4 => RejectCode::ConnectAddressInUse,
                        5 => RejectCode::ListenInvalidState,
                        6 => RejectCode::ListenUnaddressable,
                        7 => RejectCode::UdpSocketsFull,
                        8 => RejectCode::UdpPortInUse,
                        9 => RejectCode::UdpUnaddressable,
                        value => return Err(DecodeError::InvalidRejectCode(value)),
                    }),
                    68 => Event::PeerFin,
                    69 => Event::Delivered,
                    70 => Event::Closed {
                        reset_by_peer: flags & 1 != 0,
                    },
                    71 => Event::Freed,
                    _ => unreachable!("kind validated by header"),
                };
                Self::Event { id, event }
            }
        };
        Ok(Some((message, total)))
    }
}

/// Writes queued control bytes `output[*written..]` until the nonblocking `socket` would block.
/// When everything is written the buffer is cleared. While bytes remain, the written prefix is
/// dropped once it is at least as long as the rest, so the buffer holds less than twice the
/// unwritten bytes and moving the rest costs O(1) amortized per written byte.
pub(crate) fn write_queued(
    socket: &mut impl Write,
    output: &mut Vec<u8>,
    written: &mut usize,
) -> io::Result<()> {
    while *written < output.len() {
        match socket.write(&output[*written..]) {
            Ok(0) => {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "control socket write returned zero",
                ));
            }
            Ok(n) => *written += n,
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                if *written >= output.len() - *written {
                    output.drain(..*written);
                    *written = 0;
                }
                return Ok(());
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error),
        }
    }
    output.clear();
    *written = 0;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Accepts up to `room` bytes, then reports WouldBlock until `room` is raised again.
    struct Paced {
        room: usize,
        received: Vec<u8>,
    }

    impl Write for Paced {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            if self.room == 0 {
                return Err(io::ErrorKind::WouldBlock.into());
            }
            let n = buf.len().min(self.room);
            self.room -= n;
            self.received.extend_from_slice(&buf[..n]);
            Ok(n)
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn written_prefix_is_reclaimed_while_the_reader_lags() {
        const BACKLOG: usize = 64 * 1024;
        const STEP: usize = 1000;
        let byte = |index: usize| (index % 251) as u8;
        let mut peer = Paced { room: 0, received: Vec::new() };
        let (mut output, mut written) = (Vec::new(), 0);
        output.extend((0..BACKLOG).map(byte));
        let mut queued = BACKLOG;
        // The reader takes as much as is produced each round but never catches up with the
        // backlog, so the buffer never empties: memory must track the unwritten bytes,
        // not everything ever queued.
        for _ in 0..256 {
            output.extend((queued..queued + STEP).map(byte));
            queued += STEP;
            peer.room = STEP;
            write_queued(&mut peer, &mut output, &mut written).unwrap();
            let unwritten = output.len() - written;
            assert!(unwritten > 0);
            assert!(output.len() < 2 * unwritten, "buffer {} for {unwritten} unwritten bytes", output.len());
        }
        peer.room = usize::MAX;
        write_queued(&mut peer, &mut output, &mut written).unwrap();
        assert_eq!((output.len(), written), (0, 0));
        assert!(peer.received.iter().copied().eq((0..queued).map(byte)), "bytes reordered or lost");
    }
}
