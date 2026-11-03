//! Little-endian, framed control messages. Decoders consume one message at a time.

use std::fmt;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::num::NonZeroU16;

pub const MAGIC: u32 = u32::from_le_bytes(*b"JNCT");
pub const VERSION: u16 = 2;
pub const HEADER_LEN: usize = 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SocketId {
    pub slot: u32,
    pub generation: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TcpConfig {
    pub rx_capacity: u32,
    pub tx_capacity: u32,
    pub mss_cap: Option<NonZeroU16>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ListenEndpoint {
    pub addr: Option<Ipv4Addr>,
    pub port: u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UdpConfig {
    pub local: ListenEndpoint,
    pub rx_queue: u32,
    pub tx_checksum: bool,
    pub broadcast: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientMessage {
    Hello { version: u16, max_sockets: u16 },
    Op(Op),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Op {
    ConnectTcp {
        id: SocketId,
        config: TcpConfig,
        remote: SocketAddrV4,
        local: ListenEndpoint,
    },
    ListenTcp {
        id: SocketId,
        config: TcpConfig,
        local: ListenEndpoint,
    },
    BindUdp {
        id: SocketId,
        config: UdpConfig,
    },
    ShutdownTcp {
        id: SocketId,
    },
    Close {
        id: SocketId,
        unread: bool,
    },
    AbortTcp {
        id: SocketId,
    },
}

impl Op {
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HostMessage {
    Welcome(Welcome),
    Event { id: SocketId, event: Event },
    End(EndReason),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Welcome {
    pub version: u16,
    pub status: WelcomeStatus,
    pub stream: u64,
    pub max_record: u32,
    pub ingress: String,
    pub rx: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WelcomeStatus {
    Ok = 0,
    BadVersion = 1,
    SessionsFull = 2,
    BadMaxSockets = 3,
    HostResources = 4,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Event {
    Connected,
    Bound,
    Rejected(RejectCode),
    PeerFin,
    Delivered,
    Closed { reset_by_peer: bool },
    Freed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RejectCode {
    TcpSocketsFull = 1,
    ConnectInvalidState = 2,
    ConnectUnaddressable = 3,
    ConnectAddressInUse = 4,
    ListenInvalidState = 5,
    ListenUnaddressable = 6,
    UdpSocketsFull = 7,
    UdpPortInUse = 8,
    UdpUnaddressable = 9,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EndReason {
    Kicked = 1,
    ProtocolError = 2,
    HostShutdown = 3,
    /// The host took all committed records and completed graceful retirement.
    Finished = 4,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigError {
    TcpRxCapacityTooLarge,
    UdpPortZero,
    UdpRxQueueZero,
}

impl TcpConfig {
    pub fn check(&self) -> Result<(), ConfigError> {
        if self.rx_capacity > 1 << 30 {
            return Err(ConfigError::TcpRxCapacityTooLarge);
        }
        Ok(())
    }
}

impl UdpConfig {
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
            Self::UdpPortZero => "UDP local port is zero",
            Self::UdpRxQueueZero => "UDP receive queue is zero",
        })
    }
}

impl std::error::Error for ConfigError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecodeError {
    UnknownKind(u8),
    WrongLength { kind: u8, len: u16 },
    NonzeroPad,
    UnknownFlags { kind: u8, flags: u8 },
    BadMagic(u32),
    InvalidBool(u8),
    InvalidWelcomeStatus(u8),
    InvalidRejectCode(u8),
    InvalidEndReason(u8),
    InvalidUtf8(std::str::Utf8Error),
    NonzeroAbsentAddress,
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
    /// Panics if the ingress and RX paths plus their 24-byte prefix exceed a `u16` body length.
    pub fn encode(&self, out: &mut Vec<u8>) {
        match self {
            Self::Welcome(welcome) => {
                let len = welcome
                    .ingress
                    .len()
                    .checked_add(welcome.rx.len())
                    .and_then(|len| len.checked_add(24))
                    .and_then(|len| u16::try_from(len).ok())
                    .expect("Welcome paths exceed the control body length");
                put_header(out, 64, 0, len);
                out.extend_from_slice(&MAGIC.to_le_bytes());
                out.extend_from_slice(&welcome.version.to_le_bytes());
                out.extend_from_slice(&[welcome.status as u8, 0]);
                out.extend_from_slice(&welcome.stream.to_le_bytes());
                out.extend_from_slice(&welcome.max_record.to_le_bytes());
                out.extend_from_slice(&(welcome.ingress.len() as u16).to_le_bytes());
                out.extend_from_slice(welcome.ingress.as_bytes());
                out.extend_from_slice(&(welcome.rx.len() as u16).to_le_bytes());
                out.extend_from_slice(welcome.rx.as_bytes());
            }
            Self::Event { id, event } => {
                let (kind, flags, len) = match *event {
                    Event::Connected => (65, 0, 8),
                    Event::Bound => (66, 0, 8),
                    Event::Rejected(_) => (67, 0, 9),
                    Event::PeerFin => (68, 0, 8),
                    Event::Delivered => (69, 0, 8),
                    Event::Closed { reset_by_peer } => (70, u8::from(reset_by_peer), 8),
                    Event::Freed => (71, 0, 8),
                };
                put_header(out, kind, flags, len);
                put_id(out, *id);
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
        (true, 1 | 5..=7) | (false, 65 | 66 | 68..=71) => Some(8),
        (true, 2) => Some(36),
        (true, 3) => Some(28),
        (true, 4) => Some(24),
        (false, 64) => None,
        (false, 67) => Some(9),
        (false, 72) => Some(1),
        _ => return Err(DecodeError::UnknownKind(kind)),
    };
    if expected.is_some_and(|expected| len != expected) || (kind == 64 && len < 24) {
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
                    65 => Event::Connected,
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
