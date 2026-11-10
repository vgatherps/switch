use std::collections::VecDeque;
use std::sync::mpsc::Sender;

use dpdk_smoltcp::smoltcp::socket::tcp;
use dpdk_smoltcp::smoltcp::wire::{IpEndpoint, IpListenEndpoint};
use dpdk_smoltcp::{
    AddTcpError, AddUdpError, ReadySide, Stack, TcpFrame, TcpHandle, TcpReadyKey, UdpDatagram,
    UdpHandle, UdpReadyKey,
};
use junction::control::{Event, ListenEndpoint, Op, RejectCode, SocketId, TcpConfig};
use junction::host::{FromNetwork, SessionId};
use junction::rx::{TCP, UDP};
use slice_ringbuffer::{PacketMeta, RingWriter};

pub(crate) enum Socket {
    Tcp {
        handle: TcpHandle,
        key: TcpReadyKey,
        connected: bool,
        fin_seen: bool,
        peer_fin: bool,
        shutdown: bool,
        delivered: bool,
        closed: bool,
    },
    Udp {
        handle: UdpHandle,
        key: UdpReadyKey,
    },
}

pub(crate) enum HeldRx {
    Tcp { frame: TcpFrame, offset: usize },
    Udp(UdpDatagram),
}

pub(crate) struct HostSocket {
    pub session: Option<SessionId>,
    pub id: SocketId,
    pub socket: Socket,
    pub backlog: VecDeque<u8>,
    pub deferred: Option<Op>,
    pub held_rx: Option<HeldRx>,
}

pub(crate) fn event(
    to_control: &Sender<FromNetwork>,
    session: SessionId,
    id: SocketId,
    event: Event,
) {
    let _ = to_control.send(FromNetwork::Event { session, id, event });
}

fn tcp_config(config: TcpConfig) -> tcp::Config {
    tcp::Config {
        rx_capacity: config.rx_capacity as usize,
        tx_capacity: config.tx_capacity as usize,
        mss_cap: config.mss_cap.map(u16::from),
    }
}

fn local(endpoint: ListenEndpoint) -> IpListenEndpoint {
    IpListenEndpoint {
        addr: endpoint.addr.map(Into::into),
        port: endpoint.port,
    }
}

impl Socket {
    pub fn open(stack: &mut Stack, op: Op) -> Result<Self, RejectCode> {
        let handle = match op {
            Op::ConnectTcp {
                config,
                remote,
                local: endpoint,
                ..
            } => {
                let handle = add_tcp(stack, config)?;
                let remote = IpEndpoint::new((*remote.ip()).into(), remote.port());
                if let Err(error) = stack.connect(&handle, remote, local(endpoint)) {
                    stack.remove_tcp(handle);
                    return Err(match error {
                        tcp::ConnectError::InvalidState => RejectCode::ConnectInvalidState,
                        tcp::ConnectError::Unaddressable => RejectCode::ConnectUnaddressable,
                        tcp::ConnectError::AddressInUse => RejectCode::ConnectAddressInUse,
                    });
                }
                handle
            }
            Op::ListenTcp {
                config,
                local: endpoint,
                ..
            } => {
                let handle = add_tcp(stack, config)?;
                if let Err(error) = stack.listen(&handle, local(endpoint)) {
                    stack.remove_tcp(handle);
                    return Err(match error {
                        tcp::ListenError::InvalidState => RejectCode::ListenInvalidState,
                        tcp::ListenError::Unaddressable => RejectCode::ListenUnaddressable,
                    });
                }
                handle
            }
            Op::BindUdp { config, .. } => {
                let config = dpdk_smoltcp::UdpConfig {
                    port: config.local.port,
                    local_addr: config.local.addr,
                    rx_queue: config.rx_queue as usize,
                    tx_checksum: config.tx_checksum,
                    broadcast: config.broadcast,
                };
                return stack
                    .add_udp(config)
                    .map(|handle| {
                        let key = stack.udp_ready_key(&handle);
                        Self::Udp { handle, key }
                    })
                    .map_err(|error| match error {
                        AddUdpError::Full => RejectCode::UdpSocketsFull,
                        AddUdpError::PortInUse => RejectCode::UdpPortInUse,
                        AddUdpError::Unaddressable => RejectCode::UdpUnaddressable,
                        AddUdpError::InvalidConfig(reason) => {
                            panic!("control decoder accepted invalid UDP config: {reason}")
                        }
                    });
            }
            _ => unreachable!("Socket::open requires an open operation"),
        };
        let key = stack.tcp_ready_key(&handle);
        Ok(Self::Tcp {
            handle,
            key,
            connected: false,
            fin_seen: false,
            peer_fin: false,
            shutdown: false,
            delivered: false,
            closed: false,
        })
    }

    pub fn is_tcp(&self) -> bool {
        matches!(self, Self::Tcp { .. })
    }

    pub fn arm(&self, stack: &mut Stack, token: u32) {
        match self {
            Self::Tcp {
                key, closed: false, ..
            } => {
                stack.arm_tcp(*key, ReadySide::Rx, token);
            }
            Self::Udp { key, .. } => stack.arm_udp(*key, token),
            Self::Tcp { .. } => {}
        }
    }

    pub fn disarm(&self, stack: &mut Stack) {
        match self {
            Self::Tcp { key, .. } => {
                stack.disarm_tcp(*key, ReadySide::Rx);
            }
            Self::Udp { key, .. } => {
                stack.disarm_udp(*key);
            }
        }
    }

    pub fn shutdown(&mut self, stack: &mut Stack) {
        let Self::Tcp {
            handle, shutdown, ..
        } = self
        else {
            unreachable!("validated shutdown must name a TCP socket")
        };
        stack.close(handle);
        *shutdown = true;
    }

    pub fn abort(&self, stack: &mut Stack) {
        if let Self::Tcp { handle, .. } = self {
            stack.abort(handle);
        }
    }

    pub fn drop_handle(self) {
        match self {
            Self::Tcp { handle, .. } => drop(handle),
            Self::Udp { handle, .. } => drop(handle),
        }
    }
}

fn add_tcp(stack: &mut Stack, config: TcpConfig) -> Result<TcpHandle, RejectCode> {
    stack
        .add_tcp(tcp_config(config))
        .map_err(|error| match error {
            AddTcpError::SocketsFull(_) => RejectCode::TcpSocketsFull,
        })
}

impl HostSocket {
    pub fn has_unread_rx(&self) -> bool {
        matches!(self.held_rx, Some(HeldRx::Tcp { .. }))
    }

    /// Retains an owned frame and TCP offset while the session RX ring is full.
    pub fn receive(&mut self, stack: &mut Stack, rx: &mut RingWriter, udp_dropped: &mut u64) {
        if let Socket::Tcp { handle, .. } = &self.socket
            && stack.tcp(handle).reset_by_peer()
        {
            self.held_rx = None;
            return;
        }
        loop {
            let mut frame = match self.held_rx.take() {
                Some(frame) => frame,
                None => match &mut self.socket {
                    Socket::Tcp {
                        handle, fin_seen, ..
                    } => match stack.tcp_mut(handle).recv() {
                        Ok(Some(frame)) => HeldRx::Tcp { frame, offset: 0 },
                        Err(tcp::RecvError::Finished) => {
                            *fin_seen = true;
                            break;
                        }
                        Ok(None) | Err(tcp::RecvError::InvalidState) => break,
                    },
                    Socket::Udp { handle, .. } => match stack.udp_mut(handle).recv() {
                        Some(frame) => HeldRx::Udp(frame),
                        None => break,
                    },
                },
            };
            let payload = match &frame {
                HeldRx::Tcp { frame, offset } => {
                    let remaining = &frame.payload()[*offset..];
                    &remaining[..remaining.len().min(rx.max_payload())]
                }
                HeldRx::Udp(frame) => {
                    if frame.payload().len() > rx.max_payload() {
                        *udp_dropped += 1;
                        continue;
                    }
                    frame.payload()
                }
            };
            let meta = match &frame {
                HeldRx::Tcp { frame, .. } => PacketMeta::new(
                    self.id.slot,
                    self.id.generation,
                    TCP,
                    [0; 4],
                    0,
                    [0; 4],
                    frame.rx_timestamp(),
                ),
                HeldRx::Udp(frame) => {
                    let dpdk_smoltcp::smoltcp::wire::IpAddress::Ipv4(src) = frame.src().addr;
                    PacketMeta::new(
                        self.id.slot,
                        self.id.generation,
                        UDP,
                        src.octets(),
                        frame.src().port,
                        frame.dst_addr().octets(),
                        frame.rx_timestamp(),
                    )
                }
            };
            let Some(mut reserved) = rx.reserve(payload.len(), meta) else {
                self.held_rx = Some(frame);
                break;
            };
            let len = payload.len();
            reserved.payload().copy_from_slice(payload);
            reserved.commit();
            if let HeldRx::Tcp { frame: tcp, offset } = &mut frame {
                *offset += len;
                if *offset < tcp.payload().len() {
                    self.held_rx = Some(frame);
                }
            }
        }
    }

    pub fn send_tcp(&mut self, stack: &mut Stack, token: u32, payload: &[u8]) {
        let Socket::Tcp { handle, .. } = &self.socket else {
            unreachable!("validated TCP send must name a TCP socket")
        };
        if self.backlog.is_empty() {
            self.socket.disarm(stack);
            if let Ok(sent) = stack.tcp_mut(handle).send_slice(payload) {
                self.backlog.extend(&payload[sent..]);
            }
            self.socket.arm(stack, token);
        } else {
            self.backlog.extend(payload);
        }
    }

    pub fn drain_backlog(&mut self, stack: &mut Stack, token: u32) {
        let Socket::Tcp { handle, .. } = &self.socket else {
            unreachable!("only TCP sockets have a send backlog")
        };
        self.socket.disarm(stack);
        while !self.backlog.is_empty() {
            let (front, _) = self.backlog.as_slices();
            let len = front.len();
            match stack.tcp_mut(handle).send_slice(front) {
                Ok(sent) => {
                    self.backlog.drain(..sent);
                    if sent < len {
                        break;
                    }
                }
                Err(tcp::SendError::InvalidState) => {
                    self.backlog.clear();
                    break;
                }
            }
        }
        self.socket.arm(stack, token);
    }

    pub fn fired(&mut self, stack: &mut Stack, to_control: &Sender<FromNetwork>) {
        let Some(session) = self.session else {
            return;
        };
        let Socket::Tcp {
            handle,
            connected,
            fin_seen,
            peer_fin,
            shutdown,
            delivered,
            closed,
            ..
        } = &mut self.socket
        else {
            return;
        };
        if *closed {
            return;
        }
        let socket = stack.tcp(handle);
        let state = socket.state();
        let reset_by_peer = socket.reset_by_peer();
        let drained = self.held_rx.is_none() && socket.recv_queue() == 0;
        if reset_by_peer {
            self.held_rx = None;
        }
        let synchronized = matches!(
            state,
            tcp::State::Established
                | tcp::State::FinWait1
                | tcp::State::FinWait2
                | tcp::State::CloseWait
                | tcp::State::Closing
                | tcp::State::LastAck
                | tcp::State::TimeWait
        );
        if !*connected && synchronized {
            *connected = true;
            let mss = socket.max_segment_payload().expect("synchronized TCP socket has an MSS");
            event(to_control, session, self.id, Event::Connected {
                mss: u16::try_from(mss).expect("TCP MSS fits u16"),
            });
        }
        *fin_seen |= matches!(
            state,
            tcp::State::CloseWait
                | tcp::State::LastAck
                | tcp::State::Closing
                | tcp::State::TimeWait
        );
        if !reset_by_peer && !*peer_fin && *fin_seen && drained {
            *peer_fin = true;
            event(to_control, session, self.id, Event::PeerFin);
        }
        if *shutdown
            && !*delivered
            && !reset_by_peer
            && matches!(state, tcp::State::FinWait2 | tcp::State::TimeWait | tcp::State::Closed)
        {
            *delivered = true;
            event(to_control, session, self.id, Event::Delivered);
        }
        if state == tcp::State::Closed && (reset_by_peer || drained) {
            *closed = true;
            event(
                to_control,
                session,
                self.id,
                Event::Closed { reset_by_peer },
            );
        }
    }
}
