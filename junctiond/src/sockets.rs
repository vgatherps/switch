use std::collections::VecDeque;
use std::sync::mpsc::Sender;

use dpdk_smoltcp::smoltcp::socket::tcp;
use dpdk_smoltcp::smoltcp::wire::{IpEndpoint, IpListenEndpoint};
use dpdk_smoltcp::{AddTcpError, AddUdpError, ReadySide, Stack, TcpHandle, TcpReadyKey, UdpHandle};
use junction::control::{Event, ListenEndpoint, Op, RejectCode, SocketId, TcpConfig};
use junction::host::{FromNetwork, SessionId};

pub(crate) enum Socket {
    Tcp {
        handle: TcpHandle,
        key: TcpReadyKey,
        connected: bool,
        peer_fin: bool,
        shutdown: bool,
        delivered: bool,
        closed: bool,
    },
    Udp(UdpHandle),
}

pub(crate) struct HostSocket {
    pub session: Option<SessionId>,
    pub id: SocketId,
    pub socket: Socket,
    pub backlog: VecDeque<u8>,
    pub deferred: Option<Op>,
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
                    .map(Self::Udp)
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
        if let Self::Tcp {
            key, closed: false, ..
        } = self
        {
            stack.arm_tcp(*key, ReadySide::Rx, token);
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
            Self::Udp(handle) => drop(handle),
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
    pub fn send_tcp(&mut self, stack: &mut Stack, payload: &[u8]) {
        let Socket::Tcp { handle, .. } = &self.socket else {
            unreachable!("validated TCP send must name a TCP socket")
        };
        if self.backlog.is_empty() {
            if let Ok(sent) = stack.tcp_mut(handle).send_slice(payload) {
                self.backlog.extend(&payload[sent..]);
            }
        } else {
            self.backlog.extend(payload);
        }
    }

    pub fn drain_backlog(&mut self, stack: &mut Stack) {
        let Socket::Tcp { handle, .. } = &self.socket else {
            unreachable!("only TCP sockets have a send backlog")
        };
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
    }

    pub fn fired(&mut self, stack: &mut Stack, to_control: &Sender<FromNetwork>, token: u32) {
        let Some(session) = self.session else {
            return;
        };
        let Socket::Tcp {
            handle,
            connected,
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
            event(to_control, session, self.id, Event::Connected);
        }
        if !*peer_fin
            && matches!(
                state,
                tcp::State::CloseWait
                    | tcp::State::LastAck
                    | tcp::State::Closing
                    | tcp::State::TimeWait
            )
        {
            *peer_fin = true;
            event(to_control, session, self.id, Event::PeerFin);
        }
        if *shutdown && !*delivered && matches!(state, tcp::State::FinWait2 | tcp::State::TimeWait)
        {
            *delivered = true;
            event(to_control, session, self.id, Event::Delivered);
        }
        if state == tcp::State::Closed {
            *closed = true;
            event(
                to_control,
                session,
                self.id,
                Event::Closed { reset_by_peer },
            );
        }
        self.socket.arm(stack, token);
    }
}
