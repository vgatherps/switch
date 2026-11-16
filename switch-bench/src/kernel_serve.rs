//! Single-core Linux socket echo receiver; no generator or worker threads.
#![cfg(target_os = "linux")]

use std::io::{self, Read, Write};
use std::net::{Ipv4Addr, TcpListener, TcpStream, UdpSocket};
use std::os::fd::{AsRawFd, RawFd};

use tungstenite::handshake::MidHandshake;
use tungstenite::handshake::server::{NoCallback, ServerHandshake};
use tungstenite::protocol::WebSocketConfig;
use tungstenite::{HandshakeError, Message, WebSocket};

use crate::load::{Proto, TCP_BUFFER};

const BATCH: usize = 32;
const MAX_CONNS: usize = 256;
const DATAGRAM: usize = 65_507;
const WS_LIMIT: usize = TCP_BUFFER;

pub fn run(proto: Proto, bind: Ipv4Addr, port: u16) -> Result<(), String> {
    crate::host::catch_stop()?;
    let mut receiver = Receiver::bind(proto, bind, port).map_err(|e| e.to_string())?;
    eprintln!("switch-bench kernel-serve: {} echo on {bind}:{port}", proto.name());
    while !crate::host::stopped() {
        receiver.tick().map_err(|e| format!("kernel receiver: {e}"))?;
    }
    Ok(())
}

fn blocked(e: &io::Error) -> bool {
    matches!(e.kind(), io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted)
}

fn poll(fds: &mut [libc::pollfd]) -> io::Result<()> {
    // SAFETY: the slice is valid for its advertised length; zero timeout never sleeps.
    if unsafe { libc::poll(fds.as_mut_ptr(), fds.len() as libc::nfds_t, 0) } < 0 {
        let e = io::Error::last_os_error();
        if !blocked(&e) { return Err(e); }
    }
    Ok(())
}

fn descriptor(fd: RawFd, events: i16) -> libc::pollfd {
    libc::pollfd { fd, events, revents: 0 }
}

enum Receiver {
    Udp(UdpEcho),
    Streams(StreamEcho),
}

impl Receiver {
    fn bind(proto: Proto, bind: Ipv4Addr, port: u16) -> io::Result<Self> {
        match proto {
            Proto::Udp => Ok(Self::Udp(UdpEcho::new(UdpSocket::bind((bind, port))?)?)),
            Proto::Tcp | Proto::Ws => {
                let listener = TcpListener::bind((bind, port))?;
                listener.set_nonblocking(true)?;
                Ok(Self::Streams(StreamEcho {
                    listener,
                    ws: proto == Proto::Ws,
                    conns: Vec::with_capacity(MAX_CONNS),
                    fds: Vec::with_capacity(MAX_CONNS + 1),
                }))
            }
        }
    }

    fn tick(&mut self) -> io::Result<()> {
        match self {
            Self::Udp(udp) => udp.tick(),
            Self::Streams(streams) => streams.tick(),
        }
    }
}

struct UdpEcho {
    socket: UdpSocket,
    buffers: Vec<Box<[u8]>>,
    addresses: [libc::sockaddr_storage; BATCH],
    lengths: [usize; BATCH],
    address_lengths: [libc::socklen_t; BATCH],
    next: usize,
    count: usize,
}

impl UdpEcho {
    fn new(socket: UdpSocket) -> io::Result<Self> {
        socket.set_nonblocking(true)?;
        Ok(Self {
            socket,
            buffers: (0..BATCH).map(|_| vec![0; DATAGRAM].into_boxed_slice()).collect(),
            // SAFETY: zero is a valid initial representation of sockaddr_storage.
            addresses: unsafe { std::mem::zeroed() },
            lengths: [0; BATCH],
            address_lengths: [0; BATCH],
            next: 0,
            count: 0,
        })
    }

    fn tick(&mut self) -> io::Result<()> {
        let sending = self.next < self.count;
        let mut fd = [descriptor(self.socket.as_raw_fd(), if sending { libc::POLLOUT } else { libc::POLLIN })];
        poll(&mut fd)?;
        if fd[0].revents == 0 { return Ok(()); }
        // SAFETY: zeroed iovec/mmsghdr contain only integers and nullable pointers.
        let mut vectors: [libc::iovec; BATCH] = unsafe { std::mem::zeroed() };
        let mut messages: [libc::mmsghdr; BATCH] = unsafe { std::mem::zeroed() };
        let start = if sending { self.next } else { 0 };
        let count = if sending { self.count - start } else { BATCH };
        for n in 0..count {
            let i = start + n;
            vectors[n].iov_base = self.buffers[i].as_mut_ptr().cast();
            vectors[n].iov_len = if sending { self.lengths[i] } else { DATAGRAM };
            messages[n].msg_hdr.msg_iov = &mut vectors[n];
            messages[n].msg_hdr.msg_iovlen = 1;
            messages[n].msg_hdr.msg_name = (&mut self.addresses[i] as *mut libc::sockaddr_storage).cast();
            messages[n].msg_hdr.msg_namelen = if sending {
                self.address_lengths[i]
            } else {
                std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t
            };
        }
        // SAFETY: all headers point to live, disjoint buffers/addresses for this syscall.
        let result = unsafe {
            if sending {
                libc::sendmmsg(self.socket.as_raw_fd(), messages.as_mut_ptr(), count as u32, libc::MSG_DONTWAIT | libc::MSG_NOSIGNAL)
            } else {
                libc::recvmmsg(self.socket.as_raw_fd(), messages.as_mut_ptr(), count as u32, libc::MSG_DONTWAIT, std::ptr::null_mut())
            }
        };
        if result < 0 {
            let e = io::Error::last_os_error();
            return if blocked(&e) { Ok(()) } else { Err(e) };
        }
        if sending {
            self.next += result as usize;
        } else {
            self.next = 0;
            self.count = result as usize;
            for (i, message) in messages.iter().enumerate().take(self.count) {
                if message.msg_hdr.msg_flags & libc::MSG_TRUNC != 0 {
                    return Err(io::Error::new(io::ErrorKind::InvalidData, "truncated UDP datagram"));
                }
                self.lengths[i] = message.msg_len as usize;
                self.address_lengths[i] = message.msg_hdr.msg_namelen;
            }
        }
        Ok(())
    }
}

struct StreamEcho {
    listener: TcpListener,
    ws: bool,
    conns: Vec<Connection>,
    fds: Vec<libc::pollfd>,
}

impl StreamEcho {
    fn tick(&mut self) -> io::Result<()> {
        self.fds.clear();
        self.fds.push(descriptor(self.listener.as_raw_fd(), if self.conns.len() < MAX_CONNS { libc::POLLIN } else { 0 }));
        for conn in &self.conns {
            self.fds.push(descriptor(conn.fd(), libc::POLLIN | libc::POLLOUT));
        }
        poll(&mut self.fds)?;
        // Reverse removal leaves the readiness indices of unvisited connections unchanged.
        for i in (0..self.conns.len()).rev() {
            if !self.conns[i].tick(self.fds[i + 1].revents) {
                self.conns.swap_remove(i);
            }
        }
        if self.fds[0].revents & libc::POLLIN != 0 {
            for _ in 0..BATCH {
                if self.conns.len() == MAX_CONNS { break; }
                match self.listener.accept() {
                    Ok((stream, _)) => {
                        stream.set_nonblocking(true)?;
                        stream.set_nodelay(true)?;
                        self.conns.push(if self.ws {
                            Connection::Ws(WsEcho::new(stream))
                        } else {
                            Connection::Tcp(TcpEcho { stream, buffer: vec![0; TCP_BUFFER].into_boxed_slice(), next: 0, end: 0 })
                        });
                    }
                    Err(e) if blocked(&e) => break,
                    Err(e) => return Err(e),
                }
            }
        }
        Ok(())
    }
}

enum Connection { Tcp(TcpEcho), Ws(WsEcho) }
impl Connection {
    fn fd(&self) -> RawFd {
        match self {
            Self::Tcp(tcp) => tcp.stream.as_raw_fd(),
            Self::Ws(ws) => match ws.state.as_ref().expect("live websocket state") {
                WsState::Start(stream) => stream.as_raw_fd(),
                WsState::Handshake(handshake) => handshake.get_ref().get_ref().as_raw_fd(),
                WsState::Open(socket) => socket.get_ref().as_raw_fd(),
            },
        }
    }
    fn tick(&mut self, ready: i16) -> bool {
        match self {
            Self::Tcp(tcp) => tcp.tick(ready),
            Self::Ws(ws) => ws.tick(ready),
        }
    }
}

struct TcpEcho { stream: TcpStream, buffer: Box<[u8]>, next: usize, end: usize }
impl TcpEcho {
    fn tick(&mut self, ready: i16) -> bool {
        if self.next == self.end && ready & (libc::POLLIN | libc::POLLHUP | libc::POLLERR) != 0 {
            match self.stream.read(&mut self.buffer) {
                Ok(0) => return false,
                Ok(n) => { self.next = 0; self.end = n; }
                Err(e) if blocked(&e) => {},
                Err(_) => return false,
            }
        }
        if self.next < self.end && ready & (libc::POLLOUT | libc::POLLHUP | libc::POLLERR) != 0 {
            match self.stream.write(&self.buffer[self.next..self.end]) {
                Ok(0) => return false,
                Ok(n) => self.next += n,
                Err(e) if blocked(&e) => {},
                Err(_) => return false,
            }
        }
        true
    }
}

enum WsState {
    Start(TcpStream),
    Handshake(MidHandshake<ServerHandshake<TcpStream, NoCallback>>),
    Open(WebSocket<TcpStream>),
}
struct WsEcho {
    state: Option<WsState>,
    flushing: bool,
    pending: Option<Message>,
    buffered_read: bool,
}
impl WsEcho {
    fn new(stream: TcpStream) -> Self {
        Self { state: Some(WsState::Start(stream)), flushing: false, pending: None, buffered_read: true }
    }

    fn tick(&mut self, ready: i16) -> bool {
        let state = self.state.take().expect("live websocket state");
        let result = match state {
            WsState::Start(stream) => tungstenite::accept_with_config(stream, Some(
                WebSocketConfig::default()
                    .read_buffer_size(WS_LIMIT)
                    .write_buffer_size(WS_LIMIT)
                    .max_write_buffer_size(2 * WS_LIMIT + 1024)
                    .max_message_size(Some(WS_LIMIT))
                    .max_frame_size(Some(WS_LIMIT)),
            )),
            WsState::Handshake(handshake) => handshake.handshake(),
            WsState::Open(mut socket) => {
                if self.flushing {
                    if ready & (libc::POLLOUT | libc::POLLERR | libc::POLLHUP) == 0 {
                        self.state = Some(WsState::Open(socket));
                        return true;
                    }
                    match socket.flush() {
                        Ok(()) => self.flushing = false,
                        Err(tungstenite::Error::Io(e)) if blocked(&e) => {
                            self.state = Some(WsState::Open(socket));
                            return true;
                        }
                        Err(_) => return false,
                    }
                }
                let readable = ready & (libc::POLLIN | libc::POLLERR | libc::POLLHUP) != 0;
                let mut wrote = false;
                for _ in 0..BATCH {
                    let message = if let Some(message) = self.pending.take() {
                        message
                    } else {
                        if !readable && !self.buffered_read { break; }
                        match socket.read() {
                            Ok(message @ (Message::Binary(_) | Message::Text(_))) => {
                                // A successful read may have prefetched more complete frames.
                                self.buffered_read = true;
                                message
                            }
                            Ok(Message::Ping(_) | Message::Pong(_) | Message::Close(_)) => {
                                self.buffered_read = true;
                                wrote = true;
                                continue;
                            }
                            Ok(Message::Frame(_)) => return false,
                            Err(tungstenite::Error::Io(e)) if blocked(&e) => {
                                self.buffered_read = e.kind() == io::ErrorKind::Interrupted;
                                // read() may have queued an automatic pong/close.
                                wrote = true;
                                break;
                            }
                            Err(_) => return false,
                        }
                    };
                    match socket.write(message) {
                        Ok(()) => wrote = true,
                        // The frame is already retained by tungstenite on I/O failure.
                        Err(tungstenite::Error::Io(e)) if blocked(&e) => {
                            wrote = true;
                            break;
                        }
                        // Unlike WouldBlock, this frame has not been accepted.
                        Err(tungstenite::Error::WriteBufferFull(message)) => {
                            self.pending = Some(*message);
                            wrote = true;
                            break;
                        }
                        Err(_) => return false,
                    }
                }
                if wrote {
                    match socket.flush() {
                        Ok(()) => {},
                        Err(tungstenite::Error::Io(e)) if blocked(&e) => self.flushing = true,
                        Err(_) => return false,
                    }
                }
                self.state = Some(WsState::Open(socket));
                return true;
            }
        };
        match result {
            Ok(socket) => self.state = Some(WsState::Open(socket)),
            Err(HandshakeError::Interrupted(handshake)) => self.state = Some(WsState::Handshake(handshake)),
            Err(HandshakeError::Failure(_)) => return false,
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, atomic::{AtomicBool, Ordering}};
    use std::thread;
    use std::time::Duration;

    struct Server { port: u16, stop: Arc<AtomicBool>, thread: Option<thread::JoinHandle<()>> }
    impl Server {
        fn start(proto: Proto) -> Self {
            let mut receiver = Receiver::bind(proto, Ipv4Addr::LOCALHOST, 0).unwrap();
            let port = match &receiver {
                Receiver::Udp(udp) => udp.socket.local_addr().unwrap().port(),
                Receiver::Streams(streams) => streams.listener.local_addr().unwrap().port(),
            };
            let stop = Arc::new(AtomicBool::new(false));
            let done = stop.clone();
            let thread = thread::spawn(move || {
                while !done.load(Ordering::Relaxed) { receiver.tick().unwrap(); }
            });
            Self { port, stop, thread: Some(thread) }
        }
        fn tcp(&self) -> TcpStream {
            let stream = TcpStream::connect((Ipv4Addr::LOCALHOST, self.port)).unwrap();
            stream.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
            stream.set_write_timeout(Some(Duration::from_secs(5))).unwrap();
            stream
        }
    }
    impl Drop for Server {
        fn drop(&mut self) {
            self.stop.store(true, Ordering::Relaxed);
            self.thread.take().unwrap().join().unwrap();
        }
    }

    #[test]
    fn udp_echoes_batches_sources_and_empty_datagrams() {
        let server = Server::start(Proto::Udp);
        let clients: Vec<_> = (0..2).map(|_| {
            let socket = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
            socket.connect((Ipv4Addr::LOCALHOST, server.port)).unwrap();
            socket.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
            socket
        }).collect();
        for client in &clients {
            for n in 0..BATCH { client.send(&vec![n as u8; n * 8]).unwrap(); }
        }
        for client in &clients {
            let mut received = [0u8; 512];
            for n in 0..BATCH {
                let len = client.recv(&mut received).unwrap();
                assert_eq!(&received[..len], &vec![n as u8; n * 8]);
            }
        }
    }

    #[test]
    fn tcp_echoes_beyond_buffer_and_drains_after_half_close() {
        let server = Server::start(Proto::Tcp);
        let mut stream = server.tcp();
        let expected: Vec<u8> = (0..TCP_BUFFER * 4 + 19).map(|i| i as u8).collect();
        let mut writer = stream.try_clone().unwrap();
        let input = expected.clone();
        let send = thread::spawn(move || {
            for chunk in input.chunks(997) { writer.write_all(chunk).unwrap(); }
            writer.shutdown(std::net::Shutdown::Write).unwrap();
        });
        let mut actual = Vec::new();
        stream.read_to_end(&mut actual).unwrap();
        send.join().unwrap();
        assert_eq!(actual, expected);
    }

    #[test]
    fn websocket_echoes_binary_text_and_pong() {
        let server = Server::start(Proto::Ws);
        let (mut socket, _) = tungstenite::client(
            format!("ws://127.0.0.1:{}/", server.port), server.tcp(),
        ).unwrap();
        for message in [Message::binary(vec![37; WS_LIMIT]), Message::text("kernel echo")] {
            socket.send(message.clone()).unwrap();
            assert_eq!(socket.read().unwrap(), message);
        }
        socket.send(Message::Ping(vec![1, 2, 3].into())).unwrap();
        assert_eq!(socket.read().unwrap(), Message::Pong(vec![1, 2, 3].into()));
        socket.close(None).unwrap();
        assert!(matches!(socket.read().unwrap(), Message::Close(_)));
    }

    #[test]
    fn websocket_drains_burst_larger_than_round_budget() {
        let server = Server::start(Proto::Ws);
        let (mut socket, _) = tungstenite::client(
            format!("ws://127.0.0.1:{}/", server.port), server.tcp(),
        ).unwrap();
        // Queue the entire burst before flushing, then send nothing else: echoes
        // beyond the first round must drain prefetched frames without a new read event.
        socket.set_config(|config| {
            config.write_buffer_size = 256 * 1024;
            config.max_write_buffer_size = 512 * 1024;
        });
        let count = BATCH * 3 + 7;
        for n in 0..count {
            let mut payload = vec![n as u8; 1024];
            payload[..8].copy_from_slice(&(n as u64).to_be_bytes());
            socket.write(Message::binary(payload)).unwrap();
        }
        socket.flush().unwrap();
        for n in 0..count {
            let Message::Binary(payload) = socket.read().unwrap() else {
                panic!("expected binary echo");
            };
            assert_eq!(payload.len(), 1024);
            assert_eq!(&payload[..8], &(n as u64).to_be_bytes());
            assert!(payload[8..].iter().all(|&byte| byte == n as u8));
        }
    }
}
