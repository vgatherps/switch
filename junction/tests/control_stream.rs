use std::cell::RefCell;
use std::io::{self, Read, Write};
use std::net::{Ipv4Addr, SocketAddrV4};
use std::os::unix::net::{UnixListener, UnixStream};
use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use ceph_fifo::Fifo;
use ceph_proto::{Control, Retire, pack, slot, uid};
use ceph_ring::{Config, Reader};
use junction::control::{
    ClientMessage, ConfigError, EndReason, Event, HostMessage, ListenEndpoint, Op, RejectCode,
    SocketId, TcpConfig, UdpConfig, VERSION, Welcome, WelcomeStatus,
};
use junction::host::{ControlChannels, ControlConfig, EndHow, FromNetwork, HostControl, ToNetwork};
use junction::send::{self, SendRecord};
use junction::session::{ConnectError, Lost, OpenError, SendError, Session};
use shared_memory::{Backing, unique_name};
use slice_ringbuffer::RingWriter;

const WAIT: Duration = Duration::from_secs(5);
const QUIET: Duration = Duration::from_millis(30);
const HELLO_TIMEOUT: Duration = Duration::from_secs(2);
static NEXT_PATH: AtomicU64 = AtomicU64::new(0);

fn socket_path() -> PathBuf {
    std::env::temp_dir().join(format!(
        "jnct-{}-{}.sock",
        std::process::id(),
        NEXT_PATH.fetch_add(1, Ordering::Relaxed)
    ))
}

fn until(mut done: impl FnMut() -> bool) {
    let deadline = Instant::now() + WAIT;
    while !done() {
        assert!(
            Instant::now() < deadline,
            "timed out waiting for control behavior"
        );
        thread::yield_now();
    }
}

fn receive<T>(receiver: &Receiver<T>) -> T {
    receiver
        .recv_timeout(WAIT)
        .expect("channel timed out or disconnected")
}

fn rx_ring() -> (Backing, RingWriter) {
    let name = unique_name("jnct-rx-test").unwrap();
    #[cfg(target_os = "linux")]
    let mut backing = Backing::create_file(&Path::new("/dev/shm").join(name)).unwrap();
    #[cfg(target_os = "macos")]
    let mut backing = Backing::create_shm(&name).unwrap();
    let writer = RingWriter::create(&mut backing, 64 * 1024).unwrap();
    (backing, writer)
}

struct Host {
    path: PathBuf,
    reader: Option<Reader>,
    network: Receiver<ToNetwork>,
    events: Option<Sender<FromNetwork>>,
    control: Option<Receiver<Control>>,
    retire: Sender<Retire>,
    done: Receiver<io::Result<()>>,
    thread: Option<JoinHandle<()>>,
    rx: RefCell<Vec<(u64, RingWriter)>>,
}

impl Host {
    fn new(max_sessions: u32, max_sockets: u16) -> Self {
        let reader = Reader::create(Config {
            ring_size: 64 * 1024,
            max_record: 256,
            dir: "/dev/shm".into(),
        })
        .unwrap();
        let path = socket_path();
        let (to_network, network) = mpsc::channel();
        let (events, from_network) = mpsc::channel();
        let (to_fifo, control) = mpsc::channel();
        let (retire, from_fifo) = mpsc::channel();
        let host = HostControl::bind(
            ControlConfig {
                socket_path: path.clone(),
                ingress: reader.name().into(),
                rx_dir: "/dev/shm".into(),
                rx_size: 64 * 1024,
                max_record: 256,
                frame_capacity: 232,
                max_sessions,
                max_sockets,
                poll_ms: 1,
                hello_timeout: HELLO_TIMEOUT,
            },
            ControlChannels {
                to_network,
                to_fifo,
                from_network,
                retire: from_fifo,
            },
        )
        .unwrap();
        let (tx, done) = mpsc::channel();
        let thread = thread::spawn(move || {
            let _ = tx.send(host.run());
        });
        Self {
            path,
            reader: Some(reader),
            network,
            events: Some(events),
            control: Some(control),
            retire,
            done,
            thread: Some(thread),
            rx: RefCell::new(Vec::new()),
        }
    }

    fn open(&self, max_sockets: u16) -> (Session, u64) {
        let session = Session::connect(&self.path, max_sockets).unwrap();
        assert_eq!(session.frame_capacity(), 232);
        assert_eq!(session.max_record(), 256);
        assert_eq!(session.max_sockets(), usize::from(max_sockets));
        // Welcome must not be visible until both Open messages have been sent.
        let (stream, actual) =
            self.opened(self.network.try_recv().expect("Open must precede Welcome"));
        assert_eq!(actual, max_sockets);
        assert_eq!(
            self.control.as_ref().unwrap().try_recv().unwrap(),
            Control::Open {
                slot: slot(stream),
                uid: uid(stream)
            }
        );
        (session, stream)
    }

    fn opened(&self, message: ToNetwork) -> (u64, u16) {
        let ToNetwork::Open {
            session,
            max_sockets,
            rx,
        } = message
        else {
            panic!("expected Open");
        };
        self.rx.borrow_mut().push((session, rx));
        (session, max_sockets)
    }

    fn take_rx(&self, session: u64) -> RingWriter {
        let mut writers = self.rx.borrow_mut();
        let index = writers.iter().position(|(id, _)| *id == session).unwrap();
        writers.swap_remove(index).1
    }

    fn retired(&self, session: u64) {
        self.send(FromNetwork::Retired {
            session,
            rx: self.take_rx(session),
        });
    }

    fn event(&self, stream: u64, id: SocketId, event: Event) {
        self.send(FromNetwork::Event {
            session: stream,
            id,
            event,
        });
    }

    fn send(&self, message: FromNetwork) {
        self.events.as_ref().unwrap().send(message).unwrap();
    }

    fn op(&self, stream: u64) -> Op {
        match receive(&self.network) {
            ToNetwork::Op { session, op } => {
                assert_eq!(session, stream);
                op
            }
            _ => panic!("expected Op"),
        }
    }

    fn end(&self, stream: u64, expected: EndHow) {
        match receive(&self.network) {
            ToNetwork::End { session, how } => {
                assert_eq!(session, stream);
                assert_eq!(how, expected);
            }
            _ => panic!("expected End"),
        }
        if expected != EndHow::Graceful {
            self.send(FromNetwork::Released {
                session: stream,
                rx: self.take_rx(stream),
            });
        }
    }

    fn closed(&self, stream: u64) {
        assert_eq!(
            receive(self.control.as_ref().unwrap()),
            Control::Close {
                slot: slot(stream),
                uid: uid(stream)
            }
        );
    }

    fn stop(&mut self) {
        self.events.take();
        if let Some(thread) = self.thread.take() {
            receive(&self.done).unwrap();
            thread.join().unwrap();
        }
    }
}

impl Drop for Host {
    fn drop(&mut self) {
        self.stop();
        let _ = std::fs::remove_file(&self.path);
    }
}

fn tcp() -> TcpConfig {
    TcpConfig {
        rx_capacity: 1024,
        tx_capacity: 1024,
        mss_cap: None,
    }
}

fn local() -> ListenEndpoint {
    ListenEndpoint {
        addr: None,
        port: 1234,
    }
}

fn udp() -> UdpConfig {
    UdpConfig {
        local: local(),
        rx_queue: 8,
        tx_checksum: true,
        broadcast: false,
    }
}

fn events(session: &mut Session, count: usize) -> Vec<(SocketId, Event)> {
    let mut result = Vec::new();
    until(|| {
        session
            .poll_events(|id, event| result.push((id, event)))
            .unwrap();
        result.len() >= count
    });
    assert_eq!(result.len(), count);
    result
}

fn ended(session: &mut Session, reason: EndReason) {
    until(|| match session.poll_events(|_, _| {}) {
        Err(Lost::Ended(actual)) => {
            assert_eq!(actual, reason);
            true
        }
        Ok(()) => false,
        other => panic!("expected End({reason:?}), got {other:?}"),
    });
}

fn raw(path: &Path) -> UnixStream {
    let stream = UnixStream::connect(path).unwrap();
    stream.set_read_timeout(Some(WAIT)).unwrap();
    stream.set_write_timeout(Some(WAIT)).unwrap();
    stream
}

fn write_message(stream: &mut UnixStream, message: ClientMessage) {
    let mut bytes = Vec::new();
    message.encode(&mut bytes);
    stream.write_all(&bytes).unwrap();
}

fn read_message(stream: &mut UnixStream) -> HostMessage {
    let mut header = [0; 8];
    stream.read_exact(&mut header).unwrap();
    let length = u16::from_le_bytes([header[2], header[3]]) as usize;
    let mut bytes = header.to_vec();
    bytes.resize(8 + length, 0);
    stream.read_exact(&mut bytes[8..]).unwrap();
    HostMessage::decode(&bytes).unwrap().unwrap().0
}

fn hello(host: &Host, max_sockets: u16) -> (UnixStream, u64) {
    let mut stream = raw(&host.path);
    write_message(
        &mut stream,
        ClientMessage::Hello {
            version: VERSION,
            max_sockets,
        },
    );
    let HostMessage::Welcome(welcome) = read_message(&mut stream) else {
        panic!("expected Welcome")
    };
    assert_eq!(welcome.status, WelcomeStatus::Ok);
    assert_eq!(welcome.max_record, 256);
    assert_eq!(welcome.frame_capacity, 232);
    assert_eq!(welcome.ingress, host.reader.as_ref().unwrap().name());
    let (session, actual) = host.opened(host.network.try_recv().unwrap());
    assert_eq!(session, welcome.stream);
    assert_eq!(actual, max_sockets);
    (stream, welcome.stream)
}

fn handshake(host: &Host, max_sockets: u16) -> (UnixStream, u64) {
    let (stream, session) = hello(host, max_sockets);
    assert_eq!(
        host.control.as_ref().unwrap().try_recv().unwrap(),
        Control::Open {
            slot: slot(session),
            uid: uid(session)
        }
    );
    (stream, session)
}

#[test]
fn clients_have_distinct_streams_and_ordered_ops() {
    let host = Host::new(4, 8);
    let (mut a, sa) = host.open(4);
    let (mut b, sb) = host.open(4);
    assert_ne!(sa, sb);
    let remote = SocketAddrV4::new(Ipv4Addr::LOCALHOST, 4321);
    let a_tcp = a.connect_tcp(tcp(), remote, local()).unwrap();
    let b_udp = b.bind_udp(udp()).unwrap();
    a.shutdown(a_tcp);
    b.close(b_udp, true);
    a.abort(a_tcp);
    let mut a_ops = Vec::new();
    let mut b_ops = Vec::new();
    for _ in 0..5 {
        match receive(&host.network) {
            ToNetwork::Op { session, op } if session == sa => a_ops.push(op),
            ToNetwork::Op { session, op } if session == sb => b_ops.push(op),
            _ => panic!("unexpected network message"),
        }
    }
    assert_eq!(
        a_ops,
        [
            Op::ConnectTcp {
                id: a_tcp,
                config: tcp(),
                remote,
                local: local()
            },
            Op::ShutdownTcp { id: a_tcp },
            Op::AbortTcp { id: a_tcp }
        ]
    );
    assert_eq!(
        b_ops,
        [
            Op::BindUdp {
                id: b_udp,
                config: udp()
            },
            Op::Close {
                id: b_udp,
                unread: true
            }
        ]
    );
}

#[test]
fn events_are_isolated_ordered_and_release_slots() {
    let host = Host::new(2, 2);
    let (mut a, sa) = host.open(1);
    let (mut b, sb) = host.open(1);
    let first = a.listen_tcp(tcp(), local()).unwrap();
    let other = b.bind_udp(udp()).unwrap();
    for _ in 0..2 {
        receive(&host.network);
    }
    host.event(sa, first, Event::Connected { mss: 1460 });
    host.event(sa, first, Event::PeerFin);
    assert_eq!(
        events(&mut a, 2),
        [(first, Event::Connected { mss: 1460 }), (first, Event::PeerFin)]
    );
    b.poll_events(|_, _| panic!("event leaked across sessions"))
        .unwrap();
    host.event(sb, other, Event::Bound);
    assert_eq!(events(&mut b, 1), [(other, Event::Bound)]);
    host.event(sa, first, Event::Rejected(RejectCode::TcpSocketsFull));
    events(&mut a, 1);
    let second = a.listen_tcp(tcp(), local()).unwrap();
    assert_eq!(
        second,
        SocketId {
            slot: first.slot,
            generation: first.generation + 1
        }
    );
    host.op(sa);
    a.close(second, false);
    assert_eq!(
        host.op(sa),
        Op::Close {
            id: second,
            unread: false
        }
    );
    host.event(sa, second, Event::Freed);
    assert_eq!(events(&mut a, 1), [(second, Event::Freed)]);
    let third = a.listen_tcp(tcp(), local()).unwrap();
    assert_eq!(
        third,
        SocketId {
            slot: first.slot,
            generation: second.generation + 1
        }
    );
}

#[test]
fn killed_client_releases_session_without_affecting_survivor() {
    const CHILD: &str = "JUNCTION_KILLED_CHILD";
    if let Ok(path) = std::env::var(CHILD) {
        let mut session = Session::connect(Path::new(&path), 1).unwrap();
        session.bind_udp(udp()).unwrap();
        until(|| {
            session.poll_events(|_, _| {}).unwrap();
            false
        });
        unreachable!();
    }
    let host = Host::new(2, 2);
    let (mut survivor, survivor_id) = host.open(1);
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args([
            "killed_client_releases_session_without_affecting_survivor",
            "--exact",
            "--nocapture",
        ])
        .env(CHILD, &host.path)
        .stdout(Stdio::null())
        .spawn()
        .unwrap();
    let (killed, _) = host.opened(receive(&host.network));
    assert_eq!(
        receive(host.control.as_ref().unwrap()),
        Control::Open {
            slot: slot(killed),
            uid: uid(killed)
        }
    );
    assert!(matches!(host.op(killed), Op::BindUdp { .. }));
    child.kill().unwrap();
    let mut status = None;
    until(|| {
        status = child.try_wait().unwrap();
        status.is_some()
    });
    assert_eq!(status.unwrap().signal(), Some(libc::SIGKILL));
    host.end(killed, EndHow::Lost);
    host.closed(killed);
    let id = survivor.bind_udp(udp()).unwrap();
    assert_eq!(host.op(survivor_id), Op::BindUdp { id, config: udp() });
    host.event(survivor_id, id, Event::Bound);
    assert_eq!(events(&mut survivor, 1), [(id, Event::Bound)]);
    let (_replacement, replacement) = host.open(1);
    assert_eq!(slot(replacement), slot(killed));
    assert_ne!(uid(replacement), uid(killed));
}

#[test]
fn malformed_messages_and_out_of_range_ops_end_only_the_offender() {
    let host = Host::new(3, 2);
    let (mut survivor, survivor_id) = host.open(1);
    for bad in 0..3 {
        let (mut stream, session) = handshake(&host, 1);
        match bad {
            0 => stream.write_all(&[255, 0, 0, 0, 0, 0, 0, 0]).unwrap(),
            1 => write_message(
                &mut stream,
                ClientMessage::Op(Op::ShutdownTcp {
                    id: SocketId {
                        slot: 1,
                        generation: 1,
                    },
                }),
            ),
            _ => write_message(
                &mut stream,
                ClientMessage::Hello {
                    version: VERSION,
                    max_sockets: 1,
                },
            ),
        }
        assert_eq!(
            read_message(&mut stream),
            HostMessage::End(EndReason::ProtocolError)
        );
        assert_eq!(stream.read(&mut [0]).unwrap(), 0);
        host.end(session, EndHow::Abort);
        host.closed(session);
    }
    let id = survivor.bind_udp(udp()).unwrap();
    host.op(survivor_id);
    host.event(survivor_id, id, Event::Bound);
    events(&mut survivor, 1);
}

#[test]
fn network_protocol_error_uses_abort_teardown() {
    let host = Host::new(2, 2);
    let (mut client, session) = host.open(1);
    host.send(FromNetwork::ProtocolError { session });
    ended(&mut client, EndReason::ProtocolError);
    host.end(session, EndHow::Abort);
    host.closed(session);
}

#[test]
fn kick_uses_abort_teardown() {
    let host = Host::new(2, 2);
    let (mut client, session) = host.open(1);
    host.retire
        .send(Retire::Kicked {
            slot: slot(session),
            uid: uid(session),
        })
        .unwrap();
    ended(&mut client, EndReason::Kicked);
    host.end(session, EndHow::Abort);
    host.closed(session);
}

#[test]
fn finish_waits_for_fifo_and_network_retirement() {
    let mut host = Host::new(2, 2);
    let mut client = Session::connect(&host.path, 1).unwrap();
    let (session, _) = host.opened(host.network.try_recv().unwrap());
    let mut fifo = Fifo::new(
        host.reader.take().unwrap(),
        2,
        host.control.take().unwrap(),
        host.retire.clone(),
    );
    client.finish().unwrap();
    client.finish().unwrap();
    assert!(!client.closed());
    until(|| {
        if let Some(message) = fifo.next_message() {
            assert_eq!(message.stream(), session);
            assert_eq!(message.seq(), 0);
            assert!(message.end());
            assert!(message.payload().is_empty());
            // Retire::Finished is sent only when this real FIFO Message is dropped.
            assert!(host.network.try_recv().is_err());
            drop(message);
            true
        } else {
            false
        }
    });
    host.end(session, EndHow::Graceful);
    client.poll_events(|_, _| {}).unwrap();
    assert!(!client.closed());
    host.retired(session);
    until(|| {
        client.poll_events(|_, _| {}).unwrap();
        client.closed()
    });
    client
        .poll_events(|_, _| panic!("closed session emitted an event"))
        .unwrap();
    assert!(
        fifo.next_message().is_none(),
        "finish must not enqueue twice"
    );
}

#[test]
fn full_send_preserves_sequence_and_round_trips_tcp_and_udp() {
    let mut host = Host::new(2, 2);
    let mut client = Session::connect(&host.path, 2).unwrap();
    let (session, _) = host.opened(host.network.try_recv().unwrap());
    let mut fifo = Fifo::new(
        host.reader.take().unwrap(),
        2,
        host.control.take().unwrap(),
        host.retire.clone(),
    );
    let tcp_id = client.listen_tcp(tcp(), local()).unwrap();
    assert!(matches!(host.op(session), Op::ListenTcp { .. }));
    host.event(session, tcp_id, Event::Connected { mss: 1460 });
    until(|| {
        let mut connected = false;
        client
            .poll_events(|id, event| {
                assert_eq!(id, tcp_id);
                assert_eq!(event, Event::Connected { mss: 1460 });
                connected = true;
            })
            .unwrap();
        connected
    });

    let mut payload = [0xa5; 240];
    let mut sent = 0u64;
    loop {
        payload[..8].copy_from_slice(&sent.to_le_bytes());
        match client.send_tcp(tcp_id, &payload) {
            Ok(()) => sent += 1,
            Err(SendError::Full) => break,
            other => panic!("unexpected send result: {other:?}"),
        }
    }
    assert!(sent > 0);
    let mut received = 0u64;
    until(|| {
        if let Some(message) = fifo.next_message() {
            assert_eq!(message.stream(), session);
            assert_eq!(message.seq(), received);
            assert!(!message.end());
            let SendRecord::Tcp { id, payload } = send::decode(message.payload()).unwrap() else {
                panic!("expected TCP record");
            };
            assert_eq!(id, tcp_id);
            assert_eq!(&payload[..8], &received.to_le_bytes());
            assert!(payload[8..].iter().all(|byte| *byte == 0xa5));
            received += 1;
        }
        received == sent
    });
    assert!(fifo.next_message().is_none());

    // A rejected push must not leave a sequence gap: the FIFO would kick the stream.
    client.send_tcp(tcp_id, &[]).unwrap();
    let udp_id = client.bind_udp(udp()).unwrap();
    assert!(matches!(host.op(session), Op::BindUdp { .. }));
    host.event(session, udp_id, Event::Bound);
    until(|| {
        let mut bound = false;
        client
            .poll_events(|id, event| {
                assert_eq!(id, udp_id);
                assert_eq!(event, Event::Bound);
                bound = true;
            })
            .unwrap();
        bound
    });
    let to = SocketAddrV4::new(Ipv4Addr::new(192, 0, 2, 9), 0x1234);
    client.send_udp(udp_id, to, b"udp").unwrap();
    client.finish().unwrap();
    let mut tail = 0;
    until(|| {
        if let Some(message) = fifo.next_message() {
            assert_eq!(message.stream(), session);
            assert_eq!(message.seq(), sent + tail);
            match tail {
                0 => assert_eq!(
                    send::decode(message.payload()),
                    Some(SendRecord::Tcp {
                        id: tcp_id,
                        payload: b"",
                    })
                ),
                1 => assert_eq!(
                    send::decode(message.payload()),
                    Some(SendRecord::Udp {
                        id: udp_id,
                        to,
                        payload: b"udp",
                    })
                ),
                2 => assert!(message.end(), "expected clean stream end"),
                _ => panic!("unexpected extra record"),
            }
            tail += 1;
        }
        // A kicked stream is reported by the control thread rather than timing out.
        client.poll_events(|_, _| {}).unwrap();
        tail == 3
    });
    host.end(session, EndHow::Graceful);
    host.retired(session);
    until(|| {
        client.poll_events(|_, _| {}).unwrap();
        client.closed()
    });
}

#[test]
fn tcp_send_after_shutdown_is_not_open() {
    let host = Host::new(1, 1);
    let (mut client, session) = host.open(1);
    let id = client.listen_tcp(tcp(), local()).unwrap();
    assert!(matches!(host.op(session), Op::ListenTcp { .. }));
    host.event(session, id, Event::Connected { mss: 1460 });
    until(|| {
        let mut connected = false;
        client
            .poll_events(|_, event| connected |= event == Event::Connected { mss: 1460 })
            .unwrap();
        connected
    });
    client.send_tcp(id, &[0]).unwrap();
    client.shutdown(id);
    assert_eq!(client.send_tcp(id, &[0]), Err(SendError::NotOpen));
}

#[test]
fn welcomes_reject_invalid_version_limits_and_full_host() {
    let host = Host::new(1, 2);
    let mut stream = raw(&host.path);
    write_message(
        &mut stream,
        ClientMessage::Hello {
            version: VERSION + 1,
            max_sockets: 1,
        },
    );
    let HostMessage::Welcome(welcome) = read_message(&mut stream) else {
        panic!("expected Welcome")
    };
    assert_eq!(welcome.status, WelcomeStatus::BadVersion);
    assert_eq!(stream.read(&mut [0]).unwrap(), 0);
    for max in [0, 3] {
        assert!(matches!(
            Session::connect(&host.path, max),
            Err(ConnectError::Rejected(WelcomeStatus::BadMaxSockets))
        ));
    }
    let (_client, _) = handshake(&host, 2);
    assert!(matches!(
        Session::connect(&host.path, 1),
        Err(ConnectError::Rejected(WelcomeStatus::SessionsFull))
    ));
    assert!(host.network.try_recv().is_err());
    assert!(host.control.as_ref().unwrap().try_recv().is_err());
}

#[test]
fn shutdown_notifies_clients_and_returns_ok() {
    let mut host = Host::new(2, 2);
    let (mut a, _) = host.open(1);
    let (mut b, _) = host.open(1);
    host.stop();
    ended(&mut a, EndReason::HostShutdown);
    ended(&mut b, EndReason::HostShutdown);
}

#[test]
fn slot_rules_validate_config_and_handle_rejection_racing_close() {
    let host = Host::new(1, 2);
    let (mut client, session) = host.open(1);
    assert!(matches!(
        client.listen_tcp(
            TcpConfig {
                rx_capacity: (1 << 30) + 1,
                ..tcp()
            },
            local()
        ),
        Err(OpenError::Config(ConfigError::TcpRxCapacityTooLarge))
    ));
    assert!(matches!(
        client.bind_udp(UdpConfig {
            rx_queue: 0,
            ..udp()
        }),
        Err(OpenError::Config(ConfigError::UdpRxQueueZero))
    ));
    assert!(matches!(
        client.bind_udp(UdpConfig {
            local: ListenEndpoint { port: 0, ..local() },
            ..udp()
        }),
        Err(OpenError::Config(ConfigError::UdpPortZero))
    ));
    assert!(host.network.recv_timeout(QUIET).is_err());
    let first = client.bind_udp(udp()).unwrap();
    host.op(session);
    assert!(matches!(
        client.listen_tcp(tcp(), local()),
        Err(OpenError::NoFreeSlot)
    ));
    // Validation precedes allocation even when the only slot is occupied.
    assert!(matches!(
        client.bind_udp(UdpConfig {
            rx_queue: 0,
            ..udp()
        }),
        Err(OpenError::Config(_))
    ));
    host.event(session, first, Event::Rejected(RejectCode::UdpPortInUse));
    events(&mut client, 1);
    client.close(first, false);
    assert!(host.network.recv_timeout(QUIET).is_err());
    let second = client.bind_udp(udp()).unwrap();
    assert_eq!(
        second,
        SocketId {
            slot: first.slot,
            generation: first.generation + 1
        }
    );
    host.op(session);
    client.close(second, false);
    assert_eq!(
        host.op(session),
        Op::Close {
            id: second,
            unread: false
        }
    );
    host.event(session, second, Event::Rejected(RejectCode::UdpPortInUse));
    events(&mut client, 1);
    let third = client.bind_udp(udp()).unwrap();
    assert_eq!(
        third,
        SocketId {
            slot: first.slot,
            generation: second.generation + 1
        }
    );
}

#[test]
fn invalid_host_event_ids_report_invalid_data_without_panicking() {
    let host = Host::new(2, 2);
    for outside in [false, true] {
        let (mut client, session) = host.open(1);
        let current = client.bind_udp(udp()).unwrap();
        host.op(session);
        let bad = SocketId {
            slot: if outside { 1 } else { current.slot },
            generation: current.generation + 1,
        };
        host.event(session, bad, Event::Bound);
        until(
            || match client.poll_events(|_, _| panic!("invalid event reached callback")) {
                Err(Lost::Io(error)) => {
                    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
                    true
                }
                Ok(()) => false,
                other => panic!("unexpected result {other:?}"),
            },
        );
        drop(client);
        host.end(session, EndHow::Lost);
        host.closed(session);
    }
}

#[test]
fn stale_and_unknown_channel_messages_do_not_end_reused_session() {
    let host = Host::new(1, 1);
    let (client, old) = host.open(1);
    drop(client);
    host.end(old, EndHow::Lost);
    host.closed(old);
    let (mut client, current) = host.open(1);
    let id = client.bind_udp(udp()).unwrap();
    host.op(current);
    for stale in [old, pack(99, 99)] {
        host.send(FromNetwork::Event {
            session: stale,
            id,
            event: Event::Bound,
        });
        host.send(FromNetwork::ProtocolError { session: stale });
        let (_backing, rx) = rx_ring();
        host.send(FromNetwork::Retired { session: stale, rx });
        host.retire
            .send(Retire::Kicked {
                slot: slot(stale),
                uid: uid(stale),
            })
            .unwrap();
        host.retire
            .send(Retire::Finished {
                slot: slot(stale),
                uid: uid(stale),
            })
            .unwrap();
    }
    host.event(current, id, Event::Bound);
    assert_eq!(events(&mut client, 1), [(id, Event::Bound)]);
    assert!(host.network.recv_timeout(QUIET).is_err());
}

#[test]
fn client_reports_host_version_mismatch_before_attaching() {
    let path = socket_path();
    let listener = UnixListener::bind(&path).unwrap();
    let worker = thread::spawn(move || {
        let (mut socket, _) = listener.accept().unwrap();
        socket.set_read_timeout(Some(WAIT)).unwrap();
        let mut hello = [0; 16];
        socket.read_exact(&mut hello).unwrap();
        let mut bytes = Vec::new();
        HostMessage::Welcome(Welcome {
            version: VERSION + 1,
            status: WelcomeStatus::BadVersion,
            stream: 0,
            max_record: 0,
            frame_capacity: 232,
            ingress: String::new(),
            rx: String::new(),
        })
        .encode(&mut bytes);
        socket.write_all(&bytes).unwrap();
    });
    assert!(
        matches!(Session::connect(&path, 1), Err(ConnectError::Version { host }) if host == VERSION + 1)
    );
    worker.join().unwrap();
    std::fs::remove_file(path).unwrap();
}

#[test]
fn fragmented_and_batched_ops_preserve_message_order() {
    let host = Host::new(1, 2);
    let (mut socket, session) = handshake(&host, 2);
    let id = SocketId {
        slot: 0,
        generation: 1,
    };
    let mut bytes = Vec::new();
    ClientMessage::Op(Op::BindUdp { id, config: udp() }).encode(&mut bytes);
    socket.write_all(&bytes[..7]).unwrap();
    assert!(host.network.recv_timeout(QUIET).is_err());
    ClientMessage::Op(Op::Close { id, unread: false }).encode(&mut bytes);
    socket.write_all(&bytes[7..]).unwrap();
    assert_eq!(host.op(session), Op::BindUdp { id, config: udp() });
    assert_eq!(host.op(session), Op::Close { id, unread: false });
}

#[test]
fn draining_session_ignores_ops_and_hup_until_retired() {
    let host = Host::new(1, 1);
    let (mut socket, session) = handshake(&host, 1);
    host.retire
        .send(Retire::Finished {
            slot: slot(session),
            uid: uid(session),
        })
        .unwrap();
    host.end(session, EndHow::Graceful);
    write_message(
        &mut socket,
        ClientMessage::Op(Op::ShutdownTcp {
            id: SocketId {
                slot: 0,
                generation: 1,
            },
        }),
    );
    socket.shutdown(std::net::Shutdown::Both).unwrap();
    assert!(host.network.recv_timeout(QUIET).is_err());
    assert!(host.control.as_ref().unwrap().try_recv().is_err());
    host.retired(session);
    host.closed(session);
    assert!(host.network.recv_timeout(QUIET).is_err());
}

#[test]
fn disconnecting_retire_or_fifo_channel_shuts_down_live_sessions() {
    for fifo in [false, true] {
        let mut host = Host::new(2, 1);
        let (mut client, _) = host.open(1);
        if fifo {
            drop(host.control.take());
            // Opening a second client forces a send to the disconnected FIFO channel.
            let mut pending = raw(&host.path);
            write_message(
                &mut pending,
                ClientMessage::Hello {
                    version: VERSION,
                    max_sockets: 1,
                },
            );
            assert_eq!(
                read_message(&mut pending),
                HostMessage::End(EndReason::HostShutdown)
            );
        } else {
            let (replacement, _rx) = mpsc::channel();
            drop(std::mem::replace(&mut host.retire, replacement));
        }
        receive(&host.done).unwrap();
        host.thread.take().unwrap().join().unwrap();
        ended(&mut client, EndReason::HostShutdown);
    }
}

#[test]
fn client_reports_second_welcome_malformed_bytes_and_unfinished_eof() {
    let reader = Reader::create(Config {
        ring_size: 64 * 1024,
        max_record: 256,
        dir: "/dev/shm".into(),
    })
    .unwrap();
    for case in 0..3 {
        let path = socket_path();
        let listener = UnixListener::bind(&path).unwrap();
        let ingress = reader.name().to_owned();
        let (rx_backing, _rx_writer) = rx_ring();
        let rx = rx_backing.name().to_owned();
        let server = thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();
            socket.set_read_timeout(Some(WAIT)).unwrap();
            socket.set_write_timeout(Some(WAIT)).unwrap();
            let mut hello = [0; 16];
            socket.read_exact(&mut hello).unwrap();
            let welcome = HostMessage::Welcome(Welcome {
                version: VERSION,
                status: WelcomeStatus::Ok,
                stream: pack(0, 1),
                max_record: 256,
                frame_capacity: 232,
                ingress,
                rx,
            });
            let mut bytes = Vec::new();
            welcome.encode(&mut bytes);
            match case {
                0 => welcome.encode(&mut bytes),
                1 => bytes.extend_from_slice(&[255, 0, 0, 0, 0, 0, 0, 0]),
                _ => {}
            }
            socket.write_all(&bytes).unwrap();
        });
        let mut client = Session::connect(&path, 1).unwrap();
        until(
            || match (case, client.poll_events(|_, _| panic!("unexpected event"))) {
                (_, Ok(())) => false,
                (0, Err(Lost::Io(error))) => {
                    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
                    true
                }
                (1, Err(Lost::Protocol(junction::control::DecodeError::UnknownKind(255)))) => true,
                (2, Err(Lost::Disconnected)) => true,
                (_, other) => panic!("unexpected case {case}: {other:?}"),
            },
        );
        assert!(!client.closed());
        server.join().unwrap();
        std::fs::remove_file(path).unwrap();
    }
}

#[test]
fn finished_ack_waits_for_all_backpressured_events() {
    const COUNT: usize = 50_000;
    let host = Host::new(2, 1);
    let (mut client, session) = host.open(1);
    let id = client.listen_tcp(tcp(), local()).unwrap();
    host.op(session);
    let (mut observer, observer_session) = host.open(1);
    let observer_id = observer.bind_udp(udp()).unwrap();
    host.op(observer_session);
    for index in 0..COUNT {
        host.event(
            session,
            id,
            if index % 2 == 0 {
                Event::Connected { mss: 1460 }
            } else {
                Event::PeerFin
            },
        );
    }
    client.finish().unwrap();
    host.retire
        .send(Retire::Finished {
            slot: slot(session),
            uid: uid(session),
        })
        .unwrap();
    host.end(session, EndHow::Graceful);
    host.retired(session);
    // The next channel event is a barrier: Retired was handled before it is visible.
    host.event(observer_session, observer_id, Event::Bound);
    assert_eq!(events(&mut observer, 1), [(observer_id, Event::Bound)]);
    // Writer handback frees the slot independently of backpressured control output.
    host.closed(session);
    let mut received = 0;
    until(|| {
        client
            .poll_events(|actual, event| {
                assert_eq!(actual, id);
                assert_eq!(
                    event,
                    if received % 2 == 0 {
                        Event::Connected { mss: 1460 }
                    } else {
                        Event::PeerFin
                    }
                );
                received += 1;
            })
            .unwrap();
        client.closed()
    });
    assert_eq!(received, COUNT);
}

#[test]
fn poll_discards_packets_for_aborted_socket_generation_after_reopen() {
    let host = Host::new(1, 1);
    let (mut client, session) = host.open(1);
    let old = client.listen_tcp(tcp(), local()).unwrap();
    assert!(matches!(host.op(session), Op::ListenTcp { .. }));
    client.abort(old);
    assert_eq!(host.op(session), Op::AbortTcp { id: old });
    host.event(session, old, Event::Freed);
    assert_eq!(events(&mut client, 1), [(old, Event::Freed)]);
    let current = client.listen_tcp(tcp(), local()).unwrap();
    assert!(matches!(host.op(session), Op::ListenTcp { .. }));
    assert_eq!(old.slot, current.slot);
    assert_ne!(old.generation, current.generation);

    let mut writers = host.rx.borrow_mut();
    let (_, writer) = writers.iter_mut().find(|(id, _)| *id == session).unwrap();
    for (id, payload) in [(old, b"old".as_slice()), (current, b"current".as_slice())] {
        let mut record = writer
            .reserve(
                payload.len(),
                junction::rx::PacketMeta::new(
                    id.slot,
                    id.generation,
                    junction::rx::TCP,
                    [0; 4],
                    0,
                    [0; 4],
                    0,
                ),
            )
            .unwrap();
        record.payload().copy_from_slice(payload);
        record.commit();
    }
    drop(writers);

    let mut packets = Vec::new();
    client
        .poll(
            |_, _| panic!("unexpected control event"),
            |id, _, frame| packets.push((id, frame.to_vec())),
        )
        .unwrap();
    assert_eq!(packets, [(current, b"current".to_vec())]);
}

#[test]
fn connect_tcp_rejects_zero_tx_capacity_before_allocating_slot() {
    let host = Host::new(1, 1);
    let (mut client, session) = host.open(1);
    let remote = SocketAddrV4::new(Ipv4Addr::LOCALHOST, 80);
    assert!(matches!(
        client.connect_tcp(
            TcpConfig {
                tx_capacity: 0,
                ..tcp()
            },
            remote,
            local(),
        ),
        Err(OpenError::Config(ConfigError::TcpTxCapacityZero))
    ));
    let id = client.connect_tcp(tcp(), remote, local()).unwrap();
    assert_eq!(
        host.op(session),
        Op::ConnectTcp {
            id,
            config: tcp(),
            remote,
            local: local(),
        }
    );
    assert_eq!(id.slot, 0);
    assert_eq!(id.generation, 1);
}

#[test]
#[should_panic(expected = "shutdown requires a TCP socket")]
fn udp_shutdown_panics() {
    let host = Host::new(1, 1);
    let (mut client, _) = host.open(1);
    let id = client.bind_udp(udp()).unwrap();
    client.shutdown(id);
}

#[test]
#[should_panic(expected = "abort requires a TCP socket")]
fn udp_abort_panics() {
    let host = Host::new(1, 1);
    let (mut client, _) = host.open(1);
    let id = client.bind_udp(udp()).unwrap();
    client.abort(id);
}
