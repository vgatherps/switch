use std::io::{self, Read, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use ceph_ring::{Config, Reader};
use switch_junction::control::{
    ClientMessage, EndReason, HostMessage, ListenEndpoint, SocketId, TcpConfig, UdpConfig, VERSION,
    Welcome, WelcomeStatus,
};
use switch_junction::session::{FinishError, Lost, Session};
use shared_memory::{Backing, unique_name};
use slice_ringbuffer::RingWriter;

const WAIT: Duration = Duration::from_secs(5);
const STREAM: u64 = 1;
static NEXT_PATH: AtomicU64 = AtomicU64::new(0);

struct RawHost {
    path: PathBuf,
    socket: UnixStream,
    reader: Reader,
    _rx_backing: Backing,
    _rx_writer: RingWriter,
}

impl RawHost {
    fn connect() -> (Self, Session) {
        let reader = Reader::create(Config {
            ring_size: 64 * 1024,
            max_record: 256,
            dir: "/dev/shm".into(),
        })
        .unwrap();
        let name = unique_name("jnct-finish-rx").unwrap();
        #[cfg(target_os = "linux")]
        let mut rx_backing =
            Backing::create_file(&std::path::Path::new("/dev/shm").join(name)).unwrap();
        #[cfg(target_os = "macos")]
        let mut rx_backing = Backing::create_shm(&name).unwrap();
        let rx_writer = RingWriter::create(&mut rx_backing, 64 * 1024).unwrap();
        let path = std::env::temp_dir().join(format!(
            "jnct-finish-{}-{}.sock",
            std::process::id(),
            NEXT_PATH.fetch_add(1, Ordering::Relaxed)
        ));
        let listener = UnixListener::bind(&path).unwrap();
        let ingress = reader.name().to_owned();
        let rx = rx_backing.name().to_owned();
        let host = thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();
            socket.set_read_timeout(Some(WAIT)).unwrap();
            socket.set_write_timeout(Some(WAIT)).unwrap();
            let mut hello = [0; 16];
            socket.read_exact(&mut hello).unwrap();
            assert_eq!(
                ClientMessage::decode(&hello),
                Ok(Some((
                    ClientMessage::Hello {
                        version: VERSION,
                        max_sockets: 1
                    },
                    16
                )))
            );
            let mut bytes = Vec::new();
            HostMessage::Welcome(Welcome {
                version: VERSION,
                status: WelcomeStatus::Ok,
                stream: STREAM,
                max_record: 256,
                frame_capacity: 232,
                ingress,
                rx,
            })
            .encode(&mut bytes);
            socket.write_all(&bytes).unwrap();
            socket
        });
        let session = Session::connect(&path, 1).unwrap();
        let socket = host.join().unwrap();
        (
            Self {
                path,
                socket,
                reader,
                _rx_backing: rx_backing,
                _rx_writer: rx_writer,
            },
            session,
        )
    }

    fn end(&mut self, reason: EndReason) {
        let mut bytes = Vec::new();
        HostMessage::End(reason).encode(&mut bytes);
        self.socket.write_all(&bytes).unwrap();
    }

    fn end_records(&mut self) -> usize {
        let mut count = 0;
        for ring in 0..self.reader.nr_rings() {
            while self.reader.front(ring).is_some() {
                let record = self.reader.view(ring);
                assert_eq!(record.stream, STREAM);
                assert!(record.payload.is_empty());
                assert_eq!(record.seq, ceph_ring::END_OF_STREAM);
                count += 1;
                self.reader.advance(ring);
            }
        }
        self.reader.publish();
        count
    }
}

impl Drop for RawHost {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

fn until(mut done: impl FnMut() -> bool) {
    let deadline = Instant::now() + WAIT;
    while !done() {
        assert!(Instant::now() < deadline, "session finish timed out");
        thread::yield_now();
    }
}

#[test]
fn finish_waits_for_control_output_then_flushes_before_end_record() {
    let (mut host, mut client) = RawHost::connect();
    let id = client
        .listen_tcp(
            TcpConfig {
                rx_capacity: 1024,
                tx_capacity: 1024,
                mss_cap: None,
            },
            ListenEndpoint {
                addr: None,
                port: 1234,
            },
        )
        .unwrap();
    // The host deliberately reads nothing after Hello. More than a socket buffer of
    // operations must remain in the client's nonblocking output queue.
    const SHUTDOWNS: usize = 1024 * 1024;
    for _ in 0..SHUTDOWNS {
        client.shutdown(id);
    }
    assert_eq!(client.finish(), Err(FinishError::ControlPending));
    assert_eq!(
        host.end_records(),
        0,
        "pending control must not publish an end record"
    );
    assert!(!client.closed());

    host.socket.set_nonblocking(true).unwrap();
    let mut received = Vec::new();
    let mut chunk = [0; 65536];
    until(|| {
        loop {
            match host.socket.read(&mut chunk) {
                Ok(0) => panic!("client disconnected"),
                Ok(n) => received.extend_from_slice(&chunk[..n]),
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(error) => panic!("control read failed: {error}"),
            }
        }
        match client.finish() {
            Ok(()) => true,
            Err(FinishError::ControlPending) => {
                assert_eq!(host.end_records(), 0);
                false
            }
            other => panic!("unexpected finish result: {other:?}"),
        }
    });
    // Finish succeeding means all remaining bytes are already readable, without
    // another client poll or any host acknowledgement.
    until(|| {
        match host.socket.read(&mut chunk) {
            Ok(n) => received.extend_from_slice(&chunk[..n]),
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => return true,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => return false,
            other => panic!("control read failed: {other:?}"),
        }
        false
    });
    let mut messages = 0;
    let mut bytes = received.as_slice();
    while let Some((message, used)) = ClientMessage::decode(bytes).unwrap() {
        match message {
            ClientMessage::Op(switch_junction::control::Op::ListenTcp { id: actual, .. }) => {
                assert_eq!(messages, 0);
                assert_eq!(actual, id);
            }
            ClientMessage::Op(switch_junction::control::Op::ShutdownTcp { id: actual }) => {
                assert!(messages > 0);
                assert_eq!(actual, id);
            }
            other => panic!("unexpected operation: {other:?}"),
        }
        messages += 1;
        bytes = &bytes[used..];
    }
    assert!(bytes.is_empty());
    assert_eq!(messages, SHUTDOWNS + 1);
    assert_eq!(host.end_records(), 1);
    client.finish().unwrap();
    assert_eq!(host.end_records(), 0, "successful finish is idempotent");
}

// Linux rejects writes after the peer shuts down reading; macOS still accepts them.
#[cfg(target_os = "linux")]
#[test]
fn control_write_failure_is_reported_without_read_eof() {
    let (host, mut client) = RawHost::connect();
    host.socket.shutdown(std::net::Shutdown::Read).unwrap();
    // The host's write half remains open and has sent no further messages.
    client
        .poll_events(|_, _| panic!("unexpected event"))
        .unwrap();
    client
        .listen_tcp(
            TcpConfig {
                rx_capacity: 1024,
                tx_capacity: 1024,
                mss_cap: None,
            },
            ListenEndpoint {
                addr: None,
                port: 1234,
            },
        )
        .unwrap();
    assert!(matches!(
        client.poll_events(|_, _| panic!("unexpected event")),
        Err(Lost::Disconnected)
    ));
    assert!(!client.closed());
}

#[test]
fn eof_after_finish_without_acknowledgement_is_lost() {
    let (host, mut client) = RawHost::connect();
    client.finish().unwrap();
    host.socket.shutdown(std::net::Shutdown::Both).unwrap();
    until(
        || match client.poll_events(|_, _| panic!("unexpected event")) {
            Err(Lost::Disconnected) => true,
            Ok(()) => false,
            other => panic!("expected lost connection, got {other:?}"),
        },
    );
    assert!(!client.closed());
}

#[test]
fn unsolicited_finished_is_invalid_host_data() {
    let (mut host, mut client) = RawHost::connect();
    host.end(EndReason::Finished);
    until(
        || match client.poll_events(|_, _| panic!("unexpected event")) {
            Err(Lost::Io(error)) => {
                assert_eq!(error.kind(), io::ErrorKind::InvalidData);
                true
            }
            Ok(()) => false,
            other => panic!("expected invalid host data, got {other:?}"),
        },
    );
    assert!(!client.closed());
    assert_eq!(host.end_records(), 0);
}

#[test]
fn socket_operations_after_finish_panic_before_validation() {
    let operations: [fn(&mut Session); 6] = [
        |client| {
            let _ = client.connect_tcp(
                TcpConfig {
                    rx_capacity: u32::MAX,
                    tx_capacity: 1024,
                    mss_cap: None,
                },
                "127.0.0.1:1234".parse().unwrap(),
                ListenEndpoint {
                    addr: None,
                    port: 0,
                },
            );
        },
        |client| {
            let _ = client.listen_tcp(
                TcpConfig {
                    rx_capacity: u32::MAX,
                    tx_capacity: 1024,
                    mss_cap: None,
                },
                ListenEndpoint {
                    addr: None,
                    port: 1234,
                },
            );
        },
        |client| {
            let _ = client.bind_udp(UdpConfig {
                local: ListenEndpoint {
                    addr: None,
                    port: 0,
                },
                rx_queue: 1024,
                tx_checksum: true,
                broadcast: false,
            });
        },
        |client| {
            client.shutdown(SocketId {
                slot: u32::MAX,
                generation: 0,
            });
        },
        |client| {
            let _ = client.send_tcp(
                SocketId {
                    slot: u32::MAX,
                    generation: 0,
                },
                &[],
            );
        },
        |client| {
            let _ = client.send_udp(
                SocketId {
                    slot: u32::MAX,
                    generation: 0,
                },
                std::net::SocketAddrV4::new(std::net::Ipv4Addr::LOCALHOST, 1234),
                &[],
            );
        },
    ];
    for operation in operations {
        let (mut host, mut client) = RawHost::connect();
        client.finish().unwrap();
        assert_eq!(host.end_records(), 1);
        let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            operation(&mut client);
        }))
        .expect_err("socket operation after finish did not panic");
        let message = panic
            .downcast_ref::<&str>()
            .copied()
            .or_else(|| panic.downcast_ref::<String>().map(String::as_str));
        assert_eq!(
            message,
            Some("socket operation after successful Session::finish()")
        );
        assert_eq!(host.end_records(), 0);
        host.socket.set_nonblocking(true).unwrap();
        assert_eq!(
            host.socket.read(&mut [0]).unwrap_err().kind(),
            io::ErrorKind::WouldBlock,
            "socket operation after finish emitted control bytes"
        );
    }
}

#[test]
fn cleanup_after_finish_is_drop_safe_before_id_validation() {
    let operations: [fn(&mut Session, SocketId); 3] = [
        |client, id| client.close(id, false),
        |client, id| client.close(id, true),
        Session::abort,
    ];
    for cleanup in operations {
        let (mut host, mut client) = RawHost::connect();
        client.finish().unwrap();
        assert_eq!(host.end_records(), 1);
        let ids = [
            SocketId {
                slot: 0,
                generation: 0,
            },
            SocketId {
                slot: 0,
                generation: u32::MAX,
            },
            SocketId {
                slot: u32::MAX,
                generation: 0,
            },
        ];
        for id in ids {
            cleanup(&mut client, id);
            cleanup(&mut client, id);
        }
        client.finish().unwrap();
        assert_eq!(host.end_records(), 0, "cleanup emitted an end record");
        host.socket.set_nonblocking(true).unwrap();
        let mut byte = [0];
        assert_eq!(
            host.socket.read(&mut byte).unwrap_err().kind(),
            io::ErrorKind::WouldBlock,
            "cleanup emitted control bytes"
        );
    }
}

#[cfg(not(target_os = "linux"))]
#[test]
fn full_ingress_maps_to_stream_full_and_can_be_retried() {
    let (mut host, mut client) = RawHost::connect();
    let writer = ceph_ring::Writer::attach(host.reader.name()).unwrap();
    // Off Linux Writer always uses ring zero; no CPU migration can select an
    // empty ring between filling ingress and calling finish.
    while writer.try_finish(STREAM, 0).is_ok() {}
    assert_eq!(client.finish(), Err(FinishError::StreamFull));
    assert!(!client.closed());
    assert!(host.end_records() > 0);
    client.finish().unwrap();
    assert_eq!(host.end_records(), 1);
}
