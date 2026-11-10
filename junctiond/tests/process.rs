#![cfg(target_os = "linux")]

use std::fs::{self, File};
use std::net::{Ipv4Addr, SocketAddrV4, UdpSocket};
use std::os::unix::fs::symlink;
use std::path::PathBuf;
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use junction::control::{EndReason, Event, ListenEndpoint, UdpConfig};
use junction::session::{Lost, Session};

const WAIT: Duration = Duration::from_secs(20);
static NEXT: AtomicU64 = AtomicU64::new(0);

struct Daemon {
    root: PathBuf,
    dir: PathBuf,
    config: PathBuf,
    log: PathBuf,
    iface: String,
    cpu: String,
    child: Option<Child>,
}

impl Daemon {
    fn new() -> Self {
        let id = NEXT.fetch_add(1, Ordering::Relaxed);
        let root = std::env::temp_dir().join(format!("junctiond-process-{}-{id}", std::process::id()));
        fs::create_dir(&root).unwrap();
        let dir = root.join("ipc");
        let config = root.join("nic.json");
        let iface = format!("jn{}x{id}", std::process::id());
        let prefix = format!("junctiond-{}-{id}", std::process::id());
        fs::write(&config, format!(r#"{{
            "version": 1, "device": {{"vdev": "net_tap_{prefix},iface={iface}"}},
            "mac": null, "ipv4": "10.77.0.2/24", "gateway": null, "mtu": 1500,
            "paths": [], "offloads": "auto", "rx_timestamps": false, "descriptors": 512,
            "eal": {{"file_prefix": "{prefix}", "extra": ["--no-huge", "-m", "512", "--no-shconf"]}}
        }}"#)).unwrap();
        let cpu = unsafe { libc::sched_getcpu() };
        assert!(cpu >= 0);
        Self { log: root.join("daemon.log"), root, dir, config, iface, cpu: cpu.to_string(), child: None }
    }

    fn command(&self) -> Command {
        let mut command = Command::new("taskset");
        command.args(["-c", &self.cpu, env!("CARGO_BIN_EXE_junctiond"), "--config"])
            .arg(&self.config).arg("--dir").arg(&self.dir)
            .args(["--rx-size", "65536", "--max-sessions", "4", "--max-sockets", "8",
                "--tcp-sockets", "8", "--udp-sockets", "8", "--control-cpus", &self.cpu]);
        command
    }

    fn start(&mut self) -> Session {
        assert!(self.child.is_none());
        let log = File::create(&self.log).unwrap();
        self.child = Some(self.command().stdout(Stdio::from(log.try_clone().unwrap()))
            .stderr(Stdio::from(log)).spawn().unwrap());
        let deadline = Instant::now() + WAIT;
        // Kill a wedged startup so a blocking Hello/Welcome handshake cannot hang the suite.
        let pid = self.child.as_ref().unwrap().id() as libc::pid_t;
        let (ready, receiver) = std::sync::mpsc::channel::<()>();
        let watchdog = std::thread::spawn(move || {
            if matches!(receiver.recv_timeout(WAIT), Err(std::sync::mpsc::RecvTimeoutError::Timeout)) {
                unsafe { libc::kill(pid, libc::SIGKILL) };
            }
        });
        loop {
            let exited = self.child.as_mut().unwrap().try_wait().unwrap();
            assert!(exited.is_none(), "daemon exited {exited:?}: {}", self.diagnostics());
            if let Ok(session) = Session::connect(&self.dir.join("control.sock"), 8) {
                ready.send(()).unwrap();
                watchdog.join().unwrap();
                return session;
            }
            assert!(Instant::now() < deadline, "daemon did not serve: {}", self.diagnostics());
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    fn diagnostics(&self) -> String {
        fs::read_to_string(&self.log).unwrap_or_default()
    }

    fn signal(&mut self, signal: libc::c_int) {
        let pid = self.child.as_ref().unwrap().id() as libc::pid_t;
        assert_eq!(unsafe { libc::kill(pid, signal) }, 0);
    }

    fn wait(&mut self) -> ExitStatus {
        let deadline = Instant::now() + WAIT;
        loop {
            if let Some(status) = self.child.as_mut().unwrap().try_wait().unwrap() {
                self.child.take();
                return status;
            }
            assert!(Instant::now() < deadline, "daemon did not exit: {}", self.diagnostics());
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    fn shutdown(&mut self, session: &mut Session) {
        self.signal(libc::SIGTERM);
        let deadline = Instant::now() + WAIT;
        loop {
            match session.poll_events(|_, _| {}) {
                Err(Lost::Ended(EndReason::HostShutdown)) => break,
                Err(other) => panic!("expected End(HostShutdown), got {other:?}: {}", self.diagnostics()),
                Ok(()) => {}
            }
            assert!(Instant::now() < deadline, "HostShutdown did not arrive");
            std::thread::sleep(Duration::from_millis(1));
        }
        assert!(self.wait().success(), "{}", self.diagnostics());
    }

    fn kernel_peer(&self) -> UdpSocket {
        for args in [
            vec!["address", "add", "10.77.0.1/24", "dev", &self.iface],
            vec!["link", "set", &self.iface, "up"],
        ] {
            assert!(Command::new("ip").args(&args).status().unwrap().success(), "ip {args:?}");
        }
        let peer = UdpSocket::bind((Ipv4Addr::new(10, 77, 0, 1), 30001)).unwrap();
        peer.set_nonblocking(true).unwrap();
        peer
    }
}

impl Drop for Daemon {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
        fs::remove_dir_all(&self.root).unwrap();
    }
}

fn exchange(session: &mut Session, peer: &UdpSocket) {
    let id = session.bind_udp(UdpConfig {
        local: ListenEndpoint { addr: None, port: 30000 },
        rx_queue: 8,
        tx_checksum: true,
        broadcast: false,
    }).unwrap();
    let deadline = Instant::now() + WAIT;
    let mut bound = false;
    while !bound {
        session.poll_events(|socket, event| {
            assert_eq!(socket, id);
            assert!(matches!(event, Event::Bound));
            bound = true;
        }).unwrap();
        assert!(Instant::now() < deadline, "UDP bind timed out");
        std::thread::sleep(Duration::from_millis(1));
    }
    let host = SocketAddrV4::new(Ipv4Addr::new(10, 77, 0, 2), 30000);
    peer.send_to(b"kernel to junction", host).unwrap();
    let mut received = false;
    while !received {
        session.poll(|_, _| {}, |socket, meta, frame| {
            assert_eq!(socket, id);
            assert_eq!(&*frame, b"kernel to junction");
            assert_eq!(meta.src_addr, [10, 77, 0, 1]);
            assert_eq!(meta.src_port, 30001);
            assert_eq!(meta.dst_addr, [10, 77, 0, 2]);
            received = true;
        }).unwrap();
        assert!(Instant::now() < deadline, "kernel datagram did not reach Session");
        std::thread::sleep(Duration::from_millis(1));
    }
    session.send_udp(id, SocketAddrV4::new(Ipv4Addr::new(10, 77, 0, 1), 30001), b"junction to kernel").unwrap();
    let mut bytes = [0; 128];
    loop {
        match peer.recv_from(&mut bytes) {
            Ok((len, source)) => {
                assert_eq!(&bytes[..len], b"junction to kernel");
                assert_eq!(source, std::net::SocketAddr::V4(host));
                break;
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(error) => panic!("kernel receive: {error}"),
        }
        session.poll_events(|_, _| {}).unwrap();
        assert!(Instant::now() < deadline, "Session datagram did not reach kernel");
        std::thread::sleep(Duration::from_millis(1));
    }
}

#[test]
fn udp_exchange_and_sigterm_reports_host_shutdown() {
    let mut daemon = Daemon::new();
    let mut session = daemon.start();
    let peer = daemon.kernel_peer();
    exchange(&mut session, &peer);
    daemon.shutdown(&mut session);
    assert_eq!(fs::read_dir(&daemon.dir).unwrap().count(), 1, "only lock should remain");
}

#[test]
fn directory_lock_rejects_a_second_daemon_without_disturbing_the_first() {
    let mut daemon = Daemon::new();
    let mut session = daemon.start();
    let before: std::collections::BTreeSet<_> = fs::read_dir(&daemon.dir).unwrap()
        .map(|entry| entry.unwrap().file_name()).collect();
    let log = File::create(daemon.root.join("second.log")).unwrap();
    let mut second = daemon.command().stdout(Stdio::null()).stderr(Stdio::from(log)).spawn().unwrap();
    let deadline = Instant::now() + WAIT;
    let status = loop {
        if let Some(status) = second.try_wait().unwrap() { break status; }
        if Instant::now() >= deadline {
            second.kill().unwrap();
            second.wait().unwrap();
            panic!("second daemon did not fail while the first held the lock");
        }
        std::thread::sleep(Duration::from_millis(1));
    };
    assert!(!status.success());
    let stderr = fs::read_to_string(daemon.root.join("second.log")).unwrap();
    assert!(stderr.contains("owned by another junctiond"), "must fail at flock, not DPDK: {stderr}");
    let after: std::collections::BTreeSet<_> = fs::read_dir(&daemon.dir).unwrap()
        .map(|entry| entry.unwrap().file_name()).collect();
    assert_eq!(after, before, "second daemon changed the first daemon's IPC directory");
    let peer = daemon.kernel_peer();
    exchange(&mut session, &peer);
    daemon.shutdown(&mut session);
}

#[test]
fn sigkill_restart_removes_stale_backings_and_socket_but_nothing_else() {
    let mut daemon = Daemon::new();
    let session = daemon.start();
    let stale: Vec<_> = fs::read_dir(&daemon.dir).unwrap().map(|entry| entry.unwrap().path())
        .filter(|path| path.file_name().unwrap() != "lock").collect();
    assert!(stale.iter().any(|path| path.file_name().unwrap().to_str().unwrap().starts_with("jnctrx")));
    assert!(stale.iter().any(|path| path.file_name().unwrap().to_str().unwrap().starts_with("cph")));
    daemon.signal(libc::SIGKILL);
    assert!(!daemon.wait().success());
    assert!(stale.iter().all(|path| path.exists()), "SIGKILL should leave all IPC names behind");
    drop(session);
    fs::write(daemon.dir.join("unrelated"), b"keep").unwrap();
    fs::write(daemon.dir.join("cph-not-a-backing"), b"keep").unwrap();
    fs::create_dir(daemon.dir.join("jnctrx0123456789abcdef")).unwrap();
    symlink("unrelated", daemon.dir.join("cph0123456789abcdef")).unwrap();
    let mut session = daemon.start();
    for path in stale.iter().filter(|path| path.file_name().unwrap() != "control.sock") {
        assert!(!path.exists(), "stale backing survived restart: {}", path.display());
    }
    assert_eq!(fs::read(daemon.dir.join("unrelated")).unwrap(), b"keep");
    assert_eq!(fs::read(daemon.dir.join("cph-not-a-backing")).unwrap(), b"keep");
    assert!(daemon.dir.join("jnctrx0123456789abcdef").is_dir());
    assert!(daemon.dir.join("cph0123456789abcdef").is_symlink());
    let peer = daemon.kernel_peer();
    exchange(&mut session, &peer);
    daemon.shutdown(&mut session);
}

#[test]
fn usage_is_printed_for_parse_errors_but_not_runtime_failures() {
    let output = Command::new(env!("CARGO_BIN_EXE_junctiond")).output().unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("usage: junctiond"));
    let daemon = Daemon::new();
    fs::remove_file(&daemon.config).unwrap();
    let output = daemon.command().output().unwrap();
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("nic.json"), "{stderr}");
    assert!(!stderr.contains("usage: junctiond"), "{stderr}");
}
