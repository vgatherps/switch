use std::fs::{self, File, OpenOptions};
use std::io;
use std::os::fd::AsRawFd;
use std::os::unix::fs::{DirBuilderExt, FileTypeExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::time::{Duration, Instant as WallInstant};

use ceph_fifo::Fifo;
use ceph_ring::Reader;
use dpdk_smoltcp::smoltcp::time::Instant;
use junction::host::{ControlChannels, ControlConfig, HostControl};
use junctiond::network::{Network, NetworkConfig};
use zhida_open::Sizing;

use super::flags::Args;

static STOP: AtomicBool = AtomicBool::new(false);

extern "C" fn stop_signal(_: libc::c_int) {
    STOP.store(true, Ordering::Relaxed);
}

fn catch_stop() -> io::Result<()> {
    // SAFETY: the initialized handler only stores to a lock-free atomic.
    let mut action: libc::sigaction = unsafe { std::mem::zeroed() };
    action.sa_sigaction = stop_signal as extern "C" fn(libc::c_int) as *const () as usize;
    unsafe { libc::sigemptyset(&mut action.sa_mask) };
    for signal in [libc::SIGINT, libc::SIGTERM] {
        if unsafe { libc::sigaction(signal, &action, std::ptr::null_mut()) } != 0 {
            return Err(io::Error::last_os_error());
        }
    }
    Ok(())
}

fn affinity() -> io::Result<libc::cpu_set_t> {
    let mut set = unsafe { std::mem::zeroed() };
    if unsafe { libc::sched_getaffinity(0, std::mem::size_of_val(&set), &mut set) } != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(set)
}

fn pin_control(set: &libc::cpu_set_t) -> io::Result<()> {
    if unsafe { libc::sched_setaffinity(0, std::mem::size_of_val(set), set) } != 0 {
        return Err(io::Error::last_os_error());
    }
    // Linux silently intersects with the cgroup cpuset; do not silently omit requested CPUs.
    let actual = affinity()?;
    for cpu in 0..libc::CPU_SETSIZE as usize {
        if unsafe { libc::CPU_ISSET(cpu, &actual) != libc::CPU_ISSET(cpu, set) } {
            return Err(io::Error::other("--control-cpus includes a CPU outside the allowed cpuset"));
        }
    }
    Ok(())
}

struct Directory {
    path: PathBuf,
    _lock: File,
}

impl Directory {
    fn acquire(path: &Path) -> io::Result<Self> {
        fs::DirBuilder::new().recursive(true).mode(0o700).create(path)?;
        let path = fs::canonicalize(path)?;
        let lock = OpenOptions::new().read(true).write(true).create(true).truncate(false)
            .mode(0o600).custom_flags(libc::O_NOFOLLOW).open(path.join("lock"))?;
        // Keep this inode and descriptor for the entire run; never unlink the lock file.
        if unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
            return Err(io::Error::new(io::ErrorKind::AddrInUse,
                format!("{} is owned by another junctiond: {}", path.display(), io::Error::last_os_error())));
        }
        let directory = Self { path, _lock: lock };
        directory.cleanup()?;
        Ok(directory)
    }

    fn cleanup(&self) -> io::Result<()> {
        for entry in fs::read_dir(&self.path)? {
            let entry = entry?;
            let name = entry.file_name();
            let Some(name) = name.to_str() else { continue };
            let kind = entry.file_type()?;
            if name == "control.sock" {
                if !kind.is_socket() {
                    return Err(io::Error::new(io::ErrorKind::AlreadyExists,
                        "control.sock exists but is not a socket; refusing to remove it"));
                }
            } else if !kind.is_file() || !backing_name(name) {
                continue;
            }
            fs::remove_file(entry.path())?;
        }
        Ok(())
    }
}

fn backing_name(name: &str) -> bool {
    ["cph", "jnctrx"].iter().any(|prefix| {
        name.strip_prefix(prefix).is_some_and(|suffix| {
            suffix.len() == 16 && suffix.bytes().all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        })
    })
}

pub fn run(args: Args) -> Result<(), String> {
    let mask = affinity().map_err(|error| format!("network CPU affinity: {error}"))?;
    if unsafe { libc::CPU_COUNT(&mask) } != 1 {
        return Err("network thread must be pinned to exactly one CPU before opening; use taskset -c CPU".into());
    }
    catch_stop().map_err(|error| format!("signal handlers: {error}"))?;
    let config = zhida_netconf::Config::load(&args.config)
        .map_err(|error| format!("{}: {error}", args.config.display()))?;
    let directory = Directory::acquire(&args.dir).map_err(|error| format!("--dir: {error}"))?;
    let frame_capacity = u32::from(config.mtu) - 28;
    let max_record = frame_capacity + 24;
    // Ingress is per possible CPU; one MiB per ring also fits a maximum-size IP datagram.
    let reader = Reader::create(ceph_ring::Config {
        ring_size: 1024 * 1024,
        max_record: max_record as usize,
        dir: directory.path.clone(),
    }).map_err(|error| format!("ingress: {error}"))?;
    let (to_network, operations) = mpsc::channel();
    let (events, from_network) = mpsc::channel();
    let (to_fifo, fifo_control) = mpsc::channel();
    let (retire, retired) = mpsc::channel();
    let control_config = ControlConfig {
        socket_path: directory.path.join("control.sock"),
        ingress: reader.name().to_owned(),
        rx_dir: directory.path.clone(),
        rx_size: args.rx_size,
        max_record,
        frame_capacity,
        max_sessions: args.max_sessions,
        max_sockets: args.max_sockets,
        poll_ms: 1,
        hello_timeout: Duration::from_secs(5),
    };
    let channels = ControlChannels { to_network, to_fifo, from_network, retire: retired };
    let sizing = Sizing {
        frame_share: 4096,
        max_sockets: args.tcp_sockets,
        max_udp_sockets: args.udp_sockets,
        rx_staging: 128,
        tx_pending: 64,
        neighbor_capacity: 256,
    };
    let origin = WallInstant::now();
    let mut now = || Instant::from_micros(origin.elapsed().as_micros() as i64);
    // SAFETY: the main thread alone owns the port, Stack and packets; EAL outlives them.
    let opened = unsafe { zhida_open::open(&config, &sizing, &mut now) }
        .map_err(|error| format!("open: {error}"))?;
    let _eal = opened.eal;
    let host = HostControl::bind(control_config, channels).map_err(|error| format!("control: {error}"))?;
    let fifo = Fifo::new(reader, args.max_sessions, fifo_control, retire);
    let mut network = Network::new(opened.stack, fifo, args.max_sessions,
        NetworkConfig { ingress_budget: 128, control_batch: 64, fifo_batch: 64 }, operations, events);
    let control = std::thread::Builder::new().name("junction-control".into()).spawn(move || {
        let result = pin_control(&args.control_cpus).and_then(|()| host.run());
        STOP.store(true, Ordering::Relaxed);
        result
    }).map_err(|error| format!("control thread: {error}"))?;
    let mut result = Ok(());
    while !STOP.load(Ordering::Relaxed) {
        if let Err(error) = network.iteration(now()) {
            result = Err(format!("network: {error}"));
            break;
        }
    }
    // Disconnecting both network channels makes HostControl send End(HostShutdown).
    let stack = network.into_stack();
    let control_result = control.join().map_err(|_| "control thread panicked".to_owned())
        .and_then(|result| result.map_err(|error| format!("control thread: {error}")));
    let close_result = stack.into_port().try_close().map_err(|(_, error)| format!("port close: {error}"));
    let cleanup_result = directory.cleanup().map_err(|error| format!("directory cleanup: {error}"));
    result.and(control_result).and(close_result).and(cleanup_result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cleanup_names_match_only_the_reserved_backing_namespace() {
        for name in ["cph0123456789abcdef", "jnctrx0123456789abcdef"] {
            assert!(backing_name(name));
        }
        for name in ["cph", "cph-not-ours", "cph0123456789abcde", "cph0123456789abcdef0",
            "jnctrx0123456789abcdeg", "config.json", "lock", "control.sock"] {
            assert!(!backing_name(name), "{name}");
        }
    }
}
