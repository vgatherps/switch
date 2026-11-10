//! What every Zhida side of the bench shares: the clock, opening the Net from a netconf config,
//! the busy-poll round and the stop signal.

use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Instant as WallInstant, SystemTime, UNIX_EPOCH};

use dpdk_smoltcp::Eal;
use junction::session::Session;
use switch::Runtime;
use switchnet::{Instant, Net, NetConfig, RemoteConfig};
use zhida_netconf::Config;
use zhida_open::{Sizing, open};

pub const INGRESS_BUDGET: usize = 128;

/// The app either owns the NIC configuration or connects to its daemon owner.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Backend {
    Conduit(PathBuf),
    Junction(PathBuf),
}

/// Monotonic ns since the process's origin, with the wall-clock time of that origin for joining
/// samples across hosts and starting steps at a common wall-clock time (see `resync`).
#[derive(Clone, Copy)]
pub struct Clock {
    origin: WallInstant,
    unix_origin_ns: u64,
}

impl Default for Clock {
    fn default() -> Self {
        Self::new()
    }
}

impl Clock {
    pub fn new() -> Clock {
        let origin = WallInstant::now();
        Clock {
            origin,
            unix_origin_ns: unix_now_ns(),
        }
    }

    /// Re-reads the wall clock behind `unix_ns` and `mono_of_unix`; `now_ns` is unchanged.
    /// The wall clock is slewed (NTP) while the process runs, so a mapping taken once drifts.
    pub fn resync(&mut self) {
        let mono = self.now_ns();
        self.unix_origin_ns = unix_now_ns() - mono;
    }

    #[inline]
    pub fn now_ns(&self) -> u64 {
        self.origin.elapsed().as_nanos() as u64
    }

    pub fn unix_ns(&self, ns: u64) -> u64 {
        self.unix_origin_ns + ns
    }

    /// The monotonic ns of wall-clock time `unix_ns` (0 if before this clock's origin).
    pub fn mono_of_unix(&self, unix_ns: u64) -> u64 {
        unix_ns.saturating_sub(self.unix_origin_ns)
    }

    pub fn stack_now(&self, ns: u64) -> Instant {
        Instant::from_micros((ns / 1000) as i64)
    }
}

fn unix_now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock after 1970")
        .as_nanos() as u64
}

/// An open Net and what must outlive it.
pub struct Host {
    pub net: Net,
    pub rt: Runtime,
    pub clock: Clock,
    local: Option<Local>,
    xstats: Option<crate::xstats::Snapshots>,
}

struct Local {
    port_id: u16,
    _eal: Eal,
}

impl Host {
    pub fn connect(
        backend: &Backend,
        max_sockets: usize,
        sizing: impl FnOnce(usize) -> Sizing,
    ) -> Result<Host, String> {
        match backend {
            Backend::Conduit(path) => {
                let config = Self::config(path)?;
                Self::open(&config, &sizing(usize::from(config.mtu)))
            }
            Backend::Junction(path) => {
                let max_sockets = max_sockets.try_into()
                    .map_err(|_| "junction socket count exceeds 65535")?;
                let session = Session::connect(path, max_sockets)
                    .map_err(|e| format!("connect junction {}: {e}", path.display()))?;
                let mtu = remote_mtu(session.frame_capacity(), session.max_record())?;
                let clock = Clock::new();
                Ok(Host {
                    net: Net::remote(session, RemoteConfig { tx_frames: sizing(mtu).frame_share }),
                    rt: Runtime::new(clock.now_ns()),
                    clock,
                    local: None,
                    xstats: None,
                })
            }
        }
    }

    /// The netconf config at `path`.
    pub fn config(path: &Path) -> Result<Config, String> {
        Config::load(path).map_err(|e| format!("{}: {e}", path.display()))
    }

    /// Opens the Stack `config` describes (`zhida_open::open`: EAL, Port, link, gateway).
    pub fn open(config: &Config, sizing: &Sizing) -> Result<Host, String> {
        let clock = Clock::new();
        // SAFETY: EAL creates the configured device; this thread alone uses the Port and its
        // packets; EAL is kept in the Host and outlives the Port, which `close` closes first.
        let opened = unsafe { open(config, sizing, &mut || clock.stack_now(clock.now_ns())) }
            .map_err(|e| format!("open: {e}"))?;
        let net = Net::new(
            opened.stack,
            NetConfig {
                ingress_budget: INGRESS_BUDGET,
            },
        );
        Ok(Host {
            net,
            rt: Runtime::new(clock.now_ns()),
            clock,
            local: Some(Local { port_id: opened.report.port_id, _eal: opened.eal }),
            xstats: None,
        })
    }

    pub(crate) fn start_xstats(&mut self, role: &'static str) {
        if std::env::var("ZHIDA_BENCH_XSTATS").as_deref() == Ok("1") {
            match &self.local {
                Some(local) => self.xstats = Some(crate::xstats::Snapshots::start(role, local.port_id)),
                None => eprintln!("zhida-bench {role}: port xstats absent (backend junction)"),
            }
        }
    }

    /// One busy-poll round: turn the Net at microsecond resolution, poll tasks at the same
    /// sampled time in nanoseconds; never sleeps.
    #[inline]
    pub fn round(&mut self) -> Result<u64, String> {
        let now = self.clock.now_ns();
        self.net
            .turn(self.clock.stack_now(now))
            .map_err(|e| format!("turn: {e}"))?;
        self.rt.poll(now).map_err(|e| format!("poll: {e:?}"))?;
        Ok(now)
    }

    /// Drops every task, then closes the Port; fails if a socket or frame outlived them.
    pub fn close(mut self) -> Result<(), String> {
        self.rt.shutdown();
        if let Some(xstats) = self.xstats.take() {
            xstats.finish();
        }
        if self.local.is_none() {
            let mut session = self.net.into_session()
                .map_err(|_| "a socket outlived the tasks".to_owned())?;
            let deadline = WallInstant::now() + std::time::Duration::from_secs(10);
            while !session.closed() {
                let _ = session.finish();
                session.poll(|_, _| {}, |_, _, _| {})
                    .map_err(|e| format!("finish junction session: {e}"))?;
                if WallInstant::now() >= deadline {
                    return Err("junction session retirement timed out".into());
                }
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
            return Ok(());
        }
        let stack = self
            .net
            .into_stack()
            .map_err(|_| "a socket outlived the tasks".to_owned())?;
        stack
            .into_port()
            .try_close()
            .map_err(|(_, e)| format!("port close: {e}"))
    }
}

/// Welcome advertises UDP payload storage; netconf's MTU includes IPv4/UDP headers.
fn remote_mtu(frame_capacity: u32, max_record: usize) -> Result<usize, String> {
    let record_payload = max_record.checked_sub(junction::send::UDP_SEND_HEADER)
        .filter(|&size| size > 0)
        .ok_or("junction max_record cannot hold a UDP payload")?;
    Ok((frame_capacity as usize).min(record_payload) + 28)
}

static STOP: AtomicBool = AtomicBool::new(false);

extern "C" fn stop_signal(_: libc::c_int) {
    STOP.store(true, Ordering::Relaxed);
}

/// SIGINT and SIGTERM set the stop flag.
pub fn catch_stop() -> Result<(), String> {
    // SAFETY: sigaction is initialized with a C handler that only sets an atomic.
    let mut action: libc::sigaction = unsafe { std::mem::zeroed() };
    action.sa_sigaction = stop_signal as extern "C" fn(libc::c_int) as *const () as usize;
    unsafe { libc::sigemptyset(&mut action.sa_mask) };
    for signal in [libc::SIGINT, libc::SIGTERM] {
        if unsafe { libc::sigaction(signal, &action, std::ptr::null_mut()) } != 0 {
            return Err(format!("sigaction: {}", io::Error::last_os_error()));
        }
    }
    Ok(())
}

#[inline]
pub fn stopped() -> bool {
    STOP.load(Ordering::Relaxed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn host_selection_opens_config_or_connects_socket() {
        let missing = std::env::temp_dir().join(format!("zhida-bench-missing-{}", std::process::id()))
            .join("nic-or-socket");
        let no_sizing = |_| -> Sizing { panic!("unopened backend must not size a device") };
        let local = Host::connect(&Backend::Conduit(missing.clone()), 1, no_sizing).err().unwrap();
        assert!(local.starts_with(&format!("{}:", missing.display())), "{local}");
        let remote = Host::connect(&Backend::Junction(missing), 1, no_sizing).err().unwrap();
        assert!(remote.starts_with("connect junction "), "{remote}");
    }

    #[test]
    fn junction_sizing_uses_both_advertised_limits() {
        assert_eq!(remote_mtu(1472, 1496).unwrap(), 1500);
        assert_eq!(remote_mtu(1200, 4096).unwrap(), 1228);
        assert_eq!(remote_mtu(1472, 1024).unwrap(), 1028);
        assert!(remote_mtu(1472, junction::send::UDP_SEND_HEADER).is_err());
    }

    /// A mapping that has drifted 5 ms from the wall clock is put back by `resync`, without
    /// moving the monotonic time.
    #[test]
    fn resync_follows_the_wall_clock_and_keeps_monotonic_time() {
        let mut clock = Clock::new();
        clock.unix_origin_ns -= 5_000_000;
        let drift = |c: &Clock| unix_now_ns().abs_diff(c.unix_ns(c.now_ns()));
        assert!(drift(&clock) > 4_000_000);
        let before = clock.now_ns();
        clock.resync();
        assert!(drift(&clock) < 1_000_000, "drift {}", drift(&clock));
        assert!(clock.now_ns() >= before);
    }
}
