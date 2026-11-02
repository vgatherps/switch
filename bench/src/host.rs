//! What every Zhida side of the bench shares: the clock, opening the Net from a netconf config,
//! the busy-poll round and the stop signal.

use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Instant as WallInstant, SystemTime, UNIX_EPOCH};

use dpdk_smoltcp::Eal;
use switch::Runtime;
use switchnet::{Instant, Net, NetConfig};
use zhida_netconf::Config;
use zhida_open::{Sizing, open};

pub const INGRESS_BUDGET: usize = 128;

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
    port_id: u16,
    xstats: Option<crate::xstats::Snapshots>,
    _eal: Eal,
}

impl Host {
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
            port_id: opened.report.port_id,
            xstats: None,
            _eal: opened.eal,
        })
    }

    pub(crate) fn start_xstats(&mut self, role: &'static str) {
        if std::env::var("ZHIDA_BENCH_XSTATS").as_deref() == Ok("1") {
            self.xstats = Some(crate::xstats::Snapshots::start(role, self.port_id));
        }
    }

    /// One busy-poll round: receive at microsecond resolution, poll tasks at the same sampled
    /// time in nanoseconds, then send what they wrote; never sleeps.
    #[inline]
    pub fn round(&mut self) -> Result<u64, String> {
        let now = self.clock.now_ns();
        self.net
            .receive(self.clock.stack_now(now))
            .map_err(|e| format!("receive: {e}"))?;
        self.rt.poll(now).map_err(|e| format!("poll: {e:?}"))?;
        self.net.send().map_err(|e| format!("send: {e}"))?;
        Ok(now)
    }

    /// Drops every task, then closes the Port; fails if a socket or frame outlived them.
    pub fn close(mut self) -> Result<(), String> {
        self.rt.shutdown();
        if let Some(xstats) = self.xstats.take() {
            xstats.finish();
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
