//! What every Zhida side of the bench shares: the clock, opening the Net from a netconf config,
//! the busy-poll round, summed `Turn` counters, ethdev stats and the stop signal.

use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Instant as WallInstant, SystemTime, UNIX_EPOCH};

use dpdk_smoltcp::Eal;
use serde::{Deserialize, Serialize};
use switch::Runtime;
use switchnet::{Instant, Net, NetConfig, Turn};
use zhida_dpdk_sys::{rte_eth_stats, rte_eth_stats_get};
use zhida_netconf::Config;
use zhida_open::{Sizing, open};

pub const INGRESS_BUDGET: usize = 32;

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

/// `Turn` counters summed over a window; staged and pending are maxima.
#[derive(Debug, Default, Clone, Copy, Serialize, Deserialize)]
pub struct TurnSums {
    pub turns: u64,
    /// Task polls of the Runtime, summed over the turns' rounds.
    #[serde(default)]
    pub tasks_polled: u64,
    pub rx_received: u64,
    pub rx_rejected: u64,
    pub rx_processed: u64,
    pub tx_accepted: u64,
    pub max_rx_staged: u64,
    pub max_tx_pending: u64,
}

impl TurnSums {
    #[inline]
    pub fn add(&mut self, t: &Turn) {
        self.turns += 1;
        self.rx_received += t.rx_received as u64;
        self.rx_rejected += t.rx_rejected as u64;
        self.rx_processed += t.rx_processed as u64;
        self.tx_accepted += t.tx_accepted as u64;
        self.max_rx_staged = self.max_rx_staged.max(t.rx_staged as u64);
        self.max_tx_pending = self.max_tx_pending.max(t.tx_pending as u64);
    }
}

/// The ethdev counters the bench reports.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct EthStats {
    pub ipackets: u64,
    pub opackets: u64,
    pub imissed: u64,
    pub ierrors: u64,
    pub oerrors: u64,
    pub rx_nombuf: u64,
}

impl EthStats {
    /// `rte_eth_stats_get`: may be slow (ENA asks the device), so only at step boundaries.
    pub fn read(port_id: u16) -> Result<EthStats, String> {
        // SAFETY: the ethdev is open while the Port lives and `stats` is a writable output.
        let mut stats: rte_eth_stats = unsafe { std::mem::zeroed() };
        let rc = unsafe { rte_eth_stats_get(port_id, &mut stats) };
        if rc != 0 {
            return Err(format!("rte_eth_stats_get: {rc}"));
        }
        Ok(EthStats {
            ipackets: stats.ipackets,
            opackets: stats.opackets,
            imissed: stats.imissed,
            ierrors: stats.ierrors,
            oerrors: stats.oerrors,
            rx_nombuf: stats.rx_nombuf,
        })
    }

    pub fn since(&self, earlier: &EthStats) -> EthStats {
        EthStats {
            ipackets: self.ipackets - earlier.ipackets,
            opackets: self.opackets - earlier.opackets,
            imissed: self.imissed - earlier.imissed,
            ierrors: self.ierrors - earlier.ierrors,
            oerrors: self.oerrors - earlier.oerrors,
            rx_nombuf: self.rx_nombuf - earlier.rx_nombuf,
        }
    }
}

/// An open Net and what must outlive it.
pub struct Host {
    pub net: Net,
    pub rt: Runtime,
    pub clock: Clock,
    pub port_id: u16,
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
        let port_id = opened.report.port_id;
        let net = Net::new(
            opened.stack,
            NetConfig {
                ingress_budget: INGRESS_BUDGET,
            },
        );
        Ok(Host {
            net,
            rt: Runtime::new(clock.now_ns() / 1_000_000),
            clock,
            port_id,
            _eal: opened.eal,
        })
    }

    /// One busy-poll round: turn the Net, poll the tasks; never sleeps.
    #[inline]
    pub fn round(&mut self, sums: &mut TurnSums) -> Result<u64, String> {
        let now = self.clock.now_ns();
        let turn = self
            .net
            .turn(self.clock.stack_now(now))
            .map_err(|e| format!("turn: {e}"))?;
        sums.add(&turn);
        let polled = self
            .rt
            .poll(now / 1_000_000)
            .map_err(|e| format!("poll: {e:?}"))?;
        sums.tasks_polled += polled.tasks_polled as u64;
        Ok(now)
    }

    /// Drops every task, then closes the Port; fails if a socket or frame outlived them.
    pub fn close(mut self) -> Result<(), String> {
        self.rt.shutdown();
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
static DUMP: AtomicBool = AtomicBool::new(false);

extern "C" fn stop_signal(_: libc::c_int) {
    STOP.store(true, Ordering::Relaxed);
}

extern "C" fn dump_signal(_: libc::c_int) {
    DUMP.store(true, Ordering::Relaxed);
}

/// SIGINT and SIGTERM set the stop flag; SIGUSR1 requests a state dump ([`dump_requested`]).
pub fn catch_stop() -> Result<(), String> {
    for (signals, handler) in [
        (
            &[libc::SIGINT, libc::SIGTERM][..],
            stop_signal as extern "C" fn(libc::c_int),
        ),
        (&[libc::SIGUSR1][..], dump_signal),
    ] {
        // SAFETY: sigaction is initialized with a C handler that only sets an atomic.
        let mut action: libc::sigaction = unsafe { std::mem::zeroed() };
        action.sa_sigaction = handler as *const () as usize;
        unsafe { libc::sigemptyset(&mut action.sa_mask) };
        for &signal in signals {
            if unsafe { libc::sigaction(signal, &action, std::ptr::null_mut()) } != 0 {
                return Err(format!("sigaction: {}", io::Error::last_os_error()));
            }
        }
    }
    Ok(())
}

#[inline]
pub fn stopped() -> bool {
    STOP.load(Ordering::Relaxed)
}

/// Whether a SIGUSR1 arrived since the last call.
#[inline]
pub fn dump_requested() -> bool {
    DUMP.load(Ordering::Relaxed) && DUMP.swap(false, Ordering::Relaxed)
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
