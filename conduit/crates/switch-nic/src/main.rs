//! The `switch-nic` command: opens one NIC port from a network config file, on the one CPU the process may run on, and runs one mode. `info` prints what the card offers and closes it; it is the check that a machine is ready. `sockperf-server` answers sockperf 3.10 clients over UDP and TCP. `feed` reads Binance's bookTicker WebSocket over TLS through the configured gateway. Exit status: 0 success, 2 packet buffers still held when the port closed, 3 argument, setup or run error, 101 panic.
//!
//! ```text
//! switch-nic --config FILE <MODE> [MODE OPTIONS]
//!   info
//!   sockperf-server [--udp-ports P[,P..]] [--tcp-port P] [--tcp-connections N] [--seconds S] [--report-seconds R]
//!   feed [--host H] [--port P] [--path P] [--print N] [--seconds S]
//! ```
//!
//! The modes, their options and their output are described in `docs/operations/switch-nic.md`.
mod args;
mod feed;
mod hugepages;
mod nic;
mod server;

use std::fmt;
use std::io::{self, Write};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use switch_binance_feed::{Clock, INGRESS_BUDGET};
use switch_net::{Net, NetConfig};
use switch_netconf::Config;
use switch_open::{open, OpenError, Opened};
use dpdk_smoltcp::Stack;

use args::{Args, Mode};

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

static STOP: AtomicBool = AtomicBool::new(false);

pub fn line(key: &str, value: impl fmt::Display) {
    let _ = writeln!(io::stdout(), "switch-nic: {key}: {value}");
}

pub fn stopped() -> bool {
    STOP.load(Ordering::Relaxed)
}

extern "C" fn stop_signal(_: libc::c_int) {
    STOP.store(true, Ordering::Relaxed);
}

fn signals() -> Result<(), String> {
    // SAFETY: sigaction is initialized with a C handler that only sets an atomic.
    let mut action: libc::sigaction = unsafe { std::mem::zeroed() };
    action.sa_sigaction = stop_signal as *const () as usize;
    unsafe { libc::sigemptyset(&mut action.sa_mask) };
    for signal in [libc::SIGINT, libc::SIGTERM] {
        if unsafe { libc::sigaction(signal, &action, std::ptr::null_mut()) } != 0 {
            return Err(format!("sigaction: {}", io::Error::last_os_error()));
        }
    }
    Ok(())
}

#[derive(Default)]
pub struct Turns {
    received: u64,
    rejected: u64,
    processed: u64,
    transmitted: u64,
    staged: usize,
    pending: usize,
}

impl Turns {
    pub fn turn(&mut self, stack: &mut Stack, clock: &Clock) -> Result<(), String> {
        let t = stack
            .turn(clock.now(), 32)
            .map_err(|e| format!("Stack::turn: {e}"))?;
        self.received += t.rx_received as u64;
        self.rejected += t.rx_rejected as u64;
        self.processed += t.rx_processed as u64;
        self.transmitted += t.tx_accepted as u64;
        self.staged = t.rx_staged;
        self.pending = t.tx_pending;
        Ok(())
    }

    pub fn send(&mut self, stack: &mut Stack) -> Result<(), String> {
        let sent = stack.send().map_err(|e| format!("Stack::send: {e}"))?;
        self.transmitted += sent.tx_accepted as u64;
        self.pending = sent.tx_pending;
        Ok(())
    }

    pub fn flush(&mut self, stack: &mut Stack) -> Result<(), String> {
        let sent = stack.flush().map_err(|e| format!("Stack::flush: {e}"))?;
        self.transmitted += sent.tx_accepted as u64;
        self.pending = sent.tx_pending;
        Ok(())
    }

    fn print(&self) {
        line(
            "stack_totals",
            format_args!(
                "rx_received={} rx_rejected={} rx_processed={} tx_accepted={} rx_staged={} tx_pending={}",
                self.received, self.rejected, self.processed, self.transmitted, self.staged, self.pending
            ),
        );
    }
}

fn run(args: Args) -> i32 {
    let config = match Config::load(&args.config) {
        Ok(config) => config,
        Err(error) => {
            line("error", error);
            return 3;
        }
    };
    if matches!(args.mode, Mode::Feed(_)) && config.gateway.is_none() {
        line("error", "feed requires a gateway in the config");
        return 3;
    }
    let target = match &args.mode {
        Mode::Feed(f) => match feed::resolve(f) {
            Ok(t) => Some(t),
            Err(e) => {
                line("error", e);
                return 3;
            }
        },
        _ => None,
    };
    let tcp_socket = match (&args.mode, &target) {
        (Mode::Feed(_), Some(target)) => Some(nic::feed_socket(&config, *target.addr.ip())),
        (Mode::Server(s), _) => s.tcp_port.map(|_| nic::server_socket()),
        _ => None,
    };
    if let Err(e) = signals() {
        line("error", e);
        return 3;
    }
    let clock = Clock::new(Instant::now());
    let sizing = nic::sizing(&config, &args.mode, tcp_socket.as_ref());
    if let (Mode::Feed(_), Some(socket)) = (&args.mode, &tcp_socket) {
        line("feed_mss_cap", socket.mss_cap.expect("feed_socket caps the MSS"));
    }
    let free_before = hugepages::free(std::path::Path::new(hugepages::SYSFS));
    // SAFETY: EAL creates the configured PCI device or vdev from the config, so
    // the ethdev is new and no caller-attached ring exists. This sole thread
    // owns its queue pair and all packets. The selected PMD must honor Port's
    // mbuf contract and return its DMA/packet ownership at close. EAL never
    // deinitializes, so it outlives the Port.
    let opened = match unsafe { open(&config, &sizing, None, &mut || clock.now()) } {
        Ok(opened) => opened,
        Err(OpenError::PortOpen(error)) => {
            line("port_open_error", error.cause);
            if let Some(rollback) = error.rollback_error {
                line("port_rollback_error", rollback);
            }
            drop(error.retained);
            return 3;
        }
        Err(OpenError::Link {
            port_id,
            port,
            error,
        }) => {
            line("error", format_args!("link query: {error}"));
            if let Err(error) = nic::print_eth_stats(port_id) {
                line("error", error);
            }
            return nic::close(*port, 3);
        }
        Err(OpenError::LinkDown {
            port_id,
            port,
            link: _,
        }) => {
            line("error", "link down after 10 seconds");
            if let Err(error) = nic::print_eth_stats(port_id) {
                line("error", error);
            }
            return nic::close(*port, 3);
        }
        Err(OpenError::Stack { port, error }) => {
            line("error", format_args!("Stack::new: {error}"));
            return nic::close(*port, 3);
        }
        Err(OpenError::Seed { port, error }) => {
            line("error", format_args!("OS random seed: {error}"));
            return nic::close(*port, 3);
        }
        Err(OpenError::GatewayUnresolved {
            stack,
            gateway,
            waited,
        }) => {
            line(
                "error",
                format_args!("gateway {gateway} unresolved after {waited}"),
            );
            return nic::close(stack.into_port(), 3);
        }
        Err(OpenError::Receive { stack, error }) => {
            line("error", format_args!("gateway receive: {error}"));
            return nic::close(stack.into_port(), 3);
        }
        Err(error) => {
            line("error", error);
            return 3;
        }
    };
    let Opened {
        mut stack,
        eal: _eal,
        report,
    } = opened;
    for report_line in report.to_string().lines() {
        let _ = writeln!(io::stdout(), "switch-nic: {report_line}");
    }
    let free_after = hugepages::free(std::path::Path::new(hugepages::SYSFS));
    for size in hugepages::report(&free_before, &free_after) {
        line("hugepages", size);
    }
    let port_id = report.port_id;
    let rx_timestamps = report.offloads.offloads.rx_timestamp;
    let mut turns = Turns::default();
    let workload_start = Instant::now();
    let result = match &args.mode {
        Mode::Info => Ok(()),
        Mode::Server(s) => server::run(
            &mut stack,
            s,
            port_id,
            rx_timestamps,
            &mut turns,
            &clock,
            workload_start,
        ),
        Mode::Feed(f) => {
            let mut net = Net::new(stack, NetConfig { ingress_budget: INGRESS_BUDGET });
            // One read of the data ENI's PHC (the clock of its RX stamps) between two
            // CLOCK_REALTIME reads (the clock the feed compares stamps with; chrony keeps it
            // on the PHC). A fact for the log, not a gate.
            let before = switch_binance_feed::realtime_ns();
            let phc = dpdk_smoltcp::read_clock(port_id);
            let after = switch_binance_feed::realtime_ns();
            match phc {
                Ok(phc) => {
                    let mid = (i128::from(before) + i128::from(after)) / 2;
                    line("phc_check", format_args!("phc_minus_realtime_ns={}", i128::from(phc) - mid));
                }
                Err(code) if code == -libc::ENOTSUP => line("phc_check", "unsupported"),
                Err(code) => line("phc_check", format_args!("read_clock failed: {code}")),
            }
            let result = feed::run(
                &mut net,
                f,
                target.as_ref().unwrap(),
                tcp_socket.expect("feed mode has a socket"),
                &clock,
            );
            stack = net.into_stack().ok().expect("the feed dropped every handle and socket");
            result
        }
    };
    let mut code = 0;
    if let Err(e) = result {
        line("error", e);
        code = 3;
    }
    if let Err(e) = nic::print_eth_stats(port_id) {
        line("error", e);
        code = 3;
    }
    nic::close(stack.into_port(), code)
}

fn main() {
    let args = match args::parse(std::env::args_os().skip(1)) {
        Ok(args) => args,
        Err(e) => {
            line("error", e);
            line("usage", args::usage());
            std::process::exit(3);
        }
    };
    std::process::exit(run(args));
}
