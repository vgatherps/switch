use dpdk_smoltcp::smoltcp::{socket::tcp, wire::Ipv4Address};
use dpdk_smoltcp::{Port, PortError, DEFAULT_NEIGHBOR_CAPACITY};
use zhida_dpdk_sys::{rte_eth_read_clock, rte_eth_stats, rte_eth_stats_get};

use crate::args::Mode;
use crate::line;
use zhida_open::Sizing;
use sockperf_wire::sockperf::MAX_MESSAGE;

pub const STAGING: usize = 32;
pub const UDP_QUEUE: usize = 32;
pub fn feed_socket(config: &zhida_netconf::Config, ip: Ipv4Address) -> tcp::Config {
    let cap = config.path_mtu(ip)
        .checked_sub(40)
        .expect("path MTU exceeds TCP headers");
    binance_feed::socket(Some(cap))
}

pub fn sizing(
    config: &zhida_netconf::Config,
    mode: &Mode,
    feed: Option<&tcp::Config>,
) -> Sizing {
    let (tcp, udp, bytes) = match mode {
        Mode::Info => (0, 0, MAX_MESSAGE),
        Mode::Server(s) => (
            if s.tcp_port.is_some() {
                s.tcp_connections
            } else {
                0
            },
            s.udp_ports.len(),
            MAX_MESSAGE,
        ),
        Mode::Feed(_) => (1, 0, binance_feed::SOCKET_BUFFER),
    };
    let socket = tcp::Config { rx_capacity: bytes, tx_capacity: bytes, mss_cap: None };
    let mtu = usize::from(config.mtu);
    // Shares independent of the socket count: one TCP socket's full window each way and one
    // UDP socket's queue, drawn on by every socket.
    let window = |per_socket: fn(&tcp::Config, usize) -> usize| match mode {
        Mode::Feed(_) => per_socket(feed.expect("feed mode has a config"), mtu),
        _ if tcp > 0 => per_socket(&socket, mtu),
        _ => 0,
    };
    Sizing {
        rx_share: window(tcp::rx_mbuf_budget) + if udp > 0 { UDP_QUEUE } else { 0 },
        frame_share: window(tcp::tx_mbuf_budget),
        max_sockets: tcp.max(1),
        max_udp_sockets: udp,
        rx_staging: STAGING,
        tx_pending: STAGING.max(tcp),
        neighbor_capacity: DEFAULT_NEIGHBOR_CAPACITY,
    }
}

pub fn eth_stats(port_id: u16) -> Result<rte_eth_stats, String> {
    // SAFETY: the ethdev remains open and stats is a writable output.
    let mut stats: rte_eth_stats = unsafe { std::mem::zeroed() };
    let rc = unsafe { rte_eth_stats_get(port_id, &mut stats) };
    if rc != 0 {
        return Err(format!("rte_eth_stats_get: {rc}"));
    }
    Ok(stats)
}

pub fn print_eth_stats(port_id: u16) -> Result<(), String> {
    let s = eth_stats(port_id)?;
    line(
        "eth_stats",
        format_args!(
            "ipackets={} opackets={} imissed={} ierrors={} oerrors={} rx_nombuf={}",
            s.ipackets, s.opackets, s.imissed, s.ierrors, s.oerrors, s.rx_nombuf
        ),
    );
    Ok(())
}

pub fn close(port: Port, mut code: i32) -> i32 {
    match port.try_close() {
        Ok(()) => line("port_close", "ok"),
        Err((port, PortError::OutstandingBuffers { rx, tx, frame })) => {
            line(
                "port_close",
                format_args!("buffers still in use: rx={rx} tx={tx} frame={frame}"),
            );
            code = 2;
            drop(port);
        }
        Err((port, error)) => {
            line("port_close", error);
            code = 3;
            drop(port);
        }
    }
    code
}

/// The port's NIC clock (ENA: the PHC, ns), or rte_eth_read_clock's negative errno.
pub fn read_clock(port_id: u16) -> Result<u64, i32> {
    let mut clock = 0;
    // SAFETY: the ethdev stays open while this runs (the clock thread is joined before the port
    // closes) and `clock` is a writable output.
    match unsafe { rte_eth_read_clock(port_id, &mut clock) } {
        0 => Ok(clock),
        error => Err(error),
    }
}

/// Moves the calling thread to the online CPUs not in /sys/devices/system/cpu/isolated (the
/// housekeeping CPUs on the hft host). Keeps its CPUs if every CPU is isolated.
pub fn to_housekeeping() -> Result<(), String> {
    let read = |name| std::fs::read_to_string(format!("/sys/devices/system/cpu/{name}"));
    let online = read("online").map_err(|e| format!("cpu/online: {e}"))?;
    let isolated = read("isolated").map_err(|e| format!("cpu/isolated: {e}"))?;
    let isolated = cpu_list(&isolated)?;
    let cpus: Vec<usize> = cpu_list(&online)?.into_iter().filter(|cpu| !isolated.contains(cpu)).collect();
    if cpus.is_empty() {
        return Ok(());
    }
    // SAFETY: cpu_set_t is plain data; all-zero bytes are the empty set.
    let mut set: libc::cpu_set_t = unsafe { std::mem::zeroed() };
    for cpu in cpus {
        // SAFETY: CPU_SET writes one bit of `set`; an index past CPU_SETSIZE panics.
        unsafe { libc::CPU_SET(cpu, &mut set) };
    }
    // SAFETY: `set` is initialized and its size passed; pid 0 is this thread.
    if unsafe { libc::sched_setaffinity(0, std::mem::size_of_val(&set), &set) } != 0 {
        return Err(format!("sched_setaffinity: {}", std::io::Error::last_os_error()));
    }
    Ok(())
}

/// A sysfs CPU list (`0-3,8,10-11`, empty for none).
fn cpu_list(text: &str) -> Result<Vec<usize>, String> {
    let number = |s: &str| s.parse::<usize>().map_err(|e| format!("CPU list {text:?}: {e}"));
    let mut cpus = Vec::new();
    for range in text.trim().split(',').filter(|range| !range.is_empty()) {
        match range.split_once('-') {
            Some((first, last)) => cpus.extend(number(first)?..=number(last)?),
            None => cpus.push(number(range)?),
        }
    }
    Ok(cpus)
}

#[cfg(test)]
mod tests {
    use std::ffi::OsString;

    use dpdk_smoltcp::smoltcp::socket::tcp;

    use super::{cpu_list, feed_socket, sizing, STAGING, UDP_QUEUE};
    use crate::args::Args;

    fn parsed(args: &[&str]) -> Args {
        crate::args::parse(args.iter().map(OsString::from)).unwrap()
    }

    #[test]
    fn sizing_shares_hold_one_window_whatever_the_socket_count() {
        for (args, tcp, udp, bytes) in [
            (
                &["--config", "nic.json", "info"][..],
                0,
                0,
                sockperf_wire::sockperf::MAX_MESSAGE,
            ),
            (
                &[
                    "--config",
                    "nic.json",
                    "sockperf-server",
                    "--tcp-port",
                    "1",
                    "--tcp-connections",
                    "64",
                    "--udp-ports",
                    "11111,11112",
                ][..],
                64,
                2,
                sockperf_wire::sockperf::MAX_MESSAGE,
            ),
            (
                &["--config", "nic.json", "feed"][..],
                1,
                0,
                binance_feed::SOCKET_BUFFER,
            ),
        ] {
            let args = parsed(args);
            for mtu in [1500, 9000] {
                let config = test_config(mtu, "[]");
                let socket = feed_socket(&config, "203.0.113.1".parse().unwrap());
                let sizing = sizing(&config, &args.mode, Some(&socket));
                let window = tcp::Config { rx_capacity: bytes, tx_capacity: bytes, mss_cap: None };
                let expected = usize::from(tcp > 0) * tcp::rx_mbuf_budget(&window, usize::from(mtu))
                    + usize::from(udp > 0) * UDP_QUEUE;
                assert_eq!(sizing.rx_share, expected);
                assert_eq!(sizing.rx_staging, STAGING);
            }
        }
    }

    #[test]
    fn feed_sizing_uses_path_capped_budget() {
        let config = test_config(
            9000,
            r#"[{"prefix":"203.0.113.0/24","mtu":1500,"why":"feed path"}]"#,
        );
        let args = parsed(&["--config", "nic.json", "feed"]);
        let ip = "203.0.113.1".parse().unwrap();
        assert_eq!(config.path_mtu(ip).checked_sub(40).unwrap(), 1460);
        let socket = feed_socket(&config, ip);
        let capped = dpdk_smoltcp::smoltcp::socket::tcp::rx_mbuf_budget(&socket, 9000);
        let uncapped = dpdk_smoltcp::smoltcp::socket::tcp::rx_mbuf_budget(
            &binance_feed::socket(None), 9000,
        );
        assert_eq!(capped, 180);
        assert_eq!(uncapped, 30);
        let feed_sizing = sizing(&config, &args.mode, Some(&socket));
        assert_eq!(feed_sizing.rx_share, capped);
        // The send side's frame limit follows the same 1460-byte cap: 256 KiB / 1460.
        assert_eq!(feed_sizing.frame_share, 180);
        assert!(capped > uncapped);

        let socket = feed_socket(&config, "203.0.114.1".parse().unwrap());
        let feed_sizing = sizing(&config, &args.mode, Some(&socket));
        assert_eq!(feed_sizing.rx_share, uncapped);
        // Uncapped at MTU 9000: 256 KiB / 8960.
        assert_eq!(feed_sizing.frame_share, 30);
    }

    #[test]
    fn cpu_lists() {
        assert_eq!(cpu_list("0-3,8,10-11\n").unwrap(), [0, 1, 2, 3, 8, 10, 11]);
        assert_eq!(cpu_list("5\n").unwrap(), [5]);
        assert!(cpu_list("\n").unwrap().is_empty());
        assert!(cpu_list("1-x").is_err());
    }

    fn test_config(mtu: u16, paths: &str) -> zhida_netconf::Config {
        zhida_netconf::Config::from_json(&format!(
            r#"{{"version":1,"device":{{"vdev":"net_tap0,iface=cdnic0"}},"mac":null,"ipv4":"192.0.2.2/24","gateway":null,"mtu":{mtu},"paths":{paths},"offloads":"auto","rx_timestamps":false,"descriptors":1024,"eal":{{"file_prefix":"test","extra":[]}}}}"#
        ))
        .unwrap()
    }
}
