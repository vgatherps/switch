use dpdk_smoltcp::smoltcp::{socket::tcp, wire::Ipv4Address};
use dpdk_smoltcp::{Port, PortError, DEFAULT_NEIGHBOR_CAPACITY};
use switch_dpdk_sys::{rte_eth_stats, rte_eth_stats_get};

use crate::args::Mode;
use crate::line;
use switch_open::Sizing;
use switch_sockperf_wire::sockperf::MAX_MESSAGE;

pub const STAGING: usize = 32;
pub const UDP_QUEUE: usize = 32;
/// The feed's socket: switch-binance-feed's buffers, MSS capped by the path MTU to `ip`.
pub fn feed_socket(config: &switch_netconf::Config, ip: Ipv4Address) -> tcp::Config {
    let cap = config.path_mtu(ip)
        .checked_sub(40)
        .expect("path MTU exceeds TCP headers");
    switch_binance_feed::socket(Some(cap))
}

/// sockperf-server's TCP sockets: buffers for the largest sockperf message.
pub fn server_socket() -> tcp::Config {
    tcp::Config { rx_capacity: MAX_MESSAGE, tx_capacity: MAX_MESSAGE, mss_cap: None }
}

/// The open sizing for `mode`. `socket` is the config of the TCP sockets the mode opens
/// (`feed_socket` or `server_socket`), `None` if it opens none.
pub fn sizing(
    config: &switch_netconf::Config,
    mode: &Mode,
    socket: Option<&tcp::Config>,
) -> Sizing {
    let (tcp, udp) = match mode {
        Mode::Info => (0, 0),
        Mode::Server(s) => (s.tcp_port.map_or(0, |_| s.tcp_connections), s.udp_ports.len()),
        Mode::Feed(_) => (1, 0),
    };
    let mtu = usize::from(config.mtu);
    // A share independent of the socket count: one TCP socket's full send window, drawn on
    // by every socket.
    let window = |per_socket: fn(&tcp::Config, usize) -> usize| {
        socket.map_or(0, |socket| per_socket(socket, mtu))
    };
    Sizing {
        frame_share: window(tcp::tx_mbuf_budget),
        max_sockets: tcp.max(1),
        max_udp_sockets: udp,
        rx_staging: if matches!(mode, Mode::Feed(_)) { switch_binance_feed::INGRESS_BUDGET } else { STAGING },
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
            code = outstanding_buffers_code(code);
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

fn outstanding_buffers_code(run_code: i32) -> i32 {
    if run_code == 0 { 2 } else { run_code }
}

#[cfg(test)]
mod tests {
    use std::ffi::OsString;

    use super::{feed_socket, server_socket, sizing, STAGING};
    use crate::args::Args;

    fn parsed(args: &[&str]) -> Args {
        crate::args::parse(args.iter().map(OsString::from)).unwrap()
    }

    #[test]
    fn held_buffers_only_change_a_successful_run_to_exit_two() {
        assert_eq!(super::outstanding_buffers_code(0), 2);
        assert_eq!(super::outstanding_buffers_code(3), 3);
    }

    #[test]
    fn feed_socket_caps_the_mss_at_the_path_mtu() {
        let config = test_config(9000, r#"[{"prefix":"203.0.113.0/24","mtu":1500,"why":"feed path"}]"#);
        assert_eq!(feed_socket(&config, "203.0.113.1".parse().unwrap()).mss_cap, Some(1460));
        assert_eq!(feed_socket(&config, "203.0.114.1".parse().unwrap()).mss_cap, Some(8960));
    }

    #[test]
    fn feed_increases_only_rx_staging() {
        let config = test_config(1500, "[]");
        let feed = parsed(&["--config", "x", "feed"]);
        let server = parsed(&["--config", "x", "sockperf-server", "--tcp-port", "1"]);
        let feed = sizing(&config, &feed.mode, None);
        let server = sizing(&config, &server.mode, None);
        assert_eq!(feed.rx_staging, switch_binance_feed::INGRESS_BUDGET);
        assert_eq!(server.rx_staging, STAGING);
        assert_eq!(feed.tx_pending, server.tx_pending);
    }

    /// The frame share is one send window, whatever the number of sockets.
    #[test]
    fn sizing_shares_do_not_grow_with_the_socket_count() {
        let config = test_config(9000, "[]");
        let server = |connections: &str| {
            let args = parsed(&[
                "--config", "x", "sockperf-server", "--tcp-port", "1", "--tcp-connections",
                connections, "--udp-ports", "11111,11112",
            ]);
            let sizing = sizing(&config, &args.mode, Some(&server_socket()));
            (sizing.frame_share, sizing.max_sockets)
        };
        let (one, many) = (server("1"), server("64"));
        assert_eq!(one.0, many.0);
        assert_eq!((one.1, many.1), (1, 64));
    }

    fn test_config(mtu: u16, paths: &str) -> switch_netconf::Config {
        switch_netconf::Config::from_json(&format!(
            r#"{{"version":1,"device":{{"vdev":"net_tap0,iface=cdnic0"}},"mac":null,"ipv4":"192.0.2.2/24","gateway":null,"mtu":{mtu},"paths":{paths},"offloads":"auto","rx_timestamps":false,"descriptors":1024,"eal":{{"file_prefix":"test","extra":[]}}}}"#
        ))
        .unwrap()
    }
}
