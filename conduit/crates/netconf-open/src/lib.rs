//! Opens a zhida `Stack` from a `zhida-netconf` config file with one call to [`open`].
//!
//! `docs/USAGE.md` in the repository explains setup, sizing and the drive loop.
//! `examples/tcp_client.rs` is a complete program built on this crate.

use std::thread;
use std::time::{Duration, Instant};

use dpdk_smoltcp::{frame_reserve, rx_reserve, tx_reserve, Offloads, PortConfig};
use zhida_buffer::PoolConfig;
use zhida_dpdk_sys::{
    rte_eth_dev_adjust_nb_rx_tx_desc, rte_eth_link, rte_eth_link_get_nowait, RTE_PKTMBUF_HEADROOM,
};

pub mod offloads;
mod open;

pub use open::{open, OpenError, OpenReport, Opened};

const CACHE: u32 = 8;

/// What the application needs from the Port and Stack.
///
/// Every field is required. The RX pool holds the driver's working space
/// (`rx_reserve` over the adjusted RX descriptors and `rx_staging`) plus
/// `tcp_rx_frames` plus `udp_rx_datagrams`. The frame pool holds
/// `frame_reserve` over the adjusted TX descriptors and `tx_pending` plus
/// `tcp_tx_frames`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Sizing {
    /// RX mbufs for TCP receive data: the sum of `tcp::rx_mbuf_budget(&config, mtu)`
    /// over every TCP config the application will add, including its MSS cap.
    /// `Stack::add_tcp` refuses a config that does
    /// not fit.
    pub tcp_rx_frames: usize,
    /// Frame-pool mbufs for TCP send queues and application-held TX frames:
    /// the sum of `tcp::tx_mbuf_budget(&config, mtu)` over every TCP config
    /// the application will add, plus the frames it holds itself at once
    /// (`Stack::tx_frame`). `Stack::add_tcp` refuses a config that does not fit.
    pub tcp_tx_frames: usize,
    /// RX mbufs for UDP receive data: the sum of `UdpConfig::rx_queue` over
    /// every UDP socket the application will add.
    pub udp_rx_datagrams: usize,
    /// `StackConfig::max_sockets`: TCP socket slots. Must be positive.
    pub max_sockets: usize,
    /// `StackConfig::max_udp_sockets`: UDP socket slots. Zero disables UDP.
    pub max_udp_sockets: usize,
    /// `StackConfig::rx_staging`: received packets held before processing.
    /// Also counted in the RX pool.
    pub rx_staging: usize,
    /// `StackConfig::tx_pending`: frames built but not yet accepted by the
    /// PMD. Also counted in the TX pool and the frame pool.
    pub tx_pending: usize,
    /// `StackConfig::neighbor_capacity`: ARP cache entries.
    /// `dpdk_smoltcp::DEFAULT_NEIGHBOR_CAPACITY` is 256.
    pub neighbor_capacity: usize,
}

/// Return RX and TX descriptor counts adjusted by the PMD.
fn adjusted_descriptors(port_id: u16, requested: u16) -> Result<(u16, u16), String> {
    let (mut rx, mut tx) = (requested, requested);
    // SAFETY: EAL has initialized this ethdev; rx and tx are writable.
    let rc = unsafe { rte_eth_dev_adjust_nb_rx_tx_desc(port_id, &mut rx, &mut tx) };
    if rc != 0 {
        return Err(format!("rte_eth_dev_adjust_nb_rx_tx_desc: {rc}"));
    }
    Ok((rx, tx))
}

/// Build the Port configuration and its RX and TX pools.
fn port_config(
    port_id: u16,
    mtu: u16,
    offloads: Offloads,
    (rx_desc, tx_desc): (u16, u16),
    sizing: &Sizing,
) -> Result<PortConfig<'static>, String> {
    let reserve = rx_reserve(rx_desc, sizing.rx_staging, CACHE)
        .checked_add(sizing.udp_rx_datagrams)
        .ok_or("RX reserve overflow")?;
    let rx_count = u32::try_from(
        reserve
            .checked_add(sizing.tcp_rx_frames)
            .ok_or("RX pool count exceeds u32")?,
    )
    .map_err(|_| "RX pool count exceeds u32")?;
    // Stack::new refuses a TX pool below tx_reserve (a NIC PMD may hold a full
    // TX ring of sent mbufs until it reclaims them) and a frame pool below
    // frame_reserve.
    let tx_count = u32::try_from(tx_reserve(tx_desc, sizing.tx_pending))
        .map_err(|_| "TX pool count exceeds u32")?;
    let frame_count = u32::try_from(
        frame_reserve(tx_desc, sizing.tx_pending)
            .checked_add(sizing.tcp_tx_frames)
            .ok_or("frame pool count exceeds u32")?,
    )
    .map_err(|_| "frame pool count exceeds u32")?;
    let headroom = RTE_PKTMBUF_HEADROOM;
    let rx_room = headroom + (u32::from(mtu) + 26).div_ceil(1024) * 1024;
    let tx_room = headroom + u32::from(mtu) + 14;
    let pool = |name, count, room| -> Result<PoolConfig<'static>, String> {
        Ok(PoolConfig {
            name,
            count,
            cache_size: CACHE,
            data_room_size: u16::try_from(room).map_err(|_| "pool data room exceeds u16")?,
            socket_id: -1,
        })
    };
    Ok(PortConfig {
        port_id,
        mtu,
        rx_descriptors: rx_desc,
        tx_descriptors: tx_desc,
        rx_burst: 32,
        tx_burst: 32,
        queue_socket: -1,
        rx_pool: pool(c"nic_rx", rx_count, rx_room)?,
        tx_pool: pool(c"nic_tx", tx_count, tx_room)?,
        frame_pool: pool(c"nic_frame", frame_count, tx_room)?,
        offloads,
    })
}

/// The current physical link state reported by DPDK.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Link {
    pub speed_mbps: u32,
    pub full_duplex: bool,
    pub up: bool,
}

/// Wait up to ten seconds for the port link, then return its last state.
fn wait_link(port_id: u16) -> Result<Link, String> {
    let start = Instant::now();
    loop {
        // SAFETY: the Port is open; link is a writable DPDK output.
        let mut link: rte_eth_link = unsafe { std::mem::zeroed() };
        let rc = unsafe { rte_eth_link_get_nowait(port_id, &mut link) };
        if rc != 0 {
            return Err(format!("rte_eth_link_get_nowait: {rc}"));
        }
        // SAFETY: rte_eth_link's union is the bitfield view DPDK wrote.
        let fields = unsafe { link.__bindgen_anon_1.__bindgen_anon_1 };
        let state = Link {
            speed_mbps: fields.link_speed,
            full_duplex: fields.link_duplex() != 0,
            up: fields.link_status() != 0,
        };
        if state.up || start.elapsed() >= Duration::from_secs(10) {
            return Ok(state);
        }
        thread::sleep(Duration::from_millis(100));
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pools_follow_the_adjusted_descriptor_counts() {
        for (adjusted, sizing) in [
            (
                (1024, 1024),
                Sizing {
                    tcp_rx_frames: 0,
                    tcp_tx_frames: 0,
                    udp_rx_datagrams: 0,
                    max_sockets: 1,
                    max_udp_sockets: 0,
                    rx_staging: 32,
                    tx_pending: 32,
                    neighbor_capacity: 256,
                },
            ),
            (
                (127, 128),
                Sizing {
                    tcp_rx_frames: 0,
                    tcp_tx_frames: 0,
                    udp_rx_datagrams: 0,
                    max_sockets: 1,
                    max_udp_sockets: 0,
                    rx_staging: 32,
                    tx_pending: 32,
                    neighbor_capacity: 256,
                },
            ),
            (
                (4096, 4096),
                Sizing {
                    tcp_rx_frames: 2880,
                    tcp_tx_frames: 2880,
                    udp_rx_datagrams: 32,
                    max_sockets: 64,
                    max_udp_sockets: 1,
                    rx_staging: 32,
                    tx_pending: 64,
                    neighbor_capacity: 256,
                },
            ),
        ] {
            let config = port_config(0, 1500, Offloads::default(), adjusted, &sizing).unwrap();
            assert_eq!((config.rx_descriptors, config.tx_descriptors), adjusted);
            let held = u32::from(adjusted.1) + u32::try_from(sizing.tx_pending).unwrap();
            assert_eq!(config.tx_pool.count, held);
            assert_eq!(config.frame_pool.count, held + 1 + u32::try_from(sizing.tcp_tx_frames).unwrap());
        }
    }
}
