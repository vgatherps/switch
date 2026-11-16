//! One call from a network config file to a running Stack on a NIC the process owns. Programs that drive a card themselves (directly or through `switch_net::Net::new`) and the junction daemon open it with [`open`].
//!
//! [`open`] takes the loaded [`switch_netconf::Config`], a [`Sizing`] (socket slots and packet-buffer shares), optional helper CPUs for DPDK's own threads, and the clock the caller will drive the Stack with. In order it checks that the calling thread may run on exactly one CPU, starts DPDK's EAL with that CPU for its main thread, finds the device, checks a PCI card's MAC against the config, chooses offloads (module [`offloads`]), opens the NIC port with its pools, waits up to ten seconds for link, builds the Stack, and, if the config has a gateway, resolves it by ARP for up to three seconds. It returns [`Opened`]: the `stack`, the `eal` handle (keep it until the port is closed) and an [`OpenReport`] whose `Display` prints one `key: value` line per fact. [`OpenError`] names the failing step; errors after the port exists carry the port or the Stack so the caller can close it with `try_close`.
//!
//! `open` is `unsafe` because DPDK and the card write to memory the compiler cannot see. In practice the contract is: call it once per process, from the thread that will drive the Stack, for a device that EAL creates or finds from the config and nothing else has configured; keep every Stack, socket and packet buffer on that thread; keep `eal` alive until the port is closed. The `# Safety` section of [`open`] states the full contract.
//!
//! ```rust,no_run
//! use std::path::Path;
//!
//! use dpdk_smoltcp::DEFAULT_NEIGHBOR_CAPACITY;
//! use dpdk_smoltcp::smoltcp::socket::tcp;
//! use dpdk_smoltcp::smoltcp::time::Instant;
//! use dpdk_smoltcp::smoltcp::wire::Ipv4Address;
//!
//! fn setup() -> Result<(), Box<dyn std::error::Error>> {
//!     let config = switch_netconf::Config::load(Path::new("dut.json"))?;
//!     let server = Ipv4Address::new(192, 0, 2, 1);
//!     let tcp_config = tcp::Config {
//!         rx_capacity: 65536,
//!         tx_capacity: 65536,
//!         mss_cap: Some(config.path_mtu(server) - 40),
//!     };
//!     let sizing = switch_open::Sizing {
//!         frame_share: tcp::tx_mbuf_budget(&tcp_config, usize::from(config.mtu)),
//!         max_sockets: 1,
//!         max_udp_sockets: 0,
//!         rx_staging: 32,
//!         tx_pending: 32,
//!         neighbor_capacity: DEFAULT_NEIGHBOR_CAPACITY,
//!     };
//!     let start = std::time::Instant::now();
//!     let mut clock = || Instant::from_micros(i64::try_from(start.elapsed().as_micros()).unwrap());
//!     let opened = unsafe { switch_open::open(&config, &sizing, None, &mut clock) }?;
//!     println!("{}", opened.report);
//!     let (mut stack, _eal) = (opened.stack, opened.eal);
//!     let handle = stack.add_tcp(tcp_config)?;
//!     stack.connect(&handle, (server, 7000), 40000)?;
//!     // then the drive loop; keep `_eal` alive until the Port is closed
//!     Ok(())
//! }
//! ```
//!
//! See the switch book, chapter "Opening the NIC from code".

use std::thread;
use std::time::{Duration, Instant};

use dpdk_smoltcp::{frame_reserve, tx_reserve, Offloads, PortConfig};
use switch_buffer::PoolConfig;
use switch_dpdk_sys::{
    rte_eth_dev_adjust_nb_rx_tx_desc, rte_eth_link, rte_eth_link_get_nowait, RTE_PKTMBUF_HEADROOM,
};

pub mod offloads;
mod open;
#[cfg(target_os = "linux")]
mod eal_affinity;

pub use open::{open, OpenError, OpenReport, Opened};

const CACHE: u32 = 8;
const RX_BURST: u16 = 128;
/// RX pool mbufs: 2^15 - 1 as DPDK prefers. Below the pool's low mark the stack frees
/// frames TCP sockets hold past a gap (out-of-order data); this many mbufs keeps the pool
/// above that mark in practice. 82 MiB of 2 MiB hugepages at MTU 1500, 306 MiB at MTU 9001.
const RX_POOL: u32 = 32767;

/// What the application needs from the Port and Stack.
///
/// Every field is required. The RX pool holds `RX_POOL` (32767) mbufs, which all sockets
/// share; `Stack::new` refuses it if it is below `rx_reserve` over the adjusted RX
/// descriptors, the RX burst and `rx_staging`. The frame pool holds `frame_reserve` over
/// the adjusted TX descriptors and `tx_pending` plus `frame_share`. Every socket draws on
/// the shares; none is reserved per socket.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Sizing {
    /// Frames all TCP send queues and the application (`Stack::tx_frame`) may hold
    /// at once. `tcp::tx_mbuf_budget(&config, mtu)` estimates a socket's needs for
    /// full-sized segments; smaller writes can use more frames. When the pool runs
    /// out, sends queue nothing until frames return.
    pub frame_share: usize,
    /// `StackConfig::max_sockets`: TCP socket slots. Must be positive.
    pub max_sockets: usize,
    /// `StackConfig::max_udp_sockets`: UDP socket slots. Zero disables UDP.
    pub max_udp_sockets: usize,
    /// `StackConfig::rx_staging`: received packets held before processing.
    /// Also counted in `rx_reserve`.
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
    // Stack::new refuses an RX pool below rx_reserve, a TX pool below tx_reserve
    // (a NIC PMD may hold a full TX ring of sent mbufs until it reclaims them)
    // and a frame pool below frame_reserve.
    let tx_count = u32::try_from(tx_reserve(tx_desc, sizing.tx_pending))
        .map_err(|_| "TX pool count exceeds u32")?;
    let frame_count = u32::try_from(
        frame_reserve(tx_desc, sizing.tx_pending)
            .checked_add(sizing.frame_share)
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
        rx_burst: RX_BURST,
        tx_burst: 32,
        queue_socket: -1,
        rx_pool: pool(c"nic_rx", RX_POOL, rx_room)?,
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
    use dpdk_smoltcp::rx_reserve;

    #[test]
    fn pools_follow_the_adjusted_descriptor_counts() {
        for (adjusted, sizing) in [
            (
                (1024, 1024),
                Sizing {
                    frame_share: 0,
                    max_sockets: 1,
                    max_udp_sockets: 0,
                    rx_staging: 128,
                    tx_pending: 32,
                    neighbor_capacity: 256,
                },
            ),
            (
                (127, 128),
                Sizing {
                    frame_share: 0,
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
                    frame_share: 2880,
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
            assert_eq!(config.rx_burst, 128);
            assert_eq!(config.tx_burst, 32);
            let held = u32::from(adjusted.1) + u32::try_from(sizing.tx_pending).unwrap();
            assert_eq!(config.tx_pool.count, held);
            assert_eq!(config.frame_pool.count, held + 1 + u32::try_from(sizing.frame_share).unwrap());
            // Stack::new accepts the RX pool at every geometry.
            let reserve = rx_reserve(adjusted.0, config.rx_burst, sizing.rx_staging, CACHE);
            assert!(config.rx_pool.count as usize >= reserve, "RX pool below {reserve}");
        }
    }
}
