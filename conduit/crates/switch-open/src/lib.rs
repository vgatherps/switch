//! Opens a NIC from a network config file and returns a running Stack, in one call.
//!
//! Applications reach this crate as `switch::nic` ([`open`], [`Sizing`], [`Opened`],
//! [`OpenError`], [`OpenReport`], [`Link`] and [`offloads`]). `switch-nic`, `switch-bench` and
//! the junction daemon use it the same way.

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
/// above that mark in practice.
const RX_POOL: u32 = 32767;

/// How many sockets the Stack has room for and how the packet-buffer pools are sized.
///
/// [`open`] builds three pools from the config file and this value. A pool is a fixed set
/// of packet buffers made once when the port opens; nothing is allocated per packet later.
/// Each pool is a reserve, which the Stack and the NIC driver need whatever the sockets do,
/// plus a share, which every socket and the application draw on together. Nothing is
/// reserved per socket, so [`add_tcp`](dpdk_smoltcp::Stack::add_tcp) never fails because a
/// pool is small; a small pool shows up later as backpressure (table below).
///
/// | Pool | Holds | Size with `open` |
/// |---|---|---|
/// | RX | received packets, until the application drops the frame or datagram that owns each one | 32767 buffers, always |
/// | TX | packets the Stack builds itself (ACKs, ARP, resets) and TCP segments sent by copy | [`tx_reserve`]`(TX descriptors, tx_pending)` |
/// | frame | data the application sends, held until the peer acknowledges it (TCP) or the NIC has sent it (UDP) | [`frame_reserve`]`(TX descriptors, tx_pending) + frame_share` |
///
/// The descriptor counts are the config's `descriptors` after the driver's adjustment,
/// shown on the report's `descriptors` line. Each RX buffer has a data room of DPDK's
/// headroom (128 bytes) plus `(mtu + 26)` rounded up to a multiple of 1024; TX and frame
/// buffers hold headroom plus `mtu + 14`. The RX burst is 128 packets, the TX burst 32 and
/// every pool cache 8.
///
/// Every field is required. [`max_sockets`](Self::max_sockets), [`rx_staging`](Self::rx_staging),
/// [`tx_pending`](Self::tx_pending) and [`neighbor_capacity`](Self::neighbor_capacity) must
/// be positive; a zero fails `open` with [`OpenError::Stack`] carrying
/// [`StackError::InvalidConfig`](dpdk_smoltcp::StackError::InvalidConfig). The fixed RX pool
/// must cover [`rx_reserve`](dpdk_smoltcp::rx_reserve), which is
/// `2 × max(rx descriptors, 32) + 128 + rx_staging + 12`; beyond that (about 16 000
/// descriptors) `open` fails with `RxPoolTooSmall`.
///
/// # When a pool runs dry
///
/// Nothing fails; each pool produces its own backpressure.
///
/// | Pool | What happens | What frees it |
/// |---|---|---|
/// | RX | The driver has no buffer for arrivals and drops them. TCP peers retransmit; UDP senders are not told. | Reading queued data and dropping received frames the application holds. |
/// | TX | Sockets that want to send wait for a later send step; replies built while receiving are dropped. | The driver finishing earlier sends. |
/// | frame | `send_slice` queues what fits, possibly `Ok(0)`; [`Stack::tx_frame`](dpdk_smoltcp::Stack::tx_frame) returns `None`; [`Stack::udp_send`](dpdk_smoltcp::Stack::udp_send) returns `WouldBlock`. | Peers acknowledging TCP data, the application dropping frames, the driver finishing UDP sends. |
///
/// Nothing signals when frames return: retry on a later pass of the loop.
/// [`Stack::free_frames`](dpdk_smoltcp::Stack::free_frames) says how many are free now.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Sizing {
    /// Frames all TCP send queues and the application may hold at once, on top of the frame
    /// pool's reserve.
    ///
    /// A TCP socket holds up to its `tx_capacity` bytes in frames: queued data plus data sent
    /// and not yet acknowledged. [`tcp::tx_mbuf_budget`](dpdk_smoltcp::smoltcp::socket::tcp::tx_mbuf_budget)`(&config, mtu)`
    /// gives that count when every frame is a full segment. Sum it over the sockets that
    /// may be sending at the same time and add the frames the application holds itself
    /// (from [`Stack::tx_frame`](dpdk_smoltcp::Stack::tx_frame)). Small writes use more
    /// frames for the same bytes. A one-socket client uses one socket's budget.
    pub frame_share: usize,
    /// TCP socket slots. Must be positive. A socket whose handle was dropped keeps its slot
    /// until its connection has closed.
    pub max_sockets: usize,
    /// UDP socket slots. Zero disables UDP.
    pub max_udp_sockets: usize,
    /// Received packets the Stack may hold between taking them from the NIC and processing
    /// them. Must be positive. Each receive step processes at most its `ingress_budget`
    /// packets; the rest wait here. Counts towards the RX reserve. 32 suits one or a few
    /// sockets; the benchmark server and the junction daemon use 128.
    pub rx_staging: usize,
    /// Packets built but not yet accepted by the NIC driver. Must be positive. While it is
    /// full, a send step can only flush and UDP sends return `WouldBlock`. Counts towards
    /// the TX and frame reserves. The examples use 32; the benchmark server and the daemon 64.
    pub tx_pending: usize,
    /// ARP cache entries: how many on-link neighbors, the gateway included, the Stack
    /// remembers. Must be positive. When full, a new neighbor evicts the one that expires
    /// first. [`DEFAULT_NEIGHBOR_CAPACITY`](dpdk_smoltcp::DEFAULT_NEIGHBOR_CAPACITY) is 256.
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

/// The port's link state as the NIC driver reported it when [`open`] stopped waiting.
/// Shown on the report's `link_speed_mbps`, `link_duplex` and `link_status` lines.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Link {
    /// Link speed in Mbit/s, as the driver reports it.
    pub speed_mbps: u32,
    /// Full duplex (`true`) or half duplex.
    pub full_duplex: bool,
    /// Whether the link is up. Always `true` in a returned [`OpenReport`]; a link still down
    /// after ten seconds fails `open` with [`OpenError::LinkDown`].
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
