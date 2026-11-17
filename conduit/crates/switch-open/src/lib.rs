//! Opens a network card from a network config file and returns the network stack running on
//! it, in one call.
//!
//! Applications reach this crate as `switch::nic` ([`open`], [`Sizing`], [`Opened`],
//! [`OpenError`], [`OpenReport`], [`Link`] and [`offloads`]). `switch-nic`, `switch-bench` and
//! `switch-junctiond` use it the same way.

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

/// How many sockets a program can have open and how many packet buffers it has for sending.
///
/// [`open`] allocates everything this describes once, when the card opens, and none of it
/// grows afterwards. The buffers are shared by all sockets: nothing is set aside per socket,
/// so opening a socket never fails because buffers are short. A value that is too small
/// shows later, as a wait or a refused send; each field says how.
///
/// Every field is required. [`max_sockets`](Self::max_sockets),
/// [`rx_staging`](Self::rx_staging), [`tx_pending`](Self::tx_pending) and
/// [`neighbor_capacity`](Self::neighbor_capacity) must be positive; a zero fails `open` with
/// [`OpenError::Stack`].
///
/// The values the example programs use:
///
/// | Program | `frame_share` | `max_sockets` | `max_udp_sockets` | `rx_staging` | `tx_pending` |
/// |---|---|---|---|---|---|
/// | a client with one TCP connection | `tx_mbuf_budget` for its socket | 1 | 0 | 32 | 32 |
/// | a TCP server with up to 15 connections | 16 × `tx_mbuf_budget` | 16 | 0 | 32 | 32 |
/// | a UDP echo on one port | 32 | 1 | 1 | 32 | 32 |
///
/// # Receive buffers
///
/// Received packets have their own buffers, which `Sizing` does not set: 32767 of them,
/// shared by all sockets. A buffer is in use from the moment a packet arrives until the
/// task drops the frame or datagram that holds it. When none is free the card drops
/// arriving packets: TCP peers retransmit, and UDP datagrams are lost. Tasks free buffers
/// by receiving and dropping frames.
///
/// The card's receive ring takes part of those buffers. The 32767 must cover
/// `2 × max(RX descriptors, 32) + 128 + rx_staging + 12`, where the descriptor count is the
/// config's `descriptors` after the driver has adjusted it (the report's `descriptors`
/// line). With `rx_staging: 32` that allows up to 16297 descriptors, and a larger
/// `rx_staging` allows fewer. Past the limit `open` fails with [`OpenError::Stack`] carrying
/// `RxPoolTooSmall`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Sizing {
    /// Send frames to allocate on top of the network stack's own reserve. Choose enough for
    /// the frames tasks hold from `NetHandle::tx_frame` plus the frames of TCP data that is
    /// queued or sent and not yet acknowledged.
    ///
    /// It is a size, not a limit that is enforced: frames are handed out for as long as the
    /// whole pool has one free. When none is free, `tx_frame().await` waits until one comes
    /// back, and a UDP `send` of a frame that has to be copied returns `Full`. Frames come
    /// back when a peer acknowledges TCP data, when the card has sent a UDP datagram, and
    /// when a task drops a frame.
    ///
    /// To choose it, add up
    /// [`tcp::tx_mbuf_budget`](dpdk_smoltcp::smoltcp::socket::tcp::tx_mbuf_budget)`(&config, mtu)`
    /// for every TCP socket that may be sending at the same time. It estimates the frames
    /// one socket's `tx_capacity` bytes take when every frame is full, and is never less
    /// than six. Add the frames tasks hold while they fill them, usually one per sending
    /// task. A program that sends many small frames needs more, because a small frame uses
    /// a whole buffer.
    pub frame_share: usize,
    /// TCP sockets the program can have at once. Must be positive.
    ///
    /// Count every connection, the backlog of every listener, and connections that
    /// were dropped and are still closing: such a connection keeps its place until TCP has
    /// finished the close. When all places are taken, `tcp_connect` and `tcp_listen` return
    /// `SocketsFull`.
    pub max_sockets: usize,
    /// UDP sockets the program can have at once. With zero, or when all are in use,
    /// `udp_bind` returns `SocketsFull`.
    pub max_udp_sockets: usize,
    /// Received packets that can wait between being taken from the card and being
    /// processed. Must be positive.
    ///
    /// Each receive step processes at most `NetConfig::ingress_budget` packets and leaves
    /// the rest here for the next round. While this is full, no more are taken from the
    /// card, and the card drops arrivals once its own ring (the config's `descriptors`) is
    /// full too. Set it to at least `ingress_budget`. 32 suits a program with a few sockets;
    /// `switch-bench` and `switch-junctiond` use 128.
    pub rx_staging: usize,
    /// Packets that are built and waiting for the card to accept them. Must be positive.
    ///
    /// While this is full, a send step sends nothing new until the card has taken some,
    /// a UDP `send` returns `Full`, and replies the network stack builds while receiving
    /// (acknowledgements, ARP) are dropped. The examples use 32; `switch-bench` and
    /// `switch-junctiond` use 64.
    pub tx_pending: usize,
    /// Addresses on the local network, the gateway included, whose hardware address the
    /// network stack remembers. Must be positive.
    ///
    /// When it is full, a new address replaces the entry that would expire first, and the
    /// replaced one is looked up again when it is next needed.
    /// [`DEFAULT_NEIGHBOR_CAPACITY`](dpdk_smoltcp::DEFAULT_NEIGHBOR_CAPACITY), 256, is enough
    /// unless the program talks to more hosts than that on its own subnet.
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

/// The card's link state as its driver reported it when [`open`] stopped waiting.
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
