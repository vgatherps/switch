//! DPDK packet buffers and the pools they come from, as used under `dpdk_smoltcp::Port`.
//!
//! A packet buffer (DPDK's mbuf) is a fixed-size block of memory the NIC reads from and writes to directly. Buffers come from pools made once when a NIC port opens. This crate wraps DPDK's buffers ([`Mbuf`]) and pools ([`PoolManager`], and [`FramePool`] for the buffers an application fills in place and a stack sends with its headers written in front of the payload). It is the layer `dpdk-smoltcp` is built on.
//!
//! Applications see one type from this crate: [`PoolConfig`], which describes one pool (name, buffer count, per-CPU cache size, data room per buffer including DPDK's headroom, and NUMA node) inside a `dpdk_smoltcp::PortConfig`. A pool that cannot be created is reported as [`PoolCreateError`] inside `dpdk_smoltcp::PortError::Pool`. Everything else here is used by `dpdk-smoltcp` itself; the pools a `Port` creates hold single-segment buffers that are never chained, cloned or attached indirectly, and only the frame pool's buffers may be shared.
//!
//! See the switch book, chapter "Packet-buffer pools", for what the three pools hold and how to size them.

mod dpdk;
mod frame;

#[cfg(feature = "testing")]
pub mod testing;

pub use dpdk::{Mbuf, PoolConfig, PoolCreateError, PoolManager, RTE_MBUF_F_INDIRECT, SegmentIter};
pub use frame::{FramePool, FRAME_DATA_OFF};
#[cfg(feature = "testing")]
pub use dpdk::{TestPoolGuard, acquire_test_pool, ensure_eal_init};
