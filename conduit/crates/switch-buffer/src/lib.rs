//! DPDK packet buffers (mbufs) and the pools they come from: the layer under the Stack's
//! NIC port in `dpdk-smoltcp`.
//!
//! Applications do not name this crate. A program that opens a port by hand describes
//! each pool with [`PoolConfig`] and meets [`PoolCreateError`] inside a port error; the
//! rest ([`Mbuf`], [`PoolManager`], [`FramePool`]) is used by `dpdk-smoltcp` and the test
//! crates.

mod dpdk;
mod frame;

#[cfg(feature = "testing")]
pub mod testing;

pub use dpdk::{Mbuf, PoolConfig, PoolCreateError, PoolManager, RTE_MBUF_F_INDIRECT, SegmentIter};
pub use frame::{FramePool, FRAME_DATA_OFF};
#[cfg(feature = "testing")]
pub use dpdk::{TestPoolGuard, acquire_test_pool, ensure_eal_init};
