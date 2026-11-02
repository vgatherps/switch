//! The TX frame pool: mbufs that an application fills in place and a stack
//! publishes with its headers written in front of the payload.

use std::ptr::NonNull;

use zhida_dpdk_sys::{rte_mbuf, rte_mempool, RTE_PKTMBUF_HEADROOM};

use crate::{PoolConfig, PoolCreateError, PoolManager};

/// Ethernet, IPv4 and UDP header bytes between the mbuf headroom and a
/// frame's payload.
const UDP_HEADERS: u16 = 42;

/// A frame's payload offset from its mbuf's buffer start. At least 64 bytes
/// precede the payload, for headers up to 64 bytes.
pub const FRAME_DATA_OFF: u16 = RTE_PKTMBUF_HEADROOM as u16 + UDP_HEADERS;

/// A pool of TX frames: direct, single-segment mbufs that may be shared
/// (`PoolManager::new_shared`), each handed out empty with its payload at
/// `FRAME_DATA_OFF`. Dropped with frames still out, its backing is retained
/// (`PoolManager`'s drop).
pub struct FramePool {
    pool: PoolManager,
    /// Payload bytes from `FRAME_DATA_OFF` to the end of a frame's buffer.
    room: u16,
}

impl FramePool {
    /// # Safety
    /// `PoolManager::new_shared`'s contract. `config.data_room_size` holds
    /// `FRAME_DATA_OFF` + `room` bytes. Header bytes in front of a frame's
    /// payload and its metadata are written only while the frame has one
    /// reference, or by the device and the PMD's tx_prepare after publish; the
    /// payload only while the frame has one reference.
    pub unsafe fn new(config: PoolConfig<'_>, room: u16) -> Result<Self, PoolCreateError> {
        debug_assert!(
            u32::from(config.data_room_size) >= u32::from(FRAME_DATA_OFF) + u32::from(room)
        );
        let pool = PoolManager::new_shared(config)?;
        Ok(Self { pool, room })
    }

    /// An empty frame: one reference, payload offset `FRAME_DATA_OFF`, length
    /// 0. `None` when the pool is empty.
    #[inline]
    pub fn alloc(&self) -> Option<NonNull<rte_mbuf>> {
        let mbuf = self.pool.alloc_with_len(0)?.into_raw();
        // SAFETY: a fresh mbuf of this pool, owned only here; its data room
        // holds FRAME_DATA_OFF + room bytes (`new`).
        unsafe { (*mbuf.as_ptr()).set_data_off(FRAME_DATA_OFF) };
        Some(mbuf)
    }

    /// Payload room of every frame.
    pub fn room(&self) -> u16 {
        self.room
    }

    /// The backing mempool, every frame's `pool`.
    pub fn mempool(&self) -> *mut rte_mempool {
        self.pool.pool()
    }

    /// Frames in the pool.
    pub fn count(&self) -> usize {
        // SAFETY: this owner keeps the pool alive; its object count is fixed.
        usize::try_from(unsafe { (*self.mempool()).size }).expect("frame pool count exceeds usize")
    }

    pub fn in_use_count(&self) -> u32 {
        self.pool.in_use_count()
    }

    /// Frames this lcore can allocate now: the backing store plus this lcore's cache
    /// (`PoolManager::free_count`). Frames sitting in other lcores' caches are not counted.
    #[inline]
    pub fn free_count(&self) -> usize {
        self.pool.free_count() as usize
    }
}
