use std::ptr::NonNull;

use smoltcp::phy::{TxAlloc, TxBuf, TxBufOps};
use zhida_buffer::{PoolConfig, PoolCreateError};
use zhida_dpdk_sys::{rte_mbuf, rte_mempool, zhida_pktmbuf_free_seg, RTE_PKTMBUF_HEADROOM};

use crate::eal::PoolResources;

/// Bytes between the mbuf headroom and a frame's payload: Ethernet, IPv4 and
/// UDP headers. TCP's 54 header bytes also fit before the payload.
const UDP_HEADERS: u16 = 42;

/// Payload offset from the mbuf's buffer start. The device writes a frame's
/// headers immediately before it.
pub(crate) const FRAME_DATA_OFF: u16 = RTE_PKTMBUF_HEADROOM as u16 + UDP_HEADERS;

/// The Stack's TX frame pool, held behind an `Rc` by the Port and (later) by
/// each TCP socket's `TxSource`. When the last clone drops, the backing is
/// destroyed if no frame is out, else retained permanently (`PoolResources`).
pub(crate) struct FramePool {
    resources: PoolResources,
    /// The mempool pointer, non-null for this pool's life: `TxAlloc::origin`.
    origin: NonNull<()>,
    /// Payload room: the largest UDP payload, IP MTU − 28.
    room: u16,
}

impl FramePool {
    /// # Safety
    /// `PoolResources::new_frame`'s contract. The data room holds
    /// `RTE_PKTMBUF_HEADROOM` + `mtu` + 14 bytes (checked by `Port::open`).
    pub(crate) unsafe fn new(config: PoolConfig<'_>, mtu: u16) -> Result<Self, PoolCreateError> {
        let resources = unsafe { PoolResources::new_frame(config) }?;
        let origin = NonNull::new(resources.pool().pool())
            .expect("a created pool has a mempool")
            .cast();
        Ok(Self {
            resources,
            origin,
            room: (mtu + 14).saturating_sub(UDP_HEADERS),
        })
    }

    pub(crate) fn mempool(&self) -> *mut rte_mempool {
        self.resources.pool().pool()
    }

    pub(crate) fn count(&self) -> usize {
        // SAFETY: this owner keeps the pool alive; its object count is fixed.
        usize::try_from(unsafe { (*self.mempool()).size }).expect("frame pool count exceeds usize")
    }

    pub(crate) fn in_use_count(&self) -> u32 {
        self.resources.pool().in_use_count()
    }
}

impl TxAlloc for FramePool {
    /// An empty frame whose payload starts `FRAME_DATA_OFF` bytes into the
    /// mbuf's buffer. `None` when the pool is empty.
    fn alloc(&self) -> Option<TxBuf> {
        let mbuf = self.resources.pool().alloc_with_len(0)?.into_raw();
        let raw = mbuf.as_ptr();
        // SAFETY: a fresh mbuf from this pool, owned only here: direct, one
        // segment, refcnt 1, data_len 0. The data room holds FRAME_DATA_OFF +
        // room bytes (`new`), so the payload range lies in its buffer.
        unsafe {
            (*raw).set_data_off(FRAME_DATA_OFF);
            let data = NonNull::new_unchecked((*raw).data_ptr());
            Some(TxBuf::from_raw_parts(
                mbuf.cast(),
                &FRAME_OPS,
                self.origin,
                data,
                usize::from(self.room),
            ))
        }
    }

    fn origin(&self) -> NonNull<()> {
        self.origin
    }
}

/// A frame-pool TxBuf's owner is its mbuf; one TxBuf or pending TxPacket is
/// one refcount on it.
static FRAME_OPS: TxBufOps = TxBufOps {
    release: release_frame,
    share: share_frame,
    unique: unique_frame,
};

unsafe fn release_frame(owner: NonNull<()>) {
    // SAFETY: the caller gives up one reference to this live, direct,
    // single-segment frame-pool mbuf; the last reference returns it.
    unsafe { zhida_pktmbuf_free_seg(owner.cast::<rte_mbuf>().as_ptr()) };
}

unsafe fn share_frame(owner: NonNull<()>) {
    // SAFETY: the caller holds a reference to this live mbuf on its owner thread.
    unsafe { (*owner.cast::<rte_mbuf>().as_ptr()).refcnt_inc() };
}

unsafe fn unique_frame(owner: NonNull<()>) -> bool {
    // SAFETY: the caller holds a reference to this live mbuf on its owner thread.
    unsafe { (*owner.cast::<rte_mbuf>().as_ptr()).refcnt() == 1 }
}
