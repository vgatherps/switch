use std::ffi::{CStr, CString};

use switch_buffer::{
    ensure_eal_init, FramePool, PoolConfig, PoolCreateError, PoolManager, FRAME_DATA_OFF,
};
use switch_dpdk_sys::{
    dpdk_raw_pktmbuf_free, switch_get_pool_cache, switch_mempool_ops_name, switch_pktmbuf_free_seg,
    RTE_PKTMBUF_HEADROOM,
};

mod common;
use common::assert_returned_exactly_once;

fn config(name: &CStr) -> PoolConfig<'_> {
    PoolConfig {
        name,
        count: 15,
        cache_size: 4,
        data_room_size: RTE_PKTMBUF_HEADROOM as u16 + 256,
        socket_id: -1,
    }
}

/// `new_shared` builds the production pool shape and allows `share` on direct,
/// single-segment mbufs: each object returns once, when its last holder frees
/// it through switch's refcount-aware single-segment free or DPDK's standard free
/// (a PMD's completion), in either order.
#[switch_test_support::pinned_test]
fn shared_pool_allows_share_and_frees_on_last_reference() {
    ensure_eal_init();
    let name = CString::new("shared_pool").unwrap();
    let cfg = config(&name);
    assert_eq!(
        unsafe { PoolManager::new_shared(PoolConfig { cache_size: 0, ..cfg }) }.err(),
        Some(PoolCreateError::CacheDisabled)
    );
    let pool = unsafe { PoolManager::new_shared(cfg) }.unwrap();
    unsafe {
        assert_eq!((*pool.pool()).size, cfg.count);
        assert_eq!(CStr::from_ptr(switch_mempool_ops_name(pool.pool())), c"ring_sp_sc");
        assert_eq!(switch_get_pool_cache(pool.pool()), pool.cache());
    }

    for free_seg_first in [true, false] {
        let mut first = pool.alloc_with_len(32).unwrap();
        first.data_mut().fill(0x5a);
        let second = first.share();
        assert_eq!((first.refcount(), first.nb_segs()), (2, 1));
        let (a, b) = (first.into_raw().as_ptr(), second.into_raw().as_ptr());
        assert_eq!(a, b);
        type Free = unsafe extern "C" fn(*mut switch_dpdk_sys::rte_mbuf);
        let (free_a, free_b): (Free, Free) = if free_seg_first {
            (switch_pktmbuf_free_seg, dpdk_raw_pktmbuf_free)
        } else {
            (dpdk_raw_pktmbuf_free, switch_pktmbuf_free_seg)
        };
        unsafe {
            free_a(a);
            assert_eq!(pool.in_use_count(), 1);
            assert_eq!((*b).refcnt(), 1);
            let payload = std::slice::from_raw_parts((*b).data_ptr(), usize::from((*b).data_len()));
            assert!(payload.iter().all(|&byte| byte == 0x5a));
            free_b(b);
        }
        assert_eq!(pool.in_use_count(), 0);
    }

    assert_returned_exactly_once(&pool, cfg.count);
    drop(pool);
    // A clean drop releases the name.
    drop(unsafe { PoolManager::new_shared(cfg) }.unwrap());
}

/// On the owner lcore, every frame not held by a caller is free: the count follows
/// allocations and frees down to zero and back, with frees landing in this lcore's cache.
#[switch_test_support::pinned_test]
fn frame_pool_free_count_follows_held_frames() {
    ensure_eal_init();
    let name = CString::new("frame_free_count").unwrap();
    let cfg = PoolConfig {
        data_room_size: FRAME_DATA_OFF + 64,
        ..config(&name)
    };
    let frames = unsafe { FramePool::new(cfg, 64) }.unwrap();
    assert_eq!(frames.free_count(), frames.count());

    let mut held: Vec<_> = (0..frames.count()).map(|_| frames.alloc().unwrap()).collect();
    assert_eq!(frames.free_count(), 0);
    assert!(frames.alloc().is_none());
    for (freed, frame) in held.drain(..5).enumerate() {
        unsafe { switch_pktmbuf_free_seg(frame.as_ptr()) };
        assert_eq!(frames.free_count(), freed + 1);
    }
    for frame in held {
        unsafe { switch_pktmbuf_free_seg(frame.as_ptr()) };
    }
    assert_eq!(frames.free_count(), frames.count());
}
