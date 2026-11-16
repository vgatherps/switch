//! Tests for shared buffer behavior.
//!
//! Mutation safety model:
//! - share() increments refcount, both views point to same mbuf
//! - data_mut(): requires refcnt==1 AND NOT indirect (panics otherwise)
//! - prepend(): writes in place only into headroom that is exclusively ours
//!   (refcnt==1, and an indirect head the only holder of its backing buffer);
//!   otherwise links a fresh header segment, leaving the old head unchanged
//! - adj/trim: require refcnt==1 (panics otherwise); on an indirect clone they
//!   change only the clone's own header

mod common;

use common::assert_returned_exactly_once;
use switch_buffer::PoolManager;

// DPDK provides RTE_PKTMBUF_HEADROOM (128 bytes)
const DPDK_HEADROOM: u16 = 128;

#[switch_test_support::pinned_test]
fn test_shared_prepend_adds_header_segment() {
    let pool = PoolManager::new(256);
    let mut mbuf = pool.alloc_with_len(100).unwrap();
    mbuf.data_mut().fill(0xAA);
    let old_head = mbuf.as_ptr();

    let clone = mbuf.share();

    mbuf.prepend(20).expect("header segment").fill(0xBB);
    assert_eq!(mbuf.next(), Some(old_head));
    assert_eq!((mbuf.len(), mbuf.pkt_len(), mbuf.nb_segs()), (20, 120, 2));
    assert!(mbuf.data().iter().all(|&b| b == 0xBB));

    // The other holder still sees the unchanged single-segment packet.
    assert_eq!(clone.as_ptr(), old_head);
    assert_eq!((clone.headroom(), clone.len(), clone.pkt_len()), (DPDK_HEADROOM, 100, 100));
    assert_eq!((clone.nb_segs(), clone.refcount()), (1, 2));
    assert!(clone.data().iter().all(|&b| b == 0xAA));

    drop(mbuf);
    assert_eq!(clone.refcount(), 1);
    drop(clone);
    assert_returned_exactly_once(&pool, 256);
}

#[switch_test_support::pinned_test]
fn test_clone_trait_increments_refcount() {
    let pool = PoolManager::new(256);
    let mbuf = pool.alloc_with_len(100).unwrap();
    let clone = mbuf.clone();
    assert_eq!(mbuf.refcount(), 2);
    assert_eq!(clone.refcount(), 2);
    drop((mbuf, clone));
    assert_eq!(pool.in_use_count(), 0);
}

#[switch_test_support::pinned_test]
fn test_insufficient_headroom() {
    let pool = PoolManager::new(256);
    let mut mbuf = pool.alloc_with_len(100).unwrap();
    mbuf.data_mut().fill(0xDD);
    let old_head = mbuf.as_ptr();

    // DPDK has 128 bytes headroom; 150 bytes go into a new header segment.
    mbuf.prepend(150).expect("header segment").fill(0xAA);
    assert_eq!(mbuf.next(), Some(old_head));
    assert_eq!((mbuf.len(), mbuf.pkt_len(), mbuf.nb_segs()), (150, 250, 2));

    // Old head unchanged
    let old = unsafe { &*old_head };
    assert_eq!((old.data_off(), old.data_len(), old.pkt_len()), (DPDK_HEADROOM, 100, 100));
    assert_eq!((old.nb_segs(), old.refcnt()), (1, 1));
    drop(mbuf);
    assert_returned_exactly_once(&pool, 256);
}

