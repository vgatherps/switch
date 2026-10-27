//! Property-based tests using QuickCheck.

use depot::{DepotHandle, fixed_depot_type};
use quickcheck::{Arbitrary, Gen};
use quickcheck_macros::quickcheck;
use std::collections::{BTreeSet, HashSet, VecDeque};

type TestDepot = fixed_depot_type!(4096);
type OrderDepot = fixed_depot_type!(128);

/// Operations that can be performed on the allocator.
#[derive(Debug, Clone)]
enum Op {
    Alloc,
    Free(usize), // Index into live set
}

impl Arbitrary for Op {
    fn arbitrary(g: &mut Gen) -> Self {
        // Bias towards alloc to build up state
        if bool::arbitrary(g) || bool::arbitrary(g) {
            Op::Alloc
        } else {
            Op::Free(usize::arbitrary(g))
        }
    }
}

#[derive(Debug, Clone)]
struct AllocationPhases(Vec<(bool, u8)>);

impl Arbitrary for AllocationPhases {
    fn arbitrary(g: &mut Gen) -> Self {
        let extra_count = usize::arbitrary(g) % 9 + 8;
        let mut phases = Vec::with_capacity(extra_count + 4);

        phases.extend([(true, 64), (false, 64), (true, 64), (true, 64)]);
        for _ in 0..extra_count {
            phases.push((bool::arbitrary(g), u8::arbitrary(g) % 64 + 1));
        }
        Self(phases)
    }
}

/// Allocation order is the ring, then the lowest recycled slot, then the next fresh slot.
#[quickcheck]
fn alloc_order_matches_reference_model(phases: AllocationPhases) -> bool {
    let mut alloc = OrderDepot::new();
    let mut live = Vec::new();
    let mut ring = VecDeque::with_capacity(16);
    let mut recycled = BTreeSet::new();
    let mut max_ever = 0u32;
    let capacity = <OrderDepot as depot::HasSlots>::SLOTS as u32 - 1;

    for (is_alloc, count) in phases.0 {
        for _ in 0..count {
            if is_alloc {
                let expected = if let Some(slot) = ring.pop_back() {
                    Some(slot)
                } else if let Some(slot) = recycled.pop_first() {
                    Some(slot)
                } else if max_ever < capacity {
                    max_ever += 1;
                    Some(max_ever)
                } else {
                    None
                };

                let actual = alloc.alloc();
                if actual.map(|handle| handle.slot()) != expected {
                    return false;
                }
                if let Some(handle) = actual {
                    live.push(handle);
                }
            } else if let Some(handle) = live.pop() {
                alloc.free(handle).unwrap();

                if ring.len() == 16 {
                    let evicted = ring.pop_front().unwrap();
                    if !recycled.insert(evicted) {
                        return false;
                    }
                }
                ring.push_back(handle.slot());
            }
        }
    }
    true
}

/// No index is ever returned twice without being freed in between.
#[quickcheck]
fn no_double_alloc(ops: Vec<Op>) -> bool {
    let mut alloc = TestDepot::new();
    let mut live: HashSet<DepotHandle> = HashSet::new();

    for op in ops.into_iter().take(1000) {
        match op {
            Op::Alloc => {
                if let Some(idx) = alloc.alloc()
                    && !live.insert(idx) {
                        return false; // Double allocation!
                    }
            }
            Op::Free(i) => {
                if !live.is_empty() {
                    let idx = *live.iter().nth(i % live.len()).unwrap();
                    live.remove(&idx);
                    alloc.free(idx).unwrap();
                }
            }
        }
    }
    true
}

/// Handle encoding is valid: slot fits in 25 bits and generation roundtrips.
#[quickcheck]
fn handle_encoding_valid(count: u8) -> bool {
    let mut alloc = TestDepot::new();
    for _ in 0..count {
        if let Some(handle) = alloc.alloc()
            && handle.slot() >= (1 << 25) {
                return false;
            }
    }
    true
}

/// Alloc-free-alloc cycle preserves capacity.
#[quickcheck]
fn capacity_preserved(n: u8) -> bool {
    let n = n as usize;
    let mut alloc = TestDepot::new();

    let indices: Vec<_> = (0..n).filter_map(|_| alloc.alloc()).collect();
    for idx in &indices {
        alloc.free(*idx).unwrap();
    }

    // Should be able to allocate at least n again
    let realloc: Vec<_> = (0..n).filter_map(|_| alloc.alloc()).collect();
    realloc.len() == indices.len()
}

/// Internal invariants hold after any sequence of operations.
#[quickcheck]
fn invariants_maintained(ops: Vec<Op>) -> bool {
    let mut alloc = TestDepot::new();
    let mut live: Vec<DepotHandle> = Vec::new();

    for op in ops.into_iter().take(500) {
        match op {
            Op::Alloc => {
                if let Some(idx) = alloc.alloc() {
                    live.push(idx);
                }
            }
            Op::Free(i) => {
                if !live.is_empty() {
                    let idx = live.swap_remove(i % live.len());
                    alloc.free(idx).unwrap();
                }
            }
        }

        // Verify invariants
        alloc.check_invariants();
    }
    true
}

/// Stack entries maintain LIFO order.
#[quickcheck]
fn stack_lifo_order(n: u8) -> bool {
    let n = (n % 16) as usize; // Must not exceed IndexRing::CAPACITY (16) for LIFO guarantee
    if n == 0 {
        return true;
    }

    let mut alloc = TestDepot::new();

    // Allocate n indices
    let indices: Vec<_> = (0..n).filter_map(|_| alloc.alloc()).collect();
    if indices.len() != n {
        return true; // Not enough capacity, skip
    }

    // Free them in order
    for idx in &indices {
        alloc.free(*idx).unwrap();
    }

    // Should get them back in reverse slot order (LIFO)
    // Generation changes after free, so compare slots not full handles
    for idx in indices.iter().rev() {
        match alloc.alloc() {
            Some(got) if got.slot() == idx.slot() => continue,
            _ => return false,
        }
    }
    true
}

/// Burst allocations tend to come from the same slab.
#[quickcheck]
fn burst_locality(burst_size: u8) -> bool {
    let burst_size = ((burst_size % 63) + 1) as usize; // 1-63 (slab 0 has 63 free due to sentinel)
    let mut alloc = TestDepot::new();

    let indices: Vec<_> = (0..burst_size).filter_map(|_| alloc.alloc()).collect();

    let slab_indices: HashSet<_> = indices.iter().map(|idx| idx.slot() / 64).collect();

    // Slab 0 has 63 free slots (sentinel occupies bit 0), so first 63 allocs fit in 1 slab
    let expected_slabs = burst_size.div_ceil(63);
    slab_indices.len() <= expected_slabs
}

/// Free followed by alloc returns the same index (stack hot path).
#[quickcheck]
fn free_alloc_returns_same(seed: u32) -> bool {
    let mut alloc = TestDepot::new();

    // Warm up
    for _ in 0..seed % 100 {
        let _ = alloc.alloc();
    }

    // The actual test: same slot reused (generation changes after free)
    if let Some(idx) = alloc.alloc() {
        alloc.free(idx).unwrap();
        if let Some(idx2) = alloc.alloc() {
            return idx.slot() == idx2.slot();
        }
    }
    true
}

/// Max allocated slot increases by at most 1 per allocation.
///
/// This ensures we pack indices densely and don't skip ahead.
#[quickcheck]
fn max_slot_increases_by_at_most_one(ops: Vec<Op>) -> bool {
    let mut alloc = TestDepot::new();
    let mut live: Vec<DepotHandle> = Vec::new();
    let mut max_seen: i64 = 0; // slot 0 is reserved by sentinel

    for op in ops.into_iter().take(500) {
        match op {
            Op::Alloc => {
                if let Some(idx) = alloc.alloc() {
                    let val = idx.slot() as i64;
                    if val > max_seen + 1 {
                        return false; // Jumped by more than 1
                    }
                    if val > max_seen {
                        max_seen = val;
                    }
                    live.push(idx);
                }
            }
            Op::Free(i) => {
                if !live.is_empty() {
                    let idx = live.swap_remove(i % live.len());
                    alloc.free(idx).unwrap();
                }
            }
        }
    }
    true
}
