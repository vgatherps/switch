use crate::algorithm::depot_algorithm;
use crate::index_ring::IndexRing;
use crate::{DepotHandle, MAX_SLOTS};

/// Runtime-capacity slab allocator using the same allocation algorithm as [`crate::FixedDepot`].
pub struct DynDepot {
    index_stack: IndexRing,
    generations: Box<[u8]>,
    slabs: Box<[u64]>,
    l1: Box<[u64]>,
    l2: Box<[u64]>,
    l3: Box<[u64]>,
    cached: [u32; 4],
    recycled: u32,
    bump: u32,
}

impl DynDepot {
    /// Construct an empty depot. The requested slot count is rounded up to a multiple of 64;
    /// slot 0 remains reserved, so usable capacity is the rounded count minus one.
    ///
    /// # Panics
    /// Panics when `slots` is zero or greater than 2^25.
    pub fn new(slots: usize) -> Self {
        assert!(slots > 0, "DynDepot slot count must be greater than zero");
        assert!(
            slots <= MAX_SLOTS,
            "DynDepot slot count must not exceed 2^25"
        );

        let slots = slots.div_ceil(64) * 64;
        let slabs = slots / 64;
        let l1 = slabs.div_ceil(64);
        let l2 = l1.div_ceil(64);
        let l3 = l2.div_ceil(64);

        Self {
            index_stack: IndexRing::new(),
            generations: vec![0; slots].into_boxed_slice(),
            slabs: vec![0; slabs].into_boxed_slice(),
            l1: vec![0; l1].into_boxed_slice(),
            l2: vec![0; l2].into_boxed_slice(),
            l3: vec![0; l3].into_boxed_slice(),
            cached: [0; 4],
            recycled: 0,
            bump: 0,
        }
    }

    #[inline(always)]
    fn slots(&self) -> usize {
        self.generations.len()
    }

    #[inline(always)]
    fn slab_count(&self) -> usize {
        self.slabs.len()
    }

    #[inline(always)]
    fn l1_count(&self) -> usize {
        self.l1.len()
    }

    #[inline(always)]
    fn l2_count(&self) -> usize {
        self.l2.len()
    }

    #[inline(always)]
    fn l3_count(&self) -> usize {
        self.l3.len()
    }

    depot_algorithm!();
}

impl crate::DepotAlloc for DynDepot {
    #[inline(always)]
    fn alloc(&mut self) -> Option<DepotHandle> {
        DynDepot::alloc(self)
    }

    #[inline(always)]
    fn get_or_sentinel(&self, handle: Option<DepotHandle>) -> Option<u32> {
        DynDepot::get_or_sentinel(self, handle)
    }

    #[inline(always)]
    fn validate(&self, handle: DepotHandle) -> Result<u32, crate::AccessError> {
        DynDepot::validate(self, handle)
    }

    #[inline(always)]
    fn validate_live(&self, handle: DepotHandle) -> Option<u32> {
        DynDepot::validate_live(self, handle)
    }

    #[inline(always)]
    unsafe fn free_unchecked(&mut self, handle: DepotHandle) {
        unsafe { DynDepot::free_unchecked(self, handle) }
    }

    #[inline(always)]
    unsafe fn poison_unchecked(&mut self, handle: DepotHandle) {
        unsafe { DynDepot::poison_unchecked(self, handle) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_boundary_order(requested: usize, boundary_slot: u32) {
        let mut depot = DynDepot::new(requested);
        let mut boundary_handles = Vec::with_capacity(18);

        for _ in 0..boundary_slot + 8 {
            let handle = depot.alloc().unwrap();
            if handle.slot() >= boundary_slot - 9 {
                boundary_handles.push(handle);
            }
        }
        assert_eq!(boundary_handles.len(), 18);
        depot.check_invariants();

        let first_freed = [boundary_handles[0], boundary_handles[17]];
        for handle in first_freed
            .into_iter()
            .chain(boundary_handles[1..17].iter().copied())
        {
            depot.free(handle).unwrap();
            depot.check_invariants();
        }

        for expected_slot in (boundary_slot - 8..=boundary_slot + 7).rev() {
            assert_eq!(depot.alloc().unwrap().slot(), expected_slot);
            depot.check_invariants();
        }
        for expected_slot in [boundary_slot - 9, boundary_slot + 8, boundary_slot + 9] {
            assert_eq!(depot.alloc().unwrap().slot(), expected_slot);
            depot.check_invariants();
        }
    }

    #[test]
    fn new_rounds_capacity_and_reserves_slot_zero() {
        for (requested, usable) in [(63, 63), (64, 63), (65, 127)] {
            let mut depot = DynDepot::new(requested);
            assert_eq!(
                std::iter::from_fn(|| depot.alloc()).count(),
                usable,
                "requested {requested}"
            );
            depot.check_invariants();
        }
    }

    #[test]
    #[should_panic(expected = "greater than zero")]
    fn new_rejects_zero() {
        let _ = DynDepot::new(0);
    }

    #[test]
    #[should_panic(expected = "must not exceed 2^25")]
    fn new_rejects_more_than_handle_capacity() {
        let _ = DynDepot::new((1 << 25) + 1);
    }

    #[test]
    fn allocation_order_across_l1_word_boundary() {
        assert_boundary_order(4_097, 4_096);
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn allocation_order_across_l2_word_boundary() {
        assert_boundary_order(262_145, 262_144);
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn allocation_order_across_l3_word_boundary() {
        assert_boundary_order(16_777_217, 16_777_216);
    }
}
