use crate::index_ring::IndexRing;
use crate::{
    AccessError, BAD_GENERATION, DEAD_BIT, DepotHandle, FIRST_GENERATION, FreeError,
    GENERATION_MASK, MAX_SLOTS,
    encode_handle, next_live_generation,
};

/// Fixed-capacity slab allocator with flat bitmask hierarchy.
///
/// Four levels of 64-bit words track recycled slots:
/// - `slabs` (`SLABS` words): recycled-slot bitmask. Bit set = slot recycled.
/// - `l1` (`AW` words): bit set = corresponding slab has recycled slots.
/// - `l2` (`BW` words): bit set = corresponding L1 word is non-zero.
/// - `l3` (`CW` words): bit set = corresponding L2 word is non-zero.
///
/// `cached[0..4]` holds the current word index at each level (slab, l1, l2, l3).
/// On alloc miss, phases check each level's cached word bottom-up; on success,
/// `descend` refreshes all lower caches from the found word down.
///
/// Use `fixed_depot_type!` to compute const generics from a slot count.
pub struct FixedDepot<
    const SLABS: usize,
    const SLOTS: usize,
    const AW: usize,
    const BW: usize,
    const CW: usize,
> {
    index_stack: IndexRing,
    generations: [u8; SLOTS],
    slabs: [u64; SLABS],
    l1: [u64; AW],
    l2: [u64; BW],
    l3: [u64; CW],
    /// Cached `[slab, l1, l2, l3]` path. Meaningless while `recycled == 0`.
    cached: [u32; 4],
    /// Number of recycled slots represented by set bits in `slabs`.
    recycled: u32,
    /// Highest never-used slot handed out; the next fresh slot is `bump + 1`, and 0 means none yet.
    bump: u32,
}

/// Compute FixedDepot const generics from a slot count.
/// Input is rounded up to next multiple of 64. Usable capacity = rounded_slots - 1 (sentinel).
#[macro_export]
macro_rules! fixed_depot_type {
    ($slots:expr) => {
        $crate::FixedDepot<
            { ($slots as usize).div_ceil(64) },
            { (($slots as usize).div_ceil(64)) * 64 },
            { ($slots as usize).div_ceil(64).div_ceil(64) },
            { ($slots as usize).div_ceil(64).div_ceil(64).div_ceil(64) },
            { ($slots as usize).div_ceil(64).div_ceil(64).div_ceil(64).div_ceil(64) },
        >
    };
}

impl<const SLABS: usize, const SLOTS: usize, const AW: usize, const BW: usize, const CW: usize>
    FixedDepot<SLABS, SLOTS, AW, BW, CW>
{
    /// Check the const-generic shape used by this depot.
    ///
    /// ```
    /// type Depot = depot::fixed_depot_type!(1usize << 25);
    /// const _: () = Depot::validate_config();
    /// ```
    ///
    /// ```compile_fail,E0080
    /// type Depot = depot::fixed_depot_type!((1usize << 25) + 1);
    /// const _: () = Depot::validate_config();
    /// ```
    ///
    /// ```compile_fail,E0080
    /// type Depot = depot::fixed_depot_type!(0);
    /// const _: () = Depot::validate_config();
    /// ```
    pub const fn validate_config() {
        assert!(SLOTS > 0, "SLOTS must be > 0");
        assert!(SLABS * 64 == SLOTS, "SLOTS must equal SLABS * 64");
        assert!(AW * 64 >= SLABS, "AW too small");
        assert!(BW * 64 >= AW, "BW too small");
        assert!(CW * 64 >= BW, "CW too small");
        assert!(SLOTS <= MAX_SLOTS, "SLOTS exceeds maximum (2^25)");
    }

    /// Construct an empty depot.
    ///
    /// ```
    /// type Depot = depot::fixed_depot_type!(64);
    /// let _depot = Depot::new();
    /// ```
    ///
    /// ```compile_fail,E0080
    /// let _depot = depot::FixedDepot::<1, 64, 0, 1, 1>::new();
    /// ```
    pub const fn new() -> Self {
        const { Self::validate_config() };

        Self {
            index_stack: IndexRing::new(),
            generations: [0; SLOTS],
            slabs: [0; SLABS],
            l1: [0; AW],
            l2: [0; BW],
            l3: [0; CW],
            cached: [0; 4],
            recycled: 0,
            bump: 0,
        }
    }

    /// Read a summary-level word. Level 1 = l1, 2 = l2, 3 = l3.
    #[inline(always)]
    fn level_word(&self, level: usize, idx: usize) -> u64 {
        match level {
            1 => unsafe { *self.l1.get_unchecked(idx) },
            2 => unsafe { *self.l2.get_unchecked(idx) },
            3 => unsafe { *self.l3.get_unchecked(idx) },
            _ => unsafe { std::hint::unreachable_unchecked() },
        }
    }

    /// Cascade caches downward from `from` to level 0.
    /// Precondition: `cached[from]` is freshly set and `level_word(from, cached[from])` is non-zero.
    #[inline(always)]
    fn descend(&mut self, from: usize) {
        for i in (0..from).rev() {
            let w = self.level_word(i + 1, self.cached[i + 1] as usize);
            debug_assert!(w != 0);
            self.cached[i] = self.cached[i + 1] * 64 + w.trailing_zeros();
        }
    }

    #[inline]
    pub fn alloc(&mut self) -> Option<DepotHandle> {
        if let Some(handle) = self.index_stack.pop() {
            self.generations[handle.slot() as usize] = handle.generation();
            return Some(handle);
        }
        if self.recycled != 0 {
            Some(self.alloc_recycled())
        } else {
            self.alloc_fresh()
        }
    }

    #[inline]
    fn alloc_fresh(&mut self) -> Option<DepotHandle> {
        let slot = self.bump + 1;
        if slot as usize >= SLOTS {
            return None;
        }
        self.bump = slot;
        self.generations[slot as usize] = FIRST_GENERATION;
        Some(encode_handle(slot, FIRST_GENERATION))
    }

    fn alloc_recycled(&mut self) -> DepotHandle {
        let slab_idx = self.cached[0] as usize;
        debug_assert!(slab_idx < SLABS);

        let word = self.slabs[slab_idx];
        debug_assert!(word != 0);

        let bit = word.trailing_zeros();
        unsafe { *self.slabs.get_unchecked_mut(slab_idx) &= !(1u64 << bit) };

        let slot = slab_idx as u32 * 64 + bit;
        let stored = self.generations[slot as usize];
        debug_assert!(stored & DEAD_BIT != 0);
        let generation = stored & GENERATION_MASK;
        self.generations[slot as usize] = generation;
        self.recycled -= 1;

        if unsafe { *self.slabs.get_unchecked(slab_idx) == 0 } {
            self.propagate_exhausted(slab_idx);
            if self.recycled != 0 {
                self.advance_cached_slab();
            }
        }

        encode_handle(slot, generation)
    }

    #[inline]
    fn propagate_exhausted(&mut self, slab_idx: usize) {
        let l1_word = slab_idx / 64;
        let l1_bit = slab_idx % 64;
        debug_assert!(l1_word < AW);
        self.l1[l1_word] &= !(1u64 << l1_bit);
        if self.l1[l1_word] == 0 {
            let l2_word = l1_word / 64;
            let l2_bit = l1_word % 64;
            debug_assert!(l2_word < BW);
            self.l2[l2_word] &= !(1u64 << l2_bit);
            if self.l2[l2_word] == 0 {
                let l3_word = l2_word / 64;
                let l3_bit = l2_word % 64;
                debug_assert!(l3_word < CW);
                self.l3[l3_word] &= !(1u64 << l3_bit);
            }
        }
    }

    #[inline]
    fn propagate_available(&mut self, slab_idx: usize) {
        let l1_word = slab_idx / 64;
        let l1_bit = slab_idx % 64;
        debug_assert!(l1_word < AW);
        let l1_was_zero = self.l1[l1_word] == 0;
        self.l1[l1_word] |= 1u64 << l1_bit;
        if l1_was_zero {
            let l2_word = l1_word / 64;
            let l2_bit = l1_word % 64;
            debug_assert!(l2_word < BW);
            let l2_was_zero = self.l2[l2_word] == 0;
            self.l2[l2_word] |= 1u64 << l2_bit;
            if l2_was_zero {
                let l3_word = l2_word / 64;
                let l3_bit = l2_word % 64;
                debug_assert!(l3_word < CW);
                self.l3[l3_word] |= 1u64 << l3_bit;
            }
        }
    }

    #[inline]
    fn advance_cached_slab(&mut self) {
        let l1 = self.level_word(1, self.cached[1] as usize);
        if l1 != 0 {
            self.cached[0] = self.cached[1] * 64 + l1.trailing_zeros();
            return;
        }

        let l2 = self.level_word(2, self.cached[2] as usize);
        if l2 != 0 {
            self.cached[1] = self.cached[2] * 64 + l2.trailing_zeros();
            self.descend(1);
            return;
        }

        let l3 = self.level_word(3, self.cached[3] as usize);
        if l3 != 0 {
            self.cached[2] = self.cached[3] * 64 + l3.trailing_zeros();
            self.descend(2);
            return;
        }

        self.cold_refill();
    }

    #[cold]
    #[inline(never)]
    fn cold_refill(&mut self) {
        let start = self.cached[3] as usize + 1;
        for w in start..CW {
            if self.l3[w] != 0 {
                self.cached[3] = w as u32;
                self.descend(3);
                return;
            }
        }
        unreachable!("recycled count is nonzero but the summary bitmaps are empty");
    }

    pub fn free(&mut self, handle: DepotHandle) -> Result<(), FreeError> {
        let slot = handle.slot() as usize;
        let generation = handle.generation();

        if slot == 0 || slot >= SLOTS {
            return Err(FreeError::OutOfRange);
        }
        if generation == BAD_GENERATION || self.generations[slot] != generation {
            return Err(FreeError::StaleGeneration);
        }

        let next_gen = next_live_generation(self.generations[slot]);
        self.generations[slot] = next_gen | DEAD_BIT;
        let next_handle = encode_handle(slot as u32, next_gen);

        if let Some(evicted) = self.index_stack.push(next_handle) {
            self.free_to_slab(evicted.slot() / 64, evicted.slot() % 64);
        }
        Ok(())
    }

    /// # Safety
    /// Caller must ensure `handle` refers to a currently allocated slot.
    pub unsafe fn free_unchecked(&mut self, handle: DepotHandle) {
        let slot = handle.slot();
        if slot == 0 || slot as usize >= SLOTS {
            return;
        }
        let slot = slot as usize;
        let next_gen = next_live_generation(self.generations[slot]);
        self.generations[slot] = next_gen | DEAD_BIT;
        let next_handle = encode_handle(slot as u32, next_gen);

        if let Some(evicted) = self.index_stack.push(next_handle) {
            self.free_to_slab(evicted.slot() / 64, evicted.slot() % 64);
        }
    }

    /// # Safety
    /// Caller must ensure `handle` refers to a currently allocated slot.
    pub unsafe fn poison_unchecked(&mut self, handle: DepotHandle) {
        let slot = handle.slot();
        if slot == 0 || slot as usize >= SLOTS {
            return;
        }
        self.generations[slot as usize] = crate::BAD_GENERATION;
    }

    #[inline]
    fn free_to_slab(&mut self, slab_idx: u32, bit: u32) {
        let was_full = self.slabs[slab_idx as usize] == 0;
        self.slabs[slab_idx as usize] |= 1u64 << bit;
        if was_full {
            self.propagate_available(slab_idx as usize);
        }
        if self.recycled == 0 {
            self.cached = [slab_idx, slab_idx / 64, slab_idx / 4096, slab_idx / 262144];
        } else {
            self.cached[0] = self.cached[0].min(slab_idx);
            self.cached[1] = self.cached[1].min(slab_idx / 64);
            self.cached[2] = self.cached[2].min(slab_idx / 4096);
            self.cached[3] = self.cached[3].min(slab_idx / 262144);
        }
        self.recycled += 1;
    }

    #[inline(always)]
    fn validate_raw(&self, raw: u32) -> Result<u32, AccessError> {
        let slot = (raw & crate::HANDLE_SLOT_MASK) as usize;
        let generation = (raw >> crate::HANDLE_SLOT_BITS) as u8;
        if slot == 0 || slot >= SLOTS {
            return Err(AccessError::OutOfRange);
        }
        if generation == crate::BAD_GENERATION {
            return Err(AccessError::StaleGeneration);
        }
        if self.generations[slot] != generation {
            return Err(AccessError::StaleGeneration);
        }
        Ok(slot as u32)
    }

    pub fn validate(&self, handle: DepotHandle) -> Result<u32, AccessError> {
        self.validate_raw(handle.raw())
    }

    #[inline]
    pub fn validate_live(&self, handle: DepotHandle) -> Option<u32> {
        let slot = self.validate(handle).ok()? as usize;
        let slab_idx = slot / 64;
        let bit = slot % 64;

        if self.slabs[slab_idx] & (1u64 << bit) != 0 {
            return None;
        }
        if self.index_stack.contains_slot(slot as u32) {
            return None;
        }

        Some(slot as u32)
    }

    /// Resolve an optional/provenance-checked handle to a slot. `None` and slot-0
    /// handles map to row 0. Invalid handles return `None`.
    #[inline]
    pub fn get_or_sentinel(&self, handle: Option<DepotHandle>) -> Option<u32> {
        match handle {
            None => Some(0),
            Some(h) if h.slot() == 0 => Some(0),
            Some(h) => self.validate(h).ok(),
        }
    }

    pub fn check_invariants(&self) {
        assert!(
            (self.bump as usize) < SLOTS,
            "bump cursor {} out of range",
            self.bump
        );
        assert_eq!(self.slabs[0] & 1, 0, "slot 0 in recycled bitmap");

        for slot in self.bump as usize + 1..SLOTS {
            assert_eq!(
                self.generations[slot], 0,
                "untouched slot {slot} above bump cursor has generation"
            );
            assert_eq!(
                self.slabs[slot / 64] & (1u64 << (slot % 64)),
                0,
                "untouched slot {slot} above bump cursor is in recycled bitmap"
            );
        }

        let recycled = self.slabs.iter().map(|word| word.count_ones()).sum::<u32>();
        assert_eq!(self.recycled, recycled, "recycled count mismatch");

        for slab_idx in 0..SLABS {
            for bit in 0..64 {
                let slot = slab_idx * 64 + bit;
                if self.slabs[slab_idx] & (1u64 << bit) != 0 {
                    assert!(
                        self.generations[slot] & DEAD_BIT != 0,
                        "free slab slot {slot} lacks dead marker"
                    );
                }
            }
        }

        for idx in self.index_stack.iter() {
            let slot = idx.slot() as usize;
            assert!(
                slot <= self.bump as usize,
                "IndexRing slot {slot} above bump cursor"
            );
        }

        if self.recycled != 0 {
            let lowest = self
                .slabs
                .iter()
                .position(|word| *word != 0)
                .expect("recycled count is nonzero but every slab is empty");
            assert_eq!(
                self.cached[0] as usize, lowest,
                "cached slab {} is not the lowest non-empty slab {lowest}",
                self.cached[0]
            );
        }

        // Slot 0 is the null/sentinel row, never a live allocation generation.
        assert_eq!(
            self.generations[0], BAD_GENERATION,
            "slot 0 generation must stay invalid"
        );

        // L1 bit set ↔ slab word non-zero (has recycled slots)
        for slab_idx in 0..SLABS {
            let has_free = self.slabs[slab_idx] != 0;
            let l1_set = self.l1[slab_idx / 64] & (1u64 << (slab_idx % 64)) != 0;
            assert_eq!(
                has_free, l1_set,
                "L1/slab mismatch at slab {slab_idx}: has_free={has_free}, l1={l1_set}"
            );

            for bit in 0..64 {
                let slot = slab_idx * 64 + bit;
                let stored = self.generations[slot];
                let free_in_slab = self.slabs[slab_idx] & (1u64 << bit) != 0;

                if !free_in_slab && stored & DEAD_BIT != 0 {
                    assert!(
                        self.index_stack.contains_slot(slot as u32),
                        "slot {slot} has dead marker but is not in IndexRing"
                    );
                }
            }
        }

        // L2 bit set ↔ L1 word non-zero
        for w in 0..AW {
            let l2_set = self.l2[w / 64] & (1u64 << (w % 64)) != 0;
            let l1_nonzero = self.l1[w] != 0;
            assert_eq!(l2_set, l1_nonzero, "L2/L1 mismatch at block {w}");
        }

        // L3 bit set ↔ L2 word non-zero
        for w in 0..BW {
            let l3_set = self.l3[w / 64] & (1u64 << (w % 64)) != 0;
            let l2_nonzero = self.l2[w] != 0;
            assert_eq!(l3_set, l2_nonzero, "L3/L2 mismatch at group {w}");
        }

        // Padding at summary levels
        check_padding(&self.l1, SLABS, "L1");
        check_padding(&self.l2, AW, "L2");
        check_padding(&self.l3, BW, "L3");

        // IndexRing entries: matching generation, absent from the recycled bitmap
        for idx in self.index_stack.iter() {
            let slot = idx.slot() as usize;
            assert!(slot < SLOTS, "IndexRing slot {slot} out of range");
            assert_eq!(
                idx.generation() | DEAD_BIT,
                self.generations[slot],
                "IndexRing generation mismatch at slot {slot}"
            );
            let slab_idx = slot / 64;
            let bit = slot % 64;
            assert!(
                self.slabs[slab_idx] & (1u64 << bit) == 0,
                "IndexRing slot {slot} is also in recycled bitmap"
            );
        }

        // Cache consistency
        if self.recycled != 0 {
            let slab_idx = self.cached[0] as usize;
            assert!(slab_idx < SLABS, "cached[0] {slab_idx} >= SLABS {SLABS}");
            assert!(
                self.slabs[slab_idx] != 0,
                "cached slab {slab_idx} has no recycled slots"
            );
            for i in 1..4 {
                assert_eq!(
                    self.cached[i],
                    self.cached[i - 1] / 64,
                    "cached[{i}] {} != cached[{}] / 64 = {}",
                    self.cached[i],
                    i - 1,
                    self.cached[i - 1] / 64
                );
            }
        }
    }
}

fn check_padding(words: &[u64], valid_count: usize, name: &str) {
    let full = valid_count / 64;
    let partial = valid_count % 64;
    for (w, &word) in words.iter().enumerate() {
        if w < full {
            // all 64 bits valid
        } else if w == full && partial > 0 {
            let mask = !((1u64 << partial) - 1);
            assert_eq!(
                word & mask,
                0,
                "{name} padding bits set in word {w}: {word:#018x}"
            );
        } else {
            assert_eq!(word, 0, "{name} padding word {w} non-zero: {word:#018x}");
        }
    }
}

impl<const SLABS: usize, const SLOTS: usize, const AW: usize, const BW: usize, const CW: usize>
    Default for FixedDepot<SLABS, SLOTS, AW, BW, CW>
{
    fn default() -> Self {
        Self::new()
    }
}

impl<const SLABS: usize, const SLOTS: usize, const AW: usize, const BW: usize, const CW: usize>
    crate::HasSlots for FixedDepot<SLABS, SLOTS, AW, BW, CW>
{
    const SLOTS: usize = SLOTS;
}

impl<const SLABS: usize, const SLOTS: usize, const AW: usize, const BW: usize, const CW: usize>
    crate::DepotAlloc for FixedDepot<SLABS, SLOTS, AW, BW, CW>
{
    #[inline(always)]
    fn alloc(&mut self) -> Option<crate::DepotHandle> {
        FixedDepot::alloc(self)
    }

    #[inline(always)]
    fn get_or_sentinel(&self, handle: Option<crate::DepotHandle>) -> Option<u32> {
        FixedDepot::get_or_sentinel(self, handle)
    }

    #[inline(always)]
    fn validate(&self, handle: crate::DepotHandle) -> Result<u32, crate::AccessError> {
        FixedDepot::validate(self, handle)
    }

    #[inline(always)]
    fn validate_live(&self, handle: crate::DepotHandle) -> Option<u32> {
        FixedDepot::validate_live(self, handle)
    }

    #[inline(always)]
    unsafe fn free_unchecked(&mut self, handle: crate::DepotHandle) {
        unsafe { FixedDepot::free_unchecked(self, handle) }
    }

    #[inline(always)]
    unsafe fn poison_unchecked(&mut self, handle: crate::DepotHandle) {
        unsafe { FixedDepot::poison_unchecked(self, handle) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    type Depot256 = fixed_depot_type!(256);
    type Depot4096 = fixed_depot_type!(4096);

    fn assert_all_zero_bytes<
        const SLABS: usize,
        const SLOTS: usize,
        const AW: usize,
        const BW: usize,
        const CW: usize,
    >(
        depot: &FixedDepot<SLABS, SLOTS, AW, BW, CW>,
    ) {
        let fields_size = std::mem::size_of_val(&depot.index_stack)
            + std::mem::size_of_val(&depot.generations)
            + std::mem::size_of_val(&depot.slabs)
            + std::mem::size_of_val(&depot.l1)
            + std::mem::size_of_val(&depot.l2)
            + std::mem::size_of_val(&depot.l3)
            + std::mem::size_of_val(&depot.cached)
            + std::mem::size_of_val(&depot.recycled)
            + std::mem::size_of_val(&depot.bump);
        assert_eq!(std::mem::size_of_val(depot), fields_size);
        assert_eq!(
            std::mem::size_of::<IndexRing>(),
            std::mem::size_of::<[Option<DepotHandle>; crate::index_ring::CAPACITY]>()
                + std::mem::size_of::<usize>()
        );

        // SAFETY: The size checks above prove these values contain no padding bytes.
        let bytes = unsafe {
            std::slice::from_raw_parts(
                (depot as *const FixedDepot<SLABS, SLOTS, AW, BW, CW>).cast::<u8>(),
                std::mem::size_of_val(depot),
            )
        };
        assert!(bytes.iter().all(|byte| *byte == 0));
    }

    fn for_each_handle_input_class(
        mut check: impl FnMut(
            &Depot256,
            &'static str,
            Option<DepotHandle>,
            Option<u32>,
        ),
    ) {
        let mut depot = Depot256::new();

        check(&depot, "none", None, Some(0));
        check(
            &depot,
            "slot zero generation one",
            Some(DepotHandle::from_raw(1u32 << crate::HANDLE_SLOT_BITS)),
            Some(0),
        );
        check(
            &depot,
            "slot zero generation 127",
            Some(DepotHandle::from_raw(
                (GENERATION_MASK as u32) << crate::HANDLE_SLOT_BITS,
            )),
            Some(0),
        );
        check(
            &depot,
            "never allocated",
            Some(encode_handle(200, FIRST_GENERATION)),
            None,
        );
        check(
            &depot,
            "out of range",
            Some(encode_handle(300, FIRST_GENERATION)),
            None,
        );

        let live = depot.alloc().unwrap();
        check(&depot, "live", Some(live), Some(live.slot()));

        let freed = depot.alloc().unwrap();
        depot.free(freed).unwrap();
        check(&depot, "freed", Some(freed), None);

        let poisoned = depot.alloc().unwrap();
        // SAFETY: `poisoned` is live in this depot.
        unsafe { depot.poison_unchecked(poisoned) };
        check(&depot, "poisoned", Some(poisoned), None);

        let ring_slot = depot.alloc().unwrap();
        depot.free(ring_slot).unwrap();
        let forged_ring = encode_handle(
            ring_slot.slot(),
            next_live_generation(ring_slot.generation()),
        );
        check(&depot, "forged current generation in ring", Some(forged_ring), None);

        let handles: Vec<_> = (0..17).map(|_| depot.alloc().unwrap()).collect();
        for handle in &handles {
            depot.free(*handle).unwrap();
        }
        let first = handles.first().unwrap();
        let forged_slab = encode_handle(
            first.slot(),
            next_live_generation(first.generation()),
        );
        check(&depot, "forged current generation in slab", Some(forged_slab), None);
        let last = handles.last().unwrap();
        let forged_ring = encode_handle(last.slot(), next_live_generation(last.generation()));
        check(&depot, "forged current generation in ring", Some(forged_ring), None);

        let mut wrapped_depot = Depot256::new();
        let wrapped = wrapped_depot.alloc().unwrap();
        wrapped_depot.free(wrapped).unwrap();
        for _ in 0..126 {
            let handle = wrapped_depot.alloc().unwrap();
            wrapped_depot.free(handle).unwrap();
        }
        check(
            &wrapped_depot,
            "wrapped id on free slot",
            Some(wrapped),
            None,
        );
    }

    #[test]
    fn test_const_construction() {
        static _POOL: Depot256 = FixedDepot::new();
        static _POOL2: Depot4096 = FixedDepot::new();
    }

    #[test]
    fn new_is_all_zero_bytes() {
        macro_rules! check {
            ($depot:ty) => {{
                const CONST_DEPOT: $depot = <$depot>::new();
                assert_all_zero_bytes(&CONST_DEPOT);

                let runtime_depot = <$depot>::new();
                assert_all_zero_bytes(&runtime_depot);
            }};
        }

        check!(fixed_depot_type!(1));
        check!(fixed_depot_type!(100));
        check!(fixed_depot_type!(256));
        check!(fixed_depot_type!(8192));
    }

    #[test]
    fn fresh_allocs_come_from_bump_cursor() {
        let mut depot = Depot256::new();

        for expected in 1..=32 {
            let handle = depot.alloc().unwrap();
            assert_eq!(handle.slot(), expected);
            assert_eq!(depot.bump, expected);
            assert_eq!(depot.recycled, 0);
            assert!(depot.slabs.iter().all(|word| *word == 0));
            depot.check_invariants();
        }
    }

    #[test]
    fn ring_then_recycled_bitmap_then_cursor() {
        let mut depot = Depot256::new();
        let handles: Vec<_> = (0..20).map(|_| depot.alloc().unwrap()).collect();
        for handle in &handles {
            depot.free(*handle).unwrap();
        }

        assert_eq!(depot.recycled, 4);
        for expected in (5..=20).rev() {
            assert_eq!(depot.alloc().unwrap().slot(), expected);
        }
        for expected in 1..=4 {
            assert_eq!(depot.alloc().unwrap().slot(), expected);
        }
        assert_eq!(depot.alloc().unwrap().slot(), 21);
        depot.check_invariants();
    }

    #[test]
    fn recycled_count_and_cache_across_slabs() {
        let mut depot = Depot256::new();
        let handles: Vec<_> = (0..200).map(|_| depot.alloc().unwrap()).collect();
        let slab_zero = handles.iter().copied().find(|handle| handle.slot() == 1).unwrap();

        depot.free(slab_zero).unwrap();
        for handle in handles
            .iter()
            .copied()
            .filter(|handle| handle.slot() / 64 == 2)
        {
            depot.free(handle).unwrap();
        }
        assert_eq!(depot.recycled, 49);
        assert_eq!(depot.cached, [0; 4]);
        depot.check_invariants();

        for expected in (176..=191).rev() {
            assert_eq!(depot.alloc().unwrap().slot(), expected);
            depot.check_invariants();
        }
        assert_eq!(depot.alloc().unwrap().slot(), 1);
        depot.check_invariants();
        for expected in 128..=175 {
            assert_eq!(depot.alloc().unwrap().slot(), expected);
            depot.check_invariants();
        }

        assert_eq!(depot.recycled, 0);
        let fresh = depot.alloc().unwrap();
        assert_eq!(fresh.slot(), 201);
        depot.check_invariants();

        depot.free(fresh).unwrap();
        assert_eq!(depot.alloc().unwrap().slot(), fresh.slot());
        depot.check_invariants();
    }

    #[test]
    fn bitmap_refill_after_drain_resets_cache() {
        let mut depot = Depot256::new();
        let handles: Vec<_> = (0..255).map(|_| depot.alloc().unwrap()).collect();
        depot.check_invariants();

        for handle in &handles[128..=144] {
            depot.free(*handle).unwrap();
        }
        depot.check_invariants();

        for expected in (130..=145).rev() {
            assert_eq!(depot.alloc().unwrap().slot(), expected);
        }
        assert_eq!(depot.alloc().unwrap().slot(), 129);
        depot.check_invariants();

        for handle in &handles[192..=208] {
            depot.free(*handle).unwrap();
        }
        depot.check_invariants();

        for expected in (194..=209).rev() {
            assert_eq!(depot.alloc().unwrap().slot(), expected);
        }
        depot.check_invariants();

        assert_eq!(depot.alloc().unwrap().slot(), 193);
        depot.check_invariants();
    }

    #[test]
    fn cursor_exhaustion_then_recycle() {
        let mut depot = Depot256::new();
        let handles: Vec<_> = (0..255).map(|_| depot.alloc().unwrap()).collect();

        assert!(depot.alloc().is_none());
        assert_eq!(depot.bump, 255);
        for handle in handles.iter().take(17) {
            depot.free(*handle).unwrap();
        }
        assert_eq!(depot.recycled, 1);
        depot.check_invariants();

        for expected in (2..=17).rev() {
            assert_eq!(depot.alloc().unwrap().slot(), expected);
            depot.check_invariants();
        }
        assert_eq!(depot.alloc().unwrap().slot(), 1);
        depot.check_invariants();
        assert!(depot.alloc().is_none());
    }

    #[test]
    fn free_rejects_generation_zero_handle() {
        let mut depot = Depot256::new();
        let never_used = DepotHandle::from_raw(1);

        assert_eq!(depot.free(never_used), Err(FreeError::StaleGeneration));
        depot.check_invariants();
        assert_eq!(depot.alloc().unwrap().slot(), 1);
    }

    #[test]
    fn test_basic_alloc_free() {
        let mut depot = Depot256::new();
        let h = depot.alloc().unwrap();
        assert_eq!(h.generation(), FIRST_GENERATION);
        depot.free(h).unwrap();
    }

    #[test]
    fn test_generation_bumps() {
        let mut depot = Depot256::new();
        let h1 = depot.alloc().unwrap();
        depot.free(h1).unwrap();
        let h2 = depot.alloc().unwrap();
        assert_eq!(h1.slot(), h2.slot());
        assert_eq!(h2.generation(), 2);
    }

    #[test]
    fn test_stale_handle_rejected() {
        let mut depot = Depot256::new();
        let h = depot.alloc().unwrap();
        depot.free(h).unwrap();
        assert_eq!(depot.free(h), Err(FreeError::StaleGeneration));
    }

    #[test]
    fn test_returns_none_when_full() {
        let mut depot = Depot256::new();
        let mut handles = Vec::new();
        for _ in 0..255 {
            handles.push(depot.alloc().unwrap());
        }
        assert!(depot.alloc().is_none());

        depot.free(handles.pop().unwrap()).unwrap();
        assert!(depot.alloc().is_some());
    }

    #[test]
    fn test_all_slots_unique() {
        let mut depot = Depot256::new();
        let mut slots = std::collections::HashSet::new();
        for _ in 0..255 {
            let h = depot.alloc().unwrap();
            assert!(slots.insert(h.slot()));
        }
    }

    #[test]
    fn test_sentinel_never_returned() {
        let mut depot = Depot256::new();
        for _ in 0..255 {
            let h = depot.alloc().unwrap();
            assert_ne!(h.slot(), 0, "sentinel slot returned by alloc");
        }
    }

    #[test]
    fn test_alloc_free_cycle() {
        let mut depot = Depot256::new();
        for _ in 0..1000 {
            let h = depot.alloc().unwrap();
            depot.free(h).unwrap();
        }
    }

    #[test]
    fn test_burst_alloc_free() {
        let mut depot = Depot256::new();
        let mut handles = Vec::new();

        for _ in 0..200 {
            handles.push(depot.alloc().unwrap());
        }
        for h in handles.drain(..) {
            depot.free(h).unwrap();
        }

        for _ in 0..200 {
            handles.push(depot.alloc().unwrap());
        }
        assert_eq!(handles.len(), 200);
    }

    #[test]
    fn test_out_of_range() {
        let mut depot = Depot256::new();
        let bad = DepotHandle::from_raw(999);
        assert_eq!(depot.free(bad), Err(FreeError::OutOfRange));
    }

    #[test]
    fn test_slot_zero_never_valid() {
        let depot = Depot256::new();
        // Slot 0 gen 0 can't be constructed (raw 0 = NonZeroU32 niche).
        // Slot 0 gen 1 can be constructed but slot 0 is still never a live slot.
        let fake = DepotHandle::from_raw(1u32 << crate::HANDLE_SLOT_BITS);
        assert_eq!(fake.slot(), 0);
        assert_eq!(fake.generation(), 1);
        assert_eq!(depot.validate(fake), Err(AccessError::OutOfRange));
    }

    #[test]
    fn test_validate_live() {
        let mut depot = Depot256::new();
        let h = depot.alloc().unwrap();
        assert_eq!(depot.validate(h), Ok(h.slot()));
        assert_eq!(depot.validate_live(h), Some(h.slot()));
    }

    #[test]
    fn test_validate_stale() {
        let mut depot = Depot256::new();
        let h = depot.alloc().unwrap();
        depot.free(h).unwrap();
        assert_eq!(depot.validate(h), Err(AccessError::StaleGeneration));
        assert_eq!(depot.validate_live(h), None);
    }

    #[test]
    fn validate_live_rejects_dead_slot_still_in_recycle_ring() {
        let mut depot = Depot256::new();
        let h = depot.alloc().unwrap();
        depot.free(h).unwrap();

        let forged_current_dead = encode_handle(h.slot(), next_live_generation(h.generation()));
        assert_eq!(
            depot.validate(forged_current_dead),
            Err(AccessError::StaleGeneration)
        );
        assert_eq!(depot.validate_live(forged_current_dead), None);
    }

    #[test]
    fn validate_live_rejects_dead_slot_after_slab_return() {
        let mut depot = Depot256::new();
        let handles: Vec<_> = (0..17).map(|_| depot.alloc().unwrap()).collect();
        let first = handles[0];

        for h in handles {
            depot.free(h).unwrap();
        }

        let forged_current_dead =
            encode_handle(first.slot(), next_live_generation(first.generation()));
        assert_eq!(
            depot.validate(forged_current_dead),
            Err(AccessError::StaleGeneration)
        );
        assert_eq!(depot.validate_live(forged_current_dead), None);
    }

    #[test]
    fn poison_unchecked_invalidates_without_recycling() {
        let mut depot = Depot256::new();
        let h = depot.alloc().unwrap();
        let slot = h.slot();

        unsafe { depot.poison_unchecked(h) };

        assert_eq!(depot.validate(h), Err(AccessError::StaleGeneration));
        assert_eq!(depot.validate_live(h), None);
        assert_eq!(depot.get_or_sentinel(Some(h)), None);
        let forged_gen_zero = DepotHandle::from_raw(slot);
        assert_eq!(
            depot.validate(forged_gen_zero),
            Err(AccessError::StaleGeneration)
        );
        assert_eq!(depot.validate_live(forged_gen_zero), None);
        assert_eq!(depot.get_or_sentinel(Some(forged_gen_zero)), None);

        let next = depot.alloc().unwrap();
        assert_ne!(next.slot(), slot);
        depot.check_invariants();
    }

    #[test]
    fn invariant_checks_hold_basic() {
        let mut depot = Depot4096::new();
        let mut hs = Vec::new();
        for _ in 0..200 {
            hs.push(depot.alloc().unwrap());
        }
        for h in hs.into_iter().step_by(2) {
            depot.free(h).unwrap();
        }
        depot.check_invariants();
    }

    #[test]
    fn generation_wrap_skips_zero() {
        let mut depot = Depot256::new();
        let h = depot.alloc().unwrap();
        let slot = h.slot();
        assert_eq!(h.generation(), FIRST_GENERATION);
        depot.free(h).unwrap();

        for expected_gen in 2..=127u8 {
            let h = depot.alloc().unwrap();
            assert_eq!(h.slot(), slot);
            assert_eq!(h.generation(), expected_gen);
            depot.free(h).unwrap();
        }
        let h = depot.alloc().unwrap();
        assert_eq!(h.slot(), slot);
        assert_eq!(h.generation(), FIRST_GENERATION);
        depot.free(h).unwrap();
    }

    #[test]
    fn freed_handle_fails_generation_immediately() {
        let mut depot = Depot256::new();
        let h = depot.alloc().unwrap();
        depot.free(h).unwrap();
        assert_eq!(depot.validate(h), Err(AccessError::StaleGeneration));
    }

    #[test]
    fn get_or_sentinel_input_classes() {
        for_each_handle_input_class(|depot, name, handle, expected| {
            assert_eq!(depot.get_or_sentinel(handle), expected, "{name}");
            assert_eq!(
                <Depot256 as crate::DepotAlloc>::get_or_sentinel(depot, handle),
                expected,
                "trait call: {name}"
            );
        });
    }

    #[test]
    fn validate_agrees_with_validate_live() {
        for_each_handle_input_class(|depot, name, handle, _| {
            if let Some(handle) = handle {
                assert_eq!(
                    depot.validate(handle).ok(),
                    depot.validate_live(handle),
                    "{name}"
                );
            }
        });
    }

    #[test]
    fn depot_alloc_validate_uses_fixed_override() {
        let depot = Depot256::new();
        let out_of_range = encode_handle(300, FIRST_GENERATION);
        assert_eq!(
            <Depot256 as crate::DepotAlloc>::validate(&depot, out_of_range),
            Err(AccessError::OutOfRange)
        );
    }

    #[test]
    fn generation_wraps_after_127_reuses() {
        let mut depot = Depot256::new();
        let first = depot.alloc().unwrap();
        let mut current = first;

        for _ in 0..127 {
            depot.free(current).unwrap();
            current = depot.alloc().unwrap();
        }

        assert_eq!(current, first);
        assert_eq!(depot.validate(current), Ok(current.slot()));
    }

    #[test]
    fn wrapped_handle_cannot_double_free() {
        let mut depot = Depot256::new();
        let old = depot.alloc().unwrap();
        depot.free(old).unwrap();

        // One free per live generation brings the stored generation back to `old`'s value while the
        // slot is free again. Without the dead marker, `free(old)` would pass here.
        for _ in 1..GENERATION_MASK {
            let current = depot.alloc().unwrap();
            depot.free(current).unwrap();
        }

        assert_eq!(
            depot.generations[old.slot() as usize],
            old.generation() | DEAD_BIT
        );
        assert_eq!(depot.free(old), Err(FreeError::StaleGeneration));
        depot.check_invariants();
    }

    #[test]
    #[should_panic(expected = "has dead marker but is not in IndexRing")]
    fn check_invariants_rejects_dead_marker_on_allocated_slot() {
        let mut depot = Depot256::new();
        let h = depot.alloc().unwrap();
        depot.generations[h.slot() as usize] |= DEAD_BIT;
        depot.check_invariants();
    }

    #[test]
    #[should_panic(expected = "free slab slot 1 lacks dead marker")]
    fn check_invariants_rejects_live_generation_on_free_slab_slot() {
        let mut depot = Depot256::new();
        let handles: Vec<_> = (0..17).map(|_| depot.alloc().unwrap()).collect();
        for handle in handles {
            depot.free(handle).unwrap();
        }
        depot.generations[1] = FIRST_GENERATION;
        depot.check_invariants();
    }

    #[test]
    #[should_panic(expected = "untouched slot 2 above bump cursor has generation")]
    fn check_invariants_rejects_generation_above_bump_cursor() {
        let mut depot = Depot256::new();
        depot.generations[depot.bump as usize + 2] = FIRST_GENERATION;
        depot.check_invariants();
    }

    #[test]
    #[should_panic(expected = "untouched slot 2 above bump cursor is in recycled bitmap")]
    fn check_invariants_rejects_recycled_slot_above_bump_cursor() {
        let mut depot = Depot256::new();
        depot.free_to_slab(0, depot.bump + 2);
        depot.check_invariants();
    }

    #[test]
    #[should_panic(expected = "bump cursor 256 out of range")]
    fn check_invariants_rejects_out_of_range_bump_cursor() {
        let mut depot = Depot256::new();
        depot.bump = 256;
        depot.check_invariants();
    }

    #[test]
    #[should_panic(expected = "recycled count mismatch")]
    fn check_invariants_rejects_wrong_recycled_count() {
        let mut depot = Depot256::new();
        depot.recycled += 1;
        depot.check_invariants();
    }

    #[test]
    #[should_panic(expected = "IndexRing slot 1 above bump cursor")]
    fn check_invariants_rejects_ring_slot_above_bump_cursor() {
        let mut depot = Depot256::new();
        depot.index_stack.push(encode_handle(1, FIRST_GENERATION));
        depot.check_invariants();
    }

    #[test]
    #[should_panic(expected = "cached slab 1 is not the lowest non-empty slab 0")]
    fn check_invariants_rejects_cache_above_lowest_nonempty_slab() {
        let mut depot = Depot256::new();
        let handles: Vec<_> = (0..80).map(|_| depot.alloc().unwrap()).collect();
        for handle in handles.iter().take(17) {
            depot.free(*handle).unwrap();
        }
        for handle in handles.iter().skip(63) {
            depot.free(*handle).unwrap();
        }
        depot.cached[0] = 1;
        depot.check_invariants();
    }

    #[test]
    #[should_panic(expected = "IndexRing generation mismatch")]
    fn check_invariants_rejects_ring_entry_without_dead_marker() {
        let mut depot = Depot256::new();
        let h = depot.alloc().unwrap();
        depot.free(h).unwrap();
        depot.generations[h.slot() as usize] &= GENERATION_MASK;
        depot.check_invariants();
    }

    #[test]
    fn fresh_slot_rejects_forged_first_generation() {
        let mut depot = Depot256::new();
        let forged = encode_handle(1, FIRST_GENERATION);

        assert_eq!(depot.validate(forged), Err(AccessError::StaleGeneration));
        assert_eq!(depot.validate_live(forged), None);
        assert_eq!(depot.free(forged), Err(FreeError::StaleGeneration));

        let live = depot.alloc().unwrap();
        assert_eq!(live, forged);
        assert_eq!(depot.validate(live), Ok(live.slot()));
    }

    #[test]
    fn dead_bit_lifecycle_ring_and_slab() {
        let mut depot = Depot256::new();
        let handles: Vec<_> = (0..17).map(|_| depot.alloc().unwrap()).collect();
        for handle in &handles {
            depot.free(*handle).unwrap();
        }

        let slab_handle = handles[0];
        let ring_handle = handles[16];
        assert_eq!(
            depot.generations[slab_handle.slot() as usize],
            next_live_generation(slab_handle.generation()) | DEAD_BIT
        );
        assert_ne!(
            depot.slabs[slab_handle.slot() as usize / 64]
                & (1u64 << (slab_handle.slot() % 64)),
            0
        );
        assert_eq!(
            depot.generations[ring_handle.slot() as usize],
            next_live_generation(ring_handle.generation()) | DEAD_BIT
        );

        let from_ring = depot.alloc().unwrap();
        assert_eq!(from_ring.slot(), ring_handle.slot());
        assert_eq!(depot.generations[from_ring.slot() as usize], from_ring.generation());

        let mut remaining_ring = Vec::new();
        for _ in 1..16 {
            remaining_ring.push(depot.alloc().unwrap());
        }
        let from_slab = depot.alloc().unwrap();
        assert_eq!(from_slab.slot(), slab_handle.slot());
        assert_eq!(depot.generations[from_slab.slot() as usize], from_slab.generation());
        depot.check_invariants();
    }

    #[test]
    fn free_unchecked_rejects_slot_zero_and_out_of_range() {
        let mut depot = Depot256::new();
        let slot_zero_gen_one = DepotHandle::from_raw(1u32 << crate::HANDLE_SLOT_BITS);
        let oob = DepotHandle::from_raw(999);

        unsafe {
            depot.free_unchecked(slot_zero_gen_one);
            depot.free_unchecked(oob);
        }

        let h = depot.alloc().unwrap();
        assert_eq!(h.slot(), 1);
        assert_eq!(h.generation(), FIRST_GENERATION);
        depot.check_invariants();
    }

    #[test]
    fn min_on_free_packing() {
        let mut depot = Depot256::new();
        let mut handles: Vec<_> = (0..20).map(|_| depot.alloc().unwrap()).collect();

        let low_handle = handles.remove(0);
        let low_slot = low_handle.slot();
        depot.free(low_handle).unwrap();

        let h = depot.alloc().unwrap();
        assert_eq!(h.slot(), low_slot);

        for h in handles {
            depot.free(h).unwrap();
        }
        depot.free(h).unwrap();
    }

    #[test]
    fn unused_l2_bits_are_zero() {
        // Depot8192: SLABS=128, AW=2. L1 has 2 valid blocks (bits 0..127).
        // L2 should have bits 0..1 set, rest zero.
        type DepotL = fixed_depot_type!(8192);
        let mut depot = DepotL::new();
        let handles: Vec<_> = (0..8191).map(|_| depot.alloc().unwrap()).collect();
        for handle in handles {
            depot.free(handle).unwrap();
        }
        assert_ne!(depot.l1[0], 0);
        assert_ne!(depot.l1[1], 0);
        let l2_word = depot.l2[0];
        assert_eq!(l2_word, 0b11, "L2 bits 0..1 should be set");
        assert_eq!(l2_word & !0b11u64, 0, "L2 padding bits should be zero");
    }

    #[test]
    fn trailing_zeros_never_exceeds_slab_count() {
        let mut depot = Depot256::new();
        let mut handles = Vec::new();
        for _ in 0..255 {
            handles.push(depot.alloc().unwrap());
        }
        assert!(depot.alloc().is_none());

        depot.free(handles.pop().unwrap()).unwrap();
        assert!(depot.alloc().is_some());
    }

    #[test]
    fn padding_bits_stay_zero_after_operations() {
        let mut depot = Depot256::new();
        let mut handles = Vec::new();

        for _ in 0..3 {
            for _ in 0..200 {
                handles.push(depot.alloc().unwrap());
            }
            for h in handles.drain(..) {
                depot.free(h).unwrap();
            }
        }

        // L1 padding bits must still be zero
        let valid_mask = (1u64 << 4) - 1;
        assert_eq!(
            depot.l1[0] & !valid_mask,
            0,
            "L1 padding bits corrupted after alloc/free cycles"
        );
        depot.check_invariants();
    }

    #[test]
    fn multi_block_depot_invariants() {
        type DepotLarge = fixed_depot_type!(8192);
        let mut depot = DepotLarge::new();

        let mut handles = Vec::new();
        for _ in 0..5000 {
            handles.push(depot.alloc().unwrap());
        }
        depot.check_invariants();

        for h in handles.iter().step_by(3) {
            depot.free(*h).unwrap();
        }
        depot.check_invariants();
    }

    #[test]
    fn check_invariants_at_construction() {
        Depot256::new().check_invariants();
        Depot4096::new().check_invariants();
        <fixed_depot_type!(8192)>::new().check_invariants();
    }

    #[test]
    fn non_multiple_of_64_constructs() {
        type Depot100 = fixed_depot_type!(100);
        let depot = Depot100::new();
        depot.check_invariants();
    }

    #[test]
    fn non_multiple_of_64_capacity() {
        type Depot100 = fixed_depot_type!(100);
        let mut depot = Depot100::new();
        let mut handles = Vec::new();
        for _ in 0..127 {
            handles.push(depot.alloc().unwrap());
        }
        assert!(depot.alloc().is_none());

        for h in handles.drain(..) {
            depot.free(h).unwrap();
        }
        depot.check_invariants();
    }

    #[test]
    fn non_multiple_of_64_various_sizes() {
        <fixed_depot_type!(1)>::new().check_invariants();
        <fixed_depot_type!(33)>::new().check_invariants();
        <fixed_depot_type!(65)>::new().check_invariants();
        <fixed_depot_type!(100)>::new().check_invariants();
        <fixed_depot_type!(200)>::new().check_invariants();
        <fixed_depot_type!(1000)>::new().check_invariants();
    }

    #[test]
    fn non_multiple_of_64_alloc_free_cycle() {
        type Depot65 = fixed_depot_type!(65);
        let mut depot = Depot65::new();
        for _ in 0..500 {
            let h = depot.alloc().unwrap();
            depot.free(h).unwrap();
        }
        depot.check_invariants();
    }
}
