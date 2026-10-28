macro_rules! depot_algorithm {
    () => {
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
        pub fn alloc(&mut self) -> Option<$crate::DepotHandle> {
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
        fn alloc_fresh(&mut self) -> Option<$crate::DepotHandle> {
            let slot = self.bump + 1;
            if slot as usize >= self.slots() {
                return None;
            }
            self.bump = slot;
            self.generations[slot as usize] = $crate::FIRST_GENERATION;
            Some($crate::encode_handle(slot, $crate::FIRST_GENERATION))
        }

        fn alloc_recycled(&mut self) -> $crate::DepotHandle {
            let slab_idx = self.cached[0] as usize;
            debug_assert!(slab_idx < self.slab_count());

            let word = self.slabs[slab_idx];
            debug_assert!(word != 0);

            let bit = word.trailing_zeros();
            unsafe { *self.slabs.get_unchecked_mut(slab_idx) &= !(1u64 << bit) };

            let slot = slab_idx as u32 * 64 + bit;
            let stored = self.generations[slot as usize];
            debug_assert!(stored & $crate::DEAD_BIT != 0);
            let generation = stored & $crate::GENERATION_MASK;
            self.generations[slot as usize] = generation;
            self.recycled -= 1;

            if unsafe { *self.slabs.get_unchecked(slab_idx) == 0 } {
                self.propagate_exhausted(slab_idx);
                if self.recycled != 0 {
                    self.advance_cached_slab();
                }
            }

            $crate::encode_handle(slot, generation)
        }

        #[inline]
        fn propagate_exhausted(&mut self, slab_idx: usize) {
            let l1_word = slab_idx / 64;
            let l1_bit = slab_idx % 64;
            debug_assert!(l1_word < self.l1_count());
            self.l1[l1_word] &= !(1u64 << l1_bit);
            if self.l1[l1_word] == 0 {
                let l2_word = l1_word / 64;
                let l2_bit = l1_word % 64;
                debug_assert!(l2_word < self.l2_count());
                self.l2[l2_word] &= !(1u64 << l2_bit);
                if self.l2[l2_word] == 0 {
                    let l3_word = l2_word / 64;
                    let l3_bit = l2_word % 64;
                    debug_assert!(l3_word < self.l3_count());
                    self.l3[l3_word] &= !(1u64 << l3_bit);
                }
            }
        }

        #[inline]
        fn propagate_available(&mut self, slab_idx: usize) {
            let l1_word = slab_idx / 64;
            let l1_bit = slab_idx % 64;
            debug_assert!(l1_word < self.l1_count());
            let l1_was_zero = self.l1[l1_word] == 0;
            self.l1[l1_word] |= 1u64 << l1_bit;
            if l1_was_zero {
                let l2_word = l1_word / 64;
                let l2_bit = l1_word % 64;
                debug_assert!(l2_word < self.l2_count());
                let l2_was_zero = self.l2[l2_word] == 0;
                self.l2[l2_word] |= 1u64 << l2_bit;
                if l2_was_zero {
                    let l3_word = l2_word / 64;
                    let l3_bit = l2_word % 64;
                    debug_assert!(l3_word < self.l3_count());
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
            for w in start..self.l3_count() {
                if self.l3[w] != 0 {
                    self.cached[3] = w as u32;
                    self.descend(3);
                    return;
                }
            }
            unreachable!("recycled count is nonzero but the summary bitmaps are empty");
        }

        pub fn free(&mut self, handle: $crate::DepotHandle) -> Result<(), $crate::FreeError> {
            let slot = handle.slot() as usize;
            let generation = handle.generation();

            if slot == 0 || slot >= self.slots() {
                return Err($crate::FreeError::OutOfRange);
            }
            if generation == $crate::BAD_GENERATION || self.generations[slot] != generation {
                return Err($crate::FreeError::StaleGeneration);
            }

            unsafe { self.free_slot_unchecked(slot as u32) };
            Ok(())
        }

        /// # Safety
        /// Caller must ensure `handle` refers to a currently allocated slot.
        #[inline(always)]
        pub unsafe fn free_unchecked(&mut self, handle: $crate::DepotHandle) {
            unsafe { self.free_slot_unchecked(handle.slot()) }
        }

        /// Free a live slot when its handle is unavailable.
        ///
        /// # Safety
        /// `slot` must be currently allocated in this depot.
        #[inline]
        pub unsafe fn free_slot_unchecked(&mut self, slot: u32) {
            if slot == 0 || slot as usize >= self.slots() {
                return;
            }
            let slot = slot as usize;
            let next_gen = $crate::next_live_generation(self.generations[slot]);
            self.generations[slot] = next_gen | $crate::DEAD_BIT;
            let next_handle = $crate::encode_handle(slot as u32, next_gen);

            if let Some(evicted) = self.index_stack.push(next_handle) {
                self.free_to_slab(evicted.slot() / 64, evicted.slot() % 64);
            }
        }

        /// # Safety
        /// Caller must ensure `handle` refers to a currently allocated slot.
        pub unsafe fn poison_unchecked(&mut self, handle: $crate::DepotHandle) {
            let slot = handle.slot();
            if slot == 0 || slot as usize >= self.slots() {
                return;
            }
            self.generations[slot as usize] = $crate::BAD_GENERATION;
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
        fn validate_raw(&self, raw: u32) -> Result<u32, $crate::AccessError> {
            let slot = (raw & $crate::HANDLE_SLOT_MASK) as usize;
            let generation = (raw >> $crate::HANDLE_SLOT_BITS) as u8;
            if slot == 0 || slot >= self.slots() {
                return Err($crate::AccessError::OutOfRange);
            }
            if generation == $crate::BAD_GENERATION {
                return Err($crate::AccessError::StaleGeneration);
            }
            if self.generations[slot] != generation {
                return Err($crate::AccessError::StaleGeneration);
            }
            Ok(slot as u32)
        }

        pub fn validate(&self, handle: $crate::DepotHandle) -> Result<u32, $crate::AccessError> {
            self.validate_raw(handle.raw())
        }

        #[inline]
        pub fn validate_live(&self, handle: $crate::DepotHandle) -> Option<u32> {
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
        pub fn get_or_sentinel(&self, handle: Option<$crate::DepotHandle>) -> Option<u32> {
            match handle {
                None => Some(0),
                Some(h) if h.slot() == 0 => Some(0),
                Some(h) => self.validate(h).ok(),
            }
        }

        pub fn check_invariants(&self) {
            let slots = self.slots();
            let slab_count = self.slab_count();
            let l1_count = self.l1_count();
            let l2_count = self.l2_count();

            assert!(
                (self.bump as usize) < slots,
                "bump cursor {} out of range",
                self.bump
            );
            assert_eq!(self.slabs[0] & 1, 0, "slot 0 in recycled bitmap");

            for slot in self.bump as usize + 1..slots {
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

            for slab_idx in 0..slab_count {
                for bit in 0..64 {
                    let slot = slab_idx * 64 + bit;
                    if self.slabs[slab_idx] & (1u64 << bit) != 0 {
                        assert!(
                            self.generations[slot] & $crate::DEAD_BIT != 0,
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

            assert_eq!(
                self.generations[0],
                $crate::BAD_GENERATION,
                "slot 0 generation must stay invalid"
            );

            for slab_idx in 0..slab_count {
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

                    if !free_in_slab && stored & $crate::DEAD_BIT != 0 {
                        assert!(
                            self.index_stack.contains_slot(slot as u32),
                            "slot {slot} has dead marker but is not in IndexRing"
                        );
                    }
                }
            }

            for w in 0..l1_count {
                let l2_set = self.l2[w / 64] & (1u64 << (w % 64)) != 0;
                let l1_nonzero = self.l1[w] != 0;
                assert_eq!(l2_set, l1_nonzero, "L2/L1 mismatch at block {w}");
            }

            for w in 0..l2_count {
                let l3_set = self.l3[w / 64] & (1u64 << (w % 64)) != 0;
                let l2_nonzero = self.l2[w] != 0;
                assert_eq!(l3_set, l2_nonzero, "L3/L2 mismatch at group {w}");
            }

            $crate::algorithm::check_padding(&self.l1, slab_count, "L1");
            $crate::algorithm::check_padding(&self.l2, l1_count, "L2");
            $crate::algorithm::check_padding(&self.l3, l2_count, "L3");

            for idx in self.index_stack.iter() {
                let slot = idx.slot() as usize;
                assert!(slot < slots, "IndexRing slot {slot} out of range");
                assert_eq!(
                    idx.generation() | $crate::DEAD_BIT,
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

            if self.recycled != 0 {
                let slab_idx = self.cached[0] as usize;
                assert!(
                    slab_idx < slab_count,
                    "cached[0] {slab_idx} >= slab count {slab_count}"
                );
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
    };
}

pub(crate) use depot_algorithm;

pub(crate) fn check_padding(words: &[u64], valid_count: usize, name: &str) {
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
