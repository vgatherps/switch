//! # switch-depot
//!
//! A growable allocator of `u32` slot numbers. The runtime under the `switch` crate uses it
//! to number task slots; applications do not use it.
//!
//! ```rust
//! use switch_depot::DynDepot;
//!
//! let mut depot = DynDepot::new();
//! assert_eq!(depot.alloc(), 0);
//! assert_eq!(depot.alloc(), 1);
//! assert_eq!(depot.alloc(), 2);
//! depot.free(1);
//! assert_eq!(depot.alloc(), 1); // the lowest free slot comes back first
//! assert_eq!(depot.alloc(), 3);
//! ```

/// Slot numbers from 0, always the lowest free one first, so slots never used go out in
/// increasing order. No capacity: it grows by 64 slots when every slot is taken.
///
/// It keeps no handles or generations: the caller frees each slot it got exactly once.
#[derive(Debug, Default)]
pub struct DynDepot {
    free: Vec<u64>,
    summary: Vec<u64>,
}

impl DynDepot {
    /// An empty depot. Allocates nothing until the first [`alloc`](Self::alloc).
    pub const fn new() -> Self {
        Self {
            free: Vec::new(),
            summary: Vec::new(),
        }
    }

    /// Takes and returns the lowest free slot, growing by 64 slots when none is free.
    ///
    /// # Panics
    ///
    /// When growing would exceed 4,294,967,232 slots.
    #[inline]
    pub fn alloc(&mut self) -> u32 {
        let Some(top) = self.summary.iter().position(|&bits| bits != 0) else {
            return self.grow();
        };
        let summary = &mut self.summary[top];
        let word = top * 64 + summary.trailing_zeros() as usize;
        let free = &mut self.free[word];
        let bit = free.trailing_zeros();
        *free &= *free - 1;
        if *free == 0 {
            *summary &= *summary - 1;
        }
        (word * 64) as u32 + bit
    }

    /// Takes the first slot of a new bitmap word.
    #[cold]
    #[inline(never)]
    fn grow(&mut self) -> u32 {
        let word = self.free.len();
        // Every slot number, plus one, fits u32.
        let end = u32::try_from(word * 64 + 64).expect("slot numbers exceed u32");
        if word.is_multiple_of(64) {
            self.summary.push(0);
        }
        self.free.push(!1);
        self.summary[word / 64] |= 1 << (word % 64);
        end - 64
    }

    /// Returns a slot that [`alloc`](Self::alloc) handed out and that was not freed since.
    ///
    /// # Panics
    ///
    /// A slot outside the allocated bitmap panics. A slot that is already free, including an
    /// unused slot in an allocated bitmap word, panics only in debug builds; release builds
    /// do not detect it.
    #[inline]
    pub fn free(&mut self, slot: u32) {
        let word = slot as usize / 64;
        let free = &mut self.free[word];
        debug_assert_eq!(*free & 1 << (slot % 64), 0, "slot {slot} freed twice");
        *free |= 1 << (slot % 64);
        self.summary[word / 64] |= 1 << (word % 64);
    }
}

#[cfg(test)]
mod tests {
    use super::DynDepot;

    #[test]
    fn hands_out_slots_in_increasing_order_across_word_boundaries() {
        let mut slots = DynDepot::new();
        for expected in 0..64 * 64 + 65 {
            assert_eq!(slots.alloc(), expected);
        }
        assert_eq!((slots.free.len(), slots.summary.len()), (66, 2));
    }

    #[test]
    fn reuses_the_lowest_free_slot_first() {
        let mut slots = DynDepot::new();
        for _ in 0..64 * 64 + 10 {
            slots.alloc();
        }
        for slot in [4100, 70, 4095, 3, 64] {
            slots.free(slot);
        }
        for expected in [3, 64, 70, 4095, 4100, 4106] {
            assert_eq!(slots.alloc(), expected);
        }
    }

    #[test]
    fn a_full_word_is_reused_after_a_free() {
        let mut slots = DynDepot::new();
        for _ in 0..128 {
            slots.alloc();
        }
        slots.free(127);
        slots.free(0);
        assert_eq!(slots.alloc(), 0);
        assert_eq!(slots.alloc(), 127);
        assert_eq!(slots.alloc(), 128);
    }

    /// Random allocs and frees against a model: `alloc` returns the lowest slot not taken.
    #[test]
    fn random_allocs_and_frees_match_a_lowest_free_model() {
        let rounds = if cfg!(miri) { 2_000 } else { 200_000 };
        let mut slots = DynDepot::new();
        // Model: taken slots, freed slots below `high`, and `high`, the first never taken.
        let (mut taken, mut freed, mut high) = (Vec::new(), std::collections::BTreeSet::new(), 0);
        let mut state = 0x9e37_79b9_7f4a_7c15u64;
        for _ in 0..rounds {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            // Allocs outnumber frees 5:4, so the population climbs past several summary words.
            if taken.is_empty() || state % 9 < 5 {
                let lowest = freed.pop_first().unwrap_or_else(|| {
                    high += 1;
                    high - 1
                });
                assert_eq!(slots.alloc(), lowest);
                taken.push(lowest);
            } else {
                let slot = taken.swap_remove((state >> 32) as usize % taken.len());
                freed.insert(slot);
                slots.free(slot);
            }
        }
        assert!(cfg!(miri) || taken.len() > 64 * 64, "the walk stayed below one summary word");
    }
}
