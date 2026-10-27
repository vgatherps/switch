//! Circular buffer of recently-freed handles (L1 cache).

use crate::DepotHandle;

/// Number of slots in the index ring.
pub const CAPACITY: usize = 16;

#[derive(Debug)]
pub struct IndexRing {
    data: [Option<DepotHandle>; CAPACITY],
    head: usize,
}

impl IndexRing {
    #[inline]
    pub const fn new() -> Self {
        Self {
            data: [const { None }; CAPACITY],
            head: 0,
        }
    }

    #[inline(always)]
    pub fn push(&mut self, handle: DepotHandle) -> Option<DepotHandle> {
        let slot = self.head % CAPACITY;
        self.head = self.head.wrapping_add(1);
        self.data[slot].replace(handle)
    }

    #[inline(always)]
    pub fn pop(&mut self) -> Option<DepotHandle> {
        self.head = self.head.wrapping_sub(1);
        self.data[self.head % CAPACITY].take()
    }

    pub fn iter(&self) -> impl Iterator<Item = DepotHandle> + '_ {
        self.data.iter().filter_map(|slot| *slot)
    }

    #[inline(always)]
    pub fn contains_slot(&self, slot: u32) -> bool {
        self.data
            .iter()
            .any(|entry| entry.is_some_and(|handle| handle.slot() == slot))
    }
}

impl Default for IndexRing {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn push_pop_lifo() {
        let mut ring = IndexRing::new();
        let h1 = DepotHandle::from_raw(1);
        let h2 = DepotHandle::from_raw(2);
        ring.push(h1);
        ring.push(h2);
        assert_eq!(ring.pop(), Some(h2));
        assert_eq!(ring.pop(), Some(h1));
        assert_eq!(ring.pop(), None);
    }
}
