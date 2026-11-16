//! Intrusive doubly-linked list.

use crate::intrusive::links::{ListLinked, NIL};

/// Head of an intrusive doubly-linked list.
#[derive(Clone, Copy, Default)]
pub struct ListHead {
    pub first: u32,
    pub last: u32,
}

impl ListHead {
    pub const fn new() -> Self {
        Self { first: NIL, last: NIL }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.first == NIL
    }

    /// Push to back. O(1).
    pub fn push_back<T: ListLinked>(&mut self, entries: &mut [T], idx: u32) {
        debug_assert!(idx != NIL);
        let entry = &mut entries[idx as usize];
        entry.set_prev(self.last);
        entry.set_next(NIL);

        if self.last != NIL {
            entries[self.last as usize].set_next(idx);
        } else {
            self.first = idx;
        }
        self.last = idx;
    }

    /// Remove by index. O(1).
    pub fn remove<T: ListLinked>(&mut self, entries: &mut [T], idx: u32) {
        debug_assert!(idx != NIL);
        let prev = entries[idx as usize].prev();
        let next = entries[idx as usize].next();

        if prev != NIL {
            entries[prev as usize].set_next(next);
        } else {
            self.first = next;
        }

        if next != NIL {
            entries[next as usize].set_prev(prev);
        } else {
            self.last = prev;
        }

        // Clear links
        entries[idx as usize].set_prev(NIL);
        entries[idx as usize].set_next(NIL);
    }

    /// Pop from front. O(1).
    pub fn pop_front<T: ListLinked>(&mut self, entries: &mut [T]) -> Option<u32> {
        if self.first == NIL {
            return None;
        }

        let idx = self.first;
        let next = entries[idx as usize].next();

        self.first = next;
        if next != NIL {
            entries[next as usize].set_prev(NIL);
        } else {
            self.last = NIL;
        }

        entries[idx as usize].set_prev(NIL);
        entries[idx as usize].set_next(NIL);

        Some(idx)
    }
}

#[cfg(test)]
mod tests {
    use crate::intrusive::links::{IntrusiveLinks, Linked};

    #[derive(Default)]
    struct TestEntry {
        links: IntrusiveLinks,
    }

    impl Linked for TestEntry {
        fn links(&self) -> &IntrusiveLinks { &self.links }
        fn links_mut(&mut self) -> &mut IntrusiveLinks { &mut self.links }
    }

    fn make_entries(n: usize) -> Vec<TestEntry> {
        (0..n).map(|_| TestEntry { links: IntrusiveLinks::new() }).collect()
    }

    #[test]
    fn test_push_back_single() {
        let mut entries = make_entries(5);
        let mut list = super::ListHead::new();

        list.push_back(&mut entries, 0);

        assert!(!list.is_empty());
        assert_eq!(list.first, 0);
        assert_eq!(list.last, 0);
    }

    #[test]
    fn test_push_back_multiple() {
        let mut entries = make_entries(5);
        let mut list = super::ListHead::new();

        list.push_back(&mut entries, 0);
        list.push_back(&mut entries, 1);
        list.push_back(&mut entries, 2);

        assert_eq!(list.first, 0);
        assert_eq!(list.last, 2);

        // Check forward links
        assert_eq!(crate::intrusive::links::ListLinked::next(&entries[0]), 1);
        assert_eq!(crate::intrusive::links::ListLinked::next(&entries[1]), 2);
        assert_eq!(crate::intrusive::links::ListLinked::next(&entries[2]), crate::intrusive::links::NIL);

        // Check backward links
        assert_eq!(crate::intrusive::links::ListLinked::prev(&entries[0]), crate::intrusive::links::NIL);
        assert_eq!(crate::intrusive::links::ListLinked::prev(&entries[1]), 0);
        assert_eq!(crate::intrusive::links::ListLinked::prev(&entries[2]), 1);
    }

    #[test]
    fn test_pop_front() {
        let mut entries = make_entries(5);
        let mut list = super::ListHead::new();

        list.push_back(&mut entries, 0);
        list.push_back(&mut entries, 1);
        list.push_back(&mut entries, 2);

        assert_eq!(list.pop_front(&mut entries), Some(0));
        assert_eq!(list.first, 1);
        assert_eq!(crate::intrusive::links::ListLinked::prev(&entries[1]), crate::intrusive::links::NIL);

        assert_eq!(list.pop_front(&mut entries), Some(1));
        assert_eq!(list.pop_front(&mut entries), Some(2));
        assert_eq!(list.pop_front(&mut entries), None);
        assert!(list.is_empty());
    }

    #[test]
    fn test_remove_middle() {
        let mut entries = make_entries(5);
        let mut list = super::ListHead::new();

        list.push_back(&mut entries, 0);
        list.push_back(&mut entries, 1);
        list.push_back(&mut entries, 2);

        list.remove(&mut entries, 1);

        assert_eq!(list.first, 0);
        assert_eq!(list.last, 2);
        assert_eq!(crate::intrusive::links::ListLinked::next(&entries[0]), 2);
        assert_eq!(crate::intrusive::links::ListLinked::prev(&entries[2]), 0);
    }

    #[test]
    fn test_remove_first() {
        let mut entries = make_entries(5);
        let mut list = super::ListHead::new();

        list.push_back(&mut entries, 0);
        list.push_back(&mut entries, 1);
        list.push_back(&mut entries, 2);

        list.remove(&mut entries, 0);

        assert_eq!(list.first, 1);
        assert_eq!(crate::intrusive::links::ListLinked::prev(&entries[1]), crate::intrusive::links::NIL);
    }

    #[test]
    fn test_remove_last() {
        let mut entries = make_entries(5);
        let mut list = super::ListHead::new();

        list.push_back(&mut entries, 0);
        list.push_back(&mut entries, 1);
        list.push_back(&mut entries, 2);

        list.remove(&mut entries, 2);

        assert_eq!(list.last, 1);
        assert_eq!(crate::intrusive::links::ListLinked::next(&entries[1]), crate::intrusive::links::NIL);
    }
}
