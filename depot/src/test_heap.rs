//! Generic max-heap backed by slab allocator for testing.
//!
//! Uses explicit parent/left/right pointers. Allocator corruption
//! will poison the tree structure and cause test failures.

use crate::{DepotHandle, fixed_depot_type};
use std::collections::{BinaryHeap, VecDeque};

type HeapDepot = fixed_depot_type!(131072);

const HEAP_CAPACITY: usize = 131072 - 1; // sentinel takes slot 0

#[derive(Debug, Clone, Copy)]
pub enum HeapOp {
    Push(u16),
    Pop,
}

pub fn run_heap_oracle(prewarm: u16, ops: &[HeapOp]) {
    let mut slab = SlabHeap::<u16>::new();
    let mut oracle = BinaryHeap::new();

    for i in 0..prewarm {
        slab.push(i);
        oracle.push(i);
    }

    for (i, op) in ops.iter().enumerate() {
        match *op {
            HeapOp::Push(v) => {
                if slab.push(v).is_none() {
                    assert_eq!(slab.len(), HEAP_CAPACITY, "alloc failed before capacity");
                    break;
                }
                oracle.push(v);
            }
            HeapOp::Pop => {
                assert_eq!(slab.pop(), oracle.pop(), "pop mismatch at op {i}");
            }
        }
        assert_eq!(
            slab.peek(),
            oracle.peek().copied(),
            "peek mismatch at op {i}"
        );
        assert_eq!(slab.len(), oracle.len(), "len mismatch at op {i}");
    }

    while let Some(v) = slab.pop() {
        assert_eq!(Some(v), oracle.pop(), "drain mismatch");
    }
    assert!(oracle.is_empty());
    slab.check_invariants();
}

#[derive(Debug, Clone)]
struct HeapNode<T> {
    value: T,
    parent: Option<DepotHandle>,
    left: Option<DepotHandle>,
    right: Option<DepotHandle>,
}

/// Max-heap backed by slab allocator with explicit tree pointers.
pub struct SlabHeap<T> {
    alloc: Box<HeapDepot>,
    nodes: Vec<Option<HeapNode<T>>>,
    root: Option<DepotHandle>,
    len: usize,
    /// Nodes with < 2 children. Front = next insert parent, back = leaf for pop.
    open: VecDeque<DepotHandle>,
}

impl<T: Ord + Copy + std::fmt::Debug> SlabHeap<T> {
    pub fn new() -> Self {
        Self {
            alloc: Box::new(HeapDepot::new()),
            nodes: Vec::new(),
            root: None,
            len: 0,
            open: VecDeque::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn peek(&self) -> Option<T> {
        self.root.map(|idx| self.get(idx).value)
    }

    fn ensure_capacity(&mut self, idx: DepotHandle) {
        let i = idx.slot() as usize;
        if i >= self.nodes.len() {
            self.nodes.resize_with(i + 1, || None);
        }
    }

    fn get(&self, idx: DepotHandle) -> &HeapNode<T> {
        self.nodes[idx.slot() as usize]
            .as_ref()
            .expect("node slot empty")
    }

    fn get_mut(&mut self, idx: DepotHandle) -> &mut HeapNode<T> {
        self.nodes[idx.slot() as usize]
            .as_mut()
            .expect("node slot empty")
    }

    pub fn push(&mut self, value: T) -> Option<DepotHandle> {
        let idx = self.alloc.alloc()?;
        self.ensure_capacity(idx);

        let i = idx.slot() as usize;
        assert!(
            self.nodes[i].is_none(),
            "allocated slot {idx:?} not empty - double allocation!"
        );

        if self.root.is_none() {
            self.nodes[i] = Some(HeapNode {
                value,
                parent: None,
                left: None,
                right: None,
            });
            self.root = Some(idx);
            self.open.push_back(idx);
            self.len += 1;
            return Some(idx);
        }

        let parent_idx = *self.open.front().unwrap();
        let is_left = self.get(parent_idx).left.is_none();

        self.nodes[i] = Some(HeapNode {
            value,
            parent: Some(parent_idx),
            left: None,
            right: None,
        });

        let parent = self.get_mut(parent_idx);
        if is_left {
            parent.left = Some(idx);
        } else {
            parent.right = Some(idx);
        }

        if self.get(parent_idx).left.is_some() && self.get(parent_idx).right.is_some() {
            self.open.pop_front();
        }

        self.open.push_back(idx);
        self.sift_up(idx);
        self.len += 1;
        Some(idx)
    }

    pub fn pop(&mut self) -> Option<T> {
        let root_idx = self.root?;
        let root_value = self.get(root_idx).value;

        if self.len == 1 {
            self.nodes[root_idx.slot() as usize] = None;
            self.alloc.free(root_idx).unwrap();
            self.root = None;
            self.open.clear();
            self.len = 0;
            return Some(root_value);
        }

        let leaf_idx = *self.open.back().unwrap();
        debug_assert_ne!(leaf_idx, root_idx);

        let leaf_parent = self.get(leaf_idx).parent.unwrap();
        let parent_was_full = {
            let p = self.get(leaf_parent);
            p.left.is_some() && p.right.is_some()
        };

        {
            let parent = self.get_mut(leaf_parent);
            if parent.right == Some(leaf_idx) {
                parent.right = None;
            } else {
                parent.left = None;
            }
        }

        let leaf_value = self.get(leaf_idx).value;
        self.get_mut(root_idx).value = leaf_value;

        self.nodes[leaf_idx.slot() as usize] = None;
        self.alloc.free(leaf_idx).unwrap();
        self.open.pop_back();

        if parent_was_full {
            self.open.push_front(leaf_parent);
        }

        self.len -= 1;
        self.sift_down(root_idx);

        Some(root_value)
    }

    fn sift_up(&mut self, mut idx: DepotHandle) {
        while let Some(parent_idx) = self.get(idx).parent {
            let idx_val = self.get(idx).value;
            let parent_val = self.get(parent_idx).value;

            if idx_val <= parent_val {
                break;
            }

            self.get_mut(idx).value = parent_val;
            self.get_mut(parent_idx).value = idx_val;

            idx = parent_idx;
        }
    }

    fn sift_down(&mut self, mut idx: DepotHandle) {
        loop {
            let node = self.get(idx);
            let idx_val = node.value;
            let left = node.left;
            let right = node.right;

            let mut largest_idx = idx;
            let mut largest_val = idx_val;

            if let Some(l) = left {
                let l_val = self.get(l).value;
                if l_val > largest_val {
                    largest_idx = l;
                    largest_val = l_val;
                }
            }

            if let Some(r) = right {
                let r_val = self.get(r).value;
                if r_val > largest_val {
                    largest_idx = r;
                    largest_val = r_val;
                }
            }

            if largest_idx == idx {
                break;
            }

            self.get_mut(idx).value = largest_val;
            self.get_mut(largest_idx).value = idx_val;

            idx = largest_idx;
        }
    }

    pub fn check_invariants(&self) {
        self.alloc.check_invariants();

        if self.root.is_none() {
            assert_eq!(self.len, 0);
            assert!(self.open.is_empty());
            return;
        }

        let mut count = 0;
        let mut open_expected = 0;
        let mut stack = vec![(self.root.unwrap(), None::<DepotHandle>)];

        while let Some((idx, expected_parent)) = stack.pop() {
            count += 1;
            let node = self.get(idx);

            assert_eq!(node.parent, expected_parent, "node {idx:?} parent mismatch");

            let has_left = node.left.is_some();
            let has_right = node.right.is_some();

            assert!(
                has_left || !has_right,
                "node {idx:?} has right child but no left"
            );

            if !has_left || !has_right {
                open_expected += 1;
            }

            if let Some(l) = node.left {
                assert!(
                    node.value >= self.get(l).value,
                    "heap violation: {:?} < left {:?}",
                    node.value,
                    self.get(l).value
                );
                stack.push((l, Some(idx)));
            }
            if let Some(r) = node.right {
                assert!(
                    node.value >= self.get(r).value,
                    "heap violation: {:?} < right {:?}",
                    node.value,
                    self.get(r).value
                );
                stack.push((r, Some(idx)));
            }
        }

        assert_eq!(count, self.len);
        assert_eq!(self.open.len(), open_expected, "open queue size mismatch");

        for &handle in &self.open {
            let node = self.get(handle);
            assert!(
                node.left.is_none() || node.right.is_none(),
                "node {handle:?} in open queue but has 2 children"
            );
        }
    }
}

impl<T: Ord + Copy + std::fmt::Debug> Default for SlabHeap<T> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_push_pop_single() {
        let mut heap = SlabHeap::new();
        heap.push(42u32);
        assert_eq!(heap.peek(), Some(42));
        assert_eq!(heap.pop(), Some(42));
        assert_eq!(heap.len(), 0);
    }

    #[test]
    fn test_max_heap_property() {
        let mut heap = SlabHeap::new();
        heap.push(5u32);
        heap.push(10);
        heap.push(3);
        heap.push(8);

        assert_eq!(heap.pop(), Some(10));
        assert_eq!(heap.pop(), Some(8));
        assert_eq!(heap.pop(), Some(5));
        assert_eq!(heap.pop(), Some(3));
    }

    #[test]
    fn pop_on_empty() {
        let mut heap = SlabHeap::<u32>::new();
        for _ in 0..10 {
            assert_eq!(heap.pop(), None);
        }
    }

    #[test]
    fn monotonic_ascending() {
        let mut heap = SlabHeap::new();
        for i in 0u32..200 {
            heap.push(i);
        }
        let mut prev = heap.pop().unwrap();
        while let Some(v) = heap.pop() {
            assert!(prev >= v, "not descending: {prev} < {v}");
            prev = v;
        }
    }

    #[test]
    fn monotonic_descending() {
        let mut heap = SlabHeap::new();
        for i in (0u32..200).rev() {
            heap.push(i);
        }
        let mut prev = heap.pop().unwrap();
        while let Some(v) = heap.pop() {
            assert!(prev >= v, "not descending: {prev} < {v}");
            prev = v;
        }
    }

    #[test]
    fn duplicates_allowed() {
        let mut heap = SlabHeap::new();
        for _ in 0..100 {
            assert!(heap.push(42u32).is_some());
        }
        assert_eq!(heap.len(), 100);
        for _ in 0..100 {
            assert_eq!(heap.pop(), Some(42));
        }
        assert!(heap.is_empty());
    }

    #[test]
    fn alternating_push_pop() {
        let mut heap = SlabHeap::new();
        for i in 0u32..100 {
            heap.push(i);
            assert_eq!(heap.pop(), Some(i));
            assert!(heap.is_empty());
        }
    }

    #[test]
    fn heap_reuses_slot_across_generation_wrap() {
        let mut heap = SlabHeap::new();
        let first = heap.push(1u32).unwrap();
        assert_eq!(heap.pop(), Some(1));

        for value in 2..=127u32 {
            let current = heap.push(value).unwrap();
            assert_eq!(current.slot(), first.slot());
            assert_eq!(heap.pop(), Some(value));
        }

        let wrapped = heap.push(128).unwrap();
        assert_eq!(wrapped, first);
        assert_eq!(heap.pop(), Some(128));
        heap.check_invariants();
    }

    #[test]
    fn extreme_values() {
        let mut heap = SlabHeap::new();
        heap.push(i32::MIN);
        heap.push(i32::MAX);
        heap.push(0i32);
        assert_eq!(heap.pop(), Some(i32::MAX));
        assert_eq!(heap.pop(), Some(0));
        assert_eq!(heap.pop(), Some(i32::MIN));
    }

    #[test]
    fn fill_and_drain() {
        let mut heap = SlabHeap::new();
        let mut count = 0u32;
        loop {
            if heap.push(count).is_none() {
                break;
            }
            count += 1;
        }
        assert!(count > 0);
        assert_eq!(heap.len(), count as usize);

        let mut drained = 0;
        while heap.pop().is_some() {
            drained += 1;
        }
        assert_eq!(drained, count);
        assert!(heap.is_empty());
        heap.check_invariants();
    }
}
