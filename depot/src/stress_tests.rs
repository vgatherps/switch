//! Stress tests using arena-backed heap.

use crate::SlabHeap;
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use std::collections::BinaryHeap;

fn stress_heap(numbers: &[u32], check_freq: usize) {
    let mut slab_heap = SlabHeap::<u32>::new();
    let mut std_heap: BinaryHeap<u32> = BinaryHeap::new();

    let mut rng = SmallRng::seed_from_u64(12345);

    for (i, &num) in numbers.iter().enumerate() {
        let size = slab_heap.len().max(1);
        let pop_prob = (1.0 / 20.0_f64).min(1.0 / size as f64);

        if rng.r#gen::<f64>() < pop_prob && size > 0 {
            let slab_val = slab_heap.pop();
            let std_val = std_heap.pop();
            assert_eq!(slab_val, std_val, "pop mismatch at op {i}");
        } else {
            slab_heap.push(num);
            std_heap.push(num);
        }

        assert_eq!(
            slab_heap.peek(),
            std_heap.peek().copied(),
            "peek mismatch at op {i}"
        );

        if check_freq > 0 && i % check_freq == 0 {
            slab_heap.check_invariants();
        }
    }

    while let Some(slab_val) = slab_heap.pop() {
        let std_val = std_heap.pop();
        assert_eq!(Some(slab_val), std_val, "drain mismatch");
    }
    assert!(std_heap.is_empty());
}

#[test]
fn stress_1k() {
    let mut rng = SmallRng::seed_from_u64(42);
    let numbers: Vec<u32> = (0..1000).map(|_| rng.r#gen_range(0..500)).collect();
    stress_heap(&numbers, 100);
}

#[test]
fn stress_10k() {
    let mut rng = SmallRng::seed_from_u64(42);
    let numbers: Vec<u32> = (0..10_000).map(|_| rng.r#gen_range(0..5000)).collect();
    stress_heap(&numbers, 1000);
}

#[test]
fn stress_64k() {
    let mut rng = SmallRng::seed_from_u64(42);
    let numbers: Vec<u32> = (0..65_536).map(|_| rng.r#gen_range(0..30000)).collect();
    stress_heap(&numbers, 5000);
}

#[test]
#[cfg(debug_assertions)]
fn stress_64k_paranoid() {
    let mut rng = SmallRng::seed_from_u64(42);
    let numbers: Vec<u32> = (0..65_536).map(|_| rng.r#gen_range(0..30000)).collect();
    stress_heap(&numbers, 1);
}
