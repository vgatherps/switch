//! Property-based tests for SlabHeap using QuickCheck.

use depot::{HeapOp, run_heap_oracle};
use quickcheck::{Arbitrary, Gen};
use quickcheck_macros::quickcheck;

#[derive(Debug, Clone)]
struct HeapInput {
    prewarm: u16,
    ops: Vec<HeapOp>,
}

impl Arbitrary for HeapInput {
    fn arbitrary(g: &mut Gen) -> Self {
        let prewarm = u16::arbitrary(g) % 1000;
        let len = usize::arbitrary(g) % 500;
        let ops = (0..len)
            .map(|_| {
                if bool::arbitrary(g) || bool::arbitrary(g) {
                    HeapOp::Push(u16::arbitrary(g))
                } else {
                    HeapOp::Pop
                }
            })
            .collect();
        HeapInput { prewarm, ops }
    }
}

#[quickcheck]
fn heap_matches_oracle(input: HeapInput) -> bool {
    run_heap_oracle(input.prewarm, &input.ops);
    true
}
