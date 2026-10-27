#![no_main]

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use depot::{HeapOp, run_heap_oracle};

#[derive(Debug, Arbitrary)]
struct HeapInput {
    prewarm: u16,
    ops: Vec<FuzzOp>,
}

#[derive(Debug, Clone, Copy, Arbitrary)]
enum FuzzOp {
    Push(u16),
    Pop,
}

fuzz_target!(|input: HeapInput| {
    let ops: Vec<HeapOp> = input.ops.iter().map(|op| match *op {
        FuzzOp::Push(v) => HeapOp::Push(v),
        FuzzOp::Pop => HeapOp::Pop,
    }).collect();
    run_heap_oracle(input.prewarm, &ops);
});
