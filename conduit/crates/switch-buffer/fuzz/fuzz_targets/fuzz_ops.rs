#![no_main]

use libfuzzer_sys::fuzz_target;
use switch_buffer::testing::{Op, fuzz_ops};

fuzz_target!(|ops: Vec<Op>| {
    fuzz_ops(&ops);
});
