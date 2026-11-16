//! memif benchmark support: a switch Port on a net_memif vdev, the benchmark
//! workloads between two switch Stacks, and the configuration matrix the
//! `switch-memif-bench` binary runs.

pub mod matrix;
pub mod memif;
pub mod workload;
