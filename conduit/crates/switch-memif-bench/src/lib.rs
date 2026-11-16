//! Measures two `dpdk-smoltcp` Stacks exchanging TCP and UDP traffic with no NIC and no kernel in between.
//!
//! The link between the Stacks is DPDK's memif device (`net_memif`): a virtual NIC whose two ends exchange packets through shared memory. This library supplies that link as a switch `Port` ([`memif`]), the workloads the benchmark runs between the two Stacks ([`workload`]: ping-pong, bulk and one-way UDP), and the fixed set of configurations the matrix runs ([`matrix`]). Most readers run the `switch-memif-bench` command rather than use the library.
//!
//! Each Stack runs in its own process with its own EAL on its own CPU. How to run the benchmark and read its tables is in `docs/benchmarks/memif.md`.

pub mod matrix;
pub mod memif;
pub mod workload;
