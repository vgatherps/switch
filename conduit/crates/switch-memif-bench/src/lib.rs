//! Measures two `dpdk-smoltcp` Stacks exchanging TCP and UDP traffic with no NIC and no kernel in between.
//!
//! The link between the Stacks is DPDK's memif device (`net_memif`): a virtual NIC whose two ends exchange packets through shared memory. This library supplies that link as a switch `Port` ([`memif`]), the workloads the benchmark runs between the two Stacks ([`workload`]: ping-pong, bulk and one-way UDP), and the fixed set of configurations the matrix runs ([`matrix`]). Most readers run the `switch-memif-bench` command rather than use the library.
//!
//! Each Stack runs in its own process with its own EAL on its own CPU. `run` launches the memif server (peer) and the measuring client and orders their shutdown; `matrix` runs every configuration several times and prints min/median/max per configuration. Build and run it in the DPDK environment, from the repository root:
//!
//! ```text
//! cargo build --locked --release -p switch-memif-bench
//! target/release/switch-memif-bench run --shape pingpong --conns 1 --message 64 \
//!     --rounds 100000 --warmup 10000 --peer-cpu 4 --client-cpu 5 --control-cpu 3
//! target/release/switch-memif-bench matrix --peer-cpu 4 --client-cpu 5 --control-cpu 3
//! ```
//!
//! See the switch book, chapter "Stack-to-stack throughput with switch-memif-bench".

pub mod matrix;
pub mod memif;
pub mod workload;
