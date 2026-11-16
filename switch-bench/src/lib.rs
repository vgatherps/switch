//! `switch-bench`: an open-loop latency benchmark for switch echo servers. The library holds the
//! `switch-bench` command's pieces; its modules are public only for the crate's tests.
//!
//! How the benchmark works and how to run it is in `docs/benchmarks/README.md`.

pub mod coord;
pub mod hist;
pub mod host;
#[cfg(target_os = "linux")]
pub mod kernel_serve;
pub mod load;
pub mod pace;
pub mod record;
pub mod schedule;
pub mod serve;
pub mod wire;
pub mod wsclient;
mod xstats;
