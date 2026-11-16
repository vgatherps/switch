//! Latency/load bench: an open-loop switch generator with a rate sweep and a switch echo server.
//! See plans/bench/design.md and README.md.

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
