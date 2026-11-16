//! Benchmark library modules; see `src/main.rs` for the command.

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
