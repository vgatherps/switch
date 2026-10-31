//! Latency/load bench: an open-loop Zhida generator with a rate sweep, Zhida and kernel echo
//! servers. See plans/bench/design.md and README.md.

pub mod coord;
pub mod hist;
pub mod host;
pub mod kernel;
pub mod load;
pub mod pace;
pub mod pingpong;
pub mod record;
pub mod schedule;
pub mod serve;
pub mod sweep;
pub mod wire;
pub mod wsclient;
