//! Latency/load bench: an open-loop Zhida generator with a rate sweep and a Zhida echo server.
//! See plans/bench/design.md and README.md.

pub mod coord;
pub mod hist;
pub mod host;
pub mod load;
pub mod pace;
pub mod record;
pub mod schedule;
pub mod serve;
pub mod wire;
pub mod wsclient;
