//! `switch-bench`: an open-loop latency benchmark for switch echo servers.
//!
//! The binary offers a fixed rate of small UDP, TCP or `ws://` messages to an echo receiver, records how long each echo takes from the message's scheduled send time and how many never come back, and raises the rate step by step. `sweep` is the coordinator: it starts the `gen` worker processes, runs each rate on all of them and prints one JSON line per step and a summary line. `serve` is the switch echo receiver on the local backend (`--backend conduit --config F`) or the junction backend (`--backend junction --junction-socket PATH`); `kernel-serve` is a Linux kernel-socket receiver for comparison. `switch-bench/report.py` turns the JSON lines into a report. The library modules are public only for the crate's tests.
//!
//! ```text
//! switch-bench sweep --proto udp|tcp|ws --gens G --gen-cmd CMD --senders N
//!                    (--rate-min PPS --rate-max PPS | --rates PPS,PPS,...)
//!                    [--step-ms 2000] [--warmup-ms 500] [--drain-ms 200] [--budget-us W]
//! switch-bench gen   [--backend conduit --config F | --backend junction --junction-socket PATH]
//!                    --proto udp|tcp|ws --target IP:PORT --senders N --size S [--local-port 40000]
//! switch-bench serve [--backend conduit --config F | --backend junction --junction-socket PATH]
//!                    --proto udp|tcp|ws --port P
//! switch-bench kernel-serve --proto udp|tcp|ws --bind IP --port P
//! ```
//!
//! See the switch book, chapter "Measuring latency under load".

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
