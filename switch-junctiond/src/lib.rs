//! `switch-junctiond` owns one NIC port and serves TCP and UDP sockets to applications on the same machine, through a Unix control socket and shared-memory rings in its directory. Linux only. Start it as root with its main thread pinned to exactly one CPU; it runs until SIGINT or SIGTERM. Apart from DPDK's own `EAL:` lines while the NIC opens, it prints nothing; its own errors are one line on stderr, `switch-junctiond: <reason>`.
//!
//! ```text
//! taskset -c 2 switch-junctiond --config nic.json [--dir /run/switch-junctiond] [--rx-size 2097152]
//!     [--max-sessions 64] [--max-sockets 1024] [--tcp-sockets 1024] [--udp-sockets 1024]
//!     [--control-cpus 0]
//! ```
//!
//! Applications connect to a running daemon with the `switch-junction` crate. This library holds the daemon's network loop: `network::Network` busy-polls one Stack, applies the operations and send records of every connected session, and writes received packets into each session's receive ring. It is public so in-process test fixtures can run the daemon's loop without starting the binary.
//!
//! See the switch book, chapter "Running switch-junctiond".

pub mod network;
mod sockets;
