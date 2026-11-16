//! `switch-junctiond`: one process that owns a network card and serves TCP and UDP sockets
//! to programs on the same machine, which connect with `switch::junction::session::Session`.
//! Running it is described in `docs/operations/junctiond.md` in the repository.
//!
//! This library holds the daemon's network loop, [`network::Network`], which polls the card,
//! applies every session's socket operations and send records, and writes received packets
//! into each session's receive ring. It is public so in-process test fixtures can run the loop
//! without starting the binary.

pub mod network;
mod sockets;
