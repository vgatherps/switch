//! The junction daemon's network loop. `network::Network` busy-polls one Stack, applies the operations and send records of every connected session, and writes received packets into each session's receive ring. It is public so in-process test fixtures can run the daemon's loop without starting the `switch-junctiond` binary; applications connect to a running daemon with the `switch-junction` crate instead.
//!
//!
//! See the switch book, chapter "Running switch-junctiond".

pub mod network;
mod sockets;
