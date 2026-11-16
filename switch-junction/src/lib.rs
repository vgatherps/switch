//! The client side of `switch-junctiond`, the daemon that owns one network card
//! and lets several programs on the same machine use it. A [`session::Session`] connects to
//! the daemon and opens TCP and UDP sockets on its card; packet data moves through shared
//! memory. Applications reach this as `switch::junction`, and usually hand a connected
//! `Session` to `Net::remote` in `switch::net` instead of using it directly.
//!
//! [`control`] and [`send`] are the wire format, for writing another client. [`host`] is the
//! daemon's side of the control socket, public for the daemon and in-process test fixtures.

pub mod control;
pub mod host;
pub mod rx;
pub mod send;
pub mod session;

#[cfg(doctest)]
mod api_guards {
    //! A session cannot move to another thread:
    //!
    //! ```compile_fail,E0277
    //! fn require_send<T: Send>() {}
    //! require_send::<switch_junction::session::Session>();
    //! ```
}
