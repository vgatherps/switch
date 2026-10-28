//! WebSocket (RFC 6455) over received frames: messages borrow the frame
//! bytes in place; only messages spanning frames or fragments are copied,
//! into one reusable scratch buffer.

mod lower;
mod raw;
mod rx;
mod utf8;

pub use lower::{Chunk, PlainRx, RxFrame};
pub use raw::Raw;
pub use rx::{Config, Kind, Message, ProtocolError, Role, Taken, WsError, WsRx};
