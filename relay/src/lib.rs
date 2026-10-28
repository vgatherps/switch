//! WebSocket (RFC 6455) over frames. Received messages borrow the frame
//! bytes in place; only messages spanning frames or fragments are copied,
//! into one reusable scratch buffer. Sent payloads are written straight into
//! the frame buffer, behind the header.

mod lower;
mod raw;
mod rx;
mod tx;
mod utf8;

pub use lower::{Chunk, PlainRx, PlainTx, RxFrame, TxFrame};
pub use raw::Raw;
pub use rx::{Config, Kind, Message, ProtocolError, Role, Taken, WsError, WsRx};
pub use tx::{OutFrame, TxError, WsTx};
