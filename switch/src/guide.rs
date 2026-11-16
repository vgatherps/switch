#![doc = include_str!("../docs/guide/index.md")]

#[doc = include_str!("../docs/guide/first-task.md")]
pub mod first_task {}

#[doc = include_str!("../docs/guide/setup.md")]
pub mod setup {}

#[cfg(feature = "net")]
#[doc = include_str!("../docs/guide/first-connection.md")]
pub mod first_connection {}

#[doc = include_str!("../docs/guide/tasks.md")]
pub mod tasks {}

#[cfg(feature = "net")]
#[doc = include_str!("../docs/guide/drive-loop.md")]
pub mod drive_loop {}

#[cfg(feature = "net")]
#[doc = include_str!("../docs/guide/sockets.md")]
pub mod sockets {}

#[cfg(feature = "net")]
#[doc = include_str!("../docs/guide/frames.md")]
pub mod frames {}

#[cfg(feature = "net")]
#[doc = include_str!("../docs/guide/tcp.md")]
pub mod tcp {}

#[cfg(feature = "net")]
#[doc = include_str!("../docs/guide/udp.md")]
pub mod udp {}

#[cfg(feature = "net")]
#[doc = include_str!("../docs/guide/timeouts.md")]
pub mod timeouts {}

#[cfg(feature = "net")]
#[doc = include_str!("../docs/guide/shutdown.md")]
pub mod shutdown {}

#[cfg(feature = "ws")]
#[doc = include_str!("../docs/guide/websocket.md")]
pub mod websocket {}

#[cfg(feature = "tls")]
#[doc = include_str!("../docs/guide/tls.md")]
pub mod tls {}

#[cfg(feature = "net")]
#[doc = include_str!("../docs/guide/sharing.md")]
pub mod sharing {}

#[cfg(feature = "ws")]
#[doc = include_str!("../docs/guide/feed.md")]
pub mod feed {}
