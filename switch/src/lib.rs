//! Application access to the caller-driven runtime and network crates.
//! The default features expose tasks, timers and runtime primitives without networking.
//! The `net` feature adds sockets, NIC setup and junction sessions.
//! The `tls` feature adds TLS over those sockets and enables `net`.
//! The `ws` feature adds WebSocket connections and enables `tls`.

pub use switch_runtime::{
    JoinHandle, PollReport, RemoteWaker, Runtime, RuntimeError, Sleep, SleepError, SpawnError,
    SpawnToken, Spawner, StoredWaker, YieldNow, boxed, oneshot, request_turn, yield_now,
};
pub use switch_macros::facade_task as task;

#[doc(hidden)]
pub use switch_runtime::__private;

#[cfg(feature = "net")]
#[doc(inline)]
pub use switch_net as net;

#[cfg(feature = "net")]
#[doc(inline)]
pub use switch_junction as junction;

#[cfg(feature = "net")]
pub mod nic {
    pub use dpdk_smoltcp::{DEFAULT_NEIGHBOR_CAPACITY, Eal};
    pub use switch_netconf::{Config, ConfigError, Device, EalConfig, Offloads, PathRule, PciDevice};
    pub use switch_open::{Link, OpenError, OpenReport, Opened, Sizing, offloads, open};
}

#[cfg(feature = "tls")]
#[doc(inline)]
pub use switch_tls as tls;

#[cfg(feature = "ws")]
#[doc(inline)]
pub use switch_ws as ws;
