#![doc = include_str!("../docs/index.md")]

pub mod guide;

pub use switch_runtime::{
    JoinHandle, PollReport, RemoteWaker, Runtime, RuntimeError, Sleep, SleepError, SpawnError,
    SpawnToken, Spawner, StoredWaker, YieldNow, boxed, oneshot, request_turn, yield_now,
};
#[doc(inline)]
pub use switch_macros::facade_task as task;

#[doc(hidden)]
pub use switch_runtime::__private;

#[cfg(feature = "net")]
pub mod net {
    #![doc = include_str!("../docs/net.md")]
    pub use switch_net::*;
}

#[cfg(feature = "net")]
pub mod junction {
    #![doc = include_str!("../docs/junction.md")]
    pub use switch_junction::{control, rx, session};
}

#[cfg(feature = "net")]
pub mod nic {
    #![doc = include_str!("../docs/nic.md")]
    pub use dpdk_smoltcp::{DEFAULT_NEIGHBOR_CAPACITY, Eal};
    pub use switch_netconf::{Config, ConfigError, Device, EalConfig, Offloads, PathRule, PciDevice};
    pub use switch_open::{Link, OpenError, OpenReport, Opened, Sizing, offloads, open};
}

#[cfg(feature = "tls")]
pub mod tls {
    #![doc = include_str!("../docs/tls.md")]
    /// The TLS library, with its `ring` provider. Build the `ClientConfig` that
    /// [`net::tls_connect`] and `switch::ws` take from it.
    pub use rustls;
    pub use switch_tls::*;
}

#[cfg(feature = "ws")]
pub mod ws {
    #![doc = include_str!("../docs/ws.md")]
    pub use switch_ws::*;
}
