//! Reads and writes the switch network config (`dut.json`), discovers it from a
//! Linux interface, and binds the NIC to vfio-pci. The `switch-netconf` binary
//! wraps these steps; `crates/switch-nic/README.md` walks through them and
//! `docs/USAGE.md` shows how an application loads the config.

mod aws;
mod config;
pub mod discover;
pub mod imds;

pub use config::{Config, ConfigError, Device, EalConfig, Offloads, PathRule, PciDevice};
pub mod devbind;
pub mod rediscover;
