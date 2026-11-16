//! The network config file: the JSON file that describes one NIC port for a program that
//! owns the card, and the library behind the `switch-netconf` command.
//!
//! Applications reach [`Config`] and its parts as `switch::nic::Config` and so on, and pass
//! a loaded `Config` to `switch::nic::open`. The file's keys and rules are described for
//! operators in `docs/operations/config.md`. The modules [`discover`], [`rediscover`],
//! [`devbind`] and [`imds`] are the command's internals. The crate needs no DPDK.

mod aws;
mod config;
pub mod discover;
pub mod imds;

pub use config::{Config, ConfigError, Device, EalConfig, Offloads, PathRule, PciDevice};
pub mod devbind;
pub mod rediscover;
