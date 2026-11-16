//! The network config file: the JSON file (`dut.json` in the examples) that describes one NIC port for a program that owns the card. It names the device, the Stack's IPv4 address and gateway, the MTU and per-destination path MTUs, the checksum offloads, receive timestamps, ring sizes and DPDK settings. `switch-open` opens a card from it, and the `switch-netconf` command writes, checks and applies it. This crate does not use DPDK and builds on any platform.
//!
//! [`Config`] is the parsed file. [`Config::load`] reads and validates it and returns a [`ConfigError`] naming the offending key; unknown keys are rejected, `version` must be 1, and `mac` and `gateway` must be present even when `null`. [`Device`] is either a PCI card ([`PciDevice`]) or a DPDK virtual device string such as `net_tap0,iface=cdnic0`. [`Config::path_mtu`] answers the same per-destination lookup the Stack uses, so a program can cap a TCP socket's MSS at `path_mtu(addr) - 40` before it opens the card. The modules [`discover`], [`rediscover`] and [`devbind`] are what the command's `discover`, `discover -o` and `bind`/`unbind` subcommands call; they read Linux's `/sys` and `/proc` through a [`discover::Roots`] value so tests can point them at a fake tree.
//!
//! ```rust,no_run
//! use std::path::Path;
//! use smoltcp::wire::Ipv4Address;
//!
//! let config = switch_netconf::Config::load(Path::new("dut.json"))?;
//! let server = Ipv4Address::new(192, 0, 2, 1);
//! let mss_cap = config.path_mtu(server) - 40;
//! println!("{config}");
//! println!("MSS toward {server}: at most {mss_cap}");
//! # Ok::<(), switch_netconf::ConfigError>(())
//! ```
//!
//! The `switch-netconf` command: checks a network config file, writes one from a Linux interface, and moves a PCI card between its kernel driver and `vfio-pci` so a DPDK program can open it. `check` prints the config summary or the first rule it breaks; `discover` reads the interface's address, MAC, MTU, PCI details and gateway (with `--aws`, also EC2 instance metadata) and prints the config or merges it into an existing file with `-o`; `bind` refuses before writing anything unless every check passes, and puts the kernel driver back if a later step fails; `unbind` returns the card to its kernel driver. `bind` and `unbind` need root. Exit status: 0 success, 1 failure (one `switch-netconf:` line on stderr), 2 usage error.
//!
//! ```text
//! switch-netconf check FILE
//! switch-netconf discover --interface IF [--ipv4 ADDR] [--aws] [-o FILE]
//! switch-netconf bind FILE [--noiommu] [--dry-run]
//! switch-netconf unbind FILE
//! ```
//!
//! See the switch book, chapters "The network config file" and "switch-netconf: check, discover, bind, unbind".

mod aws;
mod config;
pub mod discover;
pub mod imds;

pub use config::{Config, ConfigError, Device, EalConfig, Offloads, PathRule, PciDevice};
pub mod devbind;
pub mod rediscover;
