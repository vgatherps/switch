use std::fmt;

use switch_netconf::{Config, Device};
use dpdk_smoltcp::smoltcp::iface::PathMtu;
use dpdk_smoltcp::smoltcp::time::{Duration, Instant};
use dpdk_smoltcp::smoltcp::wire::{EthernetAddress, Ipv4Address};
use dpdk_smoltcp::{
    device_info, os_random_seed, port_by_name, DeviceInfo, Eal, EalError, Port, PortError,
    PortOpenError, Stack, StackConfig, StackError,
};
use switch_dpdk_sys::{only_allowed_cpu, AffinityError};

use crate::offloads::{self, OffloadPlan};
use crate::{adjusted_descriptors, port_config, wait_link, Link, Sizing};

/// What a successful [`open`] returns.
pub struct Opened {
    /// The running Stack, ready for sockets. Its start time is the first value `open` read
    /// from the clock; every later drive call must take its time from the same clock.
    pub stack: Stack,
    /// DPDK's process setup. Keep it alive until the port is closed: the port and every
    /// packet buffer depend on it. Dropping it does nothing, and DPDK is never torn down.
    pub eal: Eal,
    /// What was opened, for printing.
    pub report: OpenReport,
}

/// Why [`open`] failed: one variant per step, in the order the steps run.
///
/// `Display` prints a short phrase naming the step, then the cause.
///
/// | Variant | Step | `Display` starts with | Carries |
/// |---|---|---|---|
/// | [`NotPinned`](Self::NotPinned) | check the thread may run on exactly one CPU | `CPU affinity:` | nothing |
/// | [`HelperAffinity`](Self::HelperAffinity) | place DPDK's helper threads | `EAL helper CPU affinity:` | nothing |
/// | [`Eal`](Self::Eal) | start DPDK | `EAL initialization:` | nothing |
/// | [`EalAffinityRestore`](Self::EalAffinityRestore) | start DPDK, then restore affinity | `EAL initialization:` | nothing |
/// | [`DeviceNotFound`](Self::DeviceNotFound) | find the device | `device lookup:` | nothing |
/// | [`DeviceInfo`](Self::DeviceInfo) | read the device's capabilities | `device information:` | nothing |
/// | [`MacMismatch`](Self::MacMismatch) | compare a PCI card's MAC with the config | `device MAC:` | nothing |
/// | [`Descriptors`](Self::Descriptors) | let the driver adjust the ring sizes | `descriptor adjustment:` | nothing |
/// | [`PortConfig`](Self::PortConfig) | size the pools | `port configuration:` | nothing |
/// | [`PortOpen`](Self::PortOpen) | open the NIC port | `port open:` | a retained `Port`, if any |
/// | [`Link`](Self::Link) | read the link state | `link query:` | the `Port` |
/// | [`LinkDown`](Self::LinkDown) | wait up to 10 s for the link | `link wait:` | the `Port` |
/// | [`Seed`](Self::Seed) | read a random seed | `stack random seed:` | the `Port` |
/// | [`Stack`](Self::Stack) | build the Stack | `Stack::new:` | the `Port` |
/// | [`GatewayUnresolved`](Self::GatewayUnresolved) | resolve the gateway by ARP, up to 3 s | `gateway` | the `Stack` |
/// | [`Receive`](Self::Receive) | resolve the gateway by ARP | `gateway receive:` | the `Stack` |
///
/// From `PortOpen` on, the port exists. Dropping the carried `Port` or `Stack` stops and
/// closes the device, reporting a failure only on stderr. To see the result, close it with
/// [`Port::try_close`](dpdk_smoltcp::Port::try_close) (for a `Stack`, after
/// [`Stack::into_port`](dpdk_smoltcp::Stack::into_port)); it reports buffers still held
/// as [`PortError::OutstandingBuffers`](dpdk_smoltcp::PortError::OutstandingBuffers).
pub enum OpenError {
    /// The calling thread may run on more or fewer than one CPU. Start the process with
    /// `taskset -c N`, or a container with `--cpuset-cpus=N`.
    NotPinned(AffinityError),
    /// DPDK's start-up failed. DPDK starts once per process: a second `open` in the same
    /// process fails here with `errno` `EALREADY`.
    Eal(EalError),
    /// Placing DPDK's helper threads failed: `helper_cpus` is empty or names a CPU at or
    /// above `CPU_SETSIZE`; a named CPU is outside the process's cpuset
    /// (`EAL helper CPUs are outside the effective cpuset`); `helper_cpus` is `Some` on a
    /// system other than Linux; `/sys/devices/system/cpu/isolated` or `online` could not be
    /// read or parsed; or setting the thread's affinity failed.
    HelperAffinity(std::io::Error),
    /// DPDK's start-up failed (`error`) and restoring the thread to its one CPU failed too
    /// (`restore`).
    EalAffinityRestore {
        error: EalError,
        restore: std::io::Error,
    },
    /// No DPDK port has the config's PCI address or vdev name.
    DeviceNotFound {
        /// The PCI address, or the vdev name (the vdev string before its first comma).
        name: String,
    },
    /// The driver did not report the device's capabilities or MAC address.
    DeviceInfo(PortError),
    /// The PCI card's MAC differs from the config's `mac`: the file was written for another
    /// card. Not checked for a vdev.
    MacMismatch {
        config: EthernetAddress,
        device: EthernetAddress,
    },
    /// The driver refused to adjust the descriptor counts (`rte_eth_dev_adjust_nb_rx_tx_desc`
    /// and its return code).
    Descriptors(String),
    /// A pool size does not fit DPDK's types: a pool count above `u32`, or an MTU so large
    /// that a buffer's data room exceeds `u16` (`pool data room exceeds u16`).
    PortConfig(String),
    /// Opening the NIC port failed. An offload the driver lacks fails here, naming it, for
    /// example `InvalidSettings("PMD lacks the requested TX TCP checksum offload")`.
    // The Port, Stack and failed-open payloads are boxed: an error does not carry them inline.
    PortOpen(Box<PortOpenError>),
    /// Reading the link state failed.
    Link {
        port_id: u16,
        port: Box<Port>,
        /// The DPDK call and its return code.
        error: String,
    },
    /// The link was still down after ten seconds.
    LinkDown {
        port_id: u16,
        port: Box<Port>,
        /// The last link state the driver reported.
        link: Link,
    },
    /// Reading the Stack's random seed from `/dev/urandom` failed.
    Seed {
        port: Box<Port>,
        error: std::io::Error,
    },
    /// [`Stack::new`](dpdk_smoltcp::Stack::new) refused: a zero [`Sizing`] field, a clock
    /// that returned a time before zero, an MTU below 88, or a pool below its reserve.
    Stack {
        port: Box<Port>,
        error: StackError,
    },
    /// The gateway did not answer ARP within three seconds, measured by the caller's clock.
    /// `Display`: `gateway <ip> unresolved after <waited>`.
    GatewayUnresolved {
        stack: Box<Stack>,
        gateway: Ipv4Address,
        waited: Duration,
    },
    /// A receive step failed while waiting for the gateway's ARP reply.
    Receive {
        stack: Box<Stack>,
        error: StackError,
    },
}

impl fmt::Debug for OpenError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotPinned(error) => f.debug_tuple("NotPinned").field(error).finish(),
            Self::Eal(error) => f.debug_tuple("Eal").field(error).finish(),
            Self::HelperAffinity(error) => f.debug_tuple("HelperAffinity").field(error).finish(),
            Self::EalAffinityRestore { error, restore } => f
                .debug_struct("EalAffinityRestore")
                .field("error", error)
                .field("restore", restore)
                .finish(),
            Self::DeviceNotFound { name } => {
                f.debug_struct("DeviceNotFound").field("name", name).finish()
            }
            Self::DeviceInfo(error) => f.debug_tuple("DeviceInfo").field(error).finish(),
            Self::MacMismatch { config, device } => f
                .debug_struct("MacMismatch")
                .field("config", config)
                .field("device", device)
                .finish(),
            Self::Descriptors(error) => f.debug_tuple("Descriptors").field(error).finish(),
            Self::PortConfig(error) => f.debug_tuple("PortConfig").field(error).finish(),
            Self::PortOpen(error) => f.debug_tuple("PortOpen").field(error).finish(),
            Self::Link {
                port_id,
                port,
                error,
            } => f
                .debug_struct("Link")
                .field("port_id", port_id)
                .field("port", port)
                .field("error", error)
                .finish(),
            Self::LinkDown {
                port_id,
                port,
                link,
            } => f
                .debug_struct("LinkDown")
                .field("port_id", port_id)
                .field("port", port)
                .field("link", link)
                .finish(),
            Self::Seed { port, error } => f
                .debug_struct("Seed")
                .field("port", port)
                .field("error", error)
                .finish(),
            Self::Stack { port, error } => f
                .debug_struct("Stack")
                .field("port", port)
                .field("error", error)
                .finish(),
            Self::GatewayUnresolved {
                gateway, waited, ..
            } => f
                .debug_struct("GatewayUnresolved")
                .field("gateway", gateway)
                .field("waited", waited)
                .finish_non_exhaustive(),
            Self::Receive { error, .. } => f
                .debug_struct("Receive")
                .field("error", error)
                .finish_non_exhaustive(),
        }
    }
}

impl fmt::Display for OpenError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotPinned(error) => write!(f, "CPU affinity: {error}"),
            Self::Eal(error) => write!(f, "EAL initialization: {error}"),
            Self::HelperAffinity(error) => write!(f, "EAL helper CPU affinity: {error}"),
            Self::EalAffinityRestore { error, restore } => write!(
                f,
                "EAL initialization: {error}; restoring polling CPU affinity: {restore}"
            ),
            Self::DeviceNotFound { name } => write!(f, "device lookup: {name:?} was not found"),
            Self::DeviceInfo(error) => write!(f, "device information: {error}"),
            Self::MacMismatch { config, device } => {
                write!(
                    f,
                    "device MAC: config {config} differs from device {device}"
                )
            }
            Self::Descriptors(error) => write!(f, "descriptor adjustment: {error}"),
            Self::PortConfig(error) => write!(f, "port configuration: {error}"),
            Self::PortOpen(error) => write!(f, "port open: {error}"),
            Self::Link { error, .. } => write!(f, "link query: {error}"),
            Self::LinkDown { .. } => write!(f, "link wait: link down after 10 seconds"),
            Self::Seed { error, .. } => write!(f, "stack random seed: {error}"),
            Self::Stack { error, .. } => write!(f, "Stack::new: {error}"),
            Self::GatewayUnresolved {
                gateway, waited, ..
            } => write!(f, "gateway {gateway} unresolved after {waited}"),
            Self::Receive { error, .. } => write!(f, "gateway receive: {error}"),
        }
    }
}

impl std::error::Error for OpenError {}

/// What [`open`] found and chose.
///
/// `Display` prints one `key: value` line per fact, in this order: `driver`, `mac`, `cpu`,
/// `port_id`, `rx_offload_capa`, `tx_offload_capa`, `max_rx_pktlen`, `min_mtu`, `max_mtu`,
/// `descriptors: rx=N tx=N`, `rx_pool_count`, `tx_pool_count`, the offload lines of
/// [`OffloadPlan`]'s `Display`, `link_speed_mbps`, `link_duplex` (`full` or `half`),
/// `link_status` (`up`), and `gateway: <ip> at <mac> after <n> ms` or `gateway: none`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenReport {
    /// The one CPU the calling thread may run on; DPDK's main thread runs there.
    pub cpu: usize,
    /// DPDK's port id for the device, as [`read_clock`](dpdk_smoltcp::read_clock) takes it.
    pub port_id: u16,
    /// What the driver reported: its name, offload capability bits, MTU range and MAC.
    pub device: DeviceInfo,
    /// The offloads requested and why.
    pub offloads: OffloadPlan,
    /// RX and TX descriptor counts after the driver's adjustment of the config's
    /// `descriptors`.
    pub descriptors: (u16, u16),
    /// Buffers in the RX pool (32767).
    pub rx_pool_count: u32,
    /// Buffers in the TX pool. The frame pool's size is not reported.
    pub tx_pool_count: u32,
    /// The link state when the wait ended.
    pub link: Link,
    /// With a configured gateway: its address, its MAC, and how long ARP took on the
    /// caller's clock. `None` without a gateway.
    pub gateway: Option<(Ipv4Address, EthernetAddress, Duration)>,
}

impl fmt::Display for OpenReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mac = self.device.mac;
        write!(f, "driver: {}", self.device.driver_name)?;
        write!(
            f,
            "\nmac: {:02x}:{:02x}:{:02x}:{:02x}:{:02x}:{:02x}",
            mac[0], mac[1], mac[2], mac[3], mac[4], mac[5]
        )?;
        write!(f, "\ncpu: {}", self.cpu)?;
        write!(f, "\nport_id: {}", self.port_id)?;
        write!(f, "\nrx_offload_capa: {:#018x}", self.device.rx_capa)?;
        write!(f, "\ntx_offload_capa: {:#018x}", self.device.tx_capa)?;
        write!(f, "\nmax_rx_pktlen: {}", self.device.max_rx_pktlen)?;
        write!(f, "\nmin_mtu: {}", self.device.min_mtu)?;
        write!(f, "\nmax_mtu: {}", self.device.max_mtu)?;
        write!(
            f,
            "\ndescriptors: rx={} tx={}",
            self.descriptors.0, self.descriptors.1
        )?;
        write!(f, "\nrx_pool_count: {}", self.rx_pool_count)?;
        write!(f, "\ntx_pool_count: {}", self.tx_pool_count)?;
        write!(f, "\n{}", self.offloads)?;
        write!(f, "\nlink_speed_mbps: {}", self.link.speed_mbps)?;
        write!(
            f,
            "\nlink_duplex: {}",
            if self.link.full_duplex {
                "full"
            } else {
                "half"
            }
        )?;
        write!(
            f,
            "\nlink_status: {}",
            if self.link.up { "up" } else { "down" }
        )?;
        match self.gateway {
            Some((ip, mac, waited)) => {
                write!(f, "\ngateway: {ip} at {mac} after {} ms", waited.total_millis())
            }
            None => write!(f, "\ngateway: none"),
        }
    }
}

/// Start DPDK, open the device the config names, and return a running Stack on it.
///
/// Call it once per process, from the thread that will drive the Stack, with that thread
/// already restricted to one CPU (`taskset -c N`, or `--cpuset-cpus=N` for a container).
/// Resolve host names before calling it: name lookup goes through the kernel, which loses
/// the interface of a real card once DPDK takes it.
///
/// `config` is the loaded network config file; `sizing` sizes the socket slots and pools
/// (see [`Sizing`]).
///
/// `helper_cpus` says where DPDK's helper threads (interrupt and control work) may run.
/// DPDK places them on the CPUs the starting thread may use, so `open` widens the thread's
/// affinity while DPDK starts and restores the single CPU afterwards, also when DPDK fails:
///
/// - `None`: if the pinned CPU is listed in `/sys/devices/system/cpu/isolated`, the
///   helpers may use every online CPU that is not isolated (Linux trims that to the
///   container's cpuset). Otherwise, or with no such CPU available, they share the pinned
///   CPU. Right for most programs.
/// - `Some(cpus)`, Linux only: the helpers may use `cpus`. For a pinned CPU that is not
///   isolated at boot, such as in a container given several CPUs. The list must be
///   non-empty, every CPU below `CPU_SETSIZE` and inside the process's cpuset. A list
///   holding only the pinned CPU behaves like `None`.
///
/// `clock` returns the Stack's time. `open` calls it for the Stack's start time and while
/// it waits for the gateway; every later drive call must use the same clock, because the
/// Stack refuses a time earlier than one it has seen.
///
/// The steps, each failing with its [`OpenError`] variant:
///
/// 1. Check the thread may run on exactly one CPU, the pinned CPU.
/// 2. Start DPDK with `--lcores=0@<cpu> --file-prefix <eal.file_prefix> --no-telemetry`,
///    then `-a <pci>[,<devargs>]` for a PCI card or `--no-pci --vdev <spec>` for a vdev,
///    then `eal.extra`.
/// 3. Find the device; for a PCI card, compare its MAC with `mac`. The Stack always uses
///    the device's MAC.
/// 4. Choose offloads (see [`offloads`](crate::offloads)), let the driver adjust the
///    descriptor counts, size the pools and open the port.
/// 5. Wait up to ten seconds of wall-clock time for the link, polling every 100 ms.
/// 6. Build the Stack.
/// 7. With a gateway in the config, send ARP for it and run receive steps until it
///    answers or the clock shows three seconds since the start time. This loop spins, and
///    it ends only when the clock advances: a clock that never moves never times out.
///
/// # Safety
/// The ethdev must be newly created, never configured or started, and under
/// exclusive control. EAL, packet operations and cleanup stay on its owner
/// thread. Preserve the production pools' direct/unique/single-segment
/// contract. The PMD must honor the requested packet profile and discharge
/// its buffers/DMA on close. The PMD must supply only valid DPDK mbufs with
/// valid links/refcounts, each transferring one reference; for net_ring this
/// also applies to whoever writes the attached RX ring. TX acceptance must
/// report at most the offered count and take the front of the offered array.
/// EAL and retained backing survive until all obligations are discharged.
/// Caller-owned attached rings must outlive device close and return queued
/// packets before pool destruction. Vdevs are created by EAL from `config`
/// and live as long as EAL.
pub unsafe fn open(
    config: &Config,
    sizing: &Sizing,
    helper_cpus: Option<&[usize]>,
    clock: &mut impl FnMut() -> Instant,
) -> Result<Opened, OpenError> {
    let cpu = only_allowed_cpu().map_err(OpenError::NotPinned)?;
    let mut argv = vec![
        "switch-open".to_owned(),
        format!("--lcores=0@{cpu}"),
        "--file-prefix".to_owned(),
        config.eal.file_prefix.clone(),
        "--no-telemetry".to_owned(),
    ];
    let device_name = match &config.device {
        Device::Pci(pci) => {
            argv.push("-a".to_owned());
            argv.push(if pci.devargs.is_empty() {
                pci.pci.clone()
            } else {
                format!("{},{}", pci.pci, pci.devargs)
            });
            pci.pci.clone()
        }
        Device::Vdev { vdev: spec } => {
            argv.push("--no-pci".to_owned());
            argv.push("--vdev".to_owned());
            argv.push(spec.clone());
            spec.split_once(',')
                .map_or_else(|| spec.clone(), |(name, _)| name.to_owned())
        }
    };
    argv.extend(config.eal.extra.iter().cloned());
    let args: Vec<_> = argv.iter().map(String::as_str).collect();
    #[cfg(target_os = "linux")]
    let eal = crate::eal_affinity::init(cpu, &args, helper_cpus)?;
    #[cfg(not(target_os = "linux"))]
    if helper_cpus.is_some() {
        return Err(OpenError::HelperAffinity(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "explicit EAL helper CPUs require Linux",
        )));
    }
    #[cfg(not(target_os = "linux"))]
    let eal = Eal::init(&args).map_err(OpenError::Eal)?;
    let port_id = port_by_name(&eal, &device_name)
        .ok_or_else(|| OpenError::DeviceNotFound { name: device_name })?;
    let device = device_info(&eal, port_id).map_err(OpenError::DeviceInfo)?;
    check_mac(config, device.mac)?;
    let offloads = offloads::resolve(&config.offloads, config.rx_timestamps, &device);
    let descriptors =
        adjusted_descriptors(port_id, config.descriptors).map_err(OpenError::Descriptors)?;
    let port_config = port_config(port_id, config.mtu, offloads.offloads, descriptors, sizing)
        .map_err(OpenError::PortConfig)?;
    let rx_pool_count = port_config.rx_pool.count;
    let tx_pool_count = port_config.tx_pool.count;
    // SAFETY: the caller accepts this function's safety contract; EAL just
    // created the configured ethdev and remains alive in `Opened`.
    let port = unsafe { Port::open(&eal, port_config) }.map_err(|error| OpenError::PortOpen(Box::new(error)))?;
    let link = match wait_link(port_id) {
        Ok(link) => link,
        Err(error) => {
            return Err(OpenError::Link {
                port_id,
                port: Box::new(port),
                error,
            })
        }
    };
    if !link.up {
        return Err(OpenError::LinkDown {
            port_id,
            port: Box::new(port),
            link,
        });
    }
    let seed = match os_random_seed() {
        Ok(seed) => seed,
        Err(error) => {
            return Err(OpenError::Seed {
                port: Box::new(port),
                error,
            })
        }
    };
    let paths: Vec<PathMtu> = config
        .paths
        .iter()
        .map(|rule| PathMtu {
            prefix: rule.prefix,
            mtu: rule.mtu,
        })
        .collect();
    let stack_config = StackConfig {
        ethernet: EthernetAddress(device.mac),
        ipv4: config.ipv4,
        gateway: config.gateway,
        paths: &paths,
        owned_ipv4: &config.owned_ipv4,
        random_seed: seed,
        neighbor_capacity: sizing.neighbor_capacity,
        max_sockets: sizing.max_sockets,
        max_udp_sockets: sizing.max_udp_sockets,
        rx_staging: sizing.rx_staging,
        tx_pending: sizing.tx_pending,
    };
    let start = clock();
    let mut stack = Stack::new(port, stack_config, start)
        .map_err(|(port, error)| OpenError::Stack {
            port: Box::new(port),
            error,
        })?;
    let gateway = if let Some(gateway) = config.gateway {
        let mut now = start;
        loop {
            if let Some(mac) = stack
                .resolve_gateway()
                .expect("the Stack was built with this gateway")
            {
                break Some((gateway, mac, now - start));
            }
            now = clock();
            let waited = now - start;
            if waited >= Duration::from_secs(3) {
                return Err(OpenError::GatewayUnresolved {
                    stack: Box::new(stack),
                    gateway,
                    waited,
                });
            }
            if let Err(error) = stack.receive(now, 32) {
                return Err(OpenError::Receive {
                    stack: Box::new(stack),
                    error,
                });
            }
        }
    } else {
        None
    };
    Ok(Opened {
        stack,
        eal,
        report: OpenReport {
            cpu,
            port_id,
            device,
            offloads,
            descriptors,
            rx_pool_count,
            tx_pool_count,
            link,
            gateway,
        },
    })
}

fn check_mac(config: &Config, device: [u8; 6]) -> Result<(), OpenError> {
    if matches!(&config.device, Device::Pci(_)) {
        if let Some(expected) = config.mac {
            let actual = EthernetAddress(device);
            if expected != actual {
                return Err(OpenError::MacMismatch {
                    config: expected,
                    device: actual,
                });
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(device: &str, mac: &str) -> Config {
        Config::from_json(&format!(
            r#"{{
              "version": 1,
              "device": {device},
              "mac": {mac},
              "ipv4": "192.0.2.2/24",
              "gateway": null,
              "mtu": 1500,
              "paths": [],
              "offloads": "auto",
              "rx_timestamps": false,
              "descriptors": 16,
              "eal": {{"file_prefix": "mac-test", "extra": []}}
            }}"#
        ))
        .unwrap()
    }

    #[test]
    fn open_pci_mac_mismatch_is_error() {
        let expected = EthernetAddress([0x02, 0, 0, 0, 0, 1]);
        let actual = EthernetAddress([0x02, 0, 0, 0, 0, 2]);
        let pci = config(
            r#"{"pci":"0000:00:06.0","pci_id":"1d0f:ec20","devargs":"","kernel_driver":"ena","kernel_interface":"eth1","numa_node":0}"#,
            r#""02:00:00:00:00:01""#,
        );
        match check_mac(&pci, actual.0) {
            Err(OpenError::MacMismatch { config, device }) => {
                assert_eq!(config, expected);
                assert_eq!(device, actual);
            }
            other => panic!("unexpected result: {other:?}"),
        }
        assert!(check_mac(&pci, expected.0).is_ok());

        let vdev = config(r#"{"vdev":"net_tap0,iface=cdmac0"}"#, "null");
        assert!(check_mac(&vdev, actual.0).is_ok());
    }
}
