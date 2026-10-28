# zhida-netconf

The zhida network config file (`dut.json`): a typed `Config`, discovery of it from a Linux interface, and binding the NIC to vfio-pci. It does not need DPDK.

Used by `zhida-open` (which turns a `Config` into a running `Stack`) and `nic-runner`. Depends on `serde`, `serde_json`, `libc` and the vendored `smoltcp` (for address and MTU types).

## API

- `Config` and its parts `Device` (`Pci(PciDevice)` or `Vdev`), `PathRule`, `Offloads` (`Auto` or explicit checksum flags), `EalConfig`. `Config::load(path)` rejects unknown fields; `ConfigError` carries the reason. Fields: device, mac, ipv4, gateway, mtu, paths, offloads, rx_timestamps, descriptors, eal.
- `discover::{discover, discover_aws, ipv4_addresses}`: read the interface's IPv4, MAC, MTU, PCI address and id, kernel driver and NUMA node from the kernel. `discover_aws` also reads ENI facts from IMDSv2 (`imds`).
- `rediscover::write`: writes a discovered config over an existing file, keeping hand-added path rules.
- `devbind::{bind, unbind}`: move the device between its kernel driver and vfio-pci, with refusals before any write.

## Binary

```sh
zhida-netconf check FILE
zhida-netconf discover --interface IF [--ipv4 ADDR] [--aws] [-o FILE]
zhida-netconf bind FILE [--noiommu] [--dry-run]     # root
zhida-netconf unbind FILE                            # root
```

Exit status is 2 for a usage error and 1 for a failure. The full procedure (hugepages, IOMMU, EC2 specifics) is in [../nic-runner/README.md](../nic-runner/README.md), sections 2 and 3.

## Test

```sh
make test-netconf    # cargo test -p zhida-netconf -p zhida-open -- --test-threads=1
```

Tests are in `tests/check.rs`, `discover.rs` and `devbind.rs`. Building the crate alone needs only a Rust toolchain; `make test-netconf` also runs the `zhida-open` tests, which need DPDK.

`bind` and `unbind` write to sysfs and are meant for Linux hosts. `bind` taints the kernel with `--noiommu`.
