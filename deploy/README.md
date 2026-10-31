# deploy: EC2 host for the Binance listener

`hft-host.sh` turns a fresh Amazon Linux 2023 instance (x86_64 or arm64) into a host that runs
`zhida-nic feed` on a DPDK-bound ENA. `run-binance.sh` starts the listener and refuses to start unless
`hft-host.sh verify` passes.

## Before you start (AWS console or CLI)

- Instance: AL2023 (`al2023-ami-kernel-default`), at least 2 CPUs. ENA hardware RX timestamps (the NIC
  clock) exist only where AWS offers the PTP hardware clock: a precision-time placement group, or a type
  with legacy access.
- Two ENIs: the primary carries SSH and keeps the default route. Attach a second ENI for DPDK, with
  ENA Express off and an Elastic IP: AWS gives no public IPv4 to an instance with several ENIs, and the
  feed needs a route to the internet.
- The security group allows outbound TCP 9443 (the default group allows all outbound).

## Run

```sh
git clone <repo> && cd <repo>
sudo deploy/hft-host.sh apply        # builds, configures, reboots once; SSH drops here
# reconnect
sudo deploy/hft-host.sh verify       # every line ok, exit 0
sudo deploy/run-binance.sh --seconds 3600
```

`apply` is idempotent: run it again after `git pull` (it rebuilds), after a kernel update (it rebuilds
the two kernel modules) or after changing the instance type (it recomputes CPUs and the command line).
Use `--interface IF` only when the host has more than one non-management ENA interface.

## What apply does

| step | what |
|---|---|
| packages, rust, dpdk | dnf toolchain with clang/lld 18; rustup with `rust-toolchain.toml`'s channel; DPDK 25.11.3 under `/opt/dpdk` with `conduit/docker/dpdk-ena-read-clock.patch` (ENA `rte_eth_read_clock` from the device PHC) and `conduit/docker/Dockerfile`'s meson options. `/opt/dpdk/share/zhida-dpdk.stamp` records the tarball and patch sha256; a missing or different stamp (a stock 25.11.3, an older patch) means a clean rebuild |
| binaries | `cargo build --release --locked -p nic-runner -p zhida-netconf`; `zhida-nic` (mimalloc global allocator) and `zhida-netconf` go into `/usr/local/bin` |
| interfaces | management = the interface of the lowest-metric IPv4 default route; data = the one other `ena` interface. Never the same interface; every step that touches the data ENI checks again first |
| cpus | app CPU = highest-numbered CPU on the data ENI's NUMA node; it and its SMT siblings are isolated, every other CPU is housekeeping. Written to `/etc/zhida/host.env` |
| config | `zhida-netconf discover --interface <data> --aws -o /etc/zhida/dut.json`, then `check` |
| cmdline | via grubby: `isolcpus=managed_irq,domain,<iso> nohz_full=<iso> rcu_nocbs=<iso> irqaffinity=<housekeeping> transparent_hugepage=madvise hugepagesz=2M hugepages=<node>:512` (1 GiB of 2 MiB pages on the data ENI's node) |
| vfio_wc | vfio-pci with write-combining (needed for ENA LLQ) from amzn-drivers' `get-vfio-with-wc.sh`, pinned commit and sha256 |
| cmdline (x86_64) | also `clocksource=tsc` (not `tsc=reliable`, which turns off the kernel's TSC stability checks). Graviton's `arch_sys_counter` needs nothing |
| unit | `zhida-host.service` runs `hft-host.sh boot` at every boot |
| reboot | when the command line or a module changed |
| boot | irqbalance off; every movable IRQ on the housekeeping CPUs; the data ENI unmanaged by systemd-networkd (`/etc/systemd/network/00-zhida-data.network`, matched by MAC) with its addresses and routes flushed; `zhida-netconf bind` (`--noiommu` without IOMMU groups) |

`verify` prints one `<step>: ok|FAIL <detail>` line for each of: cmdline, management (on its kernel
driver, with the default route), data (on vfio-pci), cpus (`isolated` and `nohz_full` in sysfs), hugepages,
pools, irqbalance, irqs, vfio_wc (the loaded vfio-pci-core is the patched one), clocksource (the current
one; anything but `tsc` fails on x86_64), unit, config.
`pools` reads zhida-nic's own `hugepages: hugepages-2048kB used=N free=M` line from the last run in
`/var/log/zhida/binance.log` and fails if the run took more than the 512 reserved pages.

## Test (local, no root)

```sh
sh deploy/test-hft-host.sh
```

It runs apply, boot, verify and run-binance.sh against a fake `/proc`, `/sys`, `/etc` tree with stub
commands. The build step is checked separately by running `hft-host.sh build` in an `amazonlinux:2023`
container. Kernel modules, grubby, the reboot, networkd, IRQs and vfio binding run only on a real instance.
