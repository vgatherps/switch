# Running on EC2

Amazon EC2 instances reach the network through ENA, Amazon's virtual NIC, and DPDK drives it with the `net_ena` [NIC driver](index.md). The steps are the ones on [Preparing a Linux machine](host-setup.md), with a few EC2-specific differences. You can follow them by hand, or let `deploy/hft-host.sh` do all of them on an Amazon Linux 2023 instance. This page covers both, then shows how to check the result and run the market-data feed on it.

## Instance and network setup

These are done in the AWS console or CLI before you log in.

The instance needs two network interfaces, which AWS calls ENIs. The primary ENI (device number 0) stays with Linux as the [management NIC](index.md): it carries SSH, DNS and the instance metadata service, and keeps the default route. Attach a second ENI for DPDK; it becomes the [data NIC](index.md). Leave ENA Express off on it.

AWS gives no public IPv4 address to an instance with more than one ENI. If your program talks to the internet (the market-data feed does), attach an Elastic IP to the data ENI, or route its subnet through a NAT gateway. Security groups apply to the data ENI as to any other: allow the ports your program uses, for example outbound TCP 9443 for the feed, or UDP and TCP 11111-11130 between two instances for `switch-nic sockperf-server`.

The setup script picks two CPUs on the data ENI's NUMA node and isolates both physical cores, so the instance needs two physical cores there plus at least one more CPU for housekeeping. For hardware receive timestamps, which the feed asks for, the instance type and placement also matter; `deploy/README.md` lists the instance families and the `precision-time` placement group AWS requires.

## By hand

On EC2 the generic steps change in these places:

- Discover with `--aws`: `switch-netconf discover --interface <data-if> --aws -o dut.json`. It reads the ENI's address, subnet, gateway (the VPC router) and VPC address ranges from the instance metadata service, refuses the primary ENI, and warns when the ENI has no public IPv4 address. Inside a Docker container it needs `--network host` to reach the metadata service. [switch-netconf](netconf-tool.md) describes the rest.
- Bind with `--noiommu`. Instances other than bare metal have no IOMMU.
- Use a `vfio-pci` module with write combining. ENA's fast transmit mode (LLQ) needs write combining on the device's memory, which mainline `vfio-pci` does not provide on older kernels. Amazon Linux 2023's `kernel6.18` package includes it; for other kernels Amazon publishes a patched module in its `amzn-drivers` repository (`userspace/dpdk/enav2-vfio-patch`). Without it the ENA driver prints a warning on stderr that LLQ is disabled.
- Pick the CPU on the data ENI's NUMA node, as everywhere.

## With hft-host.sh

`deploy/hft-host.sh` turns a fresh Amazon Linux 2023 instance (x86-64 or arm64) with two ENIs into a ready host. Run it as root from a checkout of the repository:

```sh
git clone <repo> && cd <repo>
sudo deploy/hft-host.sh apply        # builds, configures, reboots once; SSH drops here
# reconnect
sudo deploy/hft-host.sh verify       # every line ok, exit 0
```

### What apply does

`apply` runs these steps in order. Each prints one line, `<step>: ok <detail>`, or stops at the first failure.

1. Build. Installs the compiler toolchain with `dnf` and Rust with `rustup`, builds DPDK 25.11.3 under `/opt/dpdk` with the same options and patch as the Docker image, and installs release builds of `switch-nic`, `switch-netconf` and `switch-junctiond` into `/usr/local/bin`.
2. Interfaces. The management interface is the one with the lowest-metric IPv4 default route. The data interface is the one other interface on the `ena` driver; pass `--interface IF` if there are several. The script refuses if the two would be the same, and every later step that touches the data ENI checks this again first.
3. CPUs. `APP_CPU` is the highest-numbered CPU on the data ENI's NUMA node. `JUNCTION_CPU` is the highest CPU on a different physical core of that node. Both cores, with their SMT siblings, become isolated; every other CPU is housekeeping, and the script refuses if none is left.
4. Save the choices to `/etc/switch/host.env`.
5. Config. Unless the data ENI is already on `vfio-pci`, runs `switch-netconf discover --interface <data> --aws -o /etc/switch/dut.json`, then `switch-netconf check`.
6. Kernel command line. Adds, with `grubby`, the isolation options from [Preparing a Linux machine](host-setup.md#3-choosing-and-isolating-a-cpu) plus `transparent_hugepage=madvise hugepagesz=2M hugepages=<node>:512 ena.phc_enable=1`, and on x86-64 `clocksource=tsc`. That is 1 GiB of hugepages on the data ENI's node; `ena.phc_enable=1` exposes the ENA's hardware clock as `/dev/ptp_ena`.
7. Write combining. Checks that the installed `vfio-pci` supports it, or builds Amazon's module on older kernels.
8. Boot service. Installs `switch-host.service`, which runs `hft-host.sh boot` at every boot, and enables `chronyd`.
9. Reboot, if the command line or a kernel module changed. Otherwise it runs `boot` directly.

`boot` applies the settings a reboot resets: it points chrony at the ENA hardware clock once `/dev/ptp_ena` exists, turns `irqbalance` off, moves every movable interrupt to the housekeeping CPUs, takes the data ENI away from `systemd-networkd` and flushes its addresses and routes, and finally runs `switch-netconf bind` (with `--noiommu` when the machine has no IOMMU groups).

```text
 apply (first run)                              reboot          boot (every boot, from switch-host.service)
 ---------------------------------------------  ------  -----------------------------------------------
 build  interfaces  cpus  host.env  config        |      chrony  irqbalance off  IRQs to housekeeping
 cmdline  write-combining  unit  unmanaged-file   |      data ENI unmanaged + flushed
 -> "reboot: rebooting now for: ..."  ----------> |      switch-netconf bind
                                                         -> then: sudo deploy/hft-host.sh verify
```

`apply` can be run again at any time. Rerun it after `git pull` (it rebuilds), after a kernel update, or after changing the instance type (it recomputes the CPUs and the command line).

`apply --kernel-receiver` leaves the data ENI on the Linux `ena` driver instead of binding it. The AWS benchmark uses it to measure the Linux kernel as a receiver; everything else (build, isolation, interrupts) is the same. The mode is saved in `host.env`, so `boot` and `verify` follow it.

### verify

`sudo deploy/hft-host.sh verify` prints one line per setting, `<step>: ok <detail>` or `<step>: FAIL <detail>`, and exits 1 if any line failed. It checks the kernel command line, that the management interface still has the default route, that the data ENI is on `vfio-pci`, the isolated CPUs, hugepages, the hugepage use of the last feed run, `irqbalance`, interrupt placement, the write-combining module, the clock source (x86-64), the boot service, the config file and chrony.

### Files it writes

| Path | What |
|---|---|
| `/etc/switch/host.env` | The interfaces, NUMA node, `APP_CPU`, `JUNCTION_CPU`, isolated and housekeeping CPU lists, and the data driver mode |
| `/etc/switch/dut.json` | The [network config file](config.md) for the data ENI |
| `/etc/switch/binance.json` | `dut.json` with `rx_timestamps: true`, written by `run-binance.sh` |
| `/var/log/switch/binance.log` | The feed's output, appended by `run-binance.sh` |
| `/etc/systemd/system/switch-host.service` | Runs `boot` at every boot |
| `/etc/systemd/network/00-switch-data.network` | Tells `systemd-networkd` to leave the data ENI alone |
| `/etc/systemd/networkd.conf.d/90-switch-kernel-receiver.conf` | Only with `--kernel-receiver` |
| `/usr/local/bin/switch-nic`, `switch-netconf`, `switch-junctiond` | The release binaries |

### Which CPU to use

Programs that own the NIC run on `APP_CPU`. When the [junction daemon](../junction/index.md) owns it instead, the daemon's network thread runs on `JUNCTION_CPU`, its control thread on the housekeeping CPUs, and each application on `APP_CPU`. Read the values from `host.env`:

```sh
. /etc/switch/host.env
sudo taskset -c "$APP_CPU" switch-nic --config /etc/switch/dut.json info
```

## Running the feed

`deploy/run-binance.sh` runs the market-data feed of [Walkthrough: a market-data feed](../protocols/binance-feed.md) through `switch-nic feed` on a host `hft-host.sh` prepared:

```sh
sudo deploy/run-binance.sh --seconds 3600
```

It runs `hft-host.sh verify` first and refuses to start if any line fails, or if chrony does not have the ENA hardware clock selected (the instance must be in a `precision-time` placement group). It then writes `/etc/switch/binance.json`, a copy of `dut.json` with hardware receive timestamps on, refuses to start unless `switch-netconf check` reads that back, and runs `switch-nic --config /etc/switch/binance.json feed` with your options, pinned to `APP_CPU`, appending its output to `/var/log/switch/binance.log`. On an instance type whose ENA offers no receive timestamps, opening the NIC fails with `PMD lacks the requested RX timestamp offload`.

## Where to go next

[switch-nic](switch-nic.md) documents the `info`, `sockperf-server` and `feed` modes. [Benchmarking on AWS](../measuring/aws.md) runs the latency benchmark on hosts this script prepares, without logging in by hand.
