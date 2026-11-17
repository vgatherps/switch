# Running on EC2

On EC2 the data card is an ENA, which DPDK drives with its `net_ena` driver. You can prepare the instance by hand, following [Preparing a Linux machine](host-setup.md) with the changes below, or run `deploy/hft-host.sh`, which does every step on Amazon Linux 2023.

## Before you log in

Do these in the AWS console or CLI.

1. Give the instance two network interfaces (ENIs). The primary one (device number 0) stays with Linux, carrying SSH and the instance metadata service and keeping the default route. The second ENI is for your program.
2. AWS does not auto-assign a public IPv4 address when you launch with more than one network interface ([AWS: Manage the IPv4 addresses](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/working-with-ip-addresses.html)), so either attach the second ENI after launch or give the primary one an Elastic IP for SSH. If your program talks to the internet, attach an Elastic IP to the data ENI, or route its subnet through a NAT gateway; `switch-netconf discover --aws` warns when the data ENI has no public address.
3. Allow your program's ports on the data ENI's security group, for example outbound TCP 9443 for the market-data feed of `switch-nic feed`.
4. Choose an instance type with at least two physical cores on the data ENI's NUMA node plus one more CPU. The script isolates two cores and needs a CPU left for Linux.
5. For hardware receive timestamps and the card's clock (`/dev/ptp_ena`), choose a family that supports AWS's precision time placement groups, such as M7g or C7i ([AWS lists them](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/configure-ec2-ntp.html)), and launch it in a placement group with the `precision-time` strategy:

   ```sh
   aws ec2 create-placement-group --group-name G --strategy precision-time
   ```

   and launch with `--placement GroupName=G`. An instance can be in one placement group at a time, so it cannot also be in a cluster placement group.

## By hand

Follow [Preparing a Linux machine](host-setup.md) with these changes.

- Discover with `--aws`. It reads the ENI's address, subnet, gateway and VPC address ranges from the instance metadata service, and refuses the primary ENI:

  ```sh
  sudo mkdir -p /etc/switch
  sudo switch-netconf discover --interface <data-if> --aws -o /etc/switch/dut.json
  ```

  From inside a Docker container this needs `--network host`.
- If `bind` refuses with `no-IOMMU mode taints the kernel and needs --noiommu`, the ENI has no IOMMU group; bind with `--noiommu`.
- Amazon Linux 2023 manages interfaces with systemd-networkd ([AWS: Networking service](https://docs.aws.amazon.com/linux/al2023/ug/networking-service.html)). Before binding, write `/etc/systemd/network/00-switch-data.network`:

  ```ini
  [Match]
  MACAddress=<data ENI MAC>

  [Link]
  Unmanaged=yes
  ```

  then run `sudo networkctl reload` and flush the interface's addresses and routes as in [step 6](host-setup.md#6-discover-check-and-bind).
- ENA's low-latency queue mode needs a `vfio-pci` with write combining, which mainline `vfio-pci` does not have (DPDK's ENA guide). Amazon's ENA DPDK documentation in its `amzn-drivers` repository, under `userspace/dpdk`, describes the patched module. `hft-host.sh` accepts the module of Amazon Linux 2023's official `kernel6.18` package and installs Amazon's patched one on older kernels.
- To compare hardware timestamps with the system clock, add `ena.phc_enable=1` to the boot options. On a recent Amazon Linux 2023 the card's clock then appears as `/dev/ptp_ena`; after the reboot add `refclock PHC /dev/ptp_ena poll 0 delay 0.000010 prefer` to `/etc/chrony.conf` and restart `chronyd`, so the system clock follows the card's.

## With hft-host.sh

`deploy/hft-host.sh` turns a fresh Amazon Linux 2023 instance (x86-64 or arm64) with two ENIs into a ready host.

1. Clone the repository on the instance and, from its root, run:

   ```sh
   sudo deploy/hft-host.sh apply
   ```

   Each step prints `<step>: ok <detail>`, or `<step>: changed <detail>` when it changed something. The first run changes the kernel command line, prints `reboot: rebooting now for: cmdline ...` and reboots the instance; your SSH session drops. If a step fails it prints `<step>: FAIL <detail>` and the script exits with status 1.
2. Reconnect, and check:

   ```sh
   sudo deploy/hft-host.sh verify
   ```

   Every line should read `ok`, and the exit status is 0. A `FAIL` line names what is wrong, and the exit status is 1.

Run `apply` again after `git pull` (it rebuilds the tools), after a kernel update, or after changing the instance type (it recomputes the CPUs and the command line). `hft-host.sh build` runs only the build part.

### What apply sets up

| Step | What it does |
|---|---|
| `packages`, `rust`, `dpdk`, `binaries` | Installs the toolchain, builds DPDK 25.11.3 under `/opt/dpdk` the same way as the Docker image, and installs release builds of `switch-nic`, `switch-netconf` and `switch-junctiond` into `/usr/local/bin`. |
| `interfaces` | The management interface is the one with the lowest-metric IPv4 default route. The data interface is the one other interface on the `ena` driver; pass `--interface IF` if there are several. |
| `cpus` | `APP_CPU` is the highest-numbered CPU on the data ENI's NUMA node, `JUNCTION_CPU` the highest on a different physical core there. Both cores, with their hyperthread siblings, are isolated; every other CPU is a housekeeping CPU. |
| `config` | Runs `switch-netconf discover --aws` into `/etc/switch/dut.json`, then `check`. |
| `cmdline` | Adds the isolation options from [Preparing a Linux machine](host-setup.md#3-isolate-a-cpu), `transparent_hugepage=madvise hugepagesz=2M hugepages=<node>:512 ena.phc_enable=1`, and on x86-64 `clocksource=tsc`. |
| `vfio_wc` | Checks for a write-combining `vfio-pci`, or builds Amazon's on older kernels. |
| `unit` | Installs `switch-host.service`, which runs `hft-host.sh boot` at every boot. |

At every boot, `boot` points chrony at the ENA's hardware clock once `/dev/ptp_ena` exists, turns `irqbalance` off, moves every movable interrupt to the housekeeping CPUs, takes the data ENI away from systemd-networkd and flushes it, and binds it with `switch-netconf bind` (adding `--noiommu` when the machine has no IOMMU groups).

`apply --kernel-receiver` does the same but leaves the data ENI on the Linux `ena` driver. The AWS benchmark uses it to measure the kernel as a receiver. The choice is saved, so `boot` and `verify` follow it.

`apply` exits with status 100 when it reboots the instance, 0 when it finishes without one, 1 when a step fails and 2 on a usage error.

### Files

| Path | Contents |
|---|---|
| `/etc/switch/host.env` | Interfaces, NUMA node, `APP_CPU`, `JUNCTION_CPU`, the isolated and housekeeping CPU lists |
| `/etc/switch/dut.json` | The [network config file](config.md) for the data ENI |
| `/etc/systemd/network/00-switch-data.network` | Tells systemd-networkd to leave the data ENI alone |
| `/etc/systemd/system/switch-host.service`, `/usr/local/sbin/switch-hft-host` | The boot service and the copy of the script it runs |
| `/usr/local/bin/switch-nic`, `switch-netconf`, `switch-junctiond` | The tools |

### Which CPU to use

A program that owns the card runs on `APP_CPU`. With [switch-junctiond](junctiond.md), the daemon runs on `JUNCTION_CPU` with `--control-cpus` set to the housekeeping list, and each application on `APP_CPU`. Read the values from `host.env`:

```sh
. /etc/switch/host.env
sudo taskset -c "$APP_CPU" switch-nic --config /etc/switch/dut.json info
```

## Running the market-data feed

`deploy/run-binance.sh` runs `switch-nic feed` with hardware receive timestamps on a host that `hft-host.sh` prepared:

```sh
sudo deploy/run-binance.sh --seconds 3600
```

Its arguments go to `switch-nic feed` ([switch-nic](switch-nic.md#feed)). It refuses to start unless `hft-host.sh verify` passes and chrony has selected the ENA hardware clock, which requires `/dev/ptp_ena` (step 5 of [Before you log in](#before-you-log-in)). It writes `/etc/switch/binance.json`, a copy of `dut.json` with `rx_timestamps` on, runs the feed on `APP_CPU`, and appends the output to `/var/log/switch/binance.log`. On an instance type without hardware timestamps, opening the card fails with `PMD lacks the requested RX timestamp offload`.

## Running your own program

The script sets DPDK's build paths only in its own environment. To build on the host, set them in a root shell, from the repository root:

```sh
sudo -i
cd <repository root>
export PKG_CONFIG_PATH=/opt/dpdk/lib/pkgconfig \
    RUSTFLAGS='-C linker=clang -C link-arg=-fuse-ld=lld' \
    LIBCLANG_PATH=/usr/lib64/llvm18/lib PATH=/usr/lib64/llvm18/bin:/root/.cargo/bin:$PATH
cargo build --locked --release -p switch-examples --example tcp_client_local --features net
. /etc/switch/host.env
taskset -c "$APP_CPU" target/release/examples/tcp_client_local /etc/switch/dut.json <peer>:7000
```

The peer is a second instance in the same subnet running the one-shot server from [step 8](host-setup.md#8-run-your-program), with the security group allowing TCP 7000 between them.
