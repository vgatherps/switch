# deploy: EC2 host for the Binance listener

`hft-host.sh` turns a fresh Amazon Linux 2023 instance (x86_64 or arm64) into a host that runs
`switch-nic feed` on a DPDK-bound ENA. `run-binance.sh` starts the listener and refuses to start unless
`hft-host.sh verify` passes.

## Before you start (AWS console or CLI)

- Instance: AL2023 (`al2023-ami-kernel-default`), two physical cores on the data ENI's NUMA node
  plus at least one housekeeping CPU. hft-host.sh itself works on any
  ENA instance (the bench uses it on c7gn in a cluster placement group).
- For the Binance listener, the instance must give ENA hardware RX timestamps and the PTP hardware
  clock (PHC): `run-binance.sh` runs with `rx_timestamps: true`, so elsewhere Port open fails
  (`PMD lacks the requested RX timestamp offload`). Per the EC2 user guide (configure-ec2-ntp):
  - a supported Gen7+ family: M7a, M7g, M7gd, M7i, M7i-flex, M8a, M8g, M8i, M8i-flex, M9g, M9gd; C7a,
    C7gd, C7i, C7i-flex, C8a, C8g, C8gd, C8i, C8i-flex, C9g, C9gd; R7a, R7g, R7i, R8a, R8g, R9g, R9gd,
    X8g, X8i. Not C7gn.
  - launched in a placement group with the `precision-time` strategy
    (`aws ec2 create-placement-group --group-name G --strategy precision-time`, then launch with
    `--placement GroupName=G`). An instance belongs to one placement group, so not also a cluster one.
  - Without that placement group, legacy access covers only M7a/M7g/M7i, R7a/R7g/R7i and I8g/I8ge in
    US East (N. Virginia, Ohio), Asia Pacific (Tokyo, Malaysia, Thailand), Europe (Stockholm) and the
    New York City Local Zone.
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

`apply` is idempotent: run it again after `git pull` (it rebuilds), after a kernel update (it checks
vendor-native ENA write-combining on AL2023 6.18, or rebuilds vfio-pci on legacy kernels), or after
changing the instance type (it recomputes CPUs and the command line).
Use `--interface IF` only when the host has more than one non-management ENA interface.

The AWS kernel comparison runs `apply --kernel-receiver` on a fresh receiver. It keeps the data
ENI on Linux `ena` at apply and every boot, skips the VFIO module setup, and verifies `ena`
instead of `vfio-pci`. It otherwise uses the same build, CPU isolation, IRQ housekeeping and
unmanaged-interface setup; the benchmark configures the data IP and source routes afterward.
The mode is saved in `host.env` for boot and verify. Repeat the flag when reapplying on that host. On a host
whose data ENI is already on vfio-pci, `apply --kernel-receiver` refuses (`interfaces: FAIL`); run
`switch-netconf unbind /etc/switch/dut.json` first.
On the kernel receiver, `apply` also writes
`/etc/systemd/networkd.conf.d/90-switch-kernel-receiver.conf` with
`ManageForeignRoutingPolicyRules=no`, so networkd does not remove the benchmark's source-routing
rule when it reconfigures the management interface. The drop-in is written before reboot; when
no reboot is needed, apply stages the unmanaged data interface and restarts systemd-networkd to
activate the drop-in before proceeding.

## What apply does

| step | what |
|---|---|
| packages, rust, dpdk | dnf toolchain with clang/lld 18; rustup with `rust-toolchain.toml`'s channel; DPDK 25.11.3 under `/opt/dpdk` with `conduit/docker/dpdk-ena-read-clock.patch` (ENA `rte_eth_read_clock` from the device PHC) and `conduit/docker/Dockerfile`'s meson options. `/opt/dpdk/share/switch-dpdk.stamp` records the tarball and patch sha256; a missing or different stamp (a stock 25.11.3, an older patch) means a clean rebuild |
| binaries | `cargo build --release --locked -p switch-nic -p switch-netconf -p switch-junctiond`; `switch-nic` (mimalloc global allocator), `switch-netconf`, and `switch-junctiond` go into `/usr/local/bin` |
| interfaces | management = the interface of the lowest-metric IPv4 default route; data = the one other `ena` interface. Never the same interface; every step that touches the data ENI checks again first |
| cpus | `APP_CPU` = highest-numbered CPU on the data ENI's NUMA node; `JUNCTION_CPU` = highest CPU there outside the app's SMT siblings. Both physical cores, including SMT siblings, are isolated. Every other CPU is housekeeping; setup refuses if none remain. Both CPU assignments and lists are saved in `/etc/switch/host.env`. Conduit apps keep using `APP_CPU` only |
| config | `switch-netconf discover --interface <data> --aws -o /etc/switch/dut.json`, then `check`. `rx_timestamps` stays as discovered (false); run-binance.sh turns it on in its own copy |
| cmdline | via grubby: `isolcpus=managed_irq,domain,<iso> nohz_full=<iso> rcu_nocbs=<iso> irqaffinity=<housekeeping> transparent_hugepage=madvise hugepagesz=2M hugepages=<node>:512 ena.phc_enable=1` (1 GiB of 2 MiB pages on the data ENI's node; `ena.phc_enable=1` makes the ENA PTP hardware clock appear as `/dev/ptp_ena` after reboot, chrony's reference) |
| vfio_wc | official AL2023 `kernel6.18` already supplies ENA write-combining (needed for LLQ); check package ownership/integrity and loaded module identity, without installing headers or rebuilding it. Legacy kernels retain amzn-drivers' `get-vfio-with-wc.sh`, pinned commit and sha256 |
| cmdline (x86_64) | also `clocksource=tsc` (not `tsc=reliable`, which turns off the kernel's TSC stability checks). Graviton's `arch_sys_counter` needs nothing |
| unit | `switch-host.service` runs `hft-host.sh boot` at every boot |
| chrony | enable chronyd. `boot`, once `/dev/ptp_ena` exists (after the reboot that applies `ena.phc_enable=1`; never outside a precision-time placement group), appends AWS's documented PHC refclock line, `refclock PHC /dev/ptp_ena poll 0 delay 0.000010 prefer` ([Amazon Time Sync: configure chronyd to use the PHC](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/configure-ec2-ntp.html#configure-chronyd-phc)), to AL2023's `/etc/chrony.conf`, keeping its Time Sync NTP server line, and restarts chronyd. Without the device `boot` removes exactly that line if present (restarting chronyd) and leaves the rest: chronyd exits at start on a PHC refclock it cannot open |
| unmanaged config | before requesting a reboot, write `/etc/systemd/network/00-switch-data.network` with the data ENI's MAC and `Unmanaged=yes`; reload networkd only when the file changes. No addresses or routes are flushed until `boot` |
| reboot | when the command line or a module changed |
| boot | the PHC refclock once `/dev/ptp_ena` exists (chrony row); irqbalance off; every movable IRQ on the housekeeping CPUs; the data ENI unmanaged by systemd-networkd (`/etc/systemd/network/00-switch-data.network`, matched by MAC) with its addresses and routes flushed; `switch-netconf bind` (`--noiommu` without IOMMU groups) |

The unmanaged file must exist before the first reboot: otherwise AL2023 can give the data ENI a DHCP
default route before `switch-host.service` starts, making it the lowest-metric default route instead
of management. `boot` still checks the saved management interface against the live default route
before configuring or flushing data, and refuses if it changed; it never flushes the new default-route
interface to recover. A no-reboot `apply`, reapply, or standalone `boot` also writes a missing or changed
unmanaged file and reloads networkd. Write and reload failures stop setup before flushing or binding.

`verify` prints one `<step>: ok|FAIL <detail>` line for each of: cmdline, management (on its kernel
driver, with the default route), data (on vfio-pci), cpus (`isolated` and `nohz_full` in sysfs), hugepages,
pools, irqbalance, irqs, vfio_wc (the loaded vfio-pci-core matches the installed WC module), clocksource (on x86_64 the
current one must be `tsc`, and `dmesg`, read as root, must show no `clocksource: timekeeping watchdog`,
`TSC unstable` or `Marking clocksource 'tsc' as unstable` line, and its last `Switched to clocksource`
must name `tsc`; a failure prints the offending line; other architectures skip it), unit, config, chrony
(FAIL when chronyd is not running; ok with the PHC as the selected source, a `#*` PHC row in `chronyc sources`; also ok with no
`/dev/ptp_ena` at all, a host outside a precision-time placement group on NTP only; FAIL when the
device exists but is not selected, printing the sources output). With chrony on the PHC, CLOCK_REALTIME
is UTC from the Time Sync PHC, which does not smear leap seconds (the link-local NTP source does).
`pools` reads switch-nic's own `hugepages: hugepages-2048kB used=N free=M` line from the last run in
`/var/log/switch/binance.log` and fails if the run took more than the 512 reserved pages.

`run-binance.sh` runs `hft-host.sh verify` first and refuses to start if it fails, or if chrony
does not have the PHC selected (launch the instance in a precision-time placement group). It then writes
`/etc/switch/binance.json`: `dut.json` with `rx_timestamps: true`. It refuses to start unless
`switch-netconf check` reads that back, then runs `switch-nic --config /etc/switch/binance.json feed`
pinned to the app CPU, appending to `/var/log/switch/binance.log`. Each message's latency is
CLOCK_REALTIME minus its ENA hardware RX stamp (the data ENI's PHC); chrony keeps CLOCK_REALTIME on
the Time Sync PHC (the `chrony` verify line). At startup the feed logs
`phc_check: phc_minus_realtime_ns=N` from one PHC read between two CLOCK_REALTIME reads.

AWS documents the packet timestamp reference as the
[Amazon Time Sync PTP hardware clock](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/set-time.html#hardware-packet-timestamping).

The 512 x 2 MiB HugeTLB pages (1 GiB) are reserved for DPDK. `run-binance.sh` and the AWS benchmark's
receiver and senders explicitly set `MIMALLOC_ALLOW_LARGE_OS_PAGES=0`, overriding any inherited value.
The binaries still use mimalloc; its default transparent hugepage support stays enabled, and the host
keeps `transparent_hugepage=madvise`. THP uses ordinary memory rather than the reserved HugeTLB pool;
THP backing is possible, not guaranteed.

The AWS runner accepts `--backend conduit junction` for side-by-side sweeps (default `conduit`).
`switch-bench/aws/bootstrap.sh` installs both `switch-bench` and `switch-junctiond`. Junction sweeps start
`switch-junctiond` on `JUNCTION_CPU` with `--control-cpus HOUSEKEEPING`, wait for its control socket,
then run each benchmark app on `APP_CPU`. Each receiver and sender gets its own daemon, stopped
after the sweep even on failure. With the `kernel` command, the receiver stays on kernel sockets;
only senders use the selected backend. Artifact filenames include `conduit-` or `junction-`.
`python3 switch-bench/aws/switch_aws_bench.py plan --backend conduit junction` prints the offline plan.


An AWS diagnostic run with `MIMALLOC_ALLOW_LARGE_OS_PAGES=1` showed mimalloc reserving its default
1 GiB arena before EAL: 2 HugeTLB pages used and 510 still reserved left none available for DPDK.
EAL then failed allocating its services array with errno 12. Keeping the two memory sources separate
avoids that competition without enlarging the HugeTLB pool or replacing mimalloc.

## AL2023 6.18 and the first host setup failure

On the first m7g.xlarge run, `kernel-devel-6.18.56-125.164.amzn2023.aarch64` could not be found.
The package name was wrong: AL2023 uses `kernel6.18-devel` for 6.18; generic `kernel-devel` is the
6.1 package. [AWS documents the kernel-specific names](https://docs.aws.amazon.com/linux/al2023/ug/kernel-update.html).
[AL2023 repositories are version-locked](https://docs.aws.amazon.com/linux/al2023/ug/deterministic-upgrades-usage.html);
changing to `latest` or installing unrelated headers is not a fix.

More importantly, that host does not need an external VFIO rebuild. The official
[`kernel6.18-6.18.56-125.164.amzn2023` source RPM](https://cdn.amazonlinux.com/al2023/blobstore/ec20d3a788e0c8493ac8340fcac7ec15ddf92f3e776d53ad14849d82907d612d/kernel6.18-6.18.56-125.164.amzn2023.src.rpm)
applies `0065-vfio-pci-enable-mmap-ing-ena-device.patch`: ENA prefetchable BARs use `pci_iomap_wc`
and `pgprot_writecombine`. Its installed VFIO modules use `.ko.zst`. The pinned external installer
both requests the wrong package names and compresses replacements with `xz` even for `.zst` modules.
It is not invoked for the official AL2023 6.18 path.

The supported 6.18 policy trusts Amazon's kernel package contract, grounded in that published source;
it is not an independent runtime measurement of BAR memory attributes or a guarantee inferred from
a module symbol. `modinfo` must resolve an existing module owned by `kernel6.18`, vendor
`Amazon Linux`, with version/release/architecture matching `uname -r`. Its file bytes must match
that file's SHA256 digest in the owning RPM's metadata. Do not use `rpm -Vf` for this: it verifies
the entire package, and ordinary `depmod` rewrites the RPM-owned `modules.builtin.alias.bin`
index's timestamp even when VFIO is untouched. Verification also requires nonempty, equal installed/loaded
`vfio_pci_core` source versions. Custom, unowned, modified or mismatched modules fail rather than
falling back to the incompatible installer. No kernel downgrade, header substitution or extra
kernel-upgrade phase is needed. The existing command-line reboot and boot service still apply;
reconnect and run `verify` as above.

An offline inspection of the exact binary RPM reproduced that whole-package verification failure
after `depmod`, while the VFIO module's recorded digest still matched. Altering the module bytes
then failed the module-only digest check. This is a reproduced defect in the verifier, not proof
of the exact sub-check that failed on the subsequent EC2 attempt: that attempt did not log it.
Native failures now name module lookup, file existence, package ownership or digest verification.

## Test (local, no root)

```sh
sh deploy/test-hft-host.sh
```

It runs apply, boot, verify and run-binance.sh against a fake `/proc`, `/sys`, `/etc` tree with stub
commands. The build step is checked separately by running `hft-host.sh build` in an `amazonlinux:2023`
container. The reboot stub checks the exact unmanaged file at the reboot request; the simulated next
boot lets data DHCP win if it is absent, and a separate case forces a genuine management-route change
and checks that setup fails without reload, flush, or bind. Write/reload failures and no-reboot config
restoration are covered too. Real kernel modules, grubby, reboot, networkd, IRQs and vfio binding still
require a real instance.
Native WC integrity cases hash real fixture files; their checksum wrapper rejects GNU-only
verification switches so the same comparison works with GNU and macOS `sha256sum`.
