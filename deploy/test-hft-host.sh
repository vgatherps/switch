#!/bin/sh
# Tests of hft-host.sh and run-binance.sh against a fake /proc, /sys, /etc, /lib, /dev tree (HFT_ROOT)
# with stub commands; no root, no network. The build step runs with stubs that report it done.
# Usage: sh deploy/test-hft-host.sh
# shellcheck disable=SC2016 # stub bodies expand when the stub runs
set -u
here=$(cd "$(dirname "$0")" && pwd)
script=$here/hft-host.sh
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
fails=0
check() { # description, command...
    desc=$1
    shift
    if "$@"; then echo "ok: $desc"; else echo "FAIL: $desc"; fails=$((fails + 1)); fi
}

root=$tmp/root
calls=$tmp/calls
stubs=$tmp/bin
mkdir -p "$stubs"
stub() { # name body
    printf '#!/bin/sh\necho "%s $*" >>"%s"\n%s\n' "$1" "$calls" "$2" >"$stubs/$1"
    chmod +x "$stubs/$1"
}
for c in dnf pip3 curl rustup cargo install networkctl make taskset; do stub "$c" ""; done
stub uname 'case "$1" in -r) echo 6.1.0-test ;; *) echo "${FAKE_ARCH:-aarch64}" ;; esac'
stub pkg-config 'echo 25.11.3'
stub ip "[ \"\$*\" = '-4 route show default' ] && cat '$tmp/routes'; exit 0"
stub grubby "case \"\$1\" in --info=*) echo \"args=\\\"\$(cat '$tmp/grubby')\\\"\" ;; --update-kernel=*) echo \"\${2#--args=}\" >>'$tmp/grubby' ;; esac"
stub systemctl "case \"\$1\" in is-enabled) [ -e '$tmp/enabled' ] ;; enable) touch '$tmp/enabled' ;; esac"
stub pgrep "[ -e '$tmp/irqbalance' ]"
stub mountpoint '[ "$1" = -q ] && shift; [ -e "$1/.mounted" ]'
stub modinfo "[ \"\$*\" = '-F srcversion vfio_pci_core' ] && echo PATCHED; exit 0"
# zhida-netconf: discover writes the config; check needs it; bind moves the data device to vfio-pci.
stub zhida-netconf "case \"\$1\" in
    discover) echo '{}' >\"\$6\" ;;
    check) [ -f \"\$2\" ] ;;
    bind) ln -sfn ../../drivers/vfio-pci '$root/sys/bus/pci/devices/0000:00:06.0/driver' ;;
esac"
stub zhida-nic "echo 'zhida-nic: hugepages: hugepages-2048kB used=23 free=489'"
export PATH="$stubs:$PATH"

nic() { # name pci mac driver
    d=$root/sys/bus/pci/devices/$2
    mkdir -p "$d" "$root/sys/bus/pci/drivers/$4" "$root/sys/class/net/$1"
    ln -sfn "../../drivers/$4" "$d/driver"
    ln -sfn "../../../bus/pci/devices/$2" "$root/sys/class/net/$1/device"
    echo "$3" >"$root/sys/class/net/$1/address"
    echo 0 >"$d/numa_node"
}

patch_file=$here/../conduit/docker/dpdk-ena-read-clock.patch
dpdk_stamp="dpdk-25.11.3 3719acc586b310c4f60ba230683bf4f1e12c6f2f5bee11f7c01b1bbd0ded7490 patch $(sha256sum "$patch_file" | cut -d' ' -f1)"

# A fresh c7g-like host: CPUs 0-7 on node 0, no SMT, management ens5, data ens6, stock modules.
host() {
    rm -rf "$root" "$calls" "$tmp/enabled" "$tmp/irqbalance"
    mkdir -p "$root/proc" "$root/run" "$root/etc" "$root/dev/hugepages" "$root/sys/kernel/iommu_groups" \
        "$root/sys/devices/system/node/node0/hugepages/hugepages-2048kB" "$root/sys/module/vfio_pci_core" \
        "$root/sys/devices/system/clocksource/clocksource0" "$root/lib/modules/6.1.0-test/kernel/drivers/vfio/pci" \
        "$root/sys/devices/system/cpu"
    echo "ro console=ttyS0" >"$root/proc/cmdline"
    echo "ro console=ttyS0" >"$tmp/grubby"
    echo 0-7 >"$root/sys/devices/system/cpu/online"
    echo 0-7 >"$root/sys/devices/system/node/node0/cpulist"
    for c in 0 1 2 3 4 5 6 7; do
        mkdir -p "$root/sys/devices/system/cpu/cpu$c/topology"
        echo "$c" >"$root/sys/devices/system/cpu/cpu$c/topology/thread_siblings_list"
    done
    echo 0 >"$root/sys/devices/system/node/node0/hugepages/hugepages-2048kB/nr_hugepages"
    echo STOCK >"$root/sys/module/vfio_pci_core/srcversion"
    echo arch_sys_counter >"$root/sys/devices/system/clocksource/clocksource0/current_clocksource"
    : >"$root/dev/hugepages/.mounted"
    # /opt/dpdk as this script builds it: 25.11.3 with the ENA read_clock patch.
    mkdir -p "$root/opt/dpdk/share"
    echo "$dpdk_stamp" >"$root/opt/dpdk/share/zhida-dpdk.stamp"
    for irq in 10 11 12; do
        mkdir -p "$root/proc/irq/$irq"
        echo 0-7 >"$root/proc/irq/$irq/smp_affinity_list"
        echo 7 >"$root/proc/irq/$irq/effective_affinity_list"
    done
    nic ens5 0000:00:05.0 0a:00:00:00:00:05 ena
    nic ens6 0000:00:06.0 0a:00:00:00:00:06 ena
    printf '%s\n' 'default via 10.0.0.1 dev ens6 proto dhcp src 10.0.0.20 metric 513' \
        'default via 10.0.0.1 dev ens5 proto dhcp src 10.0.0.10 metric 512' >"$tmp/routes"
}
want_args="isolcpus=managed_irq,domain,7 nohz_full=7 rcu_nocbs=7 irqaffinity=0,1,2,3,4,5,6 transparent_hugepage=madvise hugepagesz=2M hugepages=0:512"
# What the reboot changes: the kernel command line, the modules, and what the kernel then reports.
reboot() {
    echo "ro console=ttyS0 $want_args" >"$root/proc/cmdline"
    echo PATCHED >"$root/sys/module/vfio_pci_core/srcversion"
    echo 512 >"$root/sys/devices/system/node/node0/hugepages/hugepages-2048kB/nr_hugepages"
    echo 7 >"$root/sys/devices/system/cpu/isolated"
    echo 7 >"$root/sys/devices/system/cpu/nohz_full"
    : >"$root/lib/modules/6.1.0-test/kernel/drivers/vfio/pci/vfio-pci-core.ko.xz_no_wc"
    # irqaffinity= puts the IRQs on the housekeeping CPUs at boot.
    for irq in 10 11 12; do echo 0-6 >"$root/proc/irq/$irq/effective_affinity_list"; done
}
run() { # mode [args]
    HFT_ROOT=$root sh "$script" "$@" >"$tmp/out" 2>&1
    echo $? >"$tmp/status"
}
status_is() { [ "$(cat "$tmp/status")" = "$1" ]; }
has() { grep -q "^$1: $2" "$tmp/out"; }
called() { grep -q -- "$1" "$calls" 2>/dev/null; }
env_has() { grep -qx "$1" "$root/etc/zhida/host.env"; }

# vfio-pci is already patched on the fake host so apply does not build it (that path downloads
# amzn-drivers); the reboot then only applies the command line.
fresh_apply() {
    host
    : >"$root/lib/modules/6.1.0-test/kernel/drivers/vfio/pci/vfio-pci-core.ko.xz_no_wc"
    echo PATCHED >"$root/sys/module/vfio_pci_core/srcversion"
    run apply
}

fresh_apply
check "first apply reboots (exit 100) for the command line" status_is 100
check "the reboot is requested" called "systemctl reboot"
check "grubby gets every argument" grep -qx "$want_args" "$tmp/grubby"
check "management is the lowest-metric default route" env_has MGMT_IF=ens5
check "data is the other ena interface" sh -c "grep -qx DATA_IF=ens6 '$root/etc/zhida/host.env' && grep -qx DATA_PCI=0000:00:06.0 '$root/etc/zhida/host.env'"
check "the app CPU is the node's highest, alone isolated" sh -c "grep -qx APP_CPU=7 '$root/etc/zhida/host.env' && grep -qx ISOLATED=7 '$root/etc/zhida/host.env' && grep -qx HOUSEKEEPING=0,1,2,3,4,5,6 '$root/etc/zhida/host.env'"
check "the config comes from zhida-netconf discover --aws on the data ENI" called "zhida-netconf discover --interface ens6 --aws -o $root/etc/zhida/dut.json"
check "the boot unit is installed and enabled" sh -c "grep -q 'ExecStart=/usr/local/sbin/zhida-hft-host boot' '$root/etc/systemd/system/zhida-host.service' && [ -e '$tmp/enabled' ]"
check "nothing touches an interface before the reboot" sh -c "! grep -q 'flush\|zhida-netconf bind' '$calls'"

reboot
HFT_ROOT=$root sh "$script" boot >"$tmp/out" 2>&1
echo $? >"$tmp/status"
check "boot (zhida-host.service) succeeds" status_is 0
check "boot makes the data ENI unmanaged by MAC" grep -qx "MACAddress=0a:00:00:00:00:06" "$root/etc/systemd/network/00-zhida-data.network"
check "boot flushes the data ENI and only it" sh -c "grep -q 'ip addr flush dev ens6' '$calls' && ! grep -q 'dev ens5' '$calls'"
check "boot binds with --noiommu (no IOMMU groups)" called "zhida-netconf bind $root/etc/zhida/dut.json --noiommu"
check "boot moves IRQs to housekeeping" grep -qx "0,1,2,3,4,5,6" "$root/proc/irq/10/smp_affinity_list"
check "boot stops irqbalance" called "systemctl disable --now -q irqbalance.service"

: >"$calls"
run apply
check "apply after the reboot needs no reboot and succeeds" status_is 0
check "a second apply does not rerun grubby" sh -c "! grep -q '^grubby --update' '$calls'"
check "a bound data ENI is not rediscovered" sh -c "! grep -q 'discover' '$calls'"
check "apply reports the data ENI as already bound" has interfaces "ok management ens5; data 0000:00:06.0 (0a:00:00:00:00:06) already on vfio-pci"

run verify
check "a finished host verifies clean" status_is 0
check "verify names every setting" sh -c "[ \$(grep -c ': ok ' '$tmp/out') = 12 ]"
check "verify sees management on its kernel driver with the default route" has management "ok ens5 on ena with the default route"
check "verify reports no run yet" has pools "ok no run logged yet"

mkdir -p "$root/var/log/zhida"
echo 'zhida-nic: hugepages: hugepages-2048kB used=600 free=0' >"$root/var/log/zhida/binance.log"
run verify
check "a run that took more pages than reserved fails" has pools "FAIL last run took 600"
HFT_ROOT=$root bash "$here/run-binance.sh" --seconds 5 >"$tmp/out" 2>&1
check "run-binance refuses while verify fails" sh -c "[ $? = 1 ] && ! grep -q '^zhida-nic ' '$calls'"
echo 'zhida-nic: hugepages: hugepages-2048kB used=23 free=489' >>"$root/var/log/zhida/binance.log"
run verify
check "the last run's own report is what counts" has pools "ok last run took 23 of 512"
: >"$calls"
HFT_ROOT=$root bash "$here/run-binance.sh" --seconds 5 >"$tmp/out" 2>&1
check "run-binance pins zhida-nic feed to the app CPU" called "taskset -c 7 zhida-nic --config $root/etc/zhida/dut.json feed --seconds 5"

echo 7 >"$root/proc/irq/11/effective_affinity_list"
ln -sfn ../../drivers/ena "$root/sys/bus/pci/devices/0000:00:06.0/driver"
echo STOCK >"$root/sys/module/vfio_pci_core/srcversion"
run verify
check "verify fails on an IRQ on an isolated CPU" has irqs "FAIL on isolated CPUs: 11(7)"
check "verify fails when data is back on ena" has data "FAIL 0000:00:06.0 on 'ena'"
check "verify fails on stock vfio-pci" has vfio_wc "FAIL loaded vfio-pci-core is not the patched one"

host
printf '%s\n' 'default via 10.0.0.1 dev ens5 metric 512' >"$tmp/routes"
rm -r "$root/sys/class/net/ens6"
run apply
check "a host with only the management ENI is refused" has interfaces "FAIL want exactly one ena interface besides management ens5, have 0"
host
run apply --interface ens5
check "--interface naming the management ENI is refused" has interfaces "FAIL --interface ens5 carries the default route"
host
nic ens7 0000:00:07.0 0a:00:00:00:00:07 ena
run apply
check "two data candidates need --interface" has interfaces "FAIL want exactly one ena interface besides management ens5, have 2: ens6 ens7"

fresh_apply
reboot
printf '%s\n' 'default via 10.0.0.1 dev ens6 metric 100' >"$tmp/routes"
: >"$calls"
HFT_ROOT=$root sh "$script" boot >"$tmp/out" 2>&1
check "boot refuses when the default route moved" has unmanaged "FAIL default route moved from ens5 to ens6"
check "and touches no interface" sh -c "! grep -q 'flush\|zhida-netconf bind' '$calls'"

host
echo 3,7 >"$root/sys/devices/system/cpu/cpu7/topology/thread_siblings_list"
: >"$root/lib/modules/6.1.0-test/kernel/drivers/vfio/pci/vfio-pci-core.ko.xz_no_wc"
run apply
check "an SMT sibling is isolated with the app CPU" sh -c "grep -qx ISOLATED=3,7 '$root/etc/zhida/host.env' && grep -qx HOUSEKEEPING=0,1,2,4,5,6 '$root/etc/zhida/host.env'"

host
: >"$root/lib/modules/6.1.0-test/kernel/drivers/vfio/pci/vfio-pci-core.ko.xz_no_wc"
echo PATCHED >"$root/sys/module/vfio_pci_core/srcversion"
FAKE_ARCH=x86_64 run apply
check "x86_64 adds clocksource=tsc and nothing else for the TSC" sh -c "grep -qx '$want_args clocksource=tsc' '$tmp/grubby'"
reboot
echo "ro $want_args clocksource=tsc" >"$root/proc/cmdline"
FAKE_ARCH=x86_64 HFT_ROOT=$root sh "$script" boot >/dev/null 2>&1
echo kvm-clock >"$root/sys/devices/system/clocksource/clocksource0/current_clocksource"
FAKE_ARCH=x86_64 run verify
check "x86_64 verify flags a clocksource other than tsc" has clocksource "FAIL kvm-clock, want tsc on x86_64"
echo tsc >"$root/sys/devices/system/clocksource/clocksource0/current_clocksource"
FAKE_ARCH=x86_64 run verify
check "x86_64 on tsc verifies clean" status_is 0

# The DPDK build options must stay the Docker image's.
dockerfile=$here/../conduit/docker/Dockerfile
set -f
for opt in $(grep -o -- "-D[a-z_]*=[^ ]*" "$dockerfile" | tr -d "'\\\\"); do
    tr -d "'\\\\" <"$script" | grep -qF -- "$opt" || { echo "FAIL: hft-host.sh lacks the Dockerfile's meson option $opt"; fails=$((fails + 1)); }
done
set +f
check "the Dockerfile's meson options are all in hft-host.sh" [ "$(grep -o -- "-D[a-z_]*=" "$dockerfile" | wc -l)" -ge 7 ]

# A stock 25.11.3 (no stamp) or one built with another patch is rebuilt; the patched one is kept.
host
run build
check "a patched DPDK is not rebuilt" sh -c "! grep -q 'curl.*dpdk-25.11.3.tar.xz' '$calls'"
check "build names the patch" has dpdk "ok 25.11.3 with dpdk-ena-read-clock.patch"
rm "$root/opt/dpdk/share/zhida-dpdk.stamp"
run build
check "a stock DPDK (pkg-config says 25.11.3, no stamp) is rebuilt" called "curl --fail --location --retry 2 -s -o $root/var/tmp/dpdk-25.11.3.tar.xz https://fast.dpdk.org/rel/dpdk-25.11.3.tar.xz"
echo "dpdk-25.11.3 3719acc586b310c4f60ba230683bf4f1e12c6f2f5bee11f7c01b1bbd0ded7490 patch 0000" >"$root/opt/dpdk/share/zhida-dpdk.stamp"
: >"$calls"
run build
check "a DPDK built with another patch is rebuilt" called "curl .*dpdk-25.11.3.tar.xz"
check "the patch hft-host.sh applies is in the repository" [ -s "$patch_file" ]

if [ "$fails" -ne 0 ]; then
    echo "test-hft-host: $fails failed"
    exit 1
fi
echo "test-hft-host: all passed"
