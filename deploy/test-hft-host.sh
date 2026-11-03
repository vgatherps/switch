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
printf '%s\n' '[Match]' 'MACAddress=0a:00:00:00:00:06' '' '[Link]' 'Unmanaged=yes' >"$tmp/unmanaged.network"
printf '%s\n' '[Network]' 'ManageForeignRoutingPolicyRules=no' >"$tmp/networkd-policy.conf"
mkdir -p "$stubs"
stub() { # name body
    printf '#!/bin/sh\necho "%s $*" >>"%s"\n%s\n' "$1" "$calls" "$2" >"$stubs/$1"
    chmod +x "$stubs/$1"
}
for c in pip3 curl rustup cargo install networkctl make taskset; do stub "$c" ""; done
stub chronyc "cat '$root/sources'"
# chronyc sources as AWS documents it, the PHC selected (`#*`).
printf '%s\n' 'MS Name/IP address         Stratum Poll Reach LastRx Last sample' \
    '===============================================================================' \
    '#* PHC0                          0   0   377     0   +184ns[ +198ns] +/- 5032ns' \
    '^- 169.254.169.123               1   4   377     8    -18us[  -18us] +/-  115us' >"$tmp/sources-phc"
# dmesg reads the fake kernel log; a missing file is an unreadable dmesg.
stub dmesg "cat '$root/kmsg'"
stub dnf 'case "$*" in *kernel-devel*) exit 1 ;; esac'
stub uname 'case "$1" in -r) echo "${FAKE_KERNEL:-6.1.0-test}" ;; *) echo "${FAKE_ARCH:-aarch64}" ;; esac'
stub pkg-config 'echo 25.11.3'
stub ip "[ \"\$*\" = '-4 route show default' ] && cat '$tmp/routes'; exit 0"
stub grubby "case \"\$1\" in --info=*) echo \"args=\\\"\$(cat '$tmp/grubby')\\\"\" ;; --update-kernel=*) echo \"\${2#--args=}\" >>'$tmp/grubby' ;; esac"
stub systemctl "case \"\$1\" in
    is-enabled) [ -e '$tmp/enabled' ] ;;
    is-active) [ \"\${FAKE_CHRONYD_INACTIVE:-}\" != 1 ] ;;
    enable) touch '$tmp/enabled' ;;
    reboot) cmp -s '$tmp/unmanaged.network' '$root/etc/systemd/network/00-zhida-data.network' && touch '$tmp/unmanaged-before-reboot'
        cmp -s '$tmp/networkd-policy.conf' '$root/etc/systemd/networkd.conf.d/90-zhida-kernel-receiver.conf' && touch '$tmp/policy-before-reboot'
        true ;;
    restart) [ \"\${FAKE_NETWORKD_FAIL:-}\" != 1 ] || exit 1
        if [ \"\$2\" = systemd-networkd.service ]; then
            cmp -s '$tmp/unmanaged.network' '$root/etc/systemd/network/00-zhida-data.network' &&
            cmp -s '$tmp/networkd-policy.conf' '$root/etc/systemd/networkd.conf.d/90-zhida-kernel-receiver.conf' &&
                touch '$tmp/policy-before-restart'
        fi ;;
esac"
stub pgrep "[ -e '$tmp/irqbalance' ]"
stub mountpoint '[ "$1" = -q ] && shift; [ -e "$1/.mounted" ]'
stub modinfo "case \"\$*\" in
    '-F srcversion vfio_pci_core') printf '%s\\n' \"\${FAKE_SRCVERSION-PATCHED}\" ;;
    '-n vfio_pci_core') [ \"\${FAKE_MODINFO_LOOKUP_FAIL:-no}\" = no ] || exit 1
        echo '$root/lib/modules/'\"\$(uname -r)\"'/kernel/drivers/vfio/pci/vfio-pci-core.ko.zst' ;;
esac"
stub rpm 'case "$1" in
    -qf) [ "${FAKE_RPM_ID:-}" != MISSING ] || exit 1
        case "$3" in
            "%{FILEDIGESTALGO}"*) echo "${FAKE_RPM_ALGO:-8}" ;;
            "[%{FILENAMES}"*) printf "%s %s\n" "$4" "${FAKE_RPM_DIGEST:-}" ;;
            *) printf "%s\n" "${FAKE_RPM_ID:-kernel6.18 Amazon Linux $(uname -r)}" ;;
        esac ;;
    # Real depmod rewrites this RPM-owned index; whole-package verification fails on its mtime.
    -Vf) echo ".......T. /lib/modules/$(uname -r)/modules.builtin.alias.bin"; exit 1 ;;
    *) exit 1 ;;
esac'
# zhida-netconf: discover writes pretty JSON with rx_timestamps false (as the real one does); check
# prints the field as the real one does, or fails without the file; bind moves the data device to
# vfio-pci.
stub zhida-netconf "case \"\$1\" in
    discover) printf '{\\n  \"version\": 1,\\n  \"rx_timestamps\": false,\\n  \"descriptors\": 1024\\n}\\n' >\"\$6\" ;;
    check) [ -f \"\$2\" ] && sed -n 's/^  \"rx_timestamps\": \\(.*\\),\$/rx_timestamps: \\1/p' \"\$2\" || exit 1
        # The real summary has more lines after rx_timestamps; a long one fails its writes
        # once a reader that stopped early (grep -q) has closed the pipe.
        [ -z \"\${FAKE_CHECK_LONG:-}\" ] || yes '  summary line' | head -n 100000 ;;
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
    unset FAKE_ARCH FAKE_KERNEL FAKE_SRCVERSION FAKE_RPM_ID FAKE_RPM_ALGO FAKE_RPM_DIGEST FAKE_MODINFO_LOOKUP_FAIL
    rm -rf "$root" "$calls" "$tmp/enabled" "$tmp/irqbalance" "$tmp/unmanaged-before-reboot" \
        "$tmp/policy-before-reboot" "$tmp/policy-before-restart"
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
    printf '%s\n' '[    0.000000] clocksource: Switched to clocksource kvm-clock' \
        '[    1.200000] clocksource: Switched to clocksource tsc-early' \
        '[    2.400000] clocksource: Switched to clocksource tsc' >"$root/kmsg"
    : >"$root/dev/hugepages/.mounted"
    # AL2023's chrony.conf: the Time Sync NTP server (abridged).
    printf '%s\n' 'server 169.254.169.123 prefer iburst minpoll 4 maxpoll 4' 'driftfile /var/lib/chrony/drift' \
        >"$root/etc/chrony.conf"
    cp "$tmp/sources-phc" "$root/sources"
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
want_args="isolcpus=managed_irq,domain,7 nohz_full=7 rcu_nocbs=7 irqaffinity=0,1,2,3,4,5,6 transparent_hugepage=madvise hugepagesz=2M hugepages=0:512 ena.phc_enable=1"
# What the reboot changes: the kernel command line, the modules, and what the kernel then reports.
reboot() {
    echo "ro console=ttyS0 $want_args" >"$root/proc/cmdline"
    # ena.phc_enable=1 makes the ENA PHC appear as /dev/ptp_ena.
    : >"$root/dev/ptp_ena"
    echo "${FAKE_SRCVERSION-PATCHED}" >"$root/sys/module/vfio_pci_core/srcversion"
    echo 512 >"$root/sys/devices/system/node/node0/hugepages/hugepages-2048kB/nr_hugepages"
    echo 7 >"$root/sys/devices/system/cpu/isolated"
    echo 7 >"$root/sys/devices/system/cpu/nohz_full"
    : >"$root/lib/modules/6.1.0-test/kernel/drivers/vfio/pci/vfio-pci-core.ko.xz_no_wc"
    # irqaffinity= puts the IRQs on the housekeeping CPUs at boot.
    for irq in 10 11 12; do echo 0-6 >"$root/proc/irq/$irq/effective_affinity_list"; done
    # Model networkd's next boot: without the MAC-matched unmanaged file, data DHCP wins.
    if cmp -s "$tmp/unmanaged.network" "$root/etc/systemd/network/00-zhida-data.network"; then
        echo 'default via 10.0.0.1 dev ens5 proto dhcp metric 512' >"$tmp/routes"
    else
        echo 'default via 10.0.0.1 dev ens6 proto dhcp metric 100' >"$tmp/routes"
    fi
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

host
run apply --kernel-receiver
check "kernel receiver apply reboots for CPU settings" status_is 100
check "kernel receiver persists Linux ENA mode" env_has DATA_DRIVER=ena
check "kernel receiver stages the data ENI unmanaged before reboot" test -e "$tmp/unmanaged-before-reboot"
check "kernel receiver installs policy drop-in before reboot" test -e "$tmp/policy-before-reboot"
check "kernel receiver does not restart networkd before a requested reboot" sh -c "! grep -q '^systemctl restart systemd-networkd.service' '$calls'"
check "kernel receiver apply never binds or unbinds" sh -c "! grep -q 'zhida-netconf bind\\|zhida-netconf unbind' '$calls'"
reboot
run boot
check "kernel receiver boot succeeds" status_is 0
check "kernel receiver boot keeps data ENI on ena" test "$(basename "$(readlink "$root/sys/bus/pci/devices/0000:00:06.0/driver")")" = ena
check "kernel receiver boot leaves NIC binding alone" sh -c "! grep -q 'zhida-netconf bind\\|zhida-netconf unbind' '$calls'"
check "kernel receiver boot moves IRQs to housekeeping" grep -qx "0,1,2,3,4,5,6" "$root/proc/irq/10/smp_affinity_list"
run verify
check "kernel receiver verifies on ena without VFIO" status_is 0
check "kernel receiver verify reports its actual driver" has data "ok 0000:00:06.0 (0a:00:00:00:00:06) on ena"
check "kernel receiver verify does not claim VFIO is loaded" has vfio_wc "ok not required for kernel receiver"
rm "$root/etc/systemd/networkd.conf.d/90-zhida-kernel-receiver.conf"
: >"$calls"
run apply --kernel-receiver
check "kernel receiver no-reboot apply succeeds" status_is 0
check "no-reboot apply installs policy before restarting networkd" test -e "$tmp/policy-before-restart"
check "no-reboot apply restarts networkd to activate policy" called "systemctl restart systemd-networkd.service"
check "no-reboot apply does not bind or unbind" sh -c "! grep -q 'zhida-netconf bind\\|zhida-netconf unbind' '$calls'"
: >"$calls"
FAKE_NETWORKD_FAIL=1 run apply --kernel-receiver
check "networkd restart failure stops kernel receiver setup" status_is 1
check "networkd restart failure is reported" has networkd_policy "FAIL cannot restart systemd-networkd"
check "networkd restart failure does not flush or bind" sh -c "! grep -q 'flush\\|zhida-netconf bind\\|zhida-netconf unbind' '$calls'"
ln -sfn ../../drivers/vfio-pci "$root/sys/bus/pci/devices/0000:00:06.0/driver"
run verify
check "kernel receiver verify rejects a non-ena data driver" has data "FAIL 0000:00:06.0 on 'vfio-pci', want ena"

fresh_apply
check "first apply reboots (exit 100) for the command line" status_is 100
check "the reboot is requested" called "systemctl reboot"
check "DPDK setup does not install kernel receiver policy" test ! -e "$root/etc/systemd/networkd.conf.d/90-zhida-kernel-receiver.conf"
check "DPDK setup does not restart networkd" sh -c "! grep -q '^systemctl restart systemd-networkd.service' '$calls'"
check "the exact MAC-matched unmanaged config exists when reboot is requested" test -e "$tmp/unmanaged-before-reboot"
check "staging reloads networkd before reboot" called "networkctl reload"
check "grubby gets every argument" grep -qx "$want_args" "$tmp/grubby"
check "management is the lowest-metric default route" env_has MGMT_IF=ens5
check "data is the other ena interface" sh -c "grep -qx DATA_IF=ens6 '$root/etc/zhida/host.env' && grep -qx DATA_PCI=0000:00:06.0 '$root/etc/zhida/host.env'"
check "the app CPU is the node's highest, alone isolated" sh -c "grep -qx APP_CPU=7 '$root/etc/zhida/host.env' && grep -qx ISOLATED=7 '$root/etc/zhida/host.env' && grep -qx HOUSEKEEPING=0,1,2,3,4,5,6 '$root/etc/zhida/host.env'"
check "the config comes from zhida-netconf discover --aws on the data ENI" called "zhida-netconf discover --interface ens6 --aws -o $root/etc/zhida/dut.json"
check "the host config keeps rx_timestamps as discovered (false)" grep -qx '  "rx_timestamps": false,' "$root/etc/zhida/dut.json"
check "the boot unit is installed and enabled" sh -c "grep -q 'ExecStart=/usr/local/sbin/zhida-hft-host boot' '$root/etc/systemd/system/zhida-host.service' && [ -e '$tmp/enabled' ]"
check "no interface is flushed or bound before the reboot" sh -c "! grep -q 'flush\|zhida-netconf bind' '$calls'"
# chronyd exits at start on a PHC refclock it cannot open: before the reboot there is no
# /dev/ptp_ena, so apply leaves chrony.conf alone.
check "apply leaves chrony.conf alone" sh -c "! grep -q '^refclock' '$root/etc/chrony.conf' && ! grep -q '^systemctl restart chronyd' '$calls'"
check "apply enables chronyd" called "systemctl enable -q chronyd.service"

reboot
HFT_ROOT=$root sh "$script" boot >"$tmp/out" 2>&1
echo $? >"$tmp/status"
check "boot (zhida-host.service) succeeds" status_is 0
check "boot makes the data ENI unmanaged by MAC" grep -qx "MACAddress=0a:00:00:00:00:06" "$root/etc/systemd/network/00-zhida-data.network"
check "boot flushes the data ENI and only it" sh -c "grep -q 'ip addr flush dev ens6' '$calls' && ! grep -q 'dev ens5' '$calls'"
check "boot binds with --noiommu (no IOMMU groups)" called "zhida-netconf bind $root/etc/zhida/dut.json --noiommu"
check "boot moves IRQs to housekeeping" grep -qx "0,1,2,3,4,5,6" "$root/proc/irq/10/smp_affinity_list"
check "boot stops irqbalance" called "systemctl disable --now -q irqbalance.service"
check "boot with /dev/ptp_ena adds AWS's PHC refclock line after AL2023's NTP server" sh -c "[ \"\$(tail -n 1 '$root/etc/chrony.conf')\" = 'refclock PHC /dev/ptp_ena poll 0 delay 0.000010 prefer' ] && grep -qx 'server 169.254.169.123 prefer iburst minpoll 4 maxpoll 4' '$root/etc/chrony.conf'"
check "boot restarts chronyd for the added line" called "systemctl restart chronyd.service"
: >"$calls"
HFT_ROOT=$root sh "$script" boot >"$tmp/out" 2>&1
check "a second boot keeps one refclock line and does not restart chronyd" sh -c "[ \$(grep -c '^refclock PHC' '$root/etc/chrony.conf') = 1 ] && ! grep -q '^systemctl restart chronyd' '$calls'"
# A host outside a precision-time placement group: no device, so chrony stays on NTP.
mv "$root/dev/ptp_ena" "$tmp/ptp_ena"
sed -i.bak '/^refclock PHC/d' "$root/etc/chrony.conf"
cp "$root/etc/chrony.conf" "$tmp/chrony.conf"
: >"$calls"
HFT_ROOT=$root sh "$script" boot >"$tmp/out" 2>&1
check "boot without /dev/ptp_ena leaves chrony.conf alone" sh -c "cmp -s '$tmp/chrony.conf' '$root/etc/chrony.conf' && ! grep -q '^systemctl restart chronyd' '$calls'"
# A host whose earlier apply added the line without the device: boot removes exactly that line.
printf '%s\n' 'refclock PHC /dev/ptp0 poll 2' >>"$root/etc/chrony.conf"
cp "$root/etc/chrony.conf" "$tmp/chrony-other.conf"
printf '%s\n' 'refclock PHC /dev/ptp_ena poll 0 delay 0.000010 prefer' >>"$root/etc/chrony.conf"
: >"$calls"
HFT_ROOT=$root sh "$script" boot >"$tmp/out" 2>&1
check "boot without /dev/ptp_ena removes only the PHC refclock line and restarts chronyd" sh -c "cmp -s '$tmp/chrony-other.conf' '$root/etc/chrony.conf' && [ \$(grep -c '^systemctl restart chronyd.service' '$calls') = 1 ]"
: >"$calls"
HFT_ROOT=$root sh "$script" boot >"$tmp/out" 2>&1
check "a second boot without /dev/ptp_ena changes nothing" sh -c "cmp -s '$tmp/chrony-other.conf' '$root/etc/chrony.conf' && ! grep -q '^systemctl restart chronyd' '$calls'"
cp "$tmp/chrony.conf" "$root/etc/chrony.conf"
mv "$tmp/ptp_ena" "$root/dev/ptp_ena"
HFT_ROOT=$root sh "$script" boot >/dev/null 2>&1

: >"$calls"
run apply
check "apply after the reboot needs no reboot and succeeds" status_is 0
check "a second apply does not rerun grubby" sh -c "! grep -q '^grubby --update' '$calls'"
check "a bound data ENI is not rediscovered" sh -c "! grep -q 'discover' '$calls'"
check "apply reports the data ENI as already bound" has interfaces "ok management ens5; data 0000:00:06.0 (0a:00:00:00:00:06) already on vfio-pci"
cp "$root/etc/zhida/host.env" "$tmp/host.env"
: >"$calls"
run apply --kernel-receiver
check "kernel receiver apply refuses a vfio-bound data ENI" sh -c "[ \$(cat '$tmp/status') = 1 ] && grep -q '^interfaces: FAIL data 0000:00:06.0 is on vfio-pci' '$tmp/out'"
check "the refused kernel receiver apply leaves host.env and the binding alone" sh -c "cmp -s '$tmp/host.env' '$root/etc/zhida/host.env' && ! grep -q '^zhida-netconf\\|^networkctl\\|^systemctl\\|^grubby' '$calls'"
check "an unchanged unmanaged config does not reload networkd" sh -c "! grep -q '^networkctl reload' '$calls'"
check "an unchanged chrony.conf keeps one refclock line and no restart" sh -c "[ \$(grep -c '^refclock PHC' '$root/etc/chrony.conf') = 1 ] && ! grep -q '^systemctl restart chronyd' '$calls'"
sed -i.bak '/^refclock PHC/d' "$root/etc/chrony.conf"
: >"$calls"
run apply
check "apply's boot restores a removed refclock line with /dev/ptp_ena present and restarts chronyd" sh -c "grep -q '^systemctl restart chronyd.service' '$calls' && grep -qx 'refclock PHC /dev/ptp_ena poll 0 delay 0.000010 prefer' '$root/etc/chrony.conf'"

rm "$root/etc/systemd/network/00-zhida-data.network"
: >"$calls"
run apply
check "no-reboot reapply restores a missing unmanaged config" status_is 0
check "no-reboot reapply writes the exact MAC-matched config" cmp -s "$tmp/unmanaged.network" "$root/etc/systemd/network/00-zhida-data.network"
check "no-reboot reapply reloads networkd for the restored config" called "networkctl reload"
check "restoring unmanaged config does not request a reboot" sh -c "! grep -q '^systemctl reboot' '$calls'"

run verify
check "a finished host verifies clean" status_is 0
check "verify names every setting" sh -c "[ \$(grep -c ': ok ' '$tmp/out') = 13 ]"
check "verify accepts chrony with the PHC selected" has chrony "ok PHC selected"
sed 's/^#\* PHC0/#- PHC0/; s/^\^- 169/^* 169/' "$tmp/sources-phc" >"$root/sources"
run verify
check "chrony on NTP, PHC not selected, fails with the sources output" sh -c "grep -q '^chrony: FAIL the PHC is not the selected source' '$tmp/out' && grep -q '^\\^\\* 169.254.169.123' '$tmp/out'"
FAKE_CHRONYD_INACTIVE=1 run verify
check "verify fails when chronyd is not running" sh -c "[ \$(cat '$tmp/status') = 1 ] && grep -q '^chrony: FAIL chronyd not running' '$tmp/out'"
# A host outside a precision-time placement group has no PHC device: NTP only is its normal state.
mv "$root/dev/ptp_ena" "$tmp/ptp_ena"
run verify
check "chrony on NTP without a PHC device verifies" sh -c "[ \$(cat '$tmp/status') = 0 ] && grep -q '^chrony: ok no PHC device' '$tmp/out'"
mv "$tmp/ptp_ena" "$root/dev/ptp_ena"
cp "$tmp/sources-phc" "$root/sources"
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
check "run-binance pins zhida-nic feed to the app CPU with its own config" called "taskset -c 7 zhida-nic --config $root/etc/zhida/binance.json feed --seconds 5"
check "run-binance's config asks for hardware RX timestamps" grep -qx '  "rx_timestamps": true,' "$root/etc/zhida/binance.json"
check "and is the host config otherwise" sh -c "sed 's/\"rx_timestamps\": true/\"rx_timestamps\": false/' '$root/etc/zhida/binance.json' | cmp -s - '$root/etc/zhida/dut.json'"
check "the host config is left as discovered" grep -qx '  "rx_timestamps": false,' "$root/etc/zhida/dut.json"
: >"$calls"
FAKE_CHECK_LONG=1 HFT_ROOT=$root bash "$here/run-binance.sh" --seconds 5 >"$tmp/out" 2>&1
check "run-binance starts when check's summary outlives an early-exiting reader" called "taskset -c 7 zhida-nic --config $root/etc/zhida/binance.json feed --seconds 5"
# The feed compares CLOCK_REALTIME with PHC stamps: without the PHC selected it refuses, although
# verify passes (no PHC device).
mv "$root/dev/ptp_ena" "$tmp/ptp_ena"
sed 's/^#\* PHC0/#- PHC0/; s/^\^- 169/^* 169/' "$tmp/sources-phc" >"$root/sources"
: >"$calls"
HFT_ROOT=$root bash "$here/run-binance.sh" --seconds 5 >"$tmp/out" 2>&1
check "run-binance refuses without the PHC selected and says to use a precision-time placement group" sh -c "[ $? = 1 ] && grep -q 'precision-time placement group' '$tmp/out' && ! grep -q '^taskset' '$calls'"
mv "$tmp/ptp_ena" "$root/dev/ptp_ena"
cp "$tmp/sources-phc" "$root/sources"
# A config whose rx_timestamps line is not netconf's format cannot be turned on: refuse.
cp "$root/etc/zhida/dut.json" "$tmp/dut"
printf '{"version": 1, "rx_timestamps": false}\n' >"$root/etc/zhida/dut.json"
: >"$calls"
HFT_ROOT=$root bash "$here/run-binance.sh" --seconds 5 >"$tmp/out" 2>&1
check "run-binance refuses when rx_timestamps cannot be set" sh -c "[ $? = 1 ] && grep -q 'not starting' '$tmp/out' && ! grep -q '^taskset' '$calls'"
cp "$tmp/dut" "$root/etc/zhida/dut.json"

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
run boot
check "boot refuses when the default route moved" has unmanaged "FAIL default route moved from ens5 to ens6"
check "a changed management route exits unsuccessfully" status_is 1
check "and neither reloads networkd nor touches an interface" sh -c "! grep -q 'networkctl reload\|flush\|zhida-netconf bind' '$calls'"

stub networkctl 'exit 1'
fresh_apply
check "a failed staging reload fails apply" status_is 1
check "the staging reload failure is reported" has unmanaged "FAIL networkctl reload failed"
check "a failed staging reload neither reboots nor touches an interface" sh -c "! grep -q 'systemctl reboot\|flush\|zhida-netconf bind' '$calls'"
stub networkctl ""

fresh_apply
reboot
rm "$root/etc/systemd/network/00-zhida-data.network"
mkdir "$root/etc/systemd/network/00-zhida-data.network"
: >"$calls"
run boot
check "an unwritable unmanaged config fails boot" status_is 1
check "the config write failure is reported" has unmanaged "FAIL cannot write"
check "a failed config write neither reloads networkd nor touches an interface" sh -c "! grep -q 'networkctl reload\|flush\|zhida-netconf bind' '$calls'"

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
check "a boot through kvm-clock and tsc-early to tsc is clean" has clocksource "ok tsc (x86_64); dmesg shows no TSC instability"
cp "$root/kmsg" "$tmp/kmsg"
for line in '[  812.000000] clocksource: timekeeping watchdog on CPU3: Marking clocksource '"'"'tsc'"'"' as unstable because the skew is too large:' \
    '[  812.000001] tsc: Marking TSC unstable due to clocksource watchdog' \
    '[  812.000002] clocksource: Marking clocksource '"'"'tsc'"'"' as unstable' \
    '[  812.000003] clocksource: Switched to clocksource hpet'; do
    { cat "$tmp/kmsg"; echo "$line"; } >"$root/kmsg"
    FAKE_ARCH=x86_64 run verify
    check "x86_64 verify fails on: $line" grep -qxF "clocksource: FAIL $line" "$tmp/out"
done
rm "$root/kmsg"
FAKE_ARCH=x86_64 run verify
check "x86_64 verify fails when dmesg cannot be read" has clocksource "FAIL cannot read dmesg"
{ cat "$tmp/kmsg"; echo '[  812.000002] clocksource: Marking clocksource '"'"'tsc'"'"' as unstable'; } >"$root/kmsg"
run verify
check "a non-x86_64 host skips the dmesg check" has clocksource "ok tsc (aarch64)"

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

# Standard AL2023 6.18 already supplies ENA WC. No headers, source download or *_no_wc marker.
native_host() {
    host
    FAKE_KERNEL=${1:-6.18.56-125.164.amzn2023.aarch64}
    FAKE_SRCVERSION=NATIVE
    export FAKE_KERNEL FAKE_SRCVERSION
    native_module=$root/lib/modules/$FAKE_KERNEL/kernel/drivers/vfio/pci/vfio-pci-core.ko.zst
    mkdir -p "$(dirname "$native_module")"
    : >"$native_module"
    FAKE_RPM_DIGEST=$(sha256sum "$native_module" | cut -d' ' -f1)
    export FAKE_RPM_DIGEST
    echo NATIVE >"$root/sys/module/vfio_pci_core/srcversion"
}

# Preserve real file hashing, but reject GNU-only verification flags even on Linux.
native_checksum=$(command -v sha256sum)
stub sha256sum "case \"\$1\" in --*) exit 2 ;; esac; exec '$native_checksum' \"\$@\""

native_host
check "a fresh native fixture resets the earlier x86_64 architecture" sh -c '[ "$(uname -m)" = aarch64 ]'
check "normal depmod metadata makes whole-package verification fail" sh -c "! rpm -Vf '$native_module' >/dev/null"
run apply
check "official 6.18 apply reboots for cmdline, not a VFIO rebuild" status_is 100
check "native WC needs no kernel-devel or amzn-drivers download" sh -c "! grep -q 'kernel-devel\|amzn-drivers' '$calls'"
check "native WC checks RPM ownership and the module's own digest" sh -c "grep -q '^rpm -qf ' '$calls' && grep -q 'FILEDIGESTALGO' '$calls' && grep -q 'FILEDIGESTS' '$calls'"
check "native apply does not touch the data ENI before reboot" sh -c "! grep -q 'flush\|zhida-netconf bind' '$calls'"
reboot
run boot
check "native WC resumes host setup after reboot" status_is 0
run verify
check "native 6.18 host verifies without a legacy backup marker" status_is 0
check "verify identifies Amazon's native ENA WC" has vfio_wc "ok Amazon Linux ENA write-combining vfio-pci-core loaded"
: >"$calls"
run apply
check "native apply after reboot is idempotent" status_is 0
check "native reapply does not request another reboot" sh -c "! grep -q '^systemctl reboot' '$calls'"

echo DIFFERENT >"$root/sys/module/vfio_pci_core/srcversion"
run verify
check "native WC rejects a loaded module that differs from disk" has vfio_wc "FAIL loaded vfio-pci-core differs from the verified on-disk module"
run apply
check "native apply reboots to replace a different loaded module" status_is 100
check "native apply names the module reboot reason" has vfio_wc "changed write-combining vfio-pci installed"

native_host
printf 'changed module bytes\n' >>"$native_module"
run apply
check "modified official 6.18 modules are refused with a digest diagnostic" grep -q "vfio_wc: FAIL .*module SHA256 differs from its owning RPM" "$tmp/out"
check "modified native modules do not fall back to the incompatible installer" sh -c "! grep -q 'kernel-devel\|amzn-drivers\|systemctl reboot' '$calls'"
run verify
check "verify does not accept modified native modules" has vfio_wc "FAIL no write-combining vfio-pci"

for identity in MISSING "kernel6.18 Custom Linux $FAKE_KERNEL" \
    "kernel6.18 Amazon Linux 6.18.55-other.aarch64" "custom-kernel Amazon Linux $FAKE_KERNEL"; do
    native_host
    FAKE_RPM_ID=$identity
    export FAKE_RPM_ID
    # A legacy backup must not authorize an untrusted 6.18 module.
    : >"${native_module}_no_wc"
    run apply
    check "native WC refuses wrong module provenance ($identity)" grep -q "vfio_wc: FAIL .*not owned by the official Amazon Linux kernel6.18 package" "$tmp/out"
done
native_host
rm "$native_module"
run apply
check "native WC diagnoses a missing module file" grep -q "vfio_wc: FAIL .*module file missing:" "$tmp/out"
native_host
FAKE_MODINFO_LOOKUP_FAIL=yes
export FAKE_MODINFO_LOOKUP_FAIL
run apply
check "native WC diagnoses unavailable module indexes" grep -q "vfio_wc: FAIL .*modinfo cannot resolve vfio_pci_core" "$tmp/out"
native_host
FAKE_RPM_ALGO=10
export FAKE_RPM_ALGO
run apply
check "native WC rejects an unsupported RPM digest algorithm" grep -q "vfio_wc: FAIL .*unsupported or unavailable RPM file digest algorithm" "$tmp/out"
native_host
FAKE_RPM_DIGEST=
run apply
check "native WC diagnoses a missing recorded module digest" grep -q "vfio_wc: FAIL .*RPM SHA256 digest unavailable" "$tmp/out"
native_host
run apply
FAKE_SRCVERSION=
export FAKE_SRCVERSION
: >"$root/sys/module/vfio_pci_core/srcversion"
run verify
check "empty loaded and disk source versions cannot verify WC" has vfio_wc "FAIL loaded vfio-pci-core differs from the verified on-disk module"

native_host 6.18.56-125.164.amzn2023.x86_64
FAKE_ARCH=x86_64 run apply
check "official x86_64 6.18 also uses vendor-native WC" status_is 100
check "native x86_64 WC needs no external rebuild" sh -c "! grep -q 'kernel-devel\|amzn-drivers' '$calls'"

host
run apply
check "legacy kernels still fail when exact kernel-devel is missing" has vfio_wc "FAIL kernel-devel-6.1.0-test or amzn-drivers"
check "legacy missing headers do not trigger a reboot" sh -c "! grep -q '^systemctl reboot' '$calls'"

if [ "$fails" -ne 0 ]; then
    echo "test-hft-host: $fails failed"
    exit 1
fi
echo "test-hft-host: all passed"
