#!/bin/sh
# Turns a fresh Amazon Linux 2023 EC2 instance (x86_64 or arm64) with two ENIs into a host for the
# Binance listener (`zhida-nic feed`, run by deploy/run-binance.sh). Run as root from the repository.
#
# Usage: hft-host.sh build|apply|boot|verify [--interface IF] [--kernel-receiver]
#
#   build   dnf toolchain, rustup (rust-toolchain.toml), DPDK 25.11.3 under /opt/dpdk with
#           conduit/docker/Dockerfile's meson options, release zhida-nic (mimalloc) and zhida-netconf
#           into /usr/local/bin.
#   apply   build, then every setting below. Reboots once when the kernel command line or the ENA
#           module changed; zhida-host.service runs `boot` at every boot, after which verify passes.
#   boot    the settings a reboot resets: irqbalance off, IRQs on the housekeeping CPUs, the data ENI
#           unmanaged and flushed, then bound to vfio-pci unless --kernel-receiver was used.
#   verify  one line per setting, "<step>: ok <detail>" or "<step>: FAIL <detail>"; exit 1 if any failed.
#
# Interfaces: the management ENI is the interface of the lowest-metric IPv4 default route; nothing
# here changes it, and every step that touches the data ENI first checks that the two differ. The data
# ENI is the one other interface on the ena driver (--interface IF when there are several). Its config
# (/etc/zhida/dut.json) comes from `zhida-netconf discover --aws`.
# CPUs: the app CPU is the highest-numbered CPU on the data ENI's NUMA node; it and its SMT siblings
# are isolated (isolcpus, nohz_full, rcu_nocbs); every other CPU is housekeeping (irqaffinity, IRQs).
# Hugepages: 512 x 2 MiB on the data ENI's NUMA node, reserved on the kernel command line.
# x86_64 also gets clocksource=tsc (not tsc=reliable: that turns off the kernel's TSC stability
# checks); Graviton's arch_sys_counter needs nothing.
# Kernel module: official AL2023 kernel6.18 supplies ENA write-combining (LLQ); older kernels use
# amzn-drivers at AMZN_COMMIT, built for the running kernel. Rerun apply after a kernel update.
set -u
R=${HFT_ROOT:-}
ETC=$R/etc/zhida
ENV=$ETC/host.env
DUT=$ETC/dut.json
LOG=$R/var/log/zhida/binance.log
UNIT=$R/etc/systemd/system/zhida-host.service
SELF=$R/usr/local/sbin/zhida-hft-host
NETWORK=$R/etc/systemd/network/00-zhida-data.network
NETWORKD_POLICY=$R/etc/systemd/networkd.conf.d/90-zhida-kernel-receiver.conf
UNMOVABLE=$R/run/zhida-unmovable-irqs
HUGEPAGES=512

DPDK_VERSION=25.11.3
DPDK_SHA256=3719acc586b310c4f60ba230683bf4f1e12c6f2f5bee11f7c01b1bbd0ded7490
AMZN_COMMIT=9d5ca4021f7c9f8421e404c1e89edb0b1325372c
AMZN_SHA256=895f08f081023b4840d0555cb79d9d13ac1dd0a6722c4dc1181a34927e2b2d39

die() { echo "hft-host: $*" >&2; exit 2; }
usage="usage: hft-host.sh build|apply|boot|verify [--interface IF] [--kernel-receiver]"
mode=${1:-}
case "$mode" in build | apply | boot | verify) ;; *) die "$usage" ;; esac
shift
want_if=
requested_driver=vfio-pci
while [ $# -gt 0 ]; do
    case "$1" in
        --interface) [ $# -ge 2 ] || die "--interface needs a value"; want_if=$2; shift 2 ;;
        --kernel-receiver) requested_driver=ena; shift ;;
        *) die "unknown option $1; $usage" ;;
    esac
done
[ -n "$R" ] || [ "$(id -u)" = 0 ] || die "run as root"

failed=0
reboot_needed=
report() { # step status detail
    echo "$1: $2 $3"
    [ "$2" = FAIL ] && failed=1
    return 0
}
# A step that cannot continue: report it and stop with status 1.
fatal() { report "$1" FAIL "$2"; exit 1; }

# CPU list (e.g. 2-5,8) to its members, one per line.
expand() {
    echo "$1" | tr ',' '\n' | while IFS=- read -r a b; do [ -n "$a" ] && seq "$a" "${b:-$a}"; done
}
# Members (one per line) back to a comma list.
join() { paste -sd, -; }
overlap() { # list list
    for c in $(expand "$1"); do expand "$2" | grep -qx "$c" && return 0; done
    return 1
}
# Driver bound to a PCI device, or empty.
pci_driver() {
    d=$(readlink "$R/sys/bus/pci/devices/$1/driver" 2>/dev/null) && basename "$d"
}
# Interface of the lowest-metric IPv4 default route.
mgmt_if() {
    ip -4 route show default | awk '{
        dev = ""; metric = 0
        for (i = 1; i < NF; i++) { if ($i == "dev") dev = $(i + 1); if ($i == "metric") metric = $(i + 1) }
        if (dev != "" && (best == "" || metric < best_metric)) { best = dev; best_metric = metric }
    } END { print best }'
}
# Writes $2 to file $1 unless it already holds exactly that; true if it wrote.
put() {
    [ -f "$1" ] && [ "$(cat "$1")" = "$2" ] && return 1
    mkdir -p "$(dirname "$1")" && printf '%s\n' "$2" >"$1"
}
# The repository this script is in, absolute: build steps cd elsewhere.
REPO=$(cd "$(dirname "$0")/.." && pwd)

# ---------------------------------------------------------------- build
# clang/lld 18, as in the Docker image (AL2023's default clang 15 rejects the shims' [[clang::musttail]]).
dpdk_env() {
    PKG_CONFIG_PATH=/opt/dpdk/lib/pkgconfig
    RUSTFLAGS="-C linker=clang -C link-arg=-fuse-ld=lld"
    LIBCLANG_PATH=/usr/lib64/llvm18/lib
    PATH=/usr/lib64/llvm18/bin:/root/.cargo/bin:$PATH
    export PKG_CONFIG_PATH RUSTFLAGS LIBCLANG_PATH PATH
}

step_packages() {
    dnf install -y -q gcc gcc-c++ make pkgconf-pkg-config clang18 clang18-libs lld18 python3-pip numactl-devel \
        libpcap-devel git tar xz patch cpio dnf-utils elfutils-libelf-devel ethtool iproute procps-ng \
        util-linux grubby >/dev/null \
        || fatal packages "dnf install failed"
    pip3 install -q meson ninja pyelftools || fatal packages "pip3 install meson ninja pyelftools failed"
    report packages ok "dnf toolchain, meson, ninja, pyelftools"
}

step_rust() {
    channel=$(sed -n 's/^channel = "\(.*\)"/\1/p' "$REPO/rust-toolchain.toml")
    if ! command -v rustup >/dev/null; then
        curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
            | sh -s -- -y -q --profile minimal --default-toolchain "$channel" >/dev/null \
            || fatal rust "rustup install failed"
    fi
    rustup toolchain install --profile minimal "$channel" >/dev/null 2>&1 \
        || fatal rust "rustup toolchain install $channel failed"
    report rust ok "$channel"
}

# The DPDK patch conduit/docker/Dockerfile applies: ENA read_clock (the device PHC) and the PHC read
# barrier.
DPDK_PATCH=$REPO/conduit/docker/dpdk-ena-read-clock.patch
# What a /opt/dpdk built by this script records in $DPDK_STAMP: the release tarball and the patch.
# pkg-config alone cannot tell a patched 25.11.3 from a stock one.
DPDK_STAMP=$R/opt/dpdk/share/zhida-dpdk.stamp
dpdk_stamp() { echo "dpdk-$DPDK_VERSION $DPDK_SHA256 patch $(sha256sum "$DPDK_PATCH" | cut -d' ' -f1)"; }

# The meson options of conduit/docker/Dockerfile; test-hft-host.sh checks they stay equal.
build_dpdk() {
    patch -s -p1 <"$DPDK_PATCH" && \
    CC=clang meson setup build . --buildtype=release --prefix=/opt/dpdk --libdir=lib \
        -Dplatform=generic -Denable_stdatomic=false -Dmbuf_refcnt_atomic=false \
        -Denable_iova_as_pa=true -Dtests=false -Ddisable_apps='*' \
        -Denable_drivers=net/ring,net/pcap,net/tap,net/memif,net/af_packet,net/ena,net/intel/ixgbe,net/intel/i40e,net/intel/ice \
        >/dev/null && ninja -C build >/dev/null && meson install -C build >/dev/null
}

step_dpdk() {
    want=$(dpdk_stamp)
    if [ "$(cat "$DPDK_STAMP" 2>/dev/null)" != "$want" ]; then
        src=$R/var/tmp/dpdk-$DPDK_VERSION
        rm -rf "$src"
        mkdir -p "$src" || fatal dpdk "cannot create $src"
        curl --fail --location --retry 2 -s -o "$src.tar.xz" "https://fast.dpdk.org/rel/dpdk-$DPDK_VERSION.tar.xz" \
            || fatal dpdk "download failed"
        echo "$DPDK_SHA256  $src.tar.xz" | sha256sum --check --quiet || fatal dpdk "sha256 mismatch"
        tar -xJf "$src.tar.xz" -C "$src" --strip-components=1 || fatal dpdk "unpack failed"
        # A stock or older install goes first, so nothing of it outlives the rebuild.
        rm -rf "$R/opt/dpdk"
        (cd "$src" && build_dpdk) || fatal dpdk "patch or meson/ninja build failed"
        echo /opt/dpdk/lib >/etc/ld.so.conf.d/dpdk.conf && ldconfig
        rm -rf "$src" "$src.tar.xz"
        echo "$want" >"$DPDK_STAMP" || fatal dpdk "cannot write $DPDK_STAMP"
    fi
    have=$(pkg-config --modversion libdpdk 2>/dev/null)
    [ "$have" = "$DPDK_VERSION" ] || fatal dpdk "pkg-config reports '${have}', want $DPDK_VERSION"
    report dpdk ok "$DPDK_VERSION with dpdk-ena-read-clock.patch in /opt/dpdk"
}

step_binaries() {
    (cd "$REPO" && cargo build -q --release --locked -p nic-runner -p zhida-netconf) \
        || fatal binaries "cargo build failed"
    install -m 0755 "$REPO/target/release/zhida-nic" "$REPO/target/release/zhida-netconf" /usr/local/bin/ \
        || fatal binaries "install failed"
    report binaries ok "/usr/local/bin/zhida-nic /usr/local/bin/zhida-netconf (release)"
}

build() {
    dpdk_env
    step_packages
    step_rust
    step_dpdk
    step_binaries
}

# ---------------------------------------------------------------- host settings
# Fixes MGMT_IF, DATA_IF, DATA_PCI, DATA_MAC and NUMA_NODE in host.env. A data ENI already on vfio-pci
# has no interface any more; its saved facts are kept.
step_interfaces() {
    MGMT_IF=$(mgmt_if)
    [ -n "$MGMT_IF" ] || fatal interfaces "no IPv4 default route: cannot tell the management interface"
    if [ -f "$ENV" ]; then
        # shellcheck source=/dev/null
        . "$ENV"
        if [ "$(pci_driver "$DATA_PCI")" = vfio-pci ]; then
            report interfaces ok "management $MGMT_IF; data $DATA_PCI ($DATA_MAC) already on vfio-pci"
            return 0
        fi
    fi
    candidates=
    for n in "$R"/sys/class/net/*; do
        name=${n##*/}
        drv=$(readlink "$n/device/driver" 2>/dev/null) || continue
        [ "$(basename "$drv")" = ena ] && [ "$name" != "$MGMT_IF" ] && candidates="$candidates $name"
    done
    if [ -n "$want_if" ]; then
        [ "$want_if" != "$MGMT_IF" ] || fatal interfaces "--interface $want_if carries the default route: it is the management interface"
        echo " $candidates " | grep -q " $want_if " || fatal interfaces "--interface $want_if is not a non-management ena interface (have:${candidates:- none})"
        DATA_IF=$want_if
    else
        # shellcheck disable=SC2086 # one word per interface name
        set -- $candidates
        [ $# -eq 1 ] || fatal interfaces "want exactly one ena interface besides management $MGMT_IF, have $#:${candidates}; attach one data ENI or pass --interface"
        DATA_IF=$1
    fi
    DATA_PCI=$(basename "$(readlink "$R/sys/class/net/$DATA_IF/device")")
    DATA_MAC=$(cat "$R/sys/class/net/$DATA_IF/address")
    NUMA_NODE=$(cat "$R/sys/class/net/$DATA_IF/device/numa_node" 2>/dev/null || echo -1)
    [ "$NUMA_NODE" -ge 0 ] 2>/dev/null || NUMA_NODE=0
    report interfaces ok "management $MGMT_IF; data $DATA_IF $DATA_PCI ($DATA_MAC) on NUMA node $NUMA_NODE"
}

step_cpus() {
    node_cpus=$(cat "$R/sys/devices/system/node/node$NUMA_NODE/cpulist" 2>/dev/null \
        || cat "$R/sys/devices/system/cpu/online")
    APP_CPU=$(expand "$node_cpus" | tail -n 1)
    [ "$APP_CPU" != 0 ] || fatal cpus "only CPU 0 on node $NUMA_NODE: nothing to isolate"
    ISOLATED=$(cat "$R/sys/devices/system/cpu/cpu$APP_CPU/topology/thread_siblings_list" 2>/dev/null || echo "$APP_CPU")
    HOUSEKEEPING=$(expand "$(cat "$R/sys/devices/system/cpu/online")" | while read -r c; do
        expand "$ISOLATED" | grep -qx "$c" || echo "$c"
    done | join)
    [ -n "$HOUSEKEEPING" ] || fatal cpus "no housekeeping CPU left"
    report cpus ok "app CPU $APP_CPU; isolated $ISOLATED; housekeeping $HOUSEKEEPING"
}

save_env() {
    put "$ENV" "MGMT_IF=$MGMT_IF
DATA_IF=$DATA_IF
DATA_PCI=$DATA_PCI
DATA_MAC=$DATA_MAC
NUMA_NODE=$NUMA_NODE
APP_CPU=$APP_CPU
ISOLATED=$ISOLATED
DATA_DRIVER=$DATA_DRIVER
HOUSEKEEPING=$HOUSEKEEPING" || true
}

step_config() {
    if [ "$(pci_driver "$DATA_PCI")" != vfio-pci ]; then
        zhida-netconf discover --interface "$DATA_IF" --aws -o "$DUT" || fatal config "zhida-netconf discover failed"
    fi
    zhida-netconf check "$DUT" >/dev/null || fatal config "zhida-netconf check $DUT failed"
    report config ok "$DUT"
}

cmdline_args() {
    args="isolcpus=managed_irq,domain,$ISOLATED nohz_full=$ISOLATED rcu_nocbs=$ISOLATED"
    args="$args irqaffinity=$HOUSEKEEPING transparent_hugepage=madvise hugepagesz=2M hugepages=$NUMA_NODE:$HUGEPAGES"
    [ "$(uname -m)" = x86_64 ] && args="$args clocksource=tsc"
    echo "$args"
}
has_args() { # "args" "words..."
    for arg in $2; do echo " $1 " | grep -q " $arg " || return 1; done
}

step_cmdline() {
    want=$(cmdline_args)
    if has_args "$(cat "$R/proc/cmdline")" "$want"; then
        report cmdline ok "$want"
        return 0
    fi
    if ! has_args "$(grubby --info=DEFAULT | sed -n 's/^args="\(.*\)"$/\1/p')" "$want"; then
        grubby --update-kernel=ALL --args="$want" || fatal cmdline "grubby failed"
    fi
    report cmdline changed "$want (takes effect at reboot)"
    reboot_needed="$reboot_needed cmdline"
}

# amzn-drivers at AMZN_COMMIT, unpacked once under /var/tmp, and the running kernel's headers.
amzn() {
    dnf install -y -q "kernel-devel-$(uname -r)" >/dev/null || return 1
    dir=/var/tmp/amzn-drivers-$AMZN_COMMIT
    if ! [ -d "$dir" ]; then
        curl --fail --location --retry 2 -s -o "$dir.tar.gz" "https://codeload.github.com/amzn/amzn-drivers/tar.gz/$AMZN_COMMIT" \
            && echo "$AMZN_SHA256  $dir.tar.gz" | sha256sum --check --quiet \
            && tar -xzf "$dir.tar.gz" -C /var/tmp && rm -f "$dir.tar.gz" || return 1
    fi
    echo "$dir"
}

vfio_dir() { echo "$R/lib/modules/$(uname -r)/kernel/drivers/vfio"; }
# Amazon's kernel6.18 source applies vfio-pci-enable-mmap-ing-ena-device.patch. Trust that vendor
# contract only for an unmodified module from the official package matching this kernel.
native_vfio_wc() {
    vfio_wc_error="modinfo cannot resolve vfio_pci_core for $(uname -r); check the installed module indexes"
    module=$(modinfo -n vfio_pci_core 2>/dev/null) || return 1
    vfio_wc_error="module file missing: $module"
    [ -f "$module" ] || return 1
    vfio_wc_error="$module is not owned by the official Amazon Linux kernel6.18 package for $(uname -r)"
    [ "$(rpm -qf --qf '%{NAME} %{VENDOR} %{VERSION}-%{RELEASE}.%{ARCH}\n' "$module" 2>/dev/null)" \
        = "kernel6.18 Amazon Linux $(uname -r)" ] || return 1
    vfio_wc_error="unsupported or unavailable RPM file digest algorithm for $module (requires SHA256)"
    [ "$(rpm -qf --qf '%{FILEDIGESTALGO}\n' "$module" 2>/dev/null)" = 8 ] || return 1
    vfio_wc_error="RPM SHA256 digest unavailable for $module"
    file_digests=$(rpm -qf --qf '[%{FILENAMES} %{FILEDIGESTS}\n]' "$module" 2>/dev/null) || return 1
    digest=$(printf '%s\n' "$file_digests" | awk -v module="$module" '$1 == module {print $2}')
    [ -n "$digest" ] || return 1
    vfio_wc_error="module SHA256 differs from its owning RPM: $module"
    actual_digest=$(sha256sum "$module") || return 1
    [ "${actual_digest%% *}" = "$digest" ]
}

# AWS's get-vfio-with-wc.sh keeps the stock module beside the patched one as *_no_wc.
vfio_wc_installed() {
    case "$(uname -r)" in
        6.18.*) native_vfio_wc ;;
        *) ls "$(vfio_dir)"/pci/vfio-pci-core.ko*_no_wc >/dev/null 2>&1 ;;
    esac
}

step_vfio_wc() {
    if ! vfio_wc_installed; then
        case "$(uname -r)" in
            6.18.*) fatal vfio_wc "requires the unmodified official AL2023 kernel6.18 module for $(uname -r): $vfio_wc_error" ;;
        esac
        dir=$(amzn) || fatal vfio_wc "kernel-devel-$(uname -r) or amzn-drivers $AMZN_COMMIT unavailable"
        (cd "$dir/userspace/dpdk/enav2-vfio-patch" && TERM=dumb bash get-vfio-with-wc.sh) >/var/log/zhida-vfio-wc.log 2>&1 \
            || fatal vfio_wc "get-vfio-with-wc.sh failed; see /var/log/zhida-vfio-wc.log"
        vfio_wc_installed || fatal vfio_wc "get-vfio-with-wc.sh left no patched vfio-pci-core in $(vfio_dir)"
    fi
    if [ -d "$R/sys/module/vfio_pci_core" ] && [ "$(cat "$R/sys/module/vfio_pci_core/srcversion")" != "$(modinfo -F srcversion vfio_pci_core)" ]; then
        report vfio_wc changed "write-combining vfio-pci installed; the loaded module goes at reboot"
        reboot_needed="$reboot_needed vfio"
        return 0
    fi
    report vfio_wc ok "write-combining vfio-pci installed for $(uname -r)"
}

step_unit() {
    install -D -m 0755 "$0" "$SELF" || fatal unit "cannot install $SELF"
    if put "$UNIT" "[Unit]
Description=Zhida host settings a reboot resets (deploy/hft-host.sh boot)
After=systemd-networkd.service network-online.target
Wants=network-online.target

[Service]
Type=oneshot
RemainAfterExit=yes
ExecStart=/usr/local/sbin/zhida-hft-host boot

[Install]
WantedBy=multi-user.target"; then
        systemctl daemon-reload
    fi
    systemctl enable -q zhida-host.service || fatal unit "systemctl enable failed"
    report unit ok "zhida-host.service runs boot at every boot"
}

# ---------------------------------------------------------------- boot (also the end of apply)
step_irqbalance() {
    systemctl disable --now -q irqbalance.service 2>/dev/null
    if pgrep -x irqbalance >/dev/null; then report irqbalance FAIL "still running"; else report irqbalance ok "off"; fi
}

step_irqs() {
    : >"$UNMOVABLE"
    for d in "$R"/proc/irq/[0-9]*; do
        echo "$HOUSEKEEPING" >"$d/smp_affinity_list" 2>/dev/null || echo "${d##*/}" >>"$UNMOVABLE"
    done
    check_irqs
}

# The data ENI must not be the management interface, and management must keep the default route.
guard_mgmt() { # step
    now=$(mgmt_if)
    [ -n "$now" ] || fatal "$1" "no IPv4 default route: refusing to touch interfaces"
    [ "$now" = "$MGMT_IF" ] || fatal "$1" "default route moved from $MGMT_IF to $now: refusing; rerun apply"
    [ "${DATA_IF:-}" != "$now" ] || fatal "$1" "data interface $DATA_IF carries the default route: refusing"
}

stage_unmanaged() {
    guard_mgmt unmanaged
    config="[Match]
MACAddress=$DATA_MAC

[Link]
Unmanaged=yes"
    if [ ! -f "$NETWORK" ] || [ "$(cat "$NETWORK")" != "$config" ]; then
        put "$NETWORK" "$config" || fatal unmanaged "cannot write $NETWORK"
        networkctl reload || fatal unmanaged "networkctl reload failed"
    fi
}

step_unmanaged() {
    stage_unmanaged
    if [ -e "$R/sys/class/net/$DATA_IF" ]; then
        ip addr flush dev "$DATA_IF" || fatal unmanaged "flushing $DATA_IF addresses failed"
        ip -4 route flush table all dev "$DATA_IF" || fatal unmanaged "flushing $DATA_IF routes failed"
    fi
    report unmanaged ok "$DATA_IF ($DATA_MAC) unmanaged by systemd-networkd, no addresses or routes"
}

step_bind() {
    guard_mgmt bind
    noiommu=
    [ -n "$(ls -A "$R/sys/kernel/iommu_groups" 2>/dev/null)" ] || noiommu=--noiommu
    zhida-netconf bind "$DUT" $noiommu || fatal bind "zhida-netconf bind $DUT $noiommu failed"
    report bind ok "$DATA_PCI on vfio-pci${noiommu:+ (no IOMMU)}"
}

boot() {
    load_env boot
    step_irqbalance
    step_irqs
    step_unmanaged
    [ "${DATA_DRIVER:-vfio-pci}" = ena ] || step_bind
}

apply() {
    build
    step_interfaces
    DATA_DRIVER=$requested_driver
    step_cpus
    save_env
    step_config
    step_cmdline
    [ "$DATA_DRIVER" = ena ] || step_vfio_wc
    step_unit
    if [ "$DATA_DRIVER" = ena ]; then
        policy_config="[Network]
ManageForeignRoutingPolicyRules=no"
        if [ ! -f "$NETWORKD_POLICY" ] || [ "$(cat "$NETWORKD_POLICY")" != "$policy_config" ]; then
            put "$NETWORKD_POLICY" "$policy_config" || fatal networkd_policy "cannot write $NETWORKD_POLICY"
        fi
    fi
    if [ -n "$reboot_needed" ]; then
        stage_unmanaged
        echo "reboot: rebooting now for:$reboot_needed; zhida-host.service finishes at boot, then run hft-host.sh verify"
        systemctl reboot
        exit 100
    fi
    if [ "$DATA_DRIVER" = ena ]; then
        stage_unmanaged
        systemctl restart systemd-networkd.service || fatal networkd_policy "cannot restart systemd-networkd"
    fi
    boot
}

load_env() { # step
    [ -f "$ENV" ] || fatal "$1" "$ENV missing: run hft-host.sh apply first"
    # shellcheck source=/dev/null
    . "$ENV"
}

# ---------------------------------------------------------------- verify
check_irqs() {
    bad='' fixed=''
    for d in "$R"/proc/irq/[0-9]*; do
        irq=${d##*/}
        eff=$(cat "$d/effective_affinity_list" 2>/dev/null || cat "$d/smp_affinity_list" 2>/dev/null)
        [ -n "$eff" ] || continue
        overlap "$eff" "$ISOLATED" || continue
        if grep -qx "$irq" "$UNMOVABLE" 2>/dev/null; then fixed="$fixed $irq"; else bad="$bad $irq($eff)"; fi
    done
    if [ -z "$bad" ]; then
        report irqs ok "movable IRQs on $HOUSEKEEPING${fixed:+; unmovable (per-CPU):$fixed}"
    else
        report irqs FAIL "on isolated CPUs:$bad${fixed:+; unmovable:$fixed}"
    fi
}

verify() {
    load_env verify
    want=$(cmdline_args)
    if has_args "$(cat "$R/proc/cmdline")" "$want"; then
        report cmdline ok "$want"
    else
        report cmdline FAIL "/proc/cmdline lacks some of: $want"
    fi

    now=$(mgmt_if)
    drv=$(readlink "$R/sys/class/net/$MGMT_IF/device/driver" 2>/dev/null)
    if [ "$now" = "$MGMT_IF" ] && [ -n "$drv" ] && [ "$(basename "$drv")" != vfio-pci ]; then
        report management ok "$MGMT_IF on $(basename "$drv") with the default route"
    else
        report management FAIL "$MGMT_IF: driver '${drv:+$(basename "$drv")}', default route on '${now}'"
    fi

    drv=$(pci_driver "$DATA_PCI")
    want_driver=${DATA_DRIVER:-vfio-pci}
    if [ "$drv" = "$want_driver" ]; then
        report data ok "$DATA_PCI ($DATA_MAC) on $want_driver"
    else
        report data FAIL "$DATA_PCI on '${drv}', want $want_driver"
    fi

    iso=$(cat "$R/sys/devices/system/cpu/isolated" 2>/dev/null)
    nohz=$(cat "$R/sys/devices/system/cpu/nohz_full" 2>/dev/null)
    if [ "$(expand "$iso" | join)" = "$(expand "$ISOLATED" | join)" ] && [ "$(expand "$nohz" | join)" = "$(expand "$ISOLATED" | join)" ] \
        && expand "$ISOLATED" | grep -qx "$APP_CPU"; then
        report cpus ok "app CPU $APP_CPU; isolated and nohz_full $ISOLATED"
    else
        report cpus FAIL "isolated '$iso', nohz_full '$nohz', want $ISOLATED (app CPU $APP_CPU)"
    fi

    node=$R/sys/devices/system/node/node$NUMA_NODE/hugepages/hugepages-2048kB
    total=$(cat "$node/nr_hugepages" 2>/dev/null || echo 0)
    free=$(cat "$node/free_hugepages" 2>/dev/null || echo 0)
    if [ "$total" -ge "$HUGEPAGES" ] && mountpoint -q "$R/dev/hugepages"; then
        report hugepages ok "node $NUMA_NODE: $total x 2 MiB, $free free; hugetlbfs on /dev/hugepages"
    else
        report hugepages FAIL "node $NUMA_NODE: $total x 2 MiB (want $HUGEPAGES), hugetlbfs $(mountpoint -q "$R/dev/hugepages" && echo mounted || echo 'not mounted')"
    fi

    # zhida-nic's own line from the last run: "zhida-nic: hugepages: hugepages-2048kB used=N free=M".
    used=$(sed -n 's/^zhida-nic: hugepages: hugepages-2048kB used=\([0-9]*\) .*/\1/p' "$LOG" 2>/dev/null | tail -n 1)
    if [ -z "$used" ]; then
        report pools ok "no run logged yet ($LOG); the first run-binance.sh reports its use"
    elif [ "$used" -le "$HUGEPAGES" ]; then
        report pools ok "last run took $used of $HUGEPAGES 2 MiB pages"
    else
        report pools FAIL "last run took $used 2 MiB pages, more than the $HUGEPAGES reserved"
    fi

    if pgrep -x irqbalance >/dev/null; then report irqbalance FAIL "running"; else report irqbalance ok "off"; fi
    check_irqs

    if [ "${DATA_DRIVER:-vfio-pci}" = ena ]; then
        report vfio_wc ok "not required for kernel receiver"
    elif ! vfio_wc_installed; then
        report vfio_wc FAIL "no write-combining vfio-pci for $(uname -r): rerun apply${vfio_wc_error:+; $vfio_wc_error}"
    elif [ -n "$(modinfo -F srcversion vfio_pci_core)" ] \
        && [ "$(cat "$R/sys/module/vfio_pci_core/srcversion" 2>/dev/null)" = "$(modinfo -F srcversion vfio_pci_core)" ]; then
        case "$(uname -r)" in
            6.18.*) report vfio_wc ok "Amazon Linux ENA write-combining vfio-pci-core loaded" ;;
            *) report vfio_wc ok "patched vfio-pci-core loaded" ;;
        esac
    else
        case "$(uname -r)" in
            6.18.*) report vfio_wc FAIL "loaded vfio-pci-core differs from the verified on-disk module" ;;
            *) report vfio_wc FAIL "loaded vfio-pci-core is not the patched one" ;;
        esac
    fi

    # x86_64: the TSC must be the clocksource now and never have been found unstable since boot.
    # Only the last "Switched to clocksource" counts: boot passes through tsc-early (and on EC2
    # kvm-clock) before tsc.
    cs=$(cat "$R/sys/devices/system/clocksource/clocksource0/current_clocksource" 2>/dev/null)
    if [ "$(uname -m)" != x86_64 ]; then
        report clocksource ok "${cs:-unknown} ($(uname -m))"
    elif [ "$cs" != tsc ]; then
        report clocksource FAIL "$cs, want tsc on x86_64"
    elif ! kmsg=$(dmesg 2>/dev/null); then
        report clocksource FAIL "cannot read dmesg (run as root)"
    elif bad=$(printf '%s\n' "$kmsg" | grep -m 1 -e 'clocksource: timekeeping watchdog' -e 'TSC unstable' \
        -e "Marking clocksource 'tsc' as unstable"); then
        report clocksource FAIL "$bad"
    elif last=$(printf '%s\n' "$kmsg" | grep 'Switched to clocksource' | tail -n 1) \
        && [ -n "$last" ] && [ "${last##*Switched to clocksource }" != tsc ]; then
        report clocksource FAIL "$last"
    else
        report clocksource ok "tsc ($(uname -m)); dmesg shows no TSC instability"
    fi

    if systemctl is-enabled -q zhida-host.service 2>/dev/null; then
        report unit ok "zhida-host.service enabled"
    else
        report unit FAIL "zhida-host.service not enabled"
    fi

    if zhida-netconf check "$DUT" >/dev/null 2>&1; then report config ok "$DUT"; else report config FAIL "zhida-netconf check $DUT"; fi
    exit $failed
}

case "$mode" in
    build) build ;;
    apply) apply ;;
    boot) boot ;;
    verify) verify ;;
esac
exit $failed
