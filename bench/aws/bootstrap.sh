#!/bin/sh
# Builds what a bench host needs, natively, on Amazon Linux 2023 (run as root by the runner from the
# pushed `git archive HEAD` at $SRC): `deploy/hft-host.sh build` (toolchain, rustup, DPDK under
# /opt/dpdk, zhida-nic, zhida-netconf), then release zhida-bench and zhida-netconf into /usr/local/bin.
# Two S3 objects, keyed by the versions that determine them (the runner names them):
#   DPDK_KEY   the /opt/dpdk install (DPDK version, arch, AL2023 VERSION_ID); restored before
#              hft-host.sh build, which then finds pkg-config reporting the pinned version and skips it
#   BENCH_KEY  zhida-bench + zhida-netconf (git tree hash, arch, DPDK version, Rust toolchain)
# A miss builds and uploads; a failed upload only costs the next run a rebuild, so it warns.
# Usage: SRC=/opt/zhida-src DPDK_KEY=s3://... BENCH_KEY=s3://... sh bootstrap.sh
set -eu
: "${SRC:?}" "${DPDK_KEY:?}" "${BENCH_KEY:?}"
say() { echo "bootstrap: $*"; }
[ -f "$SRC/deploy/hft-host.sh" ] || { say "$SRC/deploy/hft-host.sh missing"; exit 1; }

# restore KEY DIR: unpacks the cached tarball into DIR; false on a miss.
restore() {
    aws s3 cp --quiet "$1" /tmp/zhida-cache.tar.gz 2>/dev/null || return 1
    tar -xzf /tmp/zhida-cache.tar.gz -C "$2"
    rm -f /tmp/zhida-cache.tar.gz
}
# upload KEY DIR FILE...: tars FILEs (relative to DIR) to KEY.
upload() {
    key=$1 dir=$2
    shift 2
    tar -czf /tmp/zhida-upload.tar.gz -C "$dir" "$@"
    if aws s3 cp --quiet /tmp/zhida-upload.tar.gz "$key"; then say "cached $key"; else say "WARNING: upload of $key failed"; fi
    rm -f /tmp/zhida-upload.tar.gz
}

dpdk_cached=
if restore "$DPDK_KEY" /opt; then
    echo /opt/dpdk/lib >/etc/ld.so.conf.d/dpdk.conf
    ldconfig
    dpdk_cached=1
    say "DPDK from $DPDK_KEY"
fi
sh "$SRC/deploy/hft-host.sh" build
[ -n "$dpdk_cached" ] || upload "$DPDK_KEY" /opt dpdk

if restore "$BENCH_KEY" /usr/local/bin; then
    say "zhida-bench, zhida-netconf from $BENCH_KEY"
else
    cd "$SRC"
    # deploy/hft-host.sh dpdk_env, verbatim
    PKG_CONFIG_PATH=/opt/dpdk/lib/pkgconfig
    RUSTFLAGS="-C linker=clang -C link-arg=-fuse-ld=lld"
    LIBCLANG_PATH=/usr/lib64/llvm18/lib
    PATH=/usr/lib64/llvm18/bin:/root/.cargo/bin:$PATH
    export PKG_CONFIG_PATH RUSTFLAGS LIBCLANG_PATH PATH
    cargo build -q --release --locked -p zhida-bench -p zhida-netconf
    install -m 0755 target/release/zhida-bench target/release/zhida-netconf /usr/local/bin/
    upload "$BENCH_KEY" target/release zhida-bench zhida-netconf
fi
say "done"
