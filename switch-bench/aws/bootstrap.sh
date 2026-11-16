#!/bin/sh
# Builds release switch-bench and switch-junctiond into /usr/local/bin, after `deploy/hft-host.sh apply`
# built the toolchain and DPDK. Run as root: SRC=/opt/switch-src sh bootstrap.sh
set -eu
cd "${SRC:?}"
# deploy/hft-host.sh dpdk_env, verbatim
PKG_CONFIG_PATH=/opt/dpdk/lib/pkgconfig
RUSTFLAGS="-C linker=clang -C link-arg=-fuse-ld=lld"
LIBCLANG_PATH=/usr/lib64/llvm18/lib
PATH=/usr/lib64/llvm18/bin:/root/.cargo/bin:$PATH
export PKG_CONFIG_PATH RUSTFLAGS LIBCLANG_PATH PATH
cargo build -q --release --locked -p switch-bench -p switch-junctiond
install -m 0755 target/release/switch-bench target/release/switch-junctiond /usr/local/bin/
