#!/bin/bash
# Runs the Binance listener (switch-nic feed) on the host deploy/hft-host.sh set up: pinned to the
# isolated app CPU, mimalloc with the HugeTLB pool left for DPDK, output appended to /var/log/switch/binance.log
# (hft-host.sh verify reads switch-nic's hugepage line from it). Refuses to start unless verify passes.
# The listener asks for ENA hardware RX timestamps: it runs with /etc/switch/binance.json, the host's
# dut.json with rx_timestamps true, and refuses to start unless switch-netconf reads that back. On an
# instance without the timestamps (deploy/README.md) Port open then fails.
# Usage: sudo deploy/run-binance.sh [switch-nic feed options, e.g. --seconds 3600 --print 0]
set -u -o pipefail
here=$(cd "$(dirname "$0")" && pwd)
R=${HFT_ROOT:-}
log=$R/var/log/switch/binance.log
dut=$R/etc/switch/dut.json
config=$R/etc/switch/binance.json
if ! "$here/hft-host.sh" verify; then
    echo "run-binance: hft-host.sh verify failed; fix the FAIL lines (sudo deploy/hft-host.sh apply)" >&2
    exit 1
fi
# The feed compares CLOCK_REALTIME with the NIC's PHC stamps: chrony must have the PHC selected.
if ! sources=$(chronyc sources 2>&1) || ! grep -q '^#\* *PHC' <<<"$sources"; then
    echo "run-binance: chrony does not have the PHC selected (chronyc sources); launch the instance in a precision-time placement group; not starting" >&2
    exit 1
fi
# switch-netconf writes pretty JSON, one field per line (its own test pins the format).
sed 's/^  "rx_timestamps": false,$/  "rx_timestamps": true,/' "$dut" >"$config" || exit 1
# Read the whole summary first: grep -q would close the pipe while check still writes.
if ! summary=$(switch-netconf check "$config") || ! grep -qx 'rx_timestamps: true' <<<"$summary"; then
    echo "run-binance: $config does not have rx_timestamps true (switch-netconf check); not starting" >&2
    exit 1
fi
# shellcheck source=/dev/null
. "$R/etc/switch/host.env"
mkdir -p "$(dirname "$log")"
echo "run-binance: $(date -u +%FT%TZ) switch-nic feed $* on CPU $APP_CPU with $config" | tee -a "$log"
MIMALLOC_ALLOW_LARGE_OS_PAGES=0 taskset -c "$APP_CPU" \
    switch-nic --config "$config" feed "$@" 2>&1 | tee -a "$log"
