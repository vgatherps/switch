#!/bin/bash
# Runs the Binance listener (zhida-nic feed) on the host deploy/hft-host.sh set up: pinned to the
# isolated app CPU, mimalloc with the HugeTLB pool left for DPDK, output appended to /var/log/zhida/binance.log
# (hft-host.sh verify reads zhida-nic's hugepage line from it). Refuses to start unless verify passes.
# The listener asks for ENA hardware RX timestamps: it runs with /etc/zhida/binance.json, the host's
# dut.json with rx_timestamps true, and refuses to start unless zhida-netconf reads that back. On an
# instance without the timestamps (deploy/README.md) Port open then fails.
# Usage: sudo deploy/run-binance.sh [zhida-nic feed options, e.g. --seconds 3600 --print 0]
set -u -o pipefail
here=$(cd "$(dirname "$0")" && pwd)
R=${HFT_ROOT:-}
log=$R/var/log/zhida/binance.log
dut=$R/etc/zhida/dut.json
config=$R/etc/zhida/binance.json
if ! "$here/hft-host.sh" verify; then
    echo "run-binance: hft-host.sh verify failed; fix the FAIL lines (sudo deploy/hft-host.sh apply)" >&2
    exit 1
fi
# The management ENI's real Time Sync PHC is required; never substitute a wall clock.
if [ ! -c "$R/dev/ptp_ena" ]; then
    echo "run-binance: /dev/ptp_ena is not a PHC character device; enable ena.phc_enable=1 and reboot; not starting" >&2
    exit 1
fi
# zhida-netconf writes pretty JSON, one field per line (its own test pins the format).
sed 's/^  "rx_timestamps": false,$/  "rx_timestamps": true,/' "$dut" >"$config" || exit 1
if ! zhida-netconf check "$config" | grep -qx 'rx_timestamps: true'; then
    echo "run-binance: $config does not have rx_timestamps true (zhida-netconf check); not starting" >&2
    exit 1
fi
# shellcheck source=/dev/null
. "$R/etc/zhida/host.env"
mkdir -p "$(dirname "$log")"
echo "run-binance: $(date -u +%FT%TZ) zhida-nic feed $* on CPU $APP_CPU with $config" | tee -a "$log"
MIMALLOC_ALLOW_LARGE_OS_PAGES=0 taskset -c "$APP_CPU" \
    zhida-nic --config "$config" feed "$@" --phc /dev/ptp_ena 2>&1 | tee -a "$log"
