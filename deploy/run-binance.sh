#!/bin/bash
# Runs the Binance listener (zhida-nic feed) on the host deploy/hft-host.sh set up: pinned to the
# isolated app CPU, mimalloc on large pages, output also appended to /var/log/zhida/binance.log
# (hft-host.sh verify reads zhida-nic's hugepage line from it). Refuses to start unless verify passes.
# Usage: sudo deploy/run-binance.sh [zhida-nic feed options, e.g. --seconds 3600 --print 0]
set -u -o pipefail
here=$(cd "$(dirname "$0")" && pwd)
R=${HFT_ROOT:-}
log=$R/var/log/zhida/binance.log
if ! "$here/hft-host.sh" verify; then
    echo "run-binance: hft-host.sh verify failed; fix the FAIL lines (sudo deploy/hft-host.sh apply)" >&2
    exit 1
fi
# shellcheck source=/dev/null
. "$R/etc/zhida/host.env"
mkdir -p "$(dirname "$log")"
echo "run-binance: $(date -u +%FT%TZ) zhida-nic feed $* on CPU $APP_CPU" | tee -a "$log"
MIMALLOC_ALLOW_LARGE_OS_PAGES=1 taskset -c "$APP_CPU" \
    zhida-nic --config "$R/etc/zhida/dut.json" feed "$@" 2>&1 | tee -a "$log"
