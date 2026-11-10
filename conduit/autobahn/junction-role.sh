#!/bin/sh
# Run inside the role container; junctiond alone owns the tap and Stack.
set -u
mode=$1 network=$2 app=$3
shift 3
ipc=$(mktemp -d /tmp/autobahn-junction.XXXXXX) || exit 1
daemon= child=
say() { echo "autobahn-junction: $*" >&2; }
cleanup() {
    rc=$?
    trap - EXIT INT TERM HUP
    if [ -n "$child" ]; then
        kill "$child" 2>/dev/null || :
        wait "$child" || :
    fi
    if [ -n "$daemon" ]; then
        kill -TERM "$daemon" 2>/dev/null || rc=1
        wait "$daemon"; host=$?
        say "junctiond exit $host"
        [ "$host" = 0 ] || rc=1
    fi
    rm -rf "$ipc" || rc=1
    exit "$rc"
}
trap cleanup EXIT
trap 'exit 130' INT TERM HUP

taskset -c "$network" ../target/release/junctiond --config autobahn/junction-netconf.json \
    --dir "$ipc" --max-sessions 1 --max-sockets 1024 --tcp-sockets 1024 --udp-sockets 0 \
    --control-cpus "$app" &
daemon=$!
deadline=$(($(date +%s) + 60))
until [ -S "$ipc/control.sock" ]; do
    kill -0 "$daemon" 2>/dev/null || { say "junctiond exited before control socket"; exit 1; }
    [ "$(date +%s)" -lt "$deadline" ] || { say "no control socket within 60 s"; exit 1; }
    sleep 0.01
done
taskset -c "$app" ../target/release/relay-autobahn "$mode" "$@" \
    --backend junction --junction-socket "$ipc/control.sock" \
    -- autobahn/zhida-tester.sh "$mode" &
child=$!
wait "$child"; status=$?
child=
say "relay-autobahn exit $status"
exit "$status"
