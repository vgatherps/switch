#!/bin/sh
# Local validation of the bench tooling (not a measurement): inside the zhida-dpdk-test container
# (NET_ADMIN, /dev/net/tun, net.ipv4.ip_forward=1, at least 2 + GENS CPUs), from /work/conduit.
# Each Zhida process has its own net_tap port on its own subnet, with the kernel side as its gateway;
# the kernel routes between them (no bridge: net_tap gives the kernel side of a tap the Zhida MAC,
# which a bridge would take as its own). Zhida DUT 10.10.0.2 (gateway 10.10.0.1), generator i
# 10.(20+i).0.2.
# Per protocol (PROTOS): a sweep against the Zhida DUT. Every process's stderr goes to OUT_DIR/*.log.
# Last, the bounded-overload check: a TCP sweep against a listener that never reads must end on
# time with its loss reported and a complete summary.
# Usage: [GENS=2] [SENDERS=2*GENS] [PROTOS="udp tcp ws"] smoke.sh OUT_DIR [extra sweep flags...]
set -eu
out=$1
shift
gens=${GENS:-2}
protos=${PROTOS:-udp tcp ws}
senders=${SENDERS:-$((2 * gens))}
bench=${BENCH:-../target/release/zhida-bench}
cpus=$(taskset -pc $$ | sed 's/.*: //')
case "$cpus" in *-*) first=${cpus%-*}; last=${cpus#*-} ;; *) echo "need a CPU range, got $cpus" >&2; exit 2 ;; esac
[ $((last - first + 1)) -ge $((gens + 2)) ] || { echo "need $((gens + 2)) CPUs, have $cpus" >&2; exit 2; }
[ "$(cat /proc/sys/net/ipv4/ip_forward)" = 1 ] || { echo "needs net.ipv4.ip_forward=1" >&2; exit 2; }
mkdir -p "$out"
dut_addr=10.10.0.2
kernel_addr=10.10.0.1

# Zhida node NAME with net_tap iface cdNAME on 10.NET.0.0/24: Zhida .2, the kernel side .1 (its gateway).
node() { # name net
    cat >"$out/$1.json" <<EOF
{"version": 1, "device": {"vdev": "net_tap_$1,iface=cd$1"}, "mac": null, "ipv4": "10.$2.0.2/24",
 "gateway": "10.$2.0.1", "mtu": 1500, "paths": [], "offloads": "auto", "rx_timestamps": false,
 "descriptors": 512, "eal": {"file_prefix": "$1", "extra": ["--no-huge", "-m", "512", "--no-shconf"]}}
EOF
    cat >"$out/up-$1.sh" <<EOF
#!/bin/sh
# Gives cd$1 its kernel address once the Zhida process has created it.
until ip link show cd$1 >/dev/null 2>&1; do sleep 0.02; done
ip address add 10.$2.0.1/24 dev cd$1
ip link set cd$1 up
EOF
}
node dut 10
i=0
while [ $i -lt "$gens" ]; do
    node gen$i $((20 + i))
    i=$((i + 1))
done
gen_cpu=$((first + 2))
cat >"$out/gen.sh" <<EOF
#!/bin/sh
# gen.sh I PROTO TARGET SENDERS SIZE: generator worker I on CPU $gen_cpu + I, stderr to gen\$1.log
exec 2>>$out/gen\$1.log
echo "== gen \$1 \$2 -> \$3 \$(date +%T.%N)" >&2
sh $out/up-gen\$1.sh &
exec taskset -c \$(($gen_cpu + \$1)) $bench gen --config $out/gen\$1.json --proto \$2 --target \$3 \
    --senders \$4 --size \$5 --local-port 40000
EOF

dut=
sink=
trap 'kill $dut $sink 2>/dev/null; wait' EXIT
start_dut() { # proto serve-flags...
    proto=$1
    shift
    sh "$out/up-dut.sh" &
    echo "== serve $proto $* $(date +%T.%N)" >>"$out/dut.log"
    taskset -c $((first + 1)) "$bench" serve --config "$out/dut.json" --proto "$proto" --port 7000 "$@" \
        2>>"$out/dut.log" &
    dut=$!
    sleep 3
}
stop_dut() {
    kill -TERM $dut
    wait $dut
    dut=
}
run_sweep() { # proto addr out-file [flags...]: the sweep's exit status; its stderr to sweep.log
    proto=$1 addr=$2 file=$3
    shift 3
    echo "== sweep $proto -> $addr $(date +%T.%N)" >>"$out/sweep.log"
    taskset -c "$first" "$bench" sweep --proto "$proto" --gens "$gens" --senders "$senders" --size 64 \
        --gen-cmd "sh $out/gen.sh {i} $proto $addr {senders} 64" "$@" >"$file" 2>>"$out/sweep.log"
}

for proto in $protos; do
    start_dut "$proto"
    budget=
    [ "$proto" = udp ] || budget="--budget-us 50"
    # shellcheck disable=SC2086 # budget is zero or two words
    # a sweep stopped at a generator-bound step exits non-zero with a complete summary: expected
    # over net_tap, where the generators hit the tap syscall early
    if ! run_sweep "$proto" $dut_addr:7000 "$out/$proto-zhida.jsonl" $budget "$@" &&
        ! tail -1 "$out/$proto-zhida.jsonl" | grep -q '"complete":true,"generator_bound":true'; then
        echo "smoke: $proto FAILED: $(tail -1 "$out/sweep.log")" >&2
        exit 1
    fi
    echo "smoke: $proto: $(tail -1 "$out/$proto-zhida.jsonl")"
    stop_dut
done

# Bounded overload: a listener that accepts and never reads (tiny receive buffer). The generators'
# connections fill; the step must still end on time with every measured message lost (most never
# sent), and the sweep must complete with its summary instead of hanging or aborting. The DUT runs
# only so that its tap gives the kernel the 10.10.0.1 address.
start_dut tcp
python3 -c '
import socket
s = socket.socket()
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
s.bind(("0.0.0.0", 7003))
s.listen(64)
held = []
while True:
    held.append(s.accept()[0])
' &
sink=$!
sleep 0.5
started=$(date +%s)
run_sweep tcp $kernel_addr:7003 "$out/overload-check.jsonl" --rate-min 64000 --rate-max 64000 \
    --step-ms 300 --warmup-ms 100 --drain-ms 100 ||
    { echo "smoke: bounded overload: the sweep against a sink failed: $(tail -1 "$out/sweep.log")" >&2; exit 1; }
took=$(($(date +%s) - started))
python3 - "$out/overload-check.jsonl" "$took" <<'PY' || exit 1
import json, sys
lines = [json.loads(l) for l in open(sys.argv[1])]
step, summary = lines[0], lines[-1]
ok = (step["kind"] == "step" and step["drop_fraction"] == 1.0 and step["unsent"] > 0
      and summary["kind"] == "summary" and summary["complete"] and int(sys.argv[2]) < 30)
print(f"smoke: bounded overload: {sys.argv[2]}s, drop {step['drop_fraction']}, unsent {step['unsent']}"
      f" of {step['measured']}, summary complete={summary['complete']}")
sys.exit(0 if ok else 1)
PY
stop_dut
trap - EXIT
kill $sink 2>/dev/null || true
