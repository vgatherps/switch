#!/bin/sh
# Local validation of the bench tooling (not a measurement): inside the zhida-dpdk-test container
# (NET_ADMIN, /dev/net/tun, net.ipv4.ip_forward=1, at least 2 + GENS CPUs), from /work/conduit.
# Each Zhida process has its own net_tap port on its own subnet, with the kernel side as its gateway;
# the kernel routes between them (no bridge: net_tap gives the kernel side of a tap the Zhida MAC,
# which a bridge would take as its own). Zhida DUT 10.10.0.2 (gateway 10.10.0.1, where the kernel
# echo also listens), ping-pong 10.9.0.2, generator i 10.(20+i).0.2.
# Per protocol (PROTOS, default "udp tcp"): a sweep against the Zhida DUT and one against the kernel
# echo (udp and tcp on port 7001, ws on 7002); for TCP also ping-pong against both. Then the
# counter-capture check: one rate (CHECK_RATE) of UDP against the DUT with capture on and off.
# Every process's stderr goes to OUT_DIR/*.log. A failed sweep sends SIGUSR1 to the DUT and kernel
# echo processes (each prints its per-connection state to its log), saves `ss -tnoi`, and stops.
# Last, the stall watchdog check: a TCP sweep against a listener that never reads must fail fast
# with the generator's stall dump.
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
node pp 9
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
kerns=
trap 'kill $dut $kerns 2>/dev/null; wait' EXIT
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
sweep() { # proto target-name addr out-file [flags...]
    proto=$1 name=$2 addr=$3 file=$4
    shift 4
    if ! run_sweep "$proto" "$addr" "$file" "$@"; then
        echo "smoke: $proto -> $name FAILED: $(tail -1 "$out/sweep.log")" >&2
        kill -USR1 $dut $kerns 2>/dev/null || true
        ss -tnoi >"$out/ss.txt" 2>&1 || true
        sleep 1
        exit 1
    fi
    echo "smoke: $proto -> $name: $(tail -1 "$file")"
}

kernel_port() { if [ "$1" = ws ]; then echo 7002; else echo 7001; fi; }
for proto in udp tcp ws; do
    taskset -c "$first" "$bench" kernel-serve --proto $proto --bind 0.0.0.0:"$(kernel_port $proto)" \
        2>>"$out/kernel-$proto.log" &
    kerns="$kerns $!"
done

for proto in $protos; do
    start_dut "$proto" --counter-log "$out/dut-counters-$proto.jsonl"
    sweep "$proto" zhida $dut_addr:7000 "$out/$proto-zhida.jsonl" "$@"
    sweep "$proto" kernel $kernel_addr:"$(kernel_port "$proto")" "$out/$proto-kernel.jsonl" "$@"
    if [ "$proto" = tcp ]; then
        for target in zhida:$dut_addr:7000 kernel:$kernel_addr:7001; do
            name=${target%%:*}
            sh "$out/up-pp.sh" &
            taskset -c "$gen_cpu" "$bench" pingpong --config "$out/pp.json" --target "${target#*:}" \
                --size 64 --count "${PINGPONG_COUNT:-20000}" >"$out/pingpong-$name.jsonl"
            echo "smoke: tcp ping-pong -> $name: $(cut -c1-160 "$out/pingpong-$name.jsonl")"
        done
    fi
    stop_dut
done

# The DUT's in-loop counter capture must not change latency: one rate, capture on and off.
rate=${CHECK_RATE:-32000}
for mode in on off; do
    start_dut udp --counters $mode
    sweep udp zhida $dut_addr:7000 "$out/counters-$mode.jsonl" --rate-min "$rate" --rate-max "$rate" \
        --step-ms 2000 --warmup-ms 200
    stop_dut
done

# Stall watchdog: a listener that accepts and never reads (tiny receive buffer); the generators'
# connections fill, the step cannot finish, and each worker must fail with its stall dump well
# before the coordinator's own reply timeout. The DUT runs only so that its tap gives the kernel
# the 10.10.0.1 address.
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
kerns="$kerns $!"
sleep 0.5
started=$(date +%s)
if run_sweep tcp $kernel_addr:7003 "$out/stall-check.jsonl" --rate-min 64000 --rate-max 64000 \
    --step-ms 300 --warmup-ms 100 --drain-ms 100; then
    echo "smoke: stall watchdog: the sweep against a sink succeeded" >&2
    exit 1
fi
took=$(($(date +%s) - started))
grep -q '"kind":"stall"' "$out"/gen*.log || { echo "smoke: stall watchdog: no stall dump" >&2; exit 1; }
grep -q 'stalled: not done' "$out"/gen*.log || { echo "smoke: stall watchdog: no stall error" >&2; exit 1; }
echo "smoke: stall watchdog: sweep failed in ${took}s with the generator's stall dump"
stop_dut
trap - EXIT
kill $kerns 2>/dev/null || true
