# Reaching the internet through a tap

From the repository root, open a shell in the image with nftables:

```sh
docker run --rm -it --init --cpuset-cpus=0 --cap-add NET_ADMIN \
  --device /dev/net/tun --sysctl net.ipv4.ip_forward=1 \
  -v "$PWD:/work" -w /work \
  -v switch-cargo-registry:/usr/local/cargo/registry \
  -v switch-linux-target:/work/target switch-dpdk-test:binance bash
```

This gives the container permission to create a tap and forward its packets.

```sh
python3 -c 'import json; c=json.load(open("examples/tap.json")); c["gateway"]="192.0.2.1"; print(json.dumps(c))' > /tmp/wss-tap.json
```

This gives the program a route through the kernel side of the tap.

```sh
cargo build --locked -p switch-examples --features ws --example wss_client
taskset -c 0 target/debug/examples/wss_client /tmp/wss-tap.json &
client=$!
until ip link show cdnic0 >/dev/null 2>&1; do kill -0 "$client" || exit 1; sleep 0.1; done
ip addr add 192.0.2.1/24 dev cdnic0
ip link set cdnic0 up
```

This starts the client and assigns its gateway address after it creates the tap.

```sh
uplink=$(ip -4 route show default | awk '{ for (i=1; i<NF; i++) if ($i=="dev") { print $(i+1); exit } }')
nft add table ip switch_example
nft 'add chain ip switch_example postrouting { type nat hook postrouting priority srcnat; }'
nft "add rule ip switch_example postrouting ip saddr 192.0.2.2 oifname \"$uplink\" masquerade"
```

This translates outgoing packets to the container's address.

```sh
tc qdisc add dev "$uplink" clsact
tc filter add dev "$uplink" ingress pref 49152 protocol ip matchall action csum tcp
wait "$client"
```

This recomputes incoming TCP checksums before forwarding, which Docker Desktop needs,
and waits for the client. A successful run prints the card report, three book-ticker
JSON messages and `WebSocket closed`.
