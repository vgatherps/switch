# Examples

These are the programs the guide in the `switch` crate documentation walks through (`make docs`). The comment at the top of each file gives the command that runs it and the peer it talks to. `first_task`, `basic`, `tasks`, `manual_loop`, `oneshot`, `remote_wake` and `clock_loop` need only cargo:

```sh
cargo run -p switch-examples --example first_task
```

The others open a network card and need Linux with DPDK, as the guide chapter Setting up for network programs describes. They are built with `--features net`, or `--features ws` for `tls_client`, `ws_echo_server` and `wss_client`.

## Running wss_client in a container

`wss_client` connects to a server on the internet, but on the tap device the examples use, its packets reach only the container, so the container has to forward them. Forwarding takes `nftables`, which is in a second image built on the first.

1. From the repository root, build both images:

   ```sh
   make -C conduit docker-image
   make -C conduit docker-binance-image
   ```

2. Open a shell in the second image, `switch-dpdk-test:binance`. Compared with the guide's container, `--sysctl net.ipv4.ip_forward=1` lets it forward packets:

   ```sh
   docker run --rm -it --init --cpuset-cpus=0 --cap-add NET_ADMIN \
     --device /dev/net/tun --sysctl net.ipv4.ip_forward=1 \
     -v "$PWD:/work" -w /work \
     -v switch-cargo-registry:/usr/local/cargo/registry \
     -v switch-linux-target:/work/target switch-dpdk-test:binance bash
   ```

3. Write a config file that names the Linux side of the tap device as the gateway:

   ```sh
   python3 -c 'import json; c=json.load(open("examples/tap.json")); c["gateway"]="192.0.2.1"; print(json.dumps(c))' > /tmp/wss-tap.json
   ```

4. Start the client, and give the Linux side its address once the client has created the tap device:

   ```sh
   cargo build --locked -p switch-examples --features ws --example wss_client
   taskset -c 0 target/debug/examples/wss_client /tmp/wss-tap.json &
   client=$!
   until ip link show cdnic0 >/dev/null 2>&1; do kill -0 "$client" || exit 1; sleep 0.1; done
   ip addr add 192.0.2.1/24 dev cdnic0
   ip link set cdnic0 up
   ```

5. Send the client's packets out under the container's own address:

   ```sh
   uplink=$(ip -4 route show default | awk '{ for (i=1; i<NF; i++) if ($i=="dev") { print $(i+1); exit } }')
   nft add table ip switch_example
   nft 'add chain ip switch_example postrouting { type nat hook postrouting priority srcnat; }'
   nft "add rule ip switch_example postrouting ip saddr 192.0.2.2 oifname \"$uplink\" masquerade"
   ```

6. Recompute the TCP checksum of incoming packets before they are forwarded, which Docker Desktop needs, and wait for the client:

   ```sh
   tc qdisc add dev "$uplink" clsact
   tc filter add dev "$uplink" ingress pref 49152 protocol ip matchall action csum tcp
   wait "$client"
   ```

The client prints the report on the card it opened, three book-ticker messages and `WebSocket closed`.
