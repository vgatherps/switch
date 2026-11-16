# Preparing a Linux machine

These steps take a Linux machine with a spare network card to the point where a switch program owns that card. They are written for Ubuntu 24.04 and an Intel card; on EC2, follow [Running on EC2](ec2.md) instead.

DPDK as the repository builds it (the Docker image, and `deploy/hft-host.sh`) has drivers for Intel E810 (`net_ice`), X710 and relatives (`net_i40e`), 82599 and X5xx (`net_ixgbe`) and Amazon ENA (`net_ena`), plus the virtual `net_tap`, `net_af_packet`, `net_memif`, `net_pcap` and `net_ring`. Other cards, such as Mellanox ConnectX, need a DPDK built with their driver.

An E810 also needs its DDP package, `ice.pkg`. DPDK's driver loads it from `/lib/firmware/updates/intel/ice/ddp/` or `/lib/firmware/intel/ice/ddp/` when the card opens; without it the card does not start, and stderr contains `Failed to load the DDP package`.

When a program owns a card, Linux no longer sees it: the interface disappears from `ip link`, and SSH, DNS and package updates cannot use it. The machine therefore needs two interfaces. The management interface stays with Linux and keeps the default route. The data interface is the one your program takes.

```text
 1. pick the data interface                      once
 2. reserve hugepages on its NUMA node           root, every boot (or boot options)
 3. isolate a CPU                                root, boot options
 4. IOMMU on, or plan for --noiommu              root, boot options
 5. build switch-netconf and switch-nic
 6. discover, check, bind                        bind: root, every boot
 7. switch-nic info                              root
 8. run your program                             root
 9. unbind when done                             root
```

## 1. Pick the data interface

Note the interface name of the card you will hand over, for example `ens5f0`, and log in through the other interface from now on. `switch-netconf bind` in step 6 refuses while the data interface still carries a default route, but it does not look at which interface your SSH session uses.

To test two ports of one machine against each other, move the peer port into its own network namespace (`ip netns add peer; ip link set <if> netns peer`). Otherwise the kernel delivers the traffic locally and it never reaches the wire.

## 2. Reserve hugepages

A real card's packet buffers live in hugepages, which must be reserved before the program starts. Reserve them on the NUMA node the card is attached to; the node number is in `/sys/class/net/<if>/device/numa_node`. For 1 GiB of 2 MiB pages on node `N`:

```sh
echo 512 | sudo tee /sys/devices/system/node/node<N>/hugepages/hugepages-2048kB/nr_hugepages
sudo mkdir -p /dev/hugepages
mountpoint -q /dev/hugepages || sudo mount -t hugetlbfs nodev /dev/hugepages
```

This lasts until the next reboot. To make it permanent, add `hugepagesz=2M hugepages=<N>:512` to the kernel command line in step 3.

## 3. Isolate a CPU

The program's loop runs on one CPU at 100%. Pick a CPU on the card's NUMA node other than CPU 0, and keep the kernel's other work off it with these boot options, where `<iso>` is that CPU (and its hyperthread sibling) and `<housekeeping>` is every other CPU:

```text
isolcpus=managed_irq,domain,<iso> nohz_full=<iso> rcu_nocbs=<iso> irqaffinity=<housekeeping>
```

Add them, with the hugepage options from step 2 and the IOMMU option from step 4, to `GRUB_CMDLINE_LINUX` in `/etc/default/grub`, then:

```sh
sudo update-grub
sudo reboot
```

After the reboot, `cat /sys/devices/system/cpu/isolated` prints the isolated CPUs. Also stop `irqbalance`, which would otherwise move interrupts back onto the isolated CPU:

```sh
sudo systemctl disable --now irqbalance
```

[CPUs](cpus.md) explains what isolation changes for the program.

## 4. IOMMU

A PCI card is handed to a program through the `vfio-pci` kernel driver. With an IOMMU, the hardware limits which memory the card can reach. Turn it on in the firmware, and on Intel add `intel_iommu=on` to the boot options of step 3.

A machine without an IOMMU, such as most virtual machines, can still bind the card in no-IOMMU mode with `switch-netconf bind --noiommu`. That mode taints the kernel, and `bind` refuses to use it without the flag.

## 5. Build the tools

`switch-netconf` needs no DPDK. From the repository root:

```sh
cargo build --locked --release -p switch-netconf
```

`switch-nic` needs DPDK 25.11.3. On a machine with DPDK installed (the guide chapter Setting up for network programs in the `switch` crate documentation, `make docs`, shows how):

```sh
cargo build --locked --release -p switch-nic
```

The commands below assume both binaries from `target/release` are on your `PATH`.

Without a native DPDK, build the Docker image (`make -C conduit docker-image`) and use this shell function, defined in the `conduit/` directory. It builds `switch-nic` in the image and runs it pinned to `SWITCH_CPU` (default 2), with the host's network, hugepages, VFIO devices and `/lib/firmware`, for the E810's DDP package:

```sh
nic() {
  docker run --rm -it --privileged --network host \
    --cpuset-cpus="${SWITCH_CPU:-2}" \
    -v /dev/hugepages:/dev/hugepages -v /dev/vfio:/dev/vfio -v /lib/firmware:/lib/firmware:ro \
    -v "$PWD/..:/work" -w /work/conduit \
    -v switch-cargo-registry:/usr/local/cargo/registry -v switch-nic-target:/work/target \
    switch-dpdk-test:local sh -c 'cargo build --locked --release -p switch-nic &&
      exec taskset -c "$0" ../target/release/switch-nic "$@"' "${SWITCH_CPU:-2}" "$@"
}
```

File arguments to `nic` are relative to `conduit/`.

## 6. Discover, check and bind

While the card is still on its kernel driver, write its config file and check it:

```sh
switch-netconf discover --interface ens5f0 -o dut.json
switch-netconf check dut.json
```

`discover` prints `wrote dut.json`; `check` prints a summary of the file and exits 0. If the interface has no default route, `gateway` is `null`: set it by hand if the program must reach other subnets. Edit offloads, timestamps or EAL options now if you need them ([The network config file](config.md)), and run `check` again.

Next, stop your network manager from managing the interface and remove its addresses and routes, otherwise it puts the routes back and `bind` refuses. Under NetworkManager:

```sh
sudo nmcli device set ens5f0 managed no
sudo ip route flush dev ens5f0
sudo ip addr flush dev ens5f0
```

Under systemd-networkd, write a `.network` file that matches the interface and sets `Unmanaged=yes` under `[Link]`, run `sudo networkctl reload`, then flush as above.

Bind, first as a dry run:

```sh
sudo switch-netconf bind dut.json --dry-run
sudo switch-netconf bind dut.json             # with an IOMMU
sudo switch-netconf bind dut.json --noiommu   # without one
```

A successful bind ends with lines like:

```text
0000:3b:00.0 bound to vfio-pci (/dev/vfio/42)
node0 hugepages-2048kB HugePages_Free: 512
```

The binding does not survive a reboot; bind again after each boot. [switch-netconf](switch-netconf.md) lists every check `bind` makes.

## 7. Check with switch-nic info

```sh
sudo taskset -c <cpu> switch-nic --config dut.json info 2>&1 | tee info.log
```

With the Docker function: `SWITCH_CPU=<cpu> nic --config dut.json info 2>&1 | tee info.log`.

A good run exits 0 and its last lines include `switch-nic: link_status: up` and `switch-nic: port_close: ok`. DPDK writes its own messages to stderr, hence `2>&1`. [switch-nic](switch-nic.md) explains each line, and [Troubleshooting](troubleshooting.md) each error.

## 8. Run your program

A program that runs on a tap device runs unchanged on the card; only the config file and the peer change. With the example from the guide chapter Your first network program in the `switch` crate documentation (`make docs`):

```sh
cargo build --locked --release -p switch-examples --example tcp_client_local --features net
```

On a second machine on the data interface's subnet, start a one-shot server:

```sh
python3 -c 'import socket; s = socket.create_server(("0.0.0.0", 7000)); c, _ = s.accept(); c.recv(100); c.sendall(b"tick\n"); c.close()'
```

Then run the client as root, pinned to the isolated CPU:

```sh
sudo taskset -c <cpu> target/release/examples/tcp_client_local dut.json <peer>:7000
```

It prints the report from opening the card, then the server's reply `tick`, then `port closed`.

## 9. Give the card back

```sh
sudo switch-netconf unbind dut.json
```

This puts the card back on its kernel driver and prints whether the IPv4 address came back on the interface. It does not restore addresses or routes: hand the interface back to your network manager (`sudo nmcli device set ens5f0 managed yes`) for that. Remove the boot options to undo the isolation and the permanent hugepages.
