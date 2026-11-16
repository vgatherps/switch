# switch-netconf

`switch-netconf` writes and checks the [network config file](config.md), and moves a PCI card between its Linux driver and `vfio-pci`, the driver through which a DPDK program owns it.

```text
switch-netconf check FILE
switch-netconf discover --interface IF [--ipv4 ADDR] [--aws] [-o FILE]
switch-netconf bind FILE [--noiommu] [--dry-run]
switch-netconf unbind FILE
```

It needs no DPDK. Build it from the repository root:

```sh
cargo build --locked --release -p switch-netconf
```

`check` runs anywhere. `discover`, `bind` and `unbind` read Linux's `/sys` and `/proc` and run on the machine that has the card; `bind` and `unbind` need root.

| Exit status | Meaning |
|---|---|
| 0 | success |
| 1 | failure; one line on stderr, starting `switch-netconf:`, says why |
| 2 | usage error; the usage line is printed on stderr |

## check

```text
$ switch-netconf check tap.json
device: vdev net_tap0,iface=cdnic0
mac: device
ipv4: 192.0.2.2/24
gateway: null
owned_ipv4: none
mtu: 1500
paths:
offloads: {"rx_checksum":false,"tx_tcp_checksum":false,"tx_udp_checksum":false}
rx_timestamps: false
descriptors: 1024
eal: {"file_prefix":"switch-tap","extra":["--no-huge","-m","256","--no-shconf"]}
```

`mac: device` means the file has `"mac": null`, so the device's own address is used. Path rules print one per line under `paths:`, as `10.0.0.0/16 mtu 1500 (vpc)`. An invalid file prints the reason and exits 1:

```text
$ switch-netconf check /etc/switch/dut.json
switch-netconf: /etc/switch/dut.json: gateway: gateway must be inside the IPv4 subnet
```

[The network config file](config.md#checking-a-file) lists every message.

## discover

`discover` writes a config for an interface that is still on its Linux driver, from what the kernel knows about it: its IPv4 address and prefix, MAC address, MTU, PCI address and id, driver and NUMA node. It takes the gateway from the interface's default route; without one, `gateway` is `null`. It needs root only to write under `/etc`.

```sh
switch-netconf discover --interface eth1                                # print the config on stdout
sudo switch-netconf discover --interface eth1 -o /etc/switch/dut.json   # write or update the file
```

For what the kernel cannot tell it, a new file gets:

| Key | Value |
|---|---|
| `mtu` | the interface's MTU, at most 1500 |
| `paths` | one rule `0.0.0.0/0` at 1500 labelled `default` |
| `offloads` | `"auto"` |
| `rx_timestamps` | `false` |
| `descriptors` | 1024 |
| `eal` | `{"file_prefix": "switch", "extra": []}` |
| `devargs` | empty |

`discover` refuses, with one line on stderr and exit 1, when the interface does not exist, is not a PCI device, has no driver, has no IPv4 address, or has several default gateways. When the interface has several IPv4 addresses, choose one with `--ipv4 ADDR`. When the link is not up it still writes the file, and warns:

```text
switch-netconf: warning: eth1 operstate is down
```

### On EC2: --aws

With `--aws`, `discover` also asks the EC2 instance metadata service at `169.254.169.254`; from a Docker container that needs `--network host`. It then:

- refuses the instance's primary ENI, which carries SSH and the metadata service;
- requires the address to be one of the ENI's private addresses; on an interface Linux has no address for, it takes the first of them, or the one named by `--ipv4`;
- sets the gateway to the subnet's first address after the network address (the VPC router), and refuses if the kernel's default route on the interface goes elsewhere;
- lists every private address as a `/32`, and every delegated prefix, in `owned_ipv4`;
- adds a rule at 1500 labelled `vpc` for each of the VPC's address ranges;
- warns when the ENI has no public IPv4 address, since internet destinations then need an Elastic IP on it or a NAT gateway.

### Updating a file with -o

`-o FILE` writes the config to `FILE` and prints `wrote FILE` on stderr. If `FILE` exists, it must be a valid config, and `discover` updates it instead of replacing it. The discovered facts replace the old ones, and these keep their old values:

- `mtu`, if the old value was above 1500;
- `devargs`, `offloads`, `rx_timestamps`, `descriptors` and `eal`;
- every path rule not labelled `vpc` or `default`;
- `owned_ipv4`, except with `--aws`, which replaces it.

For each key that changed it prints `changed: <key>` on stderr, and for `owned_ipv4` also one `owned_ipv4 removed: <cidr>` or `owned_ipv4 added: <cidr>` line per entry. The file is replaced in one step, so an interrupted update leaves either the complete old file or the complete new one. The `changed:` lines are printed after the new file is in place: if printing them fails, `discover` exits 1 with the new file already installed. So you can edit the keys you care about and run `discover -o` again whenever the machine's network changes.

## bind

`bind` hands a PCI card to `vfio-pci`. Run it as root after `discover` and `check`:

```sh
switch-netconf bind /etc/switch/dut.json --dry-run    # prints every write, changes nothing
switch-netconf bind /etc/switch/dut.json              # the card has an IOMMU group
switch-netconf bind /etc/switch/dut.json --noiommu    # it has none
```

Before it writes anything, `bind` checks that:

1. it runs as root;
2. the config describes a PCI card;
3. the card at `pci` exists and has the config's `pci_id`;
4. if the card is on its Linux driver, its MAC address is the config's `mac`;
5. the card is on its Linux driver, on `vfio-pci`, or on no driver;
6. while the card is on its Linux driver, the interface has no route except its own subnet's;
7. every other device in the card's IOMMU group is on `vfio-pci` or on no driver, since `vfio-pci` takes a whole group;
8. if the card has no IOMMU group, `--noiommu` was given.

Each refusal is one line on stderr, exit 1, with nothing changed. [Troubleshooting](troubleshooting.md#binding) lists them.

`bind` then loads the `vfio-pci` module if needed and moves the card, printing each write as `write '<value>' > <path>`; a dry run prints the same lines prefixed with `dry-run: `. On success it prints the device node and the free hugepages on the card's NUMA node:

```text
0000:00:06.0 bound to vfio-pci (/dev/vfio/noiommu-0)
node0 hugepages-2048kB HugePages_Free: 512
```

If a step fails after the card left its Linux driver, `bind` tries to give it back to that driver and reports the driver the card ended on. A card already on `vfio-pci` prints `0000:00:06.0 already bound to vfio-pci (...)` and exits 0, so running `bind` again finishes an interrupted run.

`--noiommu` turns on the kernel's no-IOMMU mode for `vfio-pci`, which `bind` refuses to use without the flag when the card has no IOMMU group. Nothing then stops the card from writing to any memory, and the kernel marks itself tainted. It is the only way to bind a card that has no IOMMU group.

## unbind

```sh
switch-netconf unbind /etc/switch/dut.json
```

`unbind` gives the card back to its `kernel_driver`, checks that the `kernel_interface` reappeared, and prints whether the address came back:

```text
0000:00:06.0 on ena; IPv4 10.0.1.23 is back on eth1
```

It makes checks 1 to 5 of `bind` first. It does not restore addresses or routes: whatever manages the machine's network does that, or you do. A card already on its Linux driver prints `0000:00:06.0 already on ena` and exits 0.
