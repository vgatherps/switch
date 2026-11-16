# The network config file

A program that opens a card, `switch-nic` and `switch-junctiond` all read the same JSON file. It names the device, the IPv4 address the program uses, the gateway, packet sizes, offloads and a few DPDK settings. The file can have any name. For a real card these pages keep it at `/etc/switch/dut.json`, where `deploy/hft-host.sh` also writes it; the tap example is `examples/tap.json`. [switch-netconf](switch-netconf.md) writes it from a Linux interface and checks it. Programs load it with `switch::nic::Config::load`, which applies the same checks.

Every key is required except `owned_ipv4`, and unknown keys are rejected.

## Examples

A tap device is a virtual card that DPDK creates and that Linux sees as an ordinary interface, here `cdnic0`, so programs on the Linux side can be the peer. This is `examples/tap.json`:

```json
{
  "version": 1,
  "device": {"vdev": "net_tap0,iface=cdnic0"},
  "mac": null,
  "ipv4": "192.0.2.2/24",
  "gateway": null,
  "mtu": 1500,
  "paths": [],
  "offloads": {"rx_checksum": false, "tx_tcp_checksum": false, "tx_udp_checksum": false},
  "rx_timestamps": false,
  "descriptors": 1024,
  "eal": {"file_prefix": "switch-tap", "extra": ["--no-huge", "-m", "256", "--no-shconf"]}
}
```

An ENA card on EC2, as `switch-netconf discover --aws` writes it for an ENI with one private address, with the ENA driver option `llq_policy=1` added by hand:

```json
{
  "version": 1,
  "device": {
    "pci": "0000:00:06.0",
    "pci_id": "1d0f:ec20",
    "devargs": "llq_policy=1",
    "kernel_driver": "ena",
    "kernel_interface": "eth1",
    "numa_node": 0
  },
  "mac": "0a:12:34:56:78:9a",
  "ipv4": "10.0.1.23/24",
  "gateway": "10.0.1.1",
  "owned_ipv4": ["10.0.1.23/32"],
  "mtu": 1500,
  "paths": [
    {"prefix": "10.0.0.0/16", "mtu": 1500, "why": "vpc"},
    {"prefix": "0.0.0.0/0", "mtu": 1500, "why": "default"}
  ],
  "offloads": "auto",
  "rx_timestamps": false,
  "descriptors": 1024,
  "eal": {"file_prefix": "switch", "extra": []}
}
```

`version` is always 1.

## device and mac

A PCI card is an object with six keys:

| Key | Value |
|---|---|
| `pci` | The PCI address, `dddd:bb:dd.f` in lowercase hex, such as `0000:00:06.0`. |
| `pci_id` | Vendor and device id, `vvvv:dddd`, such as `1d0f:ec20`. `bind` refuses a card whose id differs. |
| `devargs` | Options for DPDK's driver, passed as `-a <pci>,<devargs>`. Empty for none. |
| `kernel_driver` | The Linux driver the card goes back to on `unbind`, such as `ena` or `ice`. |
| `kernel_interface` | The card's Linux interface name, such as `eth1`. |
| `numa_node` | The card's NUMA node as Linux reports it, `-1` for none. |

Any other DPDK device is an object with one key, `vdev`, holding DPDK's device string. `"net_tap0,iface=cdnic0"` creates a tap device named `net_tap0` that Linux shows as `cdnic0`.

`mac` is the card's Ethernet address, such as `"0a:12:34:56:78:9a"`. A PCI card requires it, and opening fails if the card reports a different address, which catches a file written for another card. A `vdev` requires `null`. The program always uses the address the device reports.

## Addresses

`ipv4` is the program's address and subnet, such as `"10.0.1.23/24"`. It decides which address the program answers ARP for, which destinations are on the local subnet, and the default source address.

`gateway` is the router for destinations outside the subnet, or `null` if the program talks only to its own subnet. It must be another unicast address inside the subnet and, except in a /31, not the subnet's network or broadcast address. When a gateway is set, opening the card waits up to three seconds for the gateway to answer ARP, so a wrong gateway fails at startup.

`owned_ipv4` lists further addresses the program answers for: single addresses as `/32`, or whole prefixes. `discover --aws` fills it with every private address of the ENI and its delegated prefixes. Sockets can listen on and send from these addresses. They do not change routing or the default source, which still come from `ipv4`. Entries must have no host bits set (`10.0.2.0/24`, not `10.0.2.7/24`) and must not include 0.0.0.0, a multicast address, 255.255.255.255, the gateway or the subnet's broadcast address.

## mtu and paths

`mtu` is the largest IP packet the card sends, headers included, at least 88. It also sizes the packet buffers. 1500 is the usual Ethernet value; EC2 supports 9001 inside a VPC ([AWS: Network MTU for your EC2 instance](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/network_mtu.html)).

The network toward a destination may carry less than the card: an EC2 internet gateway forwards packets of at most 1500 bytes, even when the VPC allows 9001. `paths` sets a limit per destination prefix:

```json
"paths": [
  {"prefix": "10.0.0.0/16", "mtu": 9001, "why": "vpc"},
  {"prefix": "0.0.0.0/0",   "mtu": 1500, "why": "default"}
]
```

The limit toward a destination, its path MTU, is the `mtu` of the longest matching prefix, capped at the card's `mtu`. When `paths` has rules, the program's own subnet counts as one more rule at the card's `mtu`, unless a rule names that prefix. A destination that no rule matches, or any destination when `paths` is empty, gets the card's `mtu`.

TCP segments toward a destination carry at most the path MTU minus 40 bytes of data, and UDP datagrams at most the path MTU minus 28. The program does not fragment packets.

Each rule's `mtu` must be at least 88, no prefix may have host bits set or appear twice, and `why` must be a non-empty label. `why` is a note for people, except that `switch-netconf discover -o` regenerates the rules labelled `vpc` and `default`. Give rules you add any other label.

## offloads and rx_timestamps

`offloads` is `"auto"` or an object with three booleans, `rx_checksum`, `tx_tcp_checksum` and `tx_udp_checksum`. `rx_timestamps` asks the card to stamp each received packet with its clock. [Offloads and timestamps](offloads.md) explains both. On a tap device use the all-`false` object.

## descriptors

`descriptors` is the number of slots in each of the card's two rings, one for receiving and one for sending. It must be greater than 0. The driver may round it to a size the card supports; the report from opening the card shows the sizes used, as `descriptors: rx=1024 tx=1024`. `discover` writes 1024.

## eal

DPDK's startup arguments. Opening the card starts DPDK with this command line, `<cpu>` being the one CPU the program is pinned to:

```text
--lcores=0@<cpu> --file-prefix <file_prefix> --no-telemetry -a <pci>[,<devargs>] <extra...>     PCI card
--lcores=0@<cpu> --file-prefix <file_prefix> --no-telemetry --no-pci --vdev <vdev> <extra...>  vdev
```

`file_prefix` must not be empty; DPDK names the files it keeps for the process after it. `extra` holds any further DPDK arguments. A tap device in a container runs without hugepages with:

```json
"eal": {"file_prefix": "switch-tap", "extra": ["--no-huge", "-m", "256", "--no-shconf"]}
```

## Checking a file

`switch-netconf check /etc/switch/dut.json` prints a summary and exits 0, or prints why the file is invalid and exits 1. Most messages name the key:

| Key | Message |
|---|---|
| `version` | `version: must be 1` |
| `device.pci`, `device.pci_id` | `device.pci: invalid PCI BDF`, `device.pci_id: invalid PCI ID` |
| `device.kernel_driver`, `device.kernel_interface`, `device.vdev` | `<key>: must not be empty` |
| `device` with a missing, extra or misspelled key | `data did not match any variant of untagged enum Device` |
| `mac` | `mac: invalid MAC address`, `mac: PCI device requires a MAC address`, `mac: applies to PCI devices; use null for a vdev`, `mac: Ethernet address must be unicast` |
| `ipv4` | `ipv4: invalid IPv4 CIDR`, `ipv4: IPv4 address must be unicast` |
| `gateway` | `gateway: invalid IPv4 address`, `gateway: gateway must be unicast`, `gateway: gateway must be inside the IPv4 subnet`, `gateway: gateway must not be our IPv4 address`, `gateway: gateway must not be the subnet's network address`, `gateway: gateway must not be the subnet's broadcast address` |
| `owned_ipv4[i]` | `owned_ipv4[i]: invalid IPv4 CIDR`, `owned_ipv4[i]: owned IPv4 prefix has host bits set`, `owned_ipv4[i]: owned IPv4 addresses must be unicast`, `owned_ipv4[i]: owned IPv4 addresses must not include the gateway`, `owned_ipv4[i]: owned IPv4 addresses must not include the subnet's broadcast address` |
| `mtu`, `paths[i].mtu` | `<key>: must be at least 88 (48-byte minimum TCP MSS + 40 bytes of headers)` |
| `paths[i].prefix` | `prefix: invalid IPv4 CIDR`, `paths[i].prefix: prefix must not have host bits set`, `paths[i].prefix: duplicate of paths[j]` |
| `paths[i].why` | `paths[i].why: must not be empty` |
| `offloads` | `offloads: offloads string must be auto` |
| `descriptors` | `descriptors: must be > 0` |
| `eal.file_prefix` | `eal.file_prefix: must not be empty` |
| a missing or unknown key | ``missing field `descriptors` ``, ``unknown field `foo`, expected one of ...`` |
