# The network config file

[Preparing a Linux machine](host-setup.md) and [Running on EC2](ec2.md) both end with a file called `dut.json`, written by `switch-netconf discover`. That file is the network config file: everything `switch_open::open` needs to know about the card. It says which device to open, which IPv4 address the Stack uses, where the gateway is, how large packets may be, which checksum work to hand to the card, and a few DPDK settings. The examples in this book call it `dut.json` for a real card and `tap.json` for a tap device. The crate `switch-netconf` reads and checks it, and the [`switch-netconf` command](netconf-tool.md), the next page, writes it from what Linux knows about an interface.

This page goes through the file key by key. The loader is strict: it rejects a file with an unknown key or a missing one, so every key below is either required or explicitly marked optional.

## A complete example

This is the tap device configuration the [first network program](../getting-started/first-connection.md) uses:

```json
{{#include ../../../examples/tap.json}}
```

And this is a config for an ENA card on EC2, the shape `switch-netconf discover --aws` writes:

```json
{{#include ../../../conduit/crates/switch-netconf/examples/dut-ena.json}}
```

Each key feeds one part of opening the card:

```text
 key               consumed by
 ---------------   ----------------------------------------------------------
 version           loader (must be 1)
 device            EAL arguments (-a <pci>[,<devargs>]  or  --no-pci --vdev <spec>),
                   then device lookup by PCI address or vdev name
 mac               MAC check against the device (PCI cards only)
 ipv4              Stack: our address and subnet
 gateway           Stack: next hop for off-subnet traffic; resolved by ARP during open
 owned_ipv4        Stack: extra addresses it answers for
 mtu               NIC port MTU, packet-buffer sizes, path MTU table
 paths             path MTU table (TCP MSS and UDP size limit per destination)
 offloads          offload choice -> NIC port configuration
 rx_timestamps     NIC port configuration (hardware receive timestamps)
 descriptors       NIC port receive and transmit ring sizes
 eal.file_prefix   EAL arguments (--file-prefix)
 eal.extra         EAL arguments, appended last
```

## The device

`device` names the card in one of two forms.

A PCI card is an object with six keys:

| Key | Meaning |
|---|---|
| `pci` | The card's PCI address, `dddd:bb:dd.f` in lowercase hex, for example `0000:00:06.0`. |
| `pci_id` | Vendor and device id, `vvvv:dddd`, for example `1d0f:ec20`. `bind` checks the card at `pci` still has this id. |
| `devargs` | Extra settings for the NIC driver, appended to the device on the EAL command line as `-a <pci>,<devargs>`. Empty string for none. ENA's `llq_policy=1` is an example. |
| `kernel_driver` | The Linux driver the card belongs to when it is not yours (`ena`, `ice`, ...). `unbind` gives the card back to it. |
| `kernel_interface` | The interface name Linux uses for the card (`eth1`). |
| `numa_node` | The memory node the card is attached to, as Linux reports it; `-1` when the machine has none. `bind` prints free hugepages on this node. |

A tap device, or any other virtual DPDK device, is an object with one key, `vdev`, holding the device string DPDK understands: `"net_tap0,iface=cdnic0"` creates a tap device that Linux shows as the interface `cdnic0`. The part before the first comma is the device name `open` looks up after EAL has created it.

`mac` is the card's Ethernet address as a string (`"0a:12:34:56:78:9a"`) or `null`. The key must be present either way. A PCI card requires a value, and `open` fails with a MAC mismatch if the card reports a different one, which catches a config written for another card. Every vdev, including a tap device, requires `null`; the loader rejects non-null values because `mac` applies to PCI devices. The Stack always uses the address the device itself reports; the config value is only a check.

## Addresses

`ipv4` is the Stack's own address with its subnet length, `"10.0.1.23/24"`. It decides three things: which address the Stack answers ARP for, which destinations are on the local subnet and reached directly, and the default source address. It must be a unicast address.

`gateway` is the next-hop router for every destination outside the subnet, or `null` if the Stack only talks to machines on its own subnet. The key must be present either way. When a gateway is set, `open` sends ARP for it and waits for the answer before returning, so a wrong gateway shows up at startup, not on the first connect. The loader requires a gateway to be:

- a unicast address,
- inside the `ipv4` subnet,
- different from our own address,
- neither the subnet's network address nor its broadcast address.

`owned_ipv4` is optional. It lists more addresses the Stack answers for: single addresses as `/32`, or whole prefixes. EC2 uses it for an interface's secondary private addresses and delegated prefixes. Sockets can listen on and bind to these addresses, and use them as their source. Adding them adds no routes and does not change the default source address: `ipv4` still decides routing, the subnet broadcast and the default source. Each entry must have no host bits set (`10.0.2.0/24`, not `10.0.2.7/24`) and may not cover 0.0.0.0, a multicast address, 255.255.255.255, the gateway or the subnet's broadcast address. When the file loads, the list is sorted, and duplicates and entries that sit inside another entry are dropped.

## MTU and path rules

The MTU is the largest IP packet the card sends, headers included. 1500 is the normal Ethernet value; EC2 supports 9001 inside a VPC. `mtu` sets the NIC port's MTU and the size of its packet buffers, and must be at least 88. (88 is the smallest MTU that carries the 48-byte minimum TCP segment plus 40 bytes of IPv4 and TCP headers.)

The port MTU is the largest packet the card can send, but the network between you and a destination may carry less: an internet gateway forwards 1500-byte packets even when the VPC allows 9001. `paths` describes those limits, one rule per destination prefix:

```json
"paths": [
  {"prefix": "10.0.0.0/16", "mtu": 9001, "why": "vpc"},
  {"prefix": "0.0.0.0/0",   "mtu": 1500, "why": "default"}
]
```

The limit toward a destination is its path MTU, found like this:

1. The longest matching prefix wins.
2. When there is at least one rule, the Stack's own subnet counts as an extra rule at the port MTU, unless a rule names exactly that prefix.
3. A rule above the port MTU is cut down to the port MTU.
4. A destination no rule matches gets the port MTU; an empty `paths` means every destination gets the port MTU.

Every rule's `mtu` must be at least 88, its prefix must have no host bits set, no prefix may appear twice, and `why` must be a non-empty label. `why` is free text for people, with one exception: `switch-netconf discover -o` regenerates the rules labelled `vpc` and `default` and keeps every other rule, so give hand-written rules any other label.

The path MTU limits two things. For TCP, the largest segment payload (the MSS) toward a destination is its path MTU minus 40. For UDP, a datagram may carry at most the path MTU minus 28 bytes of payload; the Stack does not fragment.

When you size a TCP socket before opening the card, ask the config for the same number the Stack will use. `Config::path_mtu` runs the same lookup:

```rust,ignore
mss_cap: Some(config.path_mtu(server) - 40),
```

(`conduit/crates/switch-open/examples/setup.rs` does this; [Opening the NIC from code](open.md) shows it in context.)

## Offloads and timestamps

`offloads` is either the string `"auto"` or an object with exactly three booleans:

```json
"offloads": {"rx_checksum": false, "tx_tcp_checksum": false, "tx_udp_checksum": false}
```

An explicit object is requested from the card as written. `"auto"` chooses per NIC driver. `rx_timestamps` asks the card to stamp every received packet with its own clock; it is never chosen automatically. [Offloads and hardware timestamps](offloads.md) explains both. For a tap device, use the explicit all-`false` object shown above; `"auto"` also requests nothing there.

## Descriptors and EAL settings

`descriptors` is the number of slots requested in each of the card's two rings (one receive queue, one transmit queue). It must be greater than zero. The NIC driver may round it to a size the card supports; the report `open` returns prints the numbers it actually used as `descriptors: rx=... tx=...`. The examples use 1024 for real cards; the tests use 16 on tap devices.

`eal.file_prefix` is passed to DPDK as `--file-prefix`. DPDK names the files it keeps for this process after it. It must not be empty. `switch-netconf discover` writes `"switch"`.

`eal.extra` is a list of further EAL arguments, appended after the ones `open` builds itself. `open` always passes the CPU (`--lcores`), the file prefix, `--no-telemetry`, and the device; anything else DPDK should know goes here. A tap device in a container usually runs without hugepages:

```json
"eal": {"file_prefix": "switch-tap", "extra": ["--no-huge", "-m", "256", "--no-shconf"]}
```

## Loading the file from code

```rust,ignore
let config = switch_netconf::Config::load(Path::new("dut.json"))?;
```

`Config::load` reads and validates the file; `Config::from_json` does the same for a string, and `to_json` writes a config back out. A failed load returns a `ConfigError` whose message names the key that is wrong, for example `gateway: gateway must be inside the IPv4 subnet`. `switch-netconf check dut.json` runs the same load and prints a summary, so you can check a file before your program starts.

## Validation rules

The loader applies all of these. The message is what `ConfigError` displays.

| Key | Rule | Message |
|---|---|---|
| (any) | no unknown keys, no missing keys | serde's message naming the key |
| `version` | must be 1 | `version: must be 1` |
| `device.pci` | `hhhh:hh:hh.f` lowercase hex | `device.pci: invalid PCI BDF` |
| `device.pci_id` | `hhhh:hhhh` lowercase hex | `device.pci_id: invalid PCI ID` |
| `device.kernel_driver`, `device.kernel_interface` | not empty | `<key>: must not be empty` |
| `device.vdev` | not empty | `device.vdev: must not be empty` |
| `mac` | required for a PCI card | `mac: PCI device requires a MAC address` |
| `mac` | unicast | `mac: Ethernet address must be unicast` |
| `ipv4` | unicast | `ipv4: IPv4 address must be unicast` |
| `gateway` | unicast; inside the subnet; not ours; not the network or broadcast address | `gateway: gateway must be unicast`, `... must be inside the IPv4 subnet`, `... must not be our IPv4 address`, `... must not be the subnet's network address`, `... must not be the subnet's broadcast address` |
| `owned_ipv4[i]` | no host bits; covers only unicast addresses; not the gateway; not the subnet broadcast | `owned_ipv4[i]: owned IPv4 prefix has host bits set`, `... owned IPv4 addresses must be unicast`, `... must not include the gateway`, `... must not include the subnet's broadcast address` |
| `mtu`, `paths[i].mtu` | at least 88 | `mtu: must be at least 88 (48-byte minimum TCP MSS + 40 bytes of headers)` |
| `paths[i].prefix` | no host bits; no duplicates | `paths[i].prefix: prefix must not have host bits set`, `paths[i].prefix: duplicate of paths[j]` |
| `paths[i].why` | not empty | `paths[i].why: must not be empty` |
| `offloads` | `"auto"` or the three-boolean object | `offloads: offloads string must be auto` |
| `descriptors` | greater than 0 | `descriptors: must be > 0` |
| `eal.file_prefix` | not empty | `eal.file_prefix: must not be empty` |

Next: [switch-netconf: check, discover, bind, unbind](netconf-tool.md), the command that writes, checks and applies this file.
