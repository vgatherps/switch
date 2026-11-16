# switch-netconf: check, discover, bind, unbind

`switch-netconf` is the command-line side of the [network config file](config.md). It has four subcommands:

```text
switch-netconf check FILE
switch-netconf discover --interface IF [--ipv4 ADDR] [--aws] [-o FILE]
switch-netconf bind FILE [--noiommu] [--dry-run]      # root
switch-netconf unbind FILE                             # root
```

`check` tells you whether a file is valid. `discover` writes a file from what Linux knows about an interface. `bind` takes a PCI card away from its kernel driver and gives it to `vfio-pci`, so a DPDK program can open it; `unbind` gives it back.

The tool does not use DPDK, so it builds on any machine with a Rust toolchain:

```sh
cargo build --locked --release -p switch-netconf
```

`check` runs anywhere. `discover`, `bind` and `unbind` read Linux's `/sys` and `/proc` and only make sense on the Linux machine that has the card.

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

`check` loads the file with the same rules `switch_open::open` uses and prints a summary on stdout, exit 0. `mac: device` means the file has `"mac": null`, so the device's own address is used. Path rules print one per line under `paths:`. If the file is invalid, `check` prints one line on stderr naming the file and the problem and exits 1:

```text
switch-netconf: dut.json: gateway: gateway must be inside the IPv4 subnet
```

The full list of rules and messages is in [The network config file](config.md#validation-rules).

## discover

`discover` looks at a Linux network interface that still belongs to its kernel driver and writes a config for it. Run it before `bind`, while Linux can still tell you the card's address, MAC and PCI details. It does not need root.

```sh
switch-netconf discover --interface eth1               # print the config on stdout
switch-netconf discover --interface eth1 -o dut.json   # write or update dut.json
```

From the kernel it reads the interface's IPv4 address and prefix, MAC address, MTU, PCI address, PCI vendor and device id, kernel driver and NUMA node. The gateway comes from the interface's default route in `/proc/net/route`. When the interface has no default route, `gateway` is `null`; set it by hand if the Stack must reach other subnets.

A freshly discovered file has these values for the keys Linux cannot tell it:

| Key | Value |
|---|---|
| `mtu` | the smaller of the interface's MTU and 1500 |
| `paths` | one rule `0.0.0.0/0` at 1500 with `why` `default` (with `--aws`, also one rule per VPC block at 1500 with `why` `vpc`) |
| `offloads` | `"auto"` |
| `rx_timestamps` | `false` |
| `descriptors` | 1024 |
| `eal` | `{"file_prefix": "switch", "extra": []}` |
| `devargs` | empty |

`discover` refuses, with one line on stderr and exit 1, when the interface does not exist, is not a PCI device, has no kernel driver bound, has no IPv4 address, or has several default gateways. When the interface has several IPv4 addresses, it asks you to pick one with `--ipv4 ADDR`. If the interface's link is not up, it still writes the file and warns:

```text
switch-netconf: warning: eth1 operstate is down
```

### On EC2: --aws

With `--aws`, `discover` also asks the EC2 instance metadata service about the interface. It needs the network path to `169.254.169.254`; inside Docker that means `--network host`. The extra facts it uses:

- It refuses the primary interface of the instance (device number 0), because that one carries SSH and the metadata service itself: `eth0 is the primary ENI (device-number 0); it carries IMDS and SSH`.
- The address must appear in the interface's `local-ipv4s`. On an interface Linux has no address for, it takes the first of those, or the one you name with `--ipv4`.
- The gateway is the subnet's base address plus one (the VPC router). If the kernel has a default route on the interface through a different router, `discover` refuses.
- `owned_ipv4` lists every `local-ipv4s` address as a `/32` and every delegated prefix.
- `paths` gets a rule at 1500 for each VPC address block, labelled `vpc`.
- When the interface has no public IPv4 address, it warns: internet destinations then need an Elastic IP on this interface or a NAT gateway route.

[Running on EC2](ec2.md) walks through the whole EC2 setup.

### Updating an existing file with -o

`-o FILE` writes the result to `FILE` instead of stdout and prints `wrote FILE` on stderr. If `FILE` already exists, it must be a valid config, and `discover` merges instead of overwriting. The facts it just discovered replace the old ones. These keep their old values:

- `mtu`, if the old one was above 1500 (a jumbo MTU you set by hand);
- `devargs`, `offloads`, `rx_timestamps`, `descriptors` and `eal`;
- every path rule whose `why` is not `vpc` or `default` (the `vpc` and `default` rules are regenerated; a new rule with the same prefix as a kept one is dropped);
- `owned_ipv4`, except with `--aws`, where the metadata service lists every address and the old list is replaced.

For each key that changed it prints `changed: <key>` on stderr. When `owned_ipv4` changes, it also prints one `owned_ipv4 removed: <cidr>` or `owned_ipv4 added: <cidr>` line per entry. The file is written through a temporary file that is synced and renamed into place, so a failure or a kill leaves the old file as it was.

So the workflow is: discover once, hand-edit the keys you care about (offloads, timestamps, descriptors, a jumbo MTU, extra path rules), and run `discover -o` again whenever the machine's network changes. Your edits survive.

## bind

`bind` hands a PCI card to `vfio-pci`. Run it as root, after `discover` and `check`:

```sh
switch-netconf bind dut.json --dry-run    # prints every write, changes nothing
switch-netconf bind dut.json              # machines with an IOMMU
switch-netconf bind dut.json --noiommu    # machines without one (EC2 instances other than metal)
```

Before it writes anything, `bind` checks:

1. It runs as root (`bind/unbind needs euid 0`).
2. The config describes a PCI card. A tap config is refused with `bind needs a PCI device`; tap devices need no binding.
3. The device at `pci` exists and still has the `pci_id` from the config.
4. If the card is on its kernel driver, the interface's MAC equals the config's `mac`.
5. The card is on its kernel driver, on `vfio-pci`, or on no driver. Any other driver is refused.
6. While the card is on its kernel driver, the interface carries no route except its own connected subnet. A default route or any route through a router is refused (`eth1 has route 0.0.0.0/0 via 10.0.1.1`), so binding never cuts the route you are logged in through.
7. Every other device in the card's IOMMU group is unbound or already on `vfio-pci`.
8. If the card has no IOMMU group, you passed `--noiommu`.

Each refusal is one line on stderr, exit 1, and nothing has been written.

Then it loads the `vfio-pci` module if it is missing, and makes the move with a short series of writes to sysfs, printing each one as `write '<value>' > <path>`: switch on the kernel's unsafe no-IOMMU mode if `--noiommu` needs it, detach the card from its kernel driver, mark it for `vfio-pci`, attach it, and clear the mark. It then verifies that the card is on `vfio-pci` and that its device node exists under `/dev/vfio/`, and prints:

```text
0000:00:06.0 bound to vfio-pci (/dev/vfio/noiommu-0)
node0 hugepages-2048kB HugePages_Free: 512
```

The last lines show the free hugepages on the card's NUMA node, so you can see whether the program will find packet memory there.

```text
        kernel driver (ena, ice, ...)                     vfio-pci
       ┌───────────────────────────┐   bind (root)   ┌─────────────────────┐
       │ interface eth1 exists     │ ──────────────► │ no Linux interface  │
       │ SSH/DNS could use it      │   checks 1-8    │ one DPDK program    │
       └───────────────────────────┘   first         │ may open it         │
                 ▲     ▲                             └──────────┬──────────┘
                 │     │ a write or the final check fails       │
                 │     └── after the kernel driver was detached:│
                 │         detach from vfio-pci, clear the mark,│
                 │         re-attach the kernel driver          │
                 └──────────────── unbind (root) ───────────────┘
```

If a write fails or the final check fails after `bind` detached the kernel driver, it puts the card back: detach it from `vfio-pci` if it got there, clear the mark, and re-attach the kernel driver. The error then reports the original failure, any failure of those recovery steps, and which driver the card ended up on. A write that fails on a card that had no driver changes no binding and is reported alone.

A card that is already on `vfio-pci` is not touched again: `bind` clears a leftover mark if there is one, checks the device node and prints `0000:00:06.0 already bound to vfio-pci (...)`, exit 0. So running `bind` again after an interrupted run finishes the job.

`--noiommu` turns on the kernel's unsafe no-IOMMU mode. Without an IOMMU, nothing stops the card from writing to any memory the device driver tells it to, and the kernel marks itself tainted when the mode is used. It is the only way to use DPDK on machines without an IOMMU, such as EC2 instances other than metal ones.

## unbind

```sh
switch-netconf unbind dut.json
```

`unbind` gives the card back to the kernel driver named in `kernel_driver` and checks that the interface named in `kernel_interface` came back. It runs the same checks as `bind` points 1 to 5 first. It prints, for example:

```text
0000:00:06.0 on ena; IPv4 10.0.1.23 is back on eth1
```

or `is not back`. `unbind` does not restore addresses or routes. Whatever manages the machine's network (systemd-networkd, NetworkManager, the EC2 agent) puts them back, or you do. A card already on its kernel driver prints `already on ena` and exits 0.

## Exit codes

| Code | Meaning |
|---|---|
| 0 | success |
| 1 | the command failed; one line on stderr starting `switch-netconf:` says why |
| 2 | usage error (unknown subcommand, missing or repeated option); the usage line is printed |

Warnings from `discover` start `switch-netconf: warning:` and do not change the exit code.

## From code

The library behind the command is public. `switch_netconf::Config` is the config itself; `discover::{discover, discover_aws}`, `rediscover::write` and `devbind::{bind, unbind}` are what the subcommands call. They take a `Roots` value naming where `/sys`, `/proc` and `/dev` are, which lets tests run them against a fake tree. Most programs only need `Config::load`.

Next: [Opening the NIC from code](open.md).
