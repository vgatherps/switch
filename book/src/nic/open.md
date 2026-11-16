# Opening the NIC from code

You have a [network config file](config.md) that passes `switch-netconf check` (see [the previous page](netconf-tool.md)). One call turns it into a running Stack:

```rust,ignore
let opened = unsafe { switch_open::open(&config, &sizing, None, &mut clock) }?;
```

This page explains what you pass to `open`, what it does in order, what it hands back and what each failure means. The function lives in the `switch-open` crate. Everything after it, driving the Stack through switch-net or directly, is in other chapters: [Sockets with switch-net](../net/index.md) for async tasks, [Programming the Stack directly](../stack/index.md) for a plain loop.

## The call

```rust,ignore
pub unsafe fn open(
    config: &Config,
    sizing: &Sizing,
    helper_cpus: Option<&[usize]>,
    clock: &mut impl FnMut() -> Instant,
) -> Result<Opened, OpenError>
```

Here is a complete setup, from loading the config to a connected socket on the Stack. It is the program `conduit/crates/switch-open/examples/setup.rs`:

```rust,ignore
{{#include ../../../conduit/crates/switch-open/examples/setup.rs:setup}}
```

Call `open` once per process, from the thread that will run the drive loop, with that thread already restricted to one CPU.

## What you pass

`config` is the loaded network config file.

`sizing` says how many sockets the Stack has room for and how many packet buffers to set aside for sending. Every field is required:

| Field | Meaning |
|---|---|
| `frame_share` | packet buffers that all TCP send queues and your own code may hold at once, on top of a fixed reserve |
| `max_sockets` | TCP socket slots; must be at least 1 |
| `max_udp_sockets` | UDP socket slots; 0 disables UDP |
| `rx_staging` | received packets the Stack may hold before processing them |
| `tx_pending` | packets built but not yet taken by the card |
| `neighbor_capacity` | entries in the ARP cache; `DEFAULT_NEIGHBOR_CAPACITY` is 256 |

The examples compute `frame_share` from the socket's own configuration with `tcp::tx_mbuf_budget`, and use 32 for `rx_staging` and `tx_pending`. [Packet-buffer pools](pools.md) explains what each number controls and what happens when a pool runs dry.

`helper_cpus` says where DPDK's own helper threads may run. `None` is right for most programs: if the drive-loop CPU is isolated, `open` moves the helpers onto the machine's other CPUs by itself. [The pinned CPU and helper threads](cpus.md) explains both choices.

`clock` returns the current time for the Stack, as a smoltcp `Instant`. `open` calls it to set the Stack's start time and again while it waits for the gateway. After `open` returns, every call that drives the Stack must take its time from the same clock, because the Stack refuses a time earlier than one it has already seen. The pattern used throughout this book is microseconds elapsed since one `std::time::Instant` taken at startup:

```rust,ignore
let start = std::time::Instant::now();
let mut clock = || Instant::from_micros(i64::try_from(start.elapsed().as_micros()).unwrap());
```

[The drive loop](../net/drive-loop.md) shows how a switch-net program takes both the Stack's time and the runtime's tick from that one start.

Resolve host names before you call `open`. Name lookup goes through the kernel, and with a real card the kernel is about to lose the interface the Stack will use.

## What open does, step by step

```text
 step                                         fails with                carries
 ───────────────────────────────────────────  ────────────────────────  ─────────────
 1  check the thread may run on exactly       NotPinned                 nothing
    one CPU; that CPU becomes the pinned CPU
 2  build EAL arguments from the config
 3  start EAL (helper threads placed per      Eal, HelperAffinity,      nothing
    helper_cpus; affinity restored after)     EalAffinityRestore
 4  find the device by PCI address or         DeviceNotFound            nothing
    vdev name
 5  read the device's capabilities           DeviceInfo                nothing
    PCI card: config MAC = device MAC         MacMismatch               nothing
 6  choose offloads (see offloads page)
 7  let the NIC driver adjust descriptors     Descriptors               nothing
 8  size the pools, open the NIC port         PortConfig, PortOpen      PortOpen: a
                                                                        retained Port,
                                                                        if any
 9  wait up to 10 s for the link              Link, LinkDown            the Port
10  read a random seed for the Stack          Seed                      the Port
11  build the Stack (clock() = start time)    Stack                     the Port
12  if a gateway is set: ARP for it,          GatewayUnresolved,        the Stack
    up to 3 s                                 Receive                   (and its Port)
                                     │
                                     ▼
                         Ok(Opened { stack, eal, report })
```

Step 2 builds the arguments DPDK gets: `--lcores=0@<cpu>` (DPDK's main thread on the pinned CPU), `--file-prefix <eal.file_prefix>`, `--no-telemetry`, then either `-a <pci>[,<devargs>]` for a PCI card or `--no-pci --vdev <spec>` for a virtual device, then everything in `eal.extra`.

Step 9 waits up to ten seconds for the card to report its link up. The waiting itself is a sleep loop; nothing else is running yet.

Step 12 runs only when the config has a gateway. `open` asks the Stack to resolve the gateway's MAC address with ARP and drives the Stack until it has an answer or three seconds have passed. When it returns, the report shows how long that took.

## What you get back

`Opened` has three public fields.

`stack` is the running Stack, ready for sockets. Hand it to switch-net with `Net::new(stack, NetConfig { ingress_budget })` (see [Sockets with switch-net](../net/index.md)), or drive it yourself.

`eal` is the handle for DPDK's process setup. Keep it alive until the NIC port is closed. DPDK's setup is never torn down, and dropping the handle does nothing, but every packet buffer and the NIC port depend on it, so hold it for the life of the program.

`report` describes what was opened. Print it; every line is `key: value`:

```text
driver: net_tap
mac: ...
cpu: 3
port_id: 0
rx_offload_capa: 0x...
tx_offload_capa: 0x...
max_rx_pktlen: ...
min_mtu: ...
max_mtu: ...
descriptors: rx=1024 tx=1024
rx_pool_count: 32767
tx_pool_count: ...
rx_checksum: not requested (explicit)
tx_tcp_checksum: not requested (explicit)
tx_udp_checksum: not requested (explicit)
link_speed_mbps: ...
link_duplex: full
link_status: up
gateway: none
```

`driver` is the NIC driver DPDK picked. The two `offload_capa` lines are the card's capability bits as DPDK reports them. `descriptors` shows the ring sizes after the driver's adjustment. The three checksum lines say what was requested and why ([Offloads and hardware timestamps](offloads.md) explains the reasons). With a gateway, the last line reads `gateway: 10.0.1.1 at 0a-00-00-00-00-01 after 2 ms`: the gateway's address, its MAC, and how long ARP took.

## When it fails

`OpenError` names the step that failed, and its `Display` text says the same in a short phrase, for example `link wait: link down after 10 seconds`. [When the NIC won't open](troubleshooting.md) lists every message with its cause.

Errors from step 9 on carry the NIC port, or the Stack holding it, because the port exists by then and should be closed properly. `PortOpen` may carry a retained port as well. If your program simply returns the error, the port is dropped instead of closed. That is what the example above does, and it is fine for a program that exits on error. A long-running program, or one that wants to report how many buffers were still out, closes it. `switch-nic` does:

```rust,ignore
{{#include ../../../conduit/crates/switch-nic/src/main.rs:open_errors}}
```

`nic::close` in that program calls `try_close` on the port and prints the result.

## The safety contract in plain words

`open` is `unsafe` because DPDK and the card can write to memory behind Rust's back, and the function cannot check the conditions that make that safe. Its documentation states them precisely. For a program that opens one device from its config file, they come down to this:

- Call it once per process, for a device nobody else is using. EAL creates or finds the device from the config; no other code may have configured or started it.
- Do everything with the Stack, its sockets and its packet buffers on the thread that called `open`. Received data and buffers you send all belong to that thread.
- Keep `eal` alive until the NIC port is closed.
- The NIC driver must behave as DPDK drivers do: hand over valid packet buffers and take back the ones it is given. That holds for the drivers this project uses. It matters to you only if you attach your own DPDK ring device, in which case your code writing into that ring takes on the same duty.

## Closing

To close, drop every received frame and every frame you hold, take the port back from the Stack, and close it:

```rust,ignore
{{#include ../../../conduit/crates/switch-open/examples/tcp_client.rs:close}}
```

`try_close` returns `Ok(())` once every packet buffer is back in its pool. If your code still holds some, it returns the port together with `PortError::OutstandingBuffers { rx, tx, frame }`, counting the buffers still out in each pool; drop them and call it again. With switch-net in between, shut the runtime down and take the Stack out of the `Net` first; [Shutting down cleanly](../net/shutdown.md) gives the full order.

Next: [Packet-buffer pools](pools.md).
