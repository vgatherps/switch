# Glossary

The words this book uses, each with a short definition and a link to the page that introduces it. The book uses one word for each idea; where code uses a different name, the entry says so.

**backend.** How a `Net` reaches the NIC. The local backend runs the Stack inside your process (`Net::new`); the junction backend leaves the Stack to the daemon and talks to it through a session (`Net::remote`). Task code is the same on both. See [Introduction](../introduction.md#two-ways-to-get-a-nic) and [Local and junction backends compared](../net/backends.md). On the command line of the repository's tools, `--backend conduit` means the local backend.

**backing.** A named shared-memory file that processes map to share memory. See [Shared memory and the receive ring](../blocks/rings.md).

**backpressure.** A send that cannot be queued right now: `try_send` returning `Full`, a UDP send returning `Full`, or a `room` that waits. See [TCP streams](../net/tcp.md).

**bind (a NIC).** Detach a PCI NIC from its kernel driver and attach it to `vfio-pci`, so a userspace program can drive it. Done with `switch-netconf bind`. See [switch-netconf](../nic/netconf-tool.md).

**busy polling.** Running the drive loop again at once, without sleeping, when nothing moved. It keeps the pinned CPU fully busy in exchange for the lowest latency. See [The drive loop](../net/drive-loop.md).

**cephalopod.** The group of `ceph-*` crates that let many processes send ordered messages to one reader through per-CPU shared-memory rings. The junction send ring is built on them. See [cephalopod](../blocks/cephalopod.md).

**control socket.** The daemon's Unix socket, `control.sock` in its directory, which carries each session's handshake, socket opens and closes, and events. Packet data does not use it. See [Sharing a NIC with the junction daemon](../junction/index.md).

**coordinator.** `switch-bench sweep`, the process that starts the generator workers, splits each step's rate among them and collects their results. It owns no NIC. See [Measuring latency under load](../measuring/index.md#workers).

**daemon.** `switch-junctiond`, the process that owns a NIC and runs the Stack for many applications on the same machine. See [Sharing a NIC with the junction daemon](../junction/index.md).

**data NIC; management NIC.** The data NIC is the one your program (or the daemon) owns through DPDK. The management NIC stays with Linux and carries SSH and DNS. See [Owning a NIC](../nic/index.md).

**DPDK.** A userspace NIC driver framework: it lets a program put packets on a NIC and take them off directly from its own memory, without the kernel. The Stack runs on it. DPDK builds only on Linux, and switch needs version 25.11.3. See [Introduction](../introduction.md) and [Setting up a build environment](../getting-started/environment.md).

**drive loop.** Your loop that, on the pinned CPU, calls `switch_net::round` (or the Stack's drive calls) over and over. Nothing moves between two trips through it. See [Your first network program](../getting-started/first-connection.md) and [The drive loop](../net/drive-loop.md).

**EAL.** DPDK's one-time setup per process (memory, devices, helper threads); type `Eal`. `switch_open::open` starts it for you. See [Owning a NIC](../nic/index.md).

**egress.** The step of a Stack turn that builds the packets sockets want to send: data, ACKs, retransmissions, SYNs and FINs. Data written after egress waits for the next one. See [The Stack's drive loop](../stack/drive-loop.md).

**event.** A control message from the daemon about one socket of a session: `Connected`, `Bound`, `Rejected`, `PeerFin`, `Delivered`, `Closed` or `Freed`. See [Using a Session directly](../junction/session.md).

**finish (a session).** The clean end of a session: call `finish()`, then poll until `closed()`. Dropping a session without finishing it counts as a crash, and the daemon resets its sockets. See [Closing sockets and ending a session](../junction/closing.md).

**frame.** A handle to the bytes of one packet buffer (or of one receive-ring record), so data can be sent and received without copying. `OwnedFrame` is exclusive and writable; `ImmutableFrame` is shared and read-only. Received data arrives as a `TcpFrame` or `Datagram` that wraps one. See [Frames](../net/frames.md) and [switch-frame](../blocks/frames.md). The protocol units on top of TCP are always written with a qualifier: see **WebSocket frame, TLS record, message**.

**gateway.** The next-hop router for addresses outside the NIC's subnet. Its hardware address is resolved by ARP when the NIC opens. See [The network config file](../nic/config.md).

**generator; receiver.** In a benchmark, the generator sends timed messages and records when each echo comes back; the receiver is the echo server being measured. See [Measuring latency under load](../measuring/index.md).

**hardware timestamp.** The time the NIC received a packet, as the raw value from the NIC driver, read with `rx_timestamp()`. 0 means no timestamp; units are the driver's. See [Offloads and hardware timestamps](../nic/offloads.md).

**helper threads.** Threads DPDK starts for interrupts and control work. `open` puts them on the housekeeping CPUs when the pinned CPU is isolated from the kernel scheduler or when you name helper CPUs; otherwise they share the pinned CPU. See [The pinned CPU and helper threads](../nic/cpus.md).

**housekeeping CPUs.** The CPUs left for the operating system, interrupts and helper threads. See [The pinned CPU and helper threads](../nic/cpus.md).

**hugepages.** Large memory pages the machine reserves ahead of time; real NICs take their packet memory from them. Tap devices run without them. See [Owning a NIC](../nic/index.md).

**ingress; ingress budget.** Ingress is the step of a Stack turn that processes received packets: TCP segments go to their sockets, ARP requests get replies. The ingress budget is the most received packets one turn processes; the rest wait for the next turn. It is passed as `NetConfig::ingress_budget` or to `Stack::turn`. See [The Stack's drive loop](../stack/drive-loop.md). The daemon's code also calls the send ring "ingress".

**isolated CPU.** A CPU the kernel keeps other work off, set with boot options. The pinned CPU is usually one. See [The pinned CPU and helper threads](../nic/cpus.md).

**kick.** The daemon ending a session whose send stream has a gap it cannot fill. See [Closing sockets and ending a session](../junction/closing.md).

**memif.** DPDK's shared-memory virtual NIC (`net_memif`): two processes exchange packets through shared memory instead of a wire. See [Stack-to-stack throughput with switch-memif-bench](../measuring/memif.md).

**mirrored mapping.** The same shared memory mapped twice, back to back, so a record that runs past the end of a ring continues at its start without being split. Code calls it `DoubleMapping`. See [Shared memory and the receive ring](../blocks/rings.md).

**MTU; path MTU; MSS.** The MTU is the largest IP packet the NIC port sends. A path MTU is a smaller limit the network config file sets for some destinations. The MSS is the largest TCP payload in one segment: the path MTU minus 40. See [The network config file](../nic/config.md).

**Net; NetHandle.** `Net` owns one local Stack or one junction session and belongs to its drive loop, which is the only place the network is turned; it is not `Clone`. `NetHandle` is what tasks hold to open sockets and get frames; it is cheap to clone. See [Sockets with switch-net](../net/index.md).

**network config file.** The JSON file that describes one NIC port: device, addresses, gateway, MTU, offloads and DPDK options. The examples call it `dut.json` or `tap.json`. See [The network config file](../nic/config.md).

**network thread; control thread.** The daemon's two threads of its own. The network thread runs on the daemon's pinned CPU and busy-polls the NIC and the Stack. The control thread, `junction-control`, runs on housekeeping CPUs and serves sessions over the control socket. An application's session has one background thread too, `junction-ctl`, which reads the control socket. See [Running switch-junctiond](../junction/running.md) and [Connecting an application through switch-net](../junction/apps.md).

**NIC driver.** DPDK's userspace driver for one family of NICs, such as `net_ena`, `net_ice` or `net_tap`. DPDK's documentation calls it a PMD (poll-mode driver). See [Owning a NIC](../nic/index.md).

**NIC port.** One NIC configured for DPDK, with its queues and pools; type `Port`. "Port" on its own always means a TCP or UDP port number. See [Owning a NIC](../nic/index.md).

**NUMA node.** A group of CPUs with their own memory, close to some devices. A NIC belongs to one node, and the pinned CPU and hugepages should come from the same node. See [Preparing a Linux machine](../nic/host-setup.md).

**offload.** Work the NIC does instead of the CPU, such as checksums or receive timestamps. See [Offloads and hardware timestamps](../nic/offloads.md).

**open loop; step; sweep.** Benchmark terms. In an open loop, sends follow a fixed timetable whether or not replies came back. A step holds one send rate for a fixed window. A sweep is a list of rising rates run one step after another. See [Measuring latency under load](../measuring/index.md).

**packet buffer.** DPDK's memory buffer for one packet; DPDK calls it an mbuf. See [Packet-buffer pools](../nic/pools.md).

**pass (of a poll).** One sweep over the tasks that are ready when it starts, inside a single poll of the runtime. Tasks woken during a pass run in the next pass of the same poll; the poll stops when no task is ready, after a pass that reaches 1024 task polls (a started pass finishes), or when a task calls `request_turn`. See [Driving the runtime](../runtime/driving.md#one-poll-step-by-step).

**pinned CPU.** The one CPU a drive-loop thread is allowed to run on. Opening a NIC refuses to start if the process may run on more than one. See [Your first network program](../getting-started/first-connection.md) and [The pinned CPU and helper threads](../nic/cpus.md).

**pinned test.** A test whose body runs on one long-lived thread that owns DPDK's EAL, marked `#[pinned_test]`. See [Writing tests that use DPDK](../testing/writing-tests.md).

**poll (of the runtime).** One call to `Runtime::poll(tick)`: it advances timers and runs ready tasks in passes, and returns when none is ready, after the 1024-poll budget, or when a task calls `request_turn`; ready work can remain for the next call. Nothing runs between polls. See [Your first task](../getting-started/first-task.md).

**pool; RX pool; TX pool; frame pool.** A pool is a fixed set of packet buffers made at startup. The RX pool holds arriving packets. The TX pool holds packets the Stack builds itself (ACKs, ARP, resets, copies). The frame pool holds the buffers your code writes into, which TCP keeps until they are acknowledged. See [Packet-buffer pools](../nic/pools.md).

**readiness token.** A `u32` your code arms on a socket half; the Stack reports it when that socket may have changed. See [Waiting on many sockets](../stack/readiness.md).

**receive ring.** The shared-memory ring, one per session, that the daemon writes received packets into. Code calls it the RX ring. See [Sharing a NIC with the junction daemon](../junction/index.md).

**reclaim.** Returning receive-ring space after the frames in it are dropped. It happens on the next poll of the session, in ring order. See [Using a Session directly](../junction/session.md).

**record.** One message in a ring: a received chunk or datagram in a receive ring, or one send in a send ring. See [Using a Session directly](../junction/session.md).

**room.** Space in a socket to queue a frame of a given length; a task waits for it with `TcpTx::room`. See [TCP streams](../net/tcp.md).

**round.** One turn followed by one poll of the runtime, both with the same clock sample: `switch_net::round`. See [The drive loop](../net/drive-loop.md).

**send ring.** The shared-memory rings, one per CPU and shared by every session, that carry sends from applications to the daemon. Code calls them ingress. See [Sharing a NIC with the junction daemon](../junction/index.md).

**sender-late.** A benchmark step in which a generator fell behind its timetable. The flag does not say whether the generator or receiver backpressure was the limit. See [Measuring latency under load](../measuring/index.md).

**session.** One application's connection to the daemon, type `Session` from `switch-junction`. See [Sharing a NIC with the junction daemon](../junction/index.md).

**smoltcp.** A TCP/IP stack written in Rust. The Stack is built on a copy of it vendored in this repository. See [Programming the Stack directly](../stack/index.md).

**socket slot; socket id.** A socket slot is an entry in a socket table: the Stack's (sized by `max_sockets` and `max_udp_sockets`) or a junction session's. A socket id, `SocketId { slot, generation }`, names one use of a session's socket slot; the generation counts reuse. See [Using a Session directly](../junction/session.md) and [TCP sockets on the Stack](../stack/tcp.md).

**sockperf.** A Linux network latency and load-test tool; `switch-nic sockperf-server` answers its clients. See [switch-nic](../nic/switch-nic.md#sockperf-server).

**spawner.** The cloneable `Spawner` handle that starts tasks and creates sleeps. See [Your first task](../getting-started/first-task.md).

**the Stack.** The TCP/IP stack (type `Stack`, crate `dpdk-smoltcp`) that runs over one NIC port on one thread. See [Owning a NIC](../nic/index.md) and [Programming the Stack directly](../stack/index.md).

**stream; sequence number.** cephalopod's terms for one producer's connection and the counter on each of its messages. Used only on the cephalopod page. See [cephalopod](../blocks/cephalopod.md).

**switch runtime.** The single-threaded async runtime in the `switch` crate. It does nothing until your code calls `poll`, reads no clock and starts no thread. See [The switch runtime](../runtime/index.md).

**tap device.** A virtual NIC that DPDK creates and Linux sees as an ordinary network interface, so the kernel can be the peer. Used to run programs without a spare NIC. See [Your first network program](../getting-started/first-connection.md).

**task.** One call of a `#[switch::task]` function, spawned on the runtime. See [Your first task](../getting-started/first-task.md).

**tick.** The `u64` clock value your code passes to `poll`. The unit is yours, but it must be nanoseconds once you use `within` or `Duration`-based sleeps, and ticks never go backwards. See [Your first task](../getting-started/first-task.md).

**turn.** One call that does the network's work, `Net::turn` or `Stack::turn`: take in packets, run timers, send what was queued, and wake tasks whose sockets changed. See [The drive loop](../net/drive-loop.md).

**vfio-pci.** The Linux kernel driver that lets one userspace process control a PCI device. Binding a NIC attaches it to `vfio-pci`. See [Owning a NIC](../nic/index.md).

**wake.** Marking a waiting task ready, so that a later pass of the current poll, or the next poll, runs it. See [The switch runtime](../runtime/index.md).

**WebSocket frame, TLS record, message.** The protocol units on top of TCP, always written with their qualifier. A message is one whole WebSocket message, which may span several WebSocket frames. See [TLS and WebSocket over switch-net](../protocols/index.md).

Back to the start: [Introduction](../introduction.md).
