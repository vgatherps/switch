# Connecting an application through switch-net

[Your first network program](../getting-started/first-connection.md) opened a NIC and ran one task that connected to a server, sent a line and printed the reply. This page runs the same task through the daemon you started in [Running switch-junctiond](running.md). The task does not change at all. Only the code before the drive loop and after it is different.

## What changes

The local program built its `Net` from a Stack it opened itself, and closed the NIC port at the end. With the daemon, the program connects a session instead, builds its `Net` with `Net::remote`, and finishes the session at the end:

```text
                 local backend                   junction backend
 setup           switch_open::open(...)          Session::connect(control.sock, max_sockets)
                 Net::new(stack, NetConfig)      Net::remote(session, RemoteConfig)
 tasks           hello(...)                      hello(...)              (unchanged)
 drive loop      switch_net::round(...)          switch_net::round(...)  (unchanged)
 teardown        rt.shutdown()                   rt.shutdown()
                 net.into_stack()                net.into_session()
                 port.try_close()                finish, then poll until closed()
```

The program no longer needs a network config file, hugepages or DPDK setup at run time; the daemon has done all of that. It still links DPDK libraries when it builds, because switch-net does.

## Connecting

```rust,ignore
{{#include ../../../switch-net/examples/tcp_client_junction.rs:connect}}
```

`Session::connect` takes the path of the daemon's `control.sock` and the number of sockets this application wants. It connects to the daemon, introduces itself, and returns once the daemon accepts the session. If the daemon is still opening its NIC, the call waits until it is done (see [Knowing it is ready](running.md#knowing-it-is-ready)).

The session reports two sizes the daemon chose. `frame_capacity()` is the largest packet payload the daemon will carry, its MTU minus 28 bytes of IP and UDP headers. `max_record()` is the largest single send the session accepts, header included. The example turns the frame capacity back into an MTU to size its send frames.

`Net::remote` wraps the session in a `Net`. From here on the program uses `net.handle()`, `switch_net::round` and its tasks exactly as before.

When the connect fails, the error says why:

| Error | Meaning |
|---|---|
| `ConnectError::Io(..)` | the socket could not be reached (wrong path, no daemon, permissions), or the daemon closed the connection before answering, for example because it exited during startup |
| `ConnectError::Rejected(BadMaxSockets)` | `max_sockets` was 0 or above the daemon's `--max-sockets` |
| `ConnectError::Rejected(SessionsFull)` | all `--max-sessions` sessions are in use. This can last a moment after another session ends, while the daemon releases its resources; retry |
| `ConnectError::Rejected(HostResources)` | the daemon could not create this session's receive ring |
| `ConnectError::Version { host }` | the daemon speaks a different protocol version. The application and the daemon must be built from the same version of `switch-junction`; versions must match exactly |
| `ConnectError::Protocol(..)` | the daemon's reply could not be decoded |

## Choosing `max_sockets` and `tx_frames`

`max_sockets` is the size of this session's socket table. Every TCP connection, every listener and every UDP socket the application has open at once takes one entry, and an entry is reused only after the daemon has finished with the old socket. If the table is full, opening another socket fails with `Error::SocketsFull` on the first poll. The repository's tools set it to the number of sockets they open (one per connection plus one per listener).

`RemoteConfig.tx_frames` is the number of send frames in the application's own frame pool. Each is `frame_capacity` bytes. A task's `handle.tx_frame().await` takes one from this pool and waits while the pool is empty. On the junction backend a frame returns to the pool as soon as `try_send` has copied its bytes into the send ring, so frames are not held while the peer acknowledges. The repository's programs size the pool the same way they size the local frame pool: `tcp::tx_mbuf_budget(&config, mtu)` for one connection, or that times the number of connections plus 64 for the benchmark server.

## Threads and CPUs

`Session::connect` starts one background thread, named `junction-ctl`, that reads messages from the daemon. It starts with the CPU affinity of the thread that called `connect`. So if the application runs its drive loop on a [pinned CPU](../nic/cpus.md), pin the process (with `taskset`, as in the example) before connecting; the helper thread then shares the pinned CPU and sleeps until the daemon sends something.

A `Session`, like everything in switch-net, belongs to the thread that created it. Connect it, wrap it in a `Net`, and drive it on the same thread.

## Teardown

```rust,ignore
{{#include ../../../switch-net/examples/tcp_client_junction.rs:teardown}}
```

The order matches the local program. Shut the runtime down first so every task, socket and frame is dropped. Then `net.into_session()` hands back the session; it returns `Err(net)` while any `NetHandle`, socket half or frame-holding future is still alive.

Then finish the session. `finish()` tells the daemon this application is leaving on purpose. It can return an error when the request cannot be queued yet; nothing was sent in that case, so poll and call it again. After it succeeds, keep polling until `closed()` returns true, which means the daemon has confirmed. Open TCP connections then close with a normal FIN. The deadline guards against a daemon that never answers.

Skipping this step and simply dropping the session counts as a crash: the daemon resets every socket the application still had. [Closing sockets and ending a session](closing.md) has the details, and [Shutting down cleanly](../net/shutdown.md#junction-backend) gives the teardown order for both backends side by side.

## Running it

Start the daemon on the tap config from [Running switch-junctiond](running.md), which gives the daemon the address 10.77.0.2 on interface `jn0`. In the same container, with the daemon on CPU 2 and its control thread on CPU 0:

```sh
sudo taskset -c 2 target/debug/switch-junctiond --config tap.json \
    --dir /tmp/jn --control-cpus 0 &
until [ -S /tmp/jn/control.sock ] && ip link show jn0 >/dev/null 2>&1; do sleep 0.1; done
ip addr add 10.77.0.1/24 dev jn0 && ip link set jn0 up

python3 -c 'import socket; s = socket.create_server(("0.0.0.0", 7000)); c, _ = s.accept(); c.recv(100); c.sendall(b"tick\n"); c.close()' &
taskset -c 3 target/debug/examples/tcp_client_junction /tmp/jn/control.sock 10.77.0.1:7000
```

The client prints the server's reply and then `session finished`:

```text
tick
session finished
```

Stop the daemon with `kill %1` (SIGTERM) when you are done.

## What is different in behavior

The task ran unchanged, but a few things behave differently through the daemon. Socket opens that the daemon refuses fail one control round trip later instead of on the first poll; `room` is a hint, so `try_send` can still return `Full`; UDP send errors after a send was accepted are not reported; and the program can lose the daemon. [Local and junction backends compared](../net/backends.md#the-differences) lists them all, and [Errors](../net/errors.md#when-the-daemon-goes-away) describes what each call returns after the daemon goes away.

Next: [What applications share](sharing.md).
