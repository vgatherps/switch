# Connecting an application through switch-net

[Your first network program](../getting-started/first-connection.md) opened a NIC and ran one task that connected to a server, sent a line and printed the reply. This page runs the same task through the daemon from [Running switch-junctiond](running.md), started on a tap device in [Running it](#running-it) below. The task does not change at all. Only the code before the drive loop and after it is different.

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

The session reports two sizes the daemon chose. `frame_capacity()` is the largest UDP payload that fits the daemon's MTU (MTU − 28); switch-net makes its send frames this size. `max_record()` is the largest single send the session accepts, header included. The example turns the frame capacity back into an MTU to size its frame pool.

`Net::remote` wraps the session in a `Net`. From here on the program uses `net.handle()`, `switch_net::round` and its tasks exactly as before.

When the connect fails, the error says why:

| Error | Meaning |
|---|---|
| `ConnectError::Io(..)` | the socket could not be reached (wrong path, no daemon, permissions), the daemon closed the connection before answering (for example because it exited during startup), or the ring files could not be opened |
| `ConnectError::Rejected(BadMaxSockets)` | `max_sockets` was 0 or above the daemon's `--max-sockets` |
| `ConnectError::Rejected(SessionsFull)` | all `--max-sessions` sessions are in use. This can last a moment after another session ends, while the daemon releases its resources; retry |
| `ConnectError::Rejected(HostResources)` | the daemon could not create this session's receive ring |
| `ConnectError::Version { host }` | the daemon speaks a different protocol version. The application and the daemon must be built from the same version of `switch-junction`; versions must match exactly |
| `ConnectError::Protocol(..)` | the daemon's reply could not be decoded |

## Choosing `max_sockets` and `tx_frames`

`max_sockets` is the size of this session's socket table. Every TCP connection, every listener and every UDP socket the application has open at once takes one entry, and an entry is reused only after the daemon has finished with the old socket. If the table is full, opening another socket fails with `Error::SocketsFull` on the first poll. `switch-bench` sets it to the number of sockets it opens (one per connection plus one per listener).

`RemoteConfig.tx_frames` is the number of send frames in the application's own frame pool. Each is `frame_capacity` bytes. A task's `handle.tx_frame().await` takes one from this pool and waits while the pool is empty. On the junction backend, once `try_send` has copied a frame's bytes into the send ring, the frame you passed returns to your pool, unless you kept a clone or slice of it; frames are not held while the peer acknowledges. This example sizes the pool with `tcp::tx_mbuf_budget(&config, mtu)` for its one connection; `switch-bench` uses that times the number of connections, plus 64.

## Threads and CPUs

`Session::connect` starts one background thread, named `junction-ctl`, that reads messages from the daemon. It starts with the CPU affinity of the thread that called `connect`. So if the application runs its drive loop on a [pinned CPU](../nic/cpus.md), pin the process (with `taskset`, as in the example) before connecting; the helper thread then shares the pinned CPU and sleeps until the daemon sends something.

A `Session`, like everything in switch-net, belongs to the thread that created it. Connect it, wrap it in a `Net`, and drive it on the same thread.

## Teardown

```rust,ignore
{{#include ../../../switch-net/examples/tcp_client_junction.rs:teardown}}
```

The order matches the local program. Shut the runtime down first so every task, socket and frame is dropped. Then `net.into_session()` hands back the session; it returns `Err(net)` while any `NetHandle`, socket half or frame-holding future is still alive.

Then finish the session. `finish()` tells the daemon this application is leaving on purpose. It can return an error when the request cannot be queued yet; the end request was not queued in that case, so poll and call it again. After it succeeds, keep polling until `closed()` returns true, which means the daemon has confirmed. Open TCP connections then close with a normal FIN. The deadline guards against a daemon that never answers.

Skipping this step and simply dropping the session counts as a crash: the daemon resets every socket the application still had. [Closing sockets and ending a session](closing.md) has the details, and [Shutting down cleanly](../net/shutdown.md#junction-backend) gives the teardown order for both backends side by side.

## Running it

The daemon needs CPUs of its own, so this walkthrough uses a container with four CPUs, 0 to 3: the daemon's control thread on CPU 0, its network thread on CPU 2 and the client on CPU 3. Start it like the container in [Setting up a build environment](../getting-started/environment.md), with `--cpuset-cpus=0-3`:

```sh
docker run --rm -it --init \
    --cap-add NET_ADMIN --device /dev/net/tun \
    --cpuset-cpus=0-3 \
    -v "$PWD:/work" -w /work \
    -v switch-cargo-registry:/usr/local/cargo/registry \
    -v switch-linux-target:/work/target \
    switch-dpdk-test:local bash
```

The container's shell runs as root, which the daemon and the tap device need. Save the tap config from [Running switch-junctiond](running.md) as `jn0.json` in `/work`; it gives the daemon the address 10.77.0.2 on interface `jn0`. Then build both programs, start the daemon, give the kernel side of `jn0` its address, start a one-shot server on the kernel side (the same one as in [Your first network program](../getting-started/first-connection.md)), and run the client:

```sh
cargo build --locked --release -p switch-junctiond --bins
cargo build --locked --release -p switch-net --example tcp_client_junction

taskset -c 2 target/release/switch-junctiond --config jn0.json --dir /tmp/jn --control-cpus 0 &
until [ -S /tmp/jn/control.sock ] && ip link show jn0 >/dev/null 2>&1; do sleep 0.1; done
ip addr add 10.77.0.1/24 dev jn0 && ip link set jn0 up

python3 -c 'import socket; s = socket.create_server(("0.0.0.0", 7000)); c, _ = s.accept(); c.recv(100); c.sendall(b"tick\n"); c.close()' &
taskset -c 3 target/release/examples/tcp_client_junction /tmp/jn/control.sock 10.77.0.1:7000
```

The client prints the server's reply and then `session finished`:

```text
tick
session finished
```

Running the client again within about 10 seconds against the same server fails with `PortInUse`. The client closed the connection first, so the daemon's Stack keeps it, with local port 40000, in TCP's TIME-WAIT state for 10 seconds, and the new connection would use the same addresses and ports.

## Two applications at once

The daemon exists so that several programs can use one NIC. With the daemon still running, and at least 10 seconds after the first run (for the reason just given), start two one-shot servers and two clients, the second on CPU 1:

```sh
python3 -c 'import socket; s = socket.create_server(("0.0.0.0", 7000)); c, _ = s.accept(); c.recv(100); c.sendall(b"tick\n"); c.close()' &
python3 -c 'import socket; s = socket.create_server(("0.0.0.0", 7001)); c, _ = s.accept(); c.recv(100); c.sendall(b"tock\n"); c.close()' &
taskset -c 3 target/release/examples/tcp_client_junction /tmp/jn/control.sock 10.77.0.1:7000 &
first=$!
taskset -c 1 target/release/examples/tcp_client_junction /tmp/jn/control.sock 10.77.0.1:7001
wait $first
```

Each client prints its own reply and `session finished`. They are two sessions on one daemon, and because they run on different CPUs they write into different send rings. Both use local port 40000, which the example fixes, and the daemon has only one set of ports for all its applications; they can run side by side because they connect to different server ports. [What applications share](sharing.md#addresses-and-ports) explains this.

Stop the daemon with `kill %1` (SIGTERM) when you are done.

## What is different in behavior

The task ran unchanged, but a few things behave differently through the daemon. Socket opens that the daemon refuses fail one control round trip later instead of on the first poll; `room` is a hint, so `try_send` can still return `Full`; UDP send errors after a send was accepted are not reported; and the program can lose the daemon. [Local and junction backends compared](../net/backends.md#the-differences) lists them all, and [Errors](../net/errors.md#when-the-daemon-goes-away) describes what each call returns after the daemon goes away.

Next: [What applications share](sharing.md).
