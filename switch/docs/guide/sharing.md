# Sharing a network card

A card opened with [`open`](crate::nic::open) belongs to that one process. For several programs to share a card, a daemon, `switch-junctiond`, opens it and runs TCP and UDP for all of them, and each program connects to the daemon.

```text
  program A (CPU 2)       program B (CPU 3)
    tasks, sockets          tasks, sockets
        |                        |
        +------ sessions --------+
                    |
        switch-junctiond (CPU 0)
                    |
               network card
```

The example `tcp_client_junction`, which is the client from [Your first network program](crate::guide::first_connection), keeps its task and drive loop. `main` opens and closes a daemon session.

## What behaves differently

| | Owning the card | Sharing a card |
|---|---|---|
| After `room(len)` resolves | `try_send` of that length does not return `Full` | `try_send` can still return `Full`; keep the frame and await `room` again |
| Backpressure from a slow peer | `room` waits while `tx_capacity` bytes are queued or unacknowledged | The daemon holds the bytes, without a limit; `room` does not wait for the peer |
| UDP `send` to an address that cannot be reached yet | Returns `NoRoute`, `NeighborPending` or `Unaddressable` | Returns `Ok`; the daemon drops the datagram |

The reference for [`Net`](crate::net::Net) lists every difference. Code that should run both ways sends like this:

```rust
use switch::net::{Error, ImmutableFrame, TcpTx};

async fn send(tx: &mut TcpTx, mut frame: ImmutableFrame) -> Result<(), Error> {
    loop {
        tx.room(frame.len()).await?;
        match tx.try_send(frame) {
            Ok(()) => return Ok(()),
            Err((back, Error::Full)) => frame = back,
            Err((_frame, error)) => return Err(error),
        }
    }
}
```

## What changes in the code

The config file, `Sizing` and `open` are replaced by a connection to the daemon:

```rust
use std::path::Path;

use switch::junction::session::Session;
use switch::net::{tcp, Net, RemoteConfig};

const BUFFER: usize = 64 * 1024;
const MAX_SOCKETS: u16 = 1;

fn connect(control: &str) -> Result<(Net, tcp::Config), Box<dyn std::error::Error>> {
    let socket = tcp::Config { rx_capacity: BUFFER, tx_capacity: BUFFER, mss_cap: None };
    let session = Session::connect(Path::new(control), MAX_SOCKETS)
        .map_err(|error| format!("connect {control}: {error}"))?;
    // The daemon's MTU: its frame capacity plus 28 bytes of IP and UDP headers.
    let mtu = session.frame_capacity() as usize + 28;
    let net = Net::remote(session, RemoteConfig { tx_frames: tcp::tx_mbuf_budget(&socket, mtu) });
    Ok((net, socket))
}
```

[`Session::connect`](crate::junction::session::Session::connect) takes the path of the daemon's `control.sock` and the number of sockets, TCP and UDP together, the program wants open at once. [`Net::remote`](crate::net::Net::remote) wraps the resulting [`Session`](crate::junction::session::Session), with `tx_frames` as the size of the program's pool of [send frames](crate::guide::frames).

`mss_cap` is `None`, because the daemon's network stack applies the limits of its own config.

From there the program is the same: `net.handle()`, the task, and the loop calling [`round`](crate::net::round).

At the end, the program takes the session back and finishes it, so the daemon knows it left on purpose:

```rust
use std::time::{Duration, Instant};

use switch::Runtime;
use switch::net::Net;

fn close(mut rt: Runtime, net: Net) -> Result<(), Box<dyn std::error::Error>> {
    rt.shutdown();
    let mut session = net.into_session().map_err(|_net| "a socket or handle outlived the task")?;
    let deadline = Instant::now() + Duration::from_secs(10);
    while !session.closed() {
        // Until the end of the session can be sent, `finish` fails: poll and try again.
        let _ = session.finish();
        session.poll(|_, _| {}, |_, _, _| {}).map_err(|error| format!("finish session: {error}"))?;
        if Instant::now() >= deadline {
            return Err("session finish timed out".into());
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    Ok(())
}
```

`into_session` has the same condition as `into_stack` in [Shutting down](crate::guide::shutdown). `finish` asks the daemon to end the session, and `closed` turns true when the daemon has confirmed it. A session that is dropped without this looks like a crash to the daemon, which then resets the program's open connections.

## Running it

This needs a container with three CPUs, started as in [Setting up for network programs](crate::guide::setup) with `--cpuset-cpus=0-2`. The daemon reads the same config file as a program that owns the card. Here it gets CPU 0 for the network and CPU 1 for its other threads, and the client runs on CPU 2:

```sh
cargo build -p switch-junctiond --bin switch-junctiond
cargo build -p switch-examples --features net --example tcp_client_junction
python3 -c 'import socket; s = socket.create_server(("0.0.0.0", 7000)); c, _ = s.accept(); c.recv(100); c.sendall(b"tick\n"); c.close()' &
taskset -c 0 target/debug/switch-junctiond --config examples/tap.json --dir /tmp/example-junction --control-cpus 1 &
until test -S /tmp/example-junction/control.sock && ip link show cdnic0 >/dev/null 2>&1; do sleep 0.1; done
ip addr add 192.0.2.1/24 dev cdnic0 && ip link set cdnic0 up
taskset -c 2 target/debug/examples/tcp_client_junction /tmp/example-junction/control.sock 192.0.2.1:7000
kill %2
```

```text
EAL: Detected CPU lcores: 14
EAL: Detected NUMA nodes: 1
EAL: Detected shared linkage of DPDK
EAL: Selected IOVA mode 'VA'
tick
session finished
```

The `EAL:` lines are the daemon's. Start the client under `taskset` as shown, because the session starts a helper thread that inherits the CPU affinity the program had when it connected. Running the daemon on a real card, and its flags, are in `docs/operations/junctiond.md` in the repository.

## When the daemon goes away

If the daemon stops or crashes, `round` soon returns `RoundError::Turn(TurnError::HostLost(_))`. Leave the loop, shut the runtime down and exit; the session cannot be resumed.

Next: [A complete program](crate::guide::feed).
