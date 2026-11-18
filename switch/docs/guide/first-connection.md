# Your first network program

The first network program is a TCP client that connects to a server, sends one line, prints what the server sends back, and exits when the server closes the connection.

It needs no spare network card, because it opens a tap device, `cdnic0`. The program has the address 192.0.2.2 on it and the Linux kernel has 192.0.2.1, where a few lines of Python act as the server.

```text
   your program (one CPU)                         Linux side
 +-----------------------------+           +----------------------+
 |  task: connect, send, print |           |  python3 server      |
 |  switch: TCP/IP             |           |  192.0.2.1:7000      |
 |  tap device, 192.0.2.2      | <-------> |  interface cdnic0    |
 +-----------------------------+           +----------------------+
```

## Running it

With the example [built](crate::guide::setup), start the server, start the client, and give the Linux side of the tap device its address once the client has created it:

```sh
python3 -c 'import socket; s = socket.create_server(("0.0.0.0", 7000)); c, _ = s.accept(); c.recv(100); c.sendall(b"tick\n"); c.close()' &
taskset -c 0 target/debug/examples/tcp_client_local examples/tap.json 192.0.2.1:7000 &
until ip link show cdnic0 >/dev/null 2>&1; do sleep 0.1; done
ip addr add 192.0.2.1/24 dev cdnic0 && ip link set cdnic0 up
wait
```

After the card report, the client prints `tick` and `port closed`. `docs/operations/switch-nic.md` in the repository says what each line of the report means.

```text
...
link_status: up
gateway: none
tick
port closed
```

## The program

`Cargo.toml`:

```toml
[package]
name = "tcp-client"
version = "0.1.0"
edition = "2024"

[dependencies]
switch = { path = "<clone>/switch", features = ["net"] }
```

`src/main.rs`, which is the example `tcp_client_local`:

```rust,no_run
use std::cell::Cell;
use std::net::{SocketAddr, ToSocketAddrs};
use std::path::Path;
use std::rc::Rc;
use std::time::{Duration, Instant as WallInstant};

use switch::Runtime;
use switch::net::{tcp, Error, Instant, IpEndpoint, IpListenEndpoint, Net, NetConfig, NetHandle};
use switch::nic::{open, Config, Sizing, DEFAULT_NEIGHBOR_CAPACITY};

const BUFFER: usize = 64 * 1024;
const INGRESS_BUDGET: usize = 32;
const IDLE_SLEEP: Duration = Duration::from_millis(1);
const LIMIT: Duration = Duration::from_secs(30);
const LOCAL_PORT: u16 = 40000;

/// Time since `start`, in the two units the program needs.
struct Clock {
    start: WallInstant,
}

impl Clock {
    /// Network time, in microseconds.
    fn now(&self) -> Instant {
        self.sample().0
    }

    /// The runtime's tick, in nanoseconds.
    fn ns(&self) -> u64 {
        u64::try_from(self.start.elapsed().as_nanos()).expect("elapsed time fits u64 nanoseconds")
    }

    /// Both, from one reading of the clock.
    fn sample(&self) -> (Instant, u64) {
        let nanos = self.ns();
        let micros = i64::try_from(nanos / 1_000).expect("elapsed time fits i64 microseconds");
        (Instant::from_micros(micros), nanos)
    }
}

/// Runs the conversation and leaves its result in `done` for the loop in `main`.
#[switch::task]
async fn hello(
    handle: NetHandle,
    server: IpEndpoint,
    socket: tcp::Config,
    done: Rc<Cell<Option<Result<(), Error>>>>,
) {
    done.set(Some(talk(&handle, server, socket).await));
}

async fn talk(handle: &NetHandle, server: IpEndpoint, socket: tcp::Config) -> Result<(), Error> {
    let local = IpListenEndpoint { addr: None, port: LOCAL_PORT };
    let stream = handle.tcp_connect(socket, server, local).await?;
    let (mut rx, mut tx) = stream.split();

    let line = b"hello\n";
    let mut frame = handle.tx_frame().await;
    frame.as_mut()[..line.len()].copy_from_slice(line);
    frame.truncate(line.len());
    let frame = frame.freeze();
    tx.room(line.len()).await?;
    tx.try_send(frame).map_err(|(_frame, error)| error)?;

    loop {
        match rx.recv().await {
            Ok(frame) => print!("{}", String::from_utf8_lossy(frame.payload())),
            Err(Error::Finished) => break,
            Err(error) => return Err(error),
        }
    }

    tx.shutdown();
    tx.delivered().await;
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let (Some(config), Some(server)) = (args.next(), args.next()) else {
        return Err("usage: tcp_client_local CONFIG HOST:PORT".into());
    };
    let config = Config::load(Path::new(&config))?;
    // Name lookup goes through the kernel and blocks, so do it before opening the card.
    let Some(SocketAddr::V4(server)) = server.to_socket_addrs()?.find(SocketAddr::is_ipv4) else {
        return Err("server has no IPv4 address".into());
    };

    let socket = tcp::Config {
        rx_capacity: BUFFER,
        tx_capacity: BUFFER,
        mss_cap: Some(config.path_mtu(*server.ip()) - 40),
    };
    let sizing = Sizing {
        frame_share: tcp::tx_mbuf_budget(&socket, usize::from(config.mtu)),
        max_sockets: 1,
        max_udp_sockets: 0,
        rx_staging: 32,
        tx_pending: 32,
        neighbor_capacity: DEFAULT_NEIGHBOR_CAPACITY,
    };

    let clock = Clock { start: WallInstant::now() };
    // SAFETY: this thread alone uses the card and everything made from it, and `_eal`
    // lives until the card is closed.
    let opened = unsafe { open(&config, &sizing, None, &mut || clock.now()) }?;
    println!("{}", opened.report);
    let _eal = opened.eal;

    let mut net = Net::new(opened.stack, NetConfig { ingress_budget: INGRESS_BUDGET });
    let mut rt = Runtime::new(clock.ns());
    let done = Rc::new(Cell::new(None));
    let server = IpEndpoint::new((*server.ip()).into(), server.port());
    rt.spawner()
        .spawn(hello(net.handle(), server, socket, Rc::clone(&done)))
        .map_err(|error| format!("spawn: {:?}", error.reason))?;

    let result = loop {
        if let Some(result) = done.take() {
            break result;
        }
        if clock.start.elapsed() > LIMIT {
            return Err(format!("not finished after {LIMIT:?}").into());
        }
        let (now, tick) = clock.sample();
        let moved = switch::net::round(&mut net, &mut rt, now, tick)
            .map_err(|error| format!("round: {error:?}"))?;
        if !moved {
            std::thread::sleep(IDLE_SLEEP);
        }
    };

    rt.shutdown();
    let stack = net.into_stack().map_err(|_net| "a socket or handle outlived the task")?;
    stack.into_port().try_close().map_err(|(_port, error)| format!("port close: {error}"))?;
    println!("port closed");
    result.map_err(|error| format!("connection: {error}").into())
}
```

## The config file

The first argument is a network config file, which describes the card and the program's address on it. `examples/tap.json` creates the tap device `cdnic0` and gives the program 192.0.2.2/24:

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

A real card is described by the same kind of file; `docs/operations/config.md` in the repository explains every key. [`Config::load`](crate::nic::Config::load) reads and checks it.

## Opening the card

`main` resolves the server's name before it opens the card, because `to_socket_addrs` blocks and switch has no DNS resolver of its own. Any other blocking setup also belongs before the loop starts.

[`open`](crate::nic::open) starts DPDK, opens the card the config names, and returns the network stack running on it as `opened.stack`. It fails unless the calling thread's CPU affinity mask contains exactly one CPU, which `taskset -c 0` or the one-CPU container provides. It is `unsafe`, and its reference lists what the caller promises. Its last argument is the clock, which must be the one the loop uses; [The drive loop](crate::guide::drive_loop) covers the two time units that `Clock` produces.

This program owns the card. To share a card through the daemon, see [Sharing a network card](crate::guide::sharing).

[`tcp::Config`](crate::net::tcp::Config) sizes one socket and [`Sizing`](crate::nic::Sizing) the whole program. The reference for each says how to choose its values.

## The Net and its handle

[`Net::new`](crate::net::Net::new) takes the stack. The [`Net`](crate::net::Net) stays with the loop in `main`, and tasks open sockets through a [`NetHandle`](crate::net::NetHandle) from `net.handle()`. `ingress_budget` is the most received packets the network stack processes per round.

## The task

Data is sent and received in [frames](crate::guide::frames), which are packet buffers. `room` waits until the connection can take the bytes, and `try_send` queues the frame without waiting ([TCP streams](crate::guide::tcp)). `recv` returns `Err(Error::Finished)` once the server's FIN has arrived, and `delivered` returns once the server has acknowledged the data and the FIN, or the connection has ended.

A task cannot return a value to `main` directly, so `hello` leaves the result in a shared cell.

## The loop

This loop calls [`round`](crate::net::round) and sleeps a millisecond after a round that did nothing, because the Python server needs the same CPU. [The drive loop](crate::guide::drive_loop) covers both, and [Shutting down](crate::guide::shutdown) the order in which the program ends.

Next: [Tasks](crate::guide::tasks).
