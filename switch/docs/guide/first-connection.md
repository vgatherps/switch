# Your first network program

The first network program is a TCP client. It connects to a server, sends one line, prints what the server sends back, and exits when the server closes the connection.

It needs no spare network card. The program opens a tap device: a virtual card that also appears in Linux as an ordinary interface, here `cdnic0`. Your program is on one side of it with the address 192.0.2.2. The Linux kernel is on the other side with 192.0.2.1, and a few lines of Python there play the server.

```text
   your program (one CPU)                        Linux kernel
 +-----------------------------+           +----------------------+
 |  task: connect, send, print |           |  python3 server      |
 |  switch: TCP/IP             |           |  192.0.2.1:7000      |
 |  tap device, 192.0.2.2      | <-------> |  interface cdnic0    |
 +-----------------------------+           +----------------------+
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

## Running it

In the container from [the previous chapter](crate::guide::setup), build the example, then start the server, start the client, and give the Linux side of the tap device its address once the client has created it:

```sh
cargo build -p switch-examples --features net --example tcp_client_local
python3 -c 'import socket; s = socket.create_server(("0.0.0.0", 7000)); c, _ = s.accept(); c.recv(100); c.sendall(b"tick\n"); c.close()' &
taskset -c 0 target/debug/examples/tcp_client_local examples/tap.json 192.0.2.1:7000 &
until ip link show cdnic0 >/dev/null 2>&1; do sleep 0.1; done
ip addr add 192.0.2.1/24 dev cdnic0 && ip link set cdnic0 up
wait
```

The client prints DPDK's startup lines, a report on the card it opened, the server's reply, and a last line once the card is closed:

```text
EAL: Detected CPU lcores: 14
EAL: Detected NUMA nodes: 1
EAL: Detected shared linkage of DPDK
EAL: Selected IOVA mode 'VA'
driver: net_tap
mac: be:07:75:ea:11:c1
cpu: 0
port_id: 0
rx_offload_capa: 0x000000000000200e
tx_offload_capa: 0x000000000000802e
max_rx_pktlen: 16128
min_mtu: 46
max_mtu: 65535
descriptors: rx=1024 tx=1024
rx_pool_count: 32767
tx_pool_count: 1056
rx_checksum: not requested (explicit)
tx_tcp_checksum: not requested (explicit)
tx_udp_checksum: not requested (explicit)
link_speed_mbps: 10000
link_duplex: full
link_status: up
gateway: none
tick
port closed
```

## The config file

The first argument is a network config file, a JSON file that describes the card and the program's address on it. `examples/tap.json` asks DPDK to create a tap device named `cdnic0` and gives the program 192.0.2.2/24:

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

[`open`](crate::nic::open) starts DPDK, opens the card the config names, and returns the network stack running on it, `opened.stack`. It fails unless the calling thread is allowed on exactly one CPU, which is what `taskset -c 0` arranges. `open` is `unsafe` because DPDK and the card write to memory the compiler cannot check; the comment above the call states what the program promises. Keep `opened.eal` alive until the card is closed.

`open` takes a clock because it needs the time while it starts up. Pass the clock the loop will use.

What the program will need is described by [`tcp::Config`](crate::net::tcp::Config) for each socket and [`Sizing`](crate::nic::Sizing) for the whole program. In `tcp::Config`, `rx_capacity` is the receive window the socket advertises and `tx_capacity` is how many bytes it holds that are queued or not yet acknowledged. `mss_cap` limits the payload of one TCP packet; the largest packet the config allows towards the server, less 40 bytes of IP and TCP headers, is the right value. In `Sizing`, set `max_sockets` and `max_udp_sockets` to the number of TCP and UDP sockets you will have open at once, and `frame_share` to `tcp::tx_mbuf_budget` for one socket times the number of TCP sockets. The other three values can stay as they are here; the reference for `Sizing` says what each one sizes.

## The Net and its handle

[`Net::new`](crate::net::Net::new) takes the stack. The [`Net`](crate::net::Net) belongs to the loop in `main` and is the only thing that moves packets. Tasks get a [`NetHandle`](crate::net::NetHandle) from `net.handle()`, which opens sockets and can be cloned. `ingress_budget` is the most packets the program takes in each time round the loop.

## The task

`talk` is the conversation. `tcp_connect` names the server and a local port, which you choose: switch does not pick one for you. When the connection is up, `split` gives a receiving half and a sending half.

Data is sent and received in frames, which are packet buffers. The task takes an empty frame from the handle, copies its line in, cuts the frame to that length and freezes it. `room` waits until the connection can take that many bytes, and `try_send` queues the frame. [Frames](crate::guide::frames) and [TCP streams](crate::guide::tcp) cover each call.

`recv` returns the next data from the server, in order, and `Err(Error::Finished)` once the server has closed its side. The task then closes its own side with `shutdown` and waits in `delivered` until the server has acknowledged everything.

A task cannot return a value to `main` directly, so `hello` leaves the result in a shared cell.

## The loop

The loop in `main` reads the clock and calls [`round`](crate::net::round), which receives packets, runs the tasks and sends what they queued. `round` returns whether anything happened. When nothing did, this loop sleeps a millisecond, because the Python server needs the same CPU. [The drive loop](crate::guide::drive_loop) explains the round and when to sleep.

When the task has left its result, the program takes itself apart in order: the runtime with its tasks, then the `Net`, then the card. [Shutting down](crate::guide::shutdown) explains the order.

Next: [Tasks](crate::guide::tasks).
