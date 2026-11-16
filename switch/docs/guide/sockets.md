# Opening sockets

A task opens sockets through its [`NetHandle`](crate::net::NetHandle): it connects to a TCP server, accepts a TCP connection, or binds a UDP port. Each call returns a future that resolves to the socket or to an [`Error`](crate::net::Error). Addresses are IPv4.

Every open socket takes one of the places you reserved with `max_sockets` (TCP) or `max_udp_sockets` (UDP) in [`Sizing`](crate::nic::Sizing). When none is free, the open fails with `SocketsFull`.

## Connecting

```rust
use switch::net::{tcp, Error, IpEndpoint, IpListenEndpoint, Ipv4Address, NetHandle, TcpStream};

async fn connect(handle: &NetHandle, socket: tcp::Config) -> Result<TcpStream, Error> {
    let server = IpEndpoint::new(Ipv4Address::new(192, 0, 2, 1).into(), 7000);
    let local = IpListenEndpoint { addr: None, port: 40000 };
    handle.tcp_connect(socket, server, local).await
}
```

`local` is where the connection comes from. `addr: None` means the program's own address, and the port is yours to choose: switch has no automatic port selection, and port 0 is refused with `Unaddressable`. Two connections to the same server need two different local ports.

The future resolves when the TCP handshake completes. A server that answers with a reset gives `Refused`. A server that never answers gives nothing: the connect keeps retrying, so put a [time limit](crate::guide::timeouts) on it. Dropping the future abandons the attempt and frees the socket.

## Accepting

`tcp_accept` listens on a port and resolves to the first connection that arrives. One call accepts one connection. A server that takes many calls it again each time, and does so before it starts serving the connection it just got, so that a listener is always waiting (from the example `tcp_echo_server`):

```rust
use switch::Spawner;
use switch::net::{tcp, Error, IpListenEndpoint, NetHandle, TcpStream};

#[switch::task]
async fn accept_loop(spawner: Spawner, handle: NetHandle, socket: tcp::Config, port: u16) {
    let local = IpListenEndpoint { addr: None, port };
    let mut accepting = handle.tcp_accept(socket, local);
    loop {
        let stream = match accepting.await {
            Ok(stream) => stream,
            Err(error) => {
                eprintln!("accept: {error}");
                return;
            }
        };
        // Listen for the next connection before serving this one.
        accepting = handle.tcp_accept(socket, local);
        spawner.spawn(serve(stream)).unwrap();
    }
}

#[switch::task]
async fn serve(mut stream: TcpStream) {
    loop {
        match stream.recv().await {
            Ok(frame) => print!("{}", String::from_utf8_lossy(frame.payload())),
            Err(Error::Finished) => return,
            Err(error) => {
                eprintln!("recv: {error}");
                return;
            }
        }
    }
}
```

The waiting listener counts as a socket, so a server that serves up to fifteen connections at once reserves sixteen. A connection that arrives while no listener is waiting is refused.

## Binding UDP

```rust
use switch::net::{Error, NetHandle, UdpConfig, UdpSocket};

async fn bind(handle: &NetHandle) -> Result<UdpSocket, Error> {
    let config = UdpConfig {
        port: 9000,
        local_addr: None,
        rx_queue: 64,
        tx_checksum: true,
        broadcast: false,
    };
    handle.udp_bind(config).await
}
```

`port` must not be 0. `local_addr: None` receives on every address the program has. `rx_queue` is how many received datagrams the socket holds for you before it drops new ones. [`UdpConfig`](crate::net::UdpConfig) documents the last two fields.

## What an open can return

| Call | On success | Errors |
|---|---|---|
| `tcp_connect(config, remote, local).await` | [`TcpStream`](crate::net::TcpStream) | `SocketsFull`, `Unaddressable` (port 0, or no usable remote address), `PortInUse` (this local port already has a connection to that server), `Refused`, `Closed` (the attempt ended without a reset) |
| `tcp_accept(config, local).await` | [`TcpStream`](crate::net::TcpStream) | `SocketsFull`, `Unaddressable` |
| `udp_bind(config).await` | [`UdpSocket`](crate::net::UdpSocket) | `SocketsFull`, `PortInUse`, `InvalidConfig` (port 0 or `rx_queue` 0), `Unaddressable` (`local_addr` is not one of the program's addresses) |

[`Error`](crate::net::Error) lists every variant. It implements `Display` and `std::error::Error`, so `?` into a `Box<dyn Error>` works.

All of these futures, and every socket future in the following chapters, must be awaited in a switch task. Awaited on another executor, they panic.

Before using the sockets, the next chapter looks at what they send and receive: [Frames](crate::guide::frames).
