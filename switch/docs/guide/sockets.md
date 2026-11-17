# Opening sockets

A task opens sockets through its [`NetHandle`](crate::net::NetHandle). Each call returns a future that resolves to the socket or to an [`Error`](crate::net::Error). Addresses are IPv4.

Every open socket takes one of the places reserved with `max_sockets` (TCP) or `max_udp_sockets` (UDP) in [`Sizing`](crate::nic::Sizing), and an open fails with `SocketsFull` when none is free.

## Connecting

```rust
use switch::net::{tcp, Error, IpEndpoint, IpListenEndpoint, Ipv4Address, NetHandle, TcpStream};

async fn connect(handle: &NetHandle, socket: tcp::Config) -> Result<TcpStream, Error> {
    let server = IpEndpoint::new(Ipv4Address::new(192, 0, 2, 1).into(), 7000);
    let local = IpListenEndpoint { addr: None, port: 40000 };
    handle.tcp_connect(socket, server, local).await
}
```

`addr: None` means the program's own address. switch does not allocate ephemeral ports, so the program chooses the local port. Port 0 is refused with `Unaddressable`, and a 4-tuple that is already in use with `PortInUse`.

The future resolves when the TCP handshake completes, or to `Refused` if the server answers the SYN with an RST. If the server never answers, the SYN is retransmitted without limit, so put a [time limit](crate::guide::timeouts) on the connect. Dropping the future frees the socket.

## Accepting

One `tcp_accept` call accepts one connection. A server calls it again before it serves each connection, so that a listener is always waiting (from the example `tcp_echo_server`):

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

The waiting listener counts as a socket in `max_sockets`. There is no accept backlog, so a SYN that arrives while no listener is waiting is answered with an RST.

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

`port` must not be 0, and `rx_queue` is the number of received datagrams the socket holds before it drops new ones. The reference for [`UdpConfig`](crate::net::UdpConfig) covers the other fields.

## Errors

The reference for each call on [`NetHandle`](crate::net::NetHandle) lists what it can return, and [`Error`](crate::net::Error) describes every variant.

Socket futures, here and in the following chapters, must be awaited in switch tasks. On another executor, a socket future that has to wait panics.

Next: [Frames](crate::guide::frames).
