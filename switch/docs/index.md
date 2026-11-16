switch is a Rust library for network programs that need low, predictable latency, such as market-data feeds and order gateways. It is a userspace network stack: your program sends and receives packets through the network card directly, without going through the Linux kernel.

You write the program as async tasks. A task opens TCP or UDP sockets and awaits them. TLS 1.3 and WebSocket run on top of TCP.

```rust
# #[cfg(feature = "net")] mod example {
use switch::net::{tcp, Error, IpEndpoint, IpListenEndpoint, NetHandle};

/// Connects to `server` and prints what it sends until it closes its side.
#[switch::task]
async fn print_stream(handle: NetHandle, server: IpEndpoint, socket: tcp::Config) -> Result<(), Error> {
    let local = IpListenEndpoint { addr: None, port: 40000 };
    let mut stream = handle.tcp_connect(socket, server, local).await?;
    loop {
        match stream.recv().await {
            Ok(frame) => print!("{}", String::from_utf8_lossy(frame.payload())),
            Err(Error::Finished) => return Ok(()),
            Err(error) => return Err(error),
        }
    }
}
# }
# fn main() {}
```

Your program runs one loop on one CPU. Each time round, the loop receives packets, runs the tasks that were waiting for them, and sends what those tasks wrote. One call, [`net::round`](crate::net::round), does all three.

The network parts need Linux with DPDK 25.11.3 to build; tasks and timers alone build anywhere with cargo.

Start with the [guide](crate::guide).
