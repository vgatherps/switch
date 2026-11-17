switch is an async userspace network stack for Rust, built on DPDK, for programs that need low, predictable latency such as market-data feeds and order gateways. It provides TCP and UDP sockets, a TLS 1.3 client, and WebSocket clients and servers.

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

The program calls [`net::round`](crate::net::round) in a loop on one thread, and each call receives packets, polls the tasks that are ready, and sends what they queued.

The network features need Linux and DPDK 25.11.3 to build. Without them the crate is the runtime alone, tasks and timers, and builds anywhere with cargo. The [guide](crate::guide) starts there.
