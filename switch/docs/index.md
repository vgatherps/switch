switch is a userspace network stack and async runtime for Rust, built on DPDK. It provides TCP and UDP sockets, a TLS 1.3 client, and WebSocket clients and servers.

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

The program calls [`net::round`](crate::net::round) in a loop on one thread, and each call receives packets, polls the ready tasks, and sends what they queued.

The network features need Linux and DPDK 25.11.3. Without them the crate is the runtime alone and builds anywhere with cargo. Start with the [guide](crate::guide).
