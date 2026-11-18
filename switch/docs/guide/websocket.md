# WebSocket

With the `ws` feature, a task can run WebSocket over a TCP stream as a client of `wss://` (over TLS 1.3) or `ws://` servers, and as a `ws://` server. A connection is a [`Wss`](crate::ws::net::Wss) or a [`Ws`](crate::ws::net::Ws). Both have the same `recv`, `send` and `close`, and each is used from one task.

```toml
[dependencies]
switch = { path = "<clone>/switch", features = ["ws"] }
```

## The TLS client config

A `wss://` connection needs a `rustls::ClientConfig` holding the trust roots, built with the rustls that switch re-exports as `switch::tls::rustls`. Build it in `main` before the card is opened:

```rust
use std::sync::Arc;

use switch::tls::rustls::pki_types::{CertificateDer, pem::PemObject};
use switch::tls::rustls::{self, ClientConfig, RootCertStore};

fn tls_config(ca_file: &str) -> Result<Arc<ClientConfig>, Box<dyn std::error::Error>> {
    let mut roots = RootCertStore::empty();
    for cert in CertificateDer::pem_file_iter(ca_file)? {
        roots.add(cert?)?;
    }
    let config = ClientConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
        .with_root_certificates(roots)
        .with_no_client_auth();
    Ok(Arc::new(config))
}
```

On Ubuntu the system's root certificates are in `/etc/ssl/certs/ca-certificates.crt`.

## Connecting

Open the TCP connection as usual, then hand the stream to [`connect`](crate::ws::net::connect), which runs the TLS handshake and the WebSocket upgrade:

```rust
use std::sync::Arc;
use std::time::Duration;

use switch::tls::rustls::ClientConfig;
use switch::Spawner;
use switch::net::{tcp, within, IpEndpoint, IpListenEndpoint, NetHandle};
use switch::ws::Limits;
use switch::ws::net::Wss;

async fn open(
    handle: &NetHandle,
    spawner: &Spawner,
    socket: tcp::Config,
    server: IpEndpoint,
    config: Arc<ClientConfig>,
) -> Result<Wss, String> {
    let connecting = async {
        let local = IpListenEndpoint { addr: None, port: 40000 };
        let stream = handle
            .tcp_connect(socket, server, local)
            .await
            .map_err(|error| error.to_string())?;
        let limits = Limits { max_message: 64 * 1024, max_send: 4096 };
        switch::ws::net::connect(handle, stream, config, "stream.example.com", server.port, "/ws", limits)
            .await
            .map_err(|error| error.to_string())
    };
    within(spawner, Duration::from_secs(10), connecting)
        .await
        .ok_or("connect timed out")?
}
```

The host name is used for SNI, certificate verification and the `Host` header. The TCP connect, the TLS handshake and the upgrade each wait on the server, so the whole block goes inside one [`within`](crate::guide::timeouts).

For a `ws://` server, [`connect_ws`](crate::ws::net::connect_ws) takes the same arguments without the config and returns a `Ws`.

A received message larger than `max_message` in [`Limits`](crate::ws::Limits) fails the connection, and a `send` larger than `max_send` is refused. `Limits::default()` allows 64 MiB each way.

## Receiving and sending

```rust
use switch::ws::Kind;
use switch::ws::net::{Error, Wss};

async fn talk(ws: &mut Wss) -> Result<(), Error> {
    ws.send(Kind::Text, b"{\"method\":\"SUBSCRIBE\"}").await?;
    while let Some(message) = ws.recv().await? {
        match message.text() {
            Some(text) => println!("{text}"),
            None => println!("{} binary bytes", message.bytes().len()),
        }
    }
    Ok(())
}
```

`recv` resolves to the next complete [`Message`](crate::ws::Message), or to `None` when the connection has been closed. `text()` is `Some` for a text message, and `bytes()` is the payload of either kind.

`send` sends one message of the given [`Kind`](crate::ws::Kind) and returns when all of it has been handed to TCP.

The connection does its work only inside these calls. The pong for a peer's ping is queued only by `recv`, so a task that mostly sends must still call `recv` regularly.

## Closing

To close, send a WebSocket close frame and keep receiving until `recv` returns `None`. Messages the peer sent before it saw the close still arrive in between.

```rust
use std::time::Duration;

use switch::Spawner;
use switch::net::within;
use switch::ws::net::Wss;

async fn close(spawner: &Spawner, ws: &mut Wss) -> Result<(), String> {
    let closing = async {
        ws.close(1000, "done").await.map_err(|error| error.to_string())?;
        while ws.recv().await.map_err(|error| error.to_string())?.is_some() {}
        Ok::<(), String>(())
    };
    within(spawner, Duration::from_secs(10), closing)
        .await
        .ok_or("close timed out")?
}
```

Once `recv` has returned `None`, the `Wss` can be dropped. The limit is there because a peer that never answers the close would keep `recv` waiting.

When the peer closes first, `recv` answers its close and returns `None` in the same way. `ws.peer_close()` then gives the peer's code and reason.

## Failures

`Error::Failed` reports a connection failure, described by the [`Failure`](crate::ws::net::Failure) inside. After a failure, drop the connection. To end without resetting a peer that may still be sending, call [`linger`](crate::ws::net::Wss::linger) first, inside a `within`.

An oversized outgoing message returns `Error::TooBig` and leaves the connection usable. [`Error`](crate::ws::net::Error) lists the other variants.

## Serving ws://

A server accepts TCP connections as in [Opening sockets](crate::guide::sockets) and upgrades each with [`accept_ws`](crate::ws::net::accept_ws). This task, from the example `ws_echo_server`, echoes every message:

```rust
use switch::net::{NetHandle, TcpStream};
use switch::ws::Limits;

#[switch::task]
async fn echo(handle: NetHandle, stream: TcpStream) {
    let limits = Limits {
        max_message: 64 * 1024,
        max_send: 64 * 1024,
    };
    let mut ws = switch::ws::net::accept_ws(&handle, stream, limits)
        .await
        .expect("upgrade WebSocket");
    while let Some((message, mut sender)) = ws.recv_with_sender().await.expect("receive WebSocket")
    {
        sender
            .send(message.kind(), message.bytes())
            .await
            .expect("echo WebSocket");
    }
    println!("WebSocket closed");
}
```

`recv_with_sender` is `recv` that also lends a sender, so the reply can be written straight from the received message without copying it out first. It exists on `Ws` only.

Run the server on the tap device:

```sh
cargo build -p switch-examples --features ws --example ws_echo_server
taskset -c 0 target/debug/examples/ws_echo_server examples/tap.json 7000 &
until ip link show cdnic0 >/dev/null 2>&1; do sleep 0.1; done
ip addr add 192.0.2.1/24 dev cdnic0 && ip link set cdnic0 up
```

The image has no WebSocket client, but any client run on the Linux side can connect to `ws://192.0.2.2:7000/`. When it closes with code 1000, the server prints the line below. `kill %1` stops the server.

```text
WebSocket closed
```

Next: [TLS without WebSocket](crate::guide::tls).
