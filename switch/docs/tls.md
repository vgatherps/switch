A TLS 1.3 client over a TCP stream, for your own protocol on top. For `wss://`, use [`switch::ws`](crate::ws), which runs this module underneath.

The functions a program starts from are in [`net`]:

- [`net::tls_connect`]: runs the handshake on a connected [`TcpStream`](crate::net::TcpStream) and returns a [`net::Tls`].
- [`net::Tls`]: the open session. `ready` waits for decrypted bytes, which are read through its `rx` half with `current` and `consume`; `close` ends the session.
- [`net::send_bytes`]: encrypts and sends bytes on the session's `tx` half.
- [`net::Error`]: what can fail.

The client config is a `ClientConfig` from `rustls`, which this module re-exports.

```rust,no_run
use std::sync::Arc;

use switch::tls::rustls::ClientConfig;
use switch::net::{NetHandle, TcpStream};
use switch::tls::net::{send_bytes, tls_connect, Error};

async fn ping(handle: &NetHandle, stream: TcpStream, config: Arc<ClientConfig>) -> Result<(), Error> {
    let mut tls = tls_connect(handle, stream, config, "example.com", 1).await?;
    send_bytes(&mut tls.tx, b"ping\n").await.map_err(Error::Send)?;
    while tls.ready().await? {
        let len = tls.rx.current().len();
        tls.rx.consume(len);
    }
    tls.close().await
}
```

[TLS without WebSocket](crate::guide::tls) explains each call and runs a client against a test server.
