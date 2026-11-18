# TLS without WebSocket

For a protocol of your own over TLS 1.3, [`tls_connect`](crate::tls::net::tls_connect) turns a connected [`TcpStream`](crate::net::TcpStream) into an encrypted stream of bytes. The client config is built as for [WebSocket](crate::guide::websocket), and the feature is `tls` in place of `ws`.

The example `tls_client` sends one line and prints the reply:

```rust
use std::sync::Arc;

use switch::tls::rustls::ClientConfig;
use switch::net::{tcp, IpEndpoint, IpListenEndpoint, NetHandle};
use switch::tls::net::{send_bytes, tls_connect};

async fn request(
    handle: &NetHandle,
    server: IpEndpoint,
    socket: tcp::Config,
    config: Arc<ClientConfig>,
    host: &str,
) -> Result<(), String> {
    let local = IpListenEndpoint { addr: None, port: 40000 };
    let stream = handle
        .tcp_connect(socket, server, local)
        .await
        .map_err(|error| error.to_string())?;
    let mut tls = tls_connect(handle, stream, config, host, 6)
        .await
        .map_err(|error| error.to_string())?;
    send_bytes(&mut tls.tx, b"hello\n")
        .await
        .map_err(|error| error.to_string())?;
    while tls.ready().await.map_err(|error| error.to_string())? {
        let bytes = tls.rx.current();
        print!("{}", String::from_utf8_lossy(bytes));
        let end = bytes.contains(&b'\n');
        let len = bytes.len();
        tls.rx.consume(len);
        if end {
            break;
        }
    }
    tls.close().await.map_err(|error| error.to_string())?;
    println!("TLS closed");
    Ok(())
}
```

## Connecting

`tls_connect(handle, stream, config, host, min_plain)` runs the handshake, with `host` as the SNI name and the name the server's certificate is verified against, and returns a [`Tls`](crate::tls::net::Tls) with a sending half `tx` and a receiving half `rx`. `min_plain` is the largest piece the program will send as a single TLS record; a program that sends only through `send_bytes` can pass 1.

A server that negotiates anything other than TLS 1.3 is refused with `Error::Handshake`. A certificate that does not verify is `Error::Tls`. [`Error`](crate::tls::net::Error) lists the other ways the handshake can fail.

## Writing and reading

[`send_bytes(&mut tls.tx, bytes)`](crate::tls::net::send_bytes) encrypts and sends `bytes`, waiting for room on the connection as it goes, and returns when the last record has been queued on the TCP connection.

`tls.ready().await` waits until decrypted bytes are available and returns `Ok(true)`, or `Ok(false)` when the server has sent `close_notify`. `tls.rx.current()` borrows those bytes from the connection, and `tls.rx.consume(n)` marks the first `n` as read. Unconsumed bytes are returned again by the next `ready`, but they are not joined with what arrives later, so a message that spans several pieces must be collected in the program's own buffer.

A connection that ends without the server's `close_notify` is an error from `ready`.

## Closing

`tls.close().await` sends `close_notify` followed by a FIN, and waits until the server has acknowledged both or the connection has ended.

`tls_connect`, `ready` and `close` each wait on the server, so put them inside [`within`](crate::guide::timeouts).

## Running the example

The peer here is `openssl s_server -rev`, which sends each line back reversed, with a self-signed certificate. In the container:

```sh
cargo build -p switch-examples --features tls --example tls_client
openssl req -x509 -newkey rsa:2048 -nodes -keyout /tmp/switch-key.pem -out /tmp/switch-cert.pem -days 1 \
    -subj /CN=localhost -addext subjectAltName=DNS:localhost -addext basicConstraints=critical,CA:FALSE
openssl s_server -accept 7000 -cert /tmp/switch-cert.pem -key /tmp/switch-key.pem -tls1_3 -rev -quiet &
taskset -c 0 target/debug/examples/tls_client examples/tap.json localhost 7000 /tmp/switch-cert.pem 192.0.2.1 &
until ip link show cdnic0 >/dev/null 2>&1; do sleep 0.1; done
ip addr add 192.0.2.1/24 dev cdnic0 && ip link set cdnic0 up
wait %2
kill %1
```

The arguments are the config file, the name in the certificate, the port, the certificate to trust, and the server's address. The client prints the reply and then `TLS closed`. The server prints lines of its own about the connection, among them `CONNECTION CLOSED`, whose place among the client's lines can vary:

```text
olleh
CONNECTION CLOSED
TLS closed
```

Next: [Sharing a network card](crate::guide::sharing).
