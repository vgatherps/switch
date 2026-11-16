# UDP sockets

A [`UdpSocket`](crate::net::UdpSocket) sends and receives datagrams on the port it was [bound](crate::guide::sockets) to. Like a TCP stream it splits into a receiving half, [`UdpRx`](crate::net::UdpRx), and a sending half, [`UdpTx`](crate::net::UdpTx). The example `udp_echo` sends every datagram back to where it came from:

```rust
use switch::net::{Error, NetHandle, UdpConfig};

#[switch::task]
async fn echo(handle: NetHandle, port: u16) {
    let socket = handle
        .udp_bind(UdpConfig {
            port,
            local_addr: None,
            rx_queue: 32,
            tx_checksum: true,
            broadcast: false,
        })
        .await
        .expect("bind UDP");
    let (mut rx, mut tx) = socket.split();
    loop {
        let received = rx.recv().await;
        let mut frame = handle.tx_frame().await;
        let len = received.payload().len();
        frame.as_mut()[..len].copy_from_slice(received.payload());
        frame.truncate(len);
        let mut frame = frame.freeze();
        loop {
            match tx.send(Some(received.src()), frame) {
                Ok(()) => break,
                Err((returned, Error::Full | Error::NeighborPending)) => {
                    frame = returned;
                    switch::yield_now().await;
                }
                Err((_, error)) => panic!("send UDP: {error}"),
            }
        }
    }
}
```

## Receiving

`recv` resolves to the next [`Datagram`](crate::net::Datagram) and has no error: when nothing has arrived, it waits. Besides its `payload()`, a datagram knows the sender, `src()`, and the address it was sent to, `dst_addr()`.

The socket holds up to `rx_queue` datagrams. One that arrives when the queue is full is dropped, and `tx.stats().rx_dropped_full` counts those. A datagram your task still holds counts as queued, so drop each one when you are done with it.

## Sending

`send(Some(to), frame)` sends one [frame](crate::guide::frames) as one datagram. It does not wait: the datagram is queued and leaves at the end of the round, or `send` returns the frame with an error. The destination is always given; `None` returns `NoDestination`. A frame is at most the config's `mtu` less 28 bytes, and so is a datagram.

`Full` and `NeighborPending` pass by themselves, and the echo retries them after a [`yield_now`](crate::guide::tasks), which lets the loop go round once:

- `Full`: the card has not yet taken the packets already queued. The next round's send makes room.
- `NeighborPending`: the hardware address of the destination is not known yet. switch has asked for it, and the answer arrives in a later round. This is normal for the first datagram to a new address.

The others will not pass by waiting: `NoRoute` and `Unaddressable` for a destination the program cannot send to, and `TooLarge`.

## Running the echo

In the container, start the echo on the tap device and send it a datagram from the Linux side:

```sh
cargo build -p switch-examples --features net --example udp_echo
taskset -c 0 target/debug/examples/udp_echo examples/tap.json 7000 &
until ip link show cdnic0 >/dev/null 2>&1; do sleep 0.1; done
ip addr add 192.0.2.1/24 dev cdnic0 && ip link set cdnic0 up
python3 -c 'import socket; s=socket.socket(socket.AF_INET,socket.SOCK_DGRAM); s.settimeout(5); s.sendto(b"hello",("192.0.2.2",7000)); data,_=s.recvfrom(1500); assert data==b"hello"; print("echo:",data.decode())'
kill %1
```

The echo prints the report on the card it opened, as the TCP client did, and the Python side prints:

```text
echo: hello
```

Next: [Timeouts and cancelling](crate::guide::timeouts).
