# Frames

Sockets in switch do not read into or write from your own byte slices. They pass frames: handles to the packet buffers the network card itself uses. Received data reaches a task in the buffer the card wrote it into, and a task writes what it sends into a buffer the card can transmit.

An [`OwnedFrame`](crate::net::OwnedFrame) has one holder and can be written; it dereferences to `&mut [u8]`. An [`ImmutableFrame`](crate::net::ImmutableFrame) is read-only and can be cloned without copying bytes. The buffer goes back to its pool when the last handle to it is dropped.

## Sending

```rust
use switch::net::{ImmutableFrame, NetHandle};

/// A frame holding `bytes`, ready to send.
async fn filled(handle: &NetHandle, bytes: &[u8]) -> ImmutableFrame {
    let mut frame = handle.tx_frame().await;
    frame.as_mut()[..bytes.len()].copy_from_slice(bytes);
    frame.truncate(bytes.len());
    frame.freeze()
}
```

```text
 handle.tx_frame().await     write into it       frame.truncate(n)     frame.freeze()
 ----------------------> OwnedFrame ---------> OwnedFrame ---------> ImmutableFrame
  an empty frame, full                          cut to n bytes         ready to send
  size
```

[`tx_frame`](crate::net::NetHandle::tx_frame) gives an empty frame at its full size, which is the config file's `mtu` less 28 bytes: 1472 for an MTU of 1500. Write your bytes at the front, `truncate` to what you wrote, and `freeze`. The `bytes` here must fit in one frame; longer data goes out as several.

The frames come from a pool of fixed size that all sockets share, sized by `frame_share` in [`Sizing`](crate::nic::Sizing). When the pool is empty, `tx_frame().await` waits until a frame comes back.

The sending calls, `try_send` on TCP and `send` on UDP, take the `ImmutableFrame` and return at once. When one fails it returns the same frame with the error, so nothing you filled is lost and you can send it again.

A send can avoid a copy only when you have not cloned or sliced the frame and it fits in one packet, which for TCP means at most [`max_segment_payload`](crate::net::TcpTx::max_segment_payload) bytes. Any other frame is copied once on its way out, and the reference for that method has the exact rule.

## Receiving

TCP data arrives as a [`TcpFrame`](crate::net::TcpFrame) and a UDP datagram as a [`Datagram`](crate::net::Datagram). Both give the bytes with `payload()`:

```rust
use switch::net::{Datagram, TcpFrame};

fn show(frame: &TcpFrame, datagram: &Datagram) {
    println!("{} bytes of the stream", frame.payload().len());
    println!("{} bytes from {}", datagram.payload().len(), datagram.src());
}
```

`payload_mut()` lets you change the bytes in place. `rx_timestamp()` is the card's hardware timestamp for the packet, or 0 when there is none; `docs/operations/offloads.md` in the repository shows how to turn timestamps on and what unit they are in. `into_frame()` gives up the wrapper and returns the `OwnedFrame`, which you can freeze, clone and hand around.

A received frame does not depend on its socket. It stays valid after the socket is closed.

## What holding a frame costs

A frame you hold is a packet buffer the card cannot reuse. The pools are fixed when the card is opened, so a program that keeps many received frames for a long time leaves the card fewer buffers to receive into. A held datagram also keeps its place in its socket's `rx_queue`.

Read what you need and drop the frame. To keep data for a long time, copy it out. Every frame must be dropped before the card is closed ([Shutting down](crate::guide::shutdown)).

Frames are not `Send`; they stay on the thread that runs the loop.

Next: [TCP streams](crate::guide::tcp).
