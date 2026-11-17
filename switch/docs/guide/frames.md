# Frames

Sockets send and receive frames, which are handles to the packet buffers the network card itself uses. Received data reaches a task in the buffer the card wrote it into, and a task writes what it sends into a buffer the card can transmit.

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

[`tx_frame`](crate::net::NetHandle::tx_frame) gives a blank frame of the largest size, which is the config file's `mtu` less 28 bytes, or 1472 for an MTU of 1500. The `bytes` here must fit in one frame; longer data goes out as several.

The frames come from a pool of fixed size that all sockets share, sized by `frame_share` in [`Sizing`](crate::nic::Sizing). When the pool is empty, `tx_frame().await` waits until a frame comes back.

The sending calls, `try_send` on TCP and `send` on UDP, take the `ImmutableFrame` and return at once. A call that fails returns the same frame with the error, so the frame can be sent again.

A frame that you alone hold, that still starts where `tx_frame` gave it, and that fits in one packet can go to the card without being copied. Other frames are copied on the way out.

## Receiving

TCP data arrives as a [`TcpFrame`](crate::net::TcpFrame) and a UDP datagram as a [`Datagram`](crate::net::Datagram). Both give the bytes with `payload()`:

```rust
use switch::net::{Datagram, TcpFrame};

fn show(frame: &TcpFrame, datagram: &Datagram) {
    println!("{} bytes of the stream", frame.payload().len());
    println!("{} bytes from {}", datagram.payload().len(), datagram.src());
}
```

`rx_timestamp()` is the card's hardware timestamp for the packet, or 0 when there is none; `docs/operations/offloads.md` in the repository shows how to turn timestamps on and what unit they are in.

## What holding a frame costs

A held frame is a packet buffer the card cannot reuse. The pools are fixed when the card is opened, so a program that keeps many received frames for a long time leaves the card fewer buffers to receive into. Drop a frame once it has been read, and copy out any data that must be kept for a long time. Every frame must be dropped before the card is closed ([Shutting down](crate::guide::shutdown)).

Next: [TCP streams](crate::guide::tcp).
