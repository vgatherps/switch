# Frames

switch-net does not send from or receive into your own byte slices. It hands you **frames**: handles to packet buffers, the memory a packet is built in or arrived in. Working with frames lets data go between your code and the NIC without extra copies. Every socket you opened in [Opening sockets](opening.md) sends and receives frames, and [TCP streams](tcp.md) and [UDP sockets](udp.md) use them throughout.

## Two kinds of frame

A frame is one of two types, both re-exported by switch-net:

- `OwnedFrame` is exclusive and writable. You get one when you ask for an empty frame to fill, and received data arrives in one.
- `ImmutableFrame` is shared and read-only. `freeze()` turns an `OwnedFrame` into one. It can be cloned cheaply, and every clone refers to the same bytes. Sending takes an `ImmutableFrame`.

The packet buffer is released when the last handle to it is dropped. [switch-frame: packet-buffer handles](../blocks/frames.md) describes how frames work inside: slicing, cloning limits, and release.

## Getting a frame to send

A task asks its handle for an empty frame with `handle.tx_frame().await`, writes into it, cuts it to length and freezes it. The test suite's helper does exactly that:

```rust,ignore
{{#include ../../../switch-net/tests/tx.rs:filled}}
```

```text
 handle.tx_frame().await      fill frame.as_mut()[..n]     frame.truncate(n)     frame.freeze()
 -----------------------> OwnedFrame ------------------> OwnedFrame ---------> ImmutableFrame
  waits while the pool                                     n bytes                    |
  is empty                                                                            v
                                         tx.try_send(frame) / tx.send(to, frame)
                                         error: you get the same frame back
                                         local TCP:   the socket holds it until the peer ACKs it
                                         junction:    bytes copied into the send ring, frame freed
```

The frame comes from a fixed pool. On the local backend it is the Stack's frame pool, shared by every socket, and each frame's `len()` is the IP MTU minus 28 (the IP and UDP headers). That length suits a UDP datagram. A TCP segment is shorter, so for TCP fill at most `tx.max_segment_payload()` bytes; a full-size frame is always copied when sent ([TCP streams](tcp.md#sending)). On the junction backend it is a pool inside switch-net with `RemoteConfig::tx_frames` buffers, each as large as the daemon's frame capacity (`Session::frame_capacity`).

When the pool is empty, `tx_frame().await` waits. The first turn that finds a free frame wakes every waiting task, and a task may find the pool empty again if another took the frame first; it then waits again. [Packet-buffer pools](../nic/pools.md) explains how big the local pool is.

Sending never hands a frame to a future. You queue it with the synchronous `try_send` (TCP) or `send` (UDP), and on any error you get the same frame back to retry later. A cancelled task never loses a filled frame.

## Received frames

TCP data arrives as a `TcpFrame` and UDP data as a `Datagram`. Both wrap an `OwnedFrame`:

- `payload()` and `payload_mut()` give the bytes.
- `rx_timestamp()` gives the hardware timestamp, and `Datagram` also has `src()` and `dst_addr()`.
- `into_frame()` gives you the `OwnedFrame` itself, which you can `freeze()` and clone without copying the data, for example to keep it or pass it to several consumers.

Read `rx_timestamp()` and the addresses before calling `into_frame()`: the plain frame does not carry them.

Received frames do not depend on the socket, the `Net` or the Stack. They stay valid after all three are gone: the test `frames_outlive_the_stack` in `switch-net/tests/rx.rs` receives a TCP frame and a datagram, tears down the stream, the `Net` and the Stack, and then still reads and writes both payloads.

## What holding a frame costs

A frame you keep holds its packet buffer until the last clone drops, and the buffer is not available for anything else in the meantime.

On the local backend, a received TCP or UDP frame holds a packet buffer from the Stack's RX pool, the buffers the NIC receives into. Holding many of them leaves fewer for arriving packets. A UDP frame also holds one place in its socket's receive queue (`UdpConfig::rx_queue`) until it is dropped. Holding TCP frames does not shrink the window the socket advertises, and later data on the connection still arrives. The test `held_frames_do_not_block_later_data` in the same file shows it on both backends: a task keeps every frame of 64 KiB of data, more than the socket's `rx_capacity`, and still receives all of it.

On the junction backend, received frames live in the session's receive ring, the shared memory the daemon writes received packets into. Ring space is reused only after the frames in it are dropped and a later turn reclaims it. That space is shared by every socket of the session, so one task holding frames for a long time can fill the ring for all of them. [What applications share](../junction/sharing.md) describes the effect.

## Hardware receive timestamps

`rx_timestamp()` is the NIC's receive time for the packet that carried the data, a hardware timestamp. It is the raw value from the NIC driver, in the driver's units (on AWS ENA, nanoseconds of the NIC clock). 0 means there is no timestamp, for example because the NIC does not provide one or the config file does not enable it. [Offloads and hardware timestamps](../nic/offloads.md) explains how to turn them on.

## Frames stay on their thread

Frames are not `Send`. Create, hold and drop them on the drive loop's thread. On the local backend, drop every frame you kept before closing the NIC port; [Shutting down cleanly](shutdown.md) shows the order.

Next: [TCP streams](tcp.md).
