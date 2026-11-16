# The junction control protocol

This page describes what a `Session` and the daemon exchange, for someone writing a client in another language or checking a packet capture of the control socket. Application authors do not need it; `Session` in the `switch-junction` crate implements all of it (see [Using a Session directly](../junction/session.md)). The [crate map](crates.md) before it lists where `switch-junction` and `switch-junctiond` live.

## Channels

A session uses three channels:

| Channel | Direction | Carries |
|---|---|---|
| control socket (`control.sock`, Unix stream socket) | both | handshake, socket operations, events, the end-of-session notice |
| send ring (`cph...` file, one ring per CPU) | client to daemon | send records, and the session's end record |
| receive ring (`jnctrx...` file, one per session) | daemon to client | received TCP chunks and UDP datagrams |

The two rings are described in [Shared memory and the receive ring](../blocks/rings.md) and [cephalopod](../blocks/cephalopod.md). This page covers the control socket and the send-record format.

## Handshake

```text
 client                                         daemon
   | connect(control.sock)                        |
   |--- Hello { version: 3, max_sockets } ------->|
   |                                              | checks version, max_sockets,
   |                                              | a free session slot; creates
   |                                              | the session's receive ring
   |<-- Welcome { version, status, stream,        |
   |              max_record, frame_capacity,     |
   |              ingress path, rx path } --------|
   | open the send ring (ingress path) and        |
   | the receive ring (rx path)                   |
   |--- operations ------------------------------>|
   |<-- events -----------------------------------|
```

The client sends `Hello` first. A connection that has not sent it within 5 seconds is dropped.

The protocol version is 3 (`switch_junction::control::VERSION`) and must match exactly. The daemon always answers with its own version in `Welcome`. If the versions differ, the status is `BadVersion`; `Session` checks the version first and reports `ConnectError::Version { host }`.

`Welcome.status` is one of `Ok` (0), `BadVersion` (1), `SessionsFull` (2), `BadMaxSockets` (3) or `HostResources` (4). Only with `Ok` are the other fields meaningful. `stream` is the session's id in the send ring (the session slot in the high 32 bits, a unique number in the low 32 bits). `max_record` is the largest send record in bytes, header included. `frame_capacity` is MTU − 28. `ingress` and `rx` are the paths of the two ring files (on macOS, the shared-memory object names).

## Messages

Every control message is an 8-byte header followed by a body. All integers are little-endian.

```text
 offset  size  field
 0       1     kind
 1       1     flags (bit 0 only, for kinds 6 and 70; otherwise 0)
 2       2     body length in bytes
 4       4     zero
 8       ...   body
```

A socket id in a body is 8 bytes: slot (u32), then generation (u32). IPv4 addresses are 4 bytes in network order; ports are u16 little-endian.

Client to daemon:

| Kind | Message | Body length | Body |
|---|---|---|---|
| 1 | `Hello` | 8 | magic `JNCT`, version u16, max_sockets u16 |
| 2 | `ConnectTcp` | 36 | id, TCP config, remote address, remote port, 2 zero bytes, local endpoint |
| 3 | `ListenTcp` | 28 | id, TCP config, local endpoint |
| 4 | `BindUdp` | 24 | id, local endpoint, rx_queue u32, tx_checksum u8, broadcast u8, 2 zero bytes |
| 5 | `ShutdownTcp` | 8 | id |
| 6 | `Close` | 8 | id; flag bit 0 = unread |
| 7 | `AbortTcp` | 8 | id |

A TCP config is 12 bytes: rx_capacity u32, tx_capacity u32, mss_cap u16 (0 = none), 2 zero bytes. A local endpoint is 8 bytes: address (zero when absent), port u16, has-address u8 (0 or 1), 1 zero byte.

Daemon to client:

| Kind | Message | Body length | Body |
|---|---|---|---|
| 64 | `Welcome` | 28 + path lengths | magic `JNCT`, version u16, status u8, zero u8, stream u64, max_record u32, frame_capacity u32, ingress length u16, ingress path, rx length u16, rx path |
| 65 | `Connected` | 10 | id, mss u16 |
| 66 | `Bound` | 8 | id |
| 67 | `Rejected` | 9 | id, reject code u8 |
| 68 | `PeerFin` | 8 | id |
| 69 | `Delivered` | 8 | id |
| 70 | `Closed` | 8 | id; flag bit 0 = reset by peer |
| 71 | `Freed` | 8 | id |
| 72 | `End` | 1 | reason u8: `Kicked` 1, `ProtocolError` 2, `HostShutdown` 3, `Finished` 4 |

Reject codes are `TcpSocketsFull` 1, `ConnectInvalidState` 2, `ConnectUnaddressable` 3, `ConnectAddressInUse` 4, `ListenInvalidState` 5, `ListenUnaddressable` 6, `UdpSocketsFull` 7, `UdpPortInUse` 8, `UdpUnaddressable` 9.

Both sides reject unknown kinds, wrong body lengths, nonzero padding, unknown flags, booleans other than 0 or 1, and invalid socket configs. The daemon ends a session that sends such a message with `End(ProtocolError)`.

For example, the end notice for a finished session is these 9 bytes:

```text
72, 0, 1, 0, 0, 0, 0, 0, 4
```

The crate's tests check this and a `ConnectTcp` and a `Welcome` byte for byte (`golden_bytes` in `switch-junction/tests/control.rs`).

## Send records

Each send is one record in the send ring. The record starts with a header, followed directly by the payload:

```text
 TCP send, 16-byte header          UDP send, 24-byte header
 offset size field                 offset size field
 0      1    kind = 16             0      1    kind = 17
 1      3    zero                  1      3    zero
 4      4    slot (u32)            4      4    slot (u32)
 8      4    generation (u32)      8      4    generation (u32)
 12     4    zero                  12     4    zero
 16     ...  payload               16     4    destination address
                                   20     2    destination port (u16)
                                   22     2    zero
                                   24     ...  payload
```

So a TCP payload can be at most `max_record − 16` bytes and a UDP payload `max_record − 24`. A record for a slot the session never opened, for a socket of the other kind, or with a malformed header ends the session with `ProtocolError`. Records for an earlier generation of a slot are dropped. The session ends its send stream with the ring's end record, which `Session::finish` writes.

Next: the [glossary](glossary.md) defines the terms used throughout the book.
