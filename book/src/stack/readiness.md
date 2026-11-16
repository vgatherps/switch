# Waiting on many sockets

A loop with one socket can check it after every turn. A loop with thousands cannot afford to: most turns change nothing for most sockets, and checking each one every time costs more than the network work. Readiness tokens solve this. You attach a number to a socket half you are waiting on, and after a turn the Stack hands you back only the numbers of sockets that may have changed.

switch-net is built on this. Each parked task's socket carries a token, and `Net::turn` wakes exactly the tasks whose tokens came back, so an idle turn costs the same however many tasks are waiting. This page shows the same mechanism for programs that drive the [TCP](tcp.md) and [UDP](udp.md) sockets of the previous pages directly.

## Arm, fire, pop

A readiness token is a `u32` you choose, armed on one half of a socket. A TCP socket has two halves, `ReadySide::Rx` for the code waiting to receive and `ReadySide::Tx` for the code waiting to send, each with its own token. A UDP socket has one, its receive queue.

```text
   arm_tcp(key, Rx, 7)          a segment arrives during a turn
          |                                 |
          v                                 v
    +----------+      something that   +----------+   pop_fired()  +----------+
    |  armed   | ---  may change the ->|  fired   | -------------> | disarmed |
    +----------+      socket happens   +----------+   returns 7    +----------+
          ^                                                              |
          +------------------ check the socket, then arm again ----------+
```

First take the socket's key, once, and keep it beside the handle. Taking the key checks that the handle belongs to this Stack and names a live socket; arming with the key afterwards skips that check.

```rust,ignore
let key = stack.tcp_ready_key(&handle);
stack.arm_tcp(key, ReadySide::Rx, 1);
stack.arm_tcp(key, ReadySide::Tx, 2);
```

After each turn, drain the fired tokens and service each socket they name. The [example below](#a-real-loop-the-junction-daemon) shows a complete loop.

`pop_fired` returns each fired token once, in no particular order, and `None` when there are no more. A fired token is disarmed: it will not fire again until you arm it again. Arming a half that already has a token replaces it.

A fired token means the socket may have changed. It does not say what changed, and sometimes nothing has. Always check the socket's real state (`can_recv`, `recv`, `state`, `send_queue`, ...) after a token comes back, and arm again if what you were waiting for has not happened yet.

## What fires a token

A TCP token fires on any of these:

- a received segment for the socket;
- a call to `stack.tcp_mut(&handle)` for that socket, which includes `tcp_send_frame`, `connect` and `listen`; these fire at once, between turns;
- the first `turn`, `receive` or `send` that finds a free frame after the socket's `send_slice` found the frame pool empty;
- a state change during egress, such as a close timer or the end of TIME-WAIT.

Transmitting queued data during a turn fires nothing; the peer's ACK that frees room in the send queue does. Idle turns fire nothing.

Because `tcp_mut` fires the socket's tokens, arm after you have finished reading and writing a socket, not before. Otherwise your own `recv` fires the token you just armed.

A UDP token fires when a datagram is queued on the socket.

## Disarming and removal

`stack.disarm_tcp(key, side)` and `stack.disarm_udp(key)` stop a token at once. They return `true` if a token was armed, and `false` if none was or it already fired and `pop_fired` has not returned it yet.

`remove_tcp` and `remove_udp` disarm the socket's tokens. So does the Stack when it processes a dropped handle: tokens of a dropped socket never fire.

A key is valid until its socket is removed. Only debug builds check that a key still names a live socket. In a release build, a key used after its socket was removed (or on another Stack) sends the token to whatever socket now holds that slot, where it fires for no reason, and panics if the slot does not exist.

## A real loop: the junction daemon

The junction daemon runs one Stack for many applications and receives on every socket they open. It uses each socket's index in its own socket table as the token, and arms only the receive half. After each turn it drains the fired tokens:

```rust,ignore
{{#include ../../../switch-junctiond/src/network.rs:fired}}
```

For each token it looks the socket up, disarms it, reads what arrived, and arms it again with the same token:

```rust,ignore
{{#include ../../../switch-junctiond/src/network.rs:service_rx}}
```

`socket.socket.disarm` and `socket.socket.arm` are the daemon's small wrappers around `disarm_tcp`/`disarm_udp` and `arm_tcp(key, ReadySide::Rx, token)`/`arm_udp`; a TCP socket that has closed is not armed again. `socket.receive` reads the socket and hands the data to the application's [session](../junction/session.md). The order matters. Reading goes through `tcp_mut`, which fires the socket's token, so the daemon disarms first and arms only after reading. The token it arms then fires at the next change, not because of its own read. A socket the daemon could not finish reading (the application's receive ring was full) goes on a retry list, which the daemon works through before its next turn.

Next: [Stack errors and panics](errors.md).
