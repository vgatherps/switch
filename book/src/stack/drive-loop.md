# The Stack's drive loop

The Stack does nothing on its own. Packets arrive in the NIC's receive ring and wait there; timers come due and nothing notices; data you queued sits in the socket. All of that happens only when your loop calls the Stack. This page explains the calls that do the work, what they report, how time is passed in, and how a loop can sleep without missing anything.

If you use switch-net, `Net::turn` makes these calls for you and [the drive loop](../net/drive-loop.md) in the switch-net chapter is the page to read. This page is for programs that call the Stack directly, such as one whose Stack was built on the [previous page](by-hand.md), and for readers who want to know what a turn does underneath.

## `turn`: the network work of one call

`stack.turn(now, ingress_budget)` does a bounded amount of everything the Stack has to do, in this order:

1. Once a second, run DPDK's timers.
2. Check for a pending device reset and recover the NIC port (every call).
3. Hand packets waiting in the pending queue to the NIC driver (a flush).
4. Take one burst of arriving packets from the NIC.
5. Ingress: process up to `ingress_budget` received packets. TCP segments go to their sockets, ARP requests get replies, and so on.
6. Egress: build the packets sockets want to send (data, ACKs, retransmissions, SYNs, FINs).
7. Flush again.

`now` is the current time. `ingress_budget` caps how many received packets this call processes; packets beyond it wait in the Stack's staging queue for the next call. The programs in this repository use 32 (the tap echo harness and the first Stack client) or 128 (the benchmark and the daemon).

A loop that only calls `turn` looks like this, from the tap echo harness:

```rust,ignore
{{#include ../../../conduit/crates/switch-tap-echo/src/lib.rs:serve_loop}}
```

Each time through the loop it takes the time, turns, lets the application read and write its sockets (`echo.step`), and then either goes round again at once or sleeps. Sleeping is covered [below](#sleeping-safely).

## `receive`, your work, then `send`

`turn` builds outgoing packets before your code runs. Whatever your code writes after `turn` returns waits for the next time through the loop. A server that wants its reply to leave before the loop comes round again splits the turn in two:

```text
 turn(now)                            receive(now) ... your code ... send()
 -------------------------            ----------------------------------
 timers, reset check, flush           timers, reset check, flush
 RX burst                             RX burst
 ingress (<= budget)                  ingress (<= budget)
                                      flush ACKs, resets, ARP replies
 egress  <-- built before             -----------------------------
 flush       your code runs           your code reads and writes sockets
                                      -----------------------------
 your code reads and writes           egress  <-- includes what your
 (leaves next time through)           flush       code just wrote
```

`receive(now, ingress_budget)` does the first half: timers, the reset check, a flush, the receive burst and ingress, then one more flush if packets are pending. So resets, immediate ACKs and ARP replies built while processing received packets go to the NIC before your code runs. `send()` does the second half: egress and a flush. Whatever your code wrote since `receive` leaves here.

The [complete client](index.md#a-complete-client) from the start of this part uses this shape:

```rust,ignore
{{#include ../../../conduit/crates/switch-open/examples/tcp_client.rs:loop}}
```

There is one cost. Everything egress builds leaves only in `send`, and that includes SYN-ACKs, retransmissions, delayed ACKs and window updates. Keep the work between `receive` and `send` short, or connection setup and retransmissions wait for it.

`send` runs at the time given to the last `turn` or `receive` (or `Stack::new`). It needs no `receive` before it, and calling it with nothing to send does nothing.

## `flush`

`flush()` hands pending packets to the NIC driver, burst after burst, for as long as the driver accepts everything offered. It builds no packets and does not change the time. A packet the driver accepted belongs to the driver from then on; that is not an acknowledgement from the peer, nor proof the packet reached the wire.

## Budget and counters

`turn` and `receive` return a `Turn`; `send` and `flush` return a `Sent`:

| Field | Meaning |
|---|---|
| `Turn::rx_received` | Packets this call's receive burst took from the NIC. |
| `Turn::rx_rejected` | Packets from that burst released at once: unsupported, or with a bad IPv4 header checksum reported by the NIC. |
| `Turn::rx_processed` | Staged packets processed this call. |
| `Turn::rx_staged` | Packets still waiting in staging after this call. |
| `tx_accepted` | Packets the NIC driver accepted across this call's flushes. |
| `tx_pending` | Packets still waiting for the driver after this call. |

`Turn::moved()` is true when the call received, rejected, processed or sent anything. A call where it is false was idle.

`rx_staged > 0` after a call means either the budget ran out, or there was no room to build a reply (the pending queue or the TX pool was full). Call again promptly. `tx_pending > 0` means the driver did not take everything; call `flush` until nothing is pending, then `send`.

## Sleeping safely

On a pinned CPU the simplest loop never sleeps: it calls the Stack again at once. That is what the Stack client example and the benchmarks do. A loop that shares its CPU, or wants to save power, can sleep when a call was idle, as long as it does not sleep through work.

`stack.poll_at()` returns the earliest time a socket wants the Stack called again: a retransmission timer, a delayed ACK, a close timer, or "now" if a socket has a segment ready to send. `None` means no socket has a deadline. Socket timers live in a millisecond timer wheel, so the result is never later than the earliest socket deadline rounded up to a whole millisecond, and may be earlier.

`poll_at` covers socket timers only. It does not know about packets arriving at the NIC, packets still staged or pending inside the Stack, or ARP probes owed for a stale neighbor. So never sleep until `poll_at` with no upper bound. The repository's loops cap the sleep at one millisecond: the tap echo harness above sleeps for `min(poll_at − now, 1 ms)`, no time at all if `poll_at` is already due, and 1 ms if it is `None`. A packet that arrives during the sleep waits up to that cap.

## Time

Every call that does network work takes the time as a `smoltcp::time::Instant`, re-exported as `dpdk_smoltcp::smoltcp::time::Instant`. It counts microseconds as an `i64`.

The rules are simple and strict. The start time passed to `Stack::new` must be zero or more. After that, the time given to `turn` and `receive` must never go backwards; the same time twice is fine. An earlier time returns `StackError::TimeWentBackwards { last, now }` before doing any work. `send` and `flush` take no time argument and reuse the last one.

Use one clock for every call. The repository's pattern takes a `std::time::Instant` once, just before creating the Stack at `Instant::ZERO` (or before calling `open`), and reports the microseconds elapsed since:

```rust,ignore
{{#include ../../../conduit/crates/switch-open/examples/tcp_client.rs:clock}}
```

Take time from your one clock, not from `smoltcp::time::Instant::now()`, which reads the wall clock.

## When a drive call fails or panics

`turn`, `receive`, `send` and `flush` return `StackError` on failure. `TimeWentBackwards` changes nothing; fix the clock and continue. `StackError::Port` means the NIC driver failed, or the port could not be recovered after a reset. The port is then not ready and later calls fail too: close the Stack.

A device reset is something a NIC driver can ask for after a fault. ENA, the AWS NIC driver, does so from a health watchdog that runs among DPDK's timers, which the Stack runs once a second. Every `turn` and `receive` checks whether a reset is pending, and if so resets and restarts the port before the receive burst. Packets the device held return to their pools, sockets and the ARP cache are untouched, and TCP retransmits whatever the device dropped. `dpdk_smoltcp::port_resets()` counts recoveries in the process.

A panic that unwinds out of a drive call leaves the Stack unusable. The only things you may still do are drop it or call `into_port`. The [errors page](errors.md) lists the panics.

## TCP timers you will notice

A few TCP behaviours follow from the drive loop and the Stack's TCP settings:

| Behaviour | Value |
|---|---|
| Retransmission timeout | 300 ms base (`set_rto`), backing off to at most 60 s |
| Delayed ACK | 10 ms by default; `set_ack_delay(None)` turns it off, `ack_now()` sends an owed ACK at the next egress |
| TIME-WAIT | 10 s |
| Keepalive | none; an idle connection sends nothing |
| Nagle's algorithm | none; data queued with `send_slice` is eligible to leave at the next egress |
| Congestion control | Cubic, initial window 1 MiB by default (`set_initial_window`) |
| TCP timestamps option | never negotiated |

Next: [TCP sockets on the Stack](tcp.md).
