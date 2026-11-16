# Walkthrough: a market-data feed

This page reads a complete application: a client of Binance's public bookTicker stream, which pushes the best bid and ask price for a trading pair every time they change. It connects over `wss://`, parses each update, measures how long each message took from the NIC to the application, and closes cleanly. It lives in the `switch-binance-feed` package, as a library (the feed task) and a demo binary (the setup and drive loop around it). Everything it does has been covered on the previous pages of this part, from [the overview](index.md) to [TLS without WebSocket](tls.md); here you see the pieces in one program.

## What it does

The feed connects to `wss://stream.binance.com:9443/ws/btcusdt@bookTicker` by default. Each message is a small JSON text with an update id (`u`), the symbol (`s`), the best bid price and quantity (`b`, `B`) and the best ask price and quantity (`a`, `A`). The feed parses it into a `BookTicker`, whose string fields borrow the message text and keep Binance's decimal strings as they are.

For every message the feed records one latency sample: the wall-clock time when switch-ws handed the message over, minus the message's [hardware timestamp](../nic/offloads.md). It prints the first few updates, keeps counting until it is told to stop, then prints a summary and closes the connection.

## The task

The whole feed is one switch task. It connects within `CONNECT_LIMIT` (20 seconds), reads until the `stop` channel fires, then closes within `CLOSE_LIMIT` (10 seconds):

```rust,ignore
{{#include ../../../conduit/crates/switch-binance-feed/src/lib.rs:feed}}
```

Walking through it:

1. `connect` is the function from [Connecting to a wss:// server](wss-client.md): TCP, TLS and the WebSocket upgrade under one `within`.
2. The connect itself is raced against `stop` with `first`. If the program is told to stop while still connecting, the connect future is dropped, and the socket with it.
3. `read` is the loop from [Sending and receiving messages](messages.md#receiving). It races every `recv` against `stop`; a `recv` dropped this way loses nothing. A server close ends the loop with `FeedError::ServerClosed` carrying the code and reason from `peer_close()`. A Binary message or text that is not a bookTicker object ends it with an error.
4. Whatever ended the reading, `close` runs the close handshake from [Closing connections](closing.md#closing-from-your-side): `close(1000, "")`, then `recv` until `None`, under `CLOSE_LIMIT`. When it returns, close_notify and FIN have been acknowledged and the `Net` may be dropped.
5. The task stores its `Summary` and result in `out`, a shared cell the drive loop checks.

The `stop` signal is a `switch::oneshot` channel. The task holds the receiver; the drive loop holds the sender and sends `()` when the program should stop.

### Measuring latency

Each latency sample is `realtime_ns() - message.rx_timestamp()`. `realtime_ns()` reads the system's wall clock (`CLOCK_REALTIME`). The NIC stamps packets with its own hardware clock, so the subtraction is meaningful only on a machine whose system clock is synchronised to the NIC's clock; the repository's host setup script `deploy/hft-host.sh` does this with chrony. Messages without a stamp (a [tap device](../getting-started/first-connection.md) has none) are counted as `no_stamp`. The samples go into a histogram with power-of-two buckets in nanoseconds.

## The loop around it

The demo binary runs on a tap device inside a container. Its `main` resolves the host name with the kernel resolver before DPDK starts (the NIC the program owns, the [data NIC](../nic/index.md#what-changes-when-your-process-owns-the-nic), has no DNS of its own), opens the tap device, builds the `Net`, and then runs this loop:

```rust,ignore
{{#include ../../../conduit/crates/switch-binance-feed/src/main.rs:drive}}
```

This is the [drive loop](../net/drive-loop.md) from the first network program, with three additions. It samples one clock for both the Stack time and the runtime tick (`Clock::sample`). It sleeps for 1 ms when a [round](../net/drive-loop.md) moved nothing, because in this demo the CPU is shared with a helper process; on a dedicated pinned CPU you would call `round` again at once. And it fires the `stop` sender once the helper process has exited.

```text
 drive loop                         feed task                     helper process
 ----------                         ---------                     --------------
 turn Net until ready file exists                                  set up NAT, create ready file
 spawn feed task
 round ...                          connect (TCP, TLS, upgrade)
 round ...                          read: recv, recv, recv ...
 round ...                                                         exits after N seconds
 stop.send(())  ------------------> first(..) returns None
 round ...                          close, recv until None
 round ... out is set               task ends
 into_stack(), close the port
```

## Running it

Offline, with no internet, the test suite runs the same feed task against a local TLS server with a generated certificate. The test `feed_reads_tickers_answers_ping_and_closes` checks that the feed reads updates, answers a server ping inside `recv`, and closes cleanly; others check a refused upgrade and an unknown certificate issuer. From `conduit/`:

```sh
make test-binance
```

This needs DPDK 25.11.3, `CAP_NET_ADMIN` and `/dev/net/tun`, which is to say the [Docker image](../getting-started/environment.md) on macOS.

Live, against Binance, from `conduit/`:

```sh
make docker-binance BINANCE_SECONDS=30
```

This needs internet access. It uses a separate image with nftables, because the helper process gives the tap device's kernel side the address 192.0.2.1 and forwards the feed's traffic (from 192.0.2.2) out of the container with NAT. On a Linux machine with DPDK installed, `make binance` runs the same thing natively.

The binary's own command line is

```text
switch-binance-feed [--host HOST] [--port PORT] [--path PATH] [--print N] -- <tester> [args...]
```

with defaults `stream.binance.com`, 9443, `/ws/btcusdt@bookTicker` and 10. Each option may appear once. `<tester>` is the helper process. The exit status is 0 when the tester exited 0, 1 when the tester failed, 2 when packet buffers were still in use when the NIC port closed, 3 for bad arguments or a setup, feed or teardown error, and 101 for a panic.

Binance's servers answer HTTP 451 from restricted locations. The feed then fails with `Upgrade(Status(Some(451)))` and the run exits with status 3.

The same feed task also runs on a real NIC through the `feed` mode of [switch-nic](../nic/switch-nic.md).

## Output lines

Each line has the form `<name>: <key>: <value>`, where `<name>` is the program name:

| Key | Value |
|---|---|
| `feed_connected` | the URL, the local port and `TLS 1.3` |
| `feed_message` | one of the first N updates, numbered |
| `feed_count` | how many updates were read |
| `feed_latency` | `no_stamp=N` and the nonzero histogram buckets, such as `8192..16384:12` (nanoseconds) |
| `feed_update_ids` | first and last update id, or `none` |
| `feed_close` | the close handshake, close_notify and FIN all completed |
| `feed_close_error` | the close failed after reading had already failed |
| `feed_stopped` | the stop signal came before the connect finished |

To see how this kind of latency is measured under controlled load rather than from a live feed, continue with [Measuring latency under load](../measuring/index.md). The next page in book order is [Owning a NIC](../nic/index.md).
