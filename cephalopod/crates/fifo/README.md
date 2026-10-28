# ceph-fifo

The sequencer side: `Fifo` merges the per-CPU rings into one ordered stream of messages per producer, with end-of-stream and stall handling.

Depends on [`ceph-ring`](../ring/README.md) and [`ceph-proto`](../proto/README.md). `ceph-control` is a dev-dependency (for the end-to-end test). Nothing in the workspace depends on `ceph-fifo`.

## API

- `Fifo::new(reader, max_streams, control_rx, retire_tx)`: `max_streams` must equal the value passed to `ceph_control::Listener::bind`.
- `Fifo::next_message() -> Option<Message<'_>>`: the next deliverable message, or `None` if none is available now. Dropping the message consumes its record. At most 64 consecutive messages come from one ring before it moves to the next.
- `Message`: `stream()`, `seq()`, `payload()`, `end()`. The end record has an empty payload.

Per stream, a message is delivered only when its `seq` is the expected one. A lower `seq`, or a record for a closed or unknown slot, is discarded. A higher `seq` makes the ring stall until the missing record arrives; when every ring with data is stalled, `Fifo` kicks a stream whose expected record is in no stalled ring and sends `Retire::Kicked`. A delivered end record sends `Retire::Finished`.

Feature `test-util` (enabled by the crate's own tests) exposes `Fifo::reader`.

## Examples

`examples/sequencer.rs` (consumer and control thread) and `examples/player.rs` (producer). The usage snippets are in the [cephalopod README](../../README.md).

## Test

From `cephalopod/`:

```sh
make test    # Linux tests in Docker
make e2e     # one sequencer process, four player processes (release)
```

`tests/fifo.rs` has the unit-level cases (reordering across rings, batching, kicks, discards, end of stream); `tests/e2e.rs` is the multi-process run. The Makefile runs them on Linux in Docker.
