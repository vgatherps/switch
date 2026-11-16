# ceph-fifo

The consumer side of cephalopod: `Fifo` sweeps every per-CPU ring and delivers each producer's stream in sequence order. The junction daemon runs one for the send rings of all sessions.

API reference: `cargo doc --no-deps -p ceph-fifo`. Runnable consumer and producer: `examples/sequencer.rs` and `examples/player.rs`.

From `cephalopod/` (Linux in Docker):

```sh
make test  # tests/fifo.rs and the other ceph crates
make e2e   # one sequencer process, four player processes (release)
```
