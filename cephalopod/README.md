# cephalopod

Ordered messaging from many processes to one consumer over shared memory: each producer pushes into a ring for the CPU it is running on, and the consumer delivers every producer's stream in order. The daemon's send path is built on it.

| Crate | What it is |
|---|---|
| `ceph-ring` | the per-CPU rings: `Writer` for producers, `Reader` for the consumer |
| `ceph-fifo` | `Fifo`, which restores each stream's order on the consumer side |
| `ceph-control` | a Unix-socket service that hands producers their stream ids |
| `ceph-proto` | the messages between `ceph-control` and `ceph-fifo` |
| `ceph-rseq` | the record layout and the restartable-sequence push inside `ceph-ring` |

API reference: `cargo doc --no-deps -p ceph-ring -p ceph-fifo -p ceph-control -p ceph-proto`. Runnable consumer and producer: `crates/fifo/examples/sequencer.rs` and `crates/fifo/examples/player.rs`.

From this directory (Linux in Docker, via `scripts/test-linux.sh`):

```sh
make test          # tests of the five crates
make test-release  # the same, release build
make e2e           # one sequencer process, four player processes
make bench         # push_bench
make clippy        # clippy for the Linux target, warnings are errors
make check-x86     # compile the tests for x86_64 under the amd64 image
```

`make bench` prints the time per push for a 16-byte and a 4096-byte payload.
