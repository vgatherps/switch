# ceph-ring

Shared-memory ring layout with one consumer (`Reader`) and per-CPU writers (`Writer`). There is one ring per CPU. A producer pushes into the ring of the CPU it is running on, so producers never contend.

Used by `ceph-fifo`. Depends on [`ceph-rseq`](../rseq/README.md) and `libc`. The design is in the [cephalopod README](../../README.md).

## API

- `Reader::create(Config { ring_size, max_record, dir })`: creates the shared file in `dir` (`/dev/shm`, or a hugetlbfs mount such as `/dev/hugepages`; ignored on macOS). `name()` gives the path writers attach to; `nr_rings()`.
- Consumer sweep: `poll(&mut Vec<usize>)` lists rings with committed records; `front(ring)`, `committed(ring)`, `view(ring)` read them; `advance(ring)` consumes one; `publish()` makes the consumer's positions visible to writers. The consumer never writes into a ring.
- `Writer::attach(path)`, `try_push(payload, stream, seq)`, `try_finish(stream, seq)`, returning `Result<(), PushError>` where `PushError` is `Full` or `TooLarge`. On Linux `attach` fails unless the thread has an rseq registration.
- `END_OF_STREAM` (bit 63 of `seq`) marks a stream's last record; `Header { stream, seq, len }`.

Feature `test-util` adds `Reader::write_unchecked`, used by `ceph-fifo`'s tests.

## Platforms

Linux uses the rseq writer from `ceph-rseq`. On macOS the writer takes a spinlock in the shared header instead.

## Build and test

From `cephalopod/`:

```sh
make test     # Linux tests in Docker: smoke, ring and huge tests under tests/
make bench    # example push_bench (needs Linux)
```

The tests need hugepages and `--privileged`, which `scripts/test-linux.sh` sets up.
