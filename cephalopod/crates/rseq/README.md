# ceph-rseq

The record layout and the restartable-sequence (rseq) critical section that writes one record into a per-CPU ring. Linux only for the writer; the constants and layout helpers compile everywhere.

Used by `ceph-ring`, which wraps it in `Writer::try_push`. See the [cephalopod README](../../README.md) for the overall design.

## Contents

- Layout: `HEADER_LEN` (32 bytes) with field offsets `TAG_OFFSET`, `LEN_OFFSET`, `STREAM_OFFSET`, `SEQ_OFFSET`; `record_total(len)` (header plus payload padded to 8); `tag_for(pos)` (`pos | 1`, the committed tag).
- Argument types: `Private` (a CPU's cached head and tail), `RingArgs`, `RecordArgs`, `ThreadArea`; `registered()` reports whether the thread has an rseq registration.
- `unsafe fn push_record(area, rings, record) -> Push`: appends one record to the current CPU's ring inside a single rseq critical section. `Push` is `Done`, `Full` (nothing written) or `Aborted` (preempted, migrated or signalled; retry). The caller must uphold the contract in its doc comment, including that positions never wrap.

The commit is one release store of the tag, so a reader never sees a partly written record.

## Constraints

- Linux with GNU libc, on x86_64 or aarch64; other Linux targets fail with `compile_error!`.
- The critical section is hand-written assembly.

## Build and test

`ceph-rseq` has no tests of its own (only a `libc` dev-dependency). The `ceph-ring` and `ceph-fifo` tests exercise it. From `cephalopod/`:

```sh
make test      # Linux tests in Docker, all five ceph crates
make check-x86 # compile the tests for x86_64 under the amd64 image
```

`make test` uses `rust:1.93.0-bookworm` with `--privileged` and reserves hugepages inside the container (`scripts/test-linux.sh`).
