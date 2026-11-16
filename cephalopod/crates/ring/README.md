# ceph-ring

Cephalopod's per-CPU shared-memory rings: producers push through a `Writer` into the ring of the CPU they are running on, and one `Reader` reads every ring. Used by `ceph-fifo` and by junction's send path.

API reference: `cargo doc --no-deps -p ceph-ring`.

From `cephalopod/` (Linux in Docker):

```sh
make test   # smoke, ring and huge-page tests under tests/
make bench  # examples/push_bench.rs
```
