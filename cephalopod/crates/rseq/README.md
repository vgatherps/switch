# ceph-rseq

The record layout of cephalopod's per-CPU rings and the restartable-sequence (rseq) critical section that writes one record into the ring of the current CPU. Only `ceph-ring` uses it; the writer needs Linux with glibc on x86_64 or aarch64.

API reference: `cargo doc --no-deps -p ceph-rseq`.

From `cephalopod/` (Linux in Docker):

```sh
make test       # all five ceph crates, including this crate's unit tests
make check-x86  # compile the tests for x86_64 under the amd64 image
```
