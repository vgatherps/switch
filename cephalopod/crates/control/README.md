# ceph-control

A Unix-socket service that gives each cephalopod producer its stream id and the path of the shared ring file, and tells `ceph-fifo` when streams open and close. For standalone cephalopod programs; the junction daemon does not use it.

API reference: `cargo doc --no-deps -p ceph-control`.

From `cephalopod/` (Linux in Docker):

```sh
make test  # tests/control.rs and the other ceph crates
```
