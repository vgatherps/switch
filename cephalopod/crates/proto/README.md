# ceph-proto

The messages between cephalopod's consumer (`ceph-fifo`) and its control thread (`ceph-control`), and the stream-id layout they share. The junction daemon and its client use the same types.

API reference: `cargo doc --no-deps -p ceph-proto`. It has no tests of its own; `make test` in `cephalopod/` tests the crates that use it.
