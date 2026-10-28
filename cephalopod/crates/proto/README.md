# ceph-proto

Types shared by the sequencer's polling thread (`ceph-fifo`) and its control thread (`ceph-control`). No dependencies.

## Contents

- Stream ids are `u64`: the slot index in the high 32 bits, a unique id in the low 32. `pack(slot, uid)`, `slot(stream)` and `uid(stream)` convert.
- `Control`, control thread to polling thread: `Open { slot, uid }` and `Close { slot, uid }`.
- `Retire`, polling thread to control thread: `Kicked { slot, uid }` (the polling thread dropped a stalled stream) and `Finished { slot, uid }` (the stream delivered its last record). In both cases the control thread closes the socket and frees the slot.

Used by `ceph-fifo` and `ceph-control`. The two sides pass these over `std::sync::mpsc` channels.

## Test

No tests of its own. `cargo check -p ceph-proto` builds it on any host; the crates that use it are tested with `make test` in `cephalopod/` (Docker, Linux).
