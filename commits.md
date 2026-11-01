# Prior history

Each crate dir was its own git repo before the merge into this workspace. HEAD logs at merge time.

## conduit (git@github.com:vgatherps/zhida.git)

```
9c6f21f 2026-10-15 push zhida v1.0
```

## cephalopod (https://github.com/vgatherps/cephalopod.git)

```
ab70053 2026-10-20 test runner: reserve one huge page per possible CPU, release after; huge test skips on ENOMEM
a472a9c 2026-10-20 Huge pages: rings live in a directory, page size from the filesystem
f12ca9a 2026-10-20 fifo: no gap found means drop the first stalled stream
e1d5464 2026-10-20 fifo: a stall first seen in a sweep gets one more poll before any kick (test)
defd51a 2026-10-20 Final review fixes
05ba814 2026-10-20 Makefile
9dede04 2026-10-20 fifo: explicit Phase and Front; sweep state in one struct
379c7f5 2026-10-20 push_record selects the ring in the section; RingArgs/RecordArgs; Ring type; push bench
b1969a0 2026-10-20 record_total: header plus padded payload, written plainly
52f2db8 2026-10-20 Fifo::next is next_message; migrate every caller
1d4c785 2026-10-20 GPL-3.0; clippy clean on macOS, aarch64 and x86_64 Linux
cc33e3b 2026-10-20 README: two precise sentences
c220bc7 2026-10-20 README: how it works
e60ddda 2026-10-20 README: semantics, usage, performance
b8cd3f1 2026-10-20 README: drop the socket
4d56e6b 2026-10-20 README: what it does, how to use it
df6c3fe 2026-10-20 README: cut the platform lines
7174ac1 2026-10-20 README: say what it is and how to use it
da12df0 2026-10-19 fifo: kick the stream with the gap; ring: Committed iterator
0ec21fa 2026-10-19 ring: no unlink before create; names are random
7be14a1 2026-10-19 fifo: stalled list plus a progress bit replace the stall counters
bb85198 2026-10-19 Random shm name per run; direct-write test helper; fifo tests single-threaded
29543d3 2026-10-19 fifo test: traffic case drives every sweep deterministically
8619a7b 2026-10-19 fifo: per-stream stall bound; control drains retirements before poll
dc004df 2026-10-19 README: explicit push loop; two comment rewrites
a15ac34 2026-10-19 Examples, crate docs, plain-language pass over comments
c7591e7 2026-10-19 README
75e4572 2026-10-19 End-of-stream records; cleanup
ef514bf 2026-10-19 End-to-end test: sequencer process plus four player processes
ed5ff2f 2026-10-19 fifo: discards do not consume the per-ring batch budget
0db0ac8 2026-10-19 Cut the fd-limit special case and Reader::read
192265e 2026-10-19 fifo: RAII Message; ring view/advance replace take; batch counts discards
3dc4194 2026-10-19 fifo test: control takes effect at a sweep boundary
1357716 2026-10-19 chunk 3: review fixes
ea912cc 2026-10-19 Merge chunk3-control
6aee87d 2026-10-19 ceph-fifo: get-next per-stream FIFO; ring front/take/publish
c71133d 2026-10-19 ceph-control: Unix-socket control channel
01934fb 2026-10-19 proto: Close carries the uid
fd9ab98 2026-10-19 ceph-proto: stream ids and the control-thread message types
15d43b9 2026-10-19 ceph-ring: poll takes a caller-owned buffer
3f41a3d 2026-10-19 ceph-ring: Reader::poll, staggered initial positions
1f68622 2026-10-19 ceph-ring: overflow-proof config bound, SAFETY comment
9e7a4c0 2026-10-19 ceph-ring: in-band commit (wip, pre-review)
6c22126 2026-10-19 ceph-rseq: clear the next tag word, review fixes
5913fb3 2026-10-19 ceph-rseq: in-band commit (wip, pre-review)
5fe6f01 2026-10-18 macOS development fallback: one ring, spinlock per push, loud warning
54ed34f 2026-10-18 ceph-rseq: trim tests to what catches a distinct bug
d015909 2026-10-18 ceph-ring: review fixes and test trim
30112da 2026-10-18 ceph-ring: per-CPU rings in one shm object
d504e86 2026-10-18 ceph-rseq: NEON overlapping-load copy on aarch64
951927a 2026-10-18 ceph-rseq: document each test and helper
56ddf1a 2026-10-18 ceph-rseq: rep movsb on x86_64, named operands, step-by-step asm docs
a49b518 2026-10-18 ceph-rseq: fixes from dual review
ef332ea 2026-10-18 ceph-rseq: rseq per-CPU record push (x86_64, aarch64)
2de4fe1 2026-10-18 Drop the external mapping dependency; ceph-ring maps its own shm
b1f61ef 2026-10-18 Workspace scaffold and Linux test runner
```

## kioto (no remote)

```
ce26b98 2026-05-03 Add benchmark harness and AWS sweep automation
18b520e 2026-05-01 Add local runtime benchmark baseline
```

## slice_ringbuffer and mapping (cantor-core)

Imported `potatomato/core/{slice_ringbuffer,mapping}` from
`~/trading/cantor-core` at `da50a9709f7c9d35480a0df3d9a3dbe99cf0da75`.
Initially imported unchanged, with registry dependencies external.

The shared-memory extraction moves backing ownership, page sizing and single/double
mappings into `shared-memory`, shared by `ceph-ring` and `slice_ringbuffer`. The
vendored `mapping` crate is removed; both queues retain their own publication
protocols. Backing descriptors and names now have separate RAII ownership, creator
cleanup is idempotent, and new objects use mode `0600`.

The `slice_ringbuffer` read-view reference lifetime bug, unchecked queue/metadata
association and final-write/close race remain unfixed. Its closure API is unchanged.
