# Shared memory and the receive ring

The [previous page](frames.md) showed that a frame is a handle to bytes some producer owns. On the junction backend, the bytes of every received packet live in a file that the daemon and your application both map: the session's receive ring. This page builds that ring up from the bottom. First the shared-memory file itself (the `shared-memory` crate), then the trick that keeps every record in one piece, then the ring (`slice_ringbuffer`) and the rule that decides when its space comes back.

Both crates run on Linux and macOS without root and without DPDK, so the examples here work on a laptop.

## Backings

A backing is a named shared-memory object: either a POSIX shared-memory object (a name such as `/my-ring`, created with `Backing::create_shm`) or an ordinary file at a path (`Backing::create_file`). Another process reaches the same memory by opening the same name with `Backing::open_shm` or `Backing::open_file`.

```text
 process A                     the backing                     process B
 Backing::create_shm("/r")     /r  (owner: A)                  Backing::open_shm("/r")
   |                           +----------------+                |
   +-- Mapping --------------> |  bytes         | <------------- +-- Mapping
                               +----------------+
```

The rules about names are simple and strict:

Creating is exclusive. If the name already exists, `create_shm` or `create_file` fails with `AlreadyExists`; it never attaches to an old object by accident.

The creator owns the name. Dropping the creating `Backing` removes the name (so does calling `unlink`). A handle that only opened the name never removes it, and its `unlink` does nothing.

Removing the name does not tear anything down. Mappings that already exist keep working after the name is gone and after every descriptor is closed; the memory is freed when the last mapping goes away. It does mean that a process opening the name after that point gets `NotFound`, so the creator has to keep its `Backing` alive until the other side has opened it.

Files and objects are created with mode 0600, readable and writable only by the user who created them. Two processes running as different users cannot share a backing.

A new backing has length zero. `resize(len)` sets its length. `allocate(len)` does the same but on Linux also reserves the storage up front, so a full filesystem shows up as an error from `allocate` instead of a crash the first time a page is touched; on macOS it is the same as `resize`. Never shrink a backing while it is mapped.

## Mappings and page size

`Mapping::new(fd, offset, len)` maps part of a backing into memory, readable and writable; `Mapping::read_only` maps it read-only. Both refuse a zero length, an offset that is not a multiple of the page size, and a range that runs past the end of the backing. They unmap when dropped. `as_ptr()` returns a raw pointer to the bytes: the crate does not synchronize, initialize or interpret them, which is the job of whatever protocol you build on top.

The page size is the granularity of all of this. For POSIX shared memory and ordinary files it is the system's native page size (4 KiB on most Linux machines); for a file on a Linux hugetlbfs mount it is that mount's huge-page size (usually 2 MiB). `backing.page_size()` reports it for an open backing, and `shared_memory::page_size_in(dir)` reports it for files you are about to create in a directory, so you can size things first. On hugetlbfs the crate uses huge pages and nothing else: you set up the huge-page pool on the machine, and a backing that does not fit fails instead of falling back to small pages.

`lock()` on a mapping asks the kernel to keep its pages in memory (`mlock`).

## The mirrored mapping

A ring buffer reuses a fixed region over and over, so sooner or later a record starts near the end and runs past it. Most rings deal with that by splitting the record in two or copying it. These rings avoid both with a mirrored mapping: `DoubleMapping::new(fd, offset, len, populate)` maps the same `len` bytes of the backing twice, back to back in the address space.

```text
 backing:                 [ A A A A B B B B ]        len bytes

 address space:  base                base+len              base+2*len
                  | first view        | second view (same bytes)
                  [ A A A A B B B B ][ A A A A B B B B ]
                                 \__________/
              a record starting near the end of the first view
              continues into the second view: one contiguous slice,
              and its tail is physically the start of the backing
```

A write through either view is visible through the other, because they are the same memory. `len` has to be a multiple of the backing's page size. On Linux, `populate = true` faults all the pages in when mapping, so the first packet does not pay for that; elsewhere it has no effect.

The crate's test writes eight bytes across the end of the first view and reads them back at both addresses:

```rust,ignore
{{#include ../../../shared-memory/tests/behavior.rs:wrap}}
```

## The receive ring

`slice_ringbuffer` is a ring of variable-length records with exactly one writer and one reader. Each record carries a `PacketMeta` header and a payload, and the reader gets each record back as an `OwnedFrame` from [switch-frame](frames.md) pointing straight into the shared memory. Thanks to the mirrored mapping, every payload is one contiguous slice.

The writer creates the ring on a fresh backing; the reader opens the same name. This is how the crate's tests set one up, both sides in one process:

```rust,ignore
{{#include ../../../vendor/slice_ringbuffer/src/tests.rs:ring_setup}}
```

`RingWriter::create(&mut backing, size)` lays the ring out in the backing: one page of header followed by `size` bytes of records, mapped twice. It returns an error if the backing is not fresh (its length is not zero) or if `size` is not a power of two, not a multiple of the backing's page size, or larger than 2 GiB. The largest payload is `size - 40` bytes, which `max_payload()` returns; each record also uses a 40-byte header and is rounded up to 8 bytes.

Writing a record takes two steps. `reserve(len, meta)` returns a reserved record, or `None` when the ring has no room right now; it panics if `len` is larger than `max_payload()`. You fill `payload()` and call `commit()`, which makes the record visible to the reader. A reserved record that is dropped without `commit` is never seen.

`RingReader::open(&backing)` checks that the backing holds a ring and claims the reader's place. That claim lasts for the life of the ring: only one reader can ever open it, and a second `open`, even after the first reader is dropped, fails with `AlreadyExists`. `next()` returns the next committed record as `(PacketMeta, OwnedFrame)`, or `None` when there is nothing new.

`RingWriter` can move to another thread. `RingReader` and the frames it returns cannot: they stay on the thread that opened the reader.

The reader trusts the writer. If the other process scribbles over the ring's header or lengths, `next` and `reclaim` panic rather than read out of bounds.

## Reclaim

Dropping a frame does not hand its space back to the writer by itself. The reader does that in `reclaim()`, which walks forward from the oldest record it has not yet returned and stops at the first one that still has a live frame. Records are returned to the writer strictly in order.

```text
 positions only grow; the writer may reuse everything left of "reclaimed"

        reclaimed      read by next()                committed by writer
            |                |                              |
            v                v                              v
   ... free | rec1 | rec2 | rec3 | rec4 |  rec5  rec6  ...  |  free ...
            | HELD | free | free | free |  (not read yet)   |

   reclaim() stops at rec1: rec2..rec4 are dropped but cannot be reused
   until rec1's last frame is dropped and reclaim() runs again
```

The crate's test shows exactly this: three records fill the ring, the reader drops the second and third, and the writer still cannot reserve even an empty record until the first is dropped too.

```rust,ignore
{{#include ../../../vendor/slice_ringbuffer/src/tests.rs:reclaim_prefix}}
```

This is the rule behind junction's receive behavior. A `Session` calls `reclaim` at the end of every poll, so ring space you free comes back on your next poll. One frame you keep for a long time, from any socket, holds back every record received after it, and once the ring is full the daemon cannot deliver anything more to your session until you let go. [What applications share](../junction/sharing.md) describes what the daemon does while it waits.

A frame stays valid after the reader, the writer and both backings are dropped. Its own reference keeps the mapping alive until the frame goes.

## Where the junction rings live

The daemon creates one receive ring per session, and one file for all the per-CPU send rings of [cephalopod](cephalopod.md). Both are files in the daemon's directory (the `--dir` flag): the receive rings are named `jnctrx` followed by 16 hex digits, and the send-ring file `cph` followed by 16 hex digits. The `--rx-size` flag sets the size of each receive ring and must follow the ring's size rules for the page size of that directory.

[Running switch-junctiond](../junction/running.md) covers the flags and the directory.

The other direction, from many applications to the daemon, needs a ring with many writers. That is [cephalopod](cephalopod.md).
