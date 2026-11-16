# switch-frame: packet-buffer handles

Every byte your program sends or receives through switch passes through a frame. [Frames](../net/frames.md) in the switch-net chapter shows how a task gets one to send and what a received one holds. This page, the first of the [Under the hood](index.md) part, looks at the frame types themselves, from the `switch-frame` crate: what you may do with each, when the memory behind a frame is given back, and how a library creates frames over memory it manages.

## Two types

A frame is a handle to a run of bytes that something else owns: a DPDK packet buffer, a record in a shared-memory ring, a buffer in a pool. Holding the handle keeps those bytes alive. Dropping the last handle gives them back to their owner, which this crate calls the producer.

There are two handle types.

`OwnedFrame` is the only handle to its bytes, so it can change them. It dereferences to `&[u8]` and `&mut [u8]`. This is the type you fill before sending and the type a received packet arrives as.

`ImmutableFrame` is a shared, read-only handle. It dereferences to `&[u8]` and implements `Clone`. Cloning copies no bytes: it adds one to a count kept next to the data and returns another handle to the same bytes.

You move from one to the other with two methods:

```text
                   freeze()
   OwnedFrame  ----------------->  ImmutableFrame  --clone()-->  ImmutableFrame
   (sole handle,                   (shared, read-only)            (same bytes)
    writable)   <-----------------
                   try_own()
                   Ok only when this is the last handle
```

`freeze` never fails. `try_own` returns `Ok(OwnedFrame)` when no other handle to the bytes is alive, and gives the frame back as `Err` otherwise. A clone or a slice anywhere else keeps the bytes shared.

Both types can be narrowed without copying. `slice(range)` consumes the frame and returns one that covers only `range` of the current bytes; it exists on both types. `OwnedFrame::truncate(len)` shortens the frame in place and keeps its start, which is how you trim a send frame to the bytes you actually wrote. A slice of a shared frame still counts as a handle to the original bytes, so the memory goes back only after the slice is dropped too.

The behavior is easiest to see in the crate's own test, which makes one frame, writes to it, freezes it and clones it twice:

```rust,ignore
{{#include ../../../switch-frame/src/lib.rs:clones}}
```

The producer is told nothing until the last of the three handles is gone.

## Rules

Frames are single-threaded. Neither type is `Send` or `Sync`, and the count beside the data is a plain integer, not an atomic. A frame stays on the thread that received or allocated it. To hand packet data to another thread, copy the bytes out.

The count is 16 bits wide. A frame's bytes can have at most 65,535 live handles at once; the clone that would make one more panics, and the existing handles stay valid.

`slice` panics when the range runs past the end of the frame or its start is after its end, and `truncate` panics when asked to make the frame longer. A frame consumed by a failed `slice` is still dropped normally, so its bytes still go back to the producer.

When the last handle is dropped, the producer's release function runs on the thread that dropped it, right there inside the `drop`. For frames from the Stack that means the packet buffer goes back to its pool; for frames from the junction receive ring it means the record is marked free and the next poll can [reclaim](rings.md#reclaim) its space. In both cases the memory stays in use for as long as you hold any handle to it, which is why [holding frames has a cost](../net/frames.md).

A frame does not depend on the object that gave it to you. It stays valid after the socket, the `Net`, the Stack or the `Session` it came from is gone, because it keeps the producer's storage alive by itself.

## Where frames come from

Three producers in the workspace create frames:

- the Stack, for received packets and for the send frames of the local backend, both backed by DPDK packet buffers;
- switch-net's junction backend, for send frames, from a pool of ordinary heap buffers;
- the junction receive ring, for received records, pointing into shared memory.

Your code handles all of them the same way. The producer is invisible except through what dropping the last handle releases.

## Writing a producer

This section is for library authors who want to hand out frames over their own memory. Application code never does this.

A frame is a data pointer, a length, the distance back from the data to a 16-bit count, a release function and a context pointer. The count has to sit in the same allocation as the data, before it:

```text
 one allocation owned by the producer
+-------+-------------------------------------------+
| count |  data ...                                 |
|  u16  |                                           |
+-------+-------------------------------------------+
    ^      ^
    |      frame.data_ptr()           (slice() moves this forward
    |                                  and remembers the distance)
    rc = data - offset

 clone(): count += 1           drop(): count > 1  -> count -= 1
                                       count == 1 -> release(rc, ctx)
```

You create the first handle with the unsafe constructor `OwnedFrame::from_raw_parts(data, len, rc, release, ctx)`. Its safety contract asks for these things: the count is 1 and belongs to this frame; `rc` is aligned for `u16` and the two count bytes end at or before `data`; both pointers come from the same allocation; the distance from the count to the end of the data fits in a `u32`; and `data..data + len` stays valid for reading and writing while the count is nonzero.

The release function has the signature `unsafe extern "C" fn(rc: NonNull<u16>, ctx: *mut ())`. It is called exactly once, when the last handle is dropped, with the count still at 1. `ctx` is whatever you passed to `from_raw_parts`; producers use it to find their pool. A producer reads it back from a live frame with `ctx()`.

Frames outlive the objects that handed them out, so a producer has to keep everything its release function touches alive until the last reference is gone, whenever that is. The usual way is to give each frame its own counted reference to the producer's state. switch-net's junction backend does this for its send-frame pool: `take` passes `Rc::into_raw` of the pool as `ctx`, so every live frame holds the pool, and `release_buffer` turns `ctx` back into that `Rc` with `Rc::from_raw`, returns the buffer to the pool, and drops the frame's reference.

```rust,ignore
{{#include ../../../switch-net/src/remote.rs:frame_pool}}
```

Here the count is the first two bytes of each boxed buffer and the payload follows it, which is the layout the diagram above shows. This is the pattern to copy.

The test module of `switch-frame` has a smaller producer, a test fixture: storage is a boxed struct with the count first and eight bytes after it, and release frees the box. Its `ctx` is a plain pointer to a `Producer` that the test borrows, so it is only correct because each test keeps its `Producer` alive until every frame is dropped. A real producer cannot rely on that.

```rust,ignore
{{#include ../../../switch-frame/src/lib.rs:producer}}
```

Three more methods exist for producers. `OwnedFrame::set_len` (unsafe) changes the frame's length within storage the producer knows it owns, for example to append bytes after the current end. `data_ptr()` returns the data pointer with its original provenance. `into_raw_parts()`, on both types, turns a handle into a `RawParts` value without touching the count. The Stack uses it when it queues a frame for transmission: the queued packet takes over the frame's reference. Whoever holds a `RawParts` that carries the last reference finishes it by calling its `release` with its `rc` and `ctx`.

The next page, [Shared memory and the receive ring](rings.md), shows one producer in full: the junction receive ring, whose records become frames that point into a file shared between two processes.
