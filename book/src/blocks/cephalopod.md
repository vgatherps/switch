# cephalopod: many writers, one reader

The [receive ring](rings.md) has one writer and one reader. The send direction of junction is different: every application on the machine sends to the same daemon, often from several threads, and the daemon must still see each application's sends in the order it made them. cephalopod is the set of crates that does this. It is a general library for sending ordered messages from many processes to one consumer over shared memory, and junction's send path is built on it.

This page explains the idea, runs the library's two example programs, and then states the rules a program using it can rely on.

## The idea

A ring with many writers needs the writers to agree on who writes where, which costs locks or atomic operations on every push. cephalopod sidesteps that by giving every CPU its own ring. A writer always pushes into the ring of the CPU it is running on at that moment. Only one thread runs on a CPU at a time, so each ring has one writer at any instant, and no two writers ever touch the same ring.

```text
   producer A (on CPU 0) ---push---> ring 0 --+
   producer B (on CPU 1) ---push---> ring 1 --+
   producer C (on CPU 1) ---push---> ring 1 --+--> one consumer reads
   producer A (moved to CPU 3) ----> ring 3 --+    every ring in turn
                                     ring 2 --+
            one ring per possible CPU, all in one shared-memory file
```

The consumer reads all the rings. A producer that the operating system moves to another CPU continues in a different ring, so one producer's messages can end up spread over several rings. To put them back in order, every message carries two numbers:

- a stream id, which identifies the producer's connection. A producer gets one when it connects and uses it on every message.
- a sequence number, which counts the producer's messages on that stream: 0, 1, 2, and so on.

The consumer delivers each stream's messages in sequence-number order, whichever rings they are in. That puts a stream back together after its producer moved between CPUs. It does not sort messages that a producer put into one ring in the wrong order; the [rules for producers](#rules-for-producers) below keep that from happening.

The work splits across the crates like this. `ceph-ring` is the rings: `Reader` creates the shared file and reads it, `Writer` attaches to it and pushes. `ceph-fifo` wraps a `Reader` in a `Fifo` that sweeps the rings and puts each stream back in order. `ceph-control` is a small Unix-socket service that hands each connecting producer its stream id and the path of the shared file. `ceph-proto` holds the two message types that `ceph-control` and `ceph-fifo` exchange, and `ceph-rseq` holds the low-level push that `ceph-ring` uses.

## A sequencer and a player

The `ceph-fifo` crate has two example programs. `sequencer` is the consumer: it creates the rings, starts the control service on a thread, and prints every message it receives. `player` is a producer: it connects, pushes a number of messages, ends its stream, and waits for the consumer to confirm.

```rust,ignore
{{#include ../../../cephalopod/crates/fifo/examples/sequencer.rs}}
```

The sequencer creates a `Reader` with 2 MiB rings and records of at most 4096 bytes, in the directory given on the command line. It makes two channels, one for the control service to announce streams opening and closing (`Control`) and one for the `Fifo` to report finished streams back (`Retire`). `Listener::bind` takes the socket path, the shared file's name for producers to attach to, and the maximum number of streams, and `listener.run()` serves connections until the program exits. `Fifo::new` takes the reader, the same maximum number of streams, and the other ends of the two channels. Then the sequencer calls `next_message()` forever; `None` means nothing is ready right now.

```rust,ignore
{{#include ../../../cephalopod/crates/fifo/examples/player.rs}}
```

The player calls `ceph_control::connect` on the sequencer's socket and gets a `ceph_control::Session` (cephalopod's own type, unrelated to junction's `Session`) holding its stream id and the shared file's path. `Writer::attach` maps the rings. Each `try_push` sends one message with the next sequence number, retrying while the ring is full. `try_finish(stream, count)` sends the end-of-stream record, numbered one past the last message. The player then waits until `session.closed()` is true, which happens when the sequencer has delivered the end record and closed the connection.

To run them on Linux, start the sequencer in one terminal with a socket path and a directory for the shared file. It runs until you stop it.

```text
$ cargo run --release -p ceph-fifo --example sequencer -- /tmp/ceph.sock /dev/shm
```

In a second terminal, run a player against the same socket, here with three messages. You can run any number of players, one after another or at the same time.

```text
$ cargo run --release -p ceph-fifo --example player -- /tmp/ceph.sock 3
done
```

The sequencer prints one line per message, with the payload length (each payload is the sequence number as text), then `end` when the end record arrives:

```text
stream=1 seq=0 len=1
stream=1 seq=1 len=1
stream=1 seq=2 len=1
end
```

The first player gets stream id 1 (the first stream place, 0, in the high 32 bits and the first unique id, 1, in the low 32 bits); later players get other ids.

One player's run, from connecting to `closed()`, looks like this:

```text
 player                         control service           Fifo (consumer)
   | connect(socket) ---------------> |                          |
   |                                  |-- Control::Open -------->|
   | <---- stream id + file path ---- |                          |
   | Writer::attach(path)             |                          |
   | try_push(.., seq 0) ======================= ring ==========>| delivers seq 0
   | try_push(.., seq 1) ======================= ring ==========>| delivers seq 1
   | try_finish(.., seq 2) ===================== ring ==========>| delivers end
   |                                  | <-- Retire::Finished ----|
   | <---- socket closed ------------ |-- Control::Close ------->|
   | closed() == true                 |                          |
```

## Rules for producers

The library does not number messages for you: every push takes the stream id and the sequence number from the caller. The ordering described in the next section holds only if producers follow three rules.

Each stream's messages have to be committed in sequence-number order. The simplest way is one pushing thread per stream. If several threads share one stream, they must serialize taking the next number and pushing it, so that no thread can take number 6 and push it while another thread holds 5 and has not pushed yet. Two pushes to a stream from different CPUs are fine; two pushes into the same CPU's ring in the wrong order are not, because the later number would stall that ring with the missing one queued behind it.

Move to the next sequence number only after a push succeeds. When `try_push` returns `Full`, retry with the same number; a skipped number is a gap the consumer waits for.

Sequence numbers stay below 2^63. The top bit of the number marks the end-of-stream record that `try_finish` writes.

## What a program can rely on

### Pushing and backpressure

`try_push(payload, stream, seq)` returns `Ok(())` once the message is committed in a ring, where the consumer will find it. It returns `Err(PushError::Full)` when the ring of the CPU the thread is on has no room. That is a property of one CPU's ring, not of the whole set. Space comes back after the consumer has delivered messages: it returns their space to the writers at the start of its next sweep. The usual response is to retry. It returns `Err(PushError::TooLarge)` when the payload is longer than the `max_record` the consumer chose; retrying will not help.

`try_push_with_header(header, payload, stream, seq)` writes a short header and the payload as one record, so a caller can prefix its own fields without copying the payload into a new buffer first. The header must be a multiple of 8 bytes and at most 32 bytes, or the call panics, and `max_record` limits header and payload together. This is the call junction uses.

A `Writer` is `Send` and `Sync`. One `Writer` can be shared by several threads; each push goes to whichever CPU's ring the calling thread is on. Pinning a producer thread to one CPU keeps all its pushes in one ring, but the ordering works without it.

### Ordering

Within a stream, messages are delivered in sequence-number order, starting at 0, each number exactly once. The consumer looks at the oldest message in each ring:

- if it is the next number its stream expects, it is delivered;
- if its number is lower than expected, or its stream is unknown or already closed, it is discarded;
- if its number is higher than expected, that ring stalls: the consumer leaves it alone until the missing message turns up in another ring.

```text
 stream S expects seq 0

   ring 0: [S:1] [S:2]      front is S:1, ahead of 0  -> ring 0 stalls
   ring 1: [S:0]            front is S:0              -> deliver S:0, expect 1

 next sweep:
   ring 0: [S:1] [S:2]      front is S:1, expected    -> deliver S:1, then S:2
```

A stall holds up the whole ring, including messages of other streams queued behind the stalled one, until the gap is filled.

Messages of different streams are delivered in the order the consumer comes across them as it sweeps the rings. The consumer takes up to 64 messages in a row from one ring before moving to the next, so one busy ring cannot keep the others waiting indefinitely.

### Ending a stream

`try_finish(stream, seq)` pushes the end record, with the sequence number after the last message. It only places the record in a ring. The consumer delivers it as a message whose `end()` is true and whose payload is empty; when that message is dropped, the `Fifo` reports the stream finished, and the control service closes the producer's connection and frees the stream's place for a new producer.

A producer has to keep its `ceph_control::Session` alive until `closed()` returns true. The control service treats a connection that closes as a producer that has gone away: it tells the `Fifo` to close the stream, and records of that stream that have not been delivered yet are then discarded.

### Kicking a stream

A gap that is never filled would stall rings forever. When a full sweep delivers nothing, discards nothing and finds no newly stalled ring, while some rings are stalled, the `Fifo` looks for a stream whose expected message is in none of the stalled rings. It drops that stream, which is called kicking it, and sends `Retire::Kicked` to the control service, which closes the producer's connection. If every expected message is present somewhere, a producer committed its own messages out of order within a ring, and the `Fifo` kicks the first stalled stream. Nothing more from a kicked stream is delivered. The producer sees `closed()` become true even though it never finished.

In junction, a kick ends the application's session; [Closing sockets and ending a session](../junction/closing.md) says what the application sees.

### Limits and setup

The maximum number of streams is set twice, in `Listener::bind` and in `Fifo::new`, and the two values must be the same; the `Fifo` panics when the control service announces a stream beyond its own maximum. A producer that connects while all streams are in use has its connection closed without a reply, so `connect` returns an error.

`Reader::create` panics if `ring_size` is not a power of two and a multiple of the page size of the directory, or if `max_record` does not fit in a ring (it must be at most `ring_size` minus 40). The file holds one ring for each CPU the machine can have, as listed in `/sys/devices/system/cpu/possible`. `Writer::attach` reads the layout from the file and refuses, with `InvalidData`, a file whose layout is invalid or made for a different number of CPUs. The `Reader` owns the file's name and removes it when dropped; writers that are already attached keep working.

Both sides must trust each other: the consumer reads layout and lengths straight from the shared file.

## How a push survives being moved

A thread can be interrupted or moved to another CPU at any instruction, including halfway through writing a record into "its" ring. cephalopod handles that with a Linux feature called restartable sequences (rseq). The push runs as a short critical section registered with the kernel: if the kernel preempts the thread or moves it to another CPU before the section finishes, it sends the thread to an abort path instead of resuming in the middle. The half-written record was never committed, so the push simply starts again, now on the new CPU's ring.

A record becomes visible to the consumer only when its push completes; until then the consumer ignores whatever bytes are there. The consumer only reads the rings and never writes into them. `ceph-rseq` contains this critical section and the record layout; nothing outside `ceph-ring` calls it.

## Platforms

On Linux, cephalopod needs glibc on x86_64 or aarch64; other Linux targets fail to compile. glibc registers rseq for each thread it starts. `Writer::attach` returns an error of kind `Unsupported` if the calling thread has no rseq registration, and a push from a thread without one panics.

On macOS the crates build and run with one ring guarded by a spinlock instead of per-CPU rings, and print a warning the first time a push happens. This is for development on a laptop. The `dir` setting is ignored there; the file is a POSIX shared-memory object.

## Where junction uses it

The junction daemon creates one `Reader` for all sessions, with 1 MiB rings and a `max_record` large enough for one maximum-size send, and runs a `Fifo` on its network thread. Each `Session` attaches a `Writer` and is one stream: its `send_tcp` and `send_udp` calls push a record with a small header, and `Session::finish` pushes the end record. `PushError::Full` reaches the application as `SendError::Full` (or `FinishError::StreamFull` from `finish`), which is why a send through the daemon can report the shared send ring as full. The daemon assigns stream ids over its own control socket, so `ceph-control` is not involved. [What applications share](../junction/sharing.md) explains what that means for applications running side by side.

This is the last page of the part. Next comes [Measuring latency under load](../measuring/index.md).
