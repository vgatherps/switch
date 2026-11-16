# Under the hood: frames, shared memory and rings

When an application uses the junction backend, its sockets live in another process. The [daemon](../junction/index.md) owns the NIC and runs the Stack; the application only exchanges data with it. This part of the book explains the pieces that carry that data between the two processes, and the frame type that every layer of switch hands to application code.

You do not need any of this to write a program with switch-net. Read it when you want to know what happens to a packet between the daemon and your task, when you are writing a test or a tool that talks to these crates directly, or when you want to use one of them on its own. Each crate here works without DPDK and without the rest of switch. The previous part ended with [Stack errors and panics](../stack/errors.md); this part leaves the Stack behind and looks only at what connects an application to the daemon.

## Two directions, two mechanisms

Data crosses between an application and the daemon through shared memory: a file that both processes map into their address space, so a write by one is visible to the other without a system call. The two directions use different structures, because they have different shapes.

Received packets go from one writer, the daemon, to one reader, your session. Each session gets its own receive ring. The daemon writes each received TCP chunk or UDP datagram into it as one record, and the session hands each record to your code as a frame that points straight into the ring. Nothing is copied on the way out.

Sends go the other way, from many applications to one daemon. Every session pushes its sends into one shared set of send rings, with one ring for each CPU on the machine. A sender always writes to the ring of the CPU it is running on, so two senders never write the same ring at the same moment. The daemon reads all the rings and puts each session's sends back in the order the session made them, using a sequence number on every record.

```text
 application process                              switch-junctiond
+----------------------------+                  +------------------------------+
| Session                    |                  |                              |
|                            |   send rings     |                              |
|  send ---- ceph-ring ------+--(one per CPU,---+--> ceph-fifo: merges rings   |
|            Writer          |   all sessions)  |    into per-session order    |
|                            |                  |                              |
|                            |   receive ring   |                              |
|  poll <-- slice_ringbuffer-+--(one per -------+--- slice_ringbuffer          |
|           RingReader       |   session)       |    RingWriter                |
|    |                       |                  |                              |
|    v                       |                  |                              |
|  switch-frame OwnedFrame   |                  |                              |
|  (points into the ring)    |                  |                              |
+----------------------------+                  +------------------------------+
        both kinds of ring: a shared-memory backing, mapped twice back to back
```

The control messages that set up sockets and sessions take a third path, a Unix socket between the session and the daemon. That protocol belongs to junction itself and is described in [The junction control protocol](../reference/junction-protocol.md).

## The crates

| Crate | What it is | Who touches it |
|---|---|---|
| `switch-frame` | Handles to packet bytes: `OwnedFrame` and `ImmutableFrame`. | Every application. switch-net, switch-junction and the Stack all hand you these. |
| `shared-memory` | Named shared-memory files (backings) and the mirrored mapping that keeps records contiguous. | Nobody in application code; the two ring crates use it. |
| `slice_ringbuffer` | The receive ring: one writer, one reader, records returned as frames. | Applications see only its `PacketMeta`, re-exported by switch-junction. |
| `ceph-ring` | The per-CPU send rings: many writers, one reader. | switch-junction's `Session` writes; the daemon reads. |
| `ceph-fifo` | Reads all per-CPU rings and delivers each producer's records in sequence order. | The daemon. |
| `ceph-proto` | Small types shared by `ceph-fifo` and `ceph-control`. | The daemon and switch-junction, for their channel types. |
| `ceph-control` | A Unix-socket service that hands each producer its id. | Standalone cephalopod programs only. |
| `ceph-rseq` | The record layout and the low-level push used by `ceph-ring`. | Nobody directly. |

The five `ceph-*` crates together are called cephalopod. `ceph-control` is part of cephalopod for programs that use it on its own; the junction daemon does not use it, because junction has its own control socket that assigns sessions and passes the ring file names.

An application using switch-net touches only the frame types, and an application using a [`Session`](../junction/session.md) directly also sees `PacketMeta`, the small header that says which socket a received record belongs to.

## In this part

[switch-frame: packet-buffer handles](frames.md) explains the two frame types and their ownership rules, and shows how a library creates frames over its own memory.

[Shared memory and the receive ring](rings.md) explains backings, the mirrored mapping, and the receive ring that the daemon writes and your session reads, including why holding a frame holds ring space.

[cephalopod: many writers, one reader](cephalopod.md) explains the per-CPU send rings and how the reader restores each producer's order, with a runnable two-program example.

Start with [frames](frames.md).
