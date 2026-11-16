# Under the hood: frames, shared memory and rings

When an application uses the junction backend, its sockets live in another process. The [daemon](../junction/index.md) owns the NIC and runs the Stack; the application only exchanges data with it. This part of the book explains the pieces that carry that data between the two processes, and the frame type that every layer of switch hands to application code.

You do not need any of this to write a program with switch-net. Read it when you want to know what happens to a packet between the daemon and your task, when you are writing a test or a tool that talks to these crates directly, or when you want to use one of them on its own. Each crate here works without DPDK and without the rest of switch.

## Two directions, two mechanisms

Data crosses between an application and the daemon through shared memory: a file that both processes map into their address space, so a write by one is visible to the other without a system call. The two directions use different structures, because they have different shapes.

Received packets go from one writer, the daemon, to one reader, your session. Each session gets its own receive ring. The daemon writes each received TCP chunk or UDP datagram into it as one record, and the session hands each record to your code as a frame that points straight into the ring. Nothing is copied on the way out.

Sends go the other way, from many applications to one daemon. Every session pushes its sends into one shared set of send rings, with one ring for each CPU on the machine. A sender always writes to the ring of the CPU it is running on, so two senders never write the same ring at the same moment. The daemon reads all the rings and puts each session's sends back in the order the session made them, using a sequence number on every record.

The picture below shows both directions together with the third channel, the control socket, and the threads on each side that use them.

```text
 application process                                  switch-junctiond

 +--------------------------------+                  +----------------------------------+
 | your tasks / switch-net        |                  | network thread (one pinned CPU)  |
 |                                |                  |   busy-polls the NIC and Stack   |
 |  Session::send_*               |   send rings     |                                  |
 |    ceph-ring Writer -----------+--(one per CPU;---+--> ceph-fifo Fifo: merges the    |
 |                                |   each used by   |    rings into per-session order, |
 |                                |   every session  |    applies the sends             |
 |                                |   sending from   |                                  |
 |                                |   that CPU)      |                                  |
 |                                |                  |                                  |
 |  Session::poll_packets         |   receive ring   |                                  |
 |    slice_ringbuffer RingReader <--(one per -------+--- slice_ringbuffer RingWriter   |
 |      |                         |   session)       |    writes received packets       |
 |      v                         |                  |                                  |
 |    switch-frame OwnedFrame     |                  +----------------------------------+
 |    (points into the ring)      |                  | control thread (housekeeping     |
 |                                |  control socket  |   CPUs): sessions, socket opens  |
 |  Session opens, closes <-------+--(control.sock,--+-> and closes, events             |
 |  junction-ctl thread: reads    |   Unix socket)   |                                  |
 |    events from the socket      |                  |                                  |
 +--------------------------------+                  +----------------------------------+

    both kinds of ring: a shared-memory backing (a file in the daemon's --dir),
    mapped twice back to back
```

The control socket carries the rare messages that set up and end sessions and sockets, and the events that answer them. `Session::connect` starts one reader thread, `junction-ctl`, that receives those messages for the session. That protocol belongs to junction itself and is described in [The junction control protocol](../reference/junction-protocol.md); because control and data take different paths, they are not ordered against each other, as [Using a Session directly](../junction/session.md) explains.

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
