How to write and run a switch program, from a first task to a complete WebSocket client.

1. [Your first task](crate::guide::first_task): run a task, get a value back from it and wait on a timer, with only cargo.
2. [Setting up for network programs](crate::guide::setup): get a Linux environment with DPDK, in a container or on a Linux machine.
3. [Your first network program](crate::guide::first_connection): a complete TCP client, from its `Cargo.toml` to its output.
4. [Tasks](crate::guide::tasks): spawning, sleeping, yielding, passing a value between tasks, and waking a task from another thread.
5. [The drive loop](crate::guide::drive_loop): what one round does, where its time comes from, and when the loop may sleep.
6. [Opening sockets](crate::guide::sockets): connecting, accepting and binding, and the errors each can return.
7. [Frames](crate::guide::frames): the buffers you send from and receive into.
8. [TCP streams](crate::guide::tcp): receiving, sending, waiting for room, and closing.
9. [UDP sockets](crate::guide::udp): receiving and sending datagrams.
10. [Timeouts and cancelling](crate::guide::timeouts): putting a time limit on a wait, and stopping a wait when something else happens.
11. [Shutting down](crate::guide::shutdown): the order in which a program takes itself apart.
12. [WebSocket](crate::guide::websocket): `wss://` and `ws://` clients, and a `ws://` server.
13. [TLS without WebSocket](crate::guide::tls): your own protocol over TLS 1.3.
14. [Sharing a network card](crate::guide::sharing): several programs on one card, through a daemon.
15. [A complete program](crate::guide::feed): a `wss://` market-data client, read from top to bottom.
