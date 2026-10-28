# ceph-control

The Unix-socket control plane of the sequencer: it assigns each producer a stream id and tracks the producer's session until the stream ends.

Depends on [`ceph-proto`](../proto/README.md) and `libc`. `ceph-fifo` uses it in tests only.

## API

- Consumer side: `Listener::bind(socket_path, shared_file_path, max_streams, control_tx, retire_rx)` binds the socket (removing a stale file first) and `Listener::run()` serves it. `run` assigns a free slot and a unique id and sends `Control::Open` on accept. It sends `Control::Close` when a producer disconnects, and on `Retire::Kicked` or `Retire::Finished` it closes the socket, frees the slot and sends `Control::Close`. `max_streams` must match `Fifo::new`. The shared-file path must fit in a `u16`.
- Producer side: `connect(path) -> Session { stream, path, .. }` reads the stream id and the shared-file path to pass to `Writer::attach`. `Session::closed()` turns true when the consumer has closed the socket.

Players send nothing after the handshake.

## Test

From `cephalopod/`:

```sh
make test    # Linux tests in Docker; tests/control.rs covers this crate
```
