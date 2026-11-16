# switch-sockperf-wire

Message framing for the sockperf 3.10 wire format, with no dependencies and no DPDK. `switch-nic`'s `sockperf-server` mode uses it to answer kernel `sockperf` clients.

Used by `switch-nic` only.

## API (`sockperf` module)

- `HEADER` (14 bytes) and `MAX_MESSAGE` (65507, the largest UDP payload).
- `validate_udp(message)`: checks that a datagram is a whole message, i.e. at least a header and a length field (bytes 10..14, big endian) equal to the datagram length.
- `reply(message)`: for a validated message, clears the reply-request bit and returns true when the flags ask for a reply (`flags & 7 == 3`); otherwise it changes nothing and returns false.
- `Framer::new(max)`, `push(bytes, complete)`, `reset()`: splits a TCP byte stream into messages. `push` calls `complete` for each whole message in order and keeps only the trailing partial one. A length below `HEADER` or above `max` returns `Malformed`.

## Test

```sh
cargo test -p switch-sockperf-wire
```

Plain cargo, no DPDK. `make test-switch-nic` also runs these tests.
