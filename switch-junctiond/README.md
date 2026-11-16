# switch-junctiond

See [Running switch-junctiond](../book/src/junction/running.md) in the switch book.

One Linux process owns one DPDK port and serves junction TCP/UDP sessions over shared memory and a Unix control socket. It uses the same `switch_netconf::Config` JSON file as `switch-bench`; no junction configuration section is needed.

```sh
cargo build --locked --release -p switch-junctiond --bins
sudo taskset -c 2 target/release/switch-junctiond --config /etc/switch/nic.json \
    --dir /run/switch-junctiond --control-cpus 0-1
```

Connect applications with `Session::connect(Path::new("/run/switch-junctiond/control.sock"), max_sockets)`.

| Flag | Default | Meaning |
| --- | --- | --- |
| `--config` | required | Existing netconf JSON: device, addresses, MTU, offloads and EAL arguments. |
| `--dir` | `/run/switch-junctiond` | Control socket, persistent lock file, ingress and per-session RX backings. Created mode 0700 if absent. Relative paths are resolved before publishing backing names. |
| `--rx-size` | `2097152` | Bytes per session RX ring; power of two, a backing-page multiple, at most 2 GiB. |
| `--max-sessions` | `64` | Concurrent session slots. Must be positive. |
| `--max-sockets` | `1024` | Maximum sockets requested by one session; 1–65535. |
| `--tcp-sockets` | `1024` | Global Stack TCP slots. Must be positive. |
| `--udp-sockets` | `1024` | Global Stack UDP slots; zero disables UDP. |
| `--control-cpus` | `0` | Housekeeping CPU list, including ranges (`0,2-3`). Every listed CPU must be available in the process's cpuset. |

The main thread is the busy-polling network thread. Pin it to **exactly one CPU before launch** with `taskset`; an unpinned launch fails before opening EAL or the NIC. Reserve another isolated CPU for the app. Set `--control-cpus` to housekeeping CPUs, and include them in the container's cpuset even though `taskset` restricts the network thread. In single-CPU test containers, network and control may share that CPU. The control thread handles IPC; it does not poll packets.

The parent temporarily switches to the housekeeping mask while spawning the control thread, then restores the network CPU; the control thread therefore inherits the correct affinity before executing. The same `--control-cpus` set is passed to `switch_open` for EAL helpers (including `dpdk-intr`), so helpers stay off the network CPU even without kernel CPU isolation. Supply housekeeping CPUs distinct from the network CPU; a one-CPU test environment necessarily shares it. Control configuration and socket binding are validated before the NIC opens. If the control thread returns or panics, a drop guard stops the network loop; teardown still closes the port and removes the IPC files while the directory lock is held.

Temporary control accept shortages (`EMFILE`, `ENFILE`, `ENOBUFS`, `ENOMEM`) defer new accepts to a later pass without ending established sessions. A new connection that cannot be made nonblocking is dropped; other accept errors remain fatal.

The host advertises TX frame capacity `MTU - 28`, and cephalopod maximum record size `frame_capacity + 24`. Ingress rings are 2 MiB per possible CPU. The default receive and send ring sizes fit both ordinary pages and 2 MiB hugepages when `--dir` is on a hugepage filesystem; a custom `--rx-size` must remain a multiple of that filesystem's page size. The Stack shares 4096 frame-pool mbufs across sockets, with RX staging 128, TX pending 64, and 256 neighbor entries. TCP/UDP slot limits are global, independent of per-session limits.

Use a dedicated, daemon-owned directory. `switch-junctiond` holds a nonblocking exclusive `flock` on `DIR/lock` for its lifetime; a second daemon fails without disturbing the first. The lock inode remains after exit and must not be removed while a daemon is running. After acquiring the lock, startup removes stale `control.sock` sockets and regular backing files whose entire name is `cph` or `jnctrx` followed by 16 lowercase hex digits. Those names are reserved for this daemon. Other names, directories and symlinks are untouched; a non-socket at `control.sock` causes an error rather than being deleted. Do not share the directory with another service or place unrelated files in the reserved namespace.

SIGINT and SIGTERM stop polling. Disconnecting the network channels makes the control thread send `End(HostShutdown)` to live sessions; the daemon joins that thread, drops the Stack's sockets and closes the port before releasing directory ownership. Normal shutdown removes its IPC files but keeps `lock`. SIGKILL cannot notify clients or clean files; the next successful owner removes the stale IPC names before serving again.

Process tests require Linux, DPDK, `taskset`, `ip`, CAP_NET_ADMIN and `/dev/net/tun`. They launch the real binary on a tap vdev, use a kernel UDP peer, and cover HostShutdown, directory exclusion and SIGKILL restart cleanup. Run alongside the crate's in-process tests with `cargo test --locked -p switch-junctiond -- --test-threads=1` in the DPDK container.

The helper-affinity process test needs a container cpuset containing CPUs 0 and 2 and an unpinned test runner. It starts the network thread on 0 with `--control-cpus 2` and reads every daemon thread's `/proc/PID/task/TID/status` and `comm`: the network thread must have affinity `0`, and every other thread must have affinity `2`. It skips when those CPUs are unavailable.
