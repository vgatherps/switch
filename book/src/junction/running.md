# Running switch-junctiond

This page starts the daemon on a tap device, explains each flag, and shows how to tell that it is serving and how to stop it. The [previous page](index.md) explains what the daemon is for.

## What you need

The daemon runs on Linux only and needs DPDK, so build and run it in the same environment as any other DPDK program (see [Setting up a build environment](../getting-started/environment.md)):

```sh
cargo build --locked --release -p switch-junctiond --bins
```

It also needs:

- a [network config file](../nic/config.md) describing the NIC port. The daemon reads the same file an application owning the NIC would read; there is no daemon-specific section.
- one CPU for its network thread, and at least one other CPU for its housekeeping work (in a one-CPU test container both can share that CPU).
- a directory of its own, where it puts its control socket and shared-memory files. The default is `/run/switch-junctiond`.

For a NIC bound to `vfio-pci` you also need hugepages and the rest of [Preparing a Linux machine](../nic/host-setup.md). A tap device needs none of that, which makes it the easiest way to try the daemon. This config gives the daemon a tap interface named `jn0` with address 10.77.0.2. Save it as `jn0.json`:

```json
{
  "version": 1,
  "device": { "vdev": "net_tap_jn0,iface=jn0" },
  "mac": null, "ipv4": "10.77.0.2/24", "gateway": null, "mtu": 1500,
  "paths": [], "offloads": "auto", "rx_timestamps": false, "descriptors": 512,
  "eal": { "file_prefix": "jn0", "extra": ["--no-huge", "-m", "512", "--no-shconf"] }
}
```

The `eal.extra` arguments run DPDK without hugepages, in 512 MB of ordinary memory, and with no shared config files, which a tap device allows.

## Starting it

The daemon's main thread becomes its network thread, which busy-polls the NIC forever. It must already be allowed on exactly one CPU when the daemon starts, so launch it under `taskset`:

```sh
taskset -c 2 target/release/switch-junctiond \
    --config /etc/switch/nic.json \
    --dir /run/switch-junctiond \
    --control-cpus 0-1
```

Run it as root. Here the network thread runs on CPU 2, and everything else the daemon starts (its control thread and DPDK's [helper threads](../nic/cpus.md)) runs on CPUs 0 and 1. This is the shape of a real deployment on a NIC; [Connecting an application through switch-net](apps.md#running-it) runs the daemon on the tap config above, step by step.

Apart from DPDK's own `EAL:` lines while the NIC opens, the daemon prints nothing. Its own errors are one line on stderr, `switch-junctiond: <reason>`.

## Flags

Every flag takes a value, written as `--name value`. Unknown flags, repeated flags and flags without a value are errors; the daemon then prints the error followed by its usage text.

| Flag | Default | Meaning |
|---|---|---|
| `--config PATH` | required | The network config file. |
| `--dir PATH` | `/run/switch-junctiond` | The daemon's directory. Created with mode 0700 if it does not exist. |
| `--rx-size N` | `2097152` | Size in bytes of each session's receive ring. A power of two, at most 2 GiB, and a multiple of the page size of the filesystem holding `--dir`; the default suits ordinary pages and 2 MiB hugepages. |
| `--max-sessions N` | `64` | How many sessions can be connected at once. Must be positive. |
| `--max-sockets N` | `1024` | The most sockets one session may ask for when it connects. 1 to 65535. |
| `--tcp-sockets N` | `1024` | TCP sockets in the Stack, shared by all sessions. Must be positive. |
| `--udp-sockets N` | `1024` | UDP sockets in the Stack, shared by all sessions. 0 is allowed. |
| `--control-cpus LIST` | `0` | CPUs for the control thread and DPDK's helper threads, as a Linux CPU list such as `0,2-3`. |

`--max-sockets` limits each session, while `--tcp-sockets` and `--udp-sockets` limit the whole daemon. A session that asked for 100 sockets can still be refused a TCP socket when other sessions hold all of `--tcp-sockets`; [What applications share](sharing.md) covers this.

The daemon also fixes a few sizes itself. It advertises a frame capacity of MTU − 28 bytes to every session (the largest UDP payload that fits the MTU), and it gives each CPU 2 MiB of send ring. Both ring defaults fit a `--dir` on a filesystem with 2 MiB hugepages; custom receive sizes must still be multiples of that filesystem's page size. [Connecting](apps.md#connecting) shows how an application reads these values.

## CPUs

The daemon uses CPUs the same way an application that owns its NIC does ([The pinned CPU and helper threads](../nic/cpus.md) explains the terms). On a four-CPU machine where the application also has its own pinned CPU, a typical layout is:

```text
 CPU 0, 1   housekeeping: OS, interrupts, the daemon's control thread,
            DPDK helper threads              (--control-cpus 0-1)
 CPU 2      the daemon's network thread       (taskset -c 2)
 CPU 3      the application's drive loop      (taskset -c 3)
```

Two rules are checked at startup. The network thread must be allowed on exactly one CPU, otherwise the daemon exits before it opens anything. And every CPU in `--control-cpus` must be inside the process's cpuset, the set of CPUs the machine or container makes available (in Docker, `--cpuset-cpus`). This is a different thing from the `taskset` mask: `taskset -c 2` restricts the network thread, while the control CPUs only need to be available to the process. The daemon moves its other threads onto `--control-cpus` itself, so they stay off the network CPU even when no CPU is isolated in the kernel, as long as `--control-cpus` does not include the network CPU (the daemon does not check this).

## Its directory

The directory holds everything the daemon shares with applications:

```text
/run/switch-junctiond/
  lock                    held by the running daemon; never removed
  control.sock            Unix socket that applications connect to
  cph<16 hex digits>      the send rings, one per CPU, in one file
  jnctrx<16 hex digits>   one receive ring per connected session
```

Only one daemon can use a directory. The first one holds a lock on `lock` for its whole life; a second daemon started on the same directory exits with an error saying the directory is owned by another daemon, and does not touch the files.

When it starts, the daemon removes leftovers from an earlier run that was killed: a stale `control.sock`, and regular files named exactly `cph` or `jnctrx` followed by 16 lowercase hex digits. Nothing else in the directory is touched, and a `control.sock` that is not a socket is an error, not something to delete. Give the daemon a directory of its own.

The directory is created with mode 0700 and the ring files with mode 0600, so run applications as the same user as the daemon, or as root.

## Knowing it is ready

The daemon prints no ready message. Startup goes in this order:

1. check that the network thread is pinned to one CPU;
2. read the config file;
3. lock the directory and remove stale files;
4. create the send ring;
5. create `control.sock`;
6. open the NIC (this can take seconds on real hardware);
7. start the control thread and begin serving sessions.

`control.sock` appears at step 5, before the NIC is open. An application that connects in that window is not refused: `Session::connect` waits for the daemon's answer, which comes once the daemon reaches step 7, or fails if the daemon exits first.

`Session::connect` blocks until the daemon answers, and has no timeout of its own. Retrying helps only before `control.sock` exists. So a script that starts the daemon and then an application waits until `control.sock` exists and then starts the application, whose connect returns once the daemon serves (see [Connecting](apps.md#connecting)). To bound the wait, call `connect` on a helper thread and give up, or stop the daemon, after your deadline; the daemon's own tests use such a watchdog.

## Stopping it

SIGINT or SIGTERM stops the daemon. It stops polling, tells each connected session that the daemon is shutting down, drops all sockets, closes the NIC port, removes `control.sock` and the ring files, and exits with status 0. Only `lock` stays in the directory.

An application sees the shutdown as session loss: in switch-net, the receive step of the next round returns `TurnError::HostLost` (from `switch_net::round`, as `RoundError::Turn`), and a raw `Session` poll returns `Lost::Ended(EndReason::HostShutdown)`. The shutdown notice is sent on a best-effort basis, so an application can see `Lost::Disconnected` instead. Treat both the same way. [Closing sockets and ending a session](closing.md#handling-loss) describes what to do on loss.

If the daemon is killed with SIGKILL or crashes, it cannot notify anyone or clean up. Applications see `Lost::Disconnected`, and the ring files stay in the directory until the next daemon started there removes them.

## When it fails to start

Every failure prints one line to stderr, `switch-junctiond: <reason>`, and exits with status 1. The common ones are:

| Message begins with | Cause |
|---|---|
| `network thread must be pinned to exactly one CPU` | started without `taskset -c N` (or with a mask of several CPUs) |
| the config path | the config file is missing or invalid |
| `--dir:` ... `owned by another` | another daemon is running on this directory |
| `--dir:` ... `control.sock exists but is not a socket` | something else is using the name |
| `open: EAL helper CPU affinity:` | a CPU in `--control-cpus` is not available to the process, for example outside Docker's `--cpuset-cpus`; see [When the NIC won't open](../nic/troubleshooting.md) |
| `control:` | `control.sock` could not be created, or `--rx-size` does not suit the directory's page size |
| `open:` | the NIC could not be opened; see [When the NIC won't open](../nic/troubleshooting.md) |

While running, the daemon also exits with status 1 if its control thread fails or panics, or if its network loop fails. It exits with status 1 after a stop signal, too, when closing the NIC port or cleaning up the directory fails.

The daemon's own end-to-end tests start it this same way on a tap device; [Running a sweep on one machine](../measuring/local.md) uses it for benchmarks.

Next: [Connecting an application through switch-net](apps.md).
