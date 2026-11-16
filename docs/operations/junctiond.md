# switch-junctiond

A card belongs to one process. `switch-junctiond` is a daemon that owns one card and lets several programs on the same machine use it: each program connects to the daemon through a socket in the daemon's directory instead of opening the card itself. The code a program needs for this is in the guide chapter Sharing a network card in the `switch` crate documentation (`make docs`).

The daemon runs on Linux and needs DPDK. Build it from the repository root:

```sh
cargo build --locked --release -p switch-junctiond --bins
```

It reads the same [network config file](config.md) as a program that owns the card, so a machine prepared as in [Preparing a Linux machine](host-setup.md) or [Running on EC2](ec2.md) is ready for it. Run it as root.

## Try it on a tap device

The daemon needs a CPU for its network thread and another for its other threads, and an application needs a CPU of its own, so use a container with four CPUs.

1. From the repository root, start a container from the Docker image:

   ```sh
   docker run --rm -it --init \
       --cap-add NET_ADMIN --device /dev/net/tun \
       --cpuset-cpus=0-3 \
       -v "$PWD:/work" -w /work \
       -v switch-cargo-registry:/usr/local/cargo/registry \
       -v switch-linux-target:/work/target \
       switch-dpdk-test:local bash
   ```

2. Inside it, build the daemon as above, and save this config as `jn0.json`. It gives the daemon a tap interface `jn0` with the address 10.77.0.2:

   ```json
   {
     "version": 1,
     "device": {"vdev": "net_tap_jn0,iface=jn0"},
     "mac": null,
     "ipv4": "10.77.0.2/24",
     "gateway": null,
     "mtu": 1500,
     "paths": [],
     "offloads": {"rx_checksum": false, "tx_tcp_checksum": false, "tx_udp_checksum": false},
     "rx_timestamps": false,
     "descriptors": 512,
     "eal": {"file_prefix": "jn0", "extra": ["--no-huge", "-m", "512", "--no-shconf"]}
   }
   ```

3. Start the daemon with its network thread on CPU 2 and its other threads on CPU 0, wait until it is serving, and give the Linux side of `jn0` its address:

   ```sh
   taskset -c 2 target/release/switch-junctiond --config jn0.json --control-cpus 0 &
   until [ -S /run/switch-junctiond/control.sock ] && ip link show jn0 >/dev/null 2>&1; do sleep 0.1; done
   ip addr add 10.77.0.1/24 dev jn0 && ip link set jn0 up
   ```

   The daemon prints DPDK's `EAL:` lines and nothing else. Its directory now holds the socket applications connect to:

   ```text
   $ ls /run/switch-junctiond
   control.sock  cph1858708a122c804e  lock
   ```

4. Run an application against it on CPU 3, as the guide chapter Sharing a network card shows with the `tcp_client_junction` example.

5. Stop the daemon:

   ```sh
   kill %1
   ```

   It exits with status 0 and leaves only `lock` in its directory.

## On a real card

On a prepared machine, the shape is the same:

```sh
sudo taskset -c 2 switch-junctiond --config dut.json --control-cpus 0-1
```

On EC2, after `deploy/hft-host.sh`, use the CPUs it chose:

```sh
. /etc/switch/host.env
sudo taskset -c "$JUNCTION_CPU" switch-junctiond --config /etc/switch/dut.json --control-cpus "$HOUSEKEEPING"
```

## Flags

Every flag takes a value, as `--name value`. An unknown, repeated or valueless flag prints the error followed by the usage text, and the daemon exits with status 1.

| Flag | Default | Meaning |
|---|---|---|
| `--config PATH` | required | The network config file. |
| `--dir PATH` | `/run/switch-junctiond` | The daemon's directory, created with mode 0700 if missing. |
| `--rx-size N` | `2097152` | Bytes of each application's receive ring. A power of two, at most 2 GiB, and a multiple of the page size of the filesystem holding `--dir`. |
| `--max-sessions N` | `64` | How many applications can be connected at once. |
| `--max-sockets N` | `1024` | The most sockets one application may ask for when it connects, 1 to 65535. |
| `--tcp-sockets N` | `1024` | TCP sockets shared by all applications. |
| `--udp-sockets N` | `1024` | UDP sockets shared by all applications; 0 allowed. |
| `--control-cpus LIST` | `0` | CPUs for the daemon's other threads and DPDK's, as a Linux CPU list such as `0,2-3`. |

`--max-sockets` limits each application, `--tcp-sockets` and `--udp-sockets` the whole daemon: an application that asked for 100 sockets can still be refused a TCP socket when others hold all of `--tcp-sockets`.

## CPUs

The daemon's main thread becomes its network thread, which polls the card without pause. It must be allowed on exactly one CPU when the daemon starts, so start it under `taskset -c N`; otherwise it exits before opening anything. The daemon then moves its other threads, and DPDK's, onto `--control-cpus`, so they stay off the network CPU even when no CPU is isolated. Keep the network CPU out of `--control-cpus`; the daemon does not check this.

Every CPU in `--control-cpus` must be available to the process: in Docker, inside `--cpuset-cpus`. A typical layout on four CPUs, with one application:

```text
 CPU 0, 1   Linux, interrupts, the daemon's other threads   (--control-cpus 0-1)
 CPU 2      the daemon's network thread                    (taskset -c 2)
 CPU 3      the application                                (taskset -c 3)
```

## The directory

The directory holds a lock file, `control.sock`, which applications connect to, and the shared-memory files that carry packets between the daemon and the applications. The files are readable only by their owner, so run applications as the same user as the daemon, or as root.

Only one daemon can use a directory; a second one exits with `--dir: /run/switch-junctiond is owned by another switch-junctiond: ...`. When it starts, the daemon removes the socket and shared-memory files left by an earlier daemon that was killed. Give it a directory of its own.

## Knowing it is ready

The daemon prints no ready message. `control.sock` appears before the card is open, and an application that connects then waits until the daemon has opened the card and serves it, or fails if the daemon exits. So a script waits until `control.sock` exists and then starts its applications, as in step 3. Connecting has no timeout of its own.

## Stopping it

SIGINT or SIGTERM stops the daemon. It tells each connected application that it is shutting down, closes the card, removes `control.sock` and the shared-memory files, and exits with status 0. Connected applications lose their session; the guide chapter Sharing a network card describes what they see.

If the daemon is killed with SIGKILL or crashes, it cannot tell anyone or clean up. Its files stay until the next daemon started on the directory removes them.

## When it fails

Every failure is one line on stderr, `switch-junctiond: <reason>`, and exit status 1.

| Message | Cause |
|---|---|
| `network thread must be pinned to exactly one CPU before opening; use taskset -c CPU` | started without `taskset -c N` |
| `unknown option --<name>`, `--config is required`, `--<flag>: invalid value "..."` | a flag error; the usage text follows |
| `<config path>: <reason>` | the config file is missing or invalid ([The network config file](config.md#checking-a-file)) |
| `--dir: <dir> is owned by another switch-junctiond: ...` | another daemon runs on this directory |
| `--dir: control.sock exists but is not a socket; refusing to remove it` | something else uses that name in the directory |
| `control: rx_size must be a power of two, at most 2 GiB, and a backing-page multiple` | `--rx-size` does not suit the page size of the filesystem holding `--dir` |
| `control: ...` | `control.sock` could not be created |
| `open: EAL helper CPU affinity: EAL helper CPUs are outside the effective cpuset` | a CPU in `--control-cpus` is not available to the process, for example outside Docker's `--cpuset-cpus` |
| `open: ...` | the card could not be opened; see [Troubleshooting](troubleshooting.md#opening-the-card) |

While running, the daemon also exits with status 1 if one of its threads fails or panics, and after a stop signal if closing the card or cleaning the directory fails.
