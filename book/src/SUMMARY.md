# Summary

[Introduction](introduction.md)
[How the pieces fit together](overview/architecture.md)

# Getting started

- [Your first task](getting-started/first-task.md)
- [Setting up a build environment](getting-started/environment.md)
- [Your first network program](getting-started/first-connection.md)

# The switch runtime

- [The switch runtime](runtime/index.md)
    - [Writing and spawning tasks](runtime/tasks.md)
    - [Driving the runtime](runtime/driving.md)
    - [Time and sleeping](runtime/time.md)
    - [Tasks working together, and other threads](runtime/cooperating.md)
    - [Panics, errors and shutdown](runtime/errors.md)

# Sockets with switch-net

- [Sockets with switch-net](net/index.md)
    - [The drive loop](net/drive-loop.md)
    - [Opening sockets](net/opening.md)
    - [Frames](net/frames.md)
    - [TCP streams](net/tcp.md)
    - [UDP sockets](net/udp.md)
    - [Timeouts and racing futures](net/timeouts.md)
    - [Errors](net/errors.md)
    - [Local and junction backends compared](net/backends.md)
    - [Shutting down cleanly](net/shutdown.md)

# TLS and WebSocket

- [TLS and WebSocket over switch-net](protocols/index.md)
    - [Connecting to a wss:// server](protocols/wss-client.md)
    - [Sending and receiving messages](protocols/messages.md)
    - [Closing connections and handling failures](protocols/closing.md)
    - [Serving ws:// connections](protocols/ws-server.md)
    - [TLS without WebSocket](protocols/tls.md)
    - [Walkthrough: a market-data feed](protocols/binance-feed.md)

# Owning a NIC

- [Owning a NIC](nic/index.md)
    - [Preparing a Linux machine](nic/host-setup.md)
    - [Running on EC2](nic/ec2.md)
    - [The network config file](nic/config.md)
    - [switch-netconf: check, discover, bind, unbind](nic/netconf-tool.md)
    - [switch-nic: checking a NIC and serving load](nic/switch-nic.md)
    - [Opening the NIC from code](nic/open.md)
    - [Packet-buffer pools](nic/pools.md)
    - [The pinned CPU and helper threads](nic/cpus.md)
    - [Offloads and hardware timestamps](nic/offloads.md)
    - [When the NIC won't open](nic/troubleshooting.md)

# Sharing a NIC: the junction daemon

- [Sharing a NIC with the junction daemon](junction/index.md)
    - [Running switch-junctiond](junction/running.md)
    - [Connecting an application through switch-net](junction/apps.md)
    - [What applications share](junction/sharing.md)
    - [Using a Session directly](junction/session.md)
    - [Closing sockets and ending a session](junction/closing.md)

# The Stack without async

- [Programming the Stack directly](stack/index.md)
    - [Starting DPDK and opening a NIC port by hand](stack/by-hand.md)
    - [The Stack's drive loop](stack/drive-loop.md)
    - [TCP sockets on the Stack](stack/tcp.md)
    - [UDP sockets on the Stack](stack/udp.md)
    - [Waiting on many sockets](stack/readiness.md)
    - [Stack errors and panics](stack/errors.md)

# Under the hood

- [Frames, shared memory and rings](blocks/index.md)
    - [switch-frame: packet-buffer handles](blocks/frames.md)
    - [Shared memory and the receive ring](blocks/rings.md)
    - [cephalopod: many writers, one reader](blocks/cephalopod.md)

# Measuring

- [Measuring latency under load](measuring/index.md)
    - [Running a sweep on one machine](measuring/local.md)
    - [Reading a benchmark report](measuring/reports.md)
    - [Benchmarking on AWS](measuring/aws.md)
    - [Stack-to-stack throughput with switch-memif-bench](measuring/memif.md)

# Testing

- [Running the test suite](testing/suite.md)
- [Writing tests that use DPDK](testing/writing-tests.md)
- [Protocol conformance: Autobahn and TLS-Anvil](testing/conformance.md)

# Reference

- [Crate map](reference/crates.md)
- [The junction control protocol](reference/junction-protocol.md)
- [Glossary](reference/glossary.md)
