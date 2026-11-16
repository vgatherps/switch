# Operations

1. Prepare the machine: [a Linux machine with a spare NIC](host-setup.md), or [an EC2 instance](ec2.md). A tap device needs neither.
2. Describe the card in [the network config file](config.md), using [switch-netconf](switch-netconf.md), and check that it opens with [switch-nic](switch-nic.md).
3. Choose [the CPUs](cpus.md) and [the offloads and timestamps](offloads.md) your program runs with.
4. To share one card between several programs, run [switch-junctiond](junctiond.md). When something fails, look up the message in [Troubleshooting](troubleshooting.md).
