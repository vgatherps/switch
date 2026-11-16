# switch-netconf

The network config file that describes one NIC port for a program that owns the card, and the `switch-netconf` command that writes and checks it and binds the card to `vfio-pci`. It does not need DPDK.

The file's keys are in [docs/operations/config.md](../../../docs/operations/config.md) and the command in [docs/operations/switch-netconf.md](../../../docs/operations/switch-netconf.md). Applications reach the library through `switch::nic`; its API reference is the `switch` crate's rustdoc (`make docs` at the repository root).

```sh
cargo build --locked --release -p switch-netconf
make -C conduit test-netconf    # with switch-open's tests, which need DPDK
```
