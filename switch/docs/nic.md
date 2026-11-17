Opening a network card for a program that owns it.

A card is described by a JSON config file, loaded as a [`Config`]. [`open`] starts DPDK, opens the card the config names and returns the network stack running on it, which [`Net::new`](crate::net::Net::new) takes.

- [`Config`]: the config file. [`Config::load`] reads and checks it.
- [`Sizing`]: how many sockets and send frames the program needs.
- [`open`]: opens the card. The calling thread must be allowed on exactly one CPU.
- [`Opened`]: the value `open` returns, holding the stack, a report to print, and the DPDK handle to keep until the card is closed.
- [`OpenError`]: why an open failed.

```rust,no_run
use std::path::Path;
use std::time::Instant as WallInstant;

use switch::net::{tcp, Instant, Net, NetConfig};
use switch::nic::{open, Config, Sizing, DEFAULT_NEIGHBOR_CAPACITY};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::load(Path::new("tap.json"))?;
    let socket = tcp::Config { rx_capacity: 65536, tx_capacity: 65536, mss_cap: Some(config.mtu - 40) };
    let sizing = Sizing {
        frame_share: tcp::tx_mbuf_budget(&socket, usize::from(config.mtu)),
        max_sockets: 1,
        max_udp_sockets: 0,
        rx_staging: 32,
        tx_pending: 32,
        neighbor_capacity: DEFAULT_NEIGHBOR_CAPACITY,
    };
    let start = WallInstant::now();
    let mut clock = || Instant::from_micros(i64::try_from(start.elapsed().as_micros()).unwrap());
    // SAFETY: this thread alone uses the card, and `_eal` lives until it is closed.
    let opened = unsafe { open(&config, &sizing, None, &mut clock) }?;
    println!("{}", opened.report);
    let _eal = opened.eal;
    let _net = Net::new(opened.stack, NetConfig { ingress_budget: 32 });
    Ok(())
}
```

[Your first network program](crate::guide::first_connection) walks through these calls, and [Shutting down](crate::guide::shutdown) covers closing the card. The config file's keys are in `docs/operations/config.md` in the repository.
