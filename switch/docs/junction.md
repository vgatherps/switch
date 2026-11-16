The client side of `switch-junctiond`, the daemon that lets several programs share one network card.

A program connects to the daemon with [`session::Session::connect`] and passes the session to [`Net::remote`](crate::net::Net::remote). Its tasks then use the same sockets as a program that owns its card.

- [`session::Session`]: one program's connection to the daemon. `connect` opens it; `finish`, `poll` and `closed` end it cleanly.
- [`session::ConnectError`]: why the daemon could not be reached or refused the program.
- [`session::Lost`]: why a session ended early.

```rust,no_run
use std::path::Path;

use switch::junction::session::Session;
use switch::net::{Net, RemoteConfig};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let session = Session::connect(Path::new("/run/switch-junctiond/control.sock"), 8)?;
    let _net = Net::remote(session, RemoteConfig { tx_frames: 64 });
    Ok(())
}
```

[Sharing a network card](crate::guide::sharing) shows a whole program and what behaves differently from owning the card. Running the daemon is in `docs/operations/junctiond.md` in the repository. The other modules here are the daemon's wire protocol and a lower-level use of the session, for programs that do not use [`switch::net`](crate::net).
