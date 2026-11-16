// Send a fixed message through the daemon from docs/operations/junctiond.md.
// Build without DPDK: cargo build --locked --release -p switch-junction --example tcp_session
// With that daemon running, start a receiver on its tap peer (10.77.0.1):
// python3 -c 'import socket; s=socket.socket(); s.bind(("10.77.0.1",7000)); s.listen(); c,_=s.accept(); print(c.makefile("rb").read()); c.close()'
// Run as the daemon's user (or root), on an application CPU from the walkthrough:
// taskset -c 3 target/release/examples/tcp_session /tmp/jn/control.sock 10.77.0.1:7000
// Arguments: [control socket path] [IPv4 peer:port] [local port].

use std::error::Error;
use std::net::SocketAddrV4;
use std::path::PathBuf;
use std::thread;
use std::time::Duration;

use switch_junction::control::{Event, ListenEndpoint, TcpConfig};
use switch_junction::session::{FinishError, SendError, Session};

fn main() -> Result<(), Box<dyn Error>> {
    let mut args = std::env::args().skip(1);
    let control = PathBuf::from(args.next().unwrap_or_else(|| "/run/switch-junctiond/control.sock".into()));
    let peer: SocketAddrV4 = args.next().unwrap_or_else(|| "10.77.0.1:7000".into()).parse()?;
    let local_port: u16 = args.next().unwrap_or_else(|| "30000".into()).parse()?;

    let mut session = Session::connect(&control, 1)?;
    let id = session.connect_tcp(
        TcpConfig {
            rx_capacity: 6 * 1460,
            tx_capacity: 64 * 1024,
            mss_cap: None,
        },
        peer,
        ListenEndpoint { addr: None, port: local_port },
    )?;
    let mut connected = false;
    while !connected {
        let mut failure = None;
        session.poll(|socket, event| {
            assert_eq!(socket, id);
            match event {
                Event::Connected { .. } => connected = true,
                Event::Rejected(_) | Event::Closed { .. } => failure = Some(event),
                _ => {}
            }
        }, |_, _, _| {})?;
        if let Some(event) = failure {
            return Err(format!("TCP connect failed: {event:?}").into());
        }
        thread::sleep(Duration::from_millis(1));
    }

    loop {
        match session.send_tcp(id, b"hello from Session\n") {
            Ok(()) => break,
            Err(SendError::Full) => {
                // Full committed nothing. Poll before retrying the same bytes.
                let mut failure = None;
                session.poll(|_, event| {
                    if matches!(event, Event::Rejected(_) | Event::Closed { .. }) {
                        failure = Some(event);
                    }
                }, |_, _, _| {})?;
                if let Some(event) = failure {
                    return Err(format!("TCP send failed: {event:?}").into());
                }
                thread::sleep(Duration::from_millis(1));
            }
            Err(error) => return Err(error.into()),
        }
    }

    session.shutdown(id);
    let mut delivered = false;
    while !delivered {
        let mut failure = None;
        session.poll(|_, event| {
            match event {
                Event::Delivered => delivered = true,
                Event::Rejected(_) | Event::Closed { .. } if !delivered => failure = Some(event),
                _ => {}
            }
        }, |_, _, _| {})?;
        if let Some(event) = failure {
            return Err(format!("TCP closed before delivery completed: {event:?}").into());
        }
        thread::sleep(Duration::from_millis(1));
    }
    session.close(id, false);
    let mut freed = false;
    while !freed {
        // Packet callbacks drop received frames; this sender keeps no unread data.
        session.poll(|_, event| freed |= matches!(event, Event::Freed), |_, _, _| {})?;
        thread::sleep(Duration::from_millis(1));
    }

    loop {
        match session.finish() {
            Ok(()) => break,
            Err(FinishError::ControlPending | FinishError::StreamFull) => {
                session.poll(|_, _| {}, |_, _, _| {})?;
                thread::sleep(Duration::from_millis(1));
            }
        }
    }
    while !session.closed() {
        session.poll(|_, _| {}, |_, _, _| {})?;
        thread::sleep(Duration::from_millis(1));
    }
    println!("message delivered; socket freed; session finished");
    Ok(())
}
