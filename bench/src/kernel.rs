//! The kernel echo DUT: the baseline, and the check that the generator is not the bottleneck.

use std::io::{ErrorKind, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream, UdpSocket};
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use std::thread;
use std::time::Duration;

use crate::host;

/// How often blocked calls look at the stop flag.
const STOP_CHECK: Duration = Duration::from_millis(100);

fn transient(e: &std::io::Error) -> bool {
    matches!(
        e.kind(),
        ErrorKind::WouldBlock | ErrorKind::TimedOut | ErrorKind::Interrupted
    )
}

pub fn udp(bind: SocketAddr) -> Result<(), String> {
    host::catch_stop()?;
    let socket = UdpSocket::bind(bind).map_err(|e| format!("bind {bind}: {e}"))?;
    socket
        .set_read_timeout(Some(STOP_CHECK))
        .map_err(|e| e.to_string())?;
    eprintln!("zhida-bench kernel-serve: udp echo on {bind}");
    let mut buf = vec![0u8; 65536];
    while !host::stopped() {
        match socket.recv_from(&mut buf) {
            Ok((len, from)) => match socket.send_to(&buf[..len], from) {
                Err(e) if !transient(&e) => return Err(format!("send_to {from}: {e}")),
                _ => {}
            },
            Err(e) if transient(&e) => {}
            Err(e) => return Err(format!("recv_from: {e}")),
        }
    }
    Ok(())
}

/// One echo thread's state, for the SIGUSR1 dump.
struct Conn {
    peer: SocketAddr,
    rx_bytes: AtomicU64,
    tx_bytes: AtomicU64,
    /// 0 reading, 1 writing, 2 ended.
    phase: AtomicU8,
}

impl Conn {
    fn phase(&self, phase: u8) {
        self.phase.store(phase, Ordering::Relaxed);
    }
}

/// Accepts on `bind`, one `echo` thread per connection; on SIGUSR1 prints every connection's
/// state to stderr.
fn serve_tcp(bind: SocketAddr, what: &str, echo: fn(TcpStream, &Conn)) -> Result<(), String> {
    host::catch_stop()?;
    let listener = TcpListener::bind(bind).map_err(|e| format!("bind {bind}: {e}"))?;
    listener.set_nonblocking(true).map_err(|e| e.to_string())?;
    eprintln!("zhida-bench kernel-serve: {what} echo on {bind}");
    let mut conns: Vec<Arc<Conn>> = Vec::new();
    while !host::stopped() {
        if host::dump_requested() {
            for c in &conns {
                eprintln!(
                    "zhida-bench kernel-serve: {what} {} rx {} tx {} {}",
                    c.peer,
                    c.rx_bytes.load(Ordering::Relaxed),
                    c.tx_bytes.load(Ordering::Relaxed),
                    ["reading", "writing", "ended"][usize::from(c.phase.load(Ordering::Relaxed))]
                );
            }
        }
        match listener.accept() {
            Ok((stream, peer)) => {
                let conn = Arc::new(Conn {
                    peer,
                    rx_bytes: AtomicU64::new(0),
                    tx_bytes: AtomicU64::new(0),
                    phase: AtomicU8::new(0),
                });
                conns.push(conn.clone());
                thread::spawn(move || {
                    echo(stream, &conn);
                    conn.phase(2);
                });
            }
            Err(e) if transient(&e) => thread::sleep(Duration::from_millis(1)),
            Err(e) => return Err(format!("accept: {e}")),
        }
    }
    Ok(())
}

/// One thread per connection, TCP_NODELAY, bytes echoed as they arrive.
pub fn tcp(bind: SocketAddr) -> Result<(), String> {
    serve_tcp(bind, "tcp", echo_tcp)
}

/// ws:// echo (tungstenite), one thread per connection: every message back.
pub fn ws(bind: SocketAddr) -> Result<(), String> {
    serve_tcp(bind, "ws", echo_ws)
}

fn echo_ws(stream: TcpStream, conn: &Conn) {
    if stream.set_nonblocking(false).is_err() || stream.set_nodelay(true).is_err() {
        return;
    }
    let Ok(mut ws) = tungstenite::accept(stream) else {
        return;
    };
    while !host::stopped() {
        match ws.read() {
            Ok(m @ (tungstenite::Message::Binary(_) | tungstenite::Message::Text(_))) => {
                let len = m.len() as u64;
                conn.rx_bytes.fetch_add(len, Ordering::Relaxed);
                conn.phase(1);
                if ws.send(m).is_err() {
                    return;
                }
                conn.tx_bytes.fetch_add(len, Ordering::Relaxed);
                conn.phase(0);
            }
            Ok(_) => {}
            Err(_) => return,
        }
    }
}

fn echo_tcp(mut stream: TcpStream, conn: &Conn) {
    let ok = stream.set_nonblocking(false).is_ok()
        && stream.set_nodelay(true).is_ok()
        && stream.set_read_timeout(Some(STOP_CHECK)).is_ok();
    if !ok {
        return;
    }
    let mut buf = vec![0u8; 65536];
    while !host::stopped() {
        match stream.read(&mut buf) {
            Ok(0) => return,
            Ok(n) => {
                conn.rx_bytes.fetch_add(n as u64, Ordering::Relaxed);
                conn.phase(1);
                if stream.write_all(&buf[..n]).is_err() {
                    return;
                }
                conn.tx_bytes.fetch_add(n as u64, Ordering::Relaxed);
                conn.phase(0);
            }
            Err(e) if transient(&e) => {}
            Err(_) => return,
        }
    }
}
