use std::cell::Cell;
use std::io::Read;
use std::net::{SocketAddr, ToSocketAddrs};
use std::rc::Rc;
use std::time::{Duration, Instant};

use binance_feed::{stream, webpki_client_config, Clock, Event, FeedError, Summary, Target};
use switchnet::{tcp, Net};

use crate::args::Feed;
use crate::{line, stopped};

pub fn resolve(args: &Feed) -> Result<Target, String> {
    let mut addresses = (args.host.as_str(), args.port)
        .to_socket_addrs()
        .map_err(|e| format!("DNS {}: {e}", args.host))?;
    let addr = addresses
        .find_map(|a| match a {
            SocketAddr::V4(a) => Some(a),
            SocketAddr::V6(_) => None,
        })
        .ok_or_else(|| format!("DNS {} has no IPv4 address", args.host))?;
    line("resolved", format_args!("{} -> {addr}", args.host));
    Ok(Target {
        addr,
        host: args.host.clone(),
        path: args.path.clone(),
    })
}

/// Connects, reads for `args.seconds` after the connect (or until a stop signal), closes.
pub fn run(
    net: &mut Net,
    args: &Feed,
    target: &Target,
    socket: tcp::Config,
    clock: &Clock,
) -> Result<(), String> {
    let mut bytes = [0; 2];
    std::fs::File::open("/dev/urandom")
        .and_then(|mut f| f.read_exact(&mut bytes))
        .map_err(|e| format!("local port entropy: {e}"))?;
    let local_port = 49152 + u16::from_ne_bytes(bytes) % (u16::MAX - 49152 + 1);
    let seconds = Duration::from_secs(args.seconds);
    let connected: Rc<Cell<Option<Instant>>> = Rc::default();
    let task_connected = connected.clone();
    let stop = || stopped() || connected.get().is_some_and(|at| at.elapsed() >= seconds);
    let (target, print) = (target.clone(), args.print);
    let count = binance_feed::run(
        net,
        clock,
        stop,
        move |handle, spawner, stop| async move {
            let report = |event: Event<'_>| match event {
                Event::Connected => {
                    line(
                        "feed_connected",
                        format_args!("{} local_port={local_port} TLS 1.3", target.url()),
                    );
                    task_connected.set(Some(Instant::now()));
                }
                Event::Ticker { count, ticker } if count <= print as u64 => {
                    line("feed_message", format_args!("#{count} {ticker}"))
                }
                Event::Ticker { .. } => {}
                Event::Read(Summary { count, update_ids }) => {
                    line("feed_count", count);
                    match update_ids {
                        Some((first, last)) => {
                            line("feed_update_ids", format_args!("{first}..{last}"))
                        }
                        None => line("feed_update_ids", "none"),
                    }
                }
                Event::Closed => line(
                    "feed_close",
                    "WebSocket close handshake, TLS close_notify, FIN",
                ),
                Event::CloseFailed(e) => line("feed_close_error", e),
            };
            let limit = Duration::from_secs(20);
            let close_limit = Duration::from_secs(10);
            let tls = webpki_client_config();
            let summary = stream(
                &handle, &spawner, &target, socket, tls, local_port, limit, close_limit, stop,
                report,
            )
            .await?;
            if summary.is_none() {
                line("feed_stopped", "before the feed connected");
            }
            Ok::<_, FeedError>(summary.map_or(0, |summary| summary.count))
        },
    )
    .and_then(|count| count)
    .map_err(|e| e.to_string())?;
    if count == 0 {
        return Err("no bookTicker message received".into());
    }
    Ok(())
}
