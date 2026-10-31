use std::cell::Cell;
use std::rc::Rc;
use std::time::{Duration, Instant};

use binance_feed::{
    random_local_port, stream, webpki_client_config, Clock, Event, FeedError, Summary, Target,
    CLOSE_LIMIT, CONNECT_LIMIT,
};
use switchnet::{tcp, Net};

use crate::args::Feed;
use crate::{line, stopped};

pub fn resolve(args: &Feed) -> Result<Target, String> {
    let target = binance_feed::resolve(&args.host, args.port, &args.path)?;
    line("resolved", format_args!("{} -> {}", target.host, target.addr));
    Ok(target)
}

/// Connects, reads for `args.seconds` after the connect (or until a stop signal), closes.
pub fn run(
    net: &mut Net,
    args: &Feed,
    target: &Target,
    socket: tcp::Config,
    clock: &Clock,
) -> Result<(), String> {
    let local_port = random_local_port().map_err(|e| format!("local port entropy: {e}"))?;
    let seconds = Duration::from_secs(args.seconds);
    let connected: Rc<Cell<Option<Instant>>> = Rc::default();
    let task_connected = connected.clone();
    let stop = || stopped() || connected.get().is_some_and(|at| at.elapsed() >= seconds);
    let (target, print) = (target.clone(), args.print);
    // The feed core busy-polls: a sleep would hold received packets until it ends.
    let count = binance_feed::run(
        net,
        clock,
        None,
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
                Event::Read(Summary { count, update_ids, latencies }) => {
                    line("feed_count", count);
                    if nic_clock::jumped() {
                        line("feed_latency", "latency invalid: NIC clock jumped");
                    } else {
                        line("feed_latency", latencies);
                    }
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
            let tls = webpki_client_config();
            let summary = stream(
                &handle,
                &spawner,
                &target,
                socket,
                tls,
                local_port,
                CONNECT_LIMIT,
                CLOSE_LIMIT,
                stop,
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
