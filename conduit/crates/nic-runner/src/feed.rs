use std::rc::Rc;
use std::time::Duration;

use binance_feed::{random_local_port, webpki_client_config, Clock, Target};
use switch::{oneshot, Runtime};
use switchnet::{tcp, Net};

use crate::args::Feed;
use crate::{line, stopped};

pub fn resolve(args: &Feed) -> Result<Target, String> {
    let target = binance_feed::resolve(&args.host, args.port, &args.path)?;
    line("resolved", format_args!("{} -> {}", target.host, target.addr));
    Ok(target)
}

/// Runs the feed task until it finished: it reads for `args.seconds` after the connect, or until
/// a stop signal, then closes. Busy-polls: a sleep would hold received packets until it ends.
pub fn run(
    net: &mut Net,
    args: &Feed,
    target: &Target,
    socket: tcp::Config,
    clock: &Clock,
) -> Result<(), String> {
    let resets = dpdk_smoltcp::port_resets();
    let local_port = random_local_port().map_err(|e| format!("local port entropy: {e}"))?;
    let mut rt = Runtime::new(clock.ns());
    let (stop_feed, stop_rx) = oneshot::channel();
    let mut stop_feed = Some(stop_feed);
    let out = Rc::default();
    let task = binance_feed::feed(
        net.handle(),
        rt.spawner(),
        target.clone(),
        socket,
        webpki_client_config(),
        local_port,
        "zhida-nic",
        args.print,
        Some(Duration::from_secs(args.seconds)),
        stop_rx,
        Rc::clone(&out),
        move || dpdk_smoltcp::port_resets() != resets,
    );
    rt.spawner()
        .spawn(task)
        .map_err(|e| format!("spawning the feed: {:?}", e.reason))?;
    let (summary, result) = loop {
        if let Some(outcome) = out.take() {
            break outcome;
        }
        let (now, tick) = clock.sample();
        switchnet::round(net, &mut rt, now, tick)
            .map_err(|e| format!("round: {e:?}"))?;
        if let Some(stop_feed) = stop_feed.take_if(|_| stopped()) {
            let _ = stop_feed.send(());
        }
    };
    result.map_err(|e| e.to_string())?;
    if summary.count == 0 {
        return Err("no bookTicker message received".into());
    }
    Ok(())
}
