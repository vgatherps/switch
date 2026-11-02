//! Binance bookTicker over TLS 1.3 and WebSocket on Zhida's net_tap TCP socket, as a switch
//! task on a switchnet Net. See `usage`.

use std::ffi::OsString;
use std::fs;
use std::io::{self, Write as _};
use std::path::Path;
use std::process;
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, Instant};

use binance_feed::{
    random_local_port, resolve, Clock, Target, DEFAULT_HOST, DEFAULT_PATH, DEFAULT_PORT,
    DEFAULT_PRINT, FIRST_LOCAL_PORT, INGRESS_BUDGET, SOCKET_BUFFER,
};
use rustls::ClientConfig;
use switch::{oneshot, Runtime};
use switchnet::{Net, NetConfig};
use tap_echo::driver::{self, ZHIDA_IP, EXIT_ERROR, IFACE, PREFIX_LEN, TESTER_IP};

const NAME: &str = "binance-feed";
const READY: &str = "target/binance-feed/ready";
const READY_LIMIT: Duration = Duration::from_secs(60);
/// The longest idle sleep of a round that moved nothing: the tester shares the CPU.
const IDLE_SLEEP: Duration = Duration::from_millis(1);

fn usage() -> String {
    format!(
        "\
usage: binance-feed [--host HOST] [--port PORT] [--path PATH] [--print N] -- <tester> [args...]

Resolves HOST with the kernel resolver before DPDK starts and uses its first IPv4
address. Defaults: HOST {DEFAULT_HOST}, PORT {DEFAULT_PORT}, PATH {DEFAULT_PATH},
N {DEFAULT_PRINT}. N may be 0. Each option may appear once, in any order before --.
TCP runs on Zhida at {ZHIDA_IP}/{PREFIX_LEN}, with IPv4 gateway {TESTER_IP} on
netdev {IFACE}, connecting from a random local port in {FIRST_LOCAL_PORT}-65535. TLS 1.3
only (sheath) uses the Mozilla roots from webpki-roots. data-stream.binance.vision speaks
TLS 1.2 only, so it is unreachable here; stream.binance.com may answer HTTP 451 from
restricted locations.

The tester starts before DPDK. It must wait for {IFACE} to be up, give the kernel
side {TESTER_IP} on {IFACE}, NAT Zhida's traffic out of the container, then create
{READY}. The client prints the first N bookTicker messages and keeps reading until
the tester exits (also during the connect). It then prints the count and update id range,
sends the WebSocket close, reads until the server's close echo, then sends TLS close_notify
and FIN and waits for the server's ack of them.
Only the tester process itself is killed: run it directly or have it exec its
final command. It must exit on SIGTERM. Bound the run with timeout --kill-after=N
without --foreground so expiry signals the tester too.

Exit status: 0 tester exited 0; 1 tester failed; 2 packet buffers still in use at
port close; 3 bad arguments, setup, feed or teardown error; 101 panic. Statuses 2
and 3 take precedence over the tester's status. Diagnostics on stderr are best
effort and never change the exit status.
"
    )
}

struct Args {
    host: String,
    port: u16,
    path: String,
    print: usize,
    tester: Vec<OsString>,
}

fn parse(mut args: impl Iterator<Item = OsString>) -> Result<Args, String> {
    let (mut host, mut port, mut path, mut print) = (None, None, None, None);
    loop {
        let arg = args.next().ok_or("missing -- <tester>")?;
        let flag = match arg.to_str() {
            Some("--") => break,
            Some(flag @ ("--host" | "--port" | "--path" | "--print")) => flag,
            _ => return Err(format!("unexpected argument {arg:?}")),
        };
        let duplicate = match flag {
            "--host" => host.is_some(),
            "--port" => port.is_some(),
            "--path" => path.is_some(),
            "--print" => print.is_some(),
            _ => unreachable!("only known options reach this match"),
        };
        if duplicate {
            return Err(format!("{flag} given twice"));
        }
        let value = args.next().ok_or_else(|| format!("{flag} needs a value"))?;
        let text = value.to_str().ok_or_else(|| format!("{flag} needs UTF-8, got {value:?}"))?;
        if text == "--" {
            return Err(format!("{flag} needs a value"));
        }
        match flag {
            "--host" => host = Some(text.to_owned()),
            "--path" => path = Some(text.to_owned()),
            "--port" => {
                port = Some(text.parse::<u16>().map_err(|_| {
                    format!("{flag} needs an integer from 0 to {}, got {value:?}", u16::MAX)
                })?);
            }
            "--print" => {
                print = Some(text.parse::<usize>().map_err(|_| {
                    format!("{flag} needs an integer from 0 to {}, got {value:?}", usize::MAX)
                })?);
            }
            _ => unreachable!("only known options reach this match"),
        }
    }
    let tester: Vec<OsString> = args.collect();
    if tester.is_empty() || tester[0].is_empty() {
        return Err("missing tester command after --".to_owned());
    }
    Ok(Args {
        host: host.unwrap_or_else(|| DEFAULT_HOST.to_owned()),
        port: port.unwrap_or(DEFAULT_PORT),
        path: path.unwrap_or_else(|| DEFAULT_PATH.to_owned()),
        print: print.unwrap_or(DEFAULT_PRINT),
        tester,
    })
}

fn main() {
    let args = match parse(std::env::args_os().skip(1)) {
        Ok(args) => args,
        Err(message) => {
            driver::report(NAME, format_args!("{message}\n\n{}", usage()));
            process::exit(EXIT_ERROR);
        }
    };
    let target = match resolve(&args.host, args.port, &args.path) {
        Ok(target) => target,
        Err(error) => {
            driver::report(NAME, format_args!("{error}"));
            process::exit(EXIT_ERROR);
        }
    };
    let _ = writeln!(
        io::stdout(),
        "{NAME}: {} resolved to {} by the kernel resolver",
        target.host,
        target.addr.ip(),
    );
    let tls = binance_feed::webpki_client_config();
    let local_port = match random_local_port() {
        Ok(port) => port,
        Err(error) => {
            driver::report(NAME, format_args!("reading /dev/urandom: {error}"));
            process::exit(EXIT_ERROR);
        }
    };
    match fs::remove_file(READY) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => {
            driver::report(NAME, format_args!("removing stale {READY}: {error}"));
            process::exit(EXIT_ERROR);
        }
    }
    let parent = Path::new(READY).parent().expect("READY has a parent directory");
    if let Err(error) = fs::create_dir_all(parent) {
        driver::report(NAME, format_args!("creating {}: {error}", parent.display()));
        process::exit(EXIT_ERROR);
    }
    let code = driver::run(
        NAME,
        &args.tester,
        1,
        SOCKET_BUFFER,
        Some(TESTER_IP),
        |stack, start, stop| {
            let mut net = Net::new(stack, NetConfig { ingress_budget: INGRESS_BUDGET });
            let result = feed(&mut net, start, stop, &target, tls, local_port, args.print);
            let stack = net.into_stack().ok().expect("the feed dropped every handle and socket");
            (stack, result)
        },
    );
    process::exit(code);
}

/// Turns `net` until the tester created READY, then runs the feed task until it finished, telling
/// it to stop once the tester exited.
fn feed(
    net: &mut Net,
    start: Instant,
    stop: &mut dyn FnMut() -> bool,
    target: &Target,
    tls: Arc<ClientConfig>,
    local_port: u16,
    print: usize,
) -> Result<(), String> {
    let clock = Clock::new(start);
    loop {
        net.receive(clock.now()).map_err(|error| format!("receive: {error}"))?;
        net.send().map_err(|error| format!("send: {error}"))?;
        if stop() {
            return Err(format!("tester exited before {READY} existed"));
        }
        if Path::new(READY).exists() {
            break;
        }
        if start.elapsed() >= READY_LIMIT {
            return Err(format!("no {READY} within {READY_LIMIT:?}"));
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    let mut rt = Runtime::new(clock.ns());
    let (stop_feed, stop_rx) = oneshot::channel();
    let mut stop_feed = Some(stop_feed);
    let out = Rc::default();
    let task = binance_feed::feed(
        net.handle(),
        rt.spawner(),
        target.clone(),
        binance_feed::socket(None),
        tls,
        local_port,
        NAME,
        print,
        None,
        stop_rx,
        Rc::clone(&out),
        || false,
    );
    rt.spawner()
        .spawn(task)
        .map_err(|error| format!("spawning the feed: {:?}", error.reason))?;
    let (summary, result) = loop {
        if let Some(outcome) = out.take() {
            break outcome;
        }
        let (now, tick) = clock.sample();
        let moved = switchnet::round(net, &mut rt, now, tick)
            .map_err(|error| format!("round: {error:?}"))?;
        if !moved {
            std::thread::sleep(IDLE_SLEEP);
        }
        if let Some(stop_feed) = stop_feed.take_if(|_| stop()) {
            let _ = stop_feed.send(());
        }
    };
    result.map_err(|error| error.to_string())?;
    if summary.count == 0 {
        return Err("no bookTicker message received".to_owned());
    }
    Ok(())
}
