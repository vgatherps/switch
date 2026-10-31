//! Closed-loop TCP ping-pong over one Zhida connection: send one message, wait for its whole
//! echo, repeat. The round-trip time is the baseline the open-loop latency builds on.

use std::cell::RefCell;
use std::net::SocketAddrV4;
use std::path::PathBuf;
use std::rc::Rc;

use serde::Serialize;
use switchnet::{IpEndpoint, IpListenEndpoint, SendFrameError, boxed, tcp};
use zhida_open::Sizing;

use crate::coord::Quantiles;
use crate::hist::{Histogram, Sparse};
use crate::host::{self, Host, TurnSums};
use crate::load::{TCP_BUFFER, messages};
use crate::wire::{HEADER, Header};

#[derive(Debug, Clone)]
pub struct Args {
    pub config: PathBuf,
    pub target: SocketAddrV4,
    pub size: usize,
    pub warmup: u64,
    pub count: u64,
    pub local_port: u16,
}

#[derive(Debug, Serialize)]
pub struct Report {
    pub kind: &'static str,
    pub proto: &'static str,
    pub size: usize,
    pub count: u64,
    /// Round-trip time, ns.
    pub rtt_ns: Option<Quantiles>,
    pub rtt: Sparse,
}

pub fn run(args: &Args, out: &mut dyn FnMut(&str)) -> Result<(), String> {
    if args.size < HEADER || args.count == 0 {
        return Err(format!("need --size >= {HEADER} and --count > 0"));
    }
    let config = Host::config(&args.config)?;
    let tcp_config = tcp::Config {
        rx_capacity: TCP_BUFFER,
        tx_capacity: TCP_BUFFER,
        mss_cap: None,
    };
    let mtu = usize::from(config.mtu);
    let sizing = Sizing {
        rx_share: tcp::rx_mbuf_budget(&tcp_config, mtu),
        frame_share: tcp::tx_mbuf_budget(&tcp_config, mtu) + 8,
        max_sockets: 1,
        max_udp_sockets: 0,
        rx_staging: 64,
        tx_pending: 64,
        neighbor_capacity: 256,
    };
    host::catch_stop()?;
    let mut host = Host::open(&config, &sizing)?;
    let handle = host.net.handle();
    let clock = host.clock;
    let target = IpEndpoint::new((*args.target.ip()).into(), args.target.port());
    let local = IpListenEndpoint {
        addr: None,
        port: args.local_port,
    };
    let connecting = handle
        .tcp_connect(tcp_config, target, local)
        .map_err(|e| format!("tcp connect: {e:?}"))?;
    let result: Rc<RefCell<Option<Result<Histogram, String>>>> = Rc::default();
    let (task_result, task_handle) = (result.clone(), handle.clone());
    let (size, rounds, warmup) = (args.size, args.warmup + args.count, args.warmup);
    let task = async move {
        let outcome = async {
            let stream = connecting
                .await
                .map_err(|e| format!("tcp connect: {e:?}"))?;
            let (mut rx, mut tx) = stream.split();
            let mut rtt = Histogram::new();
            let mut partial = Vec::with_capacity(size);
            for seq in 0..rounds {
                let sent_at = clock.now_ns();
                let mut frame = task_handle.tx_frame().await;
                Header {
                    step: 0,
                    sender: 0,
                    seq,
                    sched_ns: sent_at,
                }
                .write(&mut frame.room_mut()[..size]);
                frame.set_len(size);
                loop {
                    match tx.try_send(frame) {
                        Ok(()) => break,
                        Err((back, SendFrameError::Full)) => {
                            frame = back;
                            tx.room(size)
                                .await
                                .map_err(|e| format!("tcp room: {e:?}"))?;
                        }
                        Err((_, e)) => return Err(format!("tcp send: {e:?}")),
                    }
                }
                let mut echoed = None;
                while echoed.is_none() {
                    let received = rx.recv().await.map_err(|e| format!("tcp recv: {e:?}"))?;
                    let now = clock.now_ns();
                    messages(&mut partial, received.payload(), size, |m| {
                        echoed = Some((Header::read(m), now))
                    });
                }
                let (header, now) = echoed.expect("loop ended on it");
                if header.map(|h| h.seq) != Some(seq) {
                    return Err(format!("echo of message {seq} out of order"));
                }
                if seq >= warmup {
                    rtt.record(now - sent_at);
                }
            }
            Ok(rtt)
        };
        *task_result.borrow_mut() = Some(outcome.await);
    };
    host.rt
        .spawner()
        .spawn(boxed(Box::pin(task)))
        .map_err(|e| format!("spawn: {:?}", e.reason))?;
    let mut turns = TurnSums::default();
    while result.borrow().is_none() {
        host.round(&mut turns)?;
        if host::stopped() {
            return Err("stopped".into());
        }
    }
    let rtt = result.borrow_mut().take().expect("checked above")?;
    let report = Report {
        kind: "pingpong",
        proto: "tcp",
        size: args.size,
        count: args.count,
        rtt_ns: Quantiles::of(&rtt),
        rtt: rtt.to_sparse(),
    };
    out(&serde_json::to_string(&report).expect("report serializes"));
    drop(handle);
    host.close()
}
