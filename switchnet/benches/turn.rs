//! `Net::turn` against a bare `Stack::turn`, criterion group "net_turn".
//!
//! One Stack on one net_ring port (16-slot rings, RX/TX burst 8, kit
//! `RingPort::config` pools with the RX pool sized for the reserve and every
//! UDP queue), nothing on the wire, clock fixed at `T0`, ingress budget
//! `BUDGET`. Each case binds N = 0, 1 or 16 UDP sockets with empty queues.
//! - stack_turn/N: N sockets bound directly on the Stack; one `Stack::turn`.
//! - net_turn/N: the same N sockets bound through a `NetHandle`, a switch
//!   task parked in `recv` on each; one `Net::turn`. No socket becomes ready,
//!   so nothing is woken: the difference is the cell borrow plus the
//!   readiness check of N parked sockets.

use std::hint::black_box;

use criterion::{criterion_group, criterion_main, Criterion};
use dpdk_smoltcp::{rx_pool_count, rx_reserve, Eal, Port, Stack, StackConfig};
use switch::Runtime;
use switchnet::{Net, NetConfig, UdpConfig};
use zhida_test_kit::pair::{self, Side, BUDGET, T0};
use zhida_test_kit::ring_port::RingPort;
use zhida_test_kit::with_eal;

const MAX_UDP: usize = 16;
const UDP_QUEUE: usize = 4;

fn udp_config(index: usize) -> UdpConfig {
    UdpConfig { port: 7000 + index as u16, rx_queue: UDP_QUEUE, tx_checksum: true, broadcast: false }
}

fn open(eal: &Eal, fixture: &RingPort) -> Stack {
    let stack_config = StackConfig { max_udp_sockets: MAX_UDP, ..pair::config(Side::A) };
    let mut config = fixture.config();
    let reserve = rx_reserve(config.rx_descriptors, stack_config.rx_staging, config.rx_pool.cache_size);
    config.rx_pool.count =
        rx_pool_count(reserve, config.mtu, []) + u32::try_from(MAX_UDP * UDP_QUEUE).unwrap();
    match Stack::new(fixture.open_with(eal, config), stack_config, T0) {
        Ok(stack) => stack,
        Err((_, error)) => panic!("{error}"),
    }
}

fn close(fixture: RingPort, port: Port) {
    fixture.drain();
    assert_eq!(fixture.in_use(), (0, 0));
    port.try_close().unwrap();
    fixture.close();
}

fn net_turn(c: &mut Criterion) {
    with_eal(|eal| {
        let mut group = c.benchmark_group("net_turn");
        for parked in [0, 1, 16] {
            let fixture = RingPort::new(&format!("bs{parked}"), 16, 16);
            let mut stack = open(eal, &fixture);
            let handles: Vec<_> = (0..parked).map(|i| stack.add_udp(udp_config(i)).unwrap()).collect();
            group.bench_function(format!("stack_turn/{parked}"), |b| {
                b.iter(|| black_box(stack.turn(black_box(T0), BUDGET).unwrap()))
            });
            drop(handles);
            close(fixture, stack.into_port());

            let fixture = RingPort::new(&format!("bn{parked}"), 16, 16);
            let mut net = Net::new(open(eal, &fixture), NetConfig { ingress_budget: BUDGET });
            let handle = net.handle();
            let mut rt = Runtime::new(0);
            for i in 0..parked {
                let mut socket = handle.udp_bind(udp_config(i)).unwrap();
                rt.spawner().spawn(async move { drop(socket.recv().await) }).unwrap();
            }
            assert_eq!(rt.poll(0).unwrap().tasks_polled, parked);
            group.bench_function(format!("net_turn/{parked}"), |b| {
                b.iter(|| black_box(net.turn(black_box(T0)).unwrap()))
            });
            assert_eq!(rt.poll(0).unwrap().tasks_polled, 0, "an idle turn woke a task");
            drop((rt, handle));
            let stack = net.into_stack().ok().expect("every socket dropped");
            close(fixture, stack.into_port());
        }

        group.finish();
    })
}

criterion_group!(benches, net_turn);
criterion_main!(benches);
