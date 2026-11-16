use switch_test_kit::pair::{App, Pair, Side, T0, config, establish};
use switch_test_kit::with_eal;

// ANCHOR: pinned
#[switch_test_support::pinned_test]
fn eal_belongs_to_the_test_worker() {
    with_eal(|eal| {
        assert_eq!(eal.owner_lcore_id(), 7);
    });
}
// ANCHOR_END: pinned

// ANCHOR: pair
#[switch_test_support::pinned_test]
fn four_bytes_reach_the_peer() {
    with_eal(|eal| {
        let mut pair = Pair::new(
            eal,
            ["book_a", "book_b"],
            [config(Side::A), config(Side::B)],
            T0,
        );
        let ([client, server], _) = establish(&mut pair, Side::A);
        pair.a.tcp_mut(&client).send_slice(b"book").unwrap();
        pair.drive(
            |_, _| {},
            |_, b| {
                if b.tcp(&server).recv_queue() == 4 {
                    App::Done
                } else {
                    App::Idle
                }
            },
        );
        // Let the client receive any acknowledgement sent by the server.
        pair.round(|_, _| {});
        assert_eq!(pair.close(), 0);
    });
}
// ANCHOR_END: pair
