use switch_test_kit::pair::pattern;
use switch_test_kit::with_eal;

#[allow(dead_code)]
#[path = "../benches/rx/mod.rs"]
mod rx;

#[switch_test_support::pinned_test]
fn rx_seg_checks_all_bytes() {
    with_eal(|eal| {
        let k = 8usize;
        let buffer = 3usize
            .checked_mul(k)
            .expect("buffer segments")
            .checked_mul(1448)
            .expect("socket buffer");
        for style in [rx::Read::Copy, rx::Read::Drain] {
            for seg in [64usize, 200, 512, 1448] {
                let batch = k.checked_mul(seg).expect("batch bytes");
                let (mut pair, handles) = rx::setup(eal, k, buffer);
                let mut sink = vec![0; batch];
                let iterations = (buffer / batch).checked_add(2).expect("iterations");
                for iteration in 0..iterations {
                    let seed = u32::try_from(iteration)
                        .expect("payload seed")
                        .checked_add(7)
                        .expect("payload seed");
                    let payload = pattern(batch, seed);
                    let (_, reads) = rx::iteration(
                        &mut pair,
                        &handles,
                        style,
                        seg,
                        &payload,
                        &mut sink,
                        true,
                        iteration > 0,
                    );
                    if let rx::Read::Drain = style {
                        assert_eq!(
                            reads, k,
                            "draining returns every frame in order with exact payloads"
                        );
                    }
                }
                rx::close(pair);
            }
        }
    });
}
