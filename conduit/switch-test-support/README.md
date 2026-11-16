# switch-test-support

Test harness that runs DPDK tests on one long-lived thread. Not published (`publish = false`).

EAL is claimed by the thread that initializes it, and libtest runs each test on a fresh thread. `switch-test-support` runs every `#[pinned_test]` body on a single worker thread, so all tests in a binary share one EAL owner.

Dev-dependency of `switch-dpdk-sys`, `switch-buffer`, `dpdk-smoltcp`, `switch-open`, `switch-tap-echo`, `switch-binance-feed`, `switch-memif-bench` and `switch-test-kit`.

## API

- `#[pinned_test]`, re-exported from [`switch-test-support-macros`](switch-test-support-macros/README.md). Replaces `#[test]`.
- `run(test_name, bound, f)`: runs `f` on the worker and returns its result. A panic in `f` is printed in the calling test's output and resumed with the original payload.
- `PINNED_TEST_BOUND`: 180 s, the default time limit for a body.

If a body outlives its bound, `run` prints the test name and ends the process with `_exit(1)`. The worker is still stuck in that body, so no later pinned test could run. The Makefile's `timeout` around each test step is the backstop (`TEST_TIMEOUT`, 600 s).

## Test

```sh
make test-support    # cargo test -p switch-test-support -p switch-test-support-macros
```

Plain cargo; no DPDK needed. The crate itself depends only on `libc` and the macros crate.
