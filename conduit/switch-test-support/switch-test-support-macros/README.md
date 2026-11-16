# switch-test-support-macros

Proc-macro crate behind `#[pinned_test]`. Not published; use it through `switch-test-support`, which re-exports it.

`#[pinned_test]` replaces `#[test]` on a function. It keeps the other attributes and wraps the body in `switch_test_support::run(...)`, with the test's module path and name as the label. The default bound is `switch_test_support::PINNED_TEST_BOUND`. `#[pinned_test(bound_ms = N)]` overrides it, which exists so the hung-test path can be tested in under a second.

Only the `bound_ms` argument is accepted; anything else is a compile error.

Depends on `proc-macro2`, `quote` and `syn`. Test it with `make test-support`, or `cargo test -p switch-test-support-macros`; no DPDK is needed.
