# nic-clock

Latency from a NIC hardware RX stamp (ns on the NIC's PTP hardware clock, PHC) to now, with an error bound. No
dependencies, no DPDK: `cargo test -p nic-clock`. Design: `plans/clock/design.md`.

- `run(read_phc, &stop)`: one background thread, off the receive core. Every `PERIOD` (100 ms) it reads the PHC
  (`read_phc() -> Option<u64>`, `None` = this read failed) between two `counter()` reads and updates a static global
  fit; after `stop` the fit is not ready. Stop and join it before the NIC's port closes.
- `since(stamp) -> Option<Latency { ns, bound_ns }>` on the receive path: one seqlock read of the global, one counter
  read, integer math; no syscall, allocation or lock. The true latency is in `ns ± bound_ns`. `None`: stamp 0, counter
  not invariant, or fit not ready (under 1 s of consistent reads, latest read over 300 ms old, or stopped).
  `Latency::impossible()`: below `-bound_ns`, the stamp is after now; a bug, never a sample.
- Counter: x86_64 `lfence; rdtsc`, required invariant (CPUID 0x80000007 EDX bit 8) with Linux clocksource `tsc`;
  aarch64 `isb; mrs cntvct_el0` (constant-rate by architecture, CNTFRQ_EL0 set).
- Fit (`src/fit.rs`): rate from the latest read and the narrowest-window kept read at least 1 s of PHC before it,
  offset from the latest read; bound = half the latest read's window + rate uncertainty × distance + 2 ns of rounding.
  It holds while both clocks keep a constant rate. A read outside the fit's bound, or going backwards, drops the fit.

Tests: `src/fit.rs` (bound arithmetic, overlap and overflow cases), `src/counter.rs` (the x86 invariance rule),
`src/tests.rs` (fake clocks: not ready before 1 s and after 300 ms, sparse reads, a PHC step, a backward read,
`impossible` at exactly `-bound - 1`, seqlock never tears under a concurrent writer, and a property test that the
truth lies within the bound for 100 MHz–4 GHz counters, 0–10 µs read windows and 0–250 ms extrapolation),
`tests/global.rs` (`run` on a thread with a PHC derived from the real counter, `since` ready after 1.3 s and not after
stop).
