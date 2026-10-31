//! The local clock: x86_64 TSC or the arm64 generic timer's virtual counter.

/// The CPU's counter, ordered after earlier instructions: `lfence; rdtsc` on x86_64,
/// `isb; mrs cntvct_el0` on aarch64.
#[cfg(target_arch = "x86_64")]
#[inline]
pub(crate) fn counter() -> u64 {
    // SAFETY: lfence and rdtsc have no memory effects; rdtsc is user-readable on Linux.
    unsafe {
        std::arch::x86_64::_mm_lfence();
        std::arch::x86_64::_rdtsc()
    }
}

#[cfg(target_arch = "aarch64")]
#[inline]
pub(crate) fn counter() -> u64 {
    let value: u64;
    // SAFETY: CNTVCT_EL0 is readable at EL0 on Linux and macOS; isb orders it after
    // earlier instructions. No memory is touched.
    unsafe { std::arch::asm!("isb", "mrs {}, cntvct_el0", out(reg) value, options(nostack, nomem)) };
    value
}

/// Whether `counter` runs at one constant rate on every core; `Err` says why not.
/// x86_64: CPUID 0x80000007 EDX bit 8 (invariant TSC) and Linux's current clocksource is
/// `tsc` (Linux drops tsc when its cross-core checks fail). aarch64: the generic timer is
/// constant-rate and system-wide by architecture; CNTFRQ_EL0 must be set.
#[cfg(target_arch = "x86_64")]
pub(crate) fn invariant() -> Result<(), &'static str> {
    use std::arch::x86_64::__cpuid;
    // SAFETY: CPUID is available on every x86_64 CPU.
    let max_extended = unsafe { __cpuid(0x8000_0000) }.eax;
    let edx = if max_extended >= 0x8000_0007 {
        // SAFETY: as above; the leaf exists.
        unsafe { __cpuid(0x8000_0007) }.edx
    } else {
        0
    };
    let clocksource =
        std::fs::read_to_string("/sys/devices/system/clocksource/clocksource0/current_clocksource")
            .unwrap_or_default();
    x86_invariant(edx, &clocksource)
}

#[cfg(target_arch = "aarch64")]
pub(crate) fn invariant() -> Result<(), &'static str> {
    let frequency: u64;
    // SAFETY: CNTFRQ_EL0 is readable at EL0; no memory is touched.
    unsafe { std::arch::asm!("mrs {}, cntfrq_el0", out(reg) frequency, options(nostack, nomem)) };
    if frequency == 0 {
        return Err("CNTFRQ_EL0 is 0");
    }
    Ok(())
}

/// `edx`: CPUID 0x80000007 EDX (0 if the leaf is absent); `clocksource`: the sysfs file.
#[cfg_attr(not(target_arch = "x86_64"), allow(dead_code))]
fn x86_invariant(edx: u32, clocksource: &str) -> Result<(), &'static str> {
    if edx & (1 << 8) == 0 {
        return Err("no invariant TSC (CPUID 0x80000007 EDX bit 8)");
    }
    if clocksource.trim_end() != "tsc" {
        return Err("Linux clocksource is not tsc");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn x86_needs_the_bit_and_tsc_clocksource() {
        assert_eq!(x86_invariant(1 << 8, "tsc\n"), Ok(()));
        assert!(x86_invariant(0, "tsc\n").is_err());
        assert!(x86_invariant(1 << 8, "kvm-clock\n").is_err());
        assert!(x86_invariant(1 << 8, "").is_err());
        assert!(x86_invariant(!(1 << 8), "tsc\n").is_err());
    }
}
