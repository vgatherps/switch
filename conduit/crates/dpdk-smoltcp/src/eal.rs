use std::ffi::{c_char, c_int, CString};
use std::fmt;
use std::marker::PhantomData;
use std::rc::Rc;

use switch_dpdk_sys::{rte_eal_init, rte_timer_subsystem_init, switch_errno, switch_lcore_id, EALREADY};

/// The calling thread's process-lifetime EAL setup.
///
/// Dropping this owner does not clean up EAL or unregister its main lcore.
/// EAL cannot be initialized a second time after successful initialization.
///
/// ```compile_fail
/// use dpdk_smoltcp::Eal;
/// fn requires_send<T: Send>() {}
/// requires_send::<Eal>();
/// ```
///
/// ```compile_fail
/// use dpdk_smoltcp::Eal;
/// fn requires_sync<T: Sync>() {}
/// requires_sync::<Eal>();
/// ```
#[derive(Debug)]
pub struct Eal {
    owner_lcore_id: u32,
    _thread_local: PhantomData<Rc<()>>,
}

impl Eal {
    /// Initialize DPDK with caller-supplied argv, including the program name.
    ///
    /// Strings and the pointer array are copied into writable C storage. Argument
    /// strings passed to DPDK remain allocated until process exit because DPDK
    /// retains pointers to parsed string options, even after a returned failure.
    /// No options are added, and the calling thread becomes EAL's main lcore.
    /// Then initializes DPDK's timer subsystem (already initialized, e.g. by the ENA PMD at
    /// probe, is success), so `rte_timer_manage` is valid on every PMD; see `Stack::turn`.
    ///
    /// Empty argv, an unrepresentable count and interior NUL bytes are rejected
    /// before calling DPDK. Returned DPDK failures preserve its return code and
    /// `rte_errno`. DPDK's help and some argument-parser errors exit the process;
    /// this method preserves those startup semantics. Failure does not promise
    /// that initialization can be retried.
    pub fn init(args: &[&str]) -> Result<Self, EalError> {
        let argc = argc_for_count(args.len())?;
        let mut storage = args
            .iter()
            .enumerate()
            .map(|(argument, arg)| {
                CString::new(*arg)
                    .map(CString::into_bytes_with_nul)
                    .map_err(|err| EalError::InteriorNul {
                        argument,
                        position: err.nul_position(),
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let mut argv: Vec<*mut c_char> = storage
            .iter_mut()
            .map(|arg| arg.as_mut_ptr().cast())
            .collect();
        argv.push(std::ptr::null_mut());

        // Each pointer refers to writable, NUL-terminated storage for this call.
        let return_code = unsafe { rte_eal_init(argc, argv.as_mut_ptr()) };
        let errno = if return_code < 0 {
            unsafe { switch_errno() }
        } else {
            0
        };
        // EAL's global parsed options can retain pointers into these strings.
        std::mem::forget(storage);
        if return_code < 0 {
            return Err(EalError::Initialization { return_code, errno });
        }
        // SAFETY: EAL is initialized; this is its main lcore.
        let return_code = unsafe { rte_timer_subsystem_init() };
        if return_code != 0 && return_code != -(EALREADY as c_int) {
            return Err(EalError::TimerSubsystem { return_code });
        }
        Ok(Self {
            owner_lcore_id: unsafe { switch_lcore_id() },
            _thread_local: PhantomData,
        })
    }

    pub fn owner_lcore_id(&self) -> u32 {
        self.owner_lcore_id
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EalError {
    EmptyArguments,
    ArgumentCountTooLarge { count: usize },
    InteriorNul { argument: usize, position: usize },
    Initialization { return_code: c_int, errno: c_int },
    /// `rte_timer_subsystem_init` failed after EAL initialized.
    TimerSubsystem { return_code: c_int },
}

impl fmt::Display for EalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyArguments => f.write_str("EAL argv requires a program name"),
            Self::ArgumentCountTooLarge { count } => {
                write!(f, "EAL argument count {count} exceeds DPDK's C count limit")
            }
            Self::InteriorNul { argument, position } => {
                write!(
                    f,
                    "EAL argument {argument} contains a NUL at byte {position}"
                )
            }
            Self::Initialization { return_code, errno } => {
                write!(
                    f,
                    "rte_eal_init returned {return_code} with rte_errno {errno}"
                )
            }
            Self::TimerSubsystem { return_code } => {
                write!(f, "rte_timer_subsystem_init returned {return_code}")
            }
        }
    }
}

impl std::error::Error for EalError {}

fn argc_for_count(count: usize) -> Result<c_int, EalError> {
    if count == 0 {
        return Err(EalError::EmptyArguments);
    }
    // DPDK's eal_save_args computes argc + 1 in signed C arithmetic.
    c_int::try_from(count)
        .ok()
        .filter(|&argc| argc < c_int::MAX)
        .ok_or(EalError::ArgumentCountTooLarge { count })
}

#[cfg(test)]
pub(crate) fn with_test_eal<R>(body: impl FnOnce(&Eal) -> R) -> R {
    thread_local! {
        static EAL: std::cell::OnceCell<Eal> = const { std::cell::OnceCell::new() };
    }
    EAL.with(|owner| {
        let eal = owner.get_or_init(|| {
            let args = switch_test_kit::eal_args();
            Eal::init(&args.iter().map(String::as_str).collect::<Vec<_>>())
                .expect("resource test EAL initialization")
        });
        assert_eq!(eal.owner_lcore_id(), 7);
        assert_eq!(unsafe { switch_lcore_id() }, 7);
        body(eal)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn argument_count_rejects_empty_and_c_overflow() {
        assert_eq!(argc_for_count(0), Err(EalError::EmptyArguments));
        assert_eq!(argc_for_count(1), Ok(1));
        assert_eq!(argc_for_count(c_int::MAX as usize - 1), Ok(c_int::MAX - 1));
        for count in [c_int::MAX as usize, c_int::MAX as usize + 1] {
            assert_eq!(
                argc_for_count(count),
                Err(EalError::ArgumentCountTooLarge { count })
            );
        }
    }
}
