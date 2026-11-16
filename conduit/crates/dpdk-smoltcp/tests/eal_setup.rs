use dpdk_smoltcp::{Eal, EalError};
use switch_dpdk_sys::{switch_lcore_id, EALREADY};

#[test]
fn explicit_argv_initializes_once_and_drop_retains_main_lcore() {
    assert!(matches!(Eal::init(&[]), Err(EalError::EmptyArguments)));
    assert!(matches!(
        Eal::init(&["switch\0invalid"]),
        Err(EalError::InteriorNul {
            argument: 0,
            position: 6
        })
    ));
    assert!(matches!(
        Eal::init(&["switch", "--no-huge\0invalid"]),
        Err(EalError::InteriorNul {
            argument: 1,
            position: 9
        })
    ));

    let cpu = switch_dpdk_sys::only_allowed_cpu().unwrap_or_else(|error| panic!("{error}"));
    let lcores = format!("--lcores=7@{cpu}");
    let args = [
        "switch-eal-setup",
        "--no-pci",
        "--no-huge",
        "--no-shconf",
        "--no-telemetry",
        lcores.as_str(),
        "--main-lcore=7",
        "--",
        "application-argument",
    ];
    let eal = Eal::init(&args).expect("explicit test EAL argv must initialize");
    assert_eq!(eal.owner_lcore_id(), 7);
    assert_eq!(unsafe { switch_lcore_id() }, eal.owner_lcore_id());

    // The drop states what the test checks: dropping Eal keeps the main lcore.
    #[allow(clippy::drop_non_drop)]
    drop(eal);
    assert_eq!(unsafe { switch_lcore_id() }, 7);
    assert!(matches!(
        Eal::init(&args),
        Err(EalError::Initialization { return_code: -1, errno }) if errno == EALREADY as i32
    ));
}
