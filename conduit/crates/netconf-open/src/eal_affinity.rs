use std::{fs, io};

use dpdk_smoltcp::Eal;

use crate::OpenError;

fn cpu_list(text: &str) -> io::Result<libc::cpu_set_t> {
    let mut set = unsafe { std::mem::zeroed() };
    for item in text.trim().split(',').filter(|item| !item.is_empty()) {
        let invalid = || io::Error::new(io::ErrorKind::InvalidData, "invalid Linux CPU list");
        let (first, last) = item.split_once('-').unwrap_or((item, item));
        let first: usize = first.parse().map_err(|_| invalid())?;
        let last: usize = last.parse().map_err(|_| invalid())?;
        if first > last || last >= libc::CPU_SETSIZE as usize {
            return Err(invalid());
        }
        for cpu in first..=last {
            unsafe { libc::CPU_SET(cpu, &mut set) };
        }
    }
    Ok(set)
}

fn set_affinity(set: &libc::cpu_set_t) -> io::Result<()> {
    // Linux intersects this mask with the thread's effective cpuset cgroup.
    if unsafe { libc::sched_setaffinity(0, std::mem::size_of_val(set), set) } == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

pub(crate) fn init(cpu: usize, args: &[&str]) -> Result<Eal, OpenError> {
    let isolated = match fs::read_to_string("/sys/devices/system/cpu/isolated") {
        Ok(text) => cpu_list(&text).map_err(OpenError::HelperAffinity)?,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Eal::init(args).map_err(OpenError::Eal);
        }
        Err(error) => return Err(OpenError::HelperAffinity(error)),
    };
    if !unsafe { libc::CPU_ISSET(cpu, &isolated) } {
        return Eal::init(args).map_err(OpenError::Eal);
    }
    let online = fs::read_to_string("/sys/devices/system/cpu/online")
        .and_then(|text| cpu_list(&text))
        .map_err(OpenError::HelperAffinity)?;
    init_with_housekeeping(cpu, args, &isolated, &online)
}

fn init_with_housekeeping(
    cpu: usize,
    args: &[&str],
    isolated: &libc::cpu_set_t,
    online: &libc::cpu_set_t,
) -> Result<Eal, OpenError> {
    let mut polling = unsafe { std::mem::zeroed() };
    unsafe { libc::CPU_SET(cpu, &mut polling) };
    let mut expanded = polling;
    for candidate in 0..libc::CPU_SETSIZE as usize {
        if unsafe { libc::CPU_ISSET(candidate, online) && !libc::CPU_ISSET(candidate, isolated) } {
            unsafe { libc::CPU_SET(candidate, &mut expanded) };
        }
    }
    // Include the polling CPU, so a one-CPU cgroup still has a valid mask.
    set_affinity(&expanded).map_err(OpenError::HelperAffinity)?;
    let result = Eal::init(args);
    // EAL pins its main lcore on success; also restore on every error path.
    match (result, set_affinity(&polling)) {
        (Ok(eal), Ok(())) => Ok(eal),
        (Err(error), Ok(())) => Err(OpenError::Eal(error)),
        (Ok(_), Err(error)) => Err(OpenError::HelperAffinity(error)),
        (Err(error), Err(restore)) => Err(OpenError::EalAffinityRestore { error, restore }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Command;

    fn affinity(tid: libc::pid_t) -> libc::cpu_set_t {
        let mut set = unsafe { std::mem::zeroed() };
        assert_eq!(
            unsafe { libc::sched_getaffinity(tid, std::mem::size_of_val(&set), &mut set) },
            0
        );
        set
    }

    #[test]
    fn eal_helpers_avoid_polling_cpu() {
        const CHILD: &str = "ZHIDA_AFFINITY_TEST_CHILD";
        if std::env::var_os(CHILD).is_none() {
            let status = Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "eal_affinity::tests::eal_helpers_avoid_polling_cpu",
                    "--nocapture",
                ])
                .env(CHILD, "1")
                .status()
                .unwrap();
            assert!(status.success());
            return;
        }
        let allowed = affinity(0);
        let cpus: Vec<_> = (0..libc::CPU_SETSIZE as usize)
            .filter(|cpu| unsafe { libc::CPU_ISSET(*cpu, &allowed) })
            .collect();
        if cpus.len() < 2 {
            eprintln!("SKIP: EAL helper affinity requires two allowed CPUs");
            return;
        }
        let cpu = cpus[0];
        let mut isolated =
            cpu_list(&fs::read_to_string("/sys/devices/system/cpu/isolated").unwrap_or_default())
                .unwrap();
        unsafe { libc::CPU_SET(cpu, &mut isolated) };
        if cpus.len() >= 3 {
            // Also cover exclusion of an isolated CPU other than the poller.
            unsafe { libc::CPU_SET(cpus[1], &mut isolated) };
        }
        if !cpus
            .iter()
            .any(|candidate| !unsafe { libc::CPU_ISSET(*candidate, &isolated) })
        {
            eprintln!("SKIP: no allowed non-isolated housekeeping CPU");
            return;
        }
        // Model an isolated polling CPU without requiring host boot changes.
        // Both affinity changes and the DPDK helper creation below are real.
        let mut polling = unsafe { std::mem::zeroed() };
        unsafe { libc::CPU_SET(cpu, &mut polling) };
        set_affinity(&polling).unwrap();
        let args = format!("--lcores=0@{cpu}");
        assert!(matches!(
            init_with_housekeeping(cpu, &["test", "invalid\0argument"], &isolated, &allowed),
            Err(OpenError::Eal(_))
        ));
        assert_eq!(unsafe { libc::CPU_COUNT(&affinity(0)) }, 1);
        assert!(unsafe { libc::CPU_ISSET(cpu, &affinity(0)) });
        let _eal = init_with_housekeeping(
            cpu,
            &[
                "test",
                "--no-pci",
                "--no-huge",
                "--no-shconf",
                "--no-telemetry",
                &args,
            ],
            &isolated,
            &allowed,
        )
        .unwrap();
        assert_eq!(unsafe { libc::CPU_COUNT(&affinity(0)) }, 1);
        assert!(unsafe { libc::CPU_ISSET(cpu, &affinity(0)) });
        let mut helpers = 0;
        for task in fs::read_dir("/proc/self/task").unwrap() {
            let task = task.unwrap();
            let name = fs::read_to_string(task.path().join("comm")).unwrap();
            if name.trim() == "dpdk-intr" || name.trim() == "dpdk-mp-msg" {
                let tid = task.file_name().to_string_lossy().parse().unwrap();
                let set = affinity(tid);
                assert!(
                    !unsafe { libc::CPU_ISSET(cpu, &set) },
                    "{name} shares polling CPU"
                );
                assert!(unsafe { libc::CPU_COUNT(&set) } > 0);
                for candidate in 0..libc::CPU_SETSIZE as usize {
                    if unsafe { libc::CPU_ISSET(candidate, &set) } {
                        assert!(!unsafe { libc::CPU_ISSET(candidate, &isolated) });
                        assert!(unsafe { libc::CPU_ISSET(candidate, &allowed) });
                    }
                }
                helpers += 1;
            }
        }
        assert!(helpers > 0, "EAL created no observable helper thread");
    }
}
