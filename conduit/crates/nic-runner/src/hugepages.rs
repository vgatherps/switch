//! Hugepages this process took at open: the free count of each hugepage size before EAL starts
//! minus the count after the Port and its pools exist. `deploy/hft-host.sh verify` reads the line
//! from the last run's log and compares it with the pages it reserved.

use std::fs;
use std::path::Path;

/// The kernel's hugepage pools: one `hugepages-<size>kB` directory per size.
pub const SYSFS: &str = "/sys/kernel/mm/hugepages";

/// `free_hugepages` of every size under `dir`, sorted by directory name; empty if `dir` is absent.
pub fn free(dir: &Path) -> Vec<(String, u64)> {
    let Ok(entries) = fs::read_dir(dir) else {
        return Vec::new();
    };
    let mut sizes: Vec<_> = entries
        .filter_map(|entry| {
            let entry = entry.ok()?;
            let name = entry.file_name().into_string().ok()?;
            let count = fs::read_to_string(entry.path().join("free_hugepages")).ok()?;
            Some((name, count.trim().parse().ok()?))
        })
        .collect();
    sizes.sort();
    sizes
}

/// One `hugepages` report value per size: `<size> used=<before - after> free=<after>`.
pub fn report(before: &[(String, u64)], after: &[(String, u64)]) -> Vec<String> {
    after
        .iter()
        .map(|(name, free)| {
            let was = before.iter().find(|(n, _)| n == name).map_or(*free, |(_, f)| *f);
            format!("{name} used={} free={free}", was.saturating_sub(*free))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::Path;

    use super::{free, report};

    fn size(dir: &Path, name: &str, free: u64) {
        fs::create_dir_all(dir.join(name)).unwrap();
        fs::write(dir.join(name).join("free_hugepages"), format!("{free}\n")).unwrap();
    }

    #[test]
    fn pages_taken_between_two_reads() {
        let dir = std::env::temp_dir().join(format!("zhida-nic-hugepages-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        size(&dir, "hugepages-2048kB", 512);
        size(&dir, "hugepages-1048576kB", 0);
        let before = free(&dir);
        assert_eq!(
            before,
            [("hugepages-1048576kB".to_owned(), 0), ("hugepages-2048kB".to_owned(), 512)]
        );
        size(&dir, "hugepages-2048kB", 489);
        let after = free(&dir);
        assert_eq!(
            report(&before, &after),
            ["hugepages-1048576kB used=0 free=0", "hugepages-2048kB used=23 free=489"]
        );
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn no_hugetlb_no_report() {
        assert!(free(Path::new("/nonexistent/hugepages")).is_empty());
    }
}
