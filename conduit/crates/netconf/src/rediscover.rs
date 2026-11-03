use crate::{discover::Discovered, Config, ConfigError, Device};
use std::{
    fs,
    io::{ErrorKind, Write},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

/// Discovered facts replace the previous ones; a configured jumbo MTU, devargs, offloads,
/// rx_timestamps, descriptors, eal, path rules other than `vpc` and `default`, and `owned_ipv4`
/// unless the source enumerates the interface's addresses are kept.
pub(crate) fn merge(
    mut discovered: Config,
    enumerates_owned_ipv4: bool,
    previous: &Config,
) -> Result<Config, ConfigError> {
    if !enumerates_owned_ipv4 {
        discovered.owned_ipv4.clone_from(&previous.owned_ipv4);
    }
    if previous.mtu > 1500 {
        discovered.mtu = previous.mtu;
    }
    if let (Device::Pci(new), Device::Pci(old)) = (&mut discovered.device, &previous.device) {
        new.devargs.clone_from(&old.devargs);
    }
    discovered.offloads.clone_from(&previous.offloads);
    discovered.rx_timestamps = previous.rx_timestamps;
    discovered.descriptors = previous.descriptors;
    discovered.eal.clone_from(&previous.eal);
    let mut paths: Vec<_> = previous
        .paths
        .iter()
        .filter(|p| p.why != "vpc" && p.why != "default")
        .cloned()
        .collect();
    paths.extend(discovered.paths.into_iter().filter(|p| {
        !previous
            .paths
            .iter()
            .any(|old| old.why != "vpc" && old.why != "default" && old.prefix == p.prefix)
    }));
    discovered.paths = paths;
    Config::from_json(&discovered.to_json())
}

pub fn load_existing(path: &Path) -> Result<Option<Config>, ConfigError> {
    match fs::symlink_metadata(path) {
        Ok(_) => Config::load(path).map(Some),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(ConfigError(format!("{}: {e}", path.display()))),
    }
}

/// Numbers this process's temp files (with the pid, unique among live processes).
pub(crate) static TEMP_SEQ: AtomicU64 = AtomicU64::new(0);
/// Temp names tried before giving up; a name is taken only by a leftover of a dead process that
/// had this pid.
const TEMP_TRIES: u64 = 64;

/// Temp file number `n` of this process for `path`, in the same directory.
pub(crate) fn temp_path(path: &Path, n: u64) -> PathBuf {
    let name = path.file_name().map_or_else(Default::default, |n| n.to_string_lossy().into_owned());
    path.with_file_name(format!(".{name}.{}.{n}.tmp", std::process::id()))
}

/// Writes `bytes` to `path` through a new temp file in the same directory, fsynced, so a failure
/// or a kill leaves no partial file. `previous`: renamed over the file (through a symlink, its
/// target), which keeps its permissions. Otherwise hard-linked as `path`, which refuses a file
/// that appeared there since it was found absent (as `create_new` did), then the temp name is
/// removed; failing that, it is left with a warning. A temp file left by a killed run never blocks a later one: each run takes a fresh
/// name.
fn write_atomically(path: &Path, bytes: &[u8], previous: bool) -> Result<(), ConfigError> {
    let error = |at: &Path, e: std::io::Error| ConfigError(format!("{}: {e}", at.display()));
    let (path, permissions) = if previous {
        let path = fs::canonicalize(path).map_err(|e| error(path, e))?;
        let permissions = fs::metadata(&path).map_err(|e| error(&path, e))?.permissions();
        (path, Some(permissions))
    } else {
        (path.to_path_buf(), None)
    };
    let mut tries = 0;
    let (temp, mut file) = loop {
        let temp = temp_path(&path, TEMP_SEQ.fetch_add(1, Ordering::Relaxed));
        match fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp)
        {
            Ok(file) => break (temp, file),
            Err(e) if e.kind() == ErrorKind::AlreadyExists && tries + 1 < TEMP_TRIES => tries += 1,
            Err(e) => return Err(error(&temp, e)),
        }
    };
    let written = file
        .write_all(bytes)
        .and_then(|()| permissions.map_or(Ok(()), |p| file.set_permissions(p)))
        .and_then(|()| file.sync_all());
    if let Err(e) = written {
        let _ = fs::remove_file(&temp);
        return Err(error(&temp, e));
    }
    let published = if previous {
        fs::rename(&temp, &path)
    } else {
        fs::hard_link(&temp, &path)
    };
    if let Err(e) = published {
        let _ = fs::remove_file(&temp);
        return Err(error(&path, e));
    }
    // The config is in place: a temp name left behind is reported, not a failed write.
    if !previous {
        if let Err(e) = fs::remove_file(&temp) {
            eprintln!("zhida-netconf: warning: {}", error(&temp, e).0);
        }
    }
    Ok(())
}

pub fn write(
    path: &Path,
    discovered: &Discovered,
    previous: Option<&Config>,
    changes: &mut impl Write,
) -> Result<(), ConfigError> {
    let config = match previous {
        Some(old) => merge(
            discovered.config.clone(),
            discovered.enumerates_owned_ipv4,
            old,
        )?,
        None => discovered.config.clone(),
    };
    let mut changed = Vec::new();
    if let Some(old) = previous {
        let old = serde_json::to_value(old).map_err(|e| ConfigError(e.to_string()))?;
        let new = serde_json::to_value(&config).map_err(|e| ConfigError(e.to_string()))?;
        for key in [
            "version",
            "device",
            "mac",
            "ipv4",
            "gateway",
            "owned_ipv4",
            "mtu",
            "paths",
            "offloads",
            "rx_timestamps",
            "descriptors",
            "eal",
        ] {
            if old[key] != new[key] {
                changed.push(key);
            }
        }
    }
    write_atomically(path, config.to_json().as_bytes(), previous.is_some())?;
    let report = |changes: &mut dyn Write, line: std::fmt::Arguments| {
        writeln!(changes, "{line}").map_err(|e| ConfigError(e.to_string()))
    };
    for key in changed {
        report(changes, format_args!("changed: {key}"))?;
        if key == "owned_ipv4" {
            let old = &previous.expect("a changed key has a previous config").owned_ipv4;
            for cidr in old.iter().filter(|cidr| !config.owned_ipv4.contains(cidr)) {
                report(changes, format_args!("owned_ipv4 removed: {cidr}"))?;
            }
            for cidr in config.owned_ipv4.iter().filter(|cidr| !old.contains(cidr)) {
                report(changes, format_args!("owned_ipv4 added: {cidr}"))?;
            }
        }
    }
    Ok(())
}
