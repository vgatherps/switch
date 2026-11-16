use std::{
    fs,
    process::Command,
    time::{SystemTime, UNIX_EPOCH},
};

#[test]
fn check_prints_summary_and_exit_codes() {
    let path = std::env::temp_dir().join(format!(
        "switch-netconf-{}-{}.json",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let run = |args: &[&std::ffi::OsStr]| {
        Command::new(env!("CARGO_BIN_EXE_switch-netconf"))
            .args(args)
            .output()
            .unwrap()
    };
    let check = std::ffi::OsStr::new("check");
    fs::write(
        &path,
        r#"{
        "version": 1, "device": {"vdev": "net_tap0,iface=cdnic0"}, "mac": null,
        "ipv4": "10.0.1.23/24", "gateway": null, "mtu": 9001,
        "paths": [{"prefix": "10.0.0.0/16", "mtu": 9001, "why": "vpc"},
                  {"prefix": "0.0.0.0/0", "mtu": 1500, "why": "default"}],
        "offloads": "auto", "rx_timestamps": false, "descriptors": 1024,
        "eal": {"file_prefix": "switch", "extra": []}
    }"#,
    )
    .unwrap();
    let valid = run(&[check, path.as_os_str()]);
    let mut invalid_config: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(&path).unwrap()).unwrap();
    invalid_config["version"] = serde_json::json!(2);
    fs::write(&path, invalid_config.to_string()).unwrap();
    let invalid = run(&[check, path.as_os_str()]);
    fs::remove_file(&path).unwrap();
    let missing = run(&[check, path.as_os_str()]);
    assert_eq!(valid.status.code(), Some(0));
    assert!(valid.stderr.is_empty());
    let stdout = String::from_utf8(valid.stdout).unwrap();
    assert!(stdout.contains("ipv4: 10.0.1.23/24\n"));
    assert!(stdout.contains("  10.0.0.0/16 mtu 9001 (vpc)\n"));
    assert!(stdout.contains("  0.0.0.0/0 mtu 1500 (default)\n"));
    assert_eq!(invalid.status.code(), Some(1));
    let stderr = String::from_utf8(invalid.stderr).unwrap();
    assert_eq!(stderr.lines().count(), 1);
    assert!(stderr.contains("version"));
    assert!(stderr.starts_with(&format!("switch-netconf: {}: ", path.display())));
    assert_eq!(missing.status.code(), Some(1));
    assert_eq!(
        String::from_utf8(missing.stderr).unwrap().lines().count(),
        1
    );
    for args in [
        &[][..],
        &[std::ffi::OsStr::new("unknown")][..],
        &[check][..],
        &[check, path.as_os_str(), std::ffi::OsStr::new("extra")][..],
    ] {
        let output = run(args);
        assert_eq!(output.status.code(), Some(2));
        assert_eq!(
            String::from_utf8(output.stderr).unwrap(),
            "usage: switch-netconf check FILE | switch-netconf discover --interface IF [--ipv4 ADDR] [--aws] [-o FILE] | switch-netconf bind FILE [--noiommu] [--dry-run] | switch-netconf unbind FILE\n"
        );
    }
}

/// `check FILE | grep -q ...`: grep can exit before check writes. The write error is reported as
/// one line and exit 1, not a panic (101).
#[test]
fn check_with_closed_stdout_does_not_panic() {
    let path = std::env::temp_dir().join(format!(
        "switch-netconf-closed-{}-{}.json",
        std::process::id(),
        SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos()
    ));
    fs::write(
        &path,
        r#"{"version": 1, "device": {"vdev": "net_tap0,iface=cdnic0"}, "mac": null,
        "ipv4": "10.0.1.23/24", "gateway": null, "mtu": 1500, "paths": [],
        "offloads": "auto", "rx_timestamps": false, "descriptors": 1024,
        "eal": {"file_prefix": "switch", "extra": []}}"#,
    )
    .unwrap();
    let (reader, writer) = std::io::pipe().unwrap();
    drop(reader);
    let output = Command::new(env!("CARGO_BIN_EXE_switch-netconf"))
        .arg("check")
        .arg(&path)
        .stdout(writer)
        .output()
        .unwrap();
    fs::remove_file(&path).unwrap();
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert_eq!(output.status.code(), Some(1), "{stderr}");
    assert_eq!(stderr.lines().count(), 1, "{stderr}");
    assert!(stderr.starts_with("switch-netconf: "), "{stderr}");
}
