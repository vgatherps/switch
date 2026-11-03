//! Optional owner-thread snapshots of ENA allowance counters, outside the polling loop.

use std::collections::{BTreeMap, BTreeSet};
use std::time::{SystemTime, UNIX_EPOCH};

use zhida_dpdk_sys::{rte_eth_xstat, rte_eth_xstat_name, rte_eth_xstats_get, rte_eth_xstats_get_names};

const COUNTERS: [&str; 3] = [
    "pps_allowance_exceeded",
    "bw_in_allowance_exceeded",
    "bw_out_allowance_exceeded",
];
type Counters = BTreeMap<String, u64>;

fn select(names: &[String], values: &[(u64, u64)]) -> Result<Counters, String> {
    let mut counters = Counters::new();
    let mut ids = BTreeSet::new();
    for &(id, value) in values {
        let name = names.get(usize::try_from(id).map_err(|_| format!("invalid xstat id {id}"))?)
            .ok_or_else(|| format!("invalid xstat id {id}"))?;
        if !ids.insert(id) {
            return Err(format!("duplicate xstat id {id}"));
        }
        if COUNTERS.contains(&name.as_str()) && counters.insert(name.clone(), value).is_some() {
            return Err(format!("duplicate xstat name {name}"));
        }
    }
    Ok(counters)
}

fn query(port: u16) -> Result<Counters, String> {
    // SAFETY: the caller owns the live port on this thread; buffers are valid for their sizes.
    let count = unsafe { rte_eth_xstats_get_names(port, std::ptr::null_mut(), 0) };
    if count < 0 {
        return Err(format!("rte_eth_xstats_get_names count: {count}"));
    }
    let n = count as usize;
    let mut names: Vec<rte_eth_xstat_name> = (0..n).map(|_| unsafe { std::mem::zeroed() }).collect();
    let got = unsafe { rte_eth_xstats_get_names(port, names.as_mut_ptr(), count as u32) };
    if got != count {
        return Err(format!("rte_eth_xstats_get_names: expected {count}, got {got}"));
    }
    let names: Vec<String> = names.iter().map(|entry| {
        let bytes: Vec<u8> = entry.name.iter().map(|&c| c as u8).take_while(|&c| c != 0).collect();
        String::from_utf8(bytes).map_err(|e| format!("invalid xstat name: {e}"))
    }).collect::<Result<_, _>>()?;
    let mut values: Vec<rte_eth_xstat> = (0..n).map(|_| unsafe { std::mem::zeroed() }).collect();
    let got = unsafe { rte_eth_xstats_get(port, values.as_mut_ptr(), count as u32) };
    if got != count {
        return Err(format!("rte_eth_xstats_get: expected {count}, got {got}"));
    }
    select(&names, &values.iter().map(|v| (v.id, v.value)).collect::<Vec<_>>())
}

fn missing(counters: &Counters) -> Vec<&'static str> {
    COUNTERS.into_iter().filter(|name| !counters.contains_key(*name)).collect()
}

fn invalid(before: Option<(&Counters, u64)>, after: &Counters, resets: u64) -> Vec<String> {
    let mut reasons = Vec::new();
    if !missing(after).is_empty() {
        reasons.push("missing counters".into());
    }
    match before {
        None => reasons.push("startup snapshot unavailable".into()),
        Some((old, old_resets)) => {
            if !missing(old).is_empty() { reasons.push("startup counters missing".into()); }
            if resets != old_resets { reasons.push("port reset".into()); }
            for (name, value) in after {
                if old.get(name).is_some_and(|previous| value < previous) {
                    reasons.push(format!("counter decreased: {name}"));
                }
            }
        }
    }
    reasons
}

fn emit(role: &str, phase: &str, port: u16, resets: u64, result: &Result<Counters, String>, invalid: &[String]) {
    let unix_ns = SystemTime::now().duration_since(UNIX_EPOCH).expect("clock after 1970").as_nanos();
    let record = match result {
        Ok(counters) => serde_json::json!({"kind":"ena_xstats", "role":role, "phase":phase,
            "port":port, "unix_ns":unix_ns, "port_resets":resets,
            "status":if missing(counters).is_empty() { "ok" } else { "unsupported" },
            "counters":counters, "missing":missing(counters), "invalid":invalid}),
        Err(error) => serde_json::json!({"kind":"ena_xstats", "role":role, "phase":phase,
            "port":port, "unix_ns":unix_ns, "port_resets":resets,
            "status":"error", "error":error, "invalid":invalid}),
    };
    eprintln!("{record}");
}

pub(crate) struct Snapshots {
    role: &'static str,
    port: u16,
    before: Option<(Counters, u64)>,
}

impl Snapshots {
    pub(crate) fn start(role: &'static str, port: u16) -> Self {
        let resets = dpdk_smoltcp::port_resets();
        let result = query(port);
        emit(role, "startup", port, resets, &result, &[]);
        Self { role, port, before: result.ok().map(|values| (values, resets)) }
    }

    pub(crate) fn finish(self) {
        let resets = dpdk_smoltcp::port_resets();
        let result = query(self.port);
        let reasons = match &result {
            Ok(after) => invalid(self.before.as_ref().map(|(v, r)| (v, *r)), after, resets),
            Err(_) => vec!["close snapshot unavailable".into()],
        };
        emit(self.role, "close", self.port, resets, &result, &reasons);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn selects_values_by_id_and_reports_missing_without_zero() {
        let names = vec!["other".into(), "pps_allowance_exceeded".into(), "bw_in_allowance_exceeded".into()];
        let selected = select(&names, &[(2, 7), (0, 99), (1, 4)]).unwrap();
        assert_eq!(selected["pps_allowance_exceeded"], 4);
        assert_eq!(selected["bw_in_allowance_exceeded"], 7);
        assert_eq!(missing(&selected), vec!["bw_out_allowance_exceeded"]);
        assert!(!selected.contains_key("bw_out_allowance_exceeded"));
        assert!(select(&names, &[(4, 0)]).is_err());
        assert!(select(&names, &[(1, 0), (1, 1)]).is_err());
    }

    #[test]
    fn invalidates_reset_missing_and_decreased_counters() {
        let before: Counters = COUNTERS.into_iter().map(|n| (n.to_string(), 10)).collect();
        let mut after = before.clone();
        assert!(invalid(Some((&before, 1)), &after, 1).is_empty());
        assert!(invalid(Some((&before, 1)), &after, 2).contains(&"port reset".to_owned()));
        after.insert(COUNTERS[0].to_owned(), 9);
        after.remove(COUNTERS[1]);
        let reasons = invalid(Some((&before, 1)), &after, 1);
        assert!(reasons.contains(&"missing counters".to_owned()));
        assert!(reasons.contains(&format!("counter decreased: {}", COUNTERS[0])));
        assert!(invalid(None, &after, 0).contains(&"startup snapshot unavailable".to_owned()));
    }
}
