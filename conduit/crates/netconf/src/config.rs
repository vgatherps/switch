use serde::{de::Error, Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;
use smoltcp::iface::{PathMtu, PathMtus, MIN_IP_MTU};
use smoltcp::wire::{EthernetAddress, IpAddress, Ipv4Address, Ipv4Cidr};
use std::{fmt, fs, path::Path, str::FromStr};

/// Error for a port or path MTU below `MIN_IP_MTU`.
const MTU_TOO_SMALL: &str = "must be at least 88 (48-byte minimum TCP MSS + 40 bytes of headers)";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, try_from = "JsonConfig", into = "JsonConfig")]
pub struct Config {
    pub device: Device,
    pub mac: Option<EthernetAddress>,
    pub ipv4: Ipv4Cidr,
    pub gateway: Option<Ipv4Address>,
    /// IPv4 addresses the Stack owns besides `ipv4`: single addresses (/32) and prefixes. Sorted,
    /// none inside another (duplicates and contained entries are dropped on load). Ownership
    /// only: `ipv4` keeps routing, the subnet broadcast and the default source.
    pub owned_ipv4: Vec<Ipv4Cidr>,
    pub mtu: u16,
    pub paths: Vec<PathRule>,
    pub offloads: Offloads,
    pub rx_timestamps: bool,
    pub descriptors: u16,
    pub eal: EalConfig,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged, deny_unknown_fields)]
pub enum Device {
    Pci(PciDevice),
    Vdev { vdev: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PciDevice {
    pub pci: String,
    pub pci_id: String,
    pub devargs: String,
    pub kernel_driver: String,
    pub kernel_interface: String,
    pub numa_node: i32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PathRule {
    #[serde(
        serialize_with = "serialize_string",
        deserialize_with = "deserialize_prefix"
    )]
    pub prefix: Ipv4Cidr,
    pub mtu: u16,
    pub why: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, try_from = "Value", into = "JsonOffloads")]
pub enum Offloads {
    Auto,
    Explicit {
        rx_checksum: bool,
        tx_tcp_checksum: bool,
        tx_udp_checksum: bool,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EalConfig {
    pub file_prefix: String,
    pub extra: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigError(pub(crate) String);

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for ConfigError {}

fn error(field: &str, message: &str) -> ConfigError {
    ConfigError(format!("{field}: {message}"))
}

#[derive(Serialize)]
#[serde(untagged, deny_unknown_fields)]
enum JsonOffloads {
    Auto(String),
    Explicit(ExplicitOffloads),
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ExplicitOffloads {
    rx_checksum: bool,
    tx_tcp_checksum: bool,
    tx_udp_checksum: bool,
}

impl TryFrom<Value> for Offloads {
    type Error = ConfigError;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        if let Value::String(value) = value {
            return if value == "auto" {
                Ok(Self::Auto)
            } else {
                Err(error("offloads", "offloads string must be auto"))
            };
        }
        let o: ExplicitOffloads =
            serde_json::from_value(value).map_err(|e| ConfigError(e.to_string()))?;
        Ok(Self::Explicit {
            rx_checksum: o.rx_checksum,
            tx_tcp_checksum: o.tx_tcp_checksum,
            tx_udp_checksum: o.tx_udp_checksum,
        })
    }
}

impl From<Offloads> for JsonOffloads {
    fn from(offloads: Offloads) -> Self {
        match offloads {
            Offloads::Auto => Self::Auto("auto".into()),
            Offloads::Explicit {
                rx_checksum,
                tx_tcp_checksum,
                tx_udp_checksum,
            } => Self::Explicit(ExplicitOffloads {
                rx_checksum,
                tx_tcp_checksum,
                tx_udp_checksum,
            }),
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct JsonConfig {
    version: u32,
    device: Device,
    #[serde(deserialize_with = "required_option")]
    mac: Option<String>,
    ipv4: String,
    #[serde(deserialize_with = "required_option")]
    gateway: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    owned_ipv4: Vec<String>,
    mtu: u16,
    paths: Vec<PathRule>,
    offloads: Offloads,
    rx_timestamps: bool,
    descriptors: u16,
    eal: EalConfig,
}

fn required_option<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::deserialize(deserializer)
}

fn serialize_string<S: Serializer, T: fmt::Display>(value: &T, s: S) -> Result<S::Ok, S::Error> {
    s.collect_str(value)
}

fn deserialize_prefix<'de, D: Deserializer<'de>>(d: D) -> Result<Ipv4Cidr, D::Error> {
    String::deserialize(d)?
        .parse()
        .map_err(|_| D::Error::custom("prefix: invalid IPv4 CIDR"))
}

fn parse<T: FromStr>(value: &str, field: &str, name: &str) -> Result<T, ConfigError> {
    value
        .parse()
        .map_err(|_| error(field, &format!("invalid {name}")))
}

impl TryFrom<JsonConfig> for Config {
    type Error = ConfigError;

    fn try_from(json: JsonConfig) -> Result<Self, Self::Error> {
        if json.version != 1 {
            return Err(error("version", "must be 1"));
        }
        let mut config = Self {
            device: json.device,
            mac: json
                .mac
                .as_deref()
                .map(|v| parse(v, "mac", "MAC address"))
                .transpose()?,
            ipv4: parse(&json.ipv4, "ipv4", "IPv4 CIDR")?,
            gateway: json
                .gateway
                .as_deref()
                .map(|v| parse(v, "gateway", "IPv4 address"))
                .transpose()?,
            owned_ipv4: json
                .owned_ipv4
                .iter()
                .enumerate()
                .map(|(index, v)| parse(v, &format!("owned_ipv4[{index}]"), "IPv4 CIDR"))
                .collect::<Result<_, _>>()?,
            mtu: json.mtu,
            paths: json.paths,
            offloads: json.offloads,
            rx_timestamps: json.rx_timestamps,
            descriptors: json.descriptors,
            eal: json.eal,
        };
        validate(&config)?;
        config.owned_ipv4 = merge_owned(config.owned_ipv4);
        Ok(config)
    }
}

impl From<Config> for JsonConfig {
    fn from(config: Config) -> Self {
        Self {
            version: 1,
            device: config.device,
            mac: config.mac.map(mac_string),
            ipv4: config.ipv4.to_string(),
            gateway: config.gateway.map(|g| g.to_string()),
            owned_ipv4: config.owned_ipv4.iter().map(ToString::to_string).collect(),
            mtu: config.mtu,
            paths: config.paths,
            offloads: config.offloads,
            rx_timestamps: config.rx_timestamps,
            descriptors: config.descriptors,
            eal: config.eal,
        }
    }
}

fn matches_hex(value: &str, pattern: &str) -> bool {
    value.len() == pattern.len()
        && value
            .bytes()
            .zip(pattern.bytes())
            .all(|(byte, expected)| match expected {
                b'h' => byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte),
                b'f' => (b'0'..=b'7').contains(&byte),
                _ => byte == expected,
            })
}

fn validate(config: &Config) -> Result<(), ConfigError> {
    match &config.device {
        Device::Pci(pci) => {
            if !matches_hex(&pci.pci, "hhhh:hh:hh.f") {
                return Err(error("device.pci", "invalid PCI BDF"));
            }
            if !matches_hex(&pci.pci_id, "hhhh:hhhh") {
                return Err(error("device.pci_id", "invalid PCI ID"));
            }
            for (field, value) in [
                ("device.kernel_driver", &pci.kernel_driver),
                ("device.kernel_interface", &pci.kernel_interface),
            ] {
                if value.is_empty() {
                    return Err(error(field, "must not be empty"));
                }
            }
            if config.mac.is_none() {
                return Err(error("mac", "PCI device requires a MAC address"));
            }
        }
        Device::Vdev { vdev } if vdev.is_empty() => {
            return Err(error("device.vdev", "must not be empty"))
        }
        Device::Vdev { .. } => {}
    }
    if config.mac.is_some_and(|mac| !mac.is_unicast()) {
        return Err(error("mac", "Ethernet address must be unicast"));
    }
    if !IpAddress::Ipv4(config.ipv4.address()).is_unicast() {
        return Err(error("ipv4", "IPv4 address must be unicast"));
    }
    if let Some(gateway) = config.gateway {
        if !IpAddress::Ipv4(gateway).is_unicast() {
            return Err(error("gateway", "gateway must be unicast"));
        }
        if !config.ipv4.contains_addr(&gateway) {
            return Err(error("gateway", "gateway must be inside the IPv4 subnet"));
        }
        if gateway == config.ipv4.address() {
            return Err(error("gateway", "gateway must not be our IPv4 address"));
        }
        if let Some(broadcast) = config.ipv4.broadcast() {
            if gateway == config.ipv4.network().address() {
                return Err(error(
                    "gateway",
                    "gateway must not be the subnet's network address",
                ));
            }
            if gateway == broadcast {
                return Err(error(
                    "gateway",
                    "gateway must not be the subnet's broadcast address",
                ));
            }
        }
    }
    validate_owned(config)?;
    if config.mtu < MIN_IP_MTU {
        return Err(error("mtu", MTU_TOO_SMALL));
    }
    for (index, rule) in config.paths.iter().enumerate() {
        let field = format!("paths[{index}]");
        if rule.prefix != rule.prefix.network() {
            return Err(error(
                &format!("{field}.prefix"),
                "prefix must not have host bits set",
            ));
        }
        if let Some(earlier) = config.paths[..index]
            .iter()
            .position(|p| p.prefix == rule.prefix)
        {
            return Err(error(
                &format!("{field}.prefix"),
                &format!("duplicate of paths[{earlier}]"),
            ));
        }
        if rule.mtu < MIN_IP_MTU {
            return Err(error(&format!("{field}.mtu"), MTU_TOO_SMALL));
        }
        if rule.why.is_empty() {
            return Err(error(&format!("{field}.why"), "must not be empty"));
        }
    }
    if config.descriptors == 0 {
        return Err(error("descriptors", "must be > 0"));
    }
    if config.eal.file_prefix.is_empty() {
        return Err(error("eal.file_prefix", "must not be empty"));
    }
    Ok(())
}

/// First and last address of `cidr`'s whole range.
fn interval(cidr: Ipv4Cidr) -> (u32, u32) {
    let start = u32::from(cidr.network().address());
    (
        start,
        start
            | u32::MAX
                .checked_shr(u32::from(cidr.prefix_len()))
                .unwrap_or(0),
    )
}

/// The rules `Stack::new` applies to its owned ranges, checked over every address an entry
/// covers: no host bits, unicast (no 0.0.0.0, multicast or 255.255.255.255), not the gateway,
/// not the subnet's broadcast address.
fn validate_owned(config: &Config) -> Result<(), ConfigError> {
    const MULTICAST: (u32, u32) = (0xe000_0000, 0xefff_ffff);
    for (index, cidr) in config.owned_ipv4.iter().enumerate() {
        let field = format!("owned_ipv4[{index}]");
        if *cidr != cidr.network() {
            return Err(error(&field, "prefix must not have host bits set"));
        }
        let (start, end) = interval(*cidr);
        let holds = |addr: u32| start <= addr && addr <= end;
        if holds(0) || holds(u32::MAX) || (start <= MULTICAST.1 && MULTICAST.0 <= end) {
            return Err(error(&field, "owned IPv4 addresses must be unicast"));
        }
        if config
            .gateway
            .is_some_and(|gateway| holds(u32::from(gateway)))
        {
            return Err(error(
                &field,
                "owned IPv4 addresses must not include the gateway",
            ));
        }
        if config
            .ipv4
            .broadcast()
            .is_some_and(|broadcast| holds(u32::from(broadcast)))
        {
            return Err(error(
                &field,
                "owned IPv4 addresses must not include the subnet's broadcast address",
            ));
        }
    }
    Ok(())
}

/// Sorted by first address, with duplicates and entries inside another dropped: two prefixes are
/// either disjoint or one holds the other.
fn merge_owned(mut owned: Vec<Ipv4Cidr>) -> Vec<Ipv4Cidr> {
    owned.sort_by_key(|cidr| (interval(*cidr).0, cidr.prefix_len()));
    let mut merged: Vec<Ipv4Cidr> = Vec::with_capacity(owned.len());
    for cidr in owned {
        if merged
            .last()
            .is_none_or(|last| interval(*last).1 < interval(cidr).0)
        {
            merged.push(cidr);
        }
    }
    merged
}

/// Colon-separated lowercase hex, as sysfs prints it (smoltcp's Display uses dashes).
fn mac_string(mac: EthernetAddress) -> String {
    let b = mac.0;
    format!(
        "{:02x}:{:02x}:{:02x}:{:02x}:{:02x}:{:02x}",
        b[0], b[1], b[2], b[3], b[4], b[5]
    )
}

impl Config {
    pub fn from_json(json: &str) -> Result<Self, ConfigError> {
        serde_json::from_str(json).map_err(|e| ConfigError(e.to_string()))
    }

    pub fn load(path: &Path) -> Result<Self, ConfigError> {
        let json = fs::read_to_string(path)
            .map_err(|e| error(&path.display().to_string(), &e.to_string()))?;
        Self::from_json(&json)
    }

    pub fn to_json(&self) -> String {
        format!(
            "{}\n",
            serde_json::to_string_pretty(self).expect("config contains JSON-compatible values")
        )
    }

    /// Query before opening the Stack to size a capped socket with `path_mtu - 40`.
    /// This is the same destination lookup the Stack applies to TCP connects.
    pub fn path_mtu(&self, addr: Ipv4Address) -> u16 {
        let rules: Vec<PathMtu> = self.paths.iter()
            .map(|rule| PathMtu { prefix: rule.prefix, mtu: rule.mtu })
            .collect();
        PathMtus::new(&rules, self.ipv4, self.mtu)
            .expect("validated config has a valid path table")
            .ip_mtu(addr)
    }
}

impl fmt::Display for Config {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.device {
            Device::Pci(p) => writeln!(
                f,
                "device: pci {} ({}, {} {}, numa {})",
                p.pci, p.pci_id, p.kernel_driver, p.kernel_interface, p.numa_node
            )?,
            Device::Vdev { vdev } => writeln!(f, "device: vdev {vdev}")?,
        }
        writeln!(
            f,
            "mac: {}",
            self.mac.map_or_else(|| "device".into(), mac_string)
        )?;
        writeln!(f, "ipv4: {}", self.ipv4)?;
        writeln!(
            f,
            "gateway: {}",
            self.gateway
                .map_or_else(|| "null".into(), |g| g.to_string())
        )?;
        writeln!(
            f,
            "owned_ipv4: {}",
            if self.owned_ipv4.is_empty() {
                "none".to_owned()
            } else {
                self.owned_ipv4
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(", ")
            }
        )?;
        writeln!(f, "mtu: {}", self.mtu)?;
        writeln!(f, "paths:")?;
        for p in &self.paths {
            writeln!(f, "  {} mtu {} ({})", p.prefix, p.mtu, p.why)?;
        }
        writeln!(
            f,
            "offloads: {}",
            serde_json::to_string(&self.offloads).expect("offloads contain JSON-compatible values")
        )?;
        writeln!(f, "rx_timestamps: {}", self.rx_timestamps)?;
        writeln!(f, "descriptors: {}", self.descriptors)?;
        writeln!(
            f,
            "eal: {}",
            serde_json::to_string(&self.eal).expect("EAL contains JSON-compatible values")
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const EXPECTED: &str = "{\n  \"version\": 1,\n  \"device\": {\n    \"pci\": \"0000:00:06.0\",\n    \"pci_id\": \"1d0f:ec20\",\n    \"devargs\": \"\",\n    \"kernel_driver\": \"ena\",\n    \"kernel_interface\": \"eth1\",\n    \"numa_node\": 0\n  },\n  \"mac\": \"0a:12:34:56:78:9a\",\n  \"ipv4\": \"10.0.1.23/24\",\n  \"gateway\": \"10.0.1.1\",\n  \"mtu\": 9001,\n  \"paths\": [\n    {\n      \"prefix\": \"10.0.0.0/16\",\n      \"mtu\": 9001,\n      \"why\": \"vpc\"\n    },\n    {\n      \"prefix\": \"0.0.0.0/0\",\n      \"mtu\": 1500,\n      \"why\": \"default\"\n    }\n  ],\n  \"offloads\": \"auto\",\n  \"rx_timestamps\": false,\n  \"descriptors\": 1024,\n  \"eal\": {\n    \"file_prefix\": \"zhida\",\n    \"extra\": []\n  }\n}\n";

    fn example() -> Value {
        serde_json::from_str(EXPECTED).unwrap()
    }

    fn reject(value: &Value, message: &str) -> ConfigError {
        let error = Config::from_json(&value.to_string()).unwrap_err();
        assert!(error.0.contains(message), "{error}");
        if !message.contains(":") {
            assert!(
                error.0.contains("line ") && error.0.contains("column "),
                "{error}"
            );
        } else {
            assert!(error.0.starts_with(message), "{error}");
        }
        error
    }

    #[test]
    fn config_path_mtu_matches_stack_lookup() {
        let mut value = example();
        value["paths"] = json!([
            {"prefix": "10.0.0.0/16", "mtu": 9001, "why": "vpc"},
            {"prefix": "0.0.0.0/0", "mtu": 1500, "why": "internet"},
            {"prefix": "10.0.1.99/32", "mtu": 1500, "why": "jumbo-less host"},
        ]);
        let config = Config::from_json(&value.to_string()).unwrap();
        let rules: Vec<PathMtu> = config.paths.iter()
            .map(|rule| PathMtu { prefix: rule.prefix, mtu: rule.mtu })
            .collect();
        let paths = PathMtus::new(&rules, config.ipv4, config.mtu).unwrap();
        for (addr, expected) in [("10.0.1.5", 9001), ("10.0.1.99", 1500),
            ("10.0.7.7", 9001), ("1.1.1.1", 1500)]
        {
            let addr = addr.parse().unwrap();
            assert_eq!(config.path_mtu(addr), expected);
            assert_eq!(config.path_mtu(addr), paths.ip_mtu(addr));
        }
    }

    #[test]
    fn config_round_trips_pci_and_vdev() {
        let pci = Config::from_json(EXPECTED).unwrap();
        assert_eq!(pci.to_json(), EXPECTED);
        assert_eq!(Config::from_json(&pci.to_json()).unwrap(), pci);
        let mut value = example();
        value["device"] = json!({"vdev": "net_tap0,iface=cdnic0"});
        value["mac"] = Value::Null;
        value["gateway"] = Value::Null;
        value["eal"]["extra"] = json!(["--no-huge", "-m", "128", "--no-shconf"]);
        value["offloads"] =
            json!({"rx_checksum": true, "tx_tcp_checksum": false, "tx_udp_checksum": true});
        let vdev = Config::from_json(&value.to_string()).unwrap();
        assert_eq!(Config::from_json(&vdev.to_json()).unwrap(), vdev);
        assert!(vdev.to_string().contains("mac: device\n"));
        assert!(vdev.to_string().contains(
            "offloads: {\"rx_checksum\":true,\"tx_tcp_checksum\":false,\"tx_udp_checksum\":true}\n"
        ));
    }

    #[test]
    fn config_rejects_unknown_field_and_version() {
        for (pointer, path) in [
            ("", "unknown field `foo`"),
            ("/device", "did not match any variant"),
            ("/paths/0", "unknown field `foo`"),
            ("/offloads", "unknown field `foo`"),
            ("/eal", "unknown field `foo`"),
        ] {
            let mut value = example();
            value["offloads"] =
                json!({"rx_checksum": true, "tx_tcp_checksum": false, "tx_udp_checksum": true});
            value
                .pointer_mut(pointer)
                .unwrap()
                .as_object_mut()
                .unwrap()
                .insert("foo".into(), json!(true));
            reject(&value, path);
        }
        let mut value = example();
        value["version"] = json!(2);
        reject(&value, "version:");
        let mut value = example();
        value.as_object_mut().unwrap().remove("rx_timestamps");
        reject(&value, "missing field `rx_timestamps`");
        for key in ["mac", "gateway"] {
            let mut value = example();
            value.as_object_mut().unwrap().remove(key);
            reject(&value, &format!("missing field `{key}`"));
        }
        let error = Config::from_json("{\n").unwrap_err();
        assert!(error.0.contains("line 2 column 0"));
        for (pointer, replacement, path) in [
            ("/rx_timestamps", json!(0), "expected a boolean"),
            ("/mtu", json!(65536), "expected u16"),
            (
                "/device/numa_node",
                json!(2147483648_i64),
                "did not match any variant",
            ),
            ("/eal/extra", json!([false]), "expected a string"),
            ("/eal/file_prefix", json!(""), "eal.file_prefix:"),
            ("/paths/0/why", json!(""), "paths[0].why:"),
        ] {
            let mut value = example();
            *value.pointer_mut(pointer).unwrap() = replacement;
            reject(&value, path);
        }
    }

    #[test]
    fn config_rejects_gateway_outside_subnet() {
        for (gateway, message) in [
            ("10.0.2.1", "gateway must be inside the IPv4 subnet"),
            ("10.0.1.23", "gateway must not be our IPv4 address"),
            (
                "10.0.1.0",
                "gateway must not be the subnet's network address",
            ),
            (
                "10.0.1.255",
                "gateway must not be the subnet's broadcast address",
            ),
            ("224.0.0.1", "gateway must be unicast"),
        ] {
            let mut value = example();
            value["gateway"] = json!(gateway);
            assert!(reject(&value, "gateway:").0.contains(message));
        }
        let mut value = example();
        value["ipv4"] = json!("10.0.0.0/31");
        value["gateway"] = json!("10.0.0.1");
        Config::from_json(&value.to_string()).unwrap();
        value["ipv4"] = json!("10.0.0.1/31");
        value["gateway"] = json!("10.0.0.0");
        Config::from_json(&value.to_string()).unwrap();
        value["ipv4"] = json!("10.0.0.0/32");
        value["gateway"] = json!("10.0.0.0");
        assert_eq!(
            reject(&value, "gateway:").0,
            "gateway: gateway must not be our IPv4 address"
        );
        value["gateway"] = json!("10.0.0.1");
        reject(&value, "gateway:");
        value["gateway"] = Value::Null;
        Config::from_json(&value.to_string()).unwrap();
    }

    #[test]
    fn config_rejects_bad_mac_pci_id_path_and_offloads() {
        let mut value = example();
        value["mtu"] = json!(87);
        assert!(serde_json::from_value::<Config>(value).is_err());
        for (pointer, replacement, path) in [
            ("/mac", json!("bad"), "mac:"),
            ("/mac", json!("01:12:34:56:78:9a"), "mac:"),
            ("/mac", Value::Null, "mac:"),
            ("/device/pci", json!("0000:00:06.8"), "device.pci:"),
            ("/device/pci_id", json!("1D0F:ec20"), "device.pci_id:"),
            ("/device/pci_id", json!("1d0:ec20"), "device.pci_id:"),
            ("/paths/0/prefix", json!("10.0.1.0/16"), "paths[0].prefix:"),
            ("/paths/0/mtu", json!(87), "paths[0].mtu:"),
            ("/offloads", json!("on"), "offloads:"),
            (
                "/offloads",
                json!({"rx_checksum": true, "tx_tcp_checksum": false}),
                "missing field `tx_udp_checksum`",
            ),
            (
                "/offloads",
                json!({"rx_checksum": 1, "tx_tcp_checksum": false, "tx_udp_checksum": false}),
                "expected a boolean",
            ),
            ("/mtu", json!(87), "mtu:"),
            ("/descriptors", json!(0), "descriptors:"),
            ("/ipv4", json!("224.0.0.1/24"), "ipv4:"),
            ("/device", json!({"vdev": ""}), "device.vdev:"),
            ("/device/kernel_driver", json!(""), "device.kernel_driver:"),
            (
                "/device/kernel_interface",
                json!(""),
                "device.kernel_interface:",
            ),
        ] {
            let mut value = example();
            *value.pointer_mut(pointer).unwrap() = replacement;
            reject(&value, path);
        }
        let mut value = example();
        value["paths"][1]["prefix"] = value["paths"][0]["prefix"].clone();
        assert!(reject(&value, "paths[1].prefix:").0.contains("paths[0]"));
    }

    #[test]
    fn config_rejects_port_and_path_mtu_87_accepts_88() {
        for pointer in ["/mtu", "/paths/0/mtu"] {
            let mut value = example();
            *value.pointer_mut(pointer).unwrap() = json!(87);
            let field = &pointer[1..].replace("/0/", "[0].");
            reject(&value, &format!("{field}: {MTU_TOO_SMALL}"));
            *value.pointer_mut(pointer).unwrap() = json!(88);
            Config::from_json(&value.to_string()).unwrap();
        }
    }

    fn cidrs(texts: &[&str]) -> Vec<Ipv4Cidr> {
        texts.iter().map(|t| t.parse().unwrap()).collect()
    }

    /// A config without `owned_ipv4`, or with an empty list, owns nothing besides `ipv4` and
    /// writes the same JSON as before the field existed; `null` is not a list.
    #[test]
    fn owned_ipv4_absent_or_empty_keeps_the_single_address_json() {
        let config = Config::from_json(EXPECTED).unwrap();
        assert!(config.owned_ipv4.is_empty());
        assert_eq!(config.to_json(), EXPECTED);
        assert!(config.to_string().contains("owned_ipv4: none\n"));
        let mut value = example();
        value["owned_ipv4"] = json!([]);
        assert_eq!(
            Config::from_json(&value.to_string()).unwrap().to_json(),
            EXPECTED
        );
        value["owned_ipv4"] = Value::Null;
        reject(&value, "invalid type: null, expected a sequence");
    }

    /// Entries load sorted, with duplicates and entries inside another dropped; adjacent and
    /// off-subnet entries stay. The result round-trips and is shown.
    #[test]
    fn owned_ipv4_round_trips_sorted_and_merged() {
        let mut value = example();
        value["owned_ipv4"] = json!([
            "10.0.2.5/32",
            "10.0.2.0/29",
            "10.0.2.5/32",
            "10.0.2.8/29",
            "10.0.3.128/25",
            "10.0.3.0/24",
            "10.0.1.24/32",
            "172.16.0.0/16",
        ]);
        let config = Config::from_json(&value.to_string()).unwrap();
        let merged = cidrs(&[
            "10.0.1.24/32",
            "10.0.2.0/29",
            "10.0.2.8/29",
            "10.0.3.0/24",
            "172.16.0.0/16",
        ]);
        assert_eq!(config.owned_ipv4, merged);
        let json = config.to_json();
        assert!(json.contains(
            "  \"gateway\": \"10.0.1.1\",\n  \"owned_ipv4\": [\n    \"10.0.1.24/32\",\n    \"10.0.2.0/29\","
        ), "{json}");
        assert_eq!(Config::from_json(&json).unwrap(), config);
        assert!(config.to_string().contains(
            "owned_ipv4: 10.0.1.24/32, 10.0.2.0/29, 10.0.2.8/29, 10.0.3.0/24, 172.16.0.0/16\n"
        ));
    }

    /// Every address an entry covers must be unicast, not the gateway and not the subnet's
    /// broadcast address; the error names the entry as written.
    #[test]
    fn owned_ipv4_rejects_bad_entries_anywhere_in_their_range() {
        for (entry, message) in [
            ("10.0.1.5/24", "prefix must not have host bits set"),
            ("10.0.1.5", "invalid IPv4 CIDR"),
            ("0.0.0.0/32", "owned IPv4 addresses must be unicast"),
            ("0.0.0.0/8", "owned IPv4 addresses must be unicast"),
            ("224.0.0.1/32", "owned IPv4 addresses must be unicast"),
            ("239.255.255.255/32", "owned IPv4 addresses must be unicast"),
            ("192.0.0.0/2", "owned IPv4 addresses must be unicast"),
            ("255.255.255.255/32", "owned IPv4 addresses must be unicast"),
            (
                "10.0.1.1/32",
                "owned IPv4 addresses must not include the gateway",
            ),
            (
                "10.0.1.0/28",
                "owned IPv4 addresses must not include the gateway",
            ),
            (
                "10.0.1.255/32",
                "owned IPv4 addresses must not include the subnet's broadcast address",
            ),
            (
                "10.0.1.240/28",
                "owned IPv4 addresses must not include the subnet's broadcast address",
            ),
        ] {
            let mut value = example();
            value["owned_ipv4"] = json!(["10.0.1.30/32", entry]);
            assert_eq!(
                reject(&value, "owned_ipv4[1]:").0,
                format!("owned_ipv4[1]: {message}"),
                "{entry}"
            );
        }
        let mut value = example();
        value["owned_ipv4"] = json!([1]);
        reject(&value, "expected a string");
        // next to the forbidden addresses is fine
        value["owned_ipv4"] = json!([
            "10.0.1.2/32",
            "10.0.1.254/32",
            "223.255.255.255/32",
            "240.0.0.0/32"
        ]);
        Config::from_json(&value.to_string()).unwrap();
        // with no gateway, its address is ownable; a /32 subnet has no broadcast
        value["gateway"] = Value::Null;
        value["ipv4"] = json!("10.0.1.23/32");
        value["owned_ipv4"] = json!(["10.0.1.1/32", "10.0.1.255/32"]);
        Config::from_json(&value.to_string()).unwrap();
    }
}
