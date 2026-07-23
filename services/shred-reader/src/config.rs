use std::{
    collections::HashSet,
    env,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::PathBuf,
    time::Duration,
};

use anyhow::{Context, Result, bail};

const DEFAULT_ENTRYPOINTS: &str = "entrypoint.mainnet-beta.solana.com:8001,entrypoint2.mainnet-beta.solana.com:8001,entrypoint3.mainnet-beta.solana.com:8001";
const MAX_FORWARD_QUEUE_CAPACITY: usize = 131_072;

#[derive(Debug, Clone)]
pub struct Config {
    pub environment: String,
    pub entrypoints: Vec<String>,
    pub identity_path: PathBuf,
    pub bind_ip: IpAddr,
    pub advertised_ip: Option<Ipv4Addr>,
    pub gossip_port: u16,
    pub tvu_port: u16,
    pub metrics_addr: SocketAddr,
    pub shred_version: Option<u16>,
    pub require_reachability: bool,
    pub udp_recv_buffer_bytes: usize,
    pub dedup_capacity: usize,
    pub metrics_interval: Duration,
    pub sample_interval: Duration,
    pub loss_telemetry_interfaces: Vec<String>,
    pub shred_forward_addrs: Vec<SocketAddr>,
    pub forward_queue_capacity: usize,
    pub repair_enabled: bool,
    pub repair_rpc_url: String,
    pub repair_wal_path: PathBuf,
    pub repair_wal_max_bytes: u64,
    pub repair_max_peers: usize,
    pub repair_observation_queue_capacity: usize,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        let config = Self {
            environment: env_string("APP_ENV", "development"),
            entrypoints: parse_entrypoints(&env_string("SOLANA_ENTRYPOINTS", DEFAULT_ENTRYPOINTS))?,
            identity_path: PathBuf::from(env_string("IDENTITY_PATH", "./data/identity.json")),
            bind_ip: env_parse("BIND_IP", "0.0.0.0")?,
            advertised_ip: env_optional_parse("ADVERTISED_IP")?,
            gossip_port: env_parse("GOSSIP_PORT", "18001")?,
            tvu_port: env_parse("TVU_PORT", "18002")?,
            metrics_addr: env_parse("METRICS_ADDR", "127.0.0.1:19090")?,
            shred_version: env_optional_parse("SHRED_VERSION")?,
            require_reachability: env_parse("REQUIRE_REACHABILITY", "false")?,
            udp_recv_buffer_bytes: env_parse("UDP_RECV_BUFFER_BYTES", "67108864")?,
            dedup_capacity: env_parse("DEDUP_CAPACITY", "500000")?,
            metrics_interval: Duration::from_secs(env_parse("METRICS_INTERVAL_SECS", "10")?),
            sample_interval: Duration::from_secs(env_parse("SAMPLE_INTERVAL_SECS", "15")?),
            loss_telemetry_interfaces: parse_names(&env_string("LOSS_TELEMETRY_INTERFACES", "")),
            shred_forward_addrs: parse_socket_addrs(&env_string("SHRED_FORWARD_ADDRS", ""))?,
            forward_queue_capacity: env_parse("FORWARD_QUEUE_CAPACITY", "16384")?,
            repair_enabled: env_parse("REPAIR_ENABLED", "false")?,
            repair_rpc_url: env_string("REPAIR_RPC_URL", "https://api.mainnet-beta.solana.com"),
            repair_wal_path: PathBuf::from(env_string(
                "REPAIR_WAL_PATH",
                "./data/accepted.repair.wal",
            )),
            repair_wal_max_bytes: env_parse("REPAIR_WAL_MAX_BYTES", "268435456")?,
            repair_max_peers: env_parse("REPAIR_MAX_PEERS", "8")?,
            repair_observation_queue_capacity: env_parse(
                "REPAIR_OBSERVATION_QUEUE_CAPACITY",
                "32768",
            )?,
        };
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<()> {
        if self.entrypoints.is_empty() {
            bail!("SOLANA_ENTRYPOINTS must contain at least one host:port");
        }
        if self.gossip_port == self.tvu_port {
            bail!("GOSSIP_PORT and TVU_PORT must be different");
        }
        if self.gossip_port == 0 || self.tvu_port == 0 {
            bail!("GOSSIP_PORT and TVU_PORT must be nonzero");
        }
        if !self.bind_ip.is_ipv4() {
            bail!("BIND_IP must be IPv4");
        }
        if self.udp_recv_buffer_bytes < 2_048 {
            bail!("UDP_RECV_BUFFER_BYTES must be at least 2048");
        }
        if self.dedup_capacity == 0 {
            bail!("DEDUP_CAPACITY must be greater than zero");
        }
        if self.metrics_interval.is_zero() || self.sample_interval.is_zero() {
            bail!("metrics and sample intervals must be greater than zero");
        }
        if self.shred_forward_addrs.len() > 32 {
            bail!("SHRED_FORWARD_ADDRS supports at most 32 destinations");
        }
        if self.environment.eq_ignore_ascii_case("production")
            && self.shred_forward_addrs.is_empty()
        {
            bail!("SHRED_FORWARD_ADDRS must contain at least one destination in production");
        }
        if self.forward_queue_capacity == 0
            || self.forward_queue_capacity > MAX_FORWARD_QUEUE_CAPACITY
        {
            bail!("FORWARD_QUEUE_CAPACITY must be between 1 and {MAX_FORWARD_QUEUE_CAPACITY}");
        }
        if self.repair_enabled {
            if !(1..=32).contains(&self.repair_max_peers) {
                bail!("REPAIR_MAX_PEERS must be between 1 and 32");
            }
            if self.repair_observation_queue_capacity == 0
                || self.repair_observation_queue_capacity > MAX_FORWARD_QUEUE_CAPACITY
            {
                bail!(
                    "REPAIR_OBSERVATION_QUEUE_CAPACITY must be between 1 and {MAX_FORWARD_QUEUE_CAPACITY}"
                );
            }
            if !self
                .repair_wal_path
                .to_string_lossy()
                .ends_with(".repair.wal")
            {
                bail!("REPAIR_WAL_PATH must end in .repair.wal");
            }
            if self.repair_wal_max_bytes < 65_536 {
                bail!("REPAIR_WAL_MAX_BYTES must be at least 65536");
            }
            if !(self.repair_rpc_url.starts_with("https://")
                || self.repair_rpc_url.starts_with("http://"))
            {
                bail!("REPAIR_RPC_URL must use http:// or https://");
            }
        }
        if self
            .shred_forward_addrs
            .iter()
            .any(|address| !address.is_ipv4() || address.port() == 0)
        {
            bail!("SHRED_FORWARD_ADDRS destinations must be IPv4 with a nonzero port");
        }
        if let Some(ip) = self.advertised_ip
            && !is_public_ipv4(ip)
        {
            bail!("ADVERTISED_IP must be a public IPv4 address");
        }
        Ok(())
    }
}

fn parse_socket_addrs(value: &str) -> Result<Vec<SocketAddr>> {
    let mut seen = HashSet::new();
    let mut addresses = Vec::new();
    for value in value
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        let address = value.parse::<SocketAddr>().map_err(|error| {
            anyhow::anyhow!("invalid SHRED_FORWARD_ADDRS destination {value:?}: {error}")
        })?;
        if seen.insert(address) {
            addresses.push(address);
        }
    }
    Ok(addresses)
}

fn env_string(name: &str, default: &str) -> String {
    env::var(name).unwrap_or_else(|_| default.to_owned())
}

fn env_parse<T>(name: &str, default: &str) -> Result<T>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    env_string(name, default)
        .parse()
        .map_err(|error| anyhow::anyhow!("invalid {name}: {error}"))
}

fn env_optional_parse<T>(name: &str) -> Result<Option<T>>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .map(|value| {
            value
                .parse()
                .map_err(|error| anyhow::anyhow!("invalid {name}: {error}"))
        })
        .transpose()
}

fn parse_entrypoints(value: &str) -> Result<Vec<String>> {
    let entrypoints = value
        .split(',')
        .map(str::trim)
        .filter(|entrypoint| !entrypoint.is_empty())
        .map(str::to_owned)
        .collect::<Vec<_>>();

    if entrypoints.is_empty() {
        bail!("SOLANA_ENTRYPOINTS must contain at least one host:port");
    }
    for entrypoint in &entrypoints {
        if !entrypoint.contains(':') {
            bail!("entrypoint {entrypoint:?} is missing a port");
        }
    }
    Ok(entrypoints)
}

fn is_public_ipv4(ip: Ipv4Addr) -> bool {
    !(ip.is_private()
        || ip.is_loopback()
        || ip.is_link_local()
        || ip.is_multicast()
        || ip.is_broadcast()
        || ip.is_documentation()
        || ip.is_unspecified())
}

pub fn public_ipv4(ip: IpAddr) -> Result<Ipv4Addr> {
    let IpAddr::V4(ip) = ip else {
        bail!("the Solana entrypoint returned an IPv6 address; this prototype requires IPv4");
    };
    if !is_public_ipv4(ip) {
        bail!("discovered address {ip} is not a public IPv4 address");
    }
    Ok(ip)
}

pub fn nonzero_usize(value: usize, name: &str) -> Result<std::num::NonZeroUsize> {
    std::num::NonZeroUsize::new(value).with_context(|| format!("{name} must be greater than zero"))
}

fn parse_names(value: &str) -> Vec<String> {
    let mut seen = HashSet::new();
    value
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty() && seen.insert(*value))
        .map(str::to_owned)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_config() -> Config {
        Config {
            environment: "test".to_owned(),
            entrypoints: vec!["127.0.0.1:8001".to_owned()],
            identity_path: PathBuf::from("identity.json"),
            bind_ip: "0.0.0.0".parse().unwrap(),
            advertised_ip: None,
            gossip_port: 18_001,
            tvu_port: 18_002,
            metrics_addr: "127.0.0.1:19090".parse().unwrap(),
            shred_version: None,
            require_reachability: false,
            udp_recv_buffer_bytes: 65_536,
            dedup_capacity: 1_024,
            metrics_interval: Duration::from_secs(10),
            sample_interval: Duration::from_secs(15),
            loss_telemetry_interfaces: Vec::new(),
            shred_forward_addrs: Vec::new(),
            forward_queue_capacity: 1_024,
            repair_enabled: false,
            repair_rpc_url: "http://127.0.0.1:8899".to_owned(),
            repair_wal_path: PathBuf::from("accepted.repair.wal"),
            repair_wal_max_bytes: 65_536,
            repair_max_peers: 3,
            repair_observation_queue_capacity: 1_024,
        }
    }

    #[test]
    fn entrypoint_list_is_trimmed() {
        assert_eq!(
            parse_entrypoints("one.example:8001, two.example:9001").unwrap(),
            ["one.example:8001", "two.example:9001"]
        );
    }

    #[test]
    fn rejects_entrypoint_without_port() {
        assert!(parse_entrypoints("entrypoint.example").is_err());
    }

    #[test]
    fn rejects_private_advertised_address() {
        assert!(!is_public_ipv4(Ipv4Addr::new(10, 0, 0, 1)));
        assert!(is_public_ipv4(Ipv4Addr::new(1, 1, 1, 1)));
    }

    #[test]
    fn rejects_ipv6_bind_address() {
        let mut config = valid_config();
        config.bind_ip = "::".parse().unwrap();
        assert!(config.validate().unwrap_err().to_string().contains("IPv4"));
    }

    #[test]
    fn rejects_ephemeral_network_ports() {
        let mut config = valid_config();
        config.gossip_port = 0;
        assert!(
            config
                .validate()
                .unwrap_err()
                .to_string()
                .contains("nonzero")
        );
    }

    #[test]
    fn parses_and_deduplicates_forward_destinations() {
        assert_eq!(
            parse_socket_addrs("127.0.0.1:18003, 127.0.0.1:18003,192.0.2.1:9000").unwrap(),
            [
                "127.0.0.1:18003".parse().unwrap(),
                "192.0.2.1:9000".parse().unwrap(),
            ]
        );
    }

    #[test]
    fn production_requires_a_forward_destination() {
        let mut config = valid_config();
        config.environment = "production".to_owned();

        assert!(
            config
                .validate()
                .unwrap_err()
                .to_string()
                .contains("at least one destination")
        );
    }

    #[test]
    fn rejects_empty_forward_queue() {
        let mut config = valid_config();
        config.forward_queue_capacity = 0;

        assert!(
            config
                .validate()
                .unwrap_err()
                .to_string()
                .contains("FORWARD_QUEUE_CAPACITY")
        );
    }

    #[test]
    fn rejects_excessive_forward_queue() {
        let mut config = valid_config();
        config.forward_queue_capacity = MAX_FORWARD_QUEUE_CAPACITY + 1;

        assert!(
            config
                .validate()
                .unwrap_err()
                .to_string()
                .contains("FORWARD_QUEUE_CAPACITY")
        );
    }

    #[test]
    fn rejects_invalid_forward_destination() {
        assert!(parse_socket_addrs("not-an-address").is_err());
    }
}
