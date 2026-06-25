#[derive(Debug, Clone, Copy, PartialEq)]
pub struct HetznerServerType {
    pub name: &'static str,
    pub vcpus: u16,
    pub memory_gb: u16,
    pub local_disk_gb: u32,
    pub hourly_eur: f64,
}

const HETZNER_SERVER_TYPES: &[HetznerServerType] = &[
    HetznerServerType {
        name: "ccx33",
        vcpus: 8,
        memory_gb: 32,
        local_disk_gb: 240,
        hourly_eur: 0.1001,
    },
    HetznerServerType {
        name: "ccx43",
        vcpus: 16,
        memory_gb: 64,
        local_disk_gb: 360,
        hourly_eur: 0.2003,
    },
    HetznerServerType {
        name: "ccx53",
        vcpus: 32,
        memory_gb: 128,
        local_disk_gb: 600,
        hourly_eur: 0.4006,
    },
    HetznerServerType {
        name: "ccx63",
        vcpus: 48,
        memory_gb: 192,
        local_disk_gb: 960,
        hourly_eur: 0.6001,
    },
];

pub fn hetzner_server_types() -> &'static [HetznerServerType] {
    HETZNER_SERVER_TYPES
}

pub fn hetzner_server_type(name: &str) -> Option<HetznerServerType> {
    HETZNER_SERVER_TYPES
        .iter()
        .copied()
        .find(|server_type| server_type.name.eq_ignore_ascii_case(name))
}
