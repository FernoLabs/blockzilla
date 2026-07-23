//! Best-effort Linux receive-loss telemetry.
//!
//! The counters exposed here are host-wide unless they live under an explicitly selected
//! network interface.  In particular, `/proc/net/snmp` and `/proc/net/softnet_stat` cannot be
//! attributed to one socket.  They are still useful alongside the reader's own packet counters:
//! a non-zero delta tells us that loss happened in the host network stack rather than farther
//! upstream in Turbine.
//!
//! Collection intentionally has no fatal path.  Missing files, permission errors, kernels that
//! omit a field, counter resets, and non-Linux hosts are represented by `None`.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
};

/// Host-wide UDP counters from the `Udp` section of `/proc/net/snmp`.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct UdpLossCounters {
    /// Datagrams rejected by UDP for any input error.
    pub in_errors: Option<u64>,
    /// Datagrams dropped because no socket was listening on the destination port.
    pub no_ports: Option<u64>,
    /// Datagrams dropped because a socket receive buffer was full.
    pub receive_buffer_errors: Option<u64>,
    /// Datagrams rejected because their UDP checksum was invalid.
    pub checksum_errors: Option<u64>,
}

/// Host-wide, per-CPU networking backlog counters, summed over all CPUs.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SoftnetLossCounters {
    /// Packets dropped before protocol processing because the CPU backlog was full.
    pub dropped: Option<u64>,
    /// Times packet processing exhausted its budget or time slice.
    pub time_squeeze: Option<u64>,
}

/// Receive counters for one explicitly selected network interface.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct NicLossCounters {
    pub rx_dropped: Option<u64>,
    pub rx_missed_errors: Option<u64>,
    pub rx_errors: Option<u64>,
}

/// A point-in-time view of all available receive-loss counters.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct LossSnapshot {
    pub udp: UdpLossCounters,
    pub softnet: SoftnetLossCounters,
    /// Keys are the selected interface names, including interfaces whose counters are unavailable.
    pub interfaces: BTreeMap<String, NicLossCounters>,
}

/// Counter increases between two snapshots.
///
/// A field is `None` when either sample was unavailable or the counter decreased (for example,
/// after a reboot or interface reset).  This prevents a reset from being reported as packet loss.
pub type LossDelta = LossSnapshot;

impl LossSnapshot {
    pub fn delta_since(&self, previous: &Self) -> LossDelta {
        let interfaces = self
            .interfaces
            .iter()
            .map(|(name, current)| {
                let delta = previous
                    .interfaces
                    .get(name)
                    .map(|before| current.delta_since(before))
                    .unwrap_or_default();
                (name.clone(), delta)
            })
            .collect();

        Self {
            udp: self.udp.delta_since(&previous.udp),
            softnet: self.softnet.delta_since(&previous.softnet),
            interfaces,
        }
    }
}

impl UdpLossCounters {
    fn delta_since(&self, previous: &Self) -> Self {
        Self {
            in_errors: counter_delta(self.in_errors, previous.in_errors),
            no_ports: counter_delta(self.no_ports, previous.no_ports),
            receive_buffer_errors: counter_delta(
                self.receive_buffer_errors,
                previous.receive_buffer_errors,
            ),
            checksum_errors: counter_delta(self.checksum_errors, previous.checksum_errors),
        }
    }
}

impl SoftnetLossCounters {
    fn delta_since(&self, previous: &Self) -> Self {
        Self {
            dropped: counter_delta(self.dropped, previous.dropped),
            time_squeeze: counter_delta(self.time_squeeze, previous.time_squeeze),
        }
    }
}

impl NicLossCounters {
    fn delta_since(&self, previous: &Self) -> Self {
        Self {
            rx_dropped: counter_delta(self.rx_dropped, previous.rx_dropped),
            rx_missed_errors: counter_delta(self.rx_missed_errors, previous.rx_missed_errors),
            rx_errors: counter_delta(self.rx_errors, previous.rx_errors),
        }
    }
}

fn counter_delta(current: Option<u64>, previous: Option<u64>) -> Option<u64> {
    current?.checked_sub(previous?)
}

/// Reusable best-effort collector.
#[derive(Clone, Debug)]
pub struct LossTelemetry {
    snmp_path: PathBuf,
    softnet_path: PathBuf,
    net_class_path: PathBuf,
    interfaces: Vec<String>,
}

impl LossTelemetry {
    /// Builds a collector for the host's standard Linux telemetry paths.
    pub fn system<I, S>(interfaces: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self::with_paths(
            "/proc/net/snmp",
            "/proc/net/softnet_stat",
            "/sys/class/net",
            interfaces,
        )
    }

    /// Builds a collector with explicit paths.  This is useful in containers whose procfs or
    /// sysfs is mounted somewhere other than the conventional host paths.
    pub fn with_paths<I, S>(
        snmp_path: impl Into<PathBuf>,
        softnet_path: impl Into<PathBuf>,
        net_class_path: impl Into<PathBuf>,
        interfaces: I,
    ) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let interfaces = interfaces
            .into_iter()
            .map(Into::into)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        Self {
            snmp_path: snmp_path.into(),
            softnet_path: softnet_path.into(),
            net_class_path: net_class_path.into(),
            interfaces,
        }
    }

    /// Takes a snapshot without failing the caller if any source is missing or malformed.
    pub fn snapshot(&self) -> LossSnapshot {
        let udp = fs::read_to_string(&self.snmp_path)
            .ok()
            .map(|text| parse_udp_snmp(&text))
            .unwrap_or_default();
        let softnet = fs::read_to_string(&self.softnet_path)
            .ok()
            .map(|text| parse_softnet_stat(&text))
            .unwrap_or_default();
        let interfaces = self
            .interfaces
            .iter()
            .map(|name| (name.clone(), read_nic_counters(&self.net_class_path, name)))
            .collect();

        LossSnapshot {
            udp,
            softnet,
            interfaces,
        }
    }
}

fn parse_udp_snmp(input: &str) -> UdpLossCounters {
    let mut udp = UdpLossCounters::default();
    let mut header: Option<Vec<&str>> = None;

    for line in input.lines() {
        let Some((protocol, fields)) = line.split_once(':') else {
            continue;
        };
        if protocol.trim() != "Udp" {
            continue;
        }
        let fields = fields.split_whitespace().collect::<Vec<_>>();
        let Some(names) = header.take() else {
            if fields.iter().any(|field| field.parse::<u64>().is_err()) {
                header = Some(fields);
            }
            continue;
        };
        for (name, value) in names.into_iter().zip(fields) {
            let value = value.parse::<u64>().ok();
            match name {
                "InErrors" => udp.in_errors = value,
                "NoPorts" => udp.no_ports = value,
                "RcvbufErrors" => udp.receive_buffer_errors = value,
                "InCsumErrors" => udp.checksum_errors = value,
                _ => {}
            }
        }
    }

    udp
}

fn parse_softnet_stat(input: &str) -> SoftnetLossCounters {
    let mut dropped = Some(0_u64);
    let mut time_squeeze = Some(0_u64);
    let mut valid_dropped = false;
    let mut valid_time_squeeze = false;

    for line in input.lines() {
        let fields = line.split_whitespace().collect::<Vec<_>>();
        if let Some(value) = fields.get(1).and_then(|value| parse_hex_u64(value)) {
            valid_dropped = true;
            dropped = dropped.and_then(|total| total.checked_add(value));
        }
        if let Some(value) = fields.get(2).and_then(|value| parse_hex_u64(value)) {
            valid_time_squeeze = true;
            time_squeeze = time_squeeze.and_then(|total| total.checked_add(value));
        }
    }

    SoftnetLossCounters {
        dropped: valid_dropped.then_some(dropped).flatten(),
        time_squeeze: valid_time_squeeze.then_some(time_squeeze).flatten(),
    }
}

fn parse_hex_u64(value: &str) -> Option<u64> {
    u64::from_str_radix(value, 16).ok()
}

fn read_nic_counters(net_class_path: &Path, interface: &str) -> NicLossCounters {
    if !safe_interface_name(interface) {
        return NicLossCounters::default();
    }
    let statistics = net_class_path.join(interface).join("statistics");
    NicLossCounters {
        rx_dropped: read_decimal_counter(statistics.join("rx_dropped")),
        rx_missed_errors: read_decimal_counter(statistics.join("rx_missed_errors")),
        rx_errors: read_decimal_counter(statistics.join("rx_errors")),
    }
}

fn safe_interface_name(interface: &str) -> bool {
    !interface.is_empty()
        && interface != "."
        && interface != ".."
        && !interface.contains('/')
        && !interface.contains('\\')
        && !interface.contains('\0')
}

fn read_decimal_counter(path: impl AsRef<Path>) -> Option<u64> {
    fs::read_to_string(path).ok()?.trim().parse().ok()
}

/// Enables Linux's `SO_RXQ_OVFL` ancillary message for a socket.
///
/// Once enabled, `recvmsg(2)` can return a `SOL_SOCKET`/`SO_RXQ_OVFL` control message containing
/// a native-endian `u32` cumulative count of packets dropped by that socket since creation.  A
/// normal `recv_from` call does not expose ancillary messages, so callers must use a recvmsg-based
/// receive path before this becomes observable.
#[cfg(target_os = "linux")]
pub fn enable_socket_rxq_overflow<T: std::os::fd::AsRawFd>(socket: &T) -> std::io::Result<()> {
    use std::{ffi::c_void, io, mem::size_of};

    const SOL_SOCKET: std::ffi::c_int = 1;
    const SO_RXQ_OVFL: std::ffi::c_int = 40;
    let enabled: std::ffi::c_int = 1;
    // SAFETY: the socket owns a valid file descriptor for the duration of this call, and the
    // option pointer refers to an initialized integer of the size supplied to libc.
    let result = unsafe {
        setsockopt(
            socket.as_raw_fd(),
            SOL_SOCKET,
            SO_RXQ_OVFL,
            (&enabled as *const std::ffi::c_int).cast::<c_void>(),
            size_of::<std::ffi::c_int>() as u32,
        )
    };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(target_os = "linux")]
unsafe extern "C" {
    fn setsockopt(
        socket: std::ffi::c_int,
        level: std::ffi::c_int,
        option_name: std::ffi::c_int,
        option_value: *const std::ffi::c_void,
        option_len: u32,
    ) -> std::ffi::c_int;
}

/// Computes the increase between cumulative `SO_RXQ_OVFL` values, including `u32` wraparound.
#[cfg(test)]
pub fn socket_rxq_overflow_delta(current: u32, previous: u32) -> u64 {
    current.wrapping_sub(previous) as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{fs, time::SystemTime};

    #[cfg(target_os = "linux")]
    use std::net::UdpSocket;

    struct TestDir(PathBuf);

    impl TestDir {
        fn new() -> Self {
            let unique = format!(
                "shred-reader-loss-telemetry-{}-{}",
                std::process::id(),
                SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            );
            let path = std::env::temp_dir().join(unique);
            fs::create_dir(&path).unwrap();
            Self(path)
        }

        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn parses_udp_fields_by_name() {
        let input = "Ip: Forwarding DefaultTTL\n\
                     Ip: 1 64\n\
                     Udp: InDatagrams NoPorts InErrors OutDatagrams RcvbufErrors SndbufErrors InCsumErrors IgnoredMulti MemErrors\n\
                     Udp: 1000 7 11 900 5 2 3 0 0\n";
        assert_eq!(
            parse_udp_snmp(input),
            UdpLossCounters {
                in_errors: Some(11),
                no_ports: Some(7),
                receive_buffer_errors: Some(5),
                checksum_errors: Some(3),
            }
        );
    }

    #[test]
    fn malformed_or_missing_udp_fields_are_unavailable() {
        let input = "Udp: InDatagrams NoPorts InErrors RcvbufErrors\n\
                     Udp: 10 not-a-number 2\n";
        assert_eq!(
            parse_udp_snmp(input),
            UdpLossCounters {
                in_errors: Some(2),
                no_ports: None,
                receive_buffer_errors: None,
                checksum_errors: None,
            }
        );
        assert_eq!(
            parse_udp_snmp("UdpLite: InErrors\nUdpLite: 2\n"),
            UdpLossCounters::default()
        );
    }

    #[test]
    fn sums_softnet_cpu_lines() {
        let input = "0000000a 00000002 00000003 0 0 0 0 0 0 0 0 0 0 0 0\n\
                     00000014 00000004 00000005 0 0 0 0 0 0 0 0 0 0 0 0\n";
        assert_eq!(
            parse_softnet_stat(input),
            SoftnetLossCounters {
                dropped: Some(6),
                time_squeeze: Some(8),
            }
        );
    }

    #[test]
    fn malformed_softnet_fields_fail_independently() {
        let input = "0000000a xyz 00000003\n00000014 00000004 xyz\n";
        assert_eq!(
            parse_softnet_stat(input),
            SoftnetLossCounters {
                dropped: Some(4),
                time_squeeze: Some(3),
            }
        );
        assert_eq!(parse_softnet_stat(""), SoftnetLossCounters::default());
    }

    #[test]
    fn snapshot_reads_available_sources_and_keeps_missing_fields_none() {
        let root = TestDir::new();
        let snmp = root.path().join("snmp");
        let softnet = root.path().join("softnet_stat");
        let net = root.path().join("net");
        fs::write(
            &snmp,
            "Udp: InErrors NoPorts RcvbufErrors InCsumErrors\nUdp: 9 8 7 6\n",
        )
        .unwrap();
        fs::write(&softnet, "00000001 00000002 00000003\n").unwrap();
        let statistics = net.join("eth0/statistics");
        fs::create_dir_all(&statistics).unwrap();
        fs::write(statistics.join("rx_dropped"), "12\n").unwrap();
        fs::write(statistics.join("rx_errors"), "14\n").unwrap();

        let snapshot =
            LossTelemetry::with_paths(&snmp, &softnet, &net, ["eth0", "missing0", "../escape"])
                .snapshot();

        assert_eq!(snapshot.udp.in_errors, Some(9));
        assert_eq!(snapshot.softnet.dropped, Some(2));
        assert_eq!(snapshot.interfaces["eth0"].rx_dropped, Some(12));
        assert_eq!(snapshot.interfaces["eth0"].rx_missed_errors, None);
        assert_eq!(snapshot.interfaces["eth0"].rx_errors, Some(14));
        assert_eq!(snapshot.interfaces["missing0"], NicLossCounters::default());
        assert_eq!(snapshot.interfaces["../escape"], NicLossCounters::default());
    }

    #[test]
    fn missing_sources_produce_an_empty_snapshot_without_error() {
        let root = TestDir::new();
        let snapshot = LossTelemetry::with_paths(
            root.path().join("absent-snmp"),
            root.path().join("absent-softnet"),
            root.path().join("absent-net"),
            ["eth0"],
        )
        .snapshot();

        assert_eq!(snapshot.udp, UdpLossCounters::default());
        assert_eq!(snapshot.softnet, SoftnetLossCounters::default());
        assert_eq!(snapshot.interfaces["eth0"], NicLossCounters::default());
    }

    #[test]
    fn delta_reports_increases_but_not_resets_or_missing_values() {
        let before = LossSnapshot {
            udp: UdpLossCounters {
                in_errors: Some(10),
                no_ports: Some(20),
                receive_buffer_errors: None,
                checksum_errors: Some(40),
            },
            softnet: SoftnetLossCounters {
                dropped: Some(3),
                time_squeeze: Some(9),
            },
            interfaces: BTreeMap::from([(
                "eth0".to_owned(),
                NicLossCounters {
                    rx_dropped: Some(7),
                    rx_missed_errors: Some(4),
                    rx_errors: Some(2),
                },
            )]),
        };
        let after = LossSnapshot {
            udp: UdpLossCounters {
                in_errors: Some(15),
                no_ports: Some(2),
                receive_buffer_errors: Some(8),
                checksum_errors: None,
            },
            softnet: SoftnetLossCounters {
                dropped: Some(3),
                time_squeeze: Some(11),
            },
            interfaces: BTreeMap::from([(
                "eth0".to_owned(),
                NicLossCounters {
                    rx_dropped: Some(12),
                    rx_missed_errors: Some(1),
                    rx_errors: Some(5),
                },
            )]),
        };

        let delta = after.delta_since(&before);
        assert_eq!(delta.udp.in_errors, Some(5));
        assert_eq!(delta.udp.no_ports, None);
        assert_eq!(delta.udp.receive_buffer_errors, None);
        assert_eq!(delta.udp.checksum_errors, None);
        assert_eq!(delta.softnet.dropped, Some(0));
        assert_eq!(delta.softnet.time_squeeze, Some(2));
        assert_eq!(delta.interfaces["eth0"].rx_dropped, Some(5));
        assert_eq!(delta.interfaces["eth0"].rx_missed_errors, None);
        assert_eq!(delta.interfaces["eth0"].rx_errors, Some(3));
    }

    #[test]
    fn socket_overflow_counter_handles_wraparound() {
        assert_eq!(socket_rxq_overflow_delta(14, 10), 4);
        assert_eq!(socket_rxq_overflow_delta(2, u32::MAX), 3);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn enables_socket_overflow_ancillary_messages() {
        let socket = UdpSocket::bind("127.0.0.1:0").unwrap();
        enable_socket_rxq_overflow(&socket).unwrap();
    }
}
