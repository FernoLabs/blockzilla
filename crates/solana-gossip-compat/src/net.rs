//! Address-space and bind-address types, forked from `solana-net-utils`.
//!
//! Only two types were ever used from that crate, and depending on it dragged
//! in `bincode`, which is unmaintained. The behaviour here is a faithful copy
//! of `solana-net-utils` 4.1.2 so gossip keeps agreeing with the cluster about
//! which addresses are routable.
//!
//! Derived from Agave (`solana-net-utils`), licensed Apache-2.0.

use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    ops::Deref,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

/// Which addresses a node is willing to talk to.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum SocketAddrSpace {
    /// Accept any address, including private and loopback ranges.
    Unspecified,
    /// Accept only addresses reachable from the public internet.
    Global,
}

impl SocketAddrSpace {
    pub fn new(allow_private_addr: bool) -> Self {
        if allow_private_addr {
            SocketAddrSpace::Unspecified
        } else {
            SocketAddrSpace::Global
        }
    }

    /// Returns true if the address is usable in this space.
    ///
    /// The v4 and v6 arms deliberately test different things, matching upstream:
    /// v4 excludes private and loopback ranges, v6 excludes only loopback.
    /// `IpAddr::is_global` would replace both once it stabilises.
    #[inline]
    #[must_use]
    pub fn check(&self, addr: &SocketAddr) -> bool {
        if matches!(self, SocketAddrSpace::Unspecified) {
            return true;
        }
        match addr.ip() {
            IpAddr::V4(addr) => !(addr.is_private() || addr.is_loopback()),
            IpAddr::V6(addr) => !addr.is_loopback(),
        }
    }
}

/// The IP addresses this node may bind to.
///
/// Index 0 is the public internet address; index 1 and beyond are secondary
/// addresses used for multihoming. The active index is shared, so a clone
/// observes switches made through any other handle.
#[derive(Debug, Clone)]
pub struct BindIpAddrs {
    addrs: Vec<IpAddr>,
    active_index: Arc<AtomicUsize>,
}

impl Default for BindIpAddrs {
    fn default() -> Self {
        Self::new(vec![IpAddr::V4(Ipv4Addr::LOCALHOST)])
            .expect("a single loopback address is always a valid bind set")
    }
}

impl BindIpAddrs {
    pub fn new(addrs: Vec<IpAddr>) -> Result<Self, String> {
        if addrs.is_empty() {
            return Err(
                "BindIpAddrs requires at least one IP address (--bind-address)".to_string(),
            );
        }
        // Loopback, unspecified and multicast addresses cannot identify a
        // distinct host, so they are meaningless once multihoming is in play.
        if addrs.len() > 1 {
            for ip in &addrs {
                if ip.is_loopback() || ip.is_unspecified() || ip.is_multicast() {
                    return Err(format!(
                        "Invalid configuration: {ip:?} is not allowed with multiple \
                         --bind-address values (loopback, unspecified, or multicast)"
                    ));
                }
            }
        }
        Ok(Self {
            addrs,
            active_index: Arc::new(AtomicUsize::new(0)),
        })
    }

    #[inline]
    pub fn active(&self) -> IpAddr {
        self.addrs[self.active_index.load(Ordering::Acquire)]
    }

    /// Change the active address by index (0 = public internet IP, 1+ = secondary).
    pub fn set_active(&self, index: usize) -> Result<IpAddr, String> {
        if index >= self.addrs.len() {
            return Err(format!(
                "Index {index} out of range, only {} IPs available",
                self.addrs.len()
            ));
        }
        self.active_index.store(index, Ordering::Release);
        Ok(self.addrs[index])
    }

    #[inline]
    pub fn active_index(&self) -> usize {
        self.active_index.load(Ordering::Acquire)
    }

    #[inline]
    pub fn multihoming_enabled(&self) -> bool {
        self.addrs.len() > 1
    }
}

// Makes BindIpAddrs behave like &[IpAddr].
impl Deref for BindIpAddrs {
    type Target = [IpAddr];

    fn deref(&self) -> &Self::Target {
        &self.addrs
    }
}

impl AsRef<[IpAddr]> for BindIpAddrs {
    fn as_ref(&self) -> &[IpAddr] {
        &self.addrs
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv6Addr;

    #[test]
    fn global_space_rejects_private_and_loopback_but_unspecified_accepts_all() {
        let private = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 1);
        let loopback = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 1);
        let public = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8)), 1);
        let v6_loopback = SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 1);

        let global = SocketAddrSpace::new(false);
        assert_eq!(global, SocketAddrSpace::Global);
        assert!(!global.check(&private));
        assert!(!global.check(&loopback));
        assert!(!global.check(&v6_loopback));
        assert!(global.check(&public));

        let any = SocketAddrSpace::new(true);
        assert_eq!(any, SocketAddrSpace::Unspecified);
        for addr in [private, loopback, public, v6_loopback] {
            assert!(any.check(&addr));
        }
    }

    #[test]
    fn bind_addrs_reject_empty_and_reject_unroutable_when_multihomed() {
        assert!(BindIpAddrs::new(vec![]).is_err());

        // A lone loopback is fine; mixed with a second address it is not.
        assert!(BindIpAddrs::new(vec![IpAddr::V4(Ipv4Addr::LOCALHOST)]).is_ok());
        for bad in [
            IpAddr::V4(Ipv4Addr::LOCALHOST),
            IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            IpAddr::V4(Ipv4Addr::new(224, 0, 0, 1)),
        ] {
            assert!(BindIpAddrs::new(vec![IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8)), bad]).is_err());
        }
    }

    #[test]
    fn active_index_is_shared_across_clones_and_bounds_checked() {
        let a = IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8));
        let b = IpAddr::V4(Ipv4Addr::new(9, 9, 9, 9));
        let addrs = BindIpAddrs::new(vec![a, b]).expect("valid");
        assert_eq!(addrs.active(), a);
        assert_eq!(addrs.active_index(), 0);
        assert!(addrs.multihoming_enabled());
        assert_eq!(&*addrs, &[a, b]);

        let clone = addrs.clone();
        assert_eq!(addrs.set_active(1).expect("in range"), b);
        assert_eq!(clone.active(), b, "active index is shared with clones");
        assert!(addrs.set_active(2).is_err());
        assert_eq!(
            BindIpAddrs::default().active(),
            IpAddr::V4(Ipv4Addr::LOCALHOST)
        );
    }
}
