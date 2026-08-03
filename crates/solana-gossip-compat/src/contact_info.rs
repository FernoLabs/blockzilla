use std::{
    fmt,
    net::{IpAddr, SocketAddr},
};

use serde::{Deserialize, Serialize};
use solana_address::Address as Pubkey;

#[derive(
    Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, serde::Serialize, serde::Deserialize,
)]
#[serde(rename_all = "lowercase")]
pub enum Protocol {
    /// UDP protocol for repair and gossip sockets.
    UDP,
    /// QUIC protocol variant preserved for forward compatibility.
    QUIC,
}

#[derive(Debug, Clone)]
pub enum Error {
    InvalidPort(u16),
    InvalidIpAddress(IpAddr),
}

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidPort(port) => write!(formatter, "invalid socket port {port}"),
            Self::InvalidIpAddress(addr) => {
                write!(formatter, "invalid socket address IP {addr} for gossip data plane")
            }
        }
    }
}

impl std::error::Error for Error {}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ContactInfo {
    pubkey: Pubkey,
    wallclock: u64,
    shred_version: u16,
    gossip_addr: Option<SocketAddr>,
    tvu_udp: Option<SocketAddr>,
    tvu_quic: Option<SocketAddr>,
    serve_repair_udp: Option<SocketAddr>,
    serve_repair_quic: Option<SocketAddr>,
}

impl ContactInfo {
    pub fn new(pubkey: Pubkey, wallclock: u64, shred_version: u16) -> Self {
        Self {
            pubkey,
            wallclock,
            shred_version,
            gossip_addr: None,
            tvu_udp: None,
            tvu_quic: None,
            serve_repair_udp: None,
            serve_repair_quic: None,
        }
    }

    #[inline]
    pub fn pubkey(&self) -> &Pubkey {
        &self.pubkey
    }

    #[inline]
    pub fn wallclock(&self) -> u64 {
        self.wallclock
    }

    #[inline]
    pub fn shred_version(&self) -> u16 {
        self.shred_version
    }

    pub fn set_wallclock(&mut self, wallclock: u64) {
        self.wallclock = wallclock;
    }

    pub fn set_shred_version(&mut self, shred_version: u16) {
        self.shred_version = shred_version;
    }

    pub fn set_gossip<T>(&mut self, socket: T) -> Result<(), Error>
    where
        T: Into<SocketAddr>,
    {
        let socket = socket.into();
        Self::validate_socket(&socket)?;
        self.gossip_addr = Some(socket);
        Ok(())
    }

    pub fn set_tvu<T>(&mut self, protocol: Protocol, socket: T) -> Result<(), Error>
    where
        T: Into<SocketAddr>,
    {
        let socket = socket.into();
        Self::validate_socket(&socket)?;
        match protocol {
            Protocol::UDP => self.tvu_udp = Some(socket),
            Protocol::QUIC => self.tvu_quic = Some(socket),
        }
        Ok(())
    }

    pub fn set_serve_repair<T>(&mut self, protocol: Protocol, socket: T) -> Result<(), Error>
    where
        T: Into<SocketAddr>,
    {
        let socket = socket.into();
        Self::validate_socket(&socket)?;
        match protocol {
            Protocol::UDP => self.serve_repair_udp = Some(socket),
            Protocol::QUIC => self.serve_repair_quic = Some(socket),
        }
        Ok(())
    }

    pub fn gossip(&self) -> Option<SocketAddr> {
        self.gossip_addr
    }

    pub fn tvu(&self, protocol: Protocol) -> Option<SocketAddr> {
        match protocol {
            Protocol::UDP => self.tvu_udp,
            Protocol::QUIC => self.tvu_quic,
        }
    }

    pub fn serve_repair(&self, protocol: Protocol) -> Option<SocketAddr> {
        match protocol {
            Protocol::UDP => self.serve_repair_udp,
            Protocol::QUIC => self.serve_repair_quic,
        }
    }

    /// Construct a contact info entry only containing a gossip endpoint.
    pub fn new_gossip_entry_point(gossip_addr: &SocketAddr) -> Self {
        let mut node = Self::new(Pubkey::default(), 0, 0);
        if let Err(error) = node.set_gossip(*gossip_addr) {
            tracing::warn!("Invalid entrypoint address {gossip_addr}: {error}");
        }
        node
    }

    fn validate_socket(socket: &SocketAddr) -> Result<(), Error> {
        if socket.port() == 0 {
            return Err(Error::InvalidPort(socket.port()));
        }
        if socket.ip().is_unspecified() {
            return Err(Error::InvalidIpAddress(socket.ip()));
        }
        if socket.ip().is_multicast() {
            return Err(Error::InvalidIpAddress(socket.ip()));
        }
        Ok(())
    }
}

impl Default for ContactInfo {
    fn default() -> Self {
        Self::new(Pubkey::default(), 0, 0)
    }
}
