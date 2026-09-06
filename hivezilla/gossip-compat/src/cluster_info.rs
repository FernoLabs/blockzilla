use std::time::{SystemTime, UNIX_EPOCH};
use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use crate::net::{BindIpAddrs, SocketAddrSpace};
use solana_address::Address as Pubkey;
use solana_keypair::{Keypair, Signer};

use crate::contact_info::{ContactInfo, Protocol};

#[derive(Debug)]
pub struct ClusterInfo {
    my_contact_info: RwLock<ContactInfo>,
    keypair: Arc<Keypair>,
    socket_addr_space: SocketAddrSpace,
    bind_ip_addrs: RwLock<Arc<BindIpAddrs>>,
    entrypoints: RwLock<Vec<ContactInfo>>,
    peers: RwLock<HashMap<Pubkey, ContactInfo>>,
    #[allow(dead_code)]
    contact_debug_interval_ms: RwLock<u64>,
}

impl ClusterInfo {
    pub fn new(
        contact_info: ContactInfo,
        keypair: Arc<Keypair>,
        socket_addr_space: SocketAddrSpace,
    ) -> Self {
        assert_eq!(contact_info.pubkey(), &keypair.pubkey());
        let peers = HashMap::new();
        Self {
            my_contact_info: RwLock::new(contact_info),
            keypair,
            socket_addr_space,
            bind_ip_addrs: RwLock::new(Arc::new(BindIpAddrs::default())),
            entrypoints: RwLock::new(Vec::new()),
            peers: RwLock::new(peers),
            contact_debug_interval_ms: RwLock::new(0),
        }
    }

    pub fn id(&self) -> Pubkey {
        *self.my_contact_info.read().unwrap().pubkey()
    }

    pub fn keypair(&self) -> &Arc<Keypair> {
        &self.keypair
    }

    pub fn set_contact_debug_interval(&self, new: u64) {
        *self.contact_debug_interval_ms.write().unwrap() = new;
    }

    pub fn socket_addr_space(&self) -> &SocketAddrSpace {
        &self.socket_addr_space
    }

    pub fn set_bind_ip_addrs(&self, ip_addrs: Arc<BindIpAddrs>) {
        *self.bind_ip_addrs.write().unwrap() = ip_addrs;
    }

    pub fn bind_ip_addrs(&self) -> Arc<BindIpAddrs> {
        self.bind_ip_addrs.read().unwrap().clone()
    }

    pub fn set_entrypoints(&self, entrypoints: Vec<ContactInfo>) {
        *self.entrypoints.write().unwrap() = entrypoints;
    }

    pub fn gossip_peers(&self) -> Vec<ContactInfo> {
        self.peers_snapshot()
            .into_iter()
            .filter(|peer| peer.gossip().is_some())
            .collect()
    }

    pub fn tvu_peers<R>(&self, mut mapper: impl FnMut(&ContactInfo) -> R) -> Vec<R> {
        self.peers_snapshot()
            .into_iter()
            .filter(|peer| {
                peer.tvu(Protocol::UDP)
                    .or(peer.tvu(Protocol::QUIC))
                    .is_some()
            })
            .map(|peer| mapper(&peer))
            .collect()
    }

    pub fn all_peers(&self) -> Vec<(ContactInfo, u64)> {
        self.peers_snapshot()
            .into_iter()
            .map(|peer| {
                let clock = peer.wallclock();
                (peer, clock)
            })
            .collect()
    }

    pub fn repair_peers(&self, _slot: u64) -> Vec<ContactInfo> {
        self.peers_snapshot()
            .into_iter()
            .filter(|peer| {
                peer.serve_repair(Protocol::UDP)
                    .or(peer.serve_repair(Protocol::QUIC))
                    .is_some()
            })
            .collect()
    }

    fn peers_snapshot(&self) -> Vec<ContactInfo> {
        let mut peers = Vec::new();
        let self_id = self.id();
        {
            let peers_lock = self.peers.read().unwrap();
            peers.extend(peers_lock.values().cloned());
        }
        {
            let entrypoints = self.entrypoints.read().unwrap();
            peers.extend(entrypoints.iter().cloned());
        }
        peers
            .into_iter()
            .filter(|peer| peer.pubkey() != &self_id)
            .fold(Vec::new(), |mut acc, peer| {
                if !acc
                    .iter()
                    .any(|existing| existing.pubkey() == peer.pubkey())
                {
                    acc.push(peer);
                }
                acc
            })
    }

    /// Compatibility helper used by future in-crate extensions.
    pub(crate) fn add_discovered_peer(&self, contact: ContactInfo) {
        if contact.pubkey() == &self.id() {
            return;
        }
        self.peers
            .write()
            .unwrap()
            .insert(*contact.pubkey(), contact);
    }

    pub(crate) fn timestamp_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_millis() as u64)
            .unwrap_or(0)
    }
}
