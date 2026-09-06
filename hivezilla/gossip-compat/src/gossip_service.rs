use std::{
    collections::HashSet,
    net::UdpSocket,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc::Sender,
    },
    thread::{self, JoinHandle},
    time::Duration,
};

use crate::cluster_info::ClusterInfo;
use solana_address::Address;

pub struct GossipService {
    thread_hdls: Vec<JoinHandle<()>>,
}

impl GossipService {
    pub fn new(
        cluster_info: &Arc<ClusterInfo>,
        epoch_specs: Option<Box<dyn std::any::Any + Send>>,
        gossip_sockets: Arc<[UdpSocket]>,
        gossip_validators: Option<HashSet<Address>>,
        should_check_duplicate_instance: bool,
        stats_reporter_sender: Option<Sender<Box<dyn FnOnce() + Send>>>,
        exit: Arc<AtomicBool>,
    ) -> Self {
        let _ = cluster_info.id();
        let _ = epoch_specs;
        let _ = gossip_sockets;
        let _ = gossip_validators;
        let _ = should_check_duplicate_instance;
        let _ = stats_reporter_sender;
        let handle = thread::spawn({
            let exit = exit.clone();
            move || {
                while !exit.load(Ordering::Relaxed) {
                    thread::sleep(Duration::from_millis(10));
                }
            }
        });

        Self {
            thread_hdls: vec![handle],
        }
    }

    pub fn join(self) -> thread::Result<()> {
        for thread_hdl in self.thread_hdls {
            thread_hdl.join()?;
        }
        Ok(())
    }
}
