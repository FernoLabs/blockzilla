//! Optional raw packet C-ABI producer, separate from Agave's Geyser interface.
//! Loaded once before receiver threads start. It is pinned for process lifetime.

use {
    hivezilla_shred_hook_abi::{ABI_VERSION, HookApiV1, HookFactoryV1, PacketV1, REPAIR_ACCEPTED},
    solana_perf::packet::{PacketBatch, PacketRef},
    solana_streamer::{evicting_sender::EvictingSender, streamer::ChannelSend},
    std::{
        ffi::CString,
        net::IpAddr,
        os::unix::ffi::OsStrExt,
        path::Path,
        sync::{
            OnceLock,
            atomic::{AtomicBool, Ordering},
        },
        time::{SystemTime, UNIX_EPOCH},
    },
};

struct LoadedHook {
    _library: libloading::Library,
    api: HookApiV1,
    disabled: AtomicBool,
}

static HOOK: OnceLock<Option<LoadedHook>> = OnceLock::new();

fn hook() -> Option<&'static LoadedHook> {
    HOOK.get_or_init(|| {
        let library = std::env::var_os("HIVEZILLA_SHRED_HOOK_LIBRARY");
        let socket = std::env::var_os("HIVEZILLA_SHRED_HOOK_SOCKET");
        match (library, socket) {
            (None, None) => None,
            (Some(library), Some(socket)) => {
                assert!(Path::new(&library).is_absolute() && Path::new(&socket).is_absolute(), "shred hook paths must be absolute");
                // SAFETY: this is an operator-configured library; code is trusted like a Geyser plugin.
                let library = unsafe { libloading::Library::new(library) }.expect("load Hivezilla shred hook");
                // SAFETY: the pinned producer exports the exact versioned C factory.
                let factory = unsafe { library.get::<HookFactoryV1>(b"hivezilla_shred_hook_v1\0") }.expect("shred hook factory");
                // SAFETY: the factory has no arguments and returns a process-lifetime table.
                let ptr = unsafe { factory() };
                assert!(!ptr.is_null(), "null shred hook API");
                // SAFETY: trusted factory returns a valid aligned API header and table.
                let api = unsafe { &*ptr };
                assert_eq!(api.abi_version, ABI_VERSION, "shred hook ABI version");
                assert_eq!(api.struct_bytes as usize, size_of::<HookApiV1>(), "shred hook API size");
                let api = *api;
                let socket = CString::new(socket.as_bytes()).expect("shred hook socket path");
                // SAFETY: a valid C string remains owned during initialization.
                assert_eq!(unsafe { (api.init)(socket.as_ptr(), 65_536) }, 0, "initialize shred hook; start recorder first");
                info!("Hivezilla raw-shred hook initialized, C ABI {ABI_VERSION}, queue capacity 65536 packets");
                Some(LoadedHook {_library:library,api,disabled:AtomicBool::new(false)})
            }
            _ => panic!("both Hivezilla shred hook library and socket must be configured"),
        }
    }).as_ref()
}

fn observed_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos().min(u64::MAX as u128) as u64)
        .unwrap_or(0)
}

fn capture(hook: &LoadedHook, kind: u8, local_port: u16, packet: PacketRef<'_>, timestamp: u64) {
    if hook.disabled.load(Ordering::Relaxed) {
        return;
    }
    let Some(payload) = packet.data(..) else {
        if !hook.disabled.swap(true, Ordering::Relaxed) {
            error!(
                "Hivezilla raw-shred capture failed: unavailable packet bytes; validator continues"
            );
        }
        (hook.api.shutdown)(hivezilla_shred_hook_abi::END_BAD_PACKET);
        return;
    };
    let remote = packet.meta().socket_addr();
    let mut address = [0u8; 16];
    let family = match remote.ip() {
        IpAddr::V4(ip) => {
            address[..4].copy_from_slice(&ip.octets());
            4
        }
        IpAddr::V6(ip) => {
            address.copy_from_slice(&ip.octets());
            6
        }
    };
    let view = PacketV1 {
        struct_bytes: size_of::<PacketV1>() as u32,
        kind,
        address_family: family,
        local_port,
        remote_port: remote.port(),
        reserved: 0,
        remote_address: address,
        observed_unix_ns: timestamp,
        payload: payload.as_ptr(),
        payload_bytes: payload.len(),
    };
    // SAFETY: all borrowed bytes and the C view remain valid until the call returns.
    let result = unsafe { (hook.api.notify)(&view) };
    if result != 0 && !hook.disabled.swap(true, Ordering::Relaxed) {
        error!(
            "Hivezilla raw-shred capture failed with code {result}; validator continues; capture restart required"
        );
    }
}

pub(crate) struct TapSender {
    sender: EvictingSender<PacketBatch>,
    hook: Option<&'static LoadedHook>,
    kind: u8,
    local_port: u16,
}

impl TapSender {
    pub(crate) fn new(sender: EvictingSender<PacketBatch>, kind: u8, local_port: u16) -> Self {
        Self {
            sender,
            hook: hook(),
            kind,
            local_port,
        }
    }
}

impl ChannelSend<PacketBatch> for TapSender {
    fn try_send(
        &self,
        batch: PacketBatch,
    ) -> Result<(), crossbeam_channel::TrySendError<PacketBatch>> {
        if let Some(hook) = self
            .hook
            .filter(|hook| !hook.disabled.load(Ordering::Relaxed))
        {
            let timestamp = observed_ns();
            for packet in batch.iter() {
                capture(hook, self.kind, self.local_port, packet, timestamp);
            }
        }
        // Preserve the original channel behavior and original packet allocation.
        self.sender.try_send(batch)
    }
    fn is_empty(&self) -> bool {
        self.sender.is_empty()
    }
    fn len(&self) -> usize {
        self.sender.len()
    }
}

pub(crate) fn repair_accepted(packet: PacketRef<'_>, local_port: u16) {
    if let Some(Some(hook)) = HOOK.get() {
        capture(hook, REPAIR_ACCEPTED, local_port, packet, observed_ns());
    }
}
