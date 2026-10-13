//! C ABI shared by the Agave receive adapter and the Hivezilla producer.
//! No Rust-owned object or borrowed Rust slice crosses the library boundary.

use std::ffi::c_char;

pub const ABI_VERSION: u32 = 1;
pub const TURBINE_RECEIVED: u8 = 0;
pub const REPAIR_RECEIVED: u8 = 1;
pub const REPAIR_ACCEPTED: u8 = 2;
pub const MAX_PACKET_BYTES: usize = 1232;
pub const IPC_MAGIC: &[u8; 8] = b"HZSHIPC1";
pub const IPC_HEADER_BYTES: usize = 52;
pub const END_KIND: u8 = 255;
pub const END_CLEAN: u32 = 0;
pub const END_QUEUE_FULL: u32 = 1;
pub const END_TRANSPORT: u32 = 2;
pub const END_BAD_PACKET: u32 = 3;

#[repr(C)]
#[derive(Clone, Copy)]
pub struct PacketV1 {
    pub struct_bytes: u32,
    pub kind: u8,
    pub address_family: u8,
    pub local_port: u16,
    pub remote_port: u16,
    pub reserved: u16,
    /// IPv4 uses its four octets followed by twelve zero bytes; IPv6 uses sixteen octets.
    pub remote_address: [u8; 16],
    /// Time when the adapter observed the received batch, not a NIC timestamp.
    pub observed_unix_ns: u64,
    pub payload: *const u8,
    pub payload_bytes: usize,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct HookApiV1 {
    pub abi_version: u32,
    pub struct_bytes: u32,
    pub init: unsafe extern "C" fn(socket_path: *const c_char, queue_packets: u32) -> i32,
    pub notify: unsafe extern "C" fn(packet: *const PacketV1) -> i32,
    pub shutdown: extern "C" fn(reason: u32),
}

pub type HookFactoryV1 = unsafe extern "C" fn() -> *const HookApiV1;
