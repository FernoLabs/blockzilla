//! Linux-only cross-compiler C ABI fixture. Compile directly with Agave's Rust
//! toolchain; load the producer compiled by the workspace toolchain.

#[allow(dead_code)]
#[path = "../../shred-hook-abi/src/lib.rs"]
mod abi;

#[cfg(target_os = "linux")]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    use abi::*;
    use std::{
        ffi::{CString, c_char, c_int, c_void},
        io::Read,
    };
    #[link(name = "dl")]
    unsafe extern "C" {
        fn dlopen(path: *const c_char, flags: c_int) -> *mut c_void;
        fn dlsym(handle: *mut c_void, name: *const c_char) -> *mut c_void;
    }
    let args: Vec<_> = std::env::args().collect();
    assert_eq!(args.len(), 4, "usage: fixture LIBRARY SOCKET PCAP");
    let path = CString::new(args[1].as_bytes())?;
    // SAFETY: an operator-selected, locally built library; valid C path.
    let library = unsafe { dlopen(path.as_ptr(), 2) };
    assert!(!library.is_null(), "load producer");
    let symbol = CString::new("hivezilla_shred_hook_v1")?;
    // SAFETY: valid library handle and C symbol name.
    let factory = unsafe { dlsym(library, symbol.as_ptr()) };
    assert!(!factory.is_null(), "producer factory");
    // SAFETY: the exact versioned factory has this C function signature.
    let factory: HookFactoryV1 = unsafe { std::mem::transmute(factory) };
    // SAFETY: factory returns a valid process-lifetime C table.
    let api = unsafe { &*factory() };
    assert_eq!(api.abi_version, ABI_VERSION);
    assert_eq!(api.struct_bytes as usize, size_of::<HookApiV1>());
    let socket = CString::new(args[2].as_bytes())?;
    // SAFETY: socket C string is valid for the call.
    assert_eq!(unsafe { (api.init)(socket.as_ptr(), 65536) }, 0);
    let mut reader = std::io::BufReader::new(std::fs::File::open(&args[3])?);
    let mut global = [0u8; 24];
    reader.read_exact(&mut global)?;
    assert_eq!(
        &global[..4],
        &[0xd4, 0xc3, 0xb2, 0xa1],
        "fixture requires little-endian microsecond pcap"
    );
    let link = u32::from_le_bytes(global[20..24].try_into()?);
    let mut packets = 0u64;
    let mut bytes = 0u64;
    loop {
        let mut header = [0u8; 16];
        if reader.read(&mut header[..1])? == 0 {
            break;
        }
        reader.read_exact(&mut header[1..])?;
        let size = u32::from_le_bytes(header[8..12].try_into()?) as usize;
        assert!(
            size <= 65536 && size == u32::from_le_bytes(header[12..16].try_into()?) as usize,
            "truncated or oversized pcap record"
        );
        let mut packet = vec![0u8; size];
        reader.read_exact(&mut packet)?;
        let mut offset = match link {
            1 => 14,
            113 => 16,
            276 => 20,
            _ => panic!("unsupported pcap link {link}"),
        };
        if link == 1 && packet.get(12..14) == Some(&[0x81, 0x00]) {
            offset += 4;
        }
        if packet.get(offset).is_none_or(|byte| byte >> 4 != 4) {
            continue;
        }
        let ihl = (packet[offset] & 15) as usize * 4;
        if ihl < 20 || packet.get(offset + 9) != Some(&17) {
            continue;
        }
        assert_eq!(
            u16::from_be_bytes(packet[offset + 6..offset + 8].try_into()?) & 0x3fff,
            0,
            "fragmented fixture UDP packet"
        );
        let udp = offset + ihl;
        let udp_len = u16::from_be_bytes(packet[udp + 4..udp + 6].try_into()?) as usize;
        assert!(udp_len >= 8 && udp + udp_len <= packet.len());
        let payload = &packet[udp + 8..udp + udp_len];
        assert!(payload.len() <= MAX_PACKET_BYTES);
        let mut remote = [0u8; 16];
        remote[..4].copy_from_slice(&packet[offset + 12..offset + 16]);
        let seconds = u32::from_le_bytes(header[..4].try_into()?) as u64;
        let micros = u32::from_le_bytes(header[4..8].try_into()?) as u64;
        let view = PacketV1 {
            struct_bytes: size_of::<PacketV1>() as u32,
            kind: TURBINE_RECEIVED,
            address_family: 4,
            local_port: u16::from_be_bytes(packet[udp + 2..udp + 4].try_into()?),
            remote_port: u16::from_be_bytes(packet[udp..udp + 2].try_into()?),
            reserved: 0,
            remote_address: remote,
            observed_unix_ns: seconds * 1_000_000_000 + micros * 1000,
            payload: payload.as_ptr(),
            payload_bytes: payload.len(),
        };
        // SAFETY: the view and captured payload remain valid until the call returns.
        assert_eq!(unsafe { (api.notify)(&view) }, 0);
        packets += 1;
        bytes += payload.len() as u64;
    }
    assert!(packets > 0, "fixture has no UDP payloads");
    (api.shutdown)(END_CLEAN);
    // The API requests shutdown; it does not block receive threads on IPC.
    std::thread::sleep(std::time::Duration::from_millis(500));
    println!(
        "{{\"fixture_packets\":{packets},\"fixture_payload_bytes\":{bytes},\"fixture_compiler\":\"Agave toolchain\",\"producer_abi\":1}}"
    );
    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn main() {}
