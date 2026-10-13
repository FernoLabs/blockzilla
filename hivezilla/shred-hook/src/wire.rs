//! Bounded local IPC format. It is not a registered HiveSync payload format.

use hivezilla_shred_hook_abi::*;
use std::io::{self, Read};

pub fn packet_frame(packet: &PacketV1, payload: &[u8]) -> Vec<u8> {
    let mut frame = Vec::with_capacity(IPC_HEADER_BYTES + payload.len());
    frame.extend_from_slice(IPC_MAGIC);
    frame.push(packet.kind);
    frame.push(packet.address_family);
    frame.extend_from_slice(&packet.local_port.to_be_bytes());
    frame.extend_from_slice(&packet.remote_port.to_be_bytes());
    frame.extend_from_slice(&[0; 2]);
    frame.extend_from_slice(&0u64.to_be_bytes());
    frame.extend_from_slice(&packet.observed_unix_ns.to_be_bytes());
    frame.extend_from_slice(&packet.remote_address);
    frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    frame.extend_from_slice(payload);
    frame
}

pub fn end_frame(sequence: u64, reason: u32) -> Vec<u8> {
    let packet = PacketV1 {
        struct_bytes: 0,
        kind: END_KIND,
        address_family: 0,
        local_port: 0,
        remote_port: 0,
        reserved: 0,
        remote_address: [0; 16],
        observed_unix_ns: 0,
        payload: std::ptr::null(),
        payload_bytes: 0,
    };
    let mut frame = packet_frame(&packet, &reason.to_be_bytes());
    frame[16..24].copy_from_slice(&sequence.to_be_bytes());
    frame
}

/// Returns EOF only between frames. Partial frames and oversized lengths fail.
pub fn read_frame(reader: &mut impl Read) -> io::Result<Option<Vec<u8>>> {
    let mut header = [0u8; IPC_HEADER_BYTES];
    match reader.read(&mut header[..1])? {
        0 => return Ok(None),
        1 => {}
        _ => unreachable!(),
    }
    reader.read_exact(&mut header[1..])?;
    if &header[..8] != IPC_MAGIC || header[14..16] != [0, 0] {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid shred hook header",
        ));
    }
    let len = u32::from_be_bytes(header[48..52].try_into().unwrap()) as usize;
    let kind = header[8];
    if kind == END_KIND {
        if len != 4 || header[9..16] != [0; 7] || header[24..48] != [0; 24] {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid end frame",
            ));
        }
    } else if kind > REPAIR_ACCEPTED
        || !matches!(header[9], 4 | 6)
        || len > MAX_PACKET_BYTES
        || (header[9] == 4 && header[36..48] != [0; 12])
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid packet frame",
        ));
    }
    let mut frame = Vec::with_capacity(IPC_HEADER_BYTES + len);
    frame.extend_from_slice(&header);
    frame.resize(IPC_HEADER_BYTES + len, 0);
    reader.read_exact(&mut frame[IPC_HEADER_BYTES..])?;
    Ok(Some(frame))
}

pub fn sequence(frame: &[u8]) -> u64 {
    u64::from_be_bytes(frame[16..24].try_into().unwrap())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn keeps_payload_duplicates_and_repair_nonce_exactly() {
        let payload = [7u8; MAX_PACKET_BYTES];
        for kind in [TURBINE_RECEIVED, REPAIR_RECEIVED, REPAIR_ACCEPTED] {
            let packet = PacketV1 {
                struct_bytes: size_of::<PacketV1>() as u32,
                kind,
                address_family: 4,
                local_port: 8001,
                remote_port: 9000,
                reserved: 0,
                remote_address: [0; 16],
                observed_unix_ns: 123,
                payload: payload.as_ptr(),
                payload_bytes: payload.len(),
            };
            let frame = packet_frame(&packet, &payload);
            let mut twice = [frame.clone(), frame.clone()].concat();
            let mut reader = twice.as_slice();
            assert_eq!(read_frame(&mut reader).unwrap().unwrap(), frame);
            assert_eq!(read_frame(&mut reader).unwrap().unwrap(), frame);
            assert!(read_frame(&mut reader).unwrap().is_none());
            twice.truncate(twice.len() - 1);
            let mut reader = twice.as_slice();
            read_frame(&mut reader).unwrap();
            assert!(read_frame(&mut reader).is_err());
        }
    }
    #[test]
    fn rejects_length_before_allocation() {
        let mut end = end_frame(0, END_QUEUE_FULL);
        end[48..52].copy_from_slice(&u32::MAX.to_be_bytes());
        assert!(read_frame(&mut end.as_slice()).is_err());
    }
    #[test]
    fn retains_empty_and_non_shred_datagrams() {
        let packet = PacketV1 {
            struct_bytes: size_of::<PacketV1>() as u32,
            kind: 0,
            address_family: 4,
            local_port: 8000,
            remote_port: 8001,
            reserved: 0,
            remote_address: [0; 16],
            observed_unix_ns: 1,
            payload: std::ptr::null(),
            payload_bytes: 0,
        };
        for bytes in [&[][..], &[1, 2, 3][..]] {
            let frame = packet_frame(&packet, bytes);
            assert_eq!(read_frame(&mut frame.as_slice()).unwrap().unwrap(), frame);
        }
    }
}
