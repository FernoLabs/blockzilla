//! Raw packet producer. The callback copies bounded bytes into a bounded queue.
//! A worker owns all IPC writes. Failure closes the capture; it never blocks
//! validator replay on disk or claims a continuous stream after losing input.

pub mod wire;
#[cfg(feature = "recorder")]
pub mod writer;

use hivezilla_shred_hook_abi::*;
use std::{
    ffi::CStr,
    io::{self, Write},
    os::unix::net::UnixStream,
    sync::{
        Arc, OnceLock,
        atomic::{AtomicU32, Ordering},
        mpsc::{self, SyncSender, TrySendError},
    },
    thread,
    time::Duration,
};

struct Producer {
    sender: SyncSender<Vec<u8>>,
    reason: Arc<AtomicU32>,
}

impl Producer {
    fn new(mut stream: UnixStream, capacity: usize) -> io::Result<Self> {
        if !(1..=262_144).contains(&capacity) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid queue capacity",
            ));
        }
        stream.set_write_timeout(Some(Duration::from_secs(5)))?;
        let (sender, receiver) = mpsc::sync_channel::<Vec<u8>>(capacity);
        let reason = Arc::new(AtomicU32::new(0));
        let worker_reason = reason.clone();
        thread::Builder::new()
            .name("hiveShredIPC".into())
            .spawn(move || {
                let mut sequence = 0u64;
                loop {
                    match receiver.recv_timeout(Duration::from_millis(100)) {
                        Ok(mut frame) => {
                            frame[16..24].copy_from_slice(&sequence.to_be_bytes());
                            if stream.write_all(&frame).is_err() {
                                worker_reason.store(END_TRANSPORT, Ordering::Release);
                                return;
                            }
                            let Some(next) = sequence.checked_add(1) else {
                                worker_reason.store(END_BAD_PACKET, Ordering::Release);
                                return;
                            };
                            sequence = next;
                        }
                        Err(mpsc::RecvTimeoutError::Disconnected) => break,
                        Err(mpsc::RecvTimeoutError::Timeout) => {
                            if worker_reason.load(Ordering::Acquire) != 0 {
                                break;
                            }
                        }
                    }
                }
                let code = worker_reason.load(Ordering::Acquire);
                let end = wire::end_frame(sequence, if code == 4 { END_CLEAN } else { code });
                let _ = stream.write_all(&end);
            })?;
        Ok(Self { sender, reason })
    }

    fn submit(&self, frame: Vec<u8>) -> i32 {
        if self.reason.load(Ordering::Acquire) != 0 {
            return 1;
        }
        match self.sender.try_send(frame) {
            Ok(()) => 0,
            Err(TrySendError::Full(_)) => {
                self.reason
                    .compare_exchange(0, END_QUEUE_FULL, Ordering::AcqRel, Ordering::Acquire)
                    .ok();
                1
            }
            Err(TrySendError::Disconnected(_)) => {
                self.reason
                    .compare_exchange(0, END_TRANSPORT, Ordering::AcqRel, Ordering::Acquire)
                    .ok();
                1
            }
        }
    }
}

static PRODUCER: OnceLock<Producer> = OnceLock::new();

/// # Safety
/// `socket_path` must point to a valid NUL-terminated string for this call.
unsafe extern "C" fn init(socket_path: *const std::ffi::c_char, capacity: u32) -> i32 {
    if socket_path.is_null() || PRODUCER.get().is_some() {
        return -1;
    }
    // SAFETY: the adapter owns the C string for the duration of this call.
    let path = unsafe { CStr::from_ptr(socket_path) };
    use std::os::unix::ffi::OsStrExt;
    let path = std::ffi::OsStr::from_bytes(path.to_bytes());
    let result =
        UnixStream::connect(path).and_then(|stream| Producer::new(stream, capacity as usize));
    match result {
        Ok(producer) => {
            if PRODUCER.set(producer).is_ok() {
                0
            } else {
                -1
            }
        }
        Err(_) => -1,
    }
}

/// # Safety
/// The packet and its byte range must remain valid until this call returns.
unsafe extern "C" fn notify(packet: *const PacketV1) -> i32 {
    let Some(producer) = PRODUCER.get() else {
        return -1;
    };
    if producer.reason.load(Ordering::Acquire) != 0 {
        return 1;
    }
    if packet.is_null() {
        producer.reason.store(END_BAD_PACKET, Ordering::Release);
        return -1;
    }
    // SAFETY: the ABI caller supplies a valid aligned packet view.
    let packet = unsafe { &*packet };
    if packet.struct_bytes as usize != size_of::<PacketV1>()
        || packet.reserved != 0
        || packet.kind > REPAIR_ACCEPTED
        || !matches!(packet.address_family, 4 | 6)
        || (packet.payload.is_null() && packet.payload_bytes != 0)
        || packet.payload_bytes > MAX_PACKET_BYTES
    {
        producer.reason.store(END_BAD_PACKET, Ordering::Release);
        return -1;
    }
    // SAFETY: the adapter supplied this bounded byte range for the call.
    let payload = if packet.payload_bytes == 0 {
        &[]
    } else {
        unsafe { std::slice::from_raw_parts(packet.payload, packet.payload_bytes) }
    };
    producer.submit(wire::packet_frame(packet, payload))
}

extern "C" fn shutdown(reason: u32) {
    if let Some(producer) = PRODUCER.get() {
        producer
            .reason
            .compare_exchange(
                0,
                if reason == END_CLEAN {
                    4
                } else {
                    reason.min(END_BAD_PACKET)
                },
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .ok();
    }
}

static API: HookApiV1 = HookApiV1 {
    abi_version: ABI_VERSION,
    struct_bytes: size_of::<HookApiV1>() as u32,
    init,
    notify,
    shutdown,
};

/// Returns the stable C function table. The library must remain loaded while any
/// callback or worker can still run. The adapter keeps it for process lifetime.
#[unsafe(no_mangle)]
pub extern "C" fn hivezilla_shred_hook_v1() -> *const HookApiV1 {
    &API
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn full_queue_stops_capture_without_waiting_for_a_consumer() {
        let (sender, receiver) = mpsc::sync_channel(1);
        let producer = Producer {
            sender,
            reason: Arc::new(AtomicU32::new(0)),
        };
        assert_eq!(producer.submit(vec![1]), 0);
        assert_eq!(producer.submit(vec![2]), 1);
        assert_eq!(producer.reason.load(Ordering::Acquire), END_QUEUE_FULL);
        assert_eq!(receiver.recv().unwrap(), vec![1]);
        assert_eq!(producer.submit(vec![3]), 1);
        assert!(receiver.try_recv().is_err());
    }

    #[test]
    fn disconnected_consumer_stops_capture() {
        let (sender, receiver) = mpsc::sync_channel(1);
        drop(receiver);
        let producer = Producer {
            sender,
            reason: Arc::new(AtomicU32::new(0)),
        };
        assert_eq!(producer.submit(vec![1]), 1);
        assert_eq!(producer.reason.load(Ordering::Acquire), END_TRANSPORT);
    }
}
