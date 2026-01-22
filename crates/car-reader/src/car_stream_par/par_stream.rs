use crossbeam_channel::{Receiver, Sender};
use std::{sync::Arc, thread};

use crate::{
    car_stream::CarStream,
    car_stream_par::car_group_pool::{CarBlockGroupPool, PooledGroup},
    error::CarReadError,
};

pub struct UnorderedCarGroupStream {
    rx: Receiver<PooledGroup>,
    // Hold join handle if you want to join on drop, optional.
}

impl UnorderedCarGroupStream {
    pub fn recv(&self) -> Result<PooledGroup, crossbeam_channel::RecvError> {
        self.rx.recv()
    }

    pub fn try_recv(&self) -> Result<PooledGroup, crossbeam_channel::TryRecvError> {
        self.rx.try_recv()
    }

    pub fn iter(&self) -> crossbeam_channel::Iter<'_, PooledGroup> {
        self.rx.iter()
    }
}

/// Spawn a producer thread that reads sequentially and sends groups.
/// - `channel_bound` caps in-flight groups.
/// - `pool_size` caps total groups allocated (and can be >= bound).
pub fn spawn_car_group_producer<R: std::io::Read + Send + 'static>(
    mut stream: CarStream<R>,
    channel_bound: usize,
    pool: Arc<CarBlockGroupPool>,
) -> (
    UnorderedCarGroupStream,
    thread::JoinHandle<Result<(), CarReadError>>,
) {
    let (tx, rx): (Sender<PooledGroup>, Receiver<PooledGroup>) =
        crossbeam_channel::bounded(channel_bound);

    let handle = thread::spawn(move || -> Result<(), CarReadError> {
        loop {
            // Acquire reusable group.
            let mut pg = pool.checkout();

            // Read next group directly into the pooled buffer.
            match stream.next_group_into(pg.as_mut()) {
                Ok(true) => {
                    // Backpressure here if consumer is slow.
                    // If receiver is dropped, send() returns Err and we stop.
                    if tx.send(pg).is_err() {
                        return Ok(());
                    }
                }
                Ok(false) => {
                    // EOF: pg drops here, returning to pool.
                    return Ok(());
                }
                Err(e) => {
                    // Error: pg drops here, returning to pool.
                    return Err(e);
                }
            }
        }
    });

    (UnorderedCarGroupStream { rx }, handle)
}
