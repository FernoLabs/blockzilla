const MAX_RETAINED_CAPACITY: usize = 256 << 20;
const MAX_BUFFER_CAPACITY: usize = 32 << 20;
const MAX_BUFFERS_PER_CLASS: usize = 8_192;
const MAX_ZERO_CAPACITY_BUFFERS: usize = 4_096;
const LARGER_CLASS_PROBES: usize = 2;

/// Cumulative allocation statistics for a lossless block's reusable data buffers.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LosslessDataBufferPoolStats {
    pub retained_buffers: usize,
    pub retained_capacity: usize,
    pub current_buffers: usize,
    pub current_capacity: usize,
    pub peak_current_buffers: usize,
    pub peak_current_capacity: usize,
    pub takes: u64,
    pub reused_buffers: u64,
    pub fresh_buffers: u64,
    pub allocation_events: u64,
    pub growth_events: u64,
    pub discarded_buffers: u64,
    pub discarded_capacity: u64,
}

#[derive(Debug)]
pub(crate) struct LosslessDataBufferPool {
    // Class zero stores empty allocations. Positive class N stores capacities
    // in [2^(N-1), 2^N). A request uses its power-of-two ceiling class.
    free_by_capacity: Vec<Vec<Vec<u8>>>,
    stats: LosslessDataBufferPoolStats,
}

impl Default for LosslessDataBufferPool {
    fn default() -> Self {
        Self {
            free_by_capacity: (0..=usize::BITS + 1).map(|_| Vec::new()).collect(),
            stats: LosslessDataBufferPoolStats::default(),
        }
    }
}

impl LosslessDataBufferPool {
    pub(crate) fn take(&mut self, required: usize) -> Vec<u8> {
        self.stats.takes = self.stats.takes.saturating_add(1);
        let class = required_capacity_class(required);
        let last_probe = class
            .saturating_add(LARGER_CLASS_PROBES)
            .min(self.free_by_capacity.len() - 1);
        let reusable_class =
            (class..=last_probe).find(|&candidate| !self.free_by_capacity[candidate].is_empty());

        let mut buffer = if let Some(reusable_class) = reusable_class {
            let buffer = self.free_by_capacity[reusable_class]
                .pop()
                .expect("selected data-buffer class is not empty");
            self.stats.reused_buffers = self.stats.reused_buffers.saturating_add(1);
            self.stats.retained_buffers = self.stats.retained_buffers.saturating_sub(1);
            self.stats.retained_capacity = self
                .stats
                .retained_capacity
                .saturating_sub(buffer.capacity());
            buffer
        } else {
            self.stats.fresh_buffers = self.stats.fresh_buffers.saturating_add(1);
            let allocation_capacity = if required > MAX_BUFFER_CAPACITY {
                required
            } else {
                class_allocation_capacity(class, required)
            };
            if allocation_capacity != 0 {
                self.stats.allocation_events = self.stats.allocation_events.saturating_add(1);
            }
            Vec::with_capacity(allocation_capacity)
        };

        buffer.clear();
        if buffer.capacity() < required {
            let target = if required > MAX_BUFFER_CAPACITY {
                required
            } else {
                class_allocation_capacity(class, required)
            };
            buffer.reserve(target);
            self.stats.allocation_events = self.stats.allocation_events.saturating_add(1);
            self.stats.growth_events = self.stats.growth_events.saturating_add(1);
        }
        self.record_live_buffer(&buffer);
        buffer
    }

    pub(crate) fn recycle(&mut self, mut buffer: Vec<u8>) {
        buffer.clear();
        let capacity = buffer.capacity();
        self.stats.current_buffers = self.stats.current_buffers.saturating_sub(1);
        self.stats.current_capacity = self.stats.current_capacity.saturating_sub(capacity);
        let class = recycled_capacity_class(capacity);
        let class_limit = if class == 0 {
            MAX_ZERO_CAPACITY_BUFFERS
        } else {
            MAX_BUFFERS_PER_CLASS
        };
        let retained_capacity = self.stats.retained_capacity.checked_add(capacity);
        let retain = capacity <= MAX_BUFFER_CAPACITY
            && self.free_by_capacity[class].len() < class_limit
            && retained_capacity.is_some_and(|total| total <= MAX_RETAINED_CAPACITY);
        if !retain {
            self.stats.discarded_buffers = self.stats.discarded_buffers.saturating_add(1);
            self.stats.discarded_capacity = self
                .stats
                .discarded_capacity
                .saturating_add(capacity as u64);
            return;
        }

        self.stats.retained_buffers = self.stats.retained_buffers.saturating_add(1);
        self.stats.retained_capacity = retained_capacity.expect("retained capacity was checked");
        self.free_by_capacity[class].push(buffer);
    }

    /// Record a buffer created by cloning an already decoded node.
    pub(crate) fn adopt_clone(&mut self, buffer: &Vec<u8>) {
        if buffer.capacity() != 0 {
            self.stats.allocation_events = self.stats.allocation_events.saturating_add(1);
        }
        self.record_live_buffer(buffer);
    }

    pub(crate) fn checkpoint(&self) -> (usize, usize) {
        (self.stats.current_buffers, self.stats.current_capacity)
    }

    /// Forget buffers that a failed decode dropped before it could return a node.
    pub(crate) fn rollback_to_checkpoint(&mut self, checkpoint: (usize, usize)) {
        let discarded_buffers = self.stats.current_buffers.saturating_sub(checkpoint.0);
        let discarded_capacity = self.stats.current_capacity.saturating_sub(checkpoint.1);
        self.stats.current_buffers = checkpoint.0;
        self.stats.current_capacity = checkpoint.1;
        self.stats.discarded_buffers = self
            .stats
            .discarded_buffers
            .saturating_add(discarded_buffers as u64);
        self.stats.discarded_capacity = self
            .stats
            .discarded_capacity
            .saturating_add(discarded_capacity as u64);
    }

    pub(crate) fn stats(&self) -> LosslessDataBufferPoolStats {
        self.stats
    }

    pub(crate) fn release(&mut self) {
        for class in &mut self.free_by_capacity {
            class.clear();
        }
        self.stats.retained_buffers = 0;
        self.stats.retained_capacity = 0;
    }

    fn record_live_buffer(&mut self, buffer: &Vec<u8>) {
        self.stats.current_buffers = self.stats.current_buffers.saturating_add(1);
        self.stats.current_capacity = self
            .stats
            .current_capacity
            .saturating_add(buffer.capacity());
        self.stats.peak_current_buffers = self
            .stats
            .peak_current_buffers
            .max(self.stats.current_buffers);
        self.stats.peak_current_capacity = self
            .stats
            .peak_current_capacity
            .max(self.stats.current_capacity);
    }
}

#[inline]
fn required_capacity_class(required: usize) -> usize {
    if required == 0 {
        0
    } else {
        1 + (usize::BITS - (required - 1).leading_zeros()) as usize
    }
}

#[inline]
fn recycled_capacity_class(capacity: usize) -> usize {
    if capacity == 0 {
        0
    } else {
        1 + (usize::BITS - 1 - capacity.leading_zeros()) as usize
    }
}

#[inline]
fn class_allocation_capacity(class: usize, required: usize) -> usize {
    if class == 0 {
        0
    } else {
        1usize.checked_shl((class - 1) as u32).unwrap_or(required)
    }
}
