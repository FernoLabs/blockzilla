//! Bounded preparation of dense transaction-effect chunks.

use std::{
    io::Write,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{Arc, Mutex, mpsc},
};

use anyhow::{Context, Result, ensure};
use blockzilla_archive_v3::ledger::transactions::{
    ChunkFrame, EFFECT_CHUNK_TRANSACTIONS, EffectKind, EffectState,
};

const ZSTD_LEVEL: i32 = 3;

/// One non-empty effect chunk after its raw-versus-zstd choice.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct PreparedEffectChunk {
    pub(crate) stored: Vec<u8>,
    pub(crate) decoded_len: u32,
    compressed: bool,
}

impl PreparedEffectChunk {
    pub(crate) fn frame(&self) -> Result<ChunkFrame> {
        let stored_len = u32::try_from(self.stored.len()).context("effect chunk exceeds u32")?;
        if self.compressed {
            ChunkFrame::zstd(stored_len).context("invalid zstd effect chunk")
        } else {
            ChunkFrame::raw(stored_len).context("invalid raw effect chunk")
        }
    }
}

fn prepare(raw: Vec<u8>) -> Result<PreparedEffectChunk> {
    ensure!(!raw.is_empty(), "a non-empty effect chunk encoded no bytes");
    let decoded_len = u32::try_from(raw.len()).context("decoded effect chunk exceeds u32")?;
    let compressed = {
        let mut encoder =
            zstd::Encoder::new(Vec::new(), ZSTD_LEVEL).context("create zstd encoder")?;
        encoder
            .include_checksum(true)
            .context("enable zstd checksum")?;
        encoder
            .set_pledged_src_size(Some(raw.len() as u64))
            .context("set zstd content size")?;
        encoder.write_all(&raw).context("compress effect chunk")?;
        encoder.finish().context("finish effect chunk")?
    };
    if compressed.len() < raw.len() {
        Ok(PreparedEffectChunk {
            stored: compressed,
            decoded_len,
            compressed: true,
        })
    } else {
        Ok(PreparedEffectChunk {
            stored: raw,
            decoded_len,
            compressed: false,
        })
    }
}

/// Encode dense Wincode records in 256-transaction chunks.
///
/// `None` is not encoded. Its state bit must also be clear. This check makes
/// it impossible for the transaction row and the dense effect stream to
/// silently drift apart.
pub(crate) fn prepare_effect_chunks<T, E>(
    values: &[Option<T>],
    states: &[EffectState],
    kind: EffectKind,
    mut encode: impl FnMut(&T) -> std::result::Result<Vec<u8>, E>,
) -> Result<Vec<Option<PreparedEffectChunk>>>
where
    E: Into<anyhow::Error>,
{
    ensure!(
        values.len() == states.len(),
        "{kind:?} value count {} does not match effect-state count {}",
        values.len(),
        states.len()
    );
    let chunk_width = EFFECT_CHUNK_TRANSACTIONS as usize;
    let mut chunks = Vec::with_capacity(values.len().div_ceil(chunk_width));
    for (chunk_index, (value_chunk, state_chunk)) in values
        .chunks(chunk_width)
        .zip(states.chunks(chunk_width))
        .enumerate()
    {
        let mut raw = Vec::new();
        for (row, (value, state)) in value_chunk.iter().zip(state_chunk).enumerate() {
            let present = state.has_record(kind).with_context(|| {
                format!("invalid {kind:?} state in chunk {chunk_index} row {row}")
            })?;
            ensure!(
                present == value.is_some(),
                "{kind:?} state/value mismatch in chunk {chunk_index} row {row}"
            );
            if let Some(value) = value {
                raw.extend_from_slice(
                    &encode(value).map_err(Into::into).with_context(|| {
                        format!("encode {kind:?} chunk {chunk_index} row {row}")
                    })?,
                );
            }
        }
        chunks.push(if raw.is_empty() {
            None
        } else {
            Some(prepare(raw)?)
        });
    }
    Ok(chunks)
}

type PrepareEffect = Box<dyn FnOnce() -> Result<Vec<Option<PreparedEffectChunk>>> + Send + 'static>;

pub(crate) struct EffectJob {
    index: usize,
    prepare: PrepareEffect,
}

pub(crate) fn effect_job(
    index: usize,
    prepare: impl FnOnce() -> Result<Vec<Option<PreparedEffectChunk>>> + Send + 'static,
) -> EffectJob {
    EffectJob {
        index,
        prepare: Box::new(prepare),
    }
}

struct EffectJobResult {
    index: usize,
    chunks: Result<Vec<Option<PreparedEffectChunk>>>,
}

pub(crate) struct EffectPool {
    jobs: Option<mpsc::SyncSender<EffectJob>>,
    results: mpsc::Receiver<EffectJobResult>,
    handles: Vec<std::thread::JoinHandle<()>>,
}

impl EffectPool {
    fn new(worker_count: usize) -> Result<Self> {
        ensure!(worker_count != 0, "effect worker count must be non-zero");
        let (job_sender, job_receiver) = mpsc::sync_channel::<EffectJob>(6);
        let job_receiver = Arc::new(Mutex::new(job_receiver));
        let (result_sender, result_receiver) = mpsc::channel();
        let mut handles = Vec::with_capacity(worker_count);
        for worker in 0..worker_count {
            let jobs = Arc::clone(&job_receiver);
            let results = result_sender.clone();
            handles.push(
                std::thread::Builder::new()
                    .name(format!("ia-effect-{worker}"))
                    .spawn(move || {
                        loop {
                            let job = jobs
                                .lock()
                                .unwrap_or_else(std::sync::PoisonError::into_inner)
                                .recv();
                            let Ok(job) = job else { return };
                            let index = job.index;
                            let chunks = catch_unwind(AssertUnwindSafe(job.prepare))
                                .unwrap_or_else(|_| Err(anyhow::anyhow!("effect worker panicked")));
                            if results.send(EffectJobResult { index, chunks }).is_err() {
                                return;
                            }
                        }
                    })
                    .context("start effect worker")?,
            );
        }
        drop(result_sender);
        Ok(Self {
            jobs: Some(job_sender),
            results: result_receiver,
            handles,
        })
    }

    fn prepare(&self, jobs: [EffectJob; 6]) -> Result<[Vec<Option<PreparedEffectChunk>>; 6]> {
        for (index, job) in jobs.into_iter().enumerate() {
            ensure!(
                job.index == index,
                "effect jobs are not in stable file order"
            );
            self.jobs
                .as_ref()
                .expect("effect sender exists")
                .send(job)
                .map_err(|_| anyhow::anyhow!("effect job channel closed"))?;
        }
        let mut prepared: [Option<Vec<Option<PreparedEffectChunk>>>; 6] =
            std::array::from_fn(|_| None);
        for _ in 0..6 {
            let result = self.results.recv().context("effect worker stopped")?;
            ensure!(result.index < 6, "effect worker returned an invalid index");
            ensure!(
                prepared[result.index].is_none(),
                "effect job returned twice"
            );
            prepared[result.index] = Some(result.chunks?);
        }
        Ok(prepared.map(|chunks| chunks.expect("every effect job returned")))
    }
}

impl Drop for EffectPool {
    fn drop(&mut self) {
        self.jobs.take();
        for handle in self.handles.drain(..) {
            let _ = handle.join();
        }
    }
}

pub(crate) enum EffectEncoder {
    Inline,
    Pool(EffectPool),
}

impl EffectEncoder {
    pub(crate) fn new(worker_count: usize) -> Result<Self> {
        if worker_count == 0 {
            Ok(Self::Inline)
        } else {
            Ok(Self::Pool(EffectPool::new(worker_count)?))
        }
    }

    pub(crate) fn prepare(
        &self,
        jobs: [EffectJob; 6],
    ) -> Result<[Vec<Option<PreparedEffectChunk>>; 6]> {
        match self {
            Self::Inline => jobs
                .into_iter()
                .map(|job| (job.prepare)())
                .collect::<Result<Vec<_>>>()?
                .try_into()
                .map_err(|_| anyhow::anyhow!("effect result count mismatch")),
            Self::Pool(pool) => pool.prepare(jobs),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn jobs() -> [EffectJob; 6] {
        std::array::from_fn(|index| {
            effect_job(index, move || {
                Ok(vec![Some(PreparedEffectChunk {
                    stored: vec![index as u8; index + 1],
                    decoded_len: (index + 1) as u32,
                    compressed: false,
                })])
            })
        })
    }

    #[test]
    fn pool_returns_effects_in_stable_file_order() {
        let inline = EffectEncoder::new(0).unwrap().prepare(jobs()).unwrap();
        let parallel = EffectEncoder::new(4).unwrap().prepare(jobs()).unwrap();
        assert_eq!(parallel, inline);
    }

    #[test]
    fn state_and_dense_value_must_agree() {
        let states = [EffectState::new(
            blockzilla_archive_v3::ledger::transactions::CpiState::Unavailable,
        )];
        let values = [Some(7_u8)];
        assert!(
            prepare_effect_chunks(&values, &states, EffectKind::Outcome, |value| {
                Ok::<_, anyhow::Error>(vec![*value])
            })
            .is_err()
        );
    }
}
