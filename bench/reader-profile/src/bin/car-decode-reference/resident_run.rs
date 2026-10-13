//! Parallel local input for diagnostic runs, with the full source locked in RAM.
use super::{
    Args, MetadataMode, Row, common_export, decode_metrics, dynamic, resident::ResidentSource,
    sharded,
};
use anyhow::{Context, Result, ensure};
use blockzilla_source::RangeSource;
use of_car_reader::CarBlockReader;
use serde_json::{Value, json};
use std::{
    fs::File,
    io::Cursor,
    sync::{Arc, atomic::AtomicU64},
};

pub(super) struct Prepared {
    source: ResidentSource,
    index: ResidentSource,
    ranges: Vec<sharded::BlockRange>,
    config: dynamic::Config,
    metrics: dynamic::Metrics,
    retries: AtomicU64,
    before: Value,
}
impl Prepared {
    pub(super) fn open(args: &Args, epoch: u64, expected: &[[u64; 2]]) -> Result<Self> {
        let manifest: Value =
            serde_json::from_reader(File::open(args.resident_cache_manifest.as_ref().unwrap())?)?;
        ensure!(
            manifest["schema"] == "zilla-local-car-cache-v1"
                && manifest["state"] == "COMPLETE"
                && manifest["epoch"].as_u64() == Some(epoch),
            "resident input requires a completed cache manifest for this epoch"
        );
        let car_path = args
            .file
            .as_ref()
            .context("resident input requires --file")?;
        let car_name = format!("epoch-{epoch}.car");
        let index_name = format!("epoch-{epoch}-slot-ranges.raw");
        ensure!(
            car_path.file_name().and_then(|s| s.to_str()) == Some(&car_name),
            "unexpected resident CAR name"
        );
        let source = ResidentSource::open(car_path, &manifest["files"][&car_name]["stat"])?;
        let index = ResidentSource::open(
            args.raw_index
                .as_ref()
                .context("resident input requires --raw-index")?,
            &manifest["files"][&index_name]["stat"],
        )?;
        ensure!(
            index.object() == index_name,
            "unexpected resident index name"
        );
        let car_bytes = source
            .size(source.object())?
            .context("resident CAR missing")?;
        let index_bytes = index.read_all_bounded(index.object(), 8 << 20)?;
        let header = source.read_range(source.object(), 0, car_bytes.min(1 << 20) as usize)?;
        let mut reader = CarBlockReader::with_capacity(Cursor::new(header), 8192);
        reader.skip_header_bounded(1 << 20)?;
        let ranges = sharded::plan_ranges(&index_bytes, epoch, expected, car_bytes, reader.offset)?;
        let before = json!({"car":source.residency()?, "index":index.residency()?});
        Ok(Self {
            source,
            index,
            ranges,
            config: dynamic::Config {
                parsers: args.parser_workers,
                decoders: args.workers,
                job_bytes: (args.job_mib as u64) << 20,
                chunk_bytes: args.http_chunk_mib << 20,
                reuse: !args.legacy_http_buffers,
            },
            metrics: dynamic::Metrics::default(),
            retries: AtomicU64::new(0),
            before,
        })
    }
    pub(super) fn identity(&self) -> Value {
        json!({"transport":"mlocked-local-mmap", "car":self.source.identity(), "index":self.index.identity()})
    }
    pub(super) fn scan(
        &self,
        mode: MetadataMode,
        export: Option<&common_export::Export>,
        stages: Option<Arc<decode_metrics::Metrics>>,
    ) -> Result<Vec<Row>> {
        dynamic::scan_dynamic_profiled(
            std::slice::from_ref(&self.source),
            self.source.object(),
            &self.ranges,
            self.config,
            mode,
            export,
            &self.retries,
            &self.metrics,
            stages,
        )
    }
    pub(super) fn finish_receipt(&self) -> Result<Value> {
        self.source.validate_unchanged()?;
        self.index.validate_unchanged()?;
        Ok(
            json!({"before_scan":self.before, "after_scan":{"car":self.source.residency()?, "index":self.index.residency()?},
            "source":self.source.stats(), "range_buffer_bytes":self.config.parsers*self.config.chunk_bytes,
            "scope":"Full readonly source mapping locked with mlock; no HTTP. Existing range-buffer copies and CAR parser remain timed."}),
        )
    }
    pub(super) fn pipeline_receipt(&self) -> Value {
        let mut receipt = self.metrics.receipt();
        let object = receipt.as_object_mut().unwrap();
        if let Some(value) = object.remove("http_read_seconds") {
            object.insert("resident_copy_seconds".into(), value);
        }
        if let Some(workers) = object
            .get_mut("parser_workers")
            .and_then(Value::as_array_mut)
        {
            for worker in workers {
                let worker = worker.as_object_mut().unwrap();
                if let Some(value) = worker.remove("http_read_seconds") {
                    worker.insert("resident_copy_seconds".into(), value);
                }
            }
        }
        object.insert("input_transport".into(), json!("mlocked-local-mmap"));
        receipt
    }
}
