# Hivezilla

Hivezilla is the Blockzilla control plane for renting temporary machines and
spreading CAR crunching work across them.

The name keeps the Blockzilla/Godzilla kaiju spine while making the distributed
shape explicit: many short-lived workers acting like one hive to turn CAR files
into Blockzilla Archive V2 output.

## MVP shape

The first version is intentionally provider-light:

- build a JSON work plan for one or many epochs;
- assign jobs across `hive-000`, `hive-001`, ... workers;
- render a self-contained worker script for each machine;
- materialize inputs from local paths, HTTP(S), or `rclone` remotes;
- run the existing `blockzilla build-archive-v2-hot-blocks` builder;
- publish outputs back to a local path or `rclone` remote.

That makes it immediately useful for manual machines, Hetzner/AWS/GCP boxes
launched by hand, or marketplace hosts where cloud-init/user-data is enough.
Provider API drivers can then be added behind the same plan format.

## Whole-epoch fanout

This is the safest first production mode because the current Archive V2 hot
builder produces a complete epoch directory.

```bash
cargo run -p blockzilla-hivezilla -- plan \
  --start-epoch 900 \
  --end-epoch 907 \
  --worker-count 8 \
  --input-template 'r2:blockzilla-cars/epoch-{epoch}.car.zst' \
  --output-template 'r2:blockzilla-archive-v2/epoch-{epoch}' \
  --plan-out hivezilla-plan.json

cargo run -p blockzilla-hivezilla -- render-worker-script \
  --plan hivezilla-plan.json \
  --worker-id hive-000 \
  --coordinator-url http://nas.local:8787
```

Run the rendered script on the rented machine. It expects a checkout of this
repository at `/opt/blockzilla` by default, plus `aria2c` for resumable HTTP
downloads and `rclone` when the input or output uses a remote reference.
Override paths with:

```bash
HIVEZILLA_REPO_DIR=/path/to/blockzilla \
HIVEZILLA_SCRATCH_DIR=/mnt/nvme/hivezilla \
bash worker-hive-000.sh
```

Worker scripts remove each job's local input and output after publishing by
default. Set `HIVEZILLA_KEEP_JOB_SCRATCH=1` when debugging a failed rented
machine and you want the local files left in place.

Full Old Faithful CARs can be used directly as HTTP inputs. Worker scripts
prefer `aria2c` with resume enabled for these downloads and fall back to
`curl -C -` when `aria2c` is unavailable.

```bash
cargo run -p blockzilla-hivezilla -- plan \
  --start-epoch 900 \
  --end-epoch 907 \
  --worker-count 8 \
  --input-template 'https://files.old-faithful.net/{epoch}/epoch-{epoch}.car' \
  --output-template 'r2:blockzilla-archive-v2/epoch-{epoch}' \
  --plan-out hivezilla-old-faithful-full.json
```

For lower scratch use, stream the Old Faithful CAR directly into a no-registry
compact intermediate instead of downloading the raw CAR first. This writes
`archive-v2-no-registry.wincode`, builds the registry from that compact file,
repackages the final zstd block form, and removes each intermediate as soon as
the next phase no longer needs it.

```bash
cargo run -p blockzilla-hivezilla -- plan \
  --mode old-faithful-stream-no-registry \
  --start-epoch 900 \
  --end-epoch 907 \
  --worker-count 8 \
  --input-template 'https://files.old-faithful.net/{epoch}/epoch-{epoch}.car' \
  --output-template 'r2:blockzilla-archive-v2-staged/epoch-{epoch}' \
  --plan-out hivezilla-stream-no-registry.json
```

## NAS coordinator

The NAS should be the authority for lifecycle decisions. Run the coordinator on
the NAS, let workers publish their output, then have the worker POST a completion
event back to the NAS. The coordinator records the event, optionally pulls the
artifact into local NAS storage, and only then can delete the Hetzner server.

```bash
export HIVEZILLA_COORDINATOR_TOKEN='change-me'

cargo run -p blockzilla-hivezilla -- coordinate \
  --bind 0.0.0.0:8787 \
  --event-dir /volume1/blockzilla/hivezilla/events \
  --artifact-dir /volume1/blockzilla/archive-v2 \
  --destroy-hetzner
```

Worker scripts use the same token env var when notifying the NAS:

```bash
export HIVEZILLA_COORDINATOR_TOKEN='change-me'
export HIVEZILLA_PROVIDER=hetzner
export HIVEZILLA_HETZNER_SERVER_ID=12345678
bash worker-hive-000.sh
```

Use `--dry-run` on the coordinator while testing. It will accept events and log
the artifact pull / `hcloud server delete` actions without executing them.

## Hetzner sizing

Use `estimate-hetzner` before renting machines. The default model uses epoch
920-sized inputs (`390.8 GB` CAR.zst in, `163.6 GB` Archive V2 output) and
assumes one CCX63 takes three hours to build/compress that reference epoch.
Replace `--hours-per-reference-epoch` as soon as we have a real Hetzner
benchmark.

```bash
cargo run -p blockzilla-hivezilla -- estimate-hetzner \
  --epoch-count 1 \
  --target-hours 4 \
  --machine-type ccx63 \
  --hours-per-reference-epoch 3 \
  --builder-scratch-gb-per-epoch 250

cargo run -p blockzilla-hivezilla -- estimate-hetzner \
  --all-history-0-963 \
  --target-hours 24 \
  --machine-type ccx63 \
  --hours-per-reference-epoch 3
```

The estimator also checks local scratch disk. With the default modern epoch
shape, CCX63 fits comfortably; CCX53 is cheaper but too tight unless inputs are
streamed or temp/output headroom is reduced.

For full Old Faithful CARs, set `--input-gb-per-epoch` and
`--reference-input-gb` to the raw CAR size, then use
`--builder-scratch-gb-per-epoch` for the temporary registry/index space created
while the Archive V2 builder runs. If `--include-previous-car` is also needed,
expect CCX63 local disk to be too small without an attached volume or streaming
change.

## Slot-slice mode

`old-faithful-slot-slices` models the faster future path where one epoch can be
split across many machines. Today it renders a CAR-slice step using the existing
`of-slot-ranges` `fetch_slot_slice` helper and then runs the hot-block builder on
that slice.

```bash
cargo run -p blockzilla-hivezilla -- plan \
  --mode old-faithful-slot-slices \
  --start-epoch 900 \
  --end-epoch 900 \
  --worker-count 8 \
  --chunk-slots 54000 \
  --output-template 'r2:blockzilla-archive-v2/epoch-{epoch}/shard-{shard}' \
  --plan-out hivezilla-slot-plan.json
```

The stitcher and range-aware finalization pieces still need to be made explicit
before this mode should be treated as the final production layout. The useful
part now is that the plan, job IDs, output paths, and worker scripts are already
stable.

## Provider roadmap

Hivezilla should grow provider drivers in this order:

1. manual/cloud-init: render scripts and let any provider run them;
2. SSH runner: connect to already-created machines and run assigned jobs;
3. API launchers: Hetzner, AWS EC2 Spot, GCP Spot, Vast.ai, RunPod, Lambda Labs;
4. supervisor: job heartbeats, retries, artifact validation, and cost summary;
5. stitcher: combine sub-epoch shards into canonical Archive V2 epoch outputs.
