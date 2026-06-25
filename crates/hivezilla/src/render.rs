use crate::model::{BuildMode, HivezillaPlan, JobSpec};
use anyhow::{Result, anyhow};

#[derive(Debug, Clone)]
pub struct RenderWorkerScriptRequest<'a> {
    pub plan: &'a HivezillaPlan,
    pub worker_id: &'a str,
    pub repo_dir: &'a str,
    pub scratch_dir: &'a str,
    pub coordinator_url: Option<&'a str>,
    pub coordinator_token_env: &'a str,
}

pub fn render_worker_script(request: RenderWorkerScriptRequest<'_>) -> Result<String> {
    let jobs = request.plan.jobs_for_worker(request.worker_id);
    if jobs.is_empty() {
        return Err(anyhow!(
            "worker {} has no jobs in plan {}",
            request.worker_id,
            request.plan.run_id
        ));
    }

    let needs_slot_slice = jobs
        .iter()
        .any(|job| job.build_mode == BuildMode::OldFaithfulSlotSlices);
    let mut out = String::new();
    out.push_str("#!/usr/bin/env bash\n");
    out.push_str("set -euo pipefail\n\n");
    out.push_str(&format!(
        "REPO_DIR=\"${{HIVEZILLA_REPO_DIR:-{}}}\"\n",
        shell_double_quoted_default(request.repo_dir)
    ));
    out.push_str(&format!(
        "SCRATCH_DIR=\"${{HIVEZILLA_SCRATCH_DIR:-{}}}\"\n",
        shell_double_quoted_default(request.scratch_dir)
    ));
    if let Some(coordinator_url) = request.coordinator_url {
        out.push_str(&format!(
            "HIVEZILLA_COORDINATOR_URL=\"${{HIVEZILLA_COORDINATOR_URL:-{}}}\"\n",
            shell_double_quoted_default(coordinator_url)
        ));
    }
    out.push_str(&format!(
        "HIVEZILLA_COORDINATOR_TOKEN_ENV=\"{}\"\n",
        shell_double_quoted_default(request.coordinator_token_env)
    ));
    out.push_str("BLOCKZILLA_BIN=\"${BLOCKZILLA_BIN:-$REPO_DIR/target/release/blockzilla}\"\n");
    if needs_slot_slice {
        out.push_str(
            "FETCH_SLOT_SLICE_BIN=\"${FETCH_SLOT_SLICE_BIN:-$REPO_DIR/target/release/fetch_slot_slice}\"\n",
        );
    }
    out.push('\n');
    out.push_str("mkdir -p \"$SCRATCH_DIR\"\n");
    out.push_str("cd \"$REPO_DIR\"\n\n");
    out.push_str("if [[ ! -x \"$BLOCKZILLA_BIN\" ]]; then\n");
    out.push_str("  cargo build --release -p blockzilla\n");
    out.push_str("fi\n");
    if needs_slot_slice {
        out.push_str("if [[ ! -x \"$FETCH_SLOT_SLICE_BIN\" ]]; then\n");
        out.push_str("  cargo build --release -p of-slot-ranges --bin fetch_slot_slice\n");
        out.push_str("fi\n");
    }
    out.push('\n');
    out.push_str(MATERIALIZE_FUNCTIONS);
    out.push('\n');
    out.push_str(COORDINATOR_FUNCTIONS);
    out.push('\n');

    out.push_str(&format!(
        "echo \"hivezilla plan={} worker={} jobs={}\"\n",
        shell_double_quoted_default(&request.plan.run_id),
        shell_double_quoted_default(request.worker_id),
        jobs.len()
    ));

    for job in jobs {
        render_job(&mut out, request.plan, request.worker_id, job)?;
    }

    out.push_str("\necho \"hivezilla worker complete\"\n");
    Ok(out)
}

fn render_job(
    out: &mut String,
    plan: &HivezillaPlan,
    worker_id: &str,
    job: &JobSpec,
) -> Result<()> {
    let local_car = local_car_path(job);
    let local_previous_car = job
        .previous_car_uri
        .as_deref()
        .map(|uri| {
            format!(
                "$SCRATCH_DIR/input/{}-previous.{}",
                job.id,
                car_extension_for(uri)
            )
        })
        .unwrap_or_else(|| format!("$SCRATCH_DIR/input/{}-previous.car", job.id));
    let local_out = format!("$SCRATCH_DIR/output/{}", job.id);

    out.push_str("\n");
    out.push_str(&format!(
        "echo \"hivezilla job={} epoch={} slots={}..{} mode={}\"\n",
        shell_double_quoted_default(&job.id),
        job.epoch,
        job.slot_start,
        job.slot_end,
        job.build_mode.as_str()
    ));
    out.push_str(&format!(
        "notify_coordinator 'running' {} {} {} {} {} {} {} {} {}\n",
        shell_quote(&plan.run_id),
        shell_quote(worker_id),
        shell_quote(&job.id),
        job.epoch,
        job.shard_index,
        job.shard_count,
        job.slot_start,
        job.slot_end,
        shell_quote(&job.output_uri)
    ));
    out.push_str(&format!("rm -rf {}\n", shell_expand_quote(&local_out)));
    out.push_str(&format!(
        "mkdir -p {}\n",
        shell_expand_quote("$SCRATCH_DIR/input")
    ));

    match job.build_mode {
        BuildMode::WholeEpoch | BuildMode::OldFaithfulSlotSlices => {
            render_hot_blocks_job(out, plan, job, &local_car, &local_previous_car, &local_out);
        }
        BuildMode::OldFaithfulStreamNoRegistry => {
            render_stream_no_registry_job(out, job, &local_previous_car, &local_out);
        }
    }

    out.push_str(&format!(
        "publish_output {} {}\n",
        shell_expand_quote(&local_out),
        shell_quote(&job.output_uri)
    ));
    out.push_str(&format!(
        "notify_coordinator 'success' {} {} {} {} {} {} {} {} {}\n",
        shell_quote(&plan.run_id),
        shell_quote(worker_id),
        shell_quote(&job.id),
        job.epoch,
        job.shard_index,
        job.shard_count,
        job.slot_start,
        job.slot_end,
        shell_quote(&job.output_uri)
    ));
    out.push_str("if [[ \"${HIVEZILLA_KEEP_JOB_SCRATCH:-0}\" != \"1\" ]]; then\n");
    out.push_str(&format!(
        "  rm -rf {} {}\n",
        shell_expand_quote(&local_car),
        shell_expand_quote(&local_out)
    ));
    if job.previous_car_uri.is_some() {
        out.push_str(&format!(
            "  rm -rf {}\n",
            shell_expand_quote(&local_previous_car)
        ));
    }
    out.push_str("fi\n");

    Ok(())
}

fn render_hot_blocks_job(
    out: &mut String,
    plan: &HivezillaPlan,
    job: &JobSpec,
    local_car: &str,
    local_previous_car: &str,
    local_out: &str,
) {
    match job.build_mode {
        BuildMode::WholeEpoch => {
            out.push_str(&format!(
                "materialize_source {} {}\n",
                shell_quote(&job.source_uri),
                shell_expand_quote(local_car)
            ));
        }
        BuildMode::OldFaithfulSlotSlices => {
            let base_url = plan
                .strategy
                .old_faithful_base_url
                .as_deref()
                .unwrap_or("https://files.old-faithful.net");
            out.push_str(&format!(
                "\"$FETCH_SLOT_SLICE_BIN\" --epoch {} --start-slot {} --end-slot {} --output {} --base-url {}\n",
                job.epoch,
                job.slot_start,
                job.slot_end,
                shell_expand_quote(local_car),
                shell_quote(base_url)
            ));
        }
        BuildMode::OldFaithfulStreamNoRegistry => unreachable!(),
    }

    if let Some(previous_car_uri) = job.previous_car_uri.as_deref() {
        out.push_str(&format!(
            "materialize_source {} {}\n",
            shell_quote(previous_car_uri),
            shell_expand_quote(local_previous_car)
        ));
    }

    out.push_str(&format!(
        "\"$BLOCKZILLA_BIN\" build-archive-v2-hot-blocks {} {} --level {}",
        shell_expand_quote(local_car),
        shell_expand_quote(local_out),
        job.compression_level
    ));
    if job.previous_car_uri.is_some() {
        out.push_str(&format!(
            " --previous-car {}",
            shell_expand_quote(local_previous_car)
        ));
    }
    out.push('\n');
}

fn render_stream_no_registry_job(
    out: &mut String,
    job: &JobSpec,
    local_previous_car: &str,
    local_out: &str,
) {
    let no_registry_out = format!("$SCRATCH_DIR/no-registry/{}", job.id);
    let optimized_out = format!("$SCRATCH_DIR/optimized/{}", job.id);
    out.push_str(&format!(
        "rm -rf {} {}\n",
        shell_expand_quote(&no_registry_out),
        shell_expand_quote(&optimized_out)
    ));
    out.push_str(&format!(
        "\"$BLOCKZILLA_BIN\" build-archive-v2-no-registry-from-url {} {}\n",
        shell_quote(&job.source_uri),
        shell_expand_quote(&no_registry_out)
    ));

    if let Some(previous_car_uri) = job.previous_car_uri.as_deref() {
        out.push_str(&format!(
            "materialize_source {} {}\n",
            shell_quote(previous_car_uri),
            shell_expand_quote(local_previous_car)
        ));
    }

    out.push_str(&format!(
        "\"$BLOCKZILLA_BIN\" optimize-archive-v2-no-registry {}/archive-v2-no-registry.wincode {}",
        shell_expand_quote(&no_registry_out),
        shell_expand_quote(&optimized_out)
    ));
    if job.previous_car_uri.is_some() {
        out.push_str(&format!(
            " --previous-car {}",
            shell_expand_quote(local_previous_car)
        ));
    }
    out.push('\n');
    out.push_str(&format!(
        "rm -rf {}\n",
        shell_expand_quote(&no_registry_out)
    ));
    out.push_str(&format!(
        "\"$BLOCKZILLA_BIN\" repack-archive-v2-zstd-blocks {}/archive-v2.wincode {} --level {}\n",
        shell_expand_quote(&optimized_out),
        shell_expand_quote(local_out),
        job.compression_level
    ));
    out.push_str(&format!(
        "copy_archive_v2_sidecars {} {}\n",
        shell_expand_quote(&optimized_out),
        shell_expand_quote(local_out)
    ));
    out.push_str(&format!("rm -rf {}\n", shell_expand_quote(&optimized_out)));
}

const MATERIALIZE_FUNCTIONS: &str = r#"is_rclone_ref() {
  case "$1" in
    http://*|https://*) return 1 ;;
    *://*|*:*) return 0 ;;
    *) return 1 ;;
  esac
}

materialize_source() {
  local source="$1"
  local target="$2"
  mkdir -p "$(dirname "$target")"
  case "$source" in
    http://*|https://*)
      download_http_source "$source" "$target"
      ;;
    *)
      if is_rclone_ref "$source"; then
        rclone copyto "$source" "$target"
      else
        cp "$source" "$target"
      fi
      ;;
  esac
}

download_http_source() {
  local source="$1"
  local target="$2"
  local target_dir
  local target_name
  target_dir="$(dirname "$target")"
  target_name="$(basename "$target")"
  mkdir -p "$target_dir"

  if command -v aria2c >/dev/null 2>&1; then
    aria2c \
      --continue=true \
      --allow-overwrite=true \
      --auto-file-renaming=false \
      --file-allocation="${HIVEZILLA_ARIA2_FILE_ALLOCATION:-none}" \
      --max-connection-per-server="${HIVEZILLA_ARIA2_CONNECTIONS:-8}" \
      --split="${HIVEZILLA_ARIA2_SPLIT:-8}" \
      --min-split-size="${HIVEZILLA_ARIA2_MIN_SPLIT_SIZE:-64M}" \
      --summary-interval="${HIVEZILLA_ARIA2_SUMMARY_INTERVAL:-60}" \
      --dir="$target_dir" \
      --out="$target_name" \
      "$source"
  else
    curl -fL --continue-at - "$source" -o "$target"
  fi
}

publish_output() {
  local source_dir="$1"
  local destination="$2"
  if is_rclone_ref "$destination"; then
    rclone copy "$source_dir/" "$destination/"
  else
    mkdir -p "$destination"
    rsync -a "$source_dir/" "$destination/"
  fi
}

copy_archive_v2_sidecars() {
  local source_dir="$1"
  local destination_dir="$2"
  mkdir -p "$destination_dir"
  local sidecar
  for sidecar in registry.bin registry_counts.bin blockhash_registry.bin poh.wincode previous_blockhash_tail.bin; do
    if [[ -f "$source_dir/$sidecar" ]]; then
      cp "$source_dir/$sidecar" "$destination_dir/$sidecar"
    fi
  done
}
"#;

const COORDINATOR_FUNCTIONS: &str = r#"notify_coordinator() {
  local status="$1"
  local run_id="$2"
  local worker_id="$3"
  local job_id="$4"
  local epoch="$5"
  local shard_index="$6"
  local shard_count="$7"
  local slot_start="$8"
  local slot_end="$9"
  local output_uri="${10}"

  if [[ -z "${HIVEZILLA_COORDINATOR_URL:-}" ]]; then
    return 0
  fi

  local event_path="$SCRATCH_DIR/event-${job_id}.json"
  python3 - "$event_path" "$status" "$run_id" "$worker_id" "$job_id" "$epoch" "$shard_index" "$shard_count" "$slot_start" "$slot_end" "$output_uri" <<'PY'
import json
import os
import sys
import time

event_path, status, run_id, worker_id, job_id, epoch, shard_index, shard_count, slot_start, slot_end, output_uri = sys.argv[1:]
event = {
    "run_id": run_id,
    "worker_id": worker_id,
    "job_id": job_id,
    "status": status,
    "epoch": int(epoch),
    "shard_index": int(shard_index),
    "shard_count": int(shard_count),
    "slot_start": int(slot_start),
    "slot_end": int(slot_end),
    "output_uri": output_uri,
    "provider": os.environ.get("HIVEZILLA_PROVIDER"),
    "server_id": os.environ.get("HIVEZILLA_HETZNER_SERVER_ID"),
    "server_name": os.environ.get("HIVEZILLA_HETZNER_SERVER_NAME"),
    "finished_unix_secs": int(time.time()),
}
with open(event_path, "w", encoding="utf-8") as f:
    json.dump(event, f, separators=(",", ":"))
    f.write("\n")
PY

  local -a curl_args=(-fsS -X POST -H "content-type: application/json")
  local token_value=""
  if [[ -n "${HIVEZILLA_COORDINATOR_TOKEN_ENV:-}" ]]; then
    token_value="${!HIVEZILLA_COORDINATOR_TOKEN_ENV:-}"
  fi
  if [[ -n "$token_value" ]]; then
    curl_args+=(-H "authorization: Bearer $token_value")
  fi
  curl "${curl_args[@]}" --data-binary "@$event_path" "${HIVEZILLA_COORDINATOR_URL%/}/job-event"
}
"#;

fn shell_double_quoted_default(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('$', "\\$")
        .replace('`', "\\`")
}

fn shell_expand_quote(value: &str) -> String {
    format!("\"{}\"", value.replace('\\', "\\\\").replace('"', "\\\""))
}

fn shell_quote(value: &str) -> String {
    if value.is_empty() {
        return "''".to_string();
    }
    let mut quoted = String::from("'");
    for ch in value.chars() {
        if ch == '\'' {
            quoted.push_str("'\\''");
        } else {
            quoted.push(ch);
        }
    }
    quoted.push('\'');
    quoted
}

fn local_car_path(job: &JobSpec) -> String {
    let extension = match job.build_mode {
        BuildMode::WholeEpoch | BuildMode::OldFaithfulStreamNoRegistry => {
            car_extension_for(&job.source_uri)
        }
        BuildMode::OldFaithfulSlotSlices => "car",
    };
    format!("$SCRATCH_DIR/input/{}.{}", job.id, extension)
}

fn car_extension_for(uri: &str) -> &'static str {
    let lower = uri.to_ascii_lowercase();
    if lower.ends_with(".car.zst") {
        "car.zst"
    } else if lower.ends_with(".zst") {
        "zst"
    } else {
        "car"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{BuildMode, PlanRequest, build_plan};

    #[test]
    fn whole_epoch_script_uses_existing_blockzilla_builder() {
        let plan = build_plan(PlanRequest {
            epochs: vec![900],
            input_template: "r2:cars/epoch-{epoch}.car.zst".to_string(),
            output_template: "r2:archives/epoch-{epoch}".to_string(),
            ..PlanRequest::default()
        })
        .unwrap();

        let script = render_worker_script(RenderWorkerScriptRequest {
            plan: &plan,
            worker_id: "hive-000",
            repo_dir: "/repo",
            scratch_dir: "/scratch",
            coordinator_url: Some("http://nas.local:8787"),
            coordinator_token_env: "HIVEZILLA_COORDINATOR_TOKEN",
        })
        .unwrap();

        assert!(script.contains(
            "materialize_source 'r2:cars/epoch-900.car.zst' \"$SCRATCH_DIR/input/epoch-900-shard-000.car.zst\""
        ));
        assert!(script.contains("aria2c"));
        assert!(script.contains("curl -fL --continue-at -"));
        assert!(script.contains("build-archive-v2-hot-blocks"));
        assert!(script.contains(
            "HIVEZILLA_COORDINATOR_URL=\"${HIVEZILLA_COORDINATOR_URL:-http://nas.local:8787}\""
        ));
        assert!(script.contains("notify_coordinator 'success' 'hivezilla-"));
        assert!(script.contains(
            "publish_output \"$SCRATCH_DIR/output/epoch-900-shard-000\" 'r2:archives/epoch-900'"
        ));
    }

    #[test]
    fn slot_slice_script_uses_fetch_slot_slice() {
        let plan = build_plan(PlanRequest {
            epochs: vec![900],
            build_mode: BuildMode::OldFaithfulSlotSlices,
            worker_count: 8,
            chunk_slots: Some(54_000),
            ..PlanRequest::default()
        })
        .unwrap();

        let script = render_worker_script(RenderWorkerScriptRequest {
            plan: &plan,
            worker_id: "hive-000",
            repo_dir: "/repo",
            scratch_dir: "/scratch",
            coordinator_url: None,
            coordinator_token_env: "HIVEZILLA_COORDINATOR_TOKEN",
        })
        .unwrap();

        assert!(script.contains("cargo build --release -p of-slot-ranges --bin fetch_slot_slice"));
        assert!(script.contains("--epoch 900 --start-slot 388800000 --end-slot 388853999"));
    }

    #[test]
    fn stream_no_registry_script_avoids_local_current_car() {
        let plan = build_plan(PlanRequest {
            epochs: vec![900],
            build_mode: BuildMode::OldFaithfulStreamNoRegistry,
            input_template: "https://files.old-faithful.net/{epoch}/epoch-{epoch}.car".to_string(),
            output_template: "r2:archives/epoch-{epoch}".to_string(),
            ..PlanRequest::default()
        })
        .unwrap();

        let script = render_worker_script(RenderWorkerScriptRequest {
            plan: &plan,
            worker_id: "hive-000",
            repo_dir: "/repo",
            scratch_dir: "/scratch",
            coordinator_url: None,
            coordinator_token_env: "HIVEZILLA_COORDINATOR_TOKEN",
        })
        .unwrap();

        assert!(script.contains("build-archive-v2-no-registry-from-url"));
        assert!(script.contains("optimize-archive-v2-no-registry"));
        assert!(script.contains("repack-archive-v2-zstd-blocks"));
        assert!(script.contains("copy_archive_v2_sidecars"));
        assert!(
            !script
                .contains("materialize_source 'https://files.old-faithful.net/900/epoch-900.car'")
        );
    }
}
