//! Native producer/origin share exactly the same frame construction.
use super::*;
use std::{
    fs,
    io::Read,
    path::{Component, Path, PathBuf},
};
pub fn bounded_file(path: &Path, max: usize) -> Result<Vec<u8>> {
    ensure!(
        fs::symlink_metadata(path)?.file_type().is_file(),
        "not_regular_file"
    );
    let f = fs::File::open(path)?;
    ensure!(f.metadata()?.len() <= max as u64, "file_too_large");
    let mut b = Vec::new();
    f.take(max as u64 + 1).read_to_end(&mut b)?;
    ensure!(b.len() <= max, "file_grew");
    Ok(b)
}
pub fn checked_path(root: &Path, path: &Path) -> Result<PathBuf> {
    let relative = path.strip_prefix(root).context("artifact_outside_root")?;
    let mut out = root.to_owned();
    for part in relative.components() {
        let Component::Normal(part) = part else {
            anyhow::bail!("unsafe_artifact_path")
        };
        out.push(part);
        ensure!(
            !fs::symlink_metadata(&out)?.file_type().is_symlink(),
            "symlink_artifact"
        );
    }
    Ok(out)
}
pub fn build(root: &Path, path: &Path, source: &SourceIdentity, event: BankEvent) -> Result<Frame> {
    let path = checked_path(root, path)?;
    let raw = bounded_file(&path.join("DIRECT-AGAVE-COMPACT.v1.json"), MAX_META)?;
    ensure!(
        digest(&raw) == event.payload_sha256,
        "original_marker_digest_mismatch"
    );
    let marker: Value = serde_json::from_slice(&raw)?;
    ensure!(
        SourceIdentity::from_marker(&marker)? == *source,
        "source_changed"
    );
    let rows = marker["produced_blocks"]
        .as_array()
        .context("missing_bank")?;
    ensure!(rows.len() == 1, "one_bank_required");
    let identity = serde_json::to_value(&event.bank)?;
    for k in [
        "slot",
        "bank_id",
        "parent_slot",
        "blockhash",
        "parent_blockhash",
    ] {
        ensure!(rows[0][k] == identity[k], "payload_bank_mismatch");
    }
    let (marker, objects, body) = if event.kind == "block" {
        let files = marker["files"].as_array().context("missing_files")?;
        ensure!(files.len() <= 32, "too_many_files");
        let mut body = Vec::new();
        let mut objects = Vec::new();
        for file in files {
            let name = file["path"].as_str().context("missing_path")?;
            if !FILES.contains(&name) {
                ensure!(
                    matches!(name, "source-events.json" | "source-events.json.zst"),
                    "unsupported_compact_dependency"
                );
                continue;
            }
            let n = file["bytes"].as_u64().context("missing_object_bytes")?;
            ensure!(n <= (MAX_PAYLOAD - body.len()) as u64, "frame_too_large");
            let bytes = bounded_file(&path.join(name), n as usize)?;
            let sha = digest(&bytes);
            ensure!(
                bytes.len() as u64 == n && file["sha256"] == sha,
                "source_object_changed"
            );
            objects.push(Object {
                path: name.into(),
                offset: body.len(),
                bytes: bytes.len(),
                sha256: sha,
            });
            body.extend_from_slice(&bytes);
        }
        (marker, objects, body)
    } else {
        (control_marker(source, &event), vec![], vec![])
    };
    encode(
        Envelope {
            schema: ENVELOPE.into(),
            profile: PROFILE.into(),
            evidence_included: false,
            marker,
            objects,
            event,
        },
        &body,
    )
}
