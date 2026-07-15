# NAS live-root cleanup

`scripts/nas-live-root-cleanup.py` removes UI clutter without deleting data. It only
same-filesystem-renames a narrow allow-list of non-production directories into a quarantine
outside the live scan root. Production captures, mixed-epoch captures, repair bundles, and every
absolute source/input path referenced by a `REPAIR-REQUIRED.json` marker are retained.
The rename removes inventory clutter but does not reclaim disk space; quarantine retention can be
reviewed separately after the compacted archive and its receipts exist.

The tool fails closed when a candidate:

- contains live-producer artifacts;
- is referenced by a published repair marker;
- is open, appears in a running process command line, or is a process working directory;
- changed within the configured age window;
- crosses a mount, exceeds the inode-audit bound, or cannot be fully audited through `/proc`; or
- cannot be moved with an atomic same-filesystem rename.

It never calls `unlink` on capture content. Apply mode moves directories into a hidden transaction
folder, verifies their metadata/inodes after each rename, then publishes the quarantine directory
and its mode-`0600` JSON receipt with one final rename. A visible transaction journal remains in
the hidden staging folder if an interrupted operation needs review.

## Epoch 1000 NAS invocation

Copy the reviewed tool to the NAS (SSH is on port 22):

```sh
scp -P 22 scripts/nas-live-root-cleanup.py \
  ach@192.168.1.45:/home/ach/dev/blockzilla-pipeline/tools/nas-live-root-cleanup.py
ssh -p 22 ach@192.168.1.45 \
  'chmod 700 /home/ach/dev/blockzilla-pipeline/tools/nas-live-root-cleanup.py && \
   install -d -m 700 /volume1/@home/ach/dev/blockzilla-pipeline/live-quarantine'
```

Run the complete safety audit and write an atomic dry-run receipt. Root is intentional: an
incomplete `/proc` audit is rejected rather than overridden.

```sh
ssh -t -p 22 ach@192.168.1.45
TOOL=/home/ach/dev/blockzilla-pipeline/tools/nas-live-root-cleanup.py
LIVE=/volume1/@home/ach/dev/blockzilla-live
QUARANTINE=/volume1/@home/ach/dev/blockzilla-pipeline/live-quarantine
RUN=epoch1000-live-ui-cleanup-20260714

sudo python3 "$TOOL" \
  --mode dry-run \
  --run-id "$RUN" \
  --live-root "$LIVE" \
  --archive-root "$QUARANTINE" \
  --candidate .recovery-failed \
  --candidate codex-bench-runs-1k-20260710T192408Z \
  --candidate codex-samples
```

The command prints the exact receipt path. Review that file with `python3 -m json.tool`. It should
show exactly three `planned_quarantine` decisions, `global_scope_complete: true`, no process
matches, no production signals, and no repair-marker references.

Apply the independently repeated safety checks and atomic renames with the same run id:

```sh
sudo python3 "$TOOL" \
  --mode apply \
  --run-id "$RUN" \
  --live-root "$LIVE" \
  --archive-root "$QUARANTINE" \
  --candidate .recovery-failed \
  --candidate codex-bench-runs-1k-20260710T192408Z \
  --candidate codex-samples
```

The final receipt is
`/volume1/@home/ach/dev/blockzilla-pipeline/live-quarantine/$RUN/receipt.json`; retained bytes are
under `$RUN/items/`. Refreshing Hivezilla after the rename removes those three non-production rows
from its live-root inventory.

This tool intentionally does **not** move either epoch-1000 source capture, the published
epoch-1000 repair bundle, the mixed epoch-1000/1001 tail, either closed epoch-1001 fragment, or the
active post-upgrade epoch-1001 capture. Production-fragment consolidation needs a separate sealed
epoch-1001 union/coverage receipt first.
