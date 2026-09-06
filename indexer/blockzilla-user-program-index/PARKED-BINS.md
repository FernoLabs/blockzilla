# Parked binary: firewatch-index-controller

The source is retained at `src/bin/firewatch-index-controller.rs` in
`blockzilla-user-program-index`. It is not declared in `Cargo.toml`, so it is
not built, including with `developer-tools`.

`IndexManifest` moved to `blockzilla-user-program-index` during the
`codex/sample-archive-benchmark` merge and no longer carries
`archive_wire_profile`, which this controller compares in two places to refuse
an index built against a different Archive V2 hot-message grammar. The field's
type, `ArchiveV2WireProfile`, now exists only in `blockzilla-read-sdk-legacy`,
and pulling that into `blockzilla-user-program-index` would tie the new index
crate to the reader we are retiring.

Restore it with the V2 freeze, when the grammar becomes single-valued and the
comparison is no longer needed. Until then its behavior remains unported; package imports follow the consolidated indexer.
