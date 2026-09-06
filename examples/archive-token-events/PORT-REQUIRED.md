# Not in the workspace

This example is the only consumer of `blockzilla-archive-sdk`, which was removed
during the `codex/sample-archive-benchmark` merge.

`blockzilla-archive-sdk` was the first-generation unified facade (`4a0fa59b`),
superseded three days later by the three per-format read SDKs (`b3c5765b`) and
never touched again. It does not compile against the current
`blockzilla-model` (`SourceVerification::PublishedManifest` no longer
exists), it exposes no parallel scan path, and it defined a second public
`ArchiveIoSnapshot` with seven fields against the canonical eight, silently
dropping `incomplete_body_retries`.

To restore this example, port it onto `blockzilla-compact-v2-reader::archive` and
re-add it to the workspace members list. Its workload sinks already come from
`blockzilla-example-workloads`, so only source setup needs rewriting.
