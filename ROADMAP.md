# Blockzilla roadmap

## Public reference milestone

- Specify every Archive V2 file and versioned record needed for independent readers.
- Keep the complete workspace green on Linux and macOS.
- Provide a deterministic fixture build and validation command.
- Publish a read-only `getBlock` demo backed by Archive V2.
- Document corruption detection, repair boundaries, and provenance.

## Format stabilization

- Separate the large builder implementation into format, registry, block, sidecar, and repair modules.
- Add compatibility fixtures for each published record version.
- Define the pre-1.0 migration policy and the criteria for Archive V2 stability.

## Ecosystem

- Publish reusable reader crates with complete package metadata.
- Provide examples for sequential scans, random slot access, and derived indexes.
- Reproduce benchmark results through scripts and versioned small corpora.

Operational fleet management and provider-specific deployment remain outside this repository.
