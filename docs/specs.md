# Blockzilla v1

Blockzilla is a Solana archive node format focused on long-term storage, validation, and fast indexed reads.

This document describes the archive goals first, then the intended file layout. The current implementation is still work in progress, but the target is stable:

`CAR -> of-car-reader -> optimized archive -> optimized reader -> CAR`

For a lossless validation path, the reconstructed CAR should have the same hash as the source CAR.

For the current block-read optimization proposal, see [Archive V2 Hot Block Format](archive-v2-hot-block-format.md).

For the live archive production direction, see [Live Archive Producer](live-archive-producer.md).

## Design Goals

- Optimize archive size for long-term storage.
- Keep the format streamable and auditable.
- Prefer zero-copy and single-pass decoding where possible.
- Split replay-verifiable historical data from runtime / metadata-heavy data.
- Keep enough reconstruction metadata to rebuild the original CAR exactly for validation.
- Use compact encodings such as postcard and varints where that does not break streaming.

## Archive Model

For each epoch we want to split data into three categories:

1. Historical data
All data needed to replay or verify the archive content from hashes, signatures, transactions, PoH, and block structure.

2. Runtime / metadata data
Execution-derived data that is useful for indexing and filtering but is not required to prove the historical archive itself.

3. Reconstruction data
The minimal extra information needed to rebuild the original CAR byte stream layout from the optimized files.

The important point is that reconstruction metadata should live in its own file instead of being mixed into block or runtime data. This keeps the historical/runtime split clean while still allowing a strict round-trip validation mode.

## Historical vs Runtime

Historical data should contain:

- transactions payload data
- PoH entries
- block shredding information
- rewards if we decide they are part of the verifiable block archive
- standalone dataframe continuation nodes
- subsets and epoch topology
- lookup table data if needed for replay or deterministic extraction

Runtime / metadata data should contain:

- transaction metadata
- block runtime metadata
- inner instructions
- logs
- SOL balances
- token balances
- loaded addresses

Some fields may be duplicated or promoted if that makes filtering significantly better. For example, loaded addresses are runtime-derived, but they may still be included in block-oriented views because they are important for filtering.

## Reconstruction File

The spec needs a dedicated reconstruction file.

This file should contain only what is required to rebuild the original CAR layout, for example:

- exact CAR header bytes
- original node order
- node kind
- node CID
- original CAR entry index
- original CAR byte offset
- any other framing information that is not derivable from the historical/runtime files

The reconstruction file is not the source of truth for the node payload itself. It is the source of truth for how the payloads must be put back together into the original CAR stream.

## Blockzilla Folder Structure

Cache contains compressed Old Faithful CAR files.

`blockzilla-v1/` contains the optimized epoch archives.

```text
cache/
  epoch-0.car.zst
  ...
  epoch-800.car.zst

blockzilla-v1/
  epoch-0/
    epoch-0-registry.bin
    epoch-0-block-index.bin
    epoch-0-block.bin
    epoch-0-runtime.bin
    epoch-0-reconstruct.bin
  ...
  epoch-800/
    epoch-800-registry.bin
    epoch-800-block-index.bin
    epoch-800-block.bin
    epoch-800-runtime.bin
    epoch-800-reconstruct.bin
```

For the current first-pass prototype, the split is:

- `historical.bin.zst`
- `metadata-unoptimized.bin.zst`
- `reconstruct.bin.zst`
- `block-index.bin`
- `registry.bin`
- `blockhash_registry.bin`

## File Responsibilities

`registry.bin`

- pubkeys sorted by usage frequency in the epoch
- used as a compact id-to-pubkey map
- for epoch 0, includes genesis account pubkeys plus genesis owner and builtin program pubkeys

`blockhash_registry.bin`

- raw 32-byte blockhashes addressable by compact blockhash id
- for epoch 0, entry `0` is the genesis hash; real block hashes start at id `1`

`block-index.bin`

- slot / block level metadata for fast filtering
- blockhash
- offsets into data files
- transaction signatures
- loaded addresses if useful for filtering
- skipped-slot information

`block.bin`

- replay-verifiable historical archive data
- transaction payload data
- instructions
- entry grouping
- PoH information
- shredding information
- subset / epoch structural data
- in Archive V2 epoch 0, a genesis record stores the full initial account set and genesis config before block records

`runtime.bin`

- runtime-only data
- transaction metadata
- inner instructions
- logs
- balances
- execution metadata

`reconstruct.bin`

- CAR reconstruction metadata only
- exact header bytes and original ordering metadata
- lets us validate that optimized extraction can reproduce the original CAR hash

`metadata-unoptimized.bin`

- first-pass storage for metadata that we do not optimize yet
- should preserve the raw archive metadata representation so a later pass can optimize it without rereading the original CAR

## Indexing Notes

Instruction filtering is a primary goal.

Block-oriented historical data should make it easy to:

- filter all instructions involving a program
- map matching instructions back to transaction and block context
- optionally filter matching inner instructions as a second pass

A two-pass strategy for some runtime-heavy features is acceptable if it preserves simpler streaming in the main archive.

## Project Structure

`of-car-reader`

- contains only CAR parsing and reading logic
- should be reusable by other projects
- should be easy to audit and compare against other implementations
- focuses on correctness, streaming, and low-copy decoding

We currently have multiple reference implementations:

- Triton / Yellowstone Go implementation
- Jetstream Rust implementation
- Blockzilla v0 implementation

For correctness we should cross-check against Triton and Jetstream. For speed, Blockzilla-specific code should be preferred once behavior is verified.

## RPC Benchmark Timing

`getBlock` benchmarks should use the same slot plan when comparing the Cloudflare Worker against an external RPC provider such as Triton. The Worker run should measure our endpoint directly and should not add client-side rate-limit sleeps unless we are explicitly testing Worker overload behavior.

External RPC runs should respect provider limits. If a provider returns HTTP 429 or a JSON-RPC rate-limit error, the benchmark client may sleep and retry that same logical request. Reports must keep that time visible instead of hiding it inside a generic latency number.

The global benchmark report must include runner wall-clock time. Global and per-epoch summaries must include:

- logical requests attempted, OK responses, and errors
- total HTTP attempts, including retry attempts
- summed logical request elapsed time
- rate-limit events and retry attempts
- seconds spent waiting before rate-limit retries
- seconds spent receiving rate-limited responses
- total rate-limit waste seconds, defined as retry sleeps plus rate-limited response time

`elapsed_s` is the logical request duration. When rate-limit retries are enabled, it includes the failed rate-limited attempt, the retry sleep, and the final response. `final_attempt_elapsed_s` is the last HTTP attempt only. Latency percentiles use `elapsed_s`, so provider rate-limit stalls remain part of the timing comparison.

`blockzilla-format`

- defines the compact archive format
- provides read/write APIs
- should mostly contain data structures and IO codecs
- should focus on single-core streaming reads

`blockzilla`

- is the main user-facing Archive V2 CLI
- consumes CAR files via `of-car-reader`
- produces optimized Blockzilla archives
- reads the optimized archive
- streams decoded data for analytics and extraction
- should support commands using either `of-car-reader` directly or the optimized archive
- should output counters such as blocks, transactions, instructions, inner instructions, and TPS

`of-archive-importer`

- is legacy compatibility for older compact/archive conversion scripts
- should not gain new token/index benchmark commands
- can be removed once NAS scripts and docs no longer depend on it
