# Blockzilla SPYX transaction dump

This small program makes an immutable raw dump of all transactions that can touch the SPYX mint
or a discovered SPYX token account. Its defaults are:

- mint: `XsoCS1TfEyfFhfvj8EtZ528L3CaKBDBRqRapnBbDF2W`
- InitializeMint slot: `346066298`
- InitializeMint transaction: `51QCqbftjH2JdVScV8MUPEEGTTCBBwRdFLcJnhR3e7gVr5PGcJaL6HTh4hpxpJC6sjXGNafCW8eZEZxRuScDs49R`
- InitializeMint transaction index: `1509`
- decode workers: `12`

The earlier `2xCK...` transaction at slot `346330505` is a later `MintTo`. It is not the mint
creation anchor and must not be used as the scan floor.

## Raw extraction

The extractor can run in three modes.

- **default mode**: discovery runs for all target epochs. Raw copy then runs for all target epochs.
- **epoch-barrier mode**: discovery and raw copy run per epoch, with a barrier between them. This
  mode reads and decompresses the epoch twice. Enable it with `--epoch-barrier`.
- **single-read batch mode** (recommended for a new full extraction): discovery runs in parallel for
  one retained block batch. The coordinator merges new accounts in ledger order. Raw copy then runs
  in parallel against the same decompressed block bytes. Each source batch is read once, and each
  block is decompressed once. The mode rejects an owned legacy-block fallback. Enable it with
  `--single-read-batches`.

Single-read mode has an experimental `--single-read-match-hints` option. Pass A records one exact,
storage-ordered byte for each transaction that already matches the pre-merge target table. After
the batch merge, Pass B writes positive hints directly. In a clean batch, it skips negative hints
without a second account decode. In a batch that adds an account, it runs the normal exact matcher
for each negative hint so that the creation floor stays exact. The reusable hint arena is limited
to 64 MiB.

Pass A finds successful SPL Token and Token-2022 `InitializeAccount`, `InitializeAccount2`, and
`InitializeAccount3` instructions. It checks outer and inner instructions. It does not use token
balances, account close operations, or an account-state tracker. It keeps the first successful
creation position for each account and makes one fixed, sorted SPYX account list.

Pass A uses an exact message visitor and an exact metadata visitor. It does not load the full
pubkey registry. It resolves only discovered account IDs. Failed transactions cannot add an
account. The supplied mint signature must occur exactly once as the first signature of a
transaction at the mint slot. To find that transaction, Pass A reads only the first signature of
each transaction in that one slot. It reads no signatures from other slots and writes no signature
bytes to the raw dump.

Before Pass B, the program maps the applicable account list through the epoch `registry.mphf`.
Each positive MPHF result is checked against the exact 32-byte row in `registry.bin`. The program
writes a deterministic `account-ids.wincode` file for each epoch. Each epoch file contains the mint
and only the token accounts whose first creation is in that epoch or an earlier epoch. Thus, an
early epoch file remains valid after later epochs add accounts to the final global list.

In single-read batch mode, Pass A completes for all blocks in the batch before Pass B starts for
that batch. Pass A results are merged in source row order. A newly found account is visible to Pass
B for the batch, but its creation coordinate prevents an earlier transaction in the same batch
from matching it.

Pass B checks the complete transaction account list:

- all static message accounts;
- all loaded writable accounts;
- all loaded read-only accounts.

Address lookup table descriptor keys are not transaction accounts and are not matched. A listed
account is sufficient; an instruction-use check is not done. Thus, the raw dump is a safe superset.
Failed transactions are included when their account list matches. A token account is eligible at
its first successful creation transaction and after that transaction. Later close or address-reuse
events do not remove it from the historical account set.
The target mint is eligible at its verified `InitializeMint` position and after that position. A
token account is eligible at its own successful creation position. Thus, Pass B includes a token
account creation after `InitializeMint` and every later transaction that lists that account.

Pass B copies the exact source message bytes and exact source metadata bytes. It does not remap any
ID. It does not copy signatures. Each raw record stores the source signature ordinal and count for
the later consolidation phase. It also stores the source epoch and generation identity, block and
transaction position, row flags, slot, parent slot, raw blockhash ID values, block time, block
height, and source block transaction count. It does not store block rewards.

Run a trusted-local extraction:

```sh
cargo run --release -p blockzilla-token-transaction-dump -- \
  extract /volume1/blockzilla/archive /volume1/blockzilla/spyx-raw \
  --cluster-id mainnet-beta \
  --slots-per-epoch 432000 \
  --wire-profile post \
  --workers 12 \
  --last-epoch 1018 \
  --resume
```

Run a new trusted-local extraction in single-read batch mode:

```sh
cargo run --release -p blockzilla-token-transaction-dump -- \
  extract /volume1/blockzilla/archive /volume1/blockzilla/spyx-single-read \
  --cluster-id mainnet-beta \
  --slots-per-epoch 432000 \
  --wire-profile post \
  --workers 12 \
  --last-epoch 1018 \
  --single-read-batches \
  --single-read-match-hints \
  --resume
```

Run the trusted-local extraction in epoch-barrier mode:

```sh
cargo run --release -p blockzilla-token-transaction-dump -- \
  extract /volume1/blockzilla/archive /volume1/blockzilla/spyx-raw-barrier \
  --cluster-id mainnet-beta \
  --slots-per-epoch 432000 \
  --wire-profile post \
  --workers 12 \
  --last-epoch 1018 \
  --epoch-barrier
```

`--single-read-batches` and `--epoch-barrier` cannot be used together. Single-read batch mode is
compatible with `--resume`. `--single-read-match-hints` requires `--single-read-batches` and is
a performance-only option, so it can be enabled or disabled when a run resumes. Epoch-barrier mode
is not compatible with `--resume`.

Trusted-local extraction is an explicit operator trust decision. It requires `signatures.bin` and a
non-empty `registry.mphf`, in addition to the core block, index, metadata, and `registry.bin`
files. The synthetic source identity binds admitted file names, file sizes, and the asserted wire
profile. It also size-binds `blockhash_registry.bin` and `vote_hash_registry.bin` when they exist.
It does not authenticate file contents. It does not read those hash registries. It does not require
or use `prev_blockhash_tail.bin`.

Trusted-local mode supports a mixed archive during metadata normalization. It inventories each
epoch through a pinned directory descriptor before it selects a metadata decoder:

- An epoch with no metadata-schema marker keeps the historical compatibility decoder and the
  existing synthetic size-bound generation digest. A valid old unmarked generation manifest can
  exist, but it does not replace that synthetic identity.
- An epoch with the exact current-typed-errors marker must also have a valid
  `archive-v2-generation.json`. Its epoch, cluster, slots-per-epoch value, and message profile must
  match the trusted-local command. The physical metadata and message marker bytes must be exact.
  The reader then uses the published manifest digest with the strict current metadata decoder. File
  contents other than the fixed marker controls still have trusted-local size-only verification.
- A marker without a manifest, a current manifest without its physical marker, a malformed
  manifest, a special-file control, or an unknown or conflicting metadata marker stops the run.

Discovery records this per-epoch choice. Every later open repeats the descriptor-based checks and
must get the same choice and generation digest. Therefore, a cutover during a run stops that run.
A pre-cutover resume shard for a normalized epoch also fails its source-digest check. A checkpoint
made after cutover resumes against the current generation normally.

`--resume` validates every committed discovery shard, the frozen account list when it exists,
every account-ID log, every raw transaction stream, and the authenticated checkpoint. In
single-read batch mode, discovery and raw epoch shards advance as one paired prefix. An interrupted
unpaired or partial artifact is moved to a quarantine name. It is not deleted. A complete shard is
trusted only after its stream and manifest pass validation.

The one-read counters describe the extraction scan of `blocks.bin`. Trusted-local size-only
verification does not add a full block-file re-read.

## Output

The raw output has this layout:

```text
manifest.json
resume-checkpoint.json
accounts.wincode
discoveries/
  epoch-N/
    creations.wincode
epochs/
  epoch-N/
    account-ids.wincode
    transactions.wincode
    manifest.json
```

`accounts.wincode` also binds the verified mint transaction position and its source signature
ordinal and count. Raw transaction streams are length-framed Wincode records. Each stream has one
header, zero or more transaction records, and one footer. Each epoch manifest contains the raw
stream digest and the account-ID log digest. An epoch account-ID log is a prefix sidecar: it has the
mint and the accounts created no later than that epoch. The root manifest states either
`published_manifest` or `trusted_local_sizes_only` as its source binding.

## Probe

The probe reads a bounded range of one trusted-local epoch and reports Pass-A speed. It creates no
dump files:

```sh
cargo run --release -p blockzilla-token-transaction-dump -- \
  probe /volume1/blockzilla/archive/epoch-801 \
  --epoch 801 \
  --start-slot 346066298 \
  --max-blocks 10000 \
  --workers 12 \
  --wire-profile post
```

## Consolidation

Consolidation reads the immutable raw dump, reopens the source epoch registries and signature
files, makes one sorted global public-key registry and one occurrence-ordered `signatures.bin`,
and rewrites only typed public-key references. Raw shards stay unchanged. Blockhash and vote-hash
references stay source-bound and unchanged.

The consolidator validates and hashes each raw shard during its first read. It does not hash a
whole source registry or signature sidecar. It resolves only referenced registry rows, uses
bounded external sorts, builds one resident dump registry, and reuses one dense source-ID table
for each epoch. Its second pass reads physically nearby transaction frames into one bounded arena,
decodes their message and metadata fields as borrowed slices, and writes records in canonical
order. Selected adjacent signature ranges are merged and other signature ranges are read in
parallel. The final manifest is published last. Its presence is the completion marker.

Run schema-3 consolidation:

```sh
cargo run --release -p blockzilla-token-transaction-dump -- \
  consolidate /volume1/blockzilla/archive \
  /volume1/blockzilla/token-transaction-dumps/spyx-raw \
  /volume1/blockzilla/token-transaction-dumps/spyx-consolidated
```

The output directory must not exist. The completed directory contains only:

```text
manifest.json
accounts.wincode
registry.bin
signatures.bin
transactions.wincode
```

Run the separate full audit only when required. It reads and hashes all final files, so the
consolidator does not run it automatically:

```sh
cargo run --release -p blockzilla-token-transaction-dump -- \
  validate /volume1/blockzilla/token-transaction-dumps/spyx-consolidated
```

Build the metadata-derived public holder and volume history:

```sh
cargo run --release -p blockzilla-token-transaction-dump -- \
  token-report \
  /volume1/blockzilla/token-transaction-dumps/spyx-consolidated \
  /volume1/blockzilla/token-transaction-dumps/spyx-consolidated.token-report.json
```

This command hashes `transactions.wincode` during its one sequential read. It reuses the frame,
message-account, and metadata staging buffers. It verifies public token-account pre-state from the
mint-creation floor, then writes daily holder, account, supply, concentration, and public-balance
movement data. It also writes the largest and smallest positive public holders, the largest public
movement days and transactions, and exact per-address RPC pagination counts for the mint and all
discovered token accounts. The output path must be outside the immutable five-file dump, and the
command never replaces an existing report.

SPYX uses Token-2022 `ScaledUiAmount` and `ConfidentialTransfer`. The report therefore labels all
amounts as public raw amounts and base units. It does not claim complete displayed balances,
confidential balances, DEX volume, or USD volume.

Build the complete instruction-program inventory after consolidation:

```sh
cargo run --release -p blockzilla-token-transaction-dump -- \
  program-inventory \
  /volume1/blockzilla/token-transaction-dumps/spyx-consolidated \
  /volume1/blockzilla/token-transaction-dumps/spyx-consolidated.programs.json
```

This command reads and hashes `transactions.wincode` once and loads the final
`registry.bin` once. It streams all top-level and inner instructions, resolves
program indexes through static and loaded accounts, and writes one deterministic
JSON report. The report counts instruction occurrences and distinct transactions
for each program. It fails if an instruction program cannot be resolved. The
report is a separate file because the completed dump directory must keep exactly
its five manifest-bound files.

Build an attributed log inventory for a selected program set:

```sh
cargo run --release -p blockzilla-token-transaction-dump -- \
  program-log-inventory \
  /volume1/blockzilla/token-transaction-dumps/spyx-consolidated \
  programs.txt \
  /volume1/blockzilla/token-transaction-dumps/spyx-consolidated.program-logs.json
```

`programs.txt` is a UTF-8 file with one base58 program ID per line. The command
reads the compact transaction logs and writes a deterministic JSON report for
the selected programs. The report is separate from the immutable five-file
dump, and an existing report is never replaced.

Measure exact decoder coverage after programs are identified:

```sh
cargo run --release -p blockzilla-token-transaction-dump -- \
  program-coverage \
  /volume1/blockzilla/token-transaction-dumps/spyx-consolidated \
  identified-programs.txt \
  /volume1/blockzilla/token-transaction-dumps/spyx-consolidated.program-coverage.json
```

`identified-programs.txt` contains one base58 program ID per line. Blank lines
and lines that start with `#` are ignored. Duplicate IDs stop the command. The
command uses the same borrowed, single-read instruction scan as
`program-inventory`. It uses dense registry-ID flags in the hot loop. It reports
identified outer and inner instruction occurrences, transactions for which all
programs are identified, transactions that contain both identified and unknown
programs, transactions touched by at least one identified program, and
transactions that have instructions but no identified program. An
instruction-free transaction is fully covered because it needs no program
decoder. Unknown programs are ranked by their distinct transaction impact and
then by instruction occurrences.
