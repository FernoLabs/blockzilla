# SPYx explorer

This is a read-only SvelteKit explorer for the indexed SPYx Solana transaction
history. It uses the public token-balance metadata report at
`../../benchmark-results/spyx-token-report-v1/token-history-report-top100-20260831.json`.

The interface reports owners, token accounts, balance concentration,
public owner activity, token movement, verified instruction-level swaps,
transaction counts, balance continuity, and the attached Token-2022
raw-balance instruction verification. It does not claim confidential balance
coverage. Provider request estimates stay in the generated report for internal
pricing work; the public interface does not show them.

The Search page queries the transaction index by Solana signature, index record
ID, slot and archive position, wallet owner, token account, mint, or program.
It uses the same origin by default. Set a public API base when the static
explorer and search service use different origins:

```sh
PUBLIC_SPYX_API_BASE_URL=https://your-search-service.example npm run dev
```

The sidebar API reference at `/api-docs` lists every public GET route, its main
limits, and copyable examples built from the origin that serves the page. The
examples do not embed a deployment address.

The configured service must provide `GET /healthz`, the transaction lookup
routes under `/api/v1/transactions`, and the complete postings artifact. The
current service can query:

- `target-address`: the mint or a discovered target token account.
- `token-account`: a discovered target token account. This alias rejects the
  mint.
- `program`: a program used by a direct top-level instruction, a recorded inner
  CPI, or either scope. Use `instruction_scope=direct`, `inner`, or `all`. The
  default is `all`.
- `owner`: transactions linked to a target token-account owner immediately
  before or after the transaction.

Each posting row links to the original transaction lookup. Results use a fixed
page limit of at most 200 rows. A program that occurs in both scopes has one
row in the combined result. The opaque next-page cursor is bound to the
posting kind, key, program instruction scope, offset, and exact postings
manifest. The page enables owner
search only when `/healthz` reports a complete owner index. Owner results are
owner-linked target history, not signer history or general wallet actions. See
[`FIRESCOPE-PARITY.md`](FIRESCOPE-PARITY.md) for the exact feature map and
evidence rules.

The market dashboard uses the execution-proven Market V2 index. Price candles
are available for exact non-empty Solana slots, one minute, one hour, four
hours, one day, and one week. Slot OHLC uses canonical transaction and
instruction order. The DEX-program history chart adds raw SPYx volume across
all quote pairs, groups it by the executed DEX program, and keeps routed volume
as a subset. It never adds raw amounts from different quote mints. Empty time
buckets are plotted as zero. The all-time DEX table is ranked by SPYx volume.

The related public routes are:

- `/api/v1/market/slot-candles` for exact non-empty slot OHLCV.
- `/api/v1/market/candles` for time candles, including `interval=60`.
- `/api/v1/market/program-volume` for bounded SPYx volume grouped by executed
  DEX program and time bucket.

The verified complete posting artifacts have these counts:

| Item | Value |
| --- | ---: |
| Selected transactions | 7,311,137 |
| Target-address keys | 134,943 |
| Target-address postings | 29,060,229 |
| Program keys | 1,070 |
| Program postings | 39,753,473 |
| Owner keys | 112,352 |
| Owner postings | 21,691,712 |
| Total address and program postings | 68,813,702 |
| Total postings across both artifacts | 90,505,414 |

Every selected transaction has at least one target-address posting. The
mint itself occurs in 7,093,830 transactions, or 97.027726% of the dump. The
other 217,307 transactions do not mention the mint, but they do mention at
least one discovered target token account. The
target-address semantic SHA-256 is
`c31eda69358b910d3bc9ef824f0e15a9a0f0ca76c182f4ebc70a1ab4a638bd81`.
The program semantic SHA-256 is
`c4b0c302b84b6cd84c7944c7046749ff93b580db0ffea8b702513e5e8e268d75`.
The owner semantic SHA-256 is
`ce0872523a204c83a97353a9bc75d00a293421de8a2369254d9430773301154a`.

## Run

```sh
npm install
npm run dev
```

`npm run build:data` creates `static/data/spyx-summary.json` and
`static/data/spyx-programs.json`. The build removes the large per-address RPC
array, removes program evidence arrays, and fills zero-activity calendar dates
by carrying the prior public state. The Overview page shows the main token and
history summary. The Holders page has one owner ranking with account-type and
program-ID filters, concentration, balance distribution, history, and the
largest account-to-account movements. The Price page has the verified market
dashboard. The Data integrity page shows the verification result, missing-data
checks, instruction verification evidence, coverage limits, and source hashes.
`npm run check` validates the application and `npm run build` creates the
static site in `build/`.

The Programs page has a separate **SPYx account CPI** table. A row is included
only when that program executes an inner instruction whose own account list
contains the SPYx mint or a discovered SPYx token account. This is not a
transaction-level co-occurrence filter and it is not a custody claim. The
current report contains 60 program IDs and 18,205,561 matching CPI calls in
4,892,208 distinct transactions. Nineteen matching program IDs have no label;
they stay visible and searchable by ID.

The final public-balance summary also classifies holder authorities into four
wire-level classes:

- `observed_transaction_signer`: an on-curve address observed as a top-level
  transaction signer. It is a wallet or user candidate, not proof of a human.
- `attributed_program_derived_address`: an off-curve authority attributed to
  one program by committed parser or CPI evidence. This evidence does not prove
  the PDA seeds or the live Solana account owner field.
- `off_curve_unattributed`: an off-curve authority without unambiguous program
  evidence. It stays unknown and is not assigned to a program.
- `unclassified_on_curve`: an on-curve authority that was not observed as a
  top-level signer in the indexed SPYx transactions.

The summary keeps one total for each class in `class_totals`. It also includes
the precomputed top 25 holders for each class in `largest_25_by_class` and the
direct PDA-custody aggregates in `holdings_by_program`. The field name is kept
for wire compatibility. These aggregates do not measure protocol TVL or total
economic exposure. The public activity
extension adds `largest_25_by_activity_all`,
`largest_25_by_activity_by_class`, and `attributed_program_holders`. Holder
rows include `activity_transaction_count`, `public_balance_increase`,
`public_balance_decrease`, and `public_activity_volume`. Program rows contain
the same amount fields and `owner_activity_transaction_links`. These field
names are part of the current summary wire format.

Public activity volume is cumulative owner inflow plus outflow. The replay
uses signed final-balance deltas. It first removes moves between token accounts
of the same owner. This value is not DEX volume or trading volume. The holder
table can sort each column. It can filter observed signer wallets, one combined
PDA-or-program-account cohort, other on-curve authorities, and a selected
program ID. Every known and unknown program ID stays eligible for the same
filter. Program names are display metadata. They do not add, remove, rank,
sort, or filter rows.

When the source report includes `final_top_100_holder_history`, the Holders
page adds daily balance lines for one fixed cohort: the 100 largest owners at
the final dump boundary. It does not recalculate the top 100 for each day.
Every UTC date is present, and the first and last dates stay marked as partial
source-boundary days. Owner and program names add context only; the series
membership and rank come from exact owner addresses and raw balances.

The verified class totals are:

| Authority class | Holders | Token accounts | Public balance (SPYx) |
| --- | ---: | ---: | ---: |
| Observed transaction signer | 17,347 | 17,350 | 63,673.81017901 |
| Attributed program-derived address | 583 | 586 | 8,280.22780344 |
| Off-curve, unattributed | 388 | 393 | 16,589.91318094 |
| Other on-curve | 10,735 | 10,735 | 6,690.91448909 |

There are 76 direct PDA-custody aggregates. The largest are Kamino Lending at
5,067.94611647 SPYx, Raydium CLMM at 2,177.94337497 SPYx, and Whirlpool at
247.41305477 SPYx. An observed signer is only a wallet or user candidate. It is
not proof that the holder is a human.

Piggy Bank is program
`Pig2ienhM3ukiTec3x8aCdnLASpU4z8yRPLgH9QxDvm`. Its attributed PDA has a direct
balance of 12.59714925 SPYx at the dump boundary. This is rank 11 of 76 direct
PDA-custody aggregates by balance and rank 9 of 76 by direct PDA activity. Its
cumulative direct inflow is 3,876.70813461 SPYx, its cumulative direct outflow
is 3,864.11098536 SPYx, and its gross direct activity is 7,740.81911997 SPYx.
Its attributed PDA is
`5CgRTdywEQ7LK7SRM5NAgsuSWxnswREW6VeZ4i9jHCRf`. This PDA is rank 17 of 583
attributed PDAs by holding and rank 14 of 583 by activity. It has 5,917
balance-changing transactions.

These Piggy Bank values do not measure its depositor liability or the SPYx
that it deploys into other protocols. For example, Piggy Bank's transparency
page reported 459.21 SPYx as Jup Lend collateral and 12.64 SPYx in the direct
vault on August 29, 2026. The direct vault value agrees with this replay after
the different observation time and ScaledUiAmount display are considered.

Before the Search page enables any query, it checks that `/healthz` reports the
same transaction count and transaction-file SHA-256 as the loaded report. It
also requires a complete transaction index. A missing, incomplete, or
different dataset keeps every query disabled and shows the reason.

An independent strict replay report is optional for development. Attach it
with either command:

```sh
SPYX_STRICT_REPLAY_REPORT=/path/to/replay-report.json npm run build:data
node scripts/build-data.mjs --strict-replay /path/to/replay-report.json
```

The builder checks the artifact kind and mint before it exposes the replay
status, counters, blockers, and first failure on the audit page. It also checks
that the replay, program, and history reports bind to the same dump digests,
epoch range, and row counts. Without replay input, the development interface
says that strict instruction replay was not performed.

An optional runtime-owner supplement can add later Solana `Account.owner`
observations without changing the canonical replay JSON, its four authority
classes, or its verified totals. Use `--holder-authority-supplement` and
`--holder-authority-supplement-sha256`, or the matching
`SPYX_HOLDER_AUTHORITY_SUPPLEMENT` environment variables. The current snapshot
contains all 388 `off_curve_unattributed` holders in the strict replay. The RPC
queries ran at finalized slots 442,953,477 through 442,953,478. Of the 388
addresses, 181 existed and had a current Solana `Account.owner`; 207 did not
exist and therefore had no current `Account.owner`. The observed owners contain
117 custom-program-owned accounts, 59 System Program accounts, four Token-2022
accounts, and one legacy SPL Token account. They have 39 distinct runtime owner
program IDs, of which 36 are custom programs. Every existing account's full
runtime owner program ID remains visible. Custom owner rows receive additive
protocol attribution. System Program, SPL Token, Token-2022, and executable
owner IDs remain visible, but they do not count as custom protocol attribution.
Coverage reports queried addresses separately from addresses that existed and
returned an `Account.owner`. Runtime ownership does not prove PDA derivation,
PDA seeds, historical ownership at the dump boundary, custody, or protocol TVL.

To regenerate a snapshot, run:

```sh
npm run snapshot:holder-runtime-owners -- \
  --replay /path/to/replay-report.json \
  --output /path/to/runtime-owner-snapshot.json \
  --labels-from ../../benchmark-results/spyx-token-report-v1/holder-authority-runtime-owner-snapshot-full-20260830.json
```

The command uses `holder_authority.off_curve_unattributed_holders` when a new
replay contains that complete array. It uses the two exposed top lists only for
older reports. The output states which selection it used. It rejects a full
coverage claim unless the exact address set and row count match the replay's
complete off-curve array.

`npm run build` is the release build. It requires exact SHA-256 pins for the
history report, strict replay report, program-identification report, and CPI
inventory. The
builder checks each file's exact bytes before it accepts and parses the report.
It also requires a complete program report with unique program and registry
IDs. Each program's total instruction count must equal its outer plus inner
counts, and all row-derived coverage counters must match. The verified report
digests are:

- History report:
  `9a0aa92efd2c2e485e42323b16bd544454480a02bf3636dcd157d8a119c58530`
- Strict replay report:
  `55247cc036a4471812d97262ebee9e2ae23d9e651f85c46c27bfcc1d5e855754`
- Program-identification report:
  `066397944a0bc8596ad20056320d1a900d1aeb4a9893caeea03a010ac3536d3c`
- Target-account CPI inventory:
  `749346046e43eb760774c878cfe6e74285ea1a39c6530c02063e54c5a486fbed`
- Runtime-owner supplement:
  `db773348dda84d2c42231f321403c6ed1e014f6b0ce5f2fb8494f2f4c9244eff`

Run the verified release build with:

```sh
SPYX_HISTORY_REPORT_SHA256=9a0aa92efd2c2e485e42323b16bd544454480a02bf3636dcd157d8a119c58530 \
SPYX_STRICT_REPLAY_REPORT=../../benchmark-results/spyx-token-report-v1/spyx-replay-authority-portfolios-v5-0c4f0bd8.json \
SPYX_STRICT_REPLAY_REPORT_SHA256=55247cc036a4471812d97262ebee9e2ae23d9e651f85c46c27bfcc1d5e855754 \
SPYX_PROGRAM_REPORT_SHA256=066397944a0bc8596ad20056320d1a900d1aeb4a9893caeea03a010ac3536d3c \
SPYX_PROGRAM_CPI_INVENTORY_SHA256=749346046e43eb760774c878cfe6e74285ea1a39c6530c02063e54c5a486fbed \
SPYX_HOLDER_AUTHORITY_SUPPLEMENT=../../benchmark-results/spyx-token-report-v1/holder-authority-runtime-owner-snapshot-full-0c4f0bd8.json \
SPYX_HOLDER_AUTHORITY_SUPPLEMENT_SHA256=db773348dda84d2c42231f321403c6ed1e014f6b0ce5f2fb8494f2f4c9244eff \
npm run build
```

This schema 5 strict report has status `complete_match`. It processed all
7,311,137 transactions in 125.6 seconds. It had zero replay errors, zero
pre-balance mismatches, and zero post-balance mismatches. It includes 55,191
authority history series and 1,453,847 sparse forward samples. The reviewed
replay binary is
`/volume1/blockzilla/bin/blockzilla-token-transaction-dump-0c4f0bd84b45663ce03bfb728178208dfad71081bf35e2444c275b2d1eff6fa6`.
Its SHA-256 is
`0c4f0bd84b45663ce03bfb728178208dfad71081bf35e2444c275b2d1eff6fa6`.

The Holders page keeps the exact on-chain custody ranking and adds a separate
true-owner estimate. The estimate combines direct on-curve balances with
committed, non-DEX, one-way deposits into off-curve custody. Matched returns
reduce the candidate deposit. Assigned estimates never exceed current custody.
This method does not prove beneficial ownership. It does not include protocol
yield, debt, share accounting, or assets that a protocol deploys elsewhere.

The Search page adds a separate PDA authority heuristic. It uses only committed
PDA creation evidence. A PDA receives an estimate only when its creation history
has one signer candidate, that signer is not shared with another created PDA,
and the signer has a complete non-DEX portfolio estimate. The PDA custody at the
dump boundary stays separate. The external estimate excludes the PDA's own
custody, so the total cannot count it twice. Ambiguous and shared signers remain
visible but are never combined. These estimates do not change holder ranks,
custody totals, or authority totals. No program-specific rule is used for the
estimate.

A program search also returns the union of replay-attributed PDA holders and
creation-evidence rows. It does not use program labels for inclusion. The Piggy
Bank program search therefore returns
`5CgRTdywEQ7LK7SRM5NAgsuSWxnswREW6VeZ4i9jHCRf` and links to its owner result.
The owner result shows full creation accounts and the full creation signature.

The separate `spyx-pda-flow-proofs.json` artifact contains reviewed,
transaction-level evidence. Its current Piggy example records full accounts and
three indexed signatures for a Jupiter-to-strategy, strategy-to-Piggy, and
strategy-to-Jupiter sequence. The proof shows two-way flow between the Piggy PDA
token account, its creation signer, and Jupiter. It does not claim that the PDA
owns the Jupiter position. The observed Jupiter position belongs to the creation
signer. This evidence does not change the replay estimate or any balance total.

The exact on-chain custody view can group the same complete cohort by custody
owner or by program ID. The program view contains 106 program IDs, including
55 IDs without a name, plus one visible `Program not linked` group. Its 971
custody owners, 979 token accounts, balance, and public activity reconcile with
the custody-owner rows. Program names are display data only.

The current NAS static release is
`/volume1/blockzilla/web/spyx-explorer-c6b5410480299ecf`. Its archive is
`/volume1/blockzilla/web/spyx-explorer-c6b5410480299ecf.tar.gz`,
with SHA-256
`c6b5410480299ecf7e4d1f3877caa4828708cf6386ff65ff8dffa5fbd9868785`.
The static summary SHA-256 is
`3541efaa11d783e4bdae1dd9a93912b65f642ebcab955142959a49396b257219`.
The static program summary SHA-256 is
`239330a877a2ae891b398a5cf8c666184da76759f86f8bca5c366de979087305`.
The static authority-portfolio SHA-256 is
`2ab028ac5c46ef54ce51ed8099551678db6ec5451bd1ff3ad24f7780a792b85d`.
The compact portfolio-table SHA-256 is
`872f69fe92eddab06ac4aac53858ef4453edbdebae6b4f91eb6bfb517c73d601`.
The authority-history index SHA-256 is
`1929f70d1382fd8bb30d2a83c8534c858a8ba3a2665c8078587e4056e9e3a307`.
The static PDA-authority estimate SHA-256 is
`258ec36aa06dcc83e4df7ed680b813608f246ca157e30647722a800597eb6f95`.
The static PDA-flow proof SHA-256 is
`14b3c8fbfae2b1c4f8c36ae3448a22744b5f562698db4af51d8a1216036d5363`.

The equivalent command options are `--history-sha256`,
`--strict-replay-sha256`, `--programs-sha256`, and
`--program-cpi-inventory-sha256`. Set
`SPYX_HISTORY_REPORT` or use `--history` when the history report is not at the
default path. Set `SPYX_PROGRAM_REPORT` or use `--programs` when the program
report is not at the default path. Set `SPYX_PROGRAM_CPI_INVENTORY` or use
`--program-cpi-inventory` when the CPI inventory is not at the default path.
Use `npm run build:data` for an intentional development build without strict
replay.
