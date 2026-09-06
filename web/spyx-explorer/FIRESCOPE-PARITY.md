# Firescope parity map

This note compares the current Firescope application with the current SPYx explorer and query service. It uses only checked-in behavior. A control or route that returns a placeholder error is not an implemented feature.

## Scope rule

Firescope is a multi-token market index for Archive V2 data. It derives swap rows, prices, pairs, OHLCV series, and wallet swap views. SPYx is a read-only explorer for one fixed mint and one complete, selected transaction dump. It reports proven executed swaps, exact quote prices, OHLCV series, public Token-2022 balance history, source evidence, strict replay evidence, and original dump transactions.

SPYx must not copy Firescope market labels when the dump does not prove them. In SPYx:

- Public bilateral movement is not DEX volume, trade volume, or USD volume.
- Market volume and price include only rows that pass the market database commit,
  invocation-stack, token-flow, and balance-reconciliation gates.
- A stablecoin quote is a quote-token amount. It is not an oracle USD value.
- An owner posting is not a signer, wallet action, or semantic actor.
- An observed top-level signer is a wallet or user candidate. It is not proof
  of a human.
- An off-curve authority is attributed to a program only with committed parser
  or CPI evidence. This evidence does not prove PDA seeds. If the program
  evidence is not unambiguous, the off-curve authority stays unknown.
- A program posting means that a recorded outer or inner instruction invoked the program. It does not mean that a decoder succeeded.
- The source blockhash IDs are dump-local IDs. They are not RPC blockhash values.

## Current feature map

| Area | Firescope | SPYx now | Parity result |
| --- | --- | --- | --- |
| Main scope | Selects from many indexed mints and shows market data for each mint. | Uses one fixed SPYx mint and epochs 801 through 1018. | Intentionally different. Do not add a token picker unless another complete dump is added. |
| Summary | Shows price, 24-hour change, USD volume, token accounts, and archive range. | Shows the latest proven executed-swap price, 24-hour target and quote volume, 24-hour trade count, total proven trades, pairs, venues, final public balance, holders, accounts, and selected transactions. Its holder-authority data has four class totals, a precomputed top 25 per class, and `holdings_by_program`. | Implemented for the fixed mint and the selected quote. SPYx does not claim oracle USD value or full market coverage. Holder authority uses the stated evidence limits. |
| History chart | Shows pair, reference, and direct-price lines; OHLC candles; DEX overlays; point selection; and trades for the selected point. | Shows an executed-price OHLCV chart with 1-hour, 4-hour, 1-day, and 1-week intervals. It also has five report-bound modes: public balance; owners and accounts; concentration; movement, mint, and burn; and transaction counts. | Implemented for proven market rows and the existing public-balance evidence. Multi-reference and live-market parity remain out of scope. |
| Token metadata | Stores token name, symbol, URI, metadata account, and update authority. | Shows the verified mint, decimals, Token-2022 program family, and epoch range. It does not have a name, symbol, URI, or authority index. | Partial parity from the selected-dump evidence only. Do not claim Metaplex metadata parity. |
| Program view | Uses a configured DEX program set while it derives swap rows. | Shows all 1,070 observed program IDs, identity evidence, decoder-source evidence, outer and inner occurrence counts, and transaction counts. Each program ID links to its transaction postings. | SPYx has broader program inventory evidence. It does not claim that an identity or decoder source proves decode coverage. |
| Transaction lookup | Shows swap rows with slot, transaction index, signature ordinal, amounts, DEX, and price fields. | The market page lists recent proven swaps and links each row to transaction lookup. The backend and page support lookup by transaction ID, exact signature, and canonical coordinate, and return the original transaction evidence. A market detail route returns one proven swap row. | Implemented with direct links from derived market evidence to the original transaction. |
| Owner view | The wallet page returns inferred swap rows for one owner. The read model scans the swap vector for this query. | The page and backend return complete strict-replay owner postings from a fixed binary index. | Implemented as “owner-linked target transactions,” not as wallet trades, signer history, or general wallet actions. |
| Target-account view | Stores token-account rollups from token-balance rows. | The backend and page query complete target-address postings. The target-address kind accepts the mint or a discovered token account. The token-account alias accepts only a discovered target token account. | Implemented as exact resolved-message mentions. This is not a current account rollup. |
| Program transactions | Firescope can filter derived trade rows by DEX program. | The backend and page query complete program posting ranges. Each result links to the original transaction. | Implemented as recorded program invocation, not as a derived trade. |
| Audit and stats | Shows archive counters, loaded row counts, file sizes, and source paths. | Shows dump counts and hashes, metadata continuity, missing-data gates, compact-artifact details, strict replay state, market provenance and rejection counters, source files, mint-only misses, and exact standard-RPC request counts. | SPYx meets and exceeds this parity for its fixed evidence scope. |
| Storage model | Uses fixed binary records, then loads pubkeys, tokens, accounts, balance rows, and swaps into vectors and hash maps at service start. | Uses pinned source and index files, bounded sequential validation, binary search, and fixed positioned reads. The market store loads only the 311,455 verified fixed-size market rows, not the 7.3-million-transaction source dump. | Keep the SPYx model. Do not copy full dump loading or full wallet scans. |
| Live data | Contains block-stream bootstrap and unfinished live-source work. | Uses one immutable consolidated dump. | Live parity is not part of the present SPYx scope. |

## Evidence

Firescope evidence:

- `../../../firescope/web/web/src/routes/+page.svelte:1040` contains token selection, market summary, pairs, line and OHLC charts, metadata, and trade tables.
- `../../../firescope/web/web/src/routes/wallet/+page.svelte:95` contains the owner swap search and page controls.
- `../../../firescope/web/web/src/routes/stats/+page.svelte:72` contains archive, row, file, and source statistics.
- `../../../firescope/crates/firescope-backend/src/api.rs:83` registers the price, OHLCV, pair, token, trade, wallet, and stats routes.
- `../../../firescope/crates/firescope-db/src/store.rs:164` loads all compact record files into memory. `store.rs:635` scans all swap rows for an owner query.
- `../../../firescope/crates/firescope-indexer/src/dex.rs:174` derives swap rows from owner balance deltas and touched program IDs.

SPYx evidence:

- `src/lib/components/MarketDashboard.svelte` shows the verified market summary, interval controls, executed-price chart, and recent proven swaps. `src/lib/market-api.ts` defines the typed market API client and exact-ratio display conversion.
- `src/routes/+page.svelte` links the mint to its target-address postings and shows the main public-balance summary, verified token fields, largest current holders, and aggregate history.
- `src/routes/holders/+page.svelte` shows the unified owner ranking, program-ID filter, concentration, balance distribution, aggregate history, and largest public-movement transactions. Labels add display text only and do not control membership.
- `src/routes/price/+page.svelte` hosts the complete verified market dashboard. `src/lib/components/HistoryChart.svelte` defines the five public report modes.
- `src/routes/audit/+page.svelte:100` contains the dataset, continuity, strict replay, and source evidence.
- `src/routes/programs/+page.svelte:36` builds a program-posting link. `src/routes/programs/+page.svelte:62` contains the program inventory and coverage evidence.
- `src/routes/search/+page.svelte:63` binds `/healthz` to the report identity before it enables a query. `src/routes/search/+page.svelte:105` runs the gated search. `src/routes/search/+page.svelte:257` applies the opaque next-page cursor through the same gate.
- `../../indexer/blockzilla-spyx-query/src/api.rs` registers health, transaction, owner and other posting, market provenance, summary, pair, trade, and candle routes.
- `../../indexer/blockzilla-spyx-query/src/market_builder.rs` applies the fail-closed parser and reconciliation gates. `market_format.rs` defines the source-bound fixed record format. `market_store.rs` validates and serves the published artifact.
- `../../indexer/blockzilla-spyx-query/src/store.rs` opens pinned locator and signature handles, validates them in bounded chunks, and uses positioned row reads. `../../indexer/blockzilla-spyx-query/src/postings_store.rs:95` opens and verifies the postings artifact with the same fail-closed model.
- `scripts/build-data.mjs:34` reads the history report. `build-data.mjs:37` starts its validation and compaction. `build-data.mjs:424` fills missing calendar dates with zero activity and carried state.

## Completed posting indexes

The builder makes target-address and program indexes in one bounded transaction
stream scan. It uses a fixed projection scratch, dense bit sets, and tagged
external-sort runs. It merges, sorts, and removes duplicate rows before it
publishes the manifest. Each directory row is 24 bytes:

```text
u32 registry_id
u32 flags
u64 first_posting_row
u64 posting_count
```

Each posting is one `u64 transaction_ordinal`. Each directory is sorted by
registry ID. Each posting range is contiguous, strictly increasing, and
unique. The memory limit does not depend on the full posting count.

### Target address: mint or target token account

The index has one posting for each transaction and each target address in the
fully resolved message account list. It includes static and loaded addresses.
The target set is the mint plus all 134,942 discovered token accounts. It has
only one row for an address in one transaction.

Verified result:

- 134,943 accepted target addresses: one mint and 134,942 token accounts.
- 29,060,229 target-address postings.
- 7,311,137 selected transactions. Each transaction has at least one
  target-address posting.
- Semantic SHA-256:
  `c31eda69358b910d3bc9ef824f0e15a9a0f0ca76c182f4ebc70a1ab4a638bd81`.

`target-address` is the canonical kind. `token-account` is an alias for a
target token-account key. It rejects a non-target key and rejects the mint. A
mint query must use `target-address`.

### Program

The index has one posting for each transaction and each program that an outer
instruction or a metadata-recorded inner instruction uses. It resolves program
indices through static and loaded addresses. It includes failed transactions.
It has only one row for a program in one transaction. "Used" means recorded
invocation. It does not mean successful commit or known decoder.

Verified result:

- 1,070 program keys.
- 39,753,473 postings.
- Zero unresolved program IDs.
- Semantic SHA-256:
  `c4b0c302b84b6cd84c7944c7046749ff93b580db0ffea8b702513e5e8e268d75`.

The two bodies contain 68,813,702 postings. The complete artifact is
553,774,440 bytes. An independent full verifier passed all source, file,
ordering, uniqueness, range, and source-coverage checks.

### Owner

For each mentioned target token account, collect its owner immediately before
the transaction when the account is open and immediately after the transaction
when the account is open. Union these owners and remove duplicates for the
transaction.

This rule includes failed transactions. It includes the old owner on close or
authority change. It includes the new owner on initialize, reopen, or authority
change. A mint-only transaction has no owner posting. Strict replay must pass
before owner index publication.

This API is owner-linked target history. It is not signer history, wallet action history, or semantic actor history.

Verified result:

- 112,352 owner keys.
- 21,691,712 owner postings.
- 7,229,609 transactions with at least one owner posting.
- Replay-state SHA-256:
  `3570f9fb1ebe7e18fbda9d20c80fc16b80edbc0bdae3579347dd89419fb1bfe6`.
- Owner semantic SHA-256:
  `ce0872523a204c83a97353a9bc75d00a293421de8a2369254d9430773301154a`.

## Completed posting API and page work

The completed API and page have these properties:

1. The service opens each directory and body with pinned handles, positioned
   reads, and fail-closed validation.
2. It converts the base58 key to a registry ID. It uses binary search for the
   key directory and reads only the requested posting range.
3. The opaque cursor binds the posting kind, registry ID, next offset, and
   manifest SHA-256. A cursor for another index, kind, or key fails. The page
   limit is at most 200 rows.
4. Each row has the transaction ID, canonical coordinate, and first signature.
   The page links each row to the existing transaction-ID lookup.
5. `/healthz` reports complete capability flags and exact posting counts. The
   page first binds the service transaction count and source transaction
   SHA-256 to the loaded report. A mismatch or incomplete transaction index
   disables every query. A posting type also stays disabled until the service
   reports that posting index as complete.
6. The overview links the mint to target-address postings. The Programs page
   links each program to program postings.
7. The owner route uses a separate replay-bound manifest and cursor domain. It
   returns ordered owner-linked target transactions without scanning all
   transaction or market rows at request time.

## Completed market database and page

The published market database scans all 7,311,137 selected transactions. It
contains 311,455 proven executed swaps, 219 target pairs, and six venue
programs. Each fixed-size row stores raw executed integer amounts. The API
derives each price as an exact reduced ratio. It does not store a floating-point
price.

A candidate is published only when the service proves successful transaction
commit, the invocation stack, relevant token-flow ownership, and exact
transaction balance reconciliation. Failed transactions, router-only calls,
caught failed calls, ambiguous flows, unsupported relevant token effects, and
balance mismatches do not produce a market row. The manifest binds the source
transaction file, parser identity, record file, counts, and hashes.

The Price page uses this database for the latest executed price, summary
values, OHLCV chart, and recent swap table. Each recent row links to the
original dump transaction. The page keeps the public-balance report separate
from market evidence.

## Completed report charts

The explorer shows these report-bound views without new market claims:

1. Positive public-balance owners and active public token accounts over time.
2. Top-1, top-10, and top-100 public-balance concentration over time. The page
   shows the parts-per-million fields as percentages.
3. Daily public bilateral movement, inferred public mint, and inferred public
   burn. All three labels stay explicit.
4. Selected transactions, public balance-changing transactions, and public
   owner-reassignment transactions by day.
5. Final owner balance distribution, with holder count and public balance for
   each report range.
6. Largest public-movement transactions, with the report coordinate, time,
   first signature, and a link to transaction lookup.
7. Largest positive public-balance owners at the final state.
8. Daily raw balances for the fixed final-boundary top-100 owner cohort, with
   source-boundary and carry-forward flags.
8. Verified mint, decimals, Token-2022 family, and epoch-range fields.
9. Exact mint-only and complete all-target standard-RPC request models. The
   audit also states that the mint-only path misses 217,307 selected
   transactions. The complete all-target path assumes that a full historical
   token-account list already exists and includes closed accounts. It makes no
   current provider-price claim.
10. Four final holder-authority class totals, a precomputed top 25 for each
    class, and `holdings_by_program`. A top-level signer is only a wallet or
    user candidate. Program attribution for an off-curve authority requires
    committed parser or CPI evidence and does not prove PDA seeds. Off-curve
    authorities without unambiguous program evidence stay unknown.

These public-balance views use report-bound values from `spyx-summary.json`.
The data build keeps the report identity and SHA-256 checks. Separately, the
explorer derives executed-swap prices and OHLCV from the verified market
database. It does not derive market fields from public bilateral movement, and
it does not claim oracle USD value, confidential balance, or live state.
