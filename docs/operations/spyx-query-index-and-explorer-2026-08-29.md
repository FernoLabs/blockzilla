# SPYx query index and explorer handoff

Date: 2026-08-29 CEST

Owner-index release update: 2026-08-30 CEST

API-reference release update: 2026-08-30 CEST

Mobile design release update: 2026-08-30 CEST

Holder-authority release update: 2026-08-30 CEST

Holder-activity release update: 2026-08-30 CEST

## Result

The complete SPYx dump now has a verified, read-only transaction query index,
verified address, program, and owner posting indexes, and a verified
execution-proven market database.
The service can find a transaction by immutable ID, exact signature, or
canonical dump coordinate. It can also list
transactions for a target address, target token account, recorded program
invocation, or historical owner of a mentioned SPYx token account. It reads the original frame and signature bytes from the
consolidated dump. It does not copy transaction payloads into either index.

The SvelteKit explorer is in `apps/spyx-explorer`. It contains overview, data
integrity, program, transaction-search, and API-reference pages. The release
data is bound to the final strict public raw-balance replay report. The
overview has five history-chart modes, the final balance distribution, and the
largest account-to-account movements. The mint, wallet owners, and each
program ID link to indexed transaction history. The public interface uses
Solana terms such as mint, program, token account, wallet owner, slot, and
transaction index. The Data integrity page shows the verification result,
missing-data checks, instruction verification evidence, coverage limits, and
full source hashes. Provider request comparisons remain in the generated
report for internal pricing work and are not shown in the public interface.

The final replay also classifies each positive-balance holder authority from
wire-level evidence. It reports observed transaction signers, attributed
program-derived addresses, unattributed off-curve addresses, and other
on-curve addresses. It includes 76 direct PDA-custody aggregates attributed to
programs. The overview can filter these classes and a selected attributed
program. The holder table can sort by current holding or public activity. The
program table can sort direct PDA custody by balance or direct PDA activity.

Target-address, token-account, program, and owner postings are complete. Owner
postings come from strict replay. For each mentioned SPYx token account, they
include the open-account owner immediately before or after the validated
transaction. They are not signer, actor, or general wallet history.

The Search page stays disabled until the backend reports a complete index with
the exact report transaction count and transaction-file SHA-256. Only the
newest health response can change that state. The static release build also
requires exact SHA-256 pins for the history and strict replay report files. It
rejects missing, changed, incomplete, or internally inconsistent UI data.

## Strict replay gate

The query work uses the dump that passed the complete strict replay:

| Item | Value |
| --- | --- |
| Status | `complete_match` |
| Transactions | 7,311,137 |
| Signatures | 7,546,434 |
| Public pre-balance mismatches | 0 |
| Public post-balance mismatches | 0 |
| Replay errors | 0 |
| Full replay time | 72.7916 seconds |
| Tracked token accounts | 134,942 |
| Open token accounts | 56,873 |
| Closed token accounts | 78,069 |
| Final positive public-balance accounts | 29,064 |
| Final positive public-balance owners | 29,053 |
| Final public raw balance | 9,523,486,565,248 |
| Source transaction SHA-256 | `2849a8e8fbe7d8dbb553022355cfd33d0e50971166242534a398334e79d977de` |
| Strict replay report | `benchmark-results/spyx-token-report-v1/spyx-replay-holder-volume-full-20dea675.json` |
| Strict replay report SHA-256 | `2933b837ccfc5cb4551f13f089790552c4a409113b6509ff1e654376360ff841` |

This gate covers public raw Token-2022 balances in the selected dump. It does
not cover confidential balances or scaled UI amounts.

## Holder authority and public activity release

The holder activity release used this reviewed replay binary:

| Item | Value |
| --- | --- |
| NAS path | `/volume1/blockzilla/bin/blockzilla-token-transaction-dump-20dea6759e632fac676a5a6677ef5ec0721688c1bd46e52adc68598f8e8537db` |
| SHA-256 | `20dea6759e632fac676a5a6677ef5ec0721688c1bd46e52adc68598f8e8537db` |

The full replay processed all 7,311,137 transactions in 72.7916 seconds. Its
status is `complete_match`. It had zero replay errors, zero pre-balance
mismatches, and zero post-balance mismatches. The local report is
`benchmark-results/spyx-token-report-v1/spyx-replay-holder-volume-full-20dea675.json`.
Its SHA-256 is
`2933b837ccfc5cb4551f13f089790552c4a409113b6509ff1e654376360ff841`.
The final replay-state SHA-256 is
`3570f9fb1ebe7e18fbda9d20c80fc16b80edbc0bdae3579347dd89419fb1bfe6`.

Public activity volume is cumulative owner inflow plus outflow. The replay
uses signed final-balance deltas. It first removes moves between token accounts
of the same owner. This value is not DEX volume or trading volume. It reports
the top activity rows for all authority classes, all 583 current
positive-balance attributed PDAs, and activity totals for all 76 attributed
programs.

| Authority class | Holders | Token accounts | Public balance (SPYx) |
| --- | ---: | ---: | ---: |
| Observed transaction signer | 17,347 | 17,350 | 63,673.81017901 |
| Attributed program-derived address | 583 | 586 | 8,280.22780344 |
| Off-curve, unattributed | 388 | 393 | 16,589.91318094 |
| Other on-curve | 10,735 | 10,735 | 6,690.91448909 |

The 76 direct PDA-custody aggregates are sorted by attributed public balance.
The three
largest are Kamino Lending at 5,067.94611647 SPYx, Raydium CLMM at
2,177.94337497 SPYx, and Whirlpool at 247.41305477 SPYx.

Piggy Bank is program
`Pig2ienhM3ukiTec3x8aCdnLASpU4z8yRPLgH9QxDvm`. Its attributed PDA has a direct
balance of 12.59714925 SPYx at the dump boundary. It is rank 11 of 76 direct
PDA-custody aggregates by balance and rank 9 of 76 by direct PDA activity. Its
cumulative direct inflow is 3,876.70813461 SPYx, its cumulative direct outflow
is 3,864.11098536 SPYx, and its gross direct activity is 7,740.81911997 SPYx.

Its attributed PDA is
`5CgRTdywEQ7LK7SRM5NAgsuSWxnswREW6VeZ4i9jHCRf`. It is rank 17 of 583
attributed PDAs by holding and rank 14 of 583 by public activity. It has 5,917
balance-changing transactions.

These values do not measure Piggy Bank TVL, depositor liability, lending
collateral, receipt tokens, or total economic exposure. Piggy Bank's
transparency page reported 459.21 SPYx as Jup Lend collateral and 12.64 SPYx
in its direct vault on August 29, 2026. The direct vault figure agrees with the
replay after the different observation time and ScaledUiAmount display are
considered.

An observed transaction signer is a wallet or user candidate. It is not proof
that the holder is a human. Program attribution is derivation evidence. It
does not prove the PDA seeds or the live Solana account owner field.

## Identity enrichment audit

The FireWatch cache is useful as a source pipeline, but its current cached
labels do not directly match the 993 unique holders exposed by the SPYx top,
PDA, and off-curve arrays. A safe production import must use a left join. An
identity label must not change holder inclusion, class, rank, sort order, or a
program filter.

The audit found seven exact Squads V4 vault PDAs in the replay. Together they
hold 79.14527240 SPYx and have 164.98814556 SPYx of public replay activity.
Each parent relation was reproduced with the Squads seeds `multisig`, parent
multisig, `vault`, and the vault index. Two vaults have a Squads creation memo
of `{"n":"Fuse"}` and can be described as **Fuse smart wallet · Squads vault
0**. Two vaults share a parent with the self-label
`ByrealProdMoneyManager`. The three other self-labels are
`Stroud GBL Operations`, `BK Swap transaction fee`, and `transfer no gas`.
These are on-chain self-labels. None proves a beneficial owner or a project
treasury.

The reusable FireWatch sources are:

- `backend/src/name_service.rs` for ownership-checked `.sol` primary names.
- `backend/src/program_authority.rs` for Squads V4 vault derivation.
- `backend/src/database.rs` for identity, SNS, and multisig tables.

The safe next format is an append-only identity assertion and relation index
with a source URI, source commit, artifact SHA-256, observation slot,
confidence, and evidence JSON. It must keep `runtime_account_owner` separate
from `pda_derived_by_program`. The current FireWatch registry snapshots do not
pin a source commit or artifact hash, so their 20 extra candidate program
aliases were not added to the release.

## Reviewed binary

| Item | Value |
| --- | --- |
| NAS path | `/volume1/blockzilla/bin/blockzilla-spyx-query-b06fc848f9a58be6f493294eddda8159caad75a7696eb1a04f7ae0d4498c8758` |
| SHA-256 | `b06fc848f9a58be6f493294eddda8159caad75a7696eb1a04f7ae0d4498c8758` |
| Size | 7,332,096 bytes |
| Format | static PIE, x86-64 Linux |

Local checks passed:

- 98 query-service tests across the library, CLI, transaction index, and
  postings integration suites.
- 129 token-dump and strict-replay library tests.
- Workspace format check.
- Strict Clippy for all query targets and features.
- Svelte check with zero errors and zero warnings.
- 132 explorer API-reference, data-contract, and request-order tests.
- Independent review of source pinning, output containment, publication order,
  complete counts, index checksums, signature-to-source binding, postings
  ordering, and postings source coverage.

The service does not use memory maps. It holds pinned read-only file handles,
validates each index with one reusable 8 MiB buffer, and uses positioned row
reads for binary search. HTTP source-frame reads use a 12-slot semaphore and a
reusable scratch-buffer pool. The pool does not retain buffers larger than 16
MiB. Posting pages are limited to 200 rows.

## Full transaction query index

| Item | Value |
| --- | --- |
| Directory | `/volume1/blockzilla/token-transaction-dump-state/spyx-query-index-v1-b0738256` |
| Manifest SHA-256 | `2e97ff8d08d978d2c47311e1205a6ef93ee65557fb1b4bc848c17fc4624ba292` |
| Transactions | 7,311,137 |
| Signature occurrences | 7,546,434 |
| Locator bytes | 584,891,088 |
| Locator SHA-256 | `be42857ae682f6ffa4e9f3bec65670255f1f310ba8a25a2bef4597b3738e327c` |
| Signature lookup bytes | 603,714,848 |
| Signature lookup SHA-256 | `0cff5c9f95dd073abf30792a48e1ff8b2140609017a2b52d5fbd1f974197c5fb` |

The source scan completed in 39 seconds at 361.7 MiB/s. It made four bounded
signature-sort runs. The merge and manifest-last publication then completed
successfully. The build log is:

`/volume1/blockzilla/token-transaction-dump-state/spyx-query-index-v1-b0738256.log`

The full verifier exited with status 0. It checked all source and index hashes
and compared all 7,546,434 index occurrences with the exact signature bytes at
the locator-defined source ordinal. Its log is:

`/volume1/blockzilla/token-transaction-dump-state/spyx-query-index-v1-b0738256.verify.log`

## Full postings index

| Item | Value |
| --- | --- |
| Directory | `/volume1/blockzilla/token-transaction-dump-state/spyx-postings-v1-fe7a9b39` |
| Manifest SHA-256 | `891f2e6ac95734960e68670d33e41e8f68b0145af4692d28a5a7f83cf6e1adb6` |
| Complete | `true` |
| Transactions | 7,311,137 |
| Transactions with a target-address posting | 7,311,137 |
| Transactions that mention the mint | 7,093,830 |
| Transactions that do not mention the mint | 217,307 |
| Mint-only transaction coverage | 97.027726% |
| Source registry entries | 1,621,463 |
| Source registry bytes | 51,886,816 |
| Target-address keys | 134,943 |
| Target-address postings | 29,060,229 |
| Program keys | 1,070 |
| Program postings | 39,753,473 |
| Total postings | 68,813,702 |
| Artifact bytes | 553,774,440 |
| Target-address semantic SHA-256 | `c31eda69358b910d3bc9ef824f0e15a9a0f0ca76c182f4ebc70a1ab4a638bd81` |
| Program semantic SHA-256 | `c4b0c302b84b6cd84c7944c7046749ff93b580db0ffea8b702513e5e8e268d75` |

Published files:

| File | Bytes | SHA-256 |
| --- | ---: | --- |
| `target-address-directory.bin` | 3,238,760 | `c742b33b889b3daa0cab4c8cfcf329193d409c13dbb13da30801fe23f82dfbac` |
| `target-address-postings.bin` | 232,481,960 | `b306e9d939159439d4f7b2329d06a28bb4fb03474f5452c00b39833be97817df` |
| `program-directory.bin` | 25,808 | `af45aeacc7f4b254c30ae15e12ce6b618fc6a0e7c0ca82e5261b5fd15c17fb8b` |
| `program-postings.bin` | 318,027,912 | `b595c62a7c8d1a6cc692c79afc2434df1d37b330d0356e6e6a3f783df387699d` |

The manifest binds these consolidated source files:

| File | Bytes | SHA-256 |
| --- | ---: | --- |
| `manifest.json` | 1,202 | `ba894ea5848c4616b87230ff711c9307e70587dc51b3816ac14f1c68e1f616b8` |
| `transactions.wincode` | 14,613,517,576 | `2849a8e8fbe7d8dbb553022355cfd33d0e50971166242534a398334e79d977de` |
| `registry.bin` | 51,886,816 | `eb74ca724b2d8f7bb8effe47048d295083292df33b160d872840d988a72438e7` |
| `accounts.wincode` | 6,045,862 | `e77aa22f96283ede6b8732bd48fdc34567582d09c10a65f024c70cda45f07060` |

The builder scanned 14,613,517,576 transaction bytes at approximately 276 to
300 MiB/s. It made five bounded sort runs. It merged 68,813,702 rows in eight
seconds at approximately 130.6 MiB/s. It published the manifest only after all
required checks passed. The build log is:

`/volume1/blockzilla/token-transaction-dump-state/spyx-postings-v1-fe7a9b39.log`

The independent full verifier exited with status 0. It checked all source and
postings file hashes. It also checked directory order, contiguous ranges,
strict and unique posting order, registry bounds, source transaction bounds,
the mint role, and full target-address coverage. Its log is:

`/volume1/blockzilla/token-transaction-dump-state/spyx-postings-v1-fe7a9b39.verify.log`

The target-address set is the mint plus all 134,942 discovered token accounts.
The program set includes each program used by an outer instruction or a
metadata-recorded inner instruction. Static and loaded message addresses are
included. One key has at most one posting for one transaction.

## Full owner posting index

| Item | Value |
| --- | --- |
| Directory | `/volume1/blockzilla/token-transaction-dump-state/spyx-owner-postings-v1-39e68fd2` |
| Manifest SHA-256 | `39e68fd25ae701a78c3f4b66a8597b65323c9a620b3f9d2f86d9351b0eb8b763` |
| Complete | `true` |
| Transactions | 7,311,137 |
| Transactions with at least one owner posting | 7,229,609 |
| Owner keys | 112,352 |
| Owner postings | 21,691,712 |
| Artifact bytes | 176,230,400 |
| Replay state SHA-256 | `3570f9fb1ebe7e18fbda9d20c80fc16b80edbc0bdae3579347dd89419fb1bfe6` |
| Owner semantic SHA-256 | `ce0872523a204c83a97353a9bc75d00a293421de8a2369254d9430773301154a` |

Published files:

| File | Bytes | SHA-256 |
| --- | ---: | --- |
| `owner-directory-v1.bin` | 2,696,576 | `8fabd787ea8a15703dc07c32bc71562f42c7c21563de6d0a287e767bf06de654` |
| `owner-postings-v1.bin` | 173,533,824 | `655d5fdd5f536e20bebd72c14a45d6ccb9a11e166fffccb8a187bf1649735f3c` |

The builder replays every transaction in source order. It reuses message,
metadata, owner-ID, and registry-cache storage. For each resolved-message SPYx
token account, it unions the owner while open immediately before and after the
validated transaction. This includes the old owner on close or reassignment,
the new owner on initialize or reopen, and the unchanged owner for a failed
transaction. A mint-only transaction has no owner posting.

The preliminary binary built and verified the full artifact. The final binary
then fixed the accepted owner-key count, posting count, replay-state digest,
and owner semantic digest. The final binary verified the same artifact again
before service publication.

## Full execution-proven market database

| Item | Value |
| --- | --- |
| Directory | `/volume1/blockzilla/token-transaction-dump-state/spyx-market-v2-6c00a02c` |
| Manifest SHA-256 | `43d505e708741bd993a2194b8e309fe23a3b66fc2d99cd316b0cd5ad40a0f963` |
| Trade-row SHA-256 | `e79a143f1ec97ace6da179ac126564272ae43bf4066c9cbca0e789e8603b5d0f` |
| Complete | `true` |
| Source transactions | 7,311,137 |
| Successful transactions | 4,922,996 |
| Semantic swap instructions | 5,542,256 |
| Semantic target-swap instructions | 3,738,558 |
| Published proven swaps | 311,455 |
| Pairs | 219 |
| Venue programs | 6 |
| Instruction kinds | 9 |
| Trade rows | 39,866,368 bytes, 128 bytes per row |
| Parser semantic version | `2.0.0` |
| Parser fingerprint | `abce8ca080cac9b3f69db71c0eb560d7333a79d3b5fbc7018bd01bb4c0370c4c` |

The builder persisted raw executed input and output integers. It did not
persist a float price. The API derives an exact reduced rational price and a
separate chart value. Direct USD fields exist only when the quote mint is the
declared mainnet USDC or USDT mint. They are stable-quote units, not an oracle
USD valuation.

Each published row passed all of these fail-closed gates:

- The transaction succeeded and exact metadata was present.
- The venue instruction was a semantic swap. Router instructions did not
  become trades.
- The venue and token-transfer invocation paths were structurally complete and
  proven committed from the execution logs. Caught failed CPIs were excluded.
- The input and output transfers matched the parser's user, vault, and mint
  roles.
- No sibling or unmatched token flow touched a relevant swap account.
- Unknown, malformed, or otherwise unsupported committed token effects caused
  rejection.
- The aggregate committed token transfers matched the exact pre/post token
  balance deltas for every relevant account.

The 5,542,256 trade candidates have one exact disposition:

| Disposition | Instructions |
| --- | ---: |
| Published proven swap | 311,455 |
| Missing token-balance proof | 526,562 |
| Unsupported token instruction | 311,990 |
| Token transfer outside venue subtree | 261,292 |
| Unresolved directional flow | 54,989 |
| Ambiguous flow or ownership | 3,450,236 |
| Target on both or neither side | 617,308 |
| Zero executed amount | 2,470 |
| Balance mismatch | 5,954 |

The 250,000-transaction canary published 1,045 rows and passed its
incomplete-artifact verifier. The full parser scan then read 14,613,517,548
transaction bytes in 93 seconds at 150.3 MiB/s. The full verifier reopened the
source and artifact without an incomplete override and exited with status 0.
The build log is:

`/volume1/blockzilla/token-transaction-dump-state/spyx-market-v2-6c00a02c.log`

## Mint and DEX program identity

The market now has a separate immutable mint metadata artifact:

`/volume1/blockzilla/token-transaction-dump-state/spyx-mint-metadata-v1-4a441b81`

Its artifact SHA-256 is
`e92c9f5b13edae0d5b0b3fef1a2b1cc739eab65e12d608f13ead7bec1809f82a`.
It is bound to the exact market manifest, market trade file, source dump,
registry, Solana mainnet genesis hash, RPC context slot, and decoder version.
The full verifier passed.

The artifact contains exactly 220 swap mints. All 220 mint accounts were
valid, and all 220 current on-chain decimal values matched the market rows.
The decimal distribution is 30 mints with 6 decimals, 17 with 8 decimals,
and 173 with 9 decimals. On-chain display metadata resolved 219 names and
symbols: 21 from Token-2022 metadata and 198 from Metaplex metadata.

The remaining legacy mint,
`bSo13r4TkiE4KumL71LsHTPpL2euBYLFx6h9HP3piy1`, has no Metaplex metadata
account. Its valid 9-decimal mint account remains authoritative. The API uses
one explicit `official_project_site` display fallback for its name
`BlazeStake Staked SOL` and symbol `bSOL`. The fallback is bound to the exact
mint address and cites BlazeStake's official address page. It does not replace
or claim to be on-chain metadata. Thus, the API and UI display names and
symbols for all 220 mints while preserving the source distinction.

The public market term is now **DEX program**. A trade's `program` is the
executed DEX program and is the authoritative DEX volume attribution. The
`router` remains a separate named program, and `pool` remains the exact raw
pool address. Old `venue` fields and query parameters remain as compatibility
aliases. Program names come from the parser-bound program table; they never
replace the stable program addresses.

The 311,455 proven swaps group into these executed programs:

| DEX program | Trades | Primary pools | Routed trades |
| --- | ---: | ---: | ---: |
| Raydium CLMM | 201,506 | 193 | 22,258 |
| Orca Whirlpool | 99,651 | 58 | 16,564 |
| Byreal | 8,852 | 2 | 1,439 |
| PancakeSwap | 1,008 | 3 | 889 |
| Meteora DLMM | 437 | 17 | 211 |
| Meteora DAMM v2 | 1 | 1 | 0 |

There are 274 unique stored primary pool addresses. The 230 Orca two-hop rows
store only the first pool in Market V2, so the API and UI say **primary pools**
instead of claiming complete two-hop pool coverage. DEX and router volumes
overlap and must not be added together.

The current service adds two in-memory time-series indexes. It does not read
the market artifact again for each request:

- `/api/v1/market/slot-candles` returns exact, non-empty slot OHLCV. Trade
  order inside a slot is transaction ID, outer instruction index, and inner
  instruction index. With no lower slot bound, it returns the newest requested
  non-empty slots.
- `/api/v1/market/program-volume` requires a bounded time range. It adds only
  raw SPYx target units across quote pairs and groups them by the executed DEX
  program. Routed counts and volume are subsets. Router IDs are not DEX rows.

The explorer exposes exact Slot and 1m price controls. Its DEX history chart
has 7D, 30D, 90D, and All ranges. It shows the five largest DEX programs in
the range and combines all remaining program IDs as Other. Empty time buckets
are zero; the chart does not carry the prior bucket value across a gap. The
all-time DEX table is ranked by SPYx volume.

## Active service

| Item | Value |
| --- | --- |
| tmux session | `spyx-query-service-b06fc848-c6b54104` |
| Bind address | `192.168.1.46:18787` |
| Maximum source reads | 12 |
| Public origin | `https://spyx.blockzilla.dev/` |
| Cloudflare tunnel | `blockzilla-receiver` (`0759f2df-729a-4d8c-a2eb-54bd5d16dac7`) |
| Service binary | `/volume1/blockzilla/bin/blockzilla-spyx-query-b06fc848f9a58be6f493294eddda8159caad75a7696eb1a04f7ae0d4498c8758` |
| Service binary SHA-256 | `b06fc848f9a58be6f493294eddda8159caad75a7696eb1a04f7ae0d4498c8758` |
| Transaction index | `/volume1/blockzilla/token-transaction-dump-state/spyx-query-index-v1-b0738256` |
| Postings index | `/volume1/blockzilla/token-transaction-dump-state/spyx-postings-v2-2bf2c428564412d2760590e25cd7ba19dcafa0ff272c0cf85cac7772a17adfd4` |
| Owner postings and balance history | `/volume1/blockzilla/token-transaction-dump-state/spyx-owner-postings-v2-ba215422` |
| Market database | `/volume1/blockzilla/token-transaction-dump-state/spyx-market-v2-2bf2c428564412d2760590e25cd7ba19dcafa0ff272c0cf85cac7772a17adfd4` |
| Mint metadata | `/volume1/blockzilla/token-transaction-dump-state/spyx-mint-metadata-v1-2bf2c428564412d2760590e25cd7ba19dcafa0ff272c0cf85cac7772a17adfd4` |
| Static explorer | `/volume1/blockzilla/web/spyx-explorer-c6b5410480299ecf` |
| Static explorer archive | `/volume1/blockzilla/web/spyx-explorer-c6b5410480299ecf.tar.gz` |
| Static explorer archive SHA-256 | `c6b5410480299ecf7e4d1f3877caa4828708cf6386ff65ff8dffa5fbd9868785` |
| Static summary SHA-256 | `3541efaa11d783e4bdae1dd9a93912b65f642ebcab955142959a49396b257219` |
| Static programs SHA-256 | `239330a877a2ae891b398a5cf8c666184da76759f86f8bca5c366de979087305` |
| Service log | `/volume1/blockzilla/token-transaction-dump-state/spyx-query-service-b06fc848-c6b54104.log` |
| Index mode | all five artifacts complete; no incomplete-artifact override |

The service is intentionally bound only to the NAS LAN address. It is not
bound to every NAS interface. It serves the static explorer and API on the same
origin at `http://192.168.1.46:18787/`. The Svelte SPA fallback applies only to
page navigation. Missing API, health, application-asset, and report-data paths
remain HTTP 404 responses.

The proxied `spyx.blockzilla.dev` DNS record sends public HTTPS traffic through
the centrally managed `blockzilla-receiver` Cloudflare Tunnel to the same LAN
origin. The NAS service stays LAN-bound. The public UI and read-only API do not
require authentication.

Routes:

- `GET /` and the explorer page routes, including `GET /api-docs`
- `GET /healthz`
- `GET /api/v1/transactions/{id}`
- `GET /api/v1/transactions/by-signature/{signature}`
- `GET /api/v1/transactions/by-coordinate?epoch=&slot=&source_block_id=&tx_index=`
- `GET /api/v1/postings/target-address/{key}?limit=&cursor=`
- `GET /api/v1/postings/token-account/{key}?limit=&cursor=`
- `GET /api/v1/postings/program/{key}?instruction_scope=all|direct|inner&limit=&cursor=`
- `GET /api/v1/postings/owner/{key}?limit=&cursor=`
- `GET /api/v1/market/provenance`
- `GET /api/v1/market/summary?quote_mint=`
- `GET /api/v1/market/pairs`
- `GET /api/v1/market/mints`
- `GET /api/v1/market/mints/{address}`
- `GET /api/v1/market/programs`
- `GET /api/v1/market/trades?quote_mint=&program=&time_from=&time_to=&offset=&limit=`
- `GET /api/v1/market/trades/{trade_id}`
- `GET /api/v1/market/candles?quote_mint=&interval=&time_from=&time_to=&program=&max_points=`
- `GET /api/v1/market/slot-candles?quote_mint=&program=&slot_from=&slot_to=&max_points=`
- `GET /api/v1/market/program-volume?interval=&time_from=&time_to=&quote_mint=&max_points=`

The trade and candle routes still accept `venue=` as an alias for `program=`.

The final smoke test proved these points:

- Health returned `complete: true` for all posting indexes. It returned
  7,311,137 transactions, 29,060,229 target-address postings, 39,753,473
  program postings, and 21,691,712 owner postings for 112,352 owner keys.
- Signature and coordinate lookup both returned transaction ID 0 and the same
  first signature.
- Transaction ID 7,311,136 returned the final indexed transaction at epoch
  1018, slot 440,207,988.
- The SPYx mint target-address query returned 7,093,830 transactions.
- Exact USDC slot OHLCV returned slot 440,207,089 as the latest non-empty
  slot, with the same price and raw volumes through LAN and public HTTPS.
- The bounded DEX-program series reconciled each bucket total with its
  executed-program rows. Router volume remained a subset.
- Browser checks at 390 and 1,280 CSS pixels loaded both charts with no page
  overflow or alert. Slot and 1m controls returned 1,000 non-empty slots and
  527 non-empty minute candles for the default USDC pair.
- A target token-account query returned 649 transactions.
- The Compute Budget program query returned 7,013,351 transactions.
- Cursor paging returned the next ordered transaction row. The cursor stayed
  bound to its exact kind, key, offset, and manifest.
- The mint failed as a token-account query, as required.
- Owner `49mmYSJPUMgu2AcV7u3U3vQ3vean6PbDzV9pw1RmQeQh` returned 21 ordered
  owner-linked rows. The result includes transaction IDs 1,103,537,
  1,103,540, and 2,027,754. The visible Search page returned the same 21 rows
  with no browser warning or error.
- A 12-way test completed 240 transaction reads without an error. Health stayed
  OK after the test.
- Market health returned `complete: true`, 311,455 proven swaps, 219 pairs, six
  DEX programs, and the exact source transaction SHA-256. It exposes
  `programs: 6` and retains `venues: 6` for old clients.
- Mint metadata health returned `complete: true`, 220 mints, 220 valid mint
  accounts, 219 on-chain names, and one explicit unresolved on-chain record.
  The mint API returned 220 verified decimal matches and 220 display names and
  symbols after the source-labeled bSOL fallback.
- The DEX program endpoint returned six named programs whose trade counts sum
  exactly to 311,455. Program-filtered trades and candles returned the named
  DEX program, the raw compatibility address, the separate router, and the raw
  primary pool address.
- A live payload check confirmed that all 21 Token-2022 mint rows use the
  canonical API spelling `token2022` and that the production explorer accepts
  that spelling. The final mint response contained 220 rows and 220 names.
- The latest USDC trade returned transaction ID 7,311,109 and named
  `commit_proven` in its evidence. The same transaction ID resolved through the
  original transaction endpoint.
- Summary, pair, provenance, paged trade, and bounded OHLCV routes returned
  valid data. The complete USDC pair contained 293,327 published swaps.
- The pair selector is sorted by complete transaction count in descending
  order, with quote-mint registry ID as the stable tie-break.
- The market view uses TradingView Lightweight Charts 5.2.1 with the verified
  OHLC rows in a candlestick pane and verified executed quote volume in a
  separate pane. The TradingView attribution and official NOTICE remain in the
  static release.
- The explorer's production build passed its exact history and strict replay
  SHA-256 gates. A browser test checked the market summary, descending pair
  order, candlestick and volume panes, pair and interval changes, a 390-pixel
  viewport, the trade table, and the transaction-ID search handoff. It reported
  no browser warning or error.
- The mobile design release checked every page at 360 and 390 pixels and every
  page at 1,440 pixels. No page had document-level horizontal overflow. Mobile
  navigation uses five equal-width items. Programs, swaps, balance ranges, and
  large movements use compact mobile rows or cards. The initial public lists
  contain 25 programs and 10 recent swaps.
- The public Data integrity page no longer shows provider-access comparisons,
  RPC pricing inputs, compact-build counters, or raw replay-counter dumps. It
  keeps the coverage status, six missing-data checks, selected replay checks,
  coverage limits, full source hashes, and the non-blocking malformed-token
  instruction count.
- The API-reference page listed all public GET routes, used the serving origin
  in its copyable examples, and passed health, owner, market, and strict API-404
  checks on the production NAS service.
- The public holder filter returned signer wallets, attributed PDAs, unknown
  off-curve authorities, and other on-curve authorities. A program selector
  used all 76 program aggregates and all 583 current positive-balance
  attributed PDAs. The holder and program tables sorted by current holding or
  public activity. A 390-pixel browser test checked the filters, sorting,
  program names, program totals, and document width. It reported no browser
  warning or error.
- Cloudflare's `1.1.1.1` and Google's `8.8.8.8` resolvers returned the proxied
  public record. Public HTTPS returned HTTP 200 for the explorer, API reference,
  health, owner, and market routes. An unknown API route remained HTTP 404.
- Final local acceptance passed 81 query-service tests, 109 token-dump replay
  tests, strict Clippy with zero warnings, Svelte checks with zero errors and
  zero warnings, and all 62 explorer API-reference and data-contract tests.

The NAS `curl` package cannot start because `libquiche.so.0` is missing. This is
a NAS package fault. The smoke tests used the working NAS `wget` client.

## Firescope parity

`apps/spyx-explorer/FIRESCOPE-PARITY.md` contains the checked feature map. The
explorer now adds exact executed-swap prices, stable-quote volume, OHLCV,
pairs, DEX programs, primary pools, instruction paths, and source-transaction
links to the existing holder, public-balance, program, and audit views. It labels price as
the latest executed swap, not an oracle or liquidity-weighted market price.
Rejected candidates are not included in trade volume.

The target-address, program, and owner posting indexes passed their full
acceptance gates. Owner-linked target history uses strict replay state
immediately before and after each transaction. It is not signer history or
actor history.
