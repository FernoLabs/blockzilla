# Live block, gRPC and shred sources for Hivezilla — 25 September 2026

Follow-up: the [26 September decision and benchmark audit](hivezilla-provider-decision-2026-09-26.md)
supersedes the trial order and acceptance procedure below. Test Shyft first. The
volume figures are RPC-based estimates, not measurements of a live gRPC stream.
This broader review remains research history, not provider acceptance evidence.

Status: sourcing review for the live relaunch. No provider was purchased or tested
against a live key. Every price and term below was read on 25 September 2026 from the
vendor's own page unless marked otherwise. EUR figures convert at 1.14 USD.

Selection order requested: no bandwidth cap or per-byte metering first, reliability
second, cost third. Latency does not matter. The subscription being priced is the one
`record-grpc-raw` opens today: `SubscribeRequestFilterBlocks` with `include_transactions`
and `include_entries`, moving from Confirmed to Finalized, with `from_slot` replay after a
reconnect ([live relaunch review](../architecture/hivezilla-live-relaunch.md)).

## Verdict

1. Run a non-voting Agave node with the Yellowstone plugin on Solana-friendly bare metal
   as the primary source. It is the only source that delivers full blocks with entries at
   Finalized, a replay window of your choosing, complete shreds, and a local ledger for long
   gaps, with no redistribution clause. Cheapest reliable EU options: Cherry Servers Agave
   config at EUR 711 per month, or OVHcloud Scale-a2 at EUR 1,410 if a contractual
   unmetered guarantee matters more than price.
2. Do not run the node on Hetzner. Hetzner's own statement is that a single blockchain node
   violates its terms, and it locked Solana operators out in November 2022. The current
   256 GB Hetzner box also costs EUR 842 per month since the June 2026 repricing.
3. Add one flat-rate provider stream as the independent second observation. Shyft Build
   ($199) and constant-k Nexus ($179) are the two to trial. Both need a probe before paying
   for more than one month, because neither documents entries on full blocks.
4. Keep the existing zero-stake shred reader. Do not buy a paid shred feed: DoubleZero Edge
   forbids redistribution of derived data, and no paid feed guarantees every shred.
5. Use Old Faithful as the free gap repair for closed epochs. It includes PoH entries and
   full transaction metadata and appears 13.5 to 15.5 hours after an epoch ends.

Estimated monthly cost: EUR 711 node plus $199 provider, about $1,010. Provider-only
fallback without a node: Shyft plus constant-k, $378, with entries unproven.

## What the stream costs in bytes

The estimated volume is about three times the earlier planning figure. The estimate
includes `TransactionStatusMeta` (logs, balances, inner instructions). It comes from
eight RPC blocks with modeled protobuf sizes and entry counts; the compression test
uses a synthetic payload. It does not measure the exact live subscription or peaks.

| Measure | Value | Basis |
| --- | --- | --- |
| Uncompressed gRPC stream | about 12 MB/s central, 9 to 16 MB/s range, 20 MB/s busy peaks | 8-block `getBlock` samples and `getRecentPerformanceSamples` on 25 September 2026, sized against `geyser.proto` |
| Per month uncompressed | about 33 TB; use 42 TB for cap checks | 30.44 days |
| Per month with zstd per message | 5 to 13 TB | Helius reports 70 to 80 percent reduction; a per-block test gave 5 to 6.5x |
| Raw ledger bytes (what shreds carry) | about 3.8 MB/s | 8 finalized blocks, mean 0.94 MB of serialized transactions |
| Shred spool on the NAS | 25 to 32 GB per 10,000 slots | [fresh raw shred audit](../operations/fresh-raw-shred-audit.md) |
| Epoch length | 31h54m to 32h03m (about 266 ms effective per slot) | `getBlockTime` on the boundary slots of epochs 1039 to 1041 |

Slots have been 250 ms since epoch 1037 on 18 September 2026 (SIMD-0525), so a month holds
about 23 epochs. Any provider replay window quoted in slots is now shorter in wall-clock
time: 150 slots is about 40 seconds, 1,000 slots about 4 minutes.

## Providers ruled out by metering

These fail the first criterion at 33 TB per month whatever their quality. They remain fine
for short replay or repair.

| Provider | Metering | Cost at 33 TB/month | Source |
| --- | --- | --- | --- |
| Helius LaserStream | 2 credits per 0.1 MB uncompressed, about $100/TB; add-ons are Professional-only | about $3,300 in credits, or $999 plus a 25 TB add-on at $1,750 | [pricing](https://www.helius.dev/pricing) |
| Triton One Dragon's Mouth / Fumarole | $0.08/GB across all services | about $2,640 | [pricing](https://triton.one/pricing) |
| QuickNode gRPC | 15 credits per 0.1 MB from 1 October 2026, compressed bytes on port 443 | about 5 billion credits, above the $2,999 tier | [docs](https://www.quicknode.com/docs/solana/yellowstone-grpc/overview) |
| Alchemy gRPC | $75/TB | about $2,475 | [product page](https://www.alchemy.com/solana-grpc) |
| dRPC Yellowstone add-on | 5 TB or 10 TB included, then $30 or $20 per TB | $399 to $599 plus overage | [docs](https://drpc.org/docs/solana-yellowstone-geyser-grpc) |
| Tatum gRPC | 2 credits per message, hard stop when credits run out | Pro 40M credits $349 | [docs](https://docs.tatum.io/docs/grpc-data-streams.md) |
| constant-k Blade Runner | 25 MB/s throttle with a 64 MB buffer, then drops the oldest updates | $49 but lossy at bursts | [plan limits](https://www.constant-k.com/plan_limits/) |
| rpc edge | 4 TB or 10 TB per period, then throttled; site and docs offline | $249 to $499 | Common Crawl capture of rpcedge.com/pricing |
| NOWNodes, Ankr | request-count or per-GB metering; no confirmed Solana Yellowstone product | n/a | [nownodes.io/grpc](https://nownodes.io/grpc), [ankr pricing](https://www.ankr.com/rpc/pricing/) |
| Syndica ChainStream, InstantNodes | retired 14 February 2026; domain defunct | n/a | [Chainstack migration note](https://docs.chainstack.com/docs/migrating-from-syndica-to-chainstack) |

## Flat-rate gRPC candidates

Replay windows are wall-clock at 250 ms slots. "Entries" means whether the vendor documents
`include_entries` on a full-block subscription. Reliability is 1 to 5 from the adversarial
verification round.

| Provider and plan | Monthly | Bandwidth wording | All blocks, empty filter | Entries on blocks | Finalized on blocks | Replay of blocks | Rel. | Notes | Source |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Shyft Build (10 connections) | $199; $179 quarterly; about $159 semi-annual | "No Credits. No Bandwidth Limits. No Overages."; calculator lists "All Blocks 26 TB" at the flat tier | Yes in Shyft's own 2026 Rust example (`account_include: vec![]`, `include_transactions`, Finalized), although the docs page says "at least one filter" | Never enabled in any Shyft example; server allowance unknown | Yes (docs and example) | 150 slots, about 40 s | 3 | Six regions incl. Frankfurt and Amsterdam (hosted at Cherry and Latitude). No terms of service exist; the "Terms" link is a 2023 privacy policy. No status page, a public Grafana lag dashboard over plain HTTP. Forced disconnect of slow clients. Runs a custom fork; production branch was at geyser 13.1.0 on 23 June 2026. Refund only within 3 days. Five-person company founded 2022. | [pricing](https://shyft.to/solana-rpc-grpc-pricing), [filter limits](https://docs.shyft.to/solana-yellowstone-grpc/solana-grpc-filter-limits), [Rust example](https://github.com/Shyft-to/solana-defi/blob/main/general-grpc-examples/Rust/block-stream-latency-test/src/blocks.rs), [blocks doc](https://docs.shyft.to/solana-yellowstone-grpc/docs/streaming-blocks-and-blocksmeta) |
| constant-k Nexus | $179; $1,790 yearly | "Unthrottled" subscribe bandwidth; "no credits and no overage fees" | Match-all streams are Nexus-only, max 3 per region; whether an unfiltered blocks filter counts is undocumented | No: `include_entries` on a blocks filter is refused by the node; use a separate `entry` filter and join on slot | Yes | 24 h store per region, "a span, not a promise"; full-block replay is not among the verified replay types | 3 | New York (InterServer) and Frankfurt (latitude.sh FRA2). Stock yellowstone-grpc 15.2.1. 200 streams. Live status dashboard without history, 99.99 percent "goal", no refund for downtime, limits may change at any time. 3-day trial. Domain registered September 2023. | [pricing](https://www.constant-k.com/pricing/), [plan limits](https://www.constant-k.com/plan_limits/), [filters](https://docs.constant-k.com/kaldera/filters/), [replay](https://docs.constant-k.com/kaldera/replay/), [terms](https://www.constant-k.com/terms/) |
| OrbitFlare Shared gRPC (1 IP, 50 connections) | $500; dedicated node from $1,800 | "Unlimited real-time streaming data"; "does not impose monthly credit limits or request caps" | Implied: docs warn that "no account filter" streams are very heavy and suggest a dedicated node | Yes, `includeEntries` documented | Yes: "Full archive-quality block data: Block subscription at finalized" | Not documented anywhere | 3 | Only vendor whose SLA (99.99 percent, 10 to 100 percent credits) names gRPC. Status page on Better Stack. Terms allow termination for any reason. Realistic tier for a 24/7 unfiltered stream may be the $1,800 dedicated node. | [pricing](https://orbitflare.com/pricing), [blocks doc](https://docs.orbitflare.com/data-streaming/yellowstone-slot-block-monitoring), [SLA](https://orbitflare.com/sla) |
| NoLimitNodes Ultra (10 streams) or Pro (2 streams) | $199 or $49; Ultra $1,990 yearly | "no per-byte meter", "unlimited payload" | Yes: docs example subscribes to all blocks with `accountInclude: []` | Documented flag; every example sets it false | Finalized listed; no blocks example | Contradictory: product page says "we don't run a replay buffer", docs say "plan dependent" | 2 | Single region (Pittsburgh, TeraSwitch). Status API showed the WebSocket service unhealthy for hours on 24 September 2026 while gRPC stayed healthy. Terms give "no guarantees". | [pricing](https://nolimitnodes.com/pricing), [blocks doc](https://nolimitnodes.com/docs/api-reference/grpc/transactions-slots-blocks), [status](https://nolimitnodes.com/status) |
| Solana Vibe Station Basic (5 connections) | $150; $40 weekly; $15 day pass in docs only | "No credits, no usage meter"; only connection count differs by tier | Not documented | Listed as a subscription type; no flag documented | Not documented | Cart says "Replay 1000 slots" (about 4 min); Yardswitch ($500) rejects `from_slot` on blocks | 3 | Atlanta and Amsterdam, own hardware. SLA covers RPC and VPS only, gRPC is not a status-page component. Two-founder company. | [pricing](https://solanavibestation.com/pricing), [cart](https://cloud.solanavibestation.com/index.php?/cart/streaming/), [Yardswitch](https://docs.solanavibestation.com/api-reference/streaming/yardswitch) |
| Solana Tracker standalone gRPC (25 connections) | EUR 200, about $228 | "no credits, no bandwidth caps, no overage charges" | Not documented | Documented flag; example sets it false | Finalized listed | None: docs say do not assume replay | 2 | Terms 6.4 forbid redistribution of raw API data without written authorization, 6.6 allows termination for "excessive usage". No gRPC uptime evidence. | [product](https://www.solanatracker.io/yellowstone-grpc), [terms](https://www.solanatracker.io/terms) |
| Corvus Labs Starter | $160 per region | Streams "billed by plan capacity rather than volume"; own table shows $160 flat from 5 to 100 TB | Not documented | Not documented | Yes | No: replay is Processed-only and "blocks are not possible to replay" | 2 | Five metros incl. Frankfurt and Amsterdam. No terms of service, SLA, refund policy or legal entity published; status page broken; endpoints are plaintext with IP-allowlist auth. Domain registered September 2024. | [pricing](https://docs.corvus-labs.io/platform/pricing), [usage](https://docs.corvus-labs.io/solana-rpc/grpc/usage) |
| ERPC shared Standard | EUR 198; dedicated EUR 1,580 to 2,980 | No metering on gRPC SKUs; terms allow rate limits without notice | Unclear | Never documented | Yes | No for blocks: Richat backend refuses, `from_slot` only at Processed | 2 | Own copy says gRPC delivery "exceeds 99%"; no SLA. | [gRPC page](https://erpc.global/en/grpc/) |
| Chainstack Yellowstone add-on | $89 to $198 | "Unlimited stream events" | Unclear | No: reference client marks `include_entries` "Not allowed" | Unclear | about 100 slots | 2 | 17 Solana mainnet incidents between 27 June and 21 September 2026 including a 158-hour degradation; runs a release candidate (v16.0.0-rc10). Eliminated. | [docs](https://docs.chainstack.com/docs/yellowstone-grpc-geyser-plugin) |
| RPC Fast SaaS Stream / Aperture | $249 / $499 | "unlimited bandwidth" | No: "Empty account_include is not supported" | No: entries "disabled" on blocks | Yes | Undocumented | 2 | Eliminated by design. | [docs](https://docs.rpcfast.com/rpc-fast-saas-solana/data-streaming/yellowstone-grpc.md) |
| Allnodes managed Full Node | $1,920 shared node, $3,840 dedicated | "Unlimited requests"; terms allow resource changes at any time | No plugin config access | Unclear | Unclear | Unclear | 3 | Two to four times a self-run node with less control. | [hosting](https://www.allnodes.com/sol/host) |
| GetBlock dedicated node plus Yellowstone add-on | $2,900 to $3,200 | "unlimited requests"; no written no-cap guarantee | Unclear ("Fast Yellowstone" fork) | Undocumented | Yes | Undocumented | 3 | The $400 add-on price on the marketing page is stale; the add-on is $0. | [pricing](https://getblock.io/pricing.md) |
| ThorNode, Subglow, PublicNode | NFT pass; $99 to $249; free with token | n/a | ThorNode: block filters "return no updates"; Subglow: pre-filtered only; PublicNode: terms forbid data extraction | no | no | no | 1 to 2 | Eliminated. Subglow's documented endpoints return Vercel not-found. | [ThorNode](https://docs.thornode.io/api-reference/yellowstone/subscribe.md), [Subglow](https://subglow.io/pricing), [PublicNode terms](https://www.publicnode.com/terms) |

Two upstream facts apply to every provider. The shipped yellowstone-grpc example config
sets `replay_stored_slots` to 0 and `blocks.include_entries` to false, so a provider that
copied it has no replay and no entries; and geyser 14.1.0 (3 July 2026) fixed missing
Finalized block messages, 14.2.1 (22 July 2026) fixed `from_slot` replay never delivering a
block. Current release is 15.2.1. Ask every vendor for its version
([changelog](https://raw.githubusercontent.com/rpcpool/yellowstone-grpc/master/CHANGELOG.md),
[example config](https://github.com/rpcpool/yellowstone-grpc/blob/master/yellowstone-grpc-geyser/config.json)).

## Shred sources

Jito ShredStream shut down on 5 September 2026 ("The service will be completely shut down
in 60 days (September 5, 2026)", [docs.jito.wtf](https://docs.jito.wtf/lowlatencytxnfeed/)).
No paid feed promises every shred for every slot.

| Source | Monthly | Completeness | Restrictions | Source |
| --- | --- | --- | --- | --- |
| Own zero-stake reader (current) | $0 | Incomplete by design: unstaked nodes sit at the end of the Turbine tree and are served repair last | none | [cluster_nodes.rs](https://github.com/anza-xyz/agave/blob/master/turbine/src/cluster_nodes.rs) |
| Own non-voting Agave node | in the bare-metal price | Complete: full Turbine plus repair; shreds retained in the local ledger | none | see self-host section |
| DoubleZero Edge | $450 per seat per month "all other locations"; $900 London, New York, Singapore, Tokyo; $1,500 Frankfurt and Amsterdam. Billed monthly per seat, not per epoch; the per-epoch CLI path was decommissioned on 30 August 2026 although the onchain program still lists per-epoch base prices | Leader shreds from validators publishing directly (58 percent of stake in June 2026) plus regional retransmit groups marketed as "100% of shreds available"; terms disclaim completeness and continuity; UDP multicast over GRE, no replay | Terms of Use v2.0 section 2(e)(ii): Received Data may not be made available to third parties "whether in raw, processed, derived, or aggregated form"; internal retransmission under common ownership is allowed; redistribution needs a separate request. A published compact archive built from Edge shreds plausibly falls inside the prohibition. USDC prepaid balance required. | [pricing](https://doublezero.xyz/pricing), [docs](https://docs.malbeclabs.com/Edge%20Subscriber%20Connection/), [terms](https://doublezero.xyz/terms-protocol) |
| Everstake Blockspace (ex-Extrnode) | Direct Shreds $4,000 per IP per region; ShredStream tiers unpublished | "Shared lanes can drop messages under burst load" | Internal use only, no incorporation into third-party products | [pricing](https://docs.blockspace.everstake.com/direct-shreds/pricing) |
| Helius raw shreds | $1,000 per IP ($800 on Professional) | not stated | none found | [pricing](https://www.helius.dev/pricing) |
| Triton Shred Streaming | from $450 per IP per data centre, or $0.18 per 15 minutes | not stated | per-GB billing on other products | [pricing](https://triton.one/pricing) |
| OrbitFlare Shredstream | $500 standard region, $1,000 Frankfurt, Amsterdam, New York, London | "about 50 to 100 Mbps for a full shred stream"; no guarantee | needs public IPv4 and open UDP port | [docs](https://docs.orbitflare.com/shredstream.md) |
| ERPC UDP shred forwarding | EUR 398 | own FAQ: "over 90% reliability" | dedicated products discontinued 5 September 2026 | [shreds](https://erpc.global/en/shreds/) |
| NoLimitNodes UltraShreds | $100 per destination (one included on Ultra) | docs expect missing packets and incomplete shred sets | max 5 destinations | [product](https://nolimitnodes.com/products/shredstream) |
| GetBlock, Solana Vibe Station, Corvus, Shreder, BlockRazor, AllenHark | $250 to $300, or unpublished | packet loss acknowledged or unstated | various | vendor pages |
| DedicatedNodes LocalShred | free with an Amsterdam or Rotterdam server | re-fanout, "no undertaking as to completeness" | no redistribution; may be discontinued without notice | [page](https://www.dedicatednodes.io/jito-shredstream-alternative/) |
| Shyft RabbitStream | included on Build | decoded pre-execution transactions, not raw shreds | n/a | [overview](https://docs.shyft.to/solana-shredstreaming/rabbitstream-overview) |

## Self-hosted non-voting node

Anza's requirements for an RPC-class node: 256 GB RAM or more (512 GB only with all
account indexes, which Hivezilla does not need), accounts and ledger on separate NVMe, and
"1 GBit/s symmetric connection is sufficient" for unstaked nodes
([requirements](https://docs.anza.xyz/operations/requirements)). Run yellowstone-grpc-geyser
14.2.1 or later, set `filter_limits.blocks.include_entries` to true and `replay_stored_slots`
to several thousand. A zero-stake node is never leader, so the upstream "zero entries on
self-produced blocks" bug does not apply. Turbine is still the propagation path; Alpenglow
Rotor "is not yet scheduled" ([solana.com](https://solana.com/upgrades/alpenglow)).

Traffic of an unstaked node: it is a leaf of the Turbine tree and gets weight 1 in repair
peer sampling, so retransmit and repair-serving egress are about zero. AWS measured that
an RPC-only Agave node stays in sync with outbound shaped to 20 Mbit/s (6.5 TB per month),
while unshaped RPC-only nodes emit 100 to 200 TiB per month
([AWS, September 2025](https://aws.amazon.com/blogs/web3/improve-solana-node-performance-and-reduce-costs-on-aws/)).
RPC Fast reports 40 TB in plus 40 TB out per node
([case study](https://rpcfast.com/blog/solana-node-cluster-magneta-labs)). Ingress is about
25 TB per month of Turbine plus gossip and is free everywhere below. Plan for 100 TB or an
unmetered port; a 20 TB allowance only works shaped and monitored.

| Host | Config | Monthly | Traffic | Solana policy | Notes | Source |
| --- | --- | --- | --- | --- | --- | --- |
| Cherry Servers (Amsterdam, Frankfurt, others) | "Mainnet Solana Agave Validator": EPYC 9254P 24c, 384 GB ECC, 2x1 TB plus 2x4 TB NVMe, up to 10 Gbps | EUR 711 (about $811) | 100 TB egress free, then EUR 0.50/TB; unlimited ingress | Dedicated Solana line | Established host; Shyft hosts its Amsterdam and Frankfurt gRPC here | [solana servers](https://www.cherryservers.com/solana-servers) |
| HOSTKEY (Netherlands, EU) | bm.v4-epyc-mini-10g: 32 vCPU EPYC, 384 GB, 2x2 TB NVMe, 10 Gbps | EUR 559; EUR 699 for 96 vCPU | 50 TB included; unlimited 1 Gbps for EUR 100 more; fair use: no more than 70 percent of a 1 Gbps port for over 3 hours a day | Dedicated Solana page | Cheapest flat setup found; the fair-use rule is comfortable at 12 MB/s | [solana](https://hostkey.com/dedicated-servers/solana/), [traffic](https://hostkey.com/services/traffic/) |
| DedicatedNodes.io (London) | EPYC 9274F 24c, 384 GB, 2x1.6 TB enterprise NVMe, 2x10 Gbps | EUR 585 (about $667); Amsterdam 512 GB EUR 612.50 | three conflicting statements: 335 TB, 100 TB then EUR 0.726/TB, or "Solana traffic included" | Solana product line | Dutch sole trader, network since February 2025, terms disclaim uptime | [solana servers](https://www.dedicatednodes.io/solana-servers/) |
| ERPC SLV Metal MV (nine cities incl. Frankfurt, Amsterdam) | EPYC 9254 24c, 384 GB ECC, 1x1 TB plus 2x4 TB NVMe, 2x10 Gbps | EUR 798 (about $910) | 200 TB included; overage unstated | Solana-specialised | | [bare metal](https://erpc.global/en/baremetal/) |
| Edgevana (Miami only) | EPYC 9254 24c, 384 GB, 2x480 GB SSD plus 2x1.92 TB NVMe, 25 Gbps | $799 | 100 TB; overage and rental terms unpublished | Solana Foundation partner since 2022 | | [catalogue](https://nodes.edgevana.com/servers/solana-mainnet) |
| TeraSwitch (EU sites incl. Frankfurt, Amsterdam) | Genoa 9254 24c, 384 GB, 2x1.92 TB NVMe, 2x25 Gbps | about $757 (component sum) | 150 TB pooled per region, then $1.25/TB prepaid or $10/TB | no Solana page any more | hourly billed, no contract | [pricing](https://teraswitch.com/pricing) |
| OVHcloud Scale-a2 (France) | EPYC 9255 24c, 384 GB, 2x1.92 TB NVMe | EUR 1,410 plus one month setup | "5Gbps guaranteed unmetered public bandwidth"; contract asks not to use bandwidth "in an intensive manner" on non-guaranteed ranges | "All Proof of Stake (PoS) validators, staking providers, RPC nodes ... are fully supported" | Only vendor with an explicit PoS-nodes-welcome policy and a guaranteed unmetered port; 99.99 percent SLA | [Scale](https://www.ovhcloud.com/en/bare-metal/scale/), [blockchain](https://us.ovhcloud.com/solutions/blockchain/) |
| Allnodes bare metal (Frankfurt) | EPYC 9375F 32c, 384 GB ECC, 2x3.84 TB | about $1,050 list, about $840 on 12-month prepay | 50 TB included, $1/TB, or 300 TB "Solana Package" for $80 | Solana-specialised | | [options API](https://www.allnodes.com/api/v2/hosting/options) |
| Latitude.sh | "Solana Mainnet Validator Node" 9275F 24c, 768 GB, 2x100 Gbps | $1,341 | 20 TB free egress, then $0.01/GB or $1.25/TB prepaid | Solana line | metered above 20 TB | [solana](https://www.latitude.sh/solutions/solana) |
| Hivelocity | TR PRO 7965WX 24c, 1 TB RAM, 512 GB plus 2x2 TB NVMe, 10 Gbps | $750; unmetered 1 Gbps port from $199 more | 20 TB then $0.05/GB | "explicitly welcomes Solana validators" | | [solana servers](https://www.hivelocity.net/solana-servers/) |
| Hetzner AX162-2 (Falkenstein, Helsinki) | EPYC 9454P 48c, 256 GB DDR5 ECC, 2x1.92 TB NVMe DC | EUR 842.30 plus EUR 419 setup, plus EUR 1.70 IPv4; AX162-3 512 GB EUR 1,597.30 | 1 Gbit uplink unlimited; 10 Gbit add-on EUR 43 with 20 TB then EUR 1/TB | Terms ban "mining, farming and plotting"; Hetzner Official on Reddit, August 2022: "Even if you just run one node, we consider it a violation of our ToS"; November 2022 lockout of over 1,000 Solana validators; no clarification since | Catalogue standardised on 15 June 2026; the old AX162-R at EUR 242 no longer exists for new orders. Not recommended for the node. | [prices](https://docs.hetzner.com/general/infrastructure-and-availability/price-adjustment/), [traffic](https://docs.hetzner.com/robot/general/traffic/), [terms](https://www.hetzner.com/legal/terms-and-conditions/), [2022 statement](https://web.archive.org/web/2022id_/https://old.reddit.com/r/hetzner/comments/wucxs4/is_it_allowed_to_run_a_crypto_block_chain_node/), [Solana Foundation report](https://solana.com/news/validator-health-report-march-2023) |
| server.net (Amsterdam), is*hosting (Netherlands), NoLimitNodes (Frankfurt), Unihost (Poland), Vultr | 256 GB to 512 GB options | $275 plus $60 setup; $450; $926; EUR 960; $825 | 100 TB; "unmetered" (terms unverified); unmetered; 300 TB; 10 TB | various | 256 GB tiers meet only the minimum; server.net stock is thin; Vultr disks too small | [server.net](https://server.net/dedicated-servers/solana-validator), [is*hosting](https://ishosting.com/en/dedicated/solana-servers), [NoLimitNodes](https://nolimitnodes.com/products/bare-metal), [Unihost](https://unihost.com/solana-server/) |

The Hetzner policy also touches the current deployment: the live shred reader on the
Hetzner box joins gossip with its own identity and TVU port, which is a node under Hetzner's
2022 reading. Consuming a third-party gRPC stream is not obviously covered. Treat this as a
known risk of the existing box rather than a reason to move today, and keep the reader's
data replicated off the host, as the relaunch design already requires.

## Historical and repair sources for rebuilding epochs

| Source | Cost per epoch (432,000 slots) | Lag | Entries | Notes | Source |
| --- | --- | --- | --- | --- | --- |
| Old Faithful OF1 CAR archive | $0; 672 to 917 GiB per epoch for epochs 1030 to 1041 (mean 767 GiB CAR-only; 707 to 957 GiB with indexes on the [baseline](https://blockzilla.dev/baseline/) basis) | complete set public 13.5 to 15.5 hours after epoch end for epochs 1039 to 1041 (bracketed from the check logs and the 4-hourly CAR report commits); docs say 10 to 20 hours | Yes: PoH entries, shred boundaries, full transaction metadata including logs, rewards; all twelve recent recaps report zero missing metadata | Stored in Amsterdam behind Cloudflare, range requests supported, no directory listing, no terms of service and no SLA ("currently completely free to use"). GSFA indexes stopped after epoch 1020. | [recap 1041](https://files.old-faithful.net/1041/1041.recap.yaml), [OF1 files](https://docs.old-faithful.net/references/of1-files.md), [CAR report](https://github.com/rpcpool/yellowstone-faithful/blob/gha-report/docs/CAR-REPORT.md) |
| Own node's ledger | $0 | live | Yes while retained | best intra-epoch repair; retention set by `--limit-ledger-size` | |
| Helius archival `getBlock` | 432,000 credits, about $2.16; Free plan 1M credits at 10 RPS (about 12 h per epoch), Developer $49 at 50 RPS | live | No | | [credits](https://www.helius.dev/docs/billing/credits) |
| QuickNode `getBlock` (30 credits) | about 13M credits, about $8 | live | No | | [credits](https://www.quicknode.com/api-credits/sol) |
| Chainstack archive `getBlock` (2 RU) | 864,000 RU, about $13; Growth $49 has 20M RU | live | No | | [reference](https://docs.chainstack.com/reference/solana-getblock.md) |
| Shyft RPC on Build | $0 marginal at 100 RPS (about 1.2 h) | live | No | archive depth undocumented | [RPC docs](https://docs.shyft.to/solana/shyft-rpcs.md) |
| ERPC RPC with Old Faithful integration | Developer EUR 42; Unlimited Endpoints EUR 298 | live | No | | [announcement](https://labo.elsoul.nl/en/news/2025/12/29/erpc-solana-rpc-historical-data-upgrade/) |
| Triton pay-as-you-go `getBlock` / Fumarole | about $100 to $135 including $0.08/GB bandwidth; Fumarole 4-day replay | live | Fumarole yes | | [pricing](https://triton.one/pricing) |
| Helius LaserStream replay | 20 credits/MB, about $150 to $200 per epoch-equivalent; Business $499 required | 48 h window only | per filter | | [replay](https://www.helius.dev/docs/laserstream/historical-replay) |
| Bigtable, BigQuery, Dune, Flipside | not public; $6.25/TiB scanned; unpriced; redirected | days or unknown | No | not byte-faithful | |

JSON-RPC `getBlock` never returns PoH entries. Entries for a missed slot come only from your
own node's ledger, a shred source, or the Old Faithful CAR after the epoch closes.

## Recommended architecture and cost

| Role | Choice | Monthly |
| --- | --- | --- |
| Primary gRPC and complete shreds | Non-voting Agave node with Yellowstone 15.x on Cherry Servers Agave config (Amsterdam or Frankfurt), blocks at Finalized with transactions and entries, `replay_stored_slots` in the thousands, ledger retained for the longest tolerated gap | EUR 711 |
| Independent second gRPC | Shyft Build after a passing probe; constant-k Nexus as the alternative (entries via a separate `entry` filter) | $199 or $179 |
| Shred observation | existing zero-stake reader on Hetzner, plus the node | $0 |
| Repair | Old Faithful CAR for closed epochs; node ledger intra-epoch; Helius Free `getBlock` for spot checks | $0 |
| Total | | about $1,010 per month, on top of the existing Hetzner box |

The Hivezilla reader can run on the node itself so the 33 TB stream stays on loopback and
the Hetzner hop carries only zstd records (5 to 13 TB). Dedupe the two gRPC observations by
slot and blockhash and alarm on a mismatch, as the relaunch design describes.

Avoid: every metered plan for the live feed; Chainstack, RPC Fast, ThorNode, Subglow, rpc
edge, PublicNode and Corvus for the reasons in the tables; managed dedicated nodes at
$1,920 to $3,200 unless the vendor hands over the Geyser config in writing; any paid shred
feed whose terms forbid derived redistribution (DoubleZero Edge, Everstake Blockspace).

## Before paying: a one-hour probe per provider

No provider was tested against a live key. Every "unclear" cell above is settled by running
the existing recorder against a trial endpoint for about an hour and auditing the spool.
The probe needs the vendor's endpoint and token in the environment the recorder already
reads.

```bash
export BLOCKZILLA_GRPC_X_TOKEN_FILE=/run/secrets/probe_x_token
hivezilla record-grpc-raw \
  --endpoint https://<provider-endpoint>:443 \
  --origin-node-id probe-<provider> \
  --output-dir /data/probe-<provider> \
  --max-blocks 15000 \
  --timeout-secs 4000
```

Then stop it and run the offline audit and the PoH check on the stopped spool:

```bash
hivezilla inspect-grpc-raw --output-dir /data/probe-<provider> --verify-payloads
```

```bash
hivezilla verify-grpc-raw-poh --output-dir /data/probe-<provider> --min-records 10000
```

Pass criteria: compare the captured blocks with the produced-slot set returned by
finalized `getBlocks`, not every numeric slot. Check finalized blockhashes, transaction
counts, required metadata, entry structure, and reconnect coverage. The PoH command
checks structure and the final entry hash; it does not recompute the hash chain.
The recorder already allows decoded messages up to 128 MiB. It is hardcoded to
Confirmed and has no commitment option, so these commands cannot test a Finalized
subscription. See the follow-up for the longer trial and repair requirements.

Questions to put to each shortlisted vendor in writing:

- Which yellowstone-grpc version runs the fleet, and the values of
  `filter_limits.blocks.include_entries`, `filter_limits.blocks.account_include_any` and
  `replay_stored_slots`.
- Whether a 24/7 unfiltered full-block stream at 12 to 16 MB/s is acceptable on the plan, and
  what happens to a client that falls behind.
- Whether `from_slot` replay is honoured on a blocks filter at Finalized.
- Contract length, cancellation, refund, and any clause on redistributing derived data.
- Shyft: does the "at least one filter" sentence in the blocks doc apply to an empty
  `account_include`, given your own examples use one.
- constant-k: does an unfiltered blocks or entry stream count against the three match-all
  streams per region.
- Cherry Servers or the chosen host: written confirmation of the egress allowance and
  overage on the Solana configuration.

## Method and confidence

Two research rounds ran on 25 September 2026: a sweep of 155 provider and product entries
across gRPC, shred, dedicated-node, bare-metal and historical categories with two
adversarial verifications for each of 18 flat-rate candidates, then a targeted round on
DoubleZero billing and terms, Hetzner pricing and policy, constant-k and Corvus, Shyft's
filter rules, the shortlist's Yellowstone settings, Old Faithful lag, and node traffic.
Prices came from vendor pages, catalogue APIs, and in two cases a rendered page in the
browser. Nothing was signed up for or purchased.

Weakest points: entries on full blocks are unverified for every provider; provider replay
depth for a blocks filter is documented nowhere except as a refusal; the node traffic model
is derived from an 8-block sample and Agave source reading, not a month of measurement; the
Old Faithful lag is a bracket from log timestamps, not upload times; DoubleZero's
onchain program still lists per-epoch prices although the docs say the monthly portal is
the only new path.
