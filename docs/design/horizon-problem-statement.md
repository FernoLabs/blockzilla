# Horizon Problem Statement

Status: research context. This document explains the motivation for a compact,
replay-friendly archive; it is not an implemented replay or account-diff
contract. The implemented storage layout is documented separately in the
[Archive V2 reference](../reference/archive-v2-hot-block-format.md).

## What Solana Proves Today

Solana already has strong integrity primitives, but they do not prove everything people actually care about.

At a high level:

- Proof of History helps prove ordering.
- Blockhashes help anchor transactions in a particular ledger context.
- Transaction signatures prove that the transaction payload was authorized by the required signers.

Those are important guarantees, but they only prove the integrity of the transaction as an input to the runtime and its place in the chain. They do not, by themselves, prove the full runtime outcome that applications and users consume.

What people usually care about is not just:

- was this transaction signed?
- was it included?
- did it appear in the right order?

What people really care about is:

- what happened during execution?
- which accounts changed?
- what was the resulting state?
- what logs, inner instructions, balances, and side effects were produced?

That is a different problem.

## The Gap Between Chain Integrity and Runtime Truth

Today, most of the ecosystem builds on top of RPC metadata.

That includes things like:

- transaction status metadata
- logs
- inner instructions
- token balance changes
- loaded addresses
- balance deltas
- higher-level "what happened" interpretations exposed by indexers and RPC providers

This is useful, but it is not a complete trustless answer.

The problem is that this runtime-oriented data is typically consumed as delivered by an RPC or archival provider. In practice, users must trust the provider to:

- have the right runtime data
- store it correctly
- preserve it completely
- and deliver it accurately

That is not the same as independently verifying it.

In other words, Solana gives us strong guarantees about transaction inclusion and ordering, but most higher-level products depend on runtime outputs that are still effectively trusted data feeds.

## Why This Matters

This matters because the ecosystem increasingly builds on runtime results, not just on raw transactions.

People care about:

- account update streams
- account history
- token balance history
- program execution traces
- state diff products
- indexing and analytics built from runtime effects

If those products are built on top of metadata that is incomplete, provider-specific, or not independently verifiable, then the ecosystem is relying on a weak trust layer exactly where most useful products live.

That is why the real missing piece is not "more metadata." The missing piece is a generic way to store, consume, and verify account change over time.

That is the problem Horizon is trying to solve.

## Why Horizon Matters

Horizon matters because it points at the thing people actually want: a generic and reusable model for account diffs and account updates over time.

If we had a robust way to:

- store account state changes over time
- stream them efficiently
- consume them generically
- and verify that they are correct

then a large part of the ecosystem could stop depending on opaque RPC metadata as the final source of truth.

That would be a major improvement for:

- infra providers
- researchers
- indexers
- explorers
- stateful APIs
- any product that needs trustworthy historical account state

But to get there, Horizon cannot just store claimed account updates. It needs a substrate that can consume historical transactions and replay them in a way that can validate the resulting state changes.

That is where the archive format starts to matter.

## Why The Current Archive Format Is a Problem

The current archive world is not designed for this job.

Today, historical Solana archives are heavily shaped by legacy storage decisions and by runtime-oriented payloads that are useful for transport, but not ideal for replay-first validation.

The result is an archive that is:

- very large
- expensive to store
- expensive to scan
- hard to move around
- and inefficient for repeated replay-oriented consumption

It is also bloated with runtime information in forms that are not optimized for building a clean replay and state-diff pipeline.

For Horizon, that means the source material for building account diffs is harder to consume than it should be. Instead of starting from a compact replay-oriented substrate, we start from an archive that mixes:

- replay-relevant history
- runtime metadata
- transport/storage overhead
- and legacy layout constraints

That makes the whole problem more expensive.

## What We Propose With Blockzilla

With Blockzilla, we are proposing a new binary archive format designed for this next step.

The idea is simple:

- make the archive smaller
- make it more efficient to scan
- make it easier to replay
- and preserve enough structure to validate archive integrity

Instead of treating the archive as a legacy blob that downstream systems must adapt to, we want to reshape it into a format that is explicitly useful for:

- replay
- verification
- indexing
- and account diff construction

At a high level, the Blockzilla direction is to separate the archive into cleaner layers:

- replay-verifiable historical data
- runtime-heavy metadata
- reconstruction metadata needed to validate against the original archive

That separation matters because replay and verification do not always need the same material as a log-heavy analytics query. A better format should let us work on the historical core without dragging the full runtime payload through every workflow.

## Why Blockzilla Helps Horizon

Horizon needs a way to consume transactions and derive trustworthy account changes over time.

Blockzilla helps by making that input substrate much more practical.

Concretely, we want the archive layer underneath Horizon to be:

- smaller on disk
- easier to replicate
- cheaper to keep locally
- more efficient to stream through replay
- and structured enough to validate back to the original archive

This gives Horizon two important advantages.

### 1. A better replay input

If Horizon is going to build account diffs from historical transactions, then replay cost matters a lot. A smaller and more replay-oriented archive makes that job much more realistic.

### 2. A better verification story

If Blockzilla preserves validation back to the original archive, then the account updates derived by Horizon can inherit a much stronger trust story. Instead of trusting a provider's metadata export, we move toward:

- validated archive
- replay from archive
- derived account diffs
- verification of delivered account updates

That is a much stronger stack.

## Replay Is The Missing Link

The key idea is that runtime truth ultimately comes from replay.

If we want to know whether account updates are correct, we need the ability to:

- consume the historical transaction stream
- recompute Proof of History / block ordering context where relevant
- verify transaction signatures
- execute or replay the transaction flow
- validate runtime-derived metadata
- rebuild resulting account state
- and compare derived state transitions against what was delivered

That is the only way to move from "trusted runtime feed" toward "verifiable account history."

This is also why simply storing more RPC metadata is not enough. Metadata without replay is still mostly an assertion. Replay is what turns archive data into something that can support independent verification.

## Why This Matters For The Ecosystem

If this problem remains unsolved, the ecosystem keeps building more and more products on top of data that is operationally useful but not cleanly verifiable.

If we solve it, we unlock a different model:

- archive integrity can be validated
- account diffs can be built generically
- account update streams can be checked independently
- read-layer products can rely less on trusted black-box RPC behavior

That would help not only Horizon, but the broader Solana data stack.

It would also help with decentralization. The more practical it becomes to keep, replay, and validate history locally, the easier it is for more operators to participate in the historical data layer instead of depending on a small number of large providers.

## Short Version

Solana already proves that transactions were signed and ordered correctly. That is necessary, but it is not sufficient for the products people actually use.

What people consume is runtime outcome:

- logs
- metadata
- balances
- account changes
- state transitions

Today, that layer is mostly consumed through trusted RPC and indexing infrastructure.

Horizon matters because it points toward the real missing abstraction: a generic system for storing and consuming account changes over time.

Blockzilla matters because Horizon needs a better historical substrate to get there. Our proposal is a smaller, more efficient binary archive format built for replay, archive integrity verification, and account state diff construction.
