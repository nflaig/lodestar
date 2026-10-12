# Lodestar Builder

[![Discord](https://img.shields.io/discord/593655374469660673.svg?label=Discord&logo=discord)](https://discord.gg/aMxzVcr)
[![Eth Consensus Spec v1.7.0-beta.2](https://img.shields.io/badge/ETH%20consensus--spec-1.7.0_beta.2-blue)](https://github.com/ethereum/consensus-specs/releases/tag/v1.7.0-beta.2)
![ES Version](https://img.shields.io/badge/ES-2021-yellow)
![Node Version](https://img.shields.io/badge/node-24.x-green)

> This package is part of [ChainSafe's Lodestar](https://lodestar.chainsafe.io) project

Typescript implementation of the Ethereum Consensus builder client.

## Getting started

- Follow the [installation guide](https://chainsafe.github.io/lodestar/) to install Lodestar.
- Quickly try out the whole stack by [starting a local testnet](https://chainsafe.github.io/lodestar/contribution/advanced-topics/setting-up-a-testnet/).

## Recovering proposer preferences

Proposers gossip their signed preferences once. A beacon node that misses the message never
emits the matching event, and the builder does not bid for that slot. The builder polls the
pool of its beacon node at startup and once per slot. Additional trusted beacon nodes on the
same network can be polled as well:

```sh
--proposerPreferences.urls http://127.0.0.1:9597,http://127.0.0.1:9598
```

At most three additional URLs are accepted. Each is verified against the genesis and config of
the primary beacon node before its pool is used; a node on a different network is excluded for
the rest of the process. Only signed preferences for the current and next epoch are retained, and
entries are merged by proposal slot and dependent root, so an entry already known is never
replaced. The primary beacon node still provides payload attributes, duties and builder status.
Polls share one deadline, `--proposerPreferences.timeout`, and run outside the bidding path; a
failing node is reported once and skipped until it responds again.

## Controlled payloads on a devnet

The builder can use Geth's `testing_buildBlockV1` to build an exact, ordered list of signed transactions.
This experimental source supports Gloas payloads without blob transactions. It preserves the beacon
node's payload attributes, including withdrawals, randomness, parent beacon block root, slot and target
gas limit. The builder's configured execution fee recipient is applied as usual.

Create a JSON transaction plan:

```json
{
  "transactions": [],
  "extraData": "0x"
}
```

Replace the empty array with signed transaction hex strings in execution order. An empty array builds
an empty payload; `null` is rejected. Transactions must have valid nonces, balances and fees for the
requested parent. Optional `parentHash` and `slotNumber` fields bind the plan to one parent or slot.
The builder reads the file for each new build; replace it atomically when updating a plan.

Add these flags to your existing builder command to capture payloads without submitting bids:

```sh
--payload.transactionsFile /path/to/transactions.json \
--payload.testingUrl http://127.0.0.1:8545 \
--payload.outputDir /path/to/captures \
--payload.dryRun
```

Use a private Geth HTTP endpoint with the `eth` and `testing` namespaces enabled, for example
`--http --http.addr 127.0.0.1 --http.api eth,testing`. This source does not support IPC or use the
authenticated Engine endpoint. Geth must already follow the beacon node's requested execution parent.
The source checks that parent against Geth's current head and does not change forkchoice.

Capture mode still requires the existing builder key, active builder identity and proposer preferences.
It saves the raw RPC request and response, including the block access list and execution requests,
along with build timestamps. It stops before signing or storing a bid. Omit `--payload.dryRun` to use
the ordinary bidding and reveal flow with the controlled payloads.

Geth's synchronous testing build can continue after the proposal deadline. The source waits for its
RPC response before allowing another build, captures late results, and discards them from bidding.
Choose `--execution.timeout` and Geth's HTTP timeouts to cover the expected build duration. If the RPC
times out or disconnects during a build, its completion is unknown: check Geth and restart the builder before
building again. A completed build that fails validation or capture is not retried for the same request.

The adapter checks transaction order and returned attributes. Validate captures with another EL before
using a new workload on a shared devnet; these checks do not independently verify execution or roots.

## Incremental payload constructor

An external constructor can keep one session alive and return improving Gloas
payloads without blob transactions. Add these flags to the existing builder command:

```sh
--payload.constructorUrl http://127.0.0.1:8547 \
--bidding.updateIntervalMs 100 \
--bidding.updateCutoffBps 1000
```

The constructor implements the private JSON-RPC methods `zbuilder_beginV1(parentHash,
payloadAttributes, extraData?)`, `zbuilder_getPayloadV1(sessionId, knownBlockHash?)` and
`zbuilder_endV1(sessionId)`. Begin returns `{sessionId}`. Get returns a complete Engine
payload response or `null` when the supplied hash is still current. The constructor
must validate execution before exposing a result. When extra data is omitted, the
constructor chooses it, for example to identify its software revision; the result
must fit the execution payload's 32-byte limit. Lodestar additionally checks the
parent and attributes. Construction and intake remain external; Lodestar owns pricing,
signing, bid publication and reveal obligations.

With updates enabled, the API immediately offers each strictly higher bid permitted by
the pricing policy. Earlier signed revisions and payloads remain available for selection. Gossip
still publishes at most one bid per slot and parent tuple for the configured identity,
using the best ready bid at `bidding.deadlineBps` in the preceding slot. API polling
continues until `updateCutoffBps` of the proposal slot; 1000 is 1.2 seconds into a
12-second slot. Parent changes cancel construction without deleting old commitments.

The default update interval is zero, retaining one retrieval at the usual deadline.
Positive intervals require this constructor source and must be at least 25 ms. This
source cannot be combined with the controlled transaction-file source or its capture
directory. `--payload.dryRun` builds without signing or publishing.

The beacon node emits `payload_attributes` once per slot, at two thirds of the slot before
the proposal. To start construction earlier, run the builder's beacon node with
`--emitPayloadAttributes --chain.emitPayloadAttributesEarly`. It then also emits as soon
as the parent of the next slot is known: the empty tuple when the block is imported and
the full tuple when its execution payload is imported. The regular emission before the
slot still follows, so one slot may deliver the same tuple twice and the empty tuple before
the full one. The builder keys builds by tuple, and a changed tuple cancels the earlier build.
Epoch boundaries keep the regular timing only.

The commitment journal permits 64 signed revisions per slot across all parents.
Payload storage is capped at 192 payloads and 256 MiB of serialized content plus entry
allowances, which is not a resident memory limit. Exhaustion rejects new work rather than evicting
advertised payloads. Metrics distinguish prepared API revisions (`prepared`) from
successful gossip publication (`published`). Keep the constructor RPC private.

Live CLI builders persist Gloas commitments to `<dataDir>/builder-commitments`.
Payloads and signed bids are synced before exposure; reveal conflict records are
synced before signing. Recovery restores eligible revisions and checks identity,
checksums and required payloads before serving. The disk journal is limited to
768 MiB, 20,000 files and 128 MiB per file. Payloads expire after two slots;
bid and reveal metadata expire after three epochs. Dry runs do not write it.

Recovery conservatively consumes each recorded parent tuple's gossip allowance
and reserves the largest offer per recent slot as a possible unsettled liability.
This covers missed selection events while offline, but can reduce available bidding
balance until reconciliation or expiry. Startup also processes the current beacon
head. An uncertain write stops new journal writes. Use one process per journal
with local filesystem atomic-rename/fsync semantics, retain the data directory
across upgrades, and drain existing offers before switching publisher identities.
Multiple builder identities and replicated recovery are not implemented.

## License

Apache-2.0 [ChainSafe Systems](https://chainsafe.io)
