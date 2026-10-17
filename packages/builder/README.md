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
payloads, including complete blob bundles. Add these flags to the existing builder command:

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

The commitment journal permits 256 signed revisions per slot across all parents.
Payload storage is capped at 1,280 payloads and 256 MiB of retained content plus entry
allowances, with a 64 MiB admission budget per slot. Identical immutable blob sidecars
share storage within a slot; metadata and proof accounting remain conservative.
This accounting is not a resident memory limit. Exhaustion rejects new work rather than evicting
advertised payloads. Metrics distinguish prepared API revisions (`prepared`) from
successful gossip publication (`published`). Keep the constructor RPC private.

Live CLI builders persist Gloas commitments to `<dataDir>/builder-commitments`.
Payloads and signed bids are synced before exposure; reveal conflict records are
synced before signing. Recovery restores reveal obligations and checks identity,
checksums, execution-request roots and required payloads before serving. Each
new offer must match its retained slot, execution hash, execution parent, beacon
parent, request root and ordered blob commitments. Reusing an immutable payload
hash cannot replace those retained bindings. The disk journal is limited to
768 MiB, 51,457 files and 128 MiB per file. Payloads expire after two slots;
bid and reveal metadata expire after three epochs. Dry runs do not write it.

New offers leave software quota for 25,088 reveal records of at most 1 KiB each
(24.5 MiB), within those same total limits. Existing reveal files consume that
reserve. Both ordinary writes and staged hard links preserve the remaining
headroom; no previous offer is deleted to make room. Existing journals above the
new offer threshold still recover, but cannot guarantee the reserve until old
records expire. This accounting does not reserve physical blocks or protect
against ENOSPC, filesystem corruption, a failed disk or a disk that ignores fsync.

Recovery conservatively consumes each recorded parent tuple's gossip allowance
and reserves the largest offer per recent slot as a possible unsettled liability.
This covers missed selection events while offline, but can reduce available bidding
balance until reconciliation or expiry. Startup also processes the current beacon
head. An uncertain write stops new journal writes. Before recovery or signing,
the builder takes a native exclusive lock in the canonical commitment directory.
It rejects a second instance, including symlink aliases and another instance in
the same process. The lock has no stale timeout: a stopped process remains the
owner, while an exited process releases its kernel lock. Shutdown revokes new
signing first, drains journal workers, then releases ownership. A successor must
recover all commitments before serving. Use a local filesystem with reliable
exclusive locking and atomic-rename/fsync semantics, retain the data directory
across upgrades, and drain existing offers before switching publisher identities.
Never remove or replace the `.writer-lock` directory while an owner may be alive.
Signer ownership is acquired only from the main thread. A process-wide directory
registry rejects duplicate opens before they can release a POSIX record lock;
other code in that process must not open, repair or destroy `.writer-lock/LOCK`.
The lock database itself needs writable disk space at startup. An unavailable or
corrupt lock database fails startup closed, even if expired journal files could
otherwise be pruned. Preserve the directory until the former owner has exited.
The guard covers the primary identity and its configured staged identities under
one commitment root. Separate data directories, copied disks, copied keys and
snapshot rollback are not fenced. Cross-host failover needs an external signer
or fencing system and a journal that includes every acknowledged offer. This
local guard does not provide replicated recovery or make the builder mainnet ready.

After synchronous startup recovery, one worker owns journal writes and pruning.
Its queue admits at most eight offers, 64 MiB of offer data and sixteen operations.
Reveal records have reserved capacity and priority over queued offers. An offer
becomes eligible only after its durable acknowledgement; queued writes and writes
with uncertain outcomes consume revision admission. Existing ready bids can still
be served while a newer revision is being persisted.

Blob bytes, commitments and cell proofs are stored together in content-addressed
binary files shared by revisions of the same slot. Payload records retain ordered
references; recovery checks their hashes and bid commitment order. Legacy inline
blob records remain readable. Older binaries cannot recover live reference-based
payload records: before rolling back, pause new construction and drain beyond the
two-slot payload retention window. Preserve the journal and identity files; never
replace the journal with a backup that omits bids already exposed to proposers.

## Constructor order generations

A constructor advertising `orderGenerationVersion: 1` uses conditional
`zbuilder_getPayloadV2` reads. Each ready response grants 250 ms of offer permission,
measured from the request start with both monotonic and wall clocks. Heartbeats may
renew unchanged candidates. Expired, unavailable, released or superseded generations
cannot produce new API or gossip offers. Checks run after durable preparation, before
each transport attempt and before the final API response is sent. API responses use
`Cache-Control: no-store`.

The gateway must fence the constructor and drain all outstanding leases before
acknowledging cancellation or replacement. Lower-value replacement candidates may be
served through the API; already consumed gossip allowances are not reset. Previously
signed bids and payloads remain retained for reveal, even after cancellation. Recovery
restores these obligations without restoring volatile offer permission; a current
constructor admission is required before offering them again.

This protocol requires session updates. One-shot construction with leased candidates
is rejected. Constructors advertising version zero, or no version, retain V1 behavior.
Deploy a compatible gateway, constructor and publisher together before enabling order
generations. Cancellation does not retract bids already delivered to proposers, remove
independently authorized copies, or fence copied processes on other hosts. Arbitrary
VM snapshot rollback and clock restoration are outside the lease assumptions.

## Staged P2P bids

Gloas peers accept at most one gossip bid per builder index, slot and parent tuple.
To advertise later improvements over P2P, configure additional **registered** builder
identities with `--bidding.gossipIdentitiesFile /path/to/gossip-identities.json`:

```json
[
  {
    "keystore": "keystore-builder-1.json",
    "keystorePassword": "../secrets/builder-1-password.txt",
    "deadlineBps": 9750
  },
  {
    "keystore": "keystore-builder-2.json",
    "keystorePassword": "../secrets/builder-2-password.txt",
    "deadlineBps": 10000
  }
]
```

Paths are relative to the configuration file. `builderPubkey` is an optional expected
public key for each entry. Keep the existing primary keystore flags. For example,
with `--bidding.deadlineBps 9250` on a 12-second network, the primary bids 900 ms
before the proposal slot, followed by stages at 300 ms before and at the slot boundary.
Propagation and commitment persistence add latency, so measure arrival times before
choosing a schedule. A stage at or after the boundary may miss an early proposer.

There is one construction session. Each extra stage uses the latest durable primary
snapshot, computes a bid under its own balance and unsettled liabilities, and signs
with its own identity. The configured pricing policy applies to every identity.
A later stage must exceed the largest previously attempted value by Lodestar's gossip
increment: 3%, with a 100,000 gwei floor and 10,000,000 gwei cap. Competitors may already
have a higher bid. API requests continue to return revisions signed by the primary.
If the primary's balance caps its API revisions, this also limits the staged snapshot
feed, even if an additional identity has more balance.

At most four additional identities are accepted. Their deadlines must strictly increase
after the primary deadline and remain before `10000 + bidding.updateCutoffBps`.
Session updates and a durable commitment directory are required. Unknown identities fail
startup with a bounded registry request (one retry for transient failures); known inactive or underfunded identities skip
bidding. A missed stage, an insufficient increase, or a preceding publication still in
flight skips that stage. Uncertain sends are retained and never retried for that tuple.

Each identity has a separate ledger and a durable journal under
`<commitmentDir>-gossip/<pubkey>`. These directories must accompany the primary journal
through backups and deployments. Startup refuses to omit a configured signer whose journal still contains an outstanding
commitment. Keep those keys available and drain bidding before removing an identity. Shared payloads stay in
memory until the normal reveal retention expires, and each journal independently holds
the payloads needed to recover that identity's commitments.

## License

Apache-2.0 [ChainSafe Systems](https://chainsafe.io)
