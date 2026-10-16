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

## License

Apache-2.0 [ChainSafe Systems](https://chainsafe.io)
