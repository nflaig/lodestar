// Profiles actual Lodestar decoding/signing/durable retention of captured
// isolated native-control fixtures. No live endpoint, execution or KZG claim.
// node scripts/qualification/native-handoff.mjs LODESTAR_ROOT CAPTURE_ROOT OUTPUT_JSON
import assert from 'node:assert/strict';
import fs from 'node:fs';
import http from 'node:http';
import os from 'node:os';
import path from 'node:path';
import {Worker} from 'node:worker_threads';
import {pathToFileURL} from 'node:url';
import {setTimeout as delay} from 'node:timers/promises';

const [root, captures, output] = process.argv.slice(2).map(p => path.resolve(p));
const load = p => import(pathToFileURL(path.join(root, p)));
const {ssz} = await load('packages/types/lib/index.js');
const {SessionPayloadSource} = await load('packages/cli/lib/cmds/builder/sessionPayloadSource.js');
const {JsonRpcHttpClient} = await load('packages/beacon-node/lib/index.js');
const {parseExecutionPayload} = await load('packages/beacon-node/lib/execution/engine/types.js');
const {BuilderSigner} = await load('packages/builder/lib/services/builderSigner.js');
const {AsyncCommitmentJournal} = await load('packages/builder/lib/services/asyncCommitmentJournal.js');
const {CommitmentJournal} = await load('packages/builder/lib/services/commitmentJournal.js');
const {PayloadStore} = await load('packages/builder/lib/services/payloadStore.js');
const {BidStore} = await load('packages/builder/lib/services/bidStore.js');
const {BidLedger} = await load('packages/builder/lib/services/bidLedger.js');
const {createBeaconConfig} = await load('packages/config/lib/index.js');
const {getConfig} = await load('packages/config/lib/testUtils/index.js');
const {SecretKey} = await load('bindings/src/blst.js');
const hex = bytes => `0x${Buffer.from(bytes).toString('hex')}`;
const secretKey = SecretKey.fromBytes(Buffer.alloc(32, 1)); // Public fixture key only.
const genesis = Buffer.alloc(32, 9);
const signer = new BuilderSigner(createBeaconConfig(getConfig('gloas'), genesis), {secretKey, publicKey: secretKey.toPublicKey()});
const identity = {genesisValidatorsRoot: hex(genesis), pubkey: signer.getPubkeyHex()};
const work = fs.mkdtempSync(path.join(os.tmpdir(), 'native-handoff-profile-'));
const summary = values => {
  const s = [...values].sort((a, b) => a - b);
  return {n: s.length, p50: s[Math.ceil(s.length * .5) - 1], p90: s[Math.ceil(s.length * .9) - 1], p95: s[Math.ceil(s.length * .95) - 1], max: s.at(-1)};
};
const trials = [];
try {
  for (let round = 0; round < 3; round++) {
    const counts = [0, 12, 21];
    for (const count of [...counts.slice(round), ...counts.slice(0, round)]) {
      const directory = path.join(work, `${round}-${count}`);
      const bodyPath = path.join(captures, `${count}-changed.json`);
      const body = fs.readFileSync(bodyPath, 'utf8');
      const fixture = JSON.parse(body);
      const parsed = parseExecutionPayload('gloas', fixture.result.payload);
      assert.equal(parsed.blobsBundle.blobs.length, count);
      const ep = parsed.executionPayload;
      const attrs = {timestamp: ep.timestamp, prevRandao: ep.prevRandao,
        suggestedFeeRecipient: hex(ep.feeRecipient), withdrawals: ep.withdrawals,
        parentBeaconBlockRoot: Buffer.alloc(32), slotNumber: ep.slotNumber};
      const request = {fork: 'gloas', payloadAttributes: attrs,
        forkchoiceState: {headBlockHash: hex(ep.parentHash), safeBlockHash: hex(ep.parentHash), finalizedBlockHash: hex(ep.parentHash)}};
      let received = fixture.result;
      const source = new SessionPayloadSource('isolated-profile', () => ({fetch: async call => {
        if (call.method === 'zbuilder_beginV1') return {sessionId: 'fixture', orderGenerationVersion: 1};
        assert.equal(call.method, 'zbuilder_getPayloadV2');
        return received;
      }}));
      const signal = new AbortController().signal;
      const handle = await source.prepare(request, signal);
      let replyProfile;
      const worker = new Worker(new URL('./native-handoff-worker.mjs', import.meta.url), {workerData: {root, directory, identity}});
      worker.on('message', reply => { if (reply.id !== 0) replyProfile = reply.profile; });
      const journal = new AsyncCommitmentJournal(worker);
      await journal.ready.promise;
      const server = http.createServer((req, res) => {
        req.resume();
        req.on('end', () => { res.writeHead(200, {'content-type': 'application/json'}); res.end(body); });
      });
      await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
      const rpc = new JsonRpcHttpClient([`http://127.0.0.1:${server.address().port}`]);
      const rows = [];
      const txs = ssz.gloas.ExecutionPayload.serialize(ep);
      const blobs = ssz.gloas.BlobsBundle.serialize(parsed.blobsBundle);
      const requests = ssz.gloas.ExecutionRequests.serialize(parsed.executionRequests);
      try {
        for (let i = 0; i < 36; i++) {
          let started = performance.now();
          received = JSON.parse(body).result;
          const jsonParseMs = performance.now() - started;
          started = performance.now();
          const payload = await source.getPayload(handle, signal);
          const validateDecodeMs = performance.now() - started;
          started = performance.now();
          const binaryEp = ssz.gloas.ExecutionPayload.deserialize(txs);
          const binaryBlobs = ssz.gloas.BlobsBundle.deserialize(blobs);
          const binaryRequests = ssz.gloas.ExecutionRequests.deserialize(requests);
          const sszDecodeMs = performance.now() - started;
          // The SSZ comparison is component-only, not an implemented wire API.
          assert(ssz.gloas.ExecutionPayload.equals(binaryEp, ep));
          assert(ssz.gloas.BlobsBundle.equals(binaryBlobs, parsed.blobsBundle));
          assert(ssz.gloas.ExecutionRequests.equals(binaryRequests, parsed.executionRequests));
          started = performance.now();
          const transported = await rpc.fetch({method: 'zbuilder_getPayloadV2', params: ['fixture']});
          const loopbackHttpJsonMs = performance.now() - started;
          assert.equal(transported.payload.executionPayload.blockHash, hex(ep.blockHash));
          // Unique synthetic revision identities isolate retention. All input
          // bytes are preserved; these mutated hashes are not valid EL blocks.
          const hash = Buffer.alloc(32); hash.writeUInt32BE(i + 1, 28);
          const storedPayload = {...payload, executionPayload: {...payload.executionPayload, blockHash: hash}};
          const stored = {slot: ep.slotNumber, blockHash: hex(hash), parentBlockRoot: Buffer.alloc(32), payload: storedPayload};
          const bid = ssz.gloas.ExecutionPayloadBid.defaultValue();
          Object.assign(bid, {slot: ep.slotNumber, blockHash: hash, parentBlockHash: ep.parentHash,
            builderIndex: 1, value: i + 1, blobKzgCommitments: parsed.blobsBundle.commitments,
            executionRequestsRoot: ssz.gloas.ExecutionRequests.hashTreeRoot(parsed.executionRequests)});
          started = performance.now();
          const signed = signer.signExecutionPayloadBid(bid);
          const signingMs = performance.now() - started;
          started = performance.now();
          await journal.retain(signed, stored);
          const retainRoundtripMs = performance.now() - started;
          rows.push({iteration: i, warmup: i < 4, jsonParseMs, validateDecodeMs, loopbackHttpJsonMs,
            sszDecodeMs, signingMs, retainRoundtripMs, worker: replyProfile});
          await delay(1);
        }
      } finally {
        await journal.close();
        server.closeAllConnections();
        await new Promise(resolve => server.close(resolve));
      }
      const payloads = new PayloadStore(), bids = new BidStore(), ledger = new BidLedger();
      assert.equal(new CommitmentJournal(directory, identity).recover(ep.slotNumber, payloads, bids, ledger), rows.length);
      for (let i = 0; i < rows.length; i++) {
        const hash = Buffer.alloc(32); hash.writeUInt32BE(i + 1, 28);
        const restored = payloads.get(hex(hash)).payload;
        assert.deepEqual(ssz.gloas.ExecutionPayload.serialize(restored.executionPayload),
          ssz.gloas.ExecutionPayload.serialize({...ep, blockHash: hash}));
        assert.deepEqual(ssz.gloas.ExecutionRequests.serialize(restored.executionRequests), requests);
        assert.equal(restored.executionPayloadValue, parsed.executionPayloadValue);
        assert(ssz.gloas.BlobsBundle.equals(restored.blobsBundle, parsed.blobsBundle));
        assert.deepEqual(Array.from(restored.executionPayload.transactions), Array.from(ep.transactions));
      }
      trials.push({round, blobs: count, jsonBytes: Buffer.byteLength(body), componentSszBytes: txs.length + blobs.length + requests.length,
        transactions: Array.from(ep.transactions).length, recovery: 'full execution payload SSZ, execution requests, value, transactions and ordered sidecars recovered exactly', rows});
      fs.rmSync(directory, {recursive: true});
    }
  }
  const totals = Object.fromEntries([0, 12, 21].map(count => {
    const rows = trials.filter(t => t.blobs === count).flatMap(t => t.rows.filter(r => !r.warmup));
    const fields = ['jsonParseMs', 'validateDecodeMs', 'loopbackHttpJsonMs', 'sszDecodeMs', 'signingMs', 'retainRoundtripMs'];
    return [count, {...Object.fromEntries(fields.map(k => [k, summary(rows.map(r => r[k]))])),
      workerRetainMs: summary(rows.map(r => r.worker.retainMs)),
      workerFsyncMs: summary(rows.map(r => r.worker.fsyncSync ?? 0))}];
  }));
  fs.writeFileSync(output, JSON.stringify({at: new Date().toISOString(), node: process.version, root,
    scope: 'isolated component qualification, actual Lodestar code; synthetic zero-polynomial captured fixtures; stable sidecars across revisions; no native encoder or live queue wait included',
    totals, trials}, null, 2) + '\n');
  console.log(JSON.stringify(totals, null, 2));
} finally { fs.rmSync(work, {recursive: true, force: true}); }
