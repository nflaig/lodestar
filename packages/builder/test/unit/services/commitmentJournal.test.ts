import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import {afterEach, describe, expect, it, vi} from "vitest";
import {SLOTS_PER_EPOCH} from "@lodestar/params";
import {ssz} from "@lodestar/types";
import {toRootHex} from "@lodestar/utils";
import {BidLedger} from "../../../src/services/bidLedger.js";
import {BidStore} from "../../../src/services/bidStore.js";
import {CommitmentJournal} from "../../../src/services/commitmentJournal.js";
import {PayloadStore} from "../../../src/services/payloadStore.js";
import {mockBuiltPayload} from "../utils/payload.js";

const directories: string[] = [];
const identity = {genesisValidatorsRoot: toRootHex(Buffer.alloc(32, 1)), pubkey: "test-builder"};
const slot = 100;

function setup() {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "builder-journal-"));
  directories.push(directory);
  return {directory, journal: new CommitmentJournal(directory, identity)};
}

function revision(value: number, targetSlot = slot) {
  const blockHash = Buffer.alloc(32);
  blockHash.writeUInt32BE(targetSlot, 24);
  blockHash.writeUInt32BE(value, 28);
  const payload = mockBuiltPayload({slot: targetSlot, blockHash, valueGwei: value});
  const signed = ssz.gloas.SignedExecutionPayloadBid.defaultValue();
  signed.message.slot = targetSlot;
  signed.message.blockHash = payload.executionPayload.blockHash;
  signed.message.parentBlockHash = payload.executionPayload.parentHash;
  signed.message.value = value;
  signed.signature.fill(value);
  return {
    signed,
    stored: {
      slot: targetSlot,
      parentBlockRoot: signed.message.parentBlockRoot,
      blockHash: toRootHex(signed.message.blockHash),
      payload,
    },
  };
}

function recover(directory: string, currentSlot = slot) {
  const journal = new CommitmentJournal(directory, identity);
  const payloads = new PayloadStore(),
    bids = new BidStore(),
    ledger = new BidLedger();
  const count = journal.recover(currentSlot, payloads, bids, ledger);
  return {journal, payloads, bids, ledger, count};
}

afterEach(() => {
  vi.restoreAllMocks();
  for (const directory of directories.splice(0)) fs.rmSync(directory, {recursive: true, force: true});
});

describe("CommitmentJournal", () => {
  it("deduplicates sidecars across revisions, preserves repeated order and prunes by slot", () => {
    const {directory, journal} = setup();
    const first = revision(1);
    const bundle = {
      blobs: [new Uint8Array(131072).fill(1), new Uint8Array(131072).fill(1)],
      commitments: [new Uint8Array(48).fill(2), new Uint8Array(48).fill(2)],
      proofs: Array.from({length: 256}, (_, i) => new Uint8Array(48).fill(i % 128)),
    };
    first.stored.payload.blobsBundle = bundle;
    first.signed.message.blobKzgCommitments = bundle.commitments;
    journal.retain(first.signed, first.stored);
    const second = revision(2);
    second.stored.payload.blobsBundle = bundle;
    second.signed.message.blobKzgCommitments = bundle.commitments;
    journal.retain(second.signed, second.stored);
    expect(fs.readdirSync(directory).filter((name) => name.startsWith("blob-"))).toHaveLength(1);
    const restored = recover(directory);
    for (const original of [first, second]) {
      expect(restored.payloads.get(original.stored.blockHash)?.payload.blobsBundle, original.stored.blockHash).toEqual(
        bundle
      );
    }
    const later = revision(3, slot + 1);
    later.stored.payload.blobsBundle = bundle;
    later.signed.message.blobKzgCommitments = bundle.commitments;
    journal.retain(later.signed, later.stored);
    journal.prune(slot + 3);
    expect(fs.readdirSync(directory).filter((name) => name.startsWith("blob-"))).toHaveLength(1);
    expect(recover(directory, slot + 3).payloads.get(later.stored.blockHash)?.payload.blobsBundle).toEqual(bundle);
  });

  it.each(["missing", "corrupt"])("rejects a %s sidecar during recovery", (kind) => {
    const {directory, journal} = setup();
    const original = revision(1);
    original.stored.payload.blobsBundle = {
      blobs: [new Uint8Array(131072)],
      commitments: [new Uint8Array(48)],
      proofs: Array.from({length: 128}, () => new Uint8Array(48)),
    };
    original.signed.message.blobKzgCommitments = original.stored.payload.blobsBundle.commitments;
    journal.retain(original.signed, original.stored);
    const name = fs.readdirSync(directory).find((name) => name.startsWith("blob-"));
    if (!name) throw Error("Missing sidecar fixture");
    const filename = path.join(directory, name);
    if (kind === "missing") fs.unlinkSync(filename);
    else {
      const bytes = fs.readFileSync(filename);
      bytes[0] ^= 1;
      fs.writeFileSync(filename, bytes);
    }
    expect(() => recover(directory)).toThrow(
      kind === "missing" ? "COMMITMENT_BLOB_MISSING" : "COMMITMENT_BLOB_CHECKSUM"
    );
  });

  it("recovers legacy payload records containing embedded blob data", () => {
    const {directory, journal} = setup();
    const original = revision(1);
    original.signed.message.blobKzgCommitments = original.stored.payload.blobsBundle.commitments;
    journal.retain(original.signed, original.stored);
    const name = fs.readdirSync(directory).find((name) => name.startsWith("payload-"));
    if (!name) throw Error("Missing payload fixture");
    const filename = path.join(directory, name);
    const {data} = JSON.parse(fs.readFileSync(filename, "utf8"));
    delete data.blobRefs;
    data.blobs = ssz.gloas.BlobsBundle.toJson(original.stored.payload.blobsBundle);
    fs.writeFileSync(
      filename,
      JSON.stringify({data, checksum: createHash("sha256").update(JSON.stringify(data)).digest("hex")})
    );
    expect(recover(directory).payloads.get(original.stored.blockHash)?.payload.blobsBundle).toEqual(
      original.stored.payload.blobsBundle
    );
  });

  it.each(["blob-", "payload-"])("does not recover an offer if its %s write failed", (prefix) => {
    const {directory, journal} = setup();
    const original = revision(1);
    original.stored.payload.blobsBundle = {
      blobs: [new Uint8Array(131072)],
      commitments: [new Uint8Array(48)],
      proofs: Array.from({length: 128}, () => new Uint8Array(48)),
    };
    const rename = fs.renameSync;
    vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
      if (path.basename(String(to)).startsWith(prefix)) throw Error("disk failed");
      rename(from, to);
    });
    original.signed.message.blobKzgCommitments = original.stored.payload.blobsBundle.commitments;
    expect(() => journal.retain(original.signed, original.stored)).toThrow("disk failed");
    vi.restoreAllMocks();
    expect(recover(directory).count).toBe(0);
  });
  it("recovers every blob and cell proof for an older selected revision", () => {
    const {directory, journal} = setup();
    const original = revision(1);
    original.stored.payload.executionPayload.blobGasUsed = 131072n;
    original.stored.payload.blobsBundle = {
      blobs: [new Uint8Array(131072).fill(1)],
      commitments: [new Uint8Array(48).fill(2)],
      proofs: Array.from({length: 128}, (_, i) => new Uint8Array(48).fill(i)),
    };
    original.signed.message.blobKzgCommitments = original.stored.payload.blobsBundle.commitments;
    original.signed.message.blobKzgCommitments = original.stored.payload.blobsBundle.commitments;
    journal.retain(original.signed, original.stored);
    journal.retain(revision(2).signed, revision(2).stored);
    const {payloads, bids, count} = recover(directory);
    expect(count).toBe(2);
    expect(bids.has(original.signed)).toBe(true);
    const restored = payloads.get(original.stored.blockHash);
    expect(restored?.payload.blobsBundle).toEqual(original.stored.payload.blobsBundle);
    original.stored.payload.blobsBundle.blobs[0].fill(3);
    expect(restored?.payload.blobsBundle.blobs[0][0]).toBe(1);
    expect(restored?.payload.blobsBundle.proofs[127][0]).toBe(127);
  });

  it("recovers full overlapping slots and keeps old revisions selectable above a lower admission limit", () => {
    const {directory, journal} = setup();
    const originals = Array.from({length: 1024}, (_, i) => {
      const original = revision((i % 256) + 1, slot - 2 + Math.floor(i / 256));
      original.signed.message.parentBlockRoot.fill(i % 2);
      original.signed.message.blobKzgCommitments = original.stored.payload.blobsBundle.commitments;
      journal.retain(original.signed, original.stored);
      return original;
    });
    const {payloads, bids, ledger, count} = recover(directory);
    expect(count).toBe(1024);
    expect(payloads.size).toBe(1024);
    expect(ledger.getUnsettledValueGwei(4)).toBe(4 * 256);
    for (const original of originals) {
      const label = `slot ${original.stored.slot} value ${original.signed.message.value}`;
      expect(bids.has(original.signed), label).toBe(true);
      const restored = payloads.get(original.stored.blockHash);
      if (!restored) throw Error(`Missing payload for ${label}`);
      expect(ssz.gloas.ExecutionPayload.serialize(restored.payload.executionPayload), label).toEqual(
        ssz.gloas.ExecutionPayload.serialize(original.stored.payload.executionPayload)
      );
      expect(restored.payload.executionPayloadValue, label).toBe(original.stored.payload.executionPayloadValue);
    }
    expect(() => ledger.assertCanRecordRevision(ledger.getBidsForSlot(slot)[0], 64)).toThrow();
    const old = ledger.getBidsForSlot(slot - 2).find((record) => record.blockHash === originals[0].stored.blockHash);
    if (!old) throw Error("Missing oldest selectable revision");
    const blockRoot = toRootHex(Buffer.alloc(32, 7));
    const envelopeRoot = toRootHex(Buffer.alloc(32, 8));
    expect(ledger.recordWin(old, blockRoot)).not.toBeNull();
    journal.retainReveal({slot: old.slot, blockRoot, blockHash: old.blockHash, envelopeRoot});
    ledger.recordReveal(old.slot, blockRoot, old.blockHash, envelopeRoot);
    const restarted = recover(directory);
    expect(restarted.ledger.canReveal(blockRoot, old.blockHash, envelopeRoot)).toBe(true);
    expect(restarted.ledger.canReveal(blockRoot, old.blockHash, toRootHex(Buffer.alloc(32, 9)))).toBe(false);
    const later = recover(directory, slot + 1);
    expect(later.payloads.size).toBe(768);
    expect(later.count).toBe(1024);
    expect(later.bids.has(originals[0].signed)).toBe(false);
    expect(later.bids.has(originals[256].signed)).toBe(true);
    const next = revision(1, slot + 2);
    later.payloads.add(next.stored);
    later.journal.retain(next.signed, next.stored);
    expect(recover(directory, slot + 1).payloads.size).toBe(769);
  }, 20_000);

  it("fits a full bid history at the revision limit and recovers it after restart", () => {
    const {directory, journal} = setup();
    const currentSlot = slot + 3 * SLOTS_PER_EPOCH;
    for (let targetSlot = slot; targetSlot <= currentSlot + 1; targetSlot++) {
      journal.prune(targetSlot - 1);
      const original = revision(256, targetSlot);
      for (let value = 1; value <= 256; value++) {
        const signed = {...original.signed, message: {...original.signed.message, value}};
        journal.retain(signed, original.stored);
      }
    }
    const {count, payloads, ledger, bids} = recover(directory, currentSlot);
    expect(count).toBe(256 * (3 * SLOTS_PER_EPOCH + 2));
    expect(payloads.size).toBe(4);
    expect(ledger.getBidsForSlot(slot)).toHaveLength(256);
    const oldest = revision(256, currentSlot - 2);
    expect(bids.has(oldest.signed)).toBe(true);
    const record = {
      slot: currentSlot - 2,
      blockRoot: toRootHex(Buffer.alloc(32, 7)),
      blockHash: oldest.stored.blockHash,
      envelopeRoot: toRootHex(Buffer.alloc(32, 8)),
    };
    journal.retainReveal(record);
    expect(recover(directory, currentSlot).ledger.hasRevealed(record.blockRoot)).toBe(true);
  }, 120_000);

  it("recovers every selectable revision and payload, reserves one liability per slot, and consumes gossip allowance", () => {
    const {directory, journal} = setup();
    const first = revision(1),
      second = revision(2);
    journal.retain(first.signed, first.stored);
    journal.retain(second.signed, second.stored);
    const {bids, payloads, ledger, count} = recover(directory);
    expect(count).toBe(2);
    expect(bids.has(first.signed)).toBe(true);
    expect(bids.has(second.signed)).toBe(true);
    for (const original of [first, second]) {
      const restored = payloads.get(original.stored.blockHash);
      if (!restored) throw Error("Missing recovered payload");
      expect(ssz.gloas.ExecutionPayload.serialize(restored.payload.executionPayload)).toEqual(
        ssz.gloas.ExecutionPayload.serialize(original.stored.payload.executionPayload)
      );
      expect(restored.payload.executionPayloadValue).toBe(original.stored.payload.executionPayloadValue);
    }
    expect(ledger.getUnsettledValueGwei(4)).toBe(2);
    const bid = first.signed.message;
    expect(ledger.hasSubmitted(slot, toRootHex(bid.parentBlockHash), toRootHex(bid.parentBlockRoot))).toBe(true);
    const old = ledger.getBidsForSlot(slot).find((record) => record.blockHash === first.stored.blockHash);
    expect(old).toBeDefined();
    if (!old) throw Error("Missing recovered revision");
    const blockRoot = toRootHex(Buffer.alloc(32, 7));
    expect(ledger.recordWin(old, blockRoot)?.blockHash).toBe(first.stored.blockHash);
    expect(ledger.hasWon(old, blockRoot)).toBe(true);
    ledger.settlePaymentsBefore(slot + 1);
    expect(ledger.getUnsettledValueGwei(4)).toBe(0);
  });

  it("retains reveal conflicts across restart while permitting an identical retry", () => {
    const {directory, journal} = setup();
    const record = {
      slot,
      blockRoot: toRootHex(Buffer.alloc(32, 3)),
      blockHash: toRootHex(Buffer.alloc(32, 4)),
      envelopeRoot: toRootHex(Buffer.alloc(32, 5)),
    };
    journal.retainReveal(record);
    journal.retainReveal(record);
    const {ledger} = recover(directory);
    expect(ledger.canReveal(record.blockRoot, record.blockHash, record.envelopeRoot)).toBe(true);
    expect(ledger.canReveal(record.blockRoot, record.blockHash, toRootHex(Buffer.alloc(32, 6)))).toBe(false);
    expect(() => journal.retainReveal({...record, blockHash: toRootHex(Buffer.alloc(32, 9))})).toThrow(
      "COMMITMENT_REVEAL_CONFLICT"
    );
  });

  it("fails closed on disk errors, ignores an incomplete temporary write, and never recovers a partial offer", () => {
    const {directory, journal} = setup();
    const first = revision(1);
    const rename = fs.renameSync;
    vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
      if (String(to).includes("bid-")) throw Error("disk failed");
      rename(from, to);
    });
    expect(() => journal.retain(first.signed, first.stored)).toThrow("disk failed");
    expect(() => journal.retain(first.signed, first.stored)).toThrow("COMMITMENT_JOURNAL_FAILED");
    vi.restoreAllMocks();
    const recovered = recover(directory);
    expect(recovered.count).toBe(0);
    expect(recovered.bids.has(first.signed)).toBe(false);
  });

  it("rejects a missing committed payload, disk corruption, and another network or identity", () => {
    const {directory, journal} = setup();
    const first = revision(1);
    journal.retain(first.signed, first.stored);
    expect(() => new CommitmentJournal(directory, {...identity, pubkey: "another"})).toThrow(
      "COMMITMENT_JOURNAL_IDENTITY_MISMATCH"
    );
    const payloadName = fs.readdirSync(directory).find((name) => name.startsWith("payload-"));
    if (!payloadName) throw Error("Missing fixture payload");
    fs.unlinkSync(path.join(directory, payloadName));
    expect(() => recover(directory)).toThrow("COMMITMENT_PAYLOAD_MISSING");
    const bidName = fs.readdirSync(directory).find((name) => name.startsWith("bid-"));
    if (!bidName) throw Error("Missing fixture bid");
    const filename = path.join(directory, bidName);
    const record = JSON.parse(fs.readFileSync(filename, "utf8"));
    record.data.message.value = "999";
    fs.writeFileSync(filename, JSON.stringify(record));
    expect(() => recover(directory)).toThrow("COMMITMENT_JOURNAL_CHECKSUM");
  });

  it("prunes payloads after the reveal window but keeps conservative liabilities until payment settlement", () => {
    const {directory, journal} = setup();
    const first = revision(1);
    journal.retain(first.signed, first.stored);
    const recovered = recover(directory, slot + 3);
    expect(recovered.payloads.size).toBe(0);
    expect(recovered.bids.has(first.signed)).toBe(false);
    expect(recovered.ledger.getUnsettledValueGwei(4)).toBe(1);
    const expired = recover(directory, slot + 3 * SLOTS_PER_EPOCH + 1);
    expect(expired.count).toBe(0);
    expect(fs.readdirSync(directory)).toEqual(["identity.json"]);
  });
});

import {createHash} from "node:crypto";
