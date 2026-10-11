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

function revision(value: number) {
  const payload = mockBuiltPayload({slot, blockHash: Buffer.alloc(32, value), valueGwei: value});
  const signed = ssz.gloas.SignedExecutionPayloadBid.defaultValue();
  signed.message.slot = slot;
  signed.message.blockHash = payload.executionPayload.blockHash;
  signed.message.parentBlockHash = payload.executionPayload.parentHash;
  signed.message.value = value;
  signed.signature.fill(value);
  return {
    signed,
    stored: {
      slot,
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
