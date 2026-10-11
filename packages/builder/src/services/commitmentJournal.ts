import {createHash} from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import {SLOTS_PER_EPOCH} from "@lodestar/params";
import {RootHex, gloas, ssz} from "@lodestar/types";
import {fromHex, toRootHex} from "@lodestar/utils";
import {BidLedger} from "./bidLedger.js";
import {BidStore} from "./bidStore.js";
import {PayloadStore, StoredPayload} from "./payloadStore.js";

const KEEP_SLOTS = 3 * SLOTS_PER_EPOCH;
const MAX_BYTES = 768 * 1024 * 1024;
const MAX_FILE_BYTES = 128 * 1024 * 1024;
const MAX_FILES = 20_000;
const recordName = /^(payload|bid|reveal)-(\d+)-(0x[0-9a-f]{64})\.json$/;

type PayloadRecord = {
  slot: number;
  parentBlockRoot: RootHex;
  blockHash: RootHex;
  sourceId: string;
  value: string;
  payload: unknown;
  requests: unknown;
  blobs: unknown;
};

type RevealRecord = {slot: number; blockRoot: RootHex; blockHash: RootHex; envelopeRoot: RootHex};

/** Single-process Gloas journal. Commit files and their directory are synced
 * before an offer becomes externally visible. An uncertain write poisons new
 * journal operations; restarting reconciles complete records. Not a key store.
 * Disk recovery conservatively reserves all recent offers as possible wins and
 * consumes their gossip allowance. This covers selection events missed offline. */
export class CommitmentJournal {
  private readonly sizes = new Map<string, number>();
  private bytes = 0;
  private failed = false;

  constructor(
    private readonly directory: string,
    identity: {genesisValidatorsRoot: RootHex; pubkey: string}
  ) {
    const created = fs.mkdirSync(directory, {recursive: true, mode: 0o700});
    if (created) {
      // Persist newly created directory entries too: syncing a record's own
      // directory does not by itself make that directory's parent entry durable.
      const last = path.dirname(path.resolve(created));
      for (let parent = path.dirname(path.resolve(directory)); ; parent = path.dirname(parent)) {
        syncDirectory(parent);
        if (parent === last) break;
      }
    }
    const names = fs.readdirSync(directory);
    if (names.length > MAX_FILES * 2) throw Error("COMMITMENT_JOURNAL_FILE_LIMIT");
    for (const name of names) {
      if (!recordName.test(name) && name !== "identity.json") continue;
      const info = fs.lstatSync(path.join(directory, name));
      if (!info.isFile() || info.size > MAX_FILE_BYTES) throw Error("COMMITMENT_JOURNAL_INVALID_FILE");
      this.sizes.set(name, info.size);
      this.bytes += info.size;
    }
    if (this.bytes > MAX_BYTES || this.sizes.size > MAX_FILES) throw Error("COMMITMENT_JOURNAL_CAPACITY");
    const expected = {version: 1, ...identity};
    if (this.sizes.has("identity.json")) {
      if (JSON.stringify(this.read("identity.json")) !== JSON.stringify(expected))
        throw Error("COMMITMENT_JOURNAL_IDENTITY_MISMATCH");
    } else {
      if (this.sizes.size) throw Error("COMMITMENT_JOURNAL_IDENTITY_MISSING");
      this.write("identity.json", expected);
    }
  }

  retain(signedBid: gloas.SignedExecutionPayloadBid, stored: StoredPayload): void {
    const {slot, blockHash, parentBlockRoot, payload} = stored;
    if (
      slot !== signedBid.message.slot ||
      blockHash !== toRootHex(signedBid.message.blockHash) ||
      toRootHex(parentBlockRoot) !== toRootHex(signedBid.message.parentBlockRoot)
    )
      throw Error("COMMITMENT_PAYLOAD_MISMATCH");
    const payloadName = `payload-${slot}-${blockHash}.json`;
    if (!this.sizes.has(payloadName)) {
      const record: PayloadRecord = {
        slot,
        blockHash,
        parentBlockRoot: toRootHex(parentBlockRoot),
        sourceId: payload.sourceId,
        value: payload.executionPayloadValue.toString(),
        payload: ssz.gloas.ExecutionPayload.toJson(payload.executionPayload),
        requests: ssz.gloas.ExecutionRequests.toJson(payload.executionRequests),
        blobs: ssz.gloas.BlobsBundle.toJson(payload.blobsBundle),
      };
      this.write(payloadName, record);
    }
    const root = toRootHex(ssz.gloas.SignedExecutionPayloadBid.hashTreeRoot(signedBid));
    const name = `bid-${slot}-${root}.json`;
    if (!this.sizes.has(name)) this.write(name, ssz.gloas.SignedExecutionPayloadBid.toJson(signedBid));
  }

  retainReveal(record: RevealRecord): void {
    const name = `reveal-${record.slot}-${record.blockRoot}.json`;
    if (this.sizes.has(name)) {
      if (JSON.stringify(this.read(name)) !== JSON.stringify(record)) throw Error("COMMITMENT_REVEAL_CONFLICT");
    } else this.write(name, record);
  }

  recover(currentSlot: number, payloads: PayloadStore, bids: BidStore, ledger: BidLedger): number {
    this.prune(currentSlot);
    // Load all required payloads first, before any signed response can be served.
    for (const name of this.sizes.keys()) {
      if (!name.startsWith("payload-")) continue;
      const record = this.read(name) as PayloadRecord;
      const payload = ssz.gloas.ExecutionPayload.fromJson(record.payload);
      if (
        name !== `payload-${record.slot}-${record.blockHash}.json` ||
        record.slot !== payload.slotNumber ||
        record.blockHash !== toRootHex(payload.blockHash)
      )
        throw Error("COMMITMENT_PAYLOAD_MISMATCH");
      payloads.add({
        slot: record.slot,
        parentBlockRoot: fromHex(record.parentBlockRoot),
        blockHash: record.blockHash,
        payload: {
          sourceId: record.sourceId,
          executionPayload: payload,
          executionPayloadValue: BigInt(record.value),
          executionRequests: ssz.gloas.ExecutionRequests.fromJson(record.requests),
          blobsBundle: ssz.gloas.BlobsBundle.fromJson(record.blobs),
        },
      });
    }
    let recovered = 0;
    for (const name of this.sizes.keys()) {
      if (!name.startsWith("bid-")) continue;
      const signed = ssz.gloas.SignedExecutionPayloadBid.fromJson(this.read(name));
      const bid = signed.message;
      const signedBidRoot = toRootHex(ssz.gloas.SignedExecutionPayloadBid.hashTreeRoot(signed));
      if (name !== `bid-${bid.slot}-${signedBidRoot}.json`) throw Error("COMMITMENT_BID_MISMATCH");
      const identity = {
        slot: bid.slot,
        parentBlockRoot: toRootHex(bid.parentBlockRoot),
        parentBlockHash: toRootHex(bid.parentBlockHash),
        blockHash: toRootHex(bid.blockHash),
        signedBidRoot,
        valueGwei: bid.value,
      };
      ledger.recordRevision(identity);
      if (!ledger.hasSubmitted(bid.slot, identity.parentBlockHash, identity.parentBlockRoot))
        ledger.recordBid(identity);
      ledger.reserveRecoveredSlot(bid.slot, bid.value);
      if (bid.slot + 2 >= currentSlot) {
        const payload = payloads.get(identity.blockHash);
        if (
          !payload ||
          payload.slot !== bid.slot ||
          toRootHex(payload.parentBlockRoot) !== identity.parentBlockRoot ||
          toRootHex(payload.payload.executionPayload.parentHash) !== identity.parentBlockHash
        )
          throw Error("COMMITMENT_PAYLOAD_MISSING");
        bids.add(signed);
      }
      recovered++;
    }
    for (const name of this.sizes.keys()) {
      if (!name.startsWith("reveal-")) continue;
      const record = this.read(name) as RevealRecord;
      if (name !== `reveal-${record.slot}-${record.blockRoot}.json`) throw Error("COMMITMENT_REVEAL_MISMATCH");
      ledger.recordReveal(record.slot, record.blockRoot, record.blockHash, record.envelopeRoot);
    }
    return recovered;
  }

  prune(currentSlot: number): void {
    for (const name of this.sizes.keys()) {
      const match = recordName.exec(name);
      if (!match || Number(match[2]) + (match[1] === "payload" ? 2 : KEEP_SLOTS) >= currentSlot) continue;
      fs.unlinkSync(path.join(this.directory, name));
      this.bytes -= this.sizes.get(name) ?? 0;
      this.sizes.delete(name);
    }
    // Unsynced deletion can only restore expired records after a crash. Recovery
    // prunes by slot before serving; it never makes expired offers eligible again.
  }

  private read(name: string): unknown {
    const {data, checksum} = JSON.parse(fs.readFileSync(path.join(this.directory, name), "utf8")) as {
      data: unknown;
      checksum: string;
    };
    if (digest(JSON.stringify(data)) !== checksum) throw Error("COMMITMENT_JOURNAL_CHECKSUM");
    return data;
  }

  private write(name: string, data: unknown): void {
    if (this.failed) throw Error("COMMITMENT_JOURNAL_FAILED");
    const text = JSON.stringify({data, checksum: digest(JSON.stringify(data))});
    const bytes = Buffer.byteLength(text);
    if (bytes > MAX_FILE_BYTES || this.bytes + bytes > MAX_BYTES || this.sizes.size >= MAX_FILES)
      throw Error("COMMITMENT_JOURNAL_CAPACITY");
    const filename = path.join(this.directory, name);
    try {
      const fd = fs.openSync(`${filename}.tmp`, "w", 0o600);
      try {
        fs.writeFileSync(fd, text);
        fs.fsyncSync(fd);
      } finally {
        fs.closeSync(fd);
      }
      fs.renameSync(`${filename}.tmp`, filename);
      syncDirectory(this.directory);
      this.sizes.set(name, bytes);
      this.bytes += bytes;
    } catch (error) {
      this.failed = true;
      throw error;
    }
  }
}

function digest(text: string): string {
  return createHash("sha256").update(text).digest("hex");
}

function syncDirectory(directory: string): void {
  const fd = fs.openSync(directory, "r");
  try {
    fs.fsyncSync(fd);
  } finally {
    fs.closeSync(fd);
  }
}
