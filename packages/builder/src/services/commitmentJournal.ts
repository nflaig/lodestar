import {createHash} from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import {CELLS_PER_EXT_BLOB, FIELD_ELEMENTS_PER_BLOB} from "@lodestar/params";
import {RootHex, gloas, ssz} from "@lodestar/types";
import {fromHex, toRootHex} from "@lodestar/utils";
import {BidLedger} from "./bidLedger.js";
import {BidStore} from "./bidStore.js";
import {BID_HISTORY_SLOTS, MAX_COMMITMENT_JOURNAL_FILES, PAYLOAD_RETENTION_SLOTS} from "./commitmentLimits.js";
import {PayloadStore, StoredPayload} from "./payloadStore.js";

const KEEP_SLOTS = BID_HISTORY_SLOTS;
const MAX_BYTES = 768 * 1024 * 1024;
const MAX_FILE_BYTES = 128 * 1024 * 1024;
const MAX_FILES = MAX_COMMITMENT_JOURNAL_FILES;
const recordName = /^(payload|bid|reveal|blob)-(\d+)-(0x[0-9a-f]{64})\.(json|bin)$/;
const BLOB_BYTES = FIELD_ELEMENTS_PER_BLOB * 32;
const COMMITMENT_BYTES = 48;
const SIDECAR_BYTES = BLOB_BYTES + COMMITMENT_BYTES * (1 + CELLS_PER_EXT_BLOB);

type PayloadRecord = {
  slot: number;
  parentBlockRoot: RootHex;
  blockHash: RootHex;
  sourceId: string;
  value: string;
  payload: unknown;
  requests: unknown;
  blobs?: unknown;
  blobRefs?: RootHex[];
};

export type RevealRecord = {slot: number; blockRoot: RootHex; blockHash: RootHex; envelopeRoot: RootHex};
export type JournalIdentity = {genesisValidatorsRoot: RootHex; pubkey: string};

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
    identity: JournalIdentity
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
      const match = recordName.exec(name);
      if (!info.isFile() || info.size > MAX_FILE_BYTES || (match && (match[1] === "blob") !== (match[4] === "bin")))
        throw Error("COMMITMENT_JOURNAL_INVALID_FILE");
      this.sizes.set(name, info.size);
      this.bytes += info.size;
      if (match?.[1] === "blob") {
        const bytes = fs.readFileSync(path.join(directory, name));
        if (bytes.length !== SIDECAR_BYTES || `0x${createHash("sha256").update(bytes).digest("hex")}` !== match[3])
          throw Error("COMMITMENT_BLOB_CHECKSUM");
      }
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
    if (
      !ssz.gloas.ExecutionPayloadBid.fields.blobKzgCommitments.equals(
        signedBid.message.blobKzgCommitments,
        payload.blobsBundle.commitments
      )
    )
      throw Error("COMMITMENT_BLOB_MISMATCH");
    const payloadName = `payload-${slot}-${blockHash}.json`;
    if (!this.sizes.has(payloadName)) {
      const {blobs, commitments, proofs} = payload.blobsBundle;
      if (blobs.length !== commitments.length || proofs.length !== blobs.length * CELLS_PER_EXT_BLOB)
        throw Error("COMMITMENT_BLOB_SHAPE");
      const blobRefs = blobs.map((blob, i) => {
        const cellProofs = proofs.slice(i * CELLS_PER_EXT_BLOB, (i + 1) * CELLS_PER_EXT_BLOB);
        if (
          blob.length !== BLOB_BYTES ||
          commitments[i].length !== COMMITMENT_BYTES ||
          cellProofs.some((proof) => proof.length !== COMMITMENT_BYTES)
        )
          throw Error("COMMITMENT_BLOB_SHAPE");
        const bytes = Buffer.concat([blob, commitments[i], ...cellProofs]);
        const hash = `0x${createHash("sha256").update(bytes).digest("hex")}`;
        const name = `blob-${slot}-${hash}.bin`;
        if (!this.sizes.has(name)) this.writeBytes(name, bytes);
        return hash;
      });
      const record: PayloadRecord = {
        slot,
        blockHash,
        parentBlockRoot: toRootHex(parentBlockRoot),
        sourceId: payload.sourceId,
        value: payload.executionPayloadValue.toString(),
        payload: ssz.gloas.ExecutionPayload.toJson(payload.executionPayload),
        requests: ssz.gloas.ExecutionRequests.toJson(payload.executionRequests),
        blobRefs,
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
    const sidecars = new Map<string, {blob: Uint8Array; commitment: Uint8Array; proofs: Uint8Array[]}>();
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
      let blobsBundle: StoredPayload["payload"]["blobsBundle"];
      if (record.blobRefs !== undefined) {
        if (
          !Array.isArray(record.blobRefs) ||
          record.blobRefs.length * SIDECAR_BYTES > MAX_FILE_BYTES ||
          record.blobs !== undefined
        )
          throw Error("COMMITMENT_BLOB_SHAPE");
        blobsBundle = {blobs: [], commitments: [], proofs: []};
        for (const hash of record.blobRefs) {
          if (typeof hash !== "string" || !/^0x[0-9a-f]{64}$/.test(hash)) throw Error("COMMITMENT_BLOB_REFERENCE");
          const name = `blob-${record.slot}-${hash}.bin`;
          let sidecar = sidecars.get(name);
          if (!sidecar) {
            if (!this.sizes.has(name)) throw Error("COMMITMENT_BLOB_MISSING");
            const bytes = new Uint8Array(fs.readFileSync(path.join(this.directory, name)));
            if (bytes.length !== SIDECAR_BYTES || `0x${createHash("sha256").update(bytes).digest("hex")}` !== hash)
              throw Error("COMMITMENT_BLOB_CHECKSUM");
            sidecar = {
              blob: bytes.subarray(0, BLOB_BYTES),
              commitment: bytes.subarray(BLOB_BYTES, BLOB_BYTES + COMMITMENT_BYTES),
              proofs: Array.from({length: CELLS_PER_EXT_BLOB}, (_, i) =>
                bytes.subarray(BLOB_BYTES + (i + 1) * COMMITMENT_BYTES, BLOB_BYTES + (i + 2) * COMMITMENT_BYTES)
              ),
            };
            sidecars.set(name, sidecar);
          }
          blobsBundle.blobs.push(sidecar.blob);
          blobsBundle.commitments.push(sidecar.commitment);
          blobsBundle.proofs.push(...sidecar.proofs);
        }
      } else {
        blobsBundle = ssz.gloas.BlobsBundle.fromJson(record.blobs);
      }
      payloads.add(
        {
          slot: record.slot,
          parentBlockRoot: fromHex(record.parentBlockRoot),
          blockHash: record.blockHash,
          payload: {
            sourceId: record.sourceId,
            executionPayload: payload,
            executionPayloadValue: BigInt(record.value),
            executionRequests: ssz.gloas.ExecutionRequests.fromJson(record.requests),
            blobsBundle,
          },
        },
        "recovery"
      );
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
      if (bid.slot + PAYLOAD_RETENTION_SLOTS >= currentSlot) {
        const payload = payloads.get(identity.blockHash);
        if (
          !payload ||
          payload.slot !== bid.slot ||
          toRootHex(payload.parentBlockRoot) !== identity.parentBlockRoot ||
          toRootHex(payload.payload.executionPayload.parentHash) !== identity.parentBlockHash
        )
          throw Error("COMMITMENT_PAYLOAD_MISSING");
        if (
          !ssz.gloas.ExecutionPayloadBid.fields.blobKzgCommitments.equals(
            bid.blobKzgCommitments,
            payload.payload.blobsBundle.commitments
          )
        )
          throw Error("COMMITMENT_BLOB_MISMATCH");
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
      if (
        !match ||
        Number(match[2]) + (match[1] === "payload" || match[1] === "blob" ? PAYLOAD_RETENTION_SLOTS : KEEP_SLOTS) >=
          currentSlot
      )
        continue;
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
    const text = JSON.stringify({data, checksum: digest(JSON.stringify(data))});
    this.writeBytes(name, Buffer.from(text));
  }

  private writeBytes(name: string, data: Uint8Array): void {
    if (this.failed) throw Error("COMMITMENT_JOURNAL_FAILED");
    const bytes = data.byteLength;
    if (bytes > MAX_FILE_BYTES || this.bytes + bytes > MAX_BYTES || this.sizes.size >= MAX_FILES)
      throw Error("COMMITMENT_JOURNAL_CAPACITY");
    const filename = path.join(this.directory, name);
    try {
      const fd = fs.openSync(`${filename}.tmp`, "w", 0o600);
      try {
        fs.writeFileSync(fd, data);
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
