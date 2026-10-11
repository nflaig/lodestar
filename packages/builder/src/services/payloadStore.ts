import {ForkPostGloas} from "@lodestar/params";
import {BlobsBundle, ExecutionPayload, Root, RootHex, Slot, gloas, ssz} from "@lodestar/types";
import {LodestarError} from "@lodestar/utils";

// NOTE: BuiltPayload will migrate to payloadSource.ts when the payload source lands; the store
// holds it here for now.
export type BuiltPayload = {
  sourceId: string;
  executionPayload: ExecutionPayload<ForkPostGloas>;
  executionRequests: gloas.ExecutionRequests;
  blobsBundle: BlobsBundle<ForkPostGloas>;
  /** Value of the payload to the fee recipient in wei, as reported by the execution client */
  executionPayloadValue: bigint;
};

export type StoredPayload = {
  slot: Slot;
  parentBlockRoot: Root;
  blockHash: RootHex;
  payload: BuiltPayload;
};

/** Keep payloads for this many slots after their target slot, late blocks may still commit to them */
const KEEP_SLOTS = 2;

export class PayloadStoreCapacityError extends LodestarError<{
  code: "PAYLOAD_STORE_CAPACITY";
  bytes: number;
  count: number;
}> {}

export class PayloadStore {
  private readonly byBlockHash = new Map<RootHex, StoredPayload>();
  private readonly sizes = new Map<RootHex, number>();
  private retainedBytes = 0;

  constructor(
    private readonly maxBytes = 256 * 1024 * 1024,
    private readonly maxPayloads = 192
  ) {}

  add(payload: StoredPayload): void {
    if (this.byBlockHash.has(payload.blockHash)) return;
    const {executionPayload, executionRequests, blobsBundle} = payload.payload;
    const bytes =
      ssz.gloas.ExecutionPayload.value_serializedSize(executionPayload) +
      ssz.gloas.ExecutionRequests.value_serializedSize(executionRequests) +
      ssz.gloas.BlobsBundle.value_serializedSize(blobsBundle) +
      1024;
    if (this.retainedBytes + bytes > this.maxBytes || this.byBlockHash.size >= this.maxPayloads) {
      // Never evict a previously advertised payload to make room for a newer offer.
      throw new PayloadStoreCapacityError({
        code: "PAYLOAD_STORE_CAPACITY",
        bytes: this.retainedBytes,
        count: this.byBlockHash.size,
      });
    }
    this.byBlockHash.set(payload.blockHash, payload);
    this.sizes.set(payload.blockHash, bytes);
    this.retainedBytes += bytes;
  }

  get(blockHash: RootHex): StoredPayload | null {
    return this.byBlockHash.get(blockHash) ?? null;
  }

  has(blockHash: RootHex): boolean {
    return this.byBlockHash.has(blockHash);
  }

  prune(currentSlot: Slot): void {
    for (const [blockHash, {slot}] of this.byBlockHash) {
      if (slot + KEEP_SLOTS < currentSlot) {
        this.byBlockHash.delete(blockHash);
        this.retainedBytes -= this.sizes.get(blockHash) ?? 0;
        this.sizes.delete(blockHash);
      }
    }
  }

  get size(): number {
    return this.byBlockHash.size;
  }

  /** Serialized retained content plus a per-entry allowance; not process RSS. */
  get bytes(): number {
    return this.retainedBytes;
  }
}
