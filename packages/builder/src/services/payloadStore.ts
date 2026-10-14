import {ForkPostGloas} from "@lodestar/params";
import {BlobsBundle, ExecutionPayload, Root, RootHex, Slot, gloas, ssz} from "@lodestar/types";
import {LodestarError} from "@lodestar/utils";
import {MAX_RETAINED_PAYLOADS, PAYLOAD_RETENTION_SLOTS} from "./commitmentLimits.js";

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
const KEEP_SLOTS = PAYLOAD_RETENTION_SLOTS;

export class PayloadStoreCapacityError extends LodestarError<{
  code: "PAYLOAD_STORE_CAPACITY";
  bytes: number;
  count: number;
  slot: Slot;
  slotBytes: number;
  maxSlotBytes: number;
}> {}

export class PayloadStore {
  private readonly byBlockHash = new Map<RootHex, StoredPayload>();
  private readonly sizes = new Map<RootHex, number>();
  private readonly bytesBySlot = new Map<Slot, number>();
  private retainedBytes = 0;

  constructor(
    private readonly maxBytes = 256 * 1024 * 1024,
    private readonly maxPayloads = MAX_RETAINED_PAYLOADS
  ) {}

  add(payload: StoredPayload, admission: "offer" | "recovery" = "offer"): void {
    if (this.byBlockHash.has(payload.blockHash)) return;
    const {executionPayload, executionRequests, blobsBundle} = payload.payload;
    const bytes =
      ssz.gloas.ExecutionPayload.value_serializedSize(executionPayload) +
      ssz.gloas.ExecutionRequests.value_serializedSize(executionRequests) +
      ssz.gloas.BlobsBundle.value_serializedSize(blobsBundle) +
      1024;
    const slotBytes = this.bytesBySlot.get(payload.slot) ?? 0;
    const maxSlotBytes = Math.floor(this.maxBytes / (PAYLOAD_RETENTION_SLOTS + 2));
    // Recovery must retain commitments made before the per-slot admission budget existed.
    if (
      this.retainedBytes + bytes > this.maxBytes ||
      this.byBlockHash.size >= this.maxPayloads ||
      (admission === "offer" && slotBytes + bytes > maxSlotBytes)
    ) {
      // Never evict a previously advertised payload to make room for a newer offer.
      throw new PayloadStoreCapacityError({
        code: "PAYLOAD_STORE_CAPACITY",
        bytes: this.retainedBytes,
        count: this.byBlockHash.size,
        slot: payload.slot,
        slotBytes,
        maxSlotBytes,
      });
    }
    this.byBlockHash.set(payload.blockHash, payload);
    this.sizes.set(payload.blockHash, bytes);
    this.retainedBytes += bytes;
    this.bytesBySlot.set(payload.slot, slotBytes + bytes);
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
        this.bytesBySlot.delete(slot);
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
