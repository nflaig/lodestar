import {RootHex, Slot, gloas, ssz} from "@lodestar/types";
import {toRootHex} from "@lodestar/utils";

/** Keep bids for this many slots after their slot, same as the payloads they commit to */
const KEEP_SLOTS = 2;

/** Retains the published signed bids, a bid is unique per slot and parent */
export class BidStore {
  private readonly revisions = new Map<RootHex, gloas.SignedExecutionPayloadBid>();
  private readonly bySlotAndParent = new Map<string, gloas.SignedExecutionPayloadBid>();

  add(signedBid: gloas.SignedExecutionPayloadBid): void {
    const {slot, parentBlockHash, parentBlockRoot} = signedBid.message;
    this.revisions.set(toRootHex(ssz.gloas.SignedExecutionPayloadBid.hashTreeRoot(signedBid)), signedBid);
    const key = getKey(slot, toRootHex(parentBlockHash), toRootHex(parentBlockRoot));
    const best = this.bySlotAndParent.get(key);
    if (!best || signedBid.message.value > best.message.value) this.bySlotAndParent.set(key, signedBid);
  }

  get(slot: Slot, parentBlockHash: RootHex, parentBlockRoot: RootHex): gloas.SignedExecutionPayloadBid | null {
    return this.bySlotAndParent.get(getKey(slot, parentBlockHash, parentBlockRoot)) ?? null;
  }

  has(signedBid: gloas.SignedExecutionPayloadBid): boolean {
    return this.revisions.has(toRootHex(ssz.gloas.SignedExecutionPayloadBid.hashTreeRoot(signedBid)));
  }

  prune(currentSlot: Slot): void {
    for (const [root, signedBid] of this.revisions) {
      if (signedBid.message.slot + KEEP_SLOTS < currentSlot) this.revisions.delete(root);
    }
    for (const [key, signedBid] of this.bySlotAndParent) {
      if (signedBid.message.slot + KEEP_SLOTS < currentSlot) {
        this.bySlotAndParent.delete(key);
      }
    }
  }
}

function getKey(slot: Slot, parentBlockHash: RootHex, parentBlockRoot: RootHex): string {
  return `${slot}:${parentBlockHash}:${parentBlockRoot}`;
}
