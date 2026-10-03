import {RootHex, Slot, gloas} from "@lodestar/types";
import {toRootHex} from "@lodestar/utils";

/** Keep bids for this many slots after their slot, same as the payloads they commit to */
const KEEP_SLOTS = 2;

/** Retains the published signed bids, a bid is unique per slot and parent */
export class BidStore {
  private readonly bySlotAndParent = new Map<string, gloas.SignedExecutionPayloadBid>();

  add(signedBid: gloas.SignedExecutionPayloadBid): void {
    const {slot, parentBlockHash, parentBlockRoot} = signedBid.message;
    this.bySlotAndParent.set(getKey(slot, toRootHex(parentBlockHash), toRootHex(parentBlockRoot)), signedBid);
  }

  get(slot: Slot, parentBlockHash: RootHex, parentBlockRoot: RootHex): gloas.SignedExecutionPayloadBid | null {
    return this.bySlotAndParent.get(getKey(slot, parentBlockHash, parentBlockRoot)) ?? null;
  }

  prune(currentSlot: Slot): void {
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
