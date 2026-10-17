import {RootHex, Slot, gloas, ssz} from "@lodestar/types";
import {toRootHex} from "@lodestar/utils";
import {PAYLOAD_RETENTION_SLOTS} from "./commitmentLimits.js";
import {type OfferEligibility, isOfferEligible, recoveredOfferEligibility} from "./offerEligibility.js";

/** Keep bids for this many slots after their slot, same as the payloads they commit to */
const KEEP_SLOTS = PAYLOAD_RETENTION_SLOTS;

/** Retains the published signed bids, a bid is unique per slot and parent */
export class BidStore {
  private readonly revisions = new Map<RootHex, gloas.SignedExecutionPayloadBid>();
  private readonly eligibility = new Map<RootHex, OfferEligibility>();
  private readonly bySlotAndParent = new Map<string, Map<RootHex, gloas.SignedExecutionPayloadBid>>();

  add(signedBid: gloas.SignedExecutionPayloadBid, eligibility?: OfferEligibility | "recovery"): void {
    const {slot, parentBlockHash, parentBlockRoot} = signedBid.message;
    const root = toRootHex(ssz.gloas.SignedExecutionPayloadBid.hashTreeRoot(signedBid));
    this.revisions.set(root, signedBid);
    if (eligibility !== undefined)
      this.eligibility.set(root, eligibility === "recovery" ? recoveredOfferEligibility() : eligibility);
    else if (this.eligibility.get(root)?.generation === "recovered")
      // A trusted current V1 admission may validate an identical recovered bid.
      // Never erase a live generation's guard through an unqualified add.
      this.eligibility.delete(root);
    const key = getKey(slot, toRootHex(parentBlockHash), toRootHex(parentBlockRoot));
    let revisions = this.bySlotAndParent.get(key);
    if (!revisions) {
      revisions = new Map();
      this.bySlotAndParent.set(key, revisions);
    }
    revisions.set(root, signedBid);
  }

  get(slot: Slot, parentBlockHash: RootHex, parentBlockRoot: RootHex): gloas.SignedExecutionPayloadBid | null {
    let best: gloas.SignedExecutionPayloadBid | null = null;
    // At most the admitted revision limit for this slot. Guards can expire or
    // renew without changing a payload, so select from retained revisions using
    // their current permission. The stored roots avoid rehashing each candidate.
    for (const [root, bid] of this.bySlotAndParent.get(getKey(slot, parentBlockHash, parentBlockRoot)) ?? []) {
      if (isOfferEligible(this.eligibility.get(root)) && (!best || bid.message.value > best.message.value)) best = bid;
    }
    return best;
  }

  getOfferEligibility(signedBid: gloas.SignedExecutionPayloadBid): OfferEligibility | undefined {
    return this.eligibility.get(toRootHex(ssz.gloas.SignedExecutionPayloadBid.hashTreeRoot(signedBid)));
  }

  canOffer(signedBid: gloas.SignedExecutionPayloadBid): boolean {
    const root = toRootHex(ssz.gloas.SignedExecutionPayloadBid.hashTreeRoot(signedBid));
    return this.revisions.has(root) && isOfferEligible(this.eligibility.get(root));
  }

  has(signedBid: gloas.SignedExecutionPayloadBid): boolean {
    return this.revisions.has(toRootHex(ssz.gloas.SignedExecutionPayloadBid.hashTreeRoot(signedBid)));
  }

  prune(currentSlot: Slot): void {
    for (const [root, signedBid] of this.revisions) {
      if (signedBid.message.slot + KEEP_SLOTS < currentSlot) {
        this.revisions.delete(root);
        this.eligibility.delete(root);
        const {slot, parentBlockHash, parentBlockRoot} = signedBid.message;
        const key = getKey(slot, toRootHex(parentBlockHash), toRootHex(parentBlockRoot));
        const revisions = this.bySlotAndParent.get(key);
        revisions?.delete(root);
        if (revisions?.size === 0) this.bySlotAndParent.delete(key);
      }
    }
  }
}

function getKey(slot: Slot, parentBlockHash: RootHex, parentBlockRoot: RootHex): string {
  return `${slot}:${parentBlockHash}:${parentBlockRoot}`;
}
