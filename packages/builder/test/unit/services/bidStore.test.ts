import {describe, expect, it} from "vitest";
import {ssz} from "@lodestar/types";
import {toRootHex} from "@lodestar/utils";
import {BidStore} from "../../../src/services/bidStore.js";

const parentBlockHash = Buffer.alloc(32, 1);
const parentBlockRoot = Buffer.alloc(32, 2);

function signedBid(slot: number, parentHash = parentBlockHash) {
  const bid = ssz.gloas.SignedExecutionPayloadBid.defaultValue();
  bid.message.slot = slot;
  bid.message.parentBlockHash = parentHash;
  bid.message.parentBlockRoot = parentBlockRoot;
  return bid;
}

describe("BidStore", () => {
  it("returns the bid of a slot and parent", () => {
    const store = new BidStore();
    const bid = signedBid(5);
    store.add(bid);

    expect(store.get(5, toRootHex(parentBlockHash), toRootHex(parentBlockRoot))).toBe(bid);
    expect(store.get(6, toRootHex(parentBlockHash), toRootHex(parentBlockRoot))).toBeNull();
    expect(store.get(5, toRootHex(Buffer.alloc(32, 9)), toRootHex(parentBlockRoot))).toBeNull();
    expect(store.get(5, toRootHex(parentBlockHash), toRootHex(Buffer.alloc(32, 9)))).toBeNull();
  });

  it("keeps bids on different parents of the same slot", () => {
    const store = new BidStore();
    const otherParentHash = Buffer.alloc(32, 3);
    const bid = signedBid(5);
    const otherBid = signedBid(5, otherParentHash);
    store.add(bid);
    store.add(otherBid);

    expect(store.get(5, toRootHex(parentBlockHash), toRootHex(parentBlockRoot))).toBe(bid);
    expect(store.get(5, toRootHex(otherParentHash), toRootHex(parentBlockRoot))).toBe(otherBid);
  });

  it("prunes bids two slots after their slot", () => {
    const store = new BidStore();
    store.add(signedBid(5));

    store.prune(7);
    expect(store.get(5, toRootHex(parentBlockHash), toRootHex(parentBlockRoot))).not.toBeNull();

    store.prune(8);
    expect(store.get(5, toRootHex(parentBlockHash), toRootHex(parentBlockRoot))).toBeNull();
  });
});
