import {afterEach, describe, expect, it, vi} from "vitest";
import {ssz} from "@lodestar/types";
import {toRootHex} from "@lodestar/utils";
import {BidStore} from "../../../src/services/bidStore.js";
import type {OfferLease} from "../../../src/services/offerEligibility.js";

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

describe("offer generations", () => {
  afterEach(() => vi.restoreAllMocks());

  it("withdraws a cancelled generation but retains its signed reveal commitment", () => {
    vi.spyOn(performance, "now").mockReturnValue(100);
    const store = new BidStore();
    const lease: OfferLease = {generation: "0x0", validUntil: 350, validUntilUnixMs: Infinity, ready: true};
    const old = signedBid(5);
    old.message.value = 100;
    store.add(old, {generation: "0x0", lease});
    expect(store.canOffer(old)).toBe(true);
    lease.generation = "0x1";
    lease.ready = false;
    expect(store.get(5, toRootHex(parentBlockHash), toRootHex(parentBlockRoot))).toBeNull();
    expect(store.has(old)).toBe(true);
    const replacement = signedBid(5);
    replacement.message.value = 10;
    replacement.message.blockHash = Buffer.alloc(32, 8);
    lease.ready = true;
    store.add(replacement, {generation: "0x1", lease});
    expect(store.get(5, toRootHex(parentBlockHash), toRootHex(parentBlockRoot))).toBe(replacement);
    expect(store.canOffer(old)).toBe(false);
    expect(store.has(old)).toBe(true);
    // A slow stale signing/retention completion must not displace the new bid.
    store.add(old, {generation: "0x0", lease});
    expect(store.get(5, toRootHex(parentBlockHash), toRootHex(parentBlockRoot))).toBe(replacement);
  });

  it("expires offers on a stopped constructor and allows only a current heartbeat to renew", () => {
    const now = vi.spyOn(performance, "now").mockReturnValue(100);
    const store = new BidStore();
    const lease: OfferLease = {generation: "0x0", validUntil: 350, validUntilUnixMs: Infinity, ready: true};
    const bid = signedBid(5);
    store.add(bid, {generation: "0x0", lease});
    now.mockReturnValue(350);
    expect(store.canOffer(bid)).toBe(false);
    expect(store.has(bid)).toBe(true);
    lease.validUntil = 600;
    expect(store.canOffer(bid)).toBe(true);
    lease.generation = "0x1";
    expect(store.canOffer(bid)).toBe(false);
    store.prune(8);
    expect(store.has(bid)).toBe(false);
  });

  it("recovers reveal obligations without restoring volatile offer permission", () => {
    const store = new BidStore();
    const bid = signedBid(5);
    store.add(bid, "recovery");
    expect(store.has(bid)).toBe(true);
    expect(store.canOffer(bid)).toBe(false);
    expect(store.get(5, toRootHex(parentBlockHash), toRootHex(parentBlockRoot))).toBeNull();
    store.add(bid);
    expect(store.canOffer(bid)).toBe(true);
  });

  it("expires across host suspend even if the monotonic clock does not advance", () => {
    vi.spyOn(performance, "now").mockReturnValue(100);
    const wall = vi.spyOn(Date, "now").mockReturnValue(1000);
    const store = new BidStore();
    const lease: OfferLease = {generation: "0x0", validUntil: 350, validUntilUnixMs: 1250, ready: true};
    const bid = signedBid(5);
    store.add(bid, {generation: "0x0", lease});
    expect(store.canOffer(bid)).toBe(true);
    wall.mockReturnValue(1300);
    expect(store.canOffer(bid)).toBe(false);
    expect(store.has(bid)).toBe(true);
  });
});
