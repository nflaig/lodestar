import {EventEmitter} from "node:events";
import {describe, expect, it} from "vitest";
import {ssz} from "@lodestar/types";
import {toRootHex} from "@lodestar/utils";
import {AsyncCommitmentJournal, type JournalRequest} from "../../../src/services/asyncCommitmentJournal.js";
import {mockBuiltPayload} from "../utils/payload.js";

class Thread extends EventEmitter {
  readonly sent: JournalRequest[] = [];
  postMessage(request: JournalRequest): void {
    this.sent.push(request);
  }
  async terminate(): Promise<number> {
    this.emit("exit", 0);
    return 0;
  }
  finish(error?: string): void {
    this.emit("message", {id: this.sent.at(-1)?.id, error});
  }
}

function setup() {
  const thread = new Thread();
  const journal = new AsyncCommitmentJournal(thread);
  thread.emit("message", {id: 0});
  const bid = ssz.gloas.SignedExecutionPayloadBid.defaultValue();
  const payload = mockBuiltPayload();
  const stored = {
    slot: 1,
    parentBlockRoot: new Uint8Array(32),
    blockHash: toRootHex(payload.executionPayload.blockHash),
    payload,
  };
  return {thread, journal, bid, stored};
}

describe("AsyncCommitmentJournal", () => {
  it("bounds queued offers, reserves reveal capacity, and waits for every acknowledgement on close", async () => {
    const {thread, journal, bid, stored} = setup();
    const offers = Array.from({length: 8}, () => journal.retain(bid, stored));
    await expect(journal.retain(bid, stored)).rejects.toThrow("COMMITMENT_WORKER_CAPACITY");
    const reveal = journal.retainReveal({
      slot: 1,
      blockHash: stored.blockHash,
      blockRoot: stored.blockHash,
      envelopeRoot: stored.blockHash,
    });
    expect(thread.sent).toHaveLength(1);
    thread.finish();
    expect(thread.sent.at(-1)?.operation.kind).toBe("reveal");
    thread.finish();
    await reveal;
    const close = journal.close();
    for (let i = 1; i < 8; i++) thread.finish();
    await Promise.all(offers);
    await close;
    await expect(journal.retain(bid, stored)).rejects.toThrow("COMMITMENT_WORKER_CLOSED");
  });

  it("rejects outstanding work and new offers after a worker failure", async () => {
    const {thread, journal, bid, stored} = setup();
    const pending = [journal.retain(bid, stored), journal.retain(bid, stored)];
    const outcomes = Promise.allSettled(pending);
    thread.emit("error", Error("worker failed"));
    expect((await outcomes).map((outcome) => outcome.status)).toEqual(["rejected", "rejected"]);
    await expect(journal.retain(bid, stored)).rejects.toThrow("worker failed");
    await journal.close();
    expect(thread.sent).toHaveLength(1);
  });

  it("coalesces queued pruning without overtaking the active durable write", async () => {
    const {thread, journal, bid, stored} = setup();
    const offer = journal.retain(bid, stored);
    const first = journal.prune(2);
    const second = journal.prune(3);
    expect(first).toBe(second);
    expect(thread.sent).toHaveLength(1);
    thread.finish();
    expect(thread.sent.at(-1)?.operation).toEqual({kind: "prune", slot: 3});
    thread.finish();
    await Promise.all([offer, first, second]);
    await journal.close();
  });

  it("propagates uncertain disk failures without acknowledging the offer", async () => {
    const {thread, journal, bid, stored} = setup();
    const offer = journal.retain(bid, stored);
    thread.finish("disk failed");
    await expect(offer).rejects.toThrow("disk failed");
    await journal.close();
  });
});
