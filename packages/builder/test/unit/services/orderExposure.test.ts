import {createHash} from "node:crypto";
import {EventEmitter} from "node:events";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import {afterEach, describe, expect, it, vi} from "vitest";
import {ssz} from "@lodestar/types";
import {toRootHex} from "@lodestar/utils";
import {
  AsyncCommitmentJournal,
  type JournalReply,
  type JournalRequest,
} from "../../../src/services/asyncCommitmentJournal.js";
import {BidLedger} from "../../../src/services/bidLedger.js";
import {BidStore} from "../../../src/services/bidStore.js";
import {CommitmentJournal} from "../../../src/services/commitmentJournal.js";
import {BID_HISTORY_SLOTS} from "../../../src/services/commitmentLimits.js";
import {OrderExposure, type OrderExposureQuery} from "../../../src/services/orderExposure.js";
import {parseOrderMetadata} from "../../../src/services/orderMetadata.js";
import {PayloadStore} from "../../../src/services/payloadStore.js";
import {mockBuiltPayload} from "../utils/payload.js";

const orderId = `0x${"12".repeat(32)}`;
const otherId = `0x${"34".repeat(32)}`;
const identity = {genesisValidatorsRoot: toRootHex(Buffer.alloc(32, 1)), pubkey: "test"};
const directories: string[] = [];
const threads: Thread[] = [];

class Thread extends EventEmitter {
  readonly pending: JournalRequest[] = [];
  constructor(readonly journal: CommitmentJournal) {
    super();
    threads.push(this);
  }
  postMessage(request: JournalRequest): void {
    this.pending.push(request);
  }
  finish(): void {
    const request = this.pending.shift();
    if (!request) throw Error("Missing journal request");
    const reply: JournalReply = {id: request.id};
    try {
      const operation = request.operation;
      if (operation.kind === "retain") reply.reference = this.journal.retain(operation.bid, operation.stored);
      if (operation.kind === "retainReference") this.journal.retainReference(operation.bid, operation.reference);
      if (operation.kind === "prune") this.journal.prune(operation.slot);
      if (operation.kind === "reveal") this.journal.retainReveal(operation.record);
      if (operation.kind === "exposure")
        reply.exposure = this.journal.queryExposure(operation.orderId, operation.admittedAtUnixMs);
    } catch (error) {
      reply.error = error instanceof Error ? error.message : String(error);
    }
    this.emit("message", reply);
  }
  async terminate(): Promise<number> {
    this.emit("exit", 0);
    return 0;
  }
}

function setup(shared?: string) {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "order-exposure-"));
  directories.push(directory);
  const journal = new CommitmentJournal(directory, identity, undefined, shared);
  const thread = new Thread(journal);
  const worker = new AsyncCommitmentJournal(thread);
  thread.emit("message", {id: 0});
  return {directory, journal, thread, worker};
}

function fixture(index = 0, ids: string[] | null = [orderId]) {
  const payload = {
    ...mockBuiltPayload({slot: 100, valueGwei: 1}),
    orderMetadata: parseOrderMetadata({version: 1, orderIds: ids, doNotOfferBefore: 0}),
  };
  const bid = ssz.gloas.SignedExecutionPayloadBid.defaultValue();
  bid.message.slot = 100;
  bid.message.builderIndex = index;
  bid.message.blockHash = payload.executionPayload.blockHash;
  bid.message.parentBlockHash = payload.executionPayload.parentHash;
  bid.message.executionRequestsRoot = ssz.gloas.ExecutionRequests.hashTreeRoot(payload.executionRequests);
  bid.message.value = 1;
  return {
    bid,
    stored: {
      slot: 100,
      parentBlockRoot: bid.message.parentBlockRoot,
      blockHash: toRootHex(bid.message.blockHash),
      payload,
    },
  };
}

function request(exposure: OrderExposure, id = orderId): OrderExposureQuery {
  const capability = exposure.capabilities();
  return {
    version: 1,
    action: "query",
    orderId: id,
    namespace: `0x${"ab".repeat(16)}`,
    generation: "0x1",
    admittedAtUnixMs: capability.observedAtUnixMs,
    admittedPublisherInstance: capability.publisherInstance,
  };
}

afterEach(() => {
  for (const thread of threads.splice(0)) thread.emit("exit", 0);
  vi.useRealTimers();
  vi.restoreAllMocks();
  for (const directory of directories.splice(0)) fs.rmSync(directory, {recursive: true, force: true});
});

describe("order exposure receipts", () => {
  it("waits for primary and staged durable writes, including shared payload references", async () => {
    const primary = setup();
    const staged = setup(primary.directory);
    const exposure = new OrderExposure([primary.worker, staged.worker]);
    const query = request(exposure);
    const first = fixture();
    const retained = primary.worker.retain(first.bid, first.stored);
    primary.thread.finish();
    await retained;
    const second = fixture(9);
    const stagedWrite = staged.worker.retainShared(
      second.bid,
      second.stored,
      primary.worker.getReference(first.stored)
    );
    const receipt = exposure.query(query);
    primary.thread.finish();
    expect(staged.thread.pending[0].operation.kind).toBe("retainReference");
    staged.thread.finish();
    await stagedWrite;
    await Promise.resolve();
    staged.thread.finish();
    expect(await receipt).toMatchObject({
      orderId,
      namespace: query.namespace,
      generation: query.generation,
      barrierComplete: true,
      coverageComplete: true,
      priorCommitmentExposure: "possible",
      commitmentCount: 2,
      commitments: [{builderIndex: 0}, {builderIndex: 9}],
    });
    const recovered = new CommitmentJournal(staged.directory, identity);
    const payloads = new PayloadStore();
    expect(recovered.recover(100, payloads, new BidStore(), new BidLedger())).toBe(1);
    expect(payloads.get(first.stored.blockHash)?.payload.orderMetadata).toEqual(first.stored.payload.orderMetadata);
    expect(recovered.queryExposure(orderId, query.admittedAtUnixMs).commitmentCount).toBe(1);
    await primary.worker.close();
    await staged.worker.close();
  });

  it("proves none only for the same continuous instance after every barrier", async () => {
    const primary = setup();
    const exposure = new OrderExposure([primary.worker]);
    const query = request(exposure);
    const receipt = exposure.query(query);
    primary.thread.finish();
    expect(await receipt).toMatchObject({priorCommitmentExposure: "none", coverageComplete: true});
    for (const changed of [
      {...query, admittedPublisherInstance: "previous"},
      {...query, admittedAtUnixMs: exposure.capabilities().coverageSinceUnixMs - 1},
      {...query, admittedAtUnixMs: Date.now() + 60_000},
    ]) {
      const result = exposure.query(changed);
      primary.thread.finish();
      expect(await result).toMatchObject({priorCommitmentExposure: "unknown", coverageComplete: false});
    }
    await primary.worker.close();
  });

  it("covers a shared-retention fallback enqueued after its reference reply", async () => {
    const primary = setup();
    const staged = setup(primary.directory);
    const f = fixture();
    const first = primary.worker.retain(f.bid, f.stored);
    primary.thread.finish();
    await first;
    const reference = primary.worker.getReference(f.stored);
    if (!reference) throw Error("Missing durable reference");
    fs.unlinkSync(path.join(primary.directory, reference.files[0].name));
    const exposure = new OrderExposure([staged.worker]);
    const q = request(exposure);
    const second = fixture(9);
    const write = staged.worker.retainShared(second.bid, second.stored, reference);
    const result = exposure.query(q);
    for (let turn = 0; turn < 10; turn++) {
      await Promise.resolve();
      await Promise.resolve();
      if (staged.thread.pending.length > 0) staged.thread.finish();
    }
    expect(await write).toBe("snapshot");
    expect(await result).toMatchObject({priorCommitmentExposure: "possible", commitmentCount: 1});
    await primary.worker.close();
    await staged.worker.close();
  });

  it("bounds an unfinished shared fallback without allowing repeated abandoned barriers", async () => {
    vi.useFakeTimers();
    const primary = setup();
    const staged = setup(primary.directory);
    const f = fixture();
    const first = primary.worker.retain(f.bid, f.stored);
    primary.thread.finish();
    await first;
    const reference = primary.worker.getReference(f.stored);
    if (!reference) throw Error("Missing durable reference");
    fs.unlinkSync(path.join(primary.directory, reference.files[0].name));
    const exposure = new OrderExposure([staged.worker]);
    const q = request(exposure);
    const second = fixture(9);
    const write = staged.worker.retainShared(second.bid, second.stored, reference);
    const result = exposure.query(q);
    staged.thread.finish();
    await vi.advanceTimersByTimeAsync(1001);
    expect(await result).toMatchObject({priorCommitmentExposure: "unknown", barrierComplete: false});
    await expect(exposure.query(q)).rejects.toThrow("ORDER_EXPOSURE_BUSY");
    expect(staged.thread.pending).toHaveLength(1);
    expect(staged.thread.pending[0].operation.kind).toBe("retain");
    staged.thread.finish();
    await write;
    await vi.advanceTimersByTimeAsync(0);
    staged.thread.finish();
    await vi.advanceTimersByTimeAsync(0);
    const later = exposure.query(q);
    staged.thread.finish();
    expect(await later).toMatchObject({priorCommitmentExposure: "possible", commitmentCount: 1});
    await primary.worker.close();
    await staged.worker.close();
  });

  it("uses a publisher-clock admission lower bound and searches commitments older than it", async () => {
    vi.useFakeTimers();
    const primary = setup();
    const exposure = new OrderExposure([primary.worker]);
    const f = fixture();
    primary.journal.retain(f.bid, f.stored);
    await vi.advanceTimersByTimeAsync(10);
    const q = request(exposure);
    const publisherTime = Date.now();
    const gatewayTime = publisherTime + 60_000;
    expect(q.admittedAtUnixMs).toBe(publisherTime);
    expect(q.admittedAtUnixMs).not.toBe(gatewayTime);
    const positive = exposure.query(q);
    primary.thread.finish();
    expect(await positive).toMatchObject({priorCommitmentExposure: "possible", commitmentCount: 1});
    const none = exposure.query({...q, orderId: otherId});
    primary.thread.finish();
    expect(await none).toMatchObject({priorCommitmentExposure: "none", coverageComplete: true});
    primary.journal.prune(100 + BID_HISTORY_SLOTS + 1);
    const oldBound = {...q, admittedAtUnixMs: publisherTime - 10};
    const pruned = exposure.query(oldBound);
    primary.thread.finish();
    expect(await pruned).toMatchObject({priorCommitmentExposure: "unknown", coverageComplete: false});
    await primary.worker.close();
  });

  it("never proves absence for unknown membership, legacy recovery or pruned history", () => {
    const first = setup();
    const unknown = fixture(0, null);
    first.journal.retain(unknown.bid, unknown.stored);
    expect(first.journal.queryExposure(otherId, Date.now()).coverageComplete).toBe(false);
    const second = setup();
    const known = fixture();
    const time = Date.now();
    second.journal.retain(known.bid, known.stored);
    second.journal.prune(100 + BID_HISTORY_SLOTS + 1);
    expect(second.journal.queryExposure(orderId, time)).toMatchObject({coverageComplete: false, commitmentCount: 0});
    const legacy = fixture();
    delete legacy.stored.payload.orderMetadata;
    const third = setup();
    third.journal.retain(legacy.bid, legacy.stored);
    const recovered = new CommitmentJournal(third.directory, identity);
    expect(recovered.queryExposure(orderId, time).coverageComplete).toBe(false);
  });

  it("rejects metadata rebinding for a repeated payload or signed root, before and after recovery", () => {
    const {journal, directory} = setup();
    const old = fixture();
    journal.retain(old.bid, old.stored);
    const changed = fixture(0, [otherId]);
    expect(() => journal.retain(changed.bid, changed.stored)).toThrow("ORDER_METADATA_CONFLICT");
    const restored = new CommitmentJournal(directory, identity);
    expect(() => restored.retain(changed.bid, changed.stored)).toThrow("ORDER_METADATA_CONFLICT");
    const store = new PayloadStore();
    store.add(old.stored);
    expect(() => store.add(changed.stored)).toThrow("ORDER_METADATA_CONFLICT");
    expect(restored.queryExposure(orderId, Date.now()).commitmentCount).toBe(1);
    expect(restored.queryExposure(otherId, Date.now()).commitmentCount).toBe(0);
  });

  it("returns unknown on a bounded wait, then preserves a late durable positive", async () => {
    vi.useFakeTimers();
    const primary = setup();
    const exposure = new OrderExposure([primary.worker]);
    const query = request(exposure);
    const f = fixture();
    const write = primary.worker.retain(f.bid, f.stored);
    const first = exposure.query(query);
    await vi.advanceTimersByTimeAsync(1001);
    expect(await first).toMatchObject({priorCommitmentExposure: "unknown", barrierComplete: false});
    primary.thread.finish();
    await write;
    primary.thread.finish();
    await vi.advanceTimersByTimeAsync(0);
    const second = exposure.query(query);
    primary.thread.finish();
    expect(await second).toMatchObject({priorCommitmentExposure: "possible", commitmentCount: 1});
    await primary.worker.close();
  });

  it("keeps a completed positive when another identity's barrier times out", async () => {
    vi.useFakeTimers();
    const primary = setup();
    const staged = setup();
    const exposure = new OrderExposure([primary.worker, staged.worker]);
    const q = request(exposure);
    const f = fixture();
    primary.journal.retain(f.bid, f.stored);
    const result = exposure.query(q);
    primary.thread.finish();
    await vi.advanceTimersByTimeAsync(1001);
    expect(await result).toMatchObject({
      priorCommitmentExposure: "possible",
      commitmentCount: 1,
      barrierComplete: false,
      coverageComplete: false,
    });
    staged.thread.finish();
    await primary.worker.close();
    await staged.worker.close();
  });

  it("keeps timed-out queries bounded without consuming existing reveal headroom", async () => {
    vi.useFakeTimers();
    const primary = setup();
    const exposure = new OrderExposure([primary.worker]);
    const q = request(exposure);
    const f = fixture();
    const write = primary.worker.retain(f.bid, f.stored);
    const result = exposure.query(q);
    await vi.advanceTimersByTimeAsync(1001);
    expect(await result).toMatchObject({priorCommitmentExposure: "unknown"});
    for (let i = 0; i < 20; i++) await expect(exposure.query(q)).rejects.toThrow("ORDER_EXPOSURE_BUSY");
    const reveals = Array.from({length: 15}, (_, i) =>
      primary.worker.retainReveal({
        slot: 100,
        blockRoot: toRootHex(Buffer.alloc(32, i + 1)),
        blockHash: f.stored.blockHash,
        envelopeRoot: toRootHex(Buffer.alloc(32, i + 1)),
      })
    );
    const completed = Promise.all(reveals);
    primary.thread.finish();
    await write;
    for (let i = 0; i < 15; i++) {
      expect(primary.thread.pending[0].operation.kind).toBe("reveal");
      primary.thread.finish();
    }
    await completed;
    expect(primary.thread.pending[0].operation.kind).toBe("exposure");
    primary.thread.finish();
    await vi.advanceTimersByTimeAsync(0);
    await primary.worker.close();
  });

  it("invalidates absence after a forward clock discontinuity", async () => {
    vi.useFakeTimers();
    const primary = setup();
    const exposure = new OrderExposure([primary.worker]);
    const q = request(exposure);
    vi.setSystemTime(Date.now() + 10_000);
    const capability = exposure.capabilities();
    expect(capability.ready).toBe(false);
    expect(capability.publisherInstance).not.toBe(q.admittedPublisherInstance);
    const result = exposure.query(q);
    primary.thread.finish();
    expect(await result).toMatchObject({priorCommitmentExposure: "unknown", coverageComplete: false});
    await primary.worker.close();
  });

  it("never proves absence after an idle subsecond rollback, restoration and pruning", async () => {
    vi.useFakeTimers();
    const primary = setup();
    const exposure = new OrderExposure([primary.worker]);
    await vi.advanceTimersByTimeAsync(2000);
    const q = request(exposure);
    const f = fixture();
    vi.setSystemTime(Date.now() - 500);
    primary.journal.retain(f.bid, f.stored);
    vi.setSystemTime(Date.now() + 500);
    expect(primary.journal.queryExposure(orderId, q.admittedAtUnixMs).commitmentCount).toBe(1);
    primary.journal.prune(100 + BID_HISTORY_SLOTS + 1);
    const result = exposure.query(q);
    primary.thread.finish();
    expect(await result).toMatchObject({priorCommitmentExposure: "unknown", coverageComplete: false});
    await primary.worker.close();
  });

  it("covers clock rounding at prune even when the adjustment is below the tolerance", () => {
    vi.useFakeTimers();
    const primary = setup();
    const f = fixture();
    const admitted = Date.now();
    vi.setSystemTime(admitted - 1);
    primary.journal.retain(f.bid, f.stored);
    primary.journal.prune(100 + BID_HISTORY_SLOTS + 1);
    vi.setSystemTime(admitted);
    expect(primary.journal.queryExposure(orderId, admitted).coverageComplete).toBe(false);
  });

  it("changes instance and refuses absence after clock rollback or a missing identity", async () => {
    vi.useFakeTimers();
    const primary = setup();
    const exposure = new OrderExposure([primary.worker]);
    const first = exposure.capabilities();
    vi.setSystemTime(Date.now() - 1);
    const next = exposure.capabilities();
    expect(next.ready).toBe(false);
    expect(next.publisherInstance).not.toBe(first.publisherInstance);
    expect(new OrderExposure([primary.worker, undefined]).capabilities().ready).toBe(false);
    await primary.worker.close();
  });

  it("rejects too-early persistence and permits the exact not-before boundary", () => {
    vi.useFakeTimers();
    const {journal} = setup();
    const f = fixture();
    f.stored.payload.orderMetadata = parseOrderMetadata({
      version: 1,
      orderIds: [orderId],
      doNotOfferBefore: Date.now() + 1,
    });
    expect(() => journal.retain(f.bid, f.stored)).toThrow("ORDER_OFFER_TOO_EARLY");
    vi.setSystemTime(Date.now() + 1);
    journal.retain(f.bid, f.stored);
    expect(journal.queryExposure(orderId, Date.now()).commitmentCount).toBe(1);
    vi.setSystemTime(Date.now() - 1);
    expect(() => journal.retain(f.bid, f.stored)).toThrow("ORDER_OFFER_TOO_EARLY");
  });

  it("bounds returned evidence while preserving the full known count", async () => {
    const primary = setup();
    const exposure = new OrderExposure([primary.worker]);
    const q = request(exposure);
    const f = fixture();
    for (let value = 1; value <= 70; value++) {
      const bid = structuredClone(f.bid);
      bid.message.value = value;
      primary.journal.retain(bid, f.stored);
    }
    const result = exposure.query(q);
    primary.thread.finish();
    const receipt = await result;
    expect(receipt.priorCommitmentExposure).toBe("possible");
    expect(receipt.commitmentCount).toBe(70);
    expect(receipt.commitments).toHaveLength(64);
    expect(receipt.truncated).toBe(true);
    await primary.worker.close();
  });

  it("recovers a possible commitment after its final directory sync failed", () => {
    const {journal, directory} = setup();
    const f = fixture();
    const rename = fs.renameSync;
    const sync = fs.fsyncSync;
    let bidRenamed = false;
    const renaming = vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
      rename(from, to);
      if (String(to).includes("/bid-")) bidRenamed = true;
    });
    const syncing = vi.spyOn(fs, "fsyncSync").mockImplementation((fd) => {
      if (bidRenamed) throw Error("injected bid directory sync failure");
      sync(fd);
    });
    expect(() => journal.retain(f.bid, f.stored)).toThrow("injected bid directory sync failure");
    expect(journal.queryExposure(orderId, Date.now()).coverageComplete).toBe(false);
    renaming.mockRestore();
    syncing.mockRestore();
    const recovered = new CommitmentJournal(directory, identity);
    expect(recovered.queryExposure(orderId, Date.now()).commitmentCount).toBe(1);
    expect(recovered.recover(100, new PayloadStore(), new BidStore(), new BidLedger())).toBe(1);
  });

  it.each(["legacy", "unsupported metadata"] as const)(
    "retains historical reveals but refuses absence after %s recovery",
    (format) => {
      const {journal, directory} = setup();
      const f = fixture();
      journal.retain(f.bid, f.stored);
      const bidName = fs.readdirSync(directory).find((name) => name.startsWith("bid-"));
      if (!bidName) throw Error("Missing committed bid");
      const filename = path.join(directory, bidName);
      const record = JSON.parse(fs.readFileSync(filename, "utf8"));
      const data =
        format === "legacy"
          ? record.data.signedBid
          : {...record.data, orderMetadata: {version: 99, orderIds: [], doNotOfferBefore: 0}};
      fs.writeFileSync(
        filename,
        JSON.stringify({data, checksum: createHash("sha256").update(JSON.stringify(data)).digest("hex")})
      );
      const recovered = new CommitmentJournal(directory, identity);
      expect(recovered.queryExposure(otherId, Date.now()).coverageComplete).toBe(false);
      const payloads = new PayloadStore();
      const bids = new BidStore();
      expect(recovered.recover(100, payloads, bids, new BidLedger())).toBe(1);
      expect(bids.has(f.bid)).toBe(true);
      const restored = payloads.get(f.stored.blockHash);
      if (!restored) throw Error("Missing recovered payload");
      expect(ssz.gloas.ExecutionPayload.serialize(restored.payload.executionPayload)).toEqual(
        ssz.gloas.ExecutionPayload.serialize(f.stored.payload.executionPayload)
      );
    }
  );

  it("preserves a positive membership with unknown exposure time without inventing evidence", async () => {
    const primary = setup();
    const f = fixture();
    primary.journal.retain(f.bid, f.stored);
    const bidName = fs.readdirSync(primary.directory).find((name) => name.startsWith("bid-"));
    if (!bidName) throw Error("Missing committed bid");
    const filename = path.join(primary.directory, bidName);
    const record = JSON.parse(fs.readFileSync(filename, "utf8"));
    delete record.data.possibleExposureAtUnixMs;
    fs.writeFileSync(
      filename,
      JSON.stringify({
        data: record.data,
        checksum: createHash("sha256").update(JSON.stringify(record.data)).digest("hex"),
      })
    );
    const restored = new CommitmentJournal(primary.directory, identity);
    const thread = new Thread(restored);
    const worker = new AsyncCommitmentJournal(thread);
    thread.emit("message", {id: 0});
    const exposure = new OrderExposure([worker]);
    const result = exposure.query(request(exposure));
    thread.finish();
    expect(await result).toMatchObject({
      priorCommitmentExposure: "possible",
      commitmentCount: 1,
      commitments: [],
      earliestPossibleOfferAtUnixMs: null,
      coverageComplete: false,
      truncated: true,
    });
    await worker.close();
  });
});
