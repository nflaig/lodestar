import {afterEach, beforeEach, describe, expect, it, vi} from "vitest";
import {SecretKey} from "@chainsafe/lodestar-z/blst";
import {createBeaconConfig} from "@lodestar/config";
import {getConfig} from "@lodestar/config/test-utils";
import {ForkName} from "@lodestar/params";
import {gloas, ssz} from "@lodestar/types";
import {defer, fromHex, toRootHex} from "@lodestar/utils";
import type {
  AlternatePayloadAttributesEvent,
  BidderOptions,
  PayloadAttributesEvent,
} from "../../../src/services/bidder.js";
import {BuilderSigner} from "../../../src/services/builderSigner.js";
import type {BuildRequest, BuiltPayload, PayloadSourceResources} from "../../../src/services/payloadSource.js";
import {PayloadStore} from "../../../src/services/payloadStore.js";
import {ProposerPreferencesTracker} from "../../../src/services/proposerPreferencesTracker.js";
import {getApiClientStub, mockApiResponse} from "../utils/apiStub.js";
import {createBiddingModules, getPayloadSourceStub} from "../utils/bidding.js";
import {ClockMock} from "../utils/clock.js";
import {getMockedLogger} from "../utils/logger.js";
import {mockBuiltPayload} from "../utils/payload.js";

const dependentRoot = Buffer.alloc(32, 7);
const key = SecretKey.fromBytes(Buffer.alloc(32, 1));
const pubkey = key.toPublicKey().toBytes();

function attributes(root: number, hash: number): AlternatePayloadAttributesEvent {
  const data = ssz.gloas.SSEPayloadAttributes.defaultValue();
  data.proposalSlot = 1;
  data.proposerIndex = 3;
  data.parentBlockRoot = Buffer.alloc(32, root);
  data.parentBlockHash = Buffer.alloc(32, hash);
  data.payloadAttributes.parentBeaconBlockRoot = data.parentBlockRoot;
  data.payloadAttributes.slotNumber = 1;
  data.payloadAttributes.timestamp = 12;
  return {
    version: ForkName.gloas,
    data,
    context: {
      role: "alternate",
      dependentRoot,
      builders: [{index: 1, pubkey, version: 0, active: true, availableGwei: 9_000_000_000}],
    },
  };
}

describe("alternate parent bidder", () => {
  let controller: AbortController;
  let pending: Promise<void>[];

  beforeEach(() => {
    vi.useFakeTimers({now: 0});
    controller = new AbortController();
    pending = [];
  });

  afterEach(async () => {
    controller.abort();
    await vi.advanceTimersByTimeAsync(0);
    await Promise.all(pending);
    vi.useRealTimers();
  });

  function setup(options: Partial<BidderOptions> = {}) {
    const api = getApiClientStub();
    api.validator.getProposerDutiesV2.mockResolvedValue(
      mockApiResponse({
        data: [{slot: 1, validatorIndex: 3, pubkey: Buffer.alloc(48, 3)}],
        meta: {executionOptimistic: false, dependentRoot: toRootHex(dependentRoot)},
      })
    );
    api.beacon.publishExecutionPayloadBid.mockResolvedValue(mockApiResponse({}));
    const config = getConfig(ForkName.gloas);
    const clock = new ClockMock();
    clock.msToSlot = (slot) => slot * 12_000 - Date.now();
    clock.getCurrentSlot = () => Math.floor(Date.now() / 12_000);
    const payloadSource = getPayloadSourceStub();
    payloadSource.supportsUpdates = true;
    payloadSource.supportsBoundedAlternates = true;
    const sessions = new Map<string, {request: BuildRequest; payload: BuiltPayload; signal: AbortSignal}>();
    let nextId = 0;
    payloadSource.getResources.mockImplementation(() => ({
      capacity: 2,
      active: sessions.size,
      preparing: 0,
      releasing: 0,
      uncertain: 0,
      alternatesDisabled: false,
    }));
    payloadSource.prepare.mockImplementation(async (request, signal) => {
      if (sessions.size >= 2) throw Error("SessionLimit");
      const payloadId = String(++nextId);
      const payload: BuiltPayload = {
        ...mockBuiltPayload({
          slot: request.payloadAttributes.slotNumber,
          parentHash: fromHex(request.forkchoiceState.headBlockHash),
          prevRandao: request.payloadAttributes.prevRandao,
          blockHash: Buffer.alloc(32, nextId),
        }),
        fork: ForkName.gloas,
      };
      sessions.set(payloadId, {request, payload, signal});
      return {sourceId: "el", fork: ForkName.gloas, payloadId};
    });
    payloadSource.getPayload.mockImplementation(async (handle) => {
      const session = sessions.get(handle.payloadId);
      if (!session) throw Error("UnknownSession");
      return session.payload;
    });
    payloadSource.release.mockImplementation(async (handle) => {
      sessions.delete(handle.payloadId);
    });
    const proposerPreferencesTracker = new ProposerPreferencesTracker();
    const preferences = ssz.gloas.SignedProposerPreferences.defaultValue();
    preferences.message.proposalSlot = 1;
    preferences.message.dependentRoot = dependentRoot;
    preferences.message.targetGasLimit = 60_000_000n;
    proposerPreferencesTracker.onProposerPreferences(preferences);
    const payloadStore = new PayloadStore();
    const canRetainAlternate = vi.fn(() => true);
    const retainCommitment = vi
      .fn<(bid: gloas.SignedExecutionPayloadBid) => Promise<void>>()
      .mockResolvedValue(undefined);
    const logger = getMockedLogger();
    const modules = createBiddingModules({
      api,
      config,
      clock,
      payloadSource,
      payloadStore,
      proposerPreferencesTracker,
      logger,
      signal: controller.signal,
      builderSigner: new BuilderSigner(createBeaconConfig(config, Buffer.alloc(32)), {
        secretKey: key,
        publicKey: key.toPublicKey(),
      }),
      canRetainAlternate,
      retainCommitment,
      updateOptions: {updateIntervalMs: 100, alternateBranches: true, ...options},
    });
    const start = (event: PayloadAttributesEvent) => {
      pending.push(modules.bidder.onPayloadAttributes({version: event.version, data: event.data}));
    };
    const offer = (event: AlternatePayloadAttributesEvent) =>
      modules.bidStore.get(
        event.data.proposalSlot,
        toRootHex(event.data.parentBlockHash),
        toRootHex(event.data.parentBlockRoot)
      );
    return {
      ...modules,
      api,
      payloadSource,
      payloadStore,
      sessions,
      start,
      offer,
      canRetainAlternate,
      retainCommitment,
      logger,
      proposerPreferencesTracker,
    };
  }

  it("retains four head/parent FULL/EMPTY tuples while the primary keeps improving and alone gossips", async () => {
    const m = setup();
    const contexts = [attributes(4, 5), attributes(4, 6), attributes(3, 6), attributes(3, 7)];
    m.start(contexts[0]);
    for (const context of contexts.slice(1)) m.bidder.onAlternatePayloadAttributes(context);
    await vi.advanceTimersByTimeAsync(1500);
    for (const context of contexts) expect(m.offer(context), toRootHex(context.data.parentBlockRoot)).not.toBeNull();
    expect(m.payloadSource.prepare).toHaveBeenCalledTimes(4);
    expect(m.sessions.size).toBe(1);
    expect(m.payloadSource.release.mock.calls.map(([handle]) => handle.payloadId)).not.toContain("1");
    expect(m.ledger.getRevisionCount(1)).toBe(4);
    expect(m.api.beacon.publishExecutionPayloadBid).not.toHaveBeenCalled();
    for (const context of contexts.slice(1)) m.bidder.onAlternatePayloadAttributes(context);
    const primary = m.sessions.get("1");
    if (!primary) throw Error("Missing primary");
    primary.payload = {
      ...primary.payload,
      executionPayloadValue: 2_000_000_000_000_000_000n,
      executionPayload: {...primary.payload.executionPayload, blockHash: Buffer.alloc(32, 19)},
    };
    await vi.advanceTimersByTimeAsync(8700);
    expect(m.offer(contexts[0])?.message.value).toBe(1_800_000_000);
    expect(m.api.beacon.publishExecutionPayloadBid).toHaveBeenCalledOnce();
    const bid = m.api.beacon.publishExecutionPayloadBid.mock.calls[0][0].signedExecutionPayloadBid;
    expect(bid.message.parentBlockRoot).toEqual(contexts[0].data.parentBlockRoot);
    for (const context of contexts) {
      const offered = m.offer(context);
      if (!offered) throw Error("Missing retained branch");
      expect(m.payloadStore.has(toRootHex(offered.message.blockHash))).toBe(true);
    }
    expect(m.payloadSource.prepare).toHaveBeenCalledTimes(4);
  });

  it.each([false, true])(
    "keeps the primary alive across standard-topic alternate/repeated-primary interleaving (enabled=%s)",
    async (alternateBranches) => {
      const m = setup({alternateBranches});
      const primary = attributes(4, 5);
      const alternate = attributes(4, 6);
      m.start(primary);
      await vi.advanceTimersByTimeAsync(100);
      const primarySession = m.sessions.get("1");
      expect(primarySession).toBeDefined();
      for (let i = 0; i < 3; i++) {
        await m.bidder.onPayloadAttributes(alternate);
        m.start(primary);
        await vi.advanceTimersByTimeAsync(200);
      }
      expect(m.sessions.get("1")).toBe(primarySession);
      expect(m.payloadSource.release.mock.calls.map(([handle]) => handle.payloadId)).not.toContain("1");
      expect(m.payloadSource.prepare).toHaveBeenCalledTimes(alternateBranches ? 2 : 1);
      expect(m.offer(primary)).not.toBeNull();
      expect(m.offer(alternate) !== null).toBe(alternateBranches);
      expect(m.logger.error).not.toHaveBeenCalled();
    }
  );

  it("waits for an unleased primary snapshot before allocating an alternate", async () => {
    const m = setup();
    const ready = defer<BuiltPayload>();
    m.payloadSource.getPayload.mockImplementationOnce(() => ready.promise);
    m.start(attributes(4, 5));
    m.bidder.onAlternatePayloadAttributes(attributes(4, 6));
    await vi.advanceTimersByTimeAsync(200);
    expect(m.payloadSource.prepare).toHaveBeenCalledTimes(1);
    const primary = m.sessions.get("1");
    if (!primary) throw Error("Missing primary");
    ready.resolve(primary.payload);
    await vi.advanceTimersByTimeAsync(500);
    expect(m.offer(attributes(4, 6))).not.toBeNull();
  });

  it("does not use leased primary snapshots to enable alternates", async () => {
    const m = setup();
    m.payloadSource.getPayload.mockImplementation(async (handle) => {
      const session = m.sessions.get(handle.payloadId);
      if (!session) throw Error("Missing session");
      const {payload} = session;
      return {
        ...payload,
        offerEligibility: {
          generation: "0x0",
          lease: {
            generation: "0x0",
            ready: true,
            validUntil: performance.now() + 1000,
            validUntilUnixMs: Date.now() + 1000,
          },
        },
      };
    });
    m.start(attributes(4, 5));
    m.bidder.onAlternatePayloadAttributes(attributes(4, 6));
    await vi.advanceTimersByTimeAsync(1000);
    expect(m.payloadSource.prepare).toHaveBeenCalledTimes(1);
  });

  it.each(["preparing", "releasing", "uncertain"] as const)("does not allocate over a %s reservation", async (kind) => {
    const m = setup();
    m.payloadSource.getResources.mockReturnValue({
      capacity: 2,
      active: 1,
      preparing: 0,
      releasing: 0,
      uncertain: 0,
      alternatesDisabled: false,
      [kind]: 1,
    } as PayloadSourceResources);
    m.start(attributes(4, 5));
    m.bidder.onAlternatePayloadAttributes(attributes(4, 6));
    await vi.advanceTimersByTimeAsync(1000);
    expect(m.payloadSource.prepare).toHaveBeenCalledTimes(1);
  });

  it("waits around gossip and does not allocate new work too late to finish before the slot", async () => {
    const m = setup();
    m.start(attributes(4, 5));
    await vi.advanceTimersByTimeAsync(10_000);
    m.bidder.onAlternatePayloadAttributes(attributes(4, 6));
    expect(m.payloadSource.prepare).toHaveBeenCalledTimes(1);
    await vi.advanceTimersByTimeAsync(1600);
    expect(m.offer(attributes(4, 6))).not.toBeNull();
    m.bidder.onAlternatePayloadAttributes(attributes(3, 7));
    expect(m.payloadSource.prepare).toHaveBeenCalledTimes(2);
  });

  it("prioritizes the new primary and retries its unpublished alternate only after confirmed cleanup", async () => {
    const m = setup();
    const old = attributes(4, 5);
    const ended = defer<void>();
    m.payloadSource.release.mockImplementation(async (handle) => {
      if (handle.payloadId === "2") await ended.promise;
      m.sessions.delete(handle.payloadId);
    });
    m.start(old);
    m.bidder.onAlternatePayloadAttributes(attributes(4, 6));
    await vi.advanceTimersByTimeAsync(100);
    const alternate = m.sessions.get("2");
    expect(alternate).toBeDefined();
    m.start(attributes(8, 9));
    await vi.advanceTimersByTimeAsync(100);
    expect(m.payloadSource.release.mock.calls.map(([handle]) => handle.payloadId)).toContain("2");
    expect(m.offer(attributes(4, 6))).toBeNull();
    expect(m.offer(attributes(8, 9))).not.toBeNull();
    expect(m.offer(old)).not.toBeNull();
    expect(m.payloadSource.prepare).toHaveBeenCalledTimes(3);
    expect(m.sessions.size).toBe(2);
    ended.resolve();
    await vi.advanceTimersByTimeAsync(500);
    expect(m.offer(attributes(4, 6))).not.toBeNull();
    expect(m.payloadSource.prepare.mock.calls.map(([request]) => request.requireUnleased === true)).toEqual([
      false,
      true,
      false,
      true,
    ]);
    expect(m.sessions.size).toBe(1);
  });

  it("allows at most one preemption retry per tuple across repeated primary switches", async () => {
    const m = setup();
    const alternate = attributes(4, 6);
    m.start(attributes(4, 5));
    m.bidder.onAlternatePayloadAttributes(alternate);
    await vi.advanceTimersByTimeAsync(100);
    m.start(attributes(8, 9));
    await vi.advanceTimersByTimeAsync(100);
    expect(m.payloadSource.prepare.mock.calls.filter(([request]) => request.requireUnleased)).toHaveLength(2);
    m.start(attributes(10, 11));
    await vi.advanceTimersByTimeAsync(600);
    m.bidder.onAlternatePayloadAttributes(alternate);
    await vi.advanceTimersByTimeAsync(600);
    expect(m.payloadSource.prepare.mock.calls.filter(([request]) => request.requireUnleased)).toHaveLength(2);
    expect(m.offer(alternate)).toBeNull();
    expect(m.offer(attributes(10, 11))).not.toBeNull();
    expect(m.sessions.size).toBe(1);
  });

  it.each(["slot", "proposer", "dependent root"] as const)(
    "does not retry a preempted alternate when the new primary changes %s",
    async (changed) => {
      const m = setup();
      const alternate = attributes(4, 6);
      m.start(attributes(4, 5));
      m.bidder.onAlternatePayloadAttributes(alternate);
      await vi.advanceTimersByTimeAsync(100);
      const next = attributes(8, 9);
      const root = changed === "dependent root" ? Buffer.alloc(32, 8) : dependentRoot;
      if (changed === "slot") {
        next.data.proposalSlot = 2;
        next.data.payloadAttributes.slotNumber = 2;
        next.data.payloadAttributes.timestamp = 24;
      }
      if (changed === "proposer") next.data.proposerIndex = 4;
      m.api.validator.getProposerDutiesV2.mockResolvedValue(
        mockApiResponse({
          data: [{slot: next.data.proposalSlot, validatorIndex: next.data.proposerIndex, pubkey: Buffer.alloc(48, 3)}],
          meta: {executionOptimistic: false, dependentRoot: toRootHex(root)},
        })
      );
      const preferences = ssz.gloas.SignedProposerPreferences.defaultValue();
      preferences.message.proposalSlot = next.data.proposalSlot;
      preferences.message.dependentRoot = root;
      preferences.message.targetGasLimit = 60_000_000n;
      m.proposerPreferencesTracker.onProposerPreferences(preferences);
      m.start(next);
      await vi.advanceTimersByTimeAsync(700);
      expect(m.offer(next)).not.toBeNull();
      expect(m.offer(alternate)).toBeNull();
      expect(m.payloadSource.prepare.mock.calls.filter(([request]) => request.requireUnleased)).toHaveLength(1);
    }
  );

  it.each(["shutdown", "slot expired"] as const)(
    "does not retry after %s while preempted cleanup is pending",
    async (reason) => {
      const m = setup();
      const ended = defer<void>();
      m.payloadSource.release.mockImplementation(async (handle) => {
        if (handle.payloadId === "2") await ended.promise;
        m.sessions.delete(handle.payloadId);
      });
      m.start(attributes(4, 5));
      m.bidder.onAlternatePayloadAttributes(attributes(4, 6));
      await vi.advanceTimersByTimeAsync(100);
      m.start(attributes(8, 9));
      await vi.advanceTimersByTimeAsync(100);
      if (reason === "shutdown") controller.abort();
      else vi.setSystemTime(12_000);
      ended.resolve();
      await vi.advanceTimersByTimeAsync(100);
      expect(m.payloadSource.prepare.mock.calls.filter(([request]) => request.requireUnleased)).toHaveLength(1);
      expect(m.offer(attributes(4, 6))).toBeNull();
    }
  );

  it("does not turn a normal alternate deadline failure into a primary-preemption retry", async () => {
    const m = setup();
    const blocked = defer<BuiltPayload>();
    m.payloadSource.getPayload.mockImplementation(async (handle) => {
      if (handle.payloadId === "2") return blocked.promise;
      const session = m.sessions.get(handle.payloadId);
      if (!session) throw Error("Missing session");
      return session.payload;
    });
    m.start(attributes(4, 5));
    m.bidder.onAlternatePayloadAttributes(attributes(4, 6));
    await vi.advanceTimersByTimeAsync(550);
    m.start(attributes(8, 9));
    await vi.advanceTimersByTimeAsync(600);
    m.bidder.onAlternatePayloadAttributes(attributes(4, 6));
    await vi.advanceTimersByTimeAsync(100);
    expect(m.payloadSource.prepare.mock.calls.filter(([request]) => request.requireUnleased)).toHaveLength(1);
    expect(m.offer(attributes(4, 6))).toBeNull();
    expect(m.offer(attributes(8, 9))).not.toBeNull();
    expect(m.logger.debug).toHaveBeenCalledWith("Skipping alternate parent bid", {
      slot: 1,
      parentBlockRoot: toRootHex(attributes(4, 6).data.parentBlockRoot),
      parentBlockHash: toRootHex(attributes(4, 6).data.parentBlockHash),
      reason: "deadline_before_retention",
    });
  });

  it("keeps a preemption retry queued while alternate cleanup remains uncertain", async () => {
    const m = setup();
    let uncertain = 0;
    m.payloadSource.getResources.mockImplementation(() => ({
      capacity: 2,
      active: m.sessions.size,
      preparing: 0,
      releasing: 0,
      uncertain,
      alternatesDisabled: false,
    }));
    m.payloadSource.release.mockImplementation(async (handle) => {
      m.sessions.delete(handle.payloadId);
      if (handle.payloadId === "2") {
        uncertain = 1;
        throw Error("UncertainEnd");
      }
    });
    m.start(attributes(4, 5));
    m.bidder.onAlternatePayloadAttributes(attributes(4, 6));
    await vi.advanceTimersByTimeAsync(100);
    m.start(attributes(8, 9));
    await vi.advanceTimersByTimeAsync(600);
    expect(m.offer(attributes(8, 9))).not.toBeNull();
    expect(m.payloadSource.prepare.mock.calls.filter(([request]) => request.requireUnleased)).toHaveLength(1);
    expect(m.offer(attributes(4, 6))).toBeNull();
    uncertain = 0;
    await vi.advanceTimersByTimeAsync(550);
    expect(m.payloadSource.prepare.mock.calls.filter(([request]) => request.requireUnleased)).toHaveLength(2);
    expect(m.offer(attributes(4, 6))).not.toBeNull();
  });

  it("does not retry a failed retrieval when a primary switch arrives during cleanup", async () => {
    const m = setup();
    const ended = defer<void>();
    m.payloadSource.release.mockImplementation(async (handle) => {
      if (handle.payloadId === "2") await ended.promise;
      m.sessions.delete(handle.payloadId);
    });
    m.payloadSource.getPayload.mockImplementation(async (handle) => {
      if (handle.payloadId === "2") throw Error("WorkerRejected");
      const session = m.sessions.get(handle.payloadId);
      if (!session) throw Error("Missing session");
      return session.payload;
    });
    m.start(attributes(4, 5));
    m.bidder.onAlternatePayloadAttributes(attributes(4, 6));
    await vi.advanceTimersByTimeAsync(460);
    m.start(attributes(8, 9));
    await vi.advanceTimersByTimeAsync(10);
    ended.resolve();
    await vi.advanceTimersByTimeAsync(600);
    expect(m.payloadSource.prepare.mock.calls.filter(([request]) => request.requireUnleased)).toHaveLength(1);
    expect(m.offer(attributes(4, 6))).toBeNull();
  });

  it("counts a retry against the four actual alternate attempts per slot", async () => {
    const m = setup();
    const alternate = attributes(4, 6);
    m.start(attributes(4, 5));
    m.bidder.onAlternatePayloadAttributes(alternate);
    await vi.advanceTimersByTimeAsync(100);
    m.start(attributes(8, 9));
    await vi.advanceTimersByTimeAsync(600);
    expect(m.offer(alternate)).not.toBeNull();
    for (let i = 10; i < 13; i++) m.bidder.onAlternatePayloadAttributes(attributes(i, i));
    await vi.advanceTimersByTimeAsync(1600);
    expect(m.payloadSource.prepare.mock.calls.filter(([request]) => request.requireUnleased)).toHaveLength(4);
    expect(m.offer(attributes(10, 10))).not.toBeNull();
    expect(m.offer(attributes(11, 11))).not.toBeNull();
    expect(m.offer(attributes(12, 12))).toBeNull();
  });

  it("does not retry a tuple retained while it becomes the primary", async () => {
    const m = setup();
    const alternate = attributes(4, 6);
    m.start(attributes(4, 5));
    m.bidder.onAlternatePayloadAttributes(alternate);
    await vi.advanceTimersByTimeAsync(100);
    m.start(alternate);
    await vi.advanceTimersByTimeAsync(600);
    const retained = m.offer(alternate);
    expect(retained).not.toBeNull();
    m.start(attributes(8, 9));
    m.bidder.onAlternatePayloadAttributes(alternate);
    await vi.advanceTimersByTimeAsync(600);
    expect(m.payloadSource.prepare.mock.calls.filter(([request]) => request.requireUnleased)).toHaveLength(1);
    expect(m.offer(alternate)).toBe(retained);
    expect(m.logger.debug).not.toHaveBeenCalledWith("Retrying preempted alternate parent bid", expect.anything());
  });

  it("declines bids above branch-specific collateral and omits unknown identities", async () => {
    const m = setup();
    m.start(attributes(4, 5));
    const alternate = attributes(4, 6);
    alternate.context.builders[0].availableGwei = 100_000_000;
    m.bidder.onAlternatePayloadAttributes(alternate);
    const unknown = attributes(3, 7);
    unknown.context.builders[0].pubkey = Buffer.alloc(48, 2);
    m.bidder.onAlternatePayloadAttributes(unknown);
    await vi.advanceTimersByTimeAsync(1100);
    expect(m.offer(alternate)).toBeNull();
    expect(m.offer(unknown)).toBeNull();
    expect(m.payloadSource.prepare).toHaveBeenCalledTimes(2);
  });

  it("reserves half the revision budget for primary updates", async () => {
    const m = setup({maxRevisionsPerSlot: 4});
    m.start(attributes(4, 5));
    m.bidder.onAlternatePayloadAttributes(attributes(4, 6));
    m.bidder.onAlternatePayloadAttributes(attributes(3, 7));
    await vi.advanceTimersByTimeAsync(1100);
    expect(m.offer(attributes(4, 6))).not.toBeNull();
    expect(m.offer(attributes(3, 7))).toBeNull();
    const primary = m.sessions.get("1");
    if (!primary) throw Error("Missing primary");
    primary.payload = {
      ...primary.payload,
      executionPayloadValue: 2_000_000_000_000_000_000n,
      executionPayload: {...primary.payload.executionPayload, blockHash: Buffer.alloc(32, 20)},
    };
    await vi.advanceTimersByTimeAsync(100);
    expect(m.offer(attributes(4, 5))?.message.value).toBe(1_800_000_000);
    expect(m.ledger.getRevisionCount(1)).toBe(3);
  });

  it.each(["timeout", "new primary", "wall deadline"])(
    "retains a late durable alternate for reveal without offering after %s",
    async (reason) => {
      const m = setup();
      const disk = defer<void>();
      const alternate = attributes(4, 6);
      const retained: gloas.SignedExecutionPayloadBid[] = [];
      m.retainCommitment.mockImplementation(async (bid) => {
        if (toRootHex(bid.message.parentBlockHash) === toRootHex(alternate.data.parentBlockHash)) {
          retained.push(bid);
          await disk.promise;
        }
      });
      const observed: unknown[] = [];
      m.bidStore.subscribe(
        1,
        toRootHex(alternate.data.parentBlockHash),
        toRootHex(alternate.data.parentBlockRoot),
        () => {
          const bid = m.offer(alternate);
          if (bid) observed.push(bid);
        }
      );
      m.start(attributes(4, 5));
      m.bidder.onAlternatePayloadAttributes(alternate);
      await vi.advanceTimersByTimeAsync(450);
      expect(retained).toHaveLength(1);
      if (reason === "timeout") await vi.advanceTimersByTimeAsync(100);
      else if (reason === "new primary") {
        m.start(attributes(8, 9));
        await vi.advanceTimersByTimeAsync(0);
      } else vi.setSystemTime(600);
      disk.resolve();
      await vi.advanceTimersByTimeAsync(100);
      expect(m.bidStore.has(retained[0])).toBe(true);
      expect(m.payloadStore.has(toRootHex(retained[0].message.blockHash))).toBe(true);
      expect(m.bidStore.canOffer(retained[0])).toBe(false);
      expect(m.offer(alternate)).toBeNull();
      expect(observed).toEqual([]);
      await vi.advanceTimersByTimeAsync(600);
      expect(m.payloadSource.prepare.mock.calls.filter(([request]) => request.requireUnleased)).toHaveLength(1);
      expect(m.logger.debug).toHaveBeenCalledWith("Skipping alternate parent bid", {
        slot: 1,
        parentBlockRoot: toRootHex(alternate.data.parentBlockRoot),
        parentBlockHash: toRootHex(alternate.data.parentBlockHash),
        reason: reason === "new primary" ? "aborted_after_retention" : "deadline_after_retention",
      });
    }
  );

  it.each(["journal busy", "wall deadline"] as const)(
    "diagnoses an alternate skipped before persistence because of %s without retrying it",
    async (reason) => {
      const m = setup();
      const alternate = attributes(4, 6);
      m.payloadSource.getPayload.mockImplementation(async (handle) => {
        const session = m.sessions.get(handle.payloadId);
        if (!session) throw Error("Missing session");
        if (handle.payloadId === "2") {
          if (reason === "journal busy") m.canRetainAlternate.mockReturnValue(false);
          else vi.setSystemTime(600);
        }
        return session.payload;
      });
      m.start(attributes(4, 5));
      m.bidder.onAlternatePayloadAttributes(alternate);
      await vi.advanceTimersByTimeAsync(500);
      expect(m.offer(alternate)).toBeNull();
      expect(m.logger.debug).toHaveBeenCalledWith("Skipping alternate parent bid", {
        slot: 1,
        parentBlockRoot: toRootHex(alternate.data.parentBlockRoot),
        parentBlockHash: toRootHex(alternate.data.parentBlockHash),
        reason: reason === "journal busy" ? "journal_busy" : "deadline_before_retention",
      });
      m.canRetainAlternate.mockReturnValue(true);
      m.start(attributes(8, 9));
      m.bidder.onAlternatePayloadAttributes(alternate);
      await vi.advanceTimersByTimeAsync(600);
      expect(m.payloadSource.prepare.mock.calls.filter(([request]) => request.requireUnleased)).toHaveLength(1);
    }
  );

  it("refuses a primary whose proposer changed while duties were fetched", async () => {
    const m = setup();
    m.api.validator.getProposerDutiesV2.mockResolvedValue(
      mockApiResponse({
        data: [{slot: 1, validatorIndex: 4, pubkey: Buffer.alloc(48, 4)}],
        meta: {executionOptimistic: false, dependentRoot: toRootHex(dependentRoot)},
      })
    );
    m.start(attributes(4, 5));
    m.bidder.onAlternatePayloadAttributes(attributes(4, 6));
    await vi.advanceTimersByTimeAsync(1000);
    expect(m.payloadSource.prepare).not.toHaveBeenCalled();
  });

  it("waits for confirmed alternate cleanup before starting the next snapshot", async () => {
    const m = setup();
    const ended = defer<void>();
    m.payloadSource.release.mockImplementation(async (handle) => {
      if (handle.payloadId === "2") await ended.promise;
      m.sessions.delete(handle.payloadId);
    });
    m.start(attributes(4, 5));
    m.bidder.onAlternatePayloadAttributes(attributes(4, 6));
    m.bidder.onAlternatePayloadAttributes(attributes(3, 7));
    await vi.advanceTimersByTimeAsync(750);
    expect(m.offer(attributes(4, 6))).not.toBeNull();
    expect(m.payloadSource.prepare).toHaveBeenCalledTimes(2);
    expect(m.sessions.size).toBe(2);
    ended.resolve();
    await vi.advanceTimersByTimeAsync(500);
    expect(m.offer(attributes(3, 7))).not.toBeNull();
    expect(m.sessions.size).toBe(1);
  });

  it("resumes queued work when the journal becomes idle even without a new primary revision", async () => {
    const m = setup();
    m.canRetainAlternate.mockReturnValue(false);
    m.start(attributes(4, 5));
    m.bidder.onAlternatePayloadAttributes(attributes(4, 6));
    await vi.advanceTimersByTimeAsync(200);
    expect(m.payloadSource.prepare).toHaveBeenCalledTimes(1);
    m.canRetainAlternate.mockReturnValue(true);
    await vi.advanceTimersByTimeAsync(550);
    expect(m.offer(attributes(4, 6))).not.toBeNull();
    expect(m.payloadSource.prepare).toHaveBeenCalledTimes(2);
  });

  it("preserves queued same-slot contexts through a primary switch without resetting the attempt cap", async () => {
    const m = setup();
    m.start(attributes(4, 5));
    for (let i = 10; i < 14; i++) m.bidder.onAlternatePayloadAttributes(attributes(i, i));
    await vi.advanceTimersByTimeAsync(100);
    m.start(attributes(8, 9));
    await vi.advanceTimersByTimeAsync(1700);
    expect(m.offer(attributes(10, 10))).toBeNull();
    for (let i = 11; i < 14; i++) expect(m.offer(attributes(i, i)), `retained context ${i}`).not.toBeNull();
    expect(m.offer(attributes(8, 9))).not.toBeNull();
    const before = m.payloadSource.prepare.mock.calls.length;
    m.bidder.onAlternatePayloadAttributes(attributes(14, 14));
    await vi.advanceTimersByTimeAsync(600);
    expect(m.payloadSource.prepare).toHaveBeenCalledTimes(before);
    expect(m.offer(attributes(14, 14))).toBeNull();
  });
});
