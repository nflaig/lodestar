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
import {BidLedger} from "../../../src/services/bidLedger.js";
import {BidPublisher} from "../../../src/services/bidPublisher.js";
import {BidStore} from "../../../src/services/bidStore.js";
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

function hasReason(context: unknown, reason: string): boolean {
  return typeof context === "object" && context !== null && "reason" in context && context.reason === reason;
}

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

  function setup(options: Partial<BidderOptions> = {}, stageDeadlines: number[] = []) {
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
    const getBuilderStatus = vi.fn(() => ({status: "active" as const, balance: 10_000_000_000}));
    const signer = new BuilderSigner(createBeaconConfig(config, Buffer.alloc(32)), {
      secretKey: key,
      publicKey: key.toPublicKey(),
    });
    const gossipStages = stageDeadlines.map((deadlineBps, i) => {
      const ledger = new BidLedger();
      return {
        deadlineBps,
        builderIndex: i + 2,
        ledger,
        bidStore: new BidStore(),
        bidPublisher: new BidPublisher({
          api,
          config,
          signer,
          ledger,
          builderIndex: i + 2,
          hasPayload: ({blockHash}) => payloadStore.has(blockHash),
          retainCommitment,
        }),
        getBuilderStatus,
      };
    });
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
      getBuilderStatus,
      gossipStages,
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
      clock,
      payloadSource,
      payloadStore,
      sessions,
      start,
      offer,
      canRetainAlternate,
      retainCommitment,
      logger,
      proposerPreferencesTracker,
      getBuilderStatus,
    };
  }

  describe("one refresh before the slot", () => {
    const alternateRequests = (m: ReturnType<typeof setup>) =>
      m.payloadSource.prepare.mock.calls.filter(([request]) => request.requireUnleased);

    it.each([-1, 499, 5001, 750.5, NaN, Infinity])("rejects refresh offset %s", (value) => {
      expect(() => setup({alternateRefreshBeforeSlotMs: value})).toThrow("alternateRefreshBeforeSlotMs");
    });

    it("requires alternate branches", () => {
      expect(() => setup({alternateBranches: false, alternateRefreshBeforeSlotMs: 750})).toThrow(
        "ALTERNATE_REFRESH_REQUIRES_ALTERNATE_BRANCHES"
      );
    });

    it.each([0, 750])("refreshes an old primary only when enabled (%s ms)", async (lead) => {
      const m = setup({alternateRefreshBeforeSlotMs: lead, deadlineBps: 9250});
      const old = attributes(33, 44);
      m.start(old);
      await vi.advanceTimersByTimeAsync(2000);
      const previous = m.offer(old);
      if (!previous) throw Error("Missing old primary offer");
      m.start(attributes(4, 5));
      await vi.advanceTimersByTimeAsync(350);
      m.start(attributes(4, 6));
      m.bidder.onAlternatePayloadAttributes(old);
      await vi.advanceTimersByTimeAsync(8900);
      expect(alternateRequests(m)).toHaveLength(lead ? 1 : 0);
      for (const session of m.sessions.values()) {
        if (session.request.requireUnleased)
          session.payload = {...session.payload, executionPayloadValue: 2_000_000_000_000_000_000n};
      }
      await vi.advanceTimersByTimeAsync(350);
      expect(m.offer(old)?.message.value).toBe(lead ? 1_800_000_000 : 900_000_000);
      expect(m.payloadStore.has(toRootHex(previous.message.blockHash))).toBe(true);
      const current = m.offer(old);
      if (!current) throw Error("Missing current offer");
      expect(m.payloadStore.has(toRootHex(current.message.blockHash))).toBe(true);
      expect(m.api.beacon.publishExecutionPayloadBid).toHaveBeenCalledOnce();
      expect(
        toRootHex(
          m.api.beacon.publishExecutionPayloadBid.mock.calls[0][0].signedExecutionPayloadBid.message.parentBlockHash
        )
      ).toBe(toRootHex(attributes(4, 6).data.parentBlockHash));
      expect(m.sessions.size).toBe(1);
    });

    it("wakes without a new primary revision and fits the live Plataberget gossip schedule", async () => {
      const m = setup({alternateRefreshBeforeSlotMs: 750, deadlineBps: 9250}, [9850, 10000, 10400, 10800]);
      const alternate = attributes(3, 4);
      m.start(attributes(4, 5));
      m.bidder.onAlternatePayloadAttributes(alternate);
      await vi.advanceTimersByTimeAsync(11_249);
      expect(alternateRequests(m)).toHaveLength(1);
      await vi.advanceTimersByTimeAsync(1);
      expect(alternateRequests(m)).toHaveLength(2);
      expect(m.sessions.size).toBe(2);
      const refreshed = [...m.sessions.values()].find((session) => session.request.requireUnleased);
      if (!refreshed) throw Error("Missing refresh session");
      refreshed.payload = {...refreshed.payload, executionPayloadValue: 2_000_000_000_000_000_000n};
      await vi.advanceTimersByTimeAsync(350);
      expect(m.offer(alternate)?.message.value).toBe(1_800_000_000);
      expect(Date.now()).toBe(11_600);
      await vi.advanceTimersByTimeAsync(400);
      m.bidder.onAlternatePayloadAttributes(alternate);
      expect(alternateRequests(m)).toHaveLength(2);
      expect(m.api.beacon.publishExecutionPayloadBid).toHaveBeenCalledOnce();
      expect(m.sessions.size).toBe(1);
    });

    it.each([500, 1000])("skips or defers a refresh with insufficient gossip headroom (%s ms)", async (lead) => {
      const m = setup({alternateRefreshBeforeSlotMs: lead, deadlineBps: 9250}, [9850, 10000, 10400, 10800]);
      m.start(attributes(4, 5));
      m.bidder.onAlternatePayloadAttributes(attributes(3, 4));
      await vi.advanceTimersByTimeAsync(11_150);
      expect(alternateRequests(m)).toHaveLength(1);
      await vi.advanceTimersByTimeAsync(50);
      expect(alternateRequests(m)).toHaveLength(lead === 1000 ? 2 : 1);
      await vi.advanceTimersByTimeAsync(800);
      expect(alternateRequests(m)).toHaveLength(lead === 1000 ? 2 : 1);
    });

    it("lets a distinct branch pass a delayed refresh and caps total attempts", async () => {
      const m = setup({alternateRefreshBeforeSlotMs: 2000});
      m.start(attributes(4, 5));
      m.bidder.onAlternatePayloadAttributes(attributes(3, 4));
      await vi.advanceTimersByTimeAsync(600);
      for (let i = 10; i < 13; i++) m.bidder.onAlternatePayloadAttributes(attributes(i, i));
      await vi.advanceTimersByTimeAsync(1500);
      expect(alternateRequests(m)).toHaveLength(4);
      for (let i = 10; i < 13; i++) expect(m.offer(attributes(i, i))).not.toBeNull();
      await vi.advanceTimersByTimeAsync(9900);
      expect(alternateRequests(m)).toHaveLength(4);
    });

    it("starts a due distinct branch before a due retained refresh", async () => {
      const m = setup({alternateRefreshBeforeSlotMs: 2000, deadlineBps: 9500});
      const alternate = attributes(3, 4);
      m.start(attributes(4, 5));
      m.bidder.onAlternatePayloadAttributes(alternate);
      await vi.advanceTimersByTimeAsync(600);
      m.canRetainAlternate.mockReturnValue(false);
      await vi.advanceTimersByTimeAsync(9500);
      m.bidder.onAlternatePayloadAttributes(attributes(8, 9));
      m.canRetainAlternate.mockReturnValue(true);
      await vi.advanceTimersByTimeAsync(50);
      expect(alternateRequests(m).map(([r]) => r.forkchoiceState.headBlockHash)).toEqual([
        toRootHex(alternate.data.parentBlockHash),
        toRootHex(attributes(8, 9).data.parentBlockHash),
      ]);
    });

    it.each(["lower value", "equal value"] as const)(
      "keeps the prior offer without persistence for %s",
      async (change) => {
        const m = setup({alternateRefreshBeforeSlotMs: 750, deadlineBps: 9250});
        const alternate = attributes(3, 4);
        m.start(attributes(4, 5));
        m.bidder.onAlternatePayloadAttributes(alternate);
        await vi.advanceTimersByTimeAsync(600);
        const previous = m.offer(alternate);
        const writes = m.retainCommitment.mock.calls.length;
        const revisions = m.ledger.getRevisionCount(1);
        await vi.advanceTimersByTimeAsync(10_650);
        const refreshed = [...m.sessions.values()].find((s) => s.request.requireUnleased);
        if (!refreshed) throw Error("Missing refresh session");
        if (change === "lower value") refreshed.payload = {...refreshed.payload, executionPayloadValue: 1n};
        await vi.advanceTimersByTimeAsync(350);
        expect(m.offer(alternate)).toBe(previous);
        expect(m.retainCommitment).toHaveBeenCalledTimes(writes);
        expect(m.ledger.getRevisionCount(1)).toBe(revisions);
      }
    );

    it.each(["revision", "payload"] as const)("does not start a refresh without %s capacity", async (kind) => {
      const m = setup({alternateRefreshBeforeSlotMs: 750, deadlineBps: 9250, maxRevisionsPerSlot: 4});
      m.start(attributes(4, 5));
      m.bidder.onAlternatePayloadAttributes(attributes(3, 4));
      await vi.advanceTimersByTimeAsync(600);
      if (kind === "payload") {
        vi.spyOn(m.ledger, "getRevisionCount").mockReturnValue(1);
        vi.spyOn(m.payloadStore, "hasCapacityForAlternate").mockReturnValue(false);
      }
      await vi.advanceTimersByTimeAsync(11_000);
      expect(alternateRequests(m)).toHaveLength(1);
      expect(m.logger.debug.mock.calls.some(([, context]) => hasReason(context, "refresh_capacity"))).toBe(true);
    });

    it.each(["inactive", "dependent root", "missing builder"] as const)(
      "uses updated queued eligibility (%s)",
      async (change) => {
        const m = setup({alternateRefreshBeforeSlotMs: 750, deadlineBps: 9250});
        const alternate = attributes(3, 4);
        m.start(attributes(4, 5));
        m.bidder.onAlternatePayloadAttributes(alternate);
        await vi.advanceTimersByTimeAsync(600);
        const updated = structuredClone(alternate);
        if (change === "inactive") updated.context.builders[0].active = false;
        if (change === "dependent root") updated.context.dependentRoot = Buffer.alloc(32, 9);
        if (change === "missing builder") updated.context.builders = [];
        m.bidder.onAlternatePayloadAttributes(updated);
        await vi.advanceTimersByTimeAsync(11_000);
        expect(alternateRequests(m)).toHaveLength(1);
        expect(m.offer(alternate)?.message.value).toBe(900_000_000);
      }
    );

    it("rechecks fresh metadata delivered while the refresh is running", async () => {
      const m = setup({alternateRefreshBeforeSlotMs: 750, deadlineBps: 9250});
      const alternate = attributes(3, 4);
      m.start(attributes(4, 5));
      m.bidder.onAlternatePayloadAttributes(alternate);
      await vi.advanceTimersByTimeAsync(11_250);
      const updated = structuredClone(alternate);
      updated.context.builders[0].active = false;
      m.bidder.onAlternatePayloadAttributes(updated);
      await vi.advanceTimersByTimeAsync(350);
      expect(alternateRequests(m)).toHaveLength(2);
      expect(m.offer(alternate)?.message.value).toBe(900_000_000);
      expect(m.retainCommitment).toHaveBeenCalledTimes(2);
    });

    it("preempts a refresh for a new primary without retrying it", async () => {
      const m = setup({alternateRefreshBeforeSlotMs: 2000, deadlineBps: 9500});
      const alternate = attributes(3, 4);
      m.start(attributes(4, 5));
      m.bidder.onAlternatePayloadAttributes(alternate);
      await vi.advanceTimersByTimeAsync(10_100);
      expect(alternateRequests(m)).toHaveLength(2);
      m.start(attributes(8, 9));
      await vi.advanceTimersByTimeAsync(900);
      m.bidder.onAlternatePayloadAttributes(alternate);
      await vi.advanceTimersByTimeAsync(900);
      expect(alternateRequests(m)).toHaveLength(2);
      expect(m.offer(attributes(8, 9))).not.toBeNull();
    });

    it("preserves updated branch collateral when an initial attempt is retried", async () => {
      const m = setup({alternateRefreshBeforeSlotMs: 750});
      const alternate = attributes(33, 44);
      m.start(attributes(4, 5));
      m.bidder.onAlternatePayloadAttributes(alternate);
      await vi.advanceTimersByTimeAsync(200);
      const updated = structuredClone(alternate);
      updated.context.builders[0].availableGwei = 500_000_000;
      m.bidder.onAlternatePayloadAttributes(updated);
      m.start(attributes(4, 6));
      await vi.advanceTimersByTimeAsync(800);
      expect(alternateRequests(m)).toHaveLength(2);
      expect(m.offer(alternate)).toBeNull();
    });

    it("admits a distinct branch even when retained refreshes fill the queue", async () => {
      const m = setup({alternateRefreshBeforeSlotMs: 2000});
      const retained = Array.from({length: 4}, (_, i) => attributes(20 + i, 30 + i));
      for (const event of retained) {
        m.start(event);
        await vi.advanceTimersByTimeAsync(100);
      }
      m.start(attributes(40, 50));
      await vi.advanceTimersByTimeAsync(100);
      m.canRetainAlternate.mockReturnValue(false);
      for (const event of retained) m.bidder.onAlternatePayloadAttributes(event);
      const distinct = attributes(60, 70);
      m.bidder.onAlternatePayloadAttributes(distinct);
      m.canRetainAlternate.mockReturnValue(true);
      await vi.advanceTimersByTimeAsync(550);
      expect(alternateRequests(m)).toHaveLength(1);
      expect(alternateRequests(m)[0][0].forkchoiceState.headBlockHash).toBe(toRootHex(distinct.data.parentBlockHash));
      expect(m.offer(distinct)).not.toBeNull();
    });

    it("does not reuse a queued refresh when that tuple becomes primary again", async () => {
      const m = setup({alternateRefreshBeforeSlotMs: 750, deadlineBps: 9250});
      const alternate = attributes(33, 44);
      m.start(attributes(4, 5));
      m.bidder.onAlternatePayloadAttributes(alternate);
      await vi.advanceTimersByTimeAsync(600);
      m.start(alternate);
      await vi.advanceTimersByTimeAsync(11_000);
      expect(alternateRequests(m)).toHaveLength(1);
      expect(m.sessions.size).toBe(1);
    });

    it("waits for the superseded primary's cleanup even after its release times out", async () => {
      const m = setup({alternateRefreshBeforeSlotMs: 750, deadlineBps: 9250});
      const old = attributes(33, 44);
      const released = defer<void>();
      m.payloadSource.release.mockImplementation(async (handle) => {
        if (handle.payloadId === "1") await released.promise;
        m.sessions.delete(handle.payloadId);
      });
      m.start(old);
      await vi.advanceTimersByTimeAsync(2000);
      m.start(attributes(4, 5));
      m.bidder.onAlternatePayloadAttributes(old);
      await vi.advanceTimersByTimeAsync(9250);
      expect(alternateRequests(m)).toHaveLength(0);
      expect(m.sessions.size).toBe(2);
      released.resolve();
      await vi.advanceTimersByTimeAsync(400);
      expect(alternateRequests(m)).toHaveLength(1);
      expect(m.sessions.size).toBe(1);
    });

    it.each(["preferences", "branch balance"] as const)("rechecks %s at refresh retention", async (change) => {
      const m = setup({alternateRefreshBeforeSlotMs: 750, deadlineBps: 9250});
      const alternate = attributes(33, 44);
      m.start(attributes(4, 5));
      m.bidder.onAlternatePayloadAttributes(alternate);
      await vi.advanceTimersByTimeAsync(11_250);
      const session = [...m.sessions.values()].find((s) => s.request.requireUnleased);
      if (!session) throw Error("Missing refresh session");
      session.payload = {...session.payload, executionPayloadValue: 2_000_000_000_000_000_000n};
      if (change === "preferences") m.proposerPreferencesTracker.prune(2);
      else {
        const updated = structuredClone(alternate);
        updated.context.builders[0].availableGwei = 1_000_000_000;
        m.bidder.onAlternatePayloadAttributes(updated);
      }
      await vi.advanceTimersByTimeAsync(350);
      expect(m.offer(alternate)?.message.value).toBe(900_000_000);
      expect(m.retainCommitment).toHaveBeenCalledTimes(2);
    });

    it("retains a durable refresh for reveal only if branch eligibility changes during persistence", async () => {
      const m = setup({alternateRefreshBeforeSlotMs: 750, deadlineBps: 9250});
      const alternate = attributes(33, 44);
      m.start(attributes(4, 5));
      m.bidder.onAlternatePayloadAttributes(alternate);
      await vi.advanceTimersByTimeAsync(11_250);
      const session = [...m.sessions.values()].find((s) => s.request.requireUnleased);
      if (!session) throw Error("Missing refresh session");
      session.payload = {...session.payload, executionPayloadValue: 2_000_000_000_000_000_000n};
      const retained = defer<void>();
      m.retainCommitment.mockImplementation(() => retained.promise);
      await vi.advanceTimersByTimeAsync(350);
      const refreshed = m.retainCommitment.mock.calls.at(-1)?.[0];
      if (!refreshed) throw Error("Missing refresh commitment");
      const updated = structuredClone(alternate);
      updated.context.builders[0].active = false;
      m.bidder.onAlternatePayloadAttributes(updated);
      retained.resolve();
      await vi.advanceTimersByTimeAsync(20);
      expect(m.bidStore.has(refreshed)).toBe(true);
      expect(m.bidStore.canOffer(refreshed)).toBe(false);
      expect(m.payloadStore.has(toRootHex(refreshed.message.blockHash))).toBe(true);
      expect(m.offer(alternate)?.message.value).toBe(900_000_000);
      expect(alternateRequests(m)).toHaveLength(2);
    });

    it.each(["releasing", "uncertain"] as const)("waits for confirmed cleanup before refresh (%s)", async (kind) => {
      const m = setup({alternateRefreshBeforeSlotMs: 750, deadlineBps: 9250});
      m.start(attributes(4, 5));
      m.bidder.onAlternatePayloadAttributes(attributes(3, 4));
      await vi.advanceTimersByTimeAsync(600);
      m.payloadSource.getResources.mockReturnValue({
        capacity: 2,
        active: 1,
        preparing: 0,
        releasing: 0,
        uncertain: 0,
        alternatesDisabled: false,
        [kind]: 1,
      });
      await vi.advanceTimersByTimeAsync(11_000);
      expect(alternateRequests(m)).toHaveLength(1);
    });
  });

  describe("bounded journal recheck", () => {
    function startBusyAlternate(options: Partial<BidderOptions> = {}) {
      const m = setup(options);
      const primary = attributes(4, 5);
      const alternate = attributes(4, 6);
      m.payloadSource.getPayload.mockImplementation(async (handle) => {
        const session = m.sessions.get(handle.payloadId);
        if (!session) throw Error("Missing session");
        if (session.request.requireUnleased) m.canRetainAlternate.mockReturnValue(false);
        return session.payload;
      });
      m.start(primary);
      m.bidder.onAlternatePayloadAttributes(alternate);
      return {...m, primary, alternate};
    }

    function retainedAlternates(m: ReturnType<typeof startBusyAlternate>) {
      return m.retainCommitment.mock.calls.filter(
        ([bid]) => toRootHex(bid.message.parentBlockHash) === toRootHex(m.alternate.data.parentBlockHash)
      );
    }

    it("uses the copied snapshot after one idle recheck without another build or retrieval", async () => {
      const m = startBusyAlternate();
      await vi.advanceTimersByTimeAsync(350);
      expect(m.sessions.size).toBe(2);
      expect(m.offer(m.alternate)).toBeNull();
      const primary = m.sessions.get("1");
      if (!primary) throw Error("Missing primary");
      primary.payload = {
        ...primary.payload,
        executionPayloadValue: 2_000_000_000_000_000_000n,
        executionPayload: {...primary.payload.executionPayload, blockHash: Buffer.alloc(32, 19)},
      };
      m.canRetainAlternate.mockReturnValue(true);
      await vi.advanceTimersByTimeAsync(9);
      expect(m.offer(m.alternate)).toBeNull();
      await vi.advanceTimersByTimeAsync(1);
      expect(m.offer(m.alternate)).not.toBeNull();
      expect(retainedAlternates(m)).toHaveLength(1);
      expect(m.payloadSource.prepare.mock.calls.filter(([r]) => r.requireUnleased)).toHaveLength(1);
      expect(m.payloadSource.getPayload.mock.calls.filter(([h]) => h.payloadId === "2")).toHaveLength(1);
      await vi.advanceTimersByTimeAsync(40);
      expect(m.offer(m.primary)?.message.value).toBe(1_800_000_000);
      expect(m.sessions.size).toBe(1);
      expect(m.logger.debug).toHaveBeenCalledWith(
        "Alternate parent journal recheck",
        expect.objectContaining({code: "BUILDER_ALTERNATE_JOURNAL_RECHECK", outcome: "idle"})
      );
    });

    it("skips after one recheck if the journal remains busy and never restarts that build", async () => {
      const m = startBusyAlternate();
      await vi.advanceTimersByTimeAsync(360);
      expect(m.offer(m.alternate)).toBeNull();
      expect(retainedAlternates(m)).toHaveLength(0);
      expect(m.sessions.size).toBe(1);
      m.canRetainAlternate.mockReturnValue(true);
      await vi.advanceTimersByTimeAsync(600);
      expect(m.payloadSource.prepare.mock.calls.filter(([r]) => r.requireUnleased)).toHaveLength(1);
      expect(m.logger.debug).toHaveBeenCalledWith(
        "Alternate parent journal recheck",
        expect.objectContaining({outcome: "busy"})
      );
    });

    it.each([100, 99])("requires at least 100ms remaining after the wait (%ims)", async (remainingMs) => {
      const m = startBusyAlternate();
      await vi.advanceTimersByTimeAsync(350);
      m.canRetainAlternate.mockReturnValue(true);
      vi.setSystemTime(500 - remainingMs - 10);
      await vi.advanceTimersByTimeAsync(10);
      expect(retainedAlternates(m)).toHaveLength(remainingMs === 100 ? 1 : 0);
      expect(m.logger.debug).toHaveBeenCalledWith(
        "Alternate parent journal recheck",
        expect.objectContaining({outcome: remainingMs === 100 ? "idle" : "headroom", remainingMs})
      );
    });

    it("rejects the alternate if the same primary becomes unready while waiting", async () => {
      const m = startBusyAlternate({updateIntervalMs: 27});
      await vi.advanceTimersByTimeAsync(350);
      const primary = m.sessions.get("1");
      if (!primary) throw Error("Missing primary");
      primary.payload = {
        ...primary.payload,
        offerEligibility: {
          generation: "0x0",
          lease: {generation: "0x0", ready: false, validUntil: 0, validUntilUnixMs: 0},
        },
      };
      m.canRetainAlternate.mockReturnValue(true);
      await vi.advanceTimersByTimeAsync(10);
      expect(retainedAlternates(m)).toHaveLength(0);
      expect(m.offer(m.alternate)).toBeNull();
      expect(m.logger.debug).toHaveBeenCalledWith(
        "Alternate parent journal recheck",
        expect.objectContaining({outcome: "primary_not_ready"})
      );
    });

    it("does not let a recheck diagnostic failure change admission", async () => {
      const m = startBusyAlternate();
      m.logger.debug.mockImplementation((message) => {
        if (message === "Alternate parent journal recheck") throw Error("logger unavailable");
      });
      await vi.advanceTimersByTimeAsync(350);
      m.canRetainAlternate.mockReturnValue(true);
      await vi.advanceTimersByTimeAsync(10);
      expect(retainedAlternates(m)).toHaveLength(1);
      expect(m.offer(m.alternate)).not.toBeNull();
    });

    it.each(["alternatesDisabled", "uncertain"] as const)("rechecks source %s after waiting", async (field) => {
      const m = startBusyAlternate();
      await vi.advanceTimersByTimeAsync(350);
      const resources = m.payloadSource.getResources();
      if (!resources) throw Error("Missing resources");
      m.payloadSource.getResources.mockReturnValue({...resources, [field]: field === "uncertain" ? 1 : true});
      m.canRetainAlternate.mockReturnValue(true);
      await vi.advanceTimersByTimeAsync(10);
      expect(retainedAlternates(m)).toHaveLength(0);
      expect(m.offer(m.alternate)).toBeNull();
      expect(m.logger.debug).toHaveBeenCalledWith(
        "Alternate parent journal recheck",
        expect.objectContaining({outcome: field === "uncertain" ? "source_uncertain" : "source_disabled"})
      );
    });

    it("does not rebuild a copied recheck snapshot after an eligible primary replaces it", async () => {
      const m = startBusyAlternate();
      await vi.advanceTimersByTimeAsync(350);
      m.canRetainAlternate.mockReturnValue(true);
      const replacement = attributes(8, 9);
      m.start(replacement);
      await vi.advanceTimersByTimeAsync(1000);
      expect(m.offer(replacement)).not.toBeNull();
      expect(m.offer(m.alternate)).toBeNull();
      expect(retainedAlternates(m)).toHaveLength(0);
      expect(m.payloadSource.prepare.mock.calls.filter(([r]) => r.requireUnleased)).toHaveLength(1);
      expect(m.payloadSource.getPayload.mock.calls.filter(([h]) => h.payloadId === "2")).toHaveLength(1);
      expect(m.sessions.size).toBe(1);
    });

    it.each([15, 16])("preserves the 250ms deadline with %ims retrieval before a busy recheck", async (retrievalMs) => {
      const m = startBusyAlternate({alternateBuildDurationMs: 250});
      const getPayload = m.payloadSource.getPayload.getMockImplementation();
      if (!getPayload) throw Error("Missing payload implementation");
      m.payloadSource.getPayload.mockImplementation(async (...args) => {
        const payload = await getPayload(...args);
        if (args[0].payloadId === "2") await new Promise<void>((resolve) => setTimeout(resolve, retrievalMs));
        return payload;
      });
      await vi.advanceTimersByTimeAsync(125 + retrievalMs);
      m.canRetainAlternate.mockReturnValue(true);
      await vi.advanceTimersByTimeAsync(10);
      expect(retainedAlternates(m)).toHaveLength(retrievalMs === 15 ? 1 : 0);
      expect(m.payloadSource.getPayload.mock.calls.filter(([h]) => h.payloadId === "2")).toHaveLength(1);
      expect(m.logger.debug).toHaveBeenCalledWith(
        "Alternate parent journal recheck",
        expect.objectContaining({outcome: retrievalMs === 15 ? "idle" : "headroom"})
      );
    });

    it.each(["abort", "primary change", "deadline", "insufficient headroom"] as const)(
      "does not sign when %s occurs during the recheck wait",
      async (reason) => {
        const m = startBusyAlternate();
        await vi.advanceTimersByTimeAsync(350);
        m.canRetainAlternate.mockReturnValue(true);
        if (reason === "abort") controller.abort();
        else if (reason === "primary change") {
          const replacement = attributes(8, 9);
          replacement.data.proposerIndex = 4;
          m.start(replacement);
        } else vi.setSystemTime(reason === "deadline" ? 600 : 405);
        await vi.advanceTimersByTimeAsync(10);
        expect(m.offer(m.alternate)).toBeNull();
        expect(retainedAlternates(m)).toHaveLength(0);
        expect(m.payloadSource.prepare.mock.calls.filter(([r]) => r.requireUnleased)).toHaveLength(1);
        expect(m.logger.debug).toHaveBeenCalledWith(
          "Alternate parent journal recheck",
          expect.objectContaining({outcome: reason === "abort" || reason === "primary change" ? "aborted" : "headroom"})
        );
      }
    );

    it.each(["branch capacity", "head balance", "new liability"] as const)(
      "uses fresh %s after the recheck wait",
      async (change) => {
        const m = startBusyAlternate();
        if (change === "new liability") m.alternate.context.builders[0].availableGwei = 1_000_000_000;
        await vi.advanceTimersByTimeAsync(350);
        m.canRetainAlternate.mockReturnValue(true);
        if (change === "branch capacity") m.alternate.context.builders[0].availableGwei = 400_000_000;
        else if (change === "head balance")
          m.getBuilderStatus.mockReturnValue({status: "active", balance: 1_300_000_000});
        else {
          const recorded = m.ledger.getBidsForSlot(1)[0];
          expect(m.ledger.recordWin(recorded, toRootHex(Buffer.alloc(32, 88)))).not.toBeNull();
        }
        await vi.advanceTimersByTimeAsync(10);
        expect(m.offer(m.alternate)).toBeNull();
        expect(retainedAlternates(m)).toHaveLength(0);
        expect(m.logger.info).toHaveBeenCalledWith(
          "Bid policy declined to bid",
          expect.objectContaining({
            parentBlockHash: toRootHex(m.alternate.data.parentBlockHash),
            coverableGwei:
              change === "branch capacity" ? 400_000_000 : change === "head balance" ? 300_000_000 : 100_000_000,
          })
        );
      }
    );

    it("rejects an inactive branch changed while waiting before it signs", async () => {
      const m = startBusyAlternate();
      await vi.advanceTimersByTimeAsync(350);
      m.canRetainAlternate.mockReturnValue(true);
      m.alternate.context.builders[0].active = false;
      await vi.advanceTimersByTimeAsync(10);
      expect(retainedAlternates(m)).toHaveLength(0);
      expect(m.offer(m.alternate)).toBeNull();
    });

    it("checks revision capacity after waiting instead of reserving ahead of primary updates", async () => {
      const m = startBusyAlternate({maxRevisionsPerSlot: 4});
      await vi.advanceTimersByTimeAsync(350);
      m.canRetainAlternate.mockReturnValue(true);
      const recorded = m.ledger.getBidsForSlot(1)[0];
      m.ledger.recordRevision({
        ...recorded,
        blockHash: toRootHex(Buffer.alloc(32, 87)),
        signedBidRoot: toRootHex(Buffer.alloc(32, 86)),
      });
      await vi.advanceTimersByTimeAsync(10);
      expect(m.ledger.getRevisionCount(1)).toBe(2);
      expect(retainedAlternates(m)).toHaveLength(0);
      expect(m.offer(m.alternate)).toBeNull();
    });
  });

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

  it.each([250, 500, 1000])(
    "finishes a 100ms alternate journal write within the existing %ims budget",
    async (duration) => {
      const m = setup({alternateBuildDurationMs: duration});
      const primary = attributes(4, 5);
      const alternate = attributes(4, 6);
      let retainedAt: number | undefined;
      m.retainCommitment.mockImplementation(async (bid) => {
        if (toRootHex(bid.message.parentBlockHash) === toRootHex(alternate.data.parentBlockHash)) {
          await new Promise<void>((resolve) => setTimeout(resolve, 100));
          retainedAt = Date.now();
        }
      });
      m.start(primary);
      const startedAt = Date.now();
      m.bidder.onAlternatePayloadAttributes(alternate);
      await vi.advanceTimersByTimeAsync(duration);

      expect(retainedAt).toBeDefined();
      expect(retainedAt).toBeLessThan(startedAt + duration);
      expect(m.offer(alternate)).not.toBeNull();
      expect(m.offer(primary)).not.toBeNull();
      expect(m.sessions.size).toBe(1);
      expect(m.payloadSource.release.mock.calls.map(([handle]) => handle.payloadId)).not.toContain("1");
    }
  );

  it.each([20, 30])(
    "includes %ims of retrieval in the shortest alternate retention deadline",
    async (retrievalTime) => {
      const m = setup({alternateBuildDurationMs: 250});
      const alternate = attributes(4, 6);
      const retained: gloas.SignedExecutionPayloadBid[] = [];
      m.payloadSource.getPayload.mockImplementation(async (handle) => {
        if (handle.payloadId === "2") await new Promise<void>((resolve) => setTimeout(resolve, retrievalTime));
        const session = m.sessions.get(handle.payloadId);
        if (!session) throw Error("Missing session");
        return session.payload;
      });
      m.retainCommitment.mockImplementation(async (bid) => {
        if (toRootHex(bid.message.parentBlockHash) === toRootHex(alternate.data.parentBlockHash)) {
          await new Promise<void>((resolve) => setTimeout(resolve, 100));
          retained.push(bid);
        }
      });
      m.start(attributes(4, 5));
      m.bidder.onAlternatePayloadAttributes(alternate);
      await vi.advanceTimersByTimeAsync(300);

      expect(retained).toHaveLength(1);
      expect(m.bidStore.has(retained[0])).toBe(true);
      expect(m.payloadStore.has(toRootHex(retained[0].message.blockHash))).toBe(true);
      expect(m.bidStore.canOffer(retained[0])).toBe(retrievalTime === 20);
      expect(m.offer(alternate) !== null).toBe(retrievalTime === 20);
      if (retrievalTime === 30)
        expect(m.logger.debug).toHaveBeenCalledWith("Skipping alternate parent bid", {
          slot: 1,
          parentBlockRoot: toRootHex(alternate.data.parentBlockRoot),
          parentBlockHash: toRootHex(alternate.data.parentBlockHash),
          reason: "deadline_after_retention",
        });
    }
  );

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

  it("logs source blocking reasons once per slot despite repeated resource polls", async () => {
    const m = setup();
    const resources: PayloadSourceResources = {
      capacity: 2,
      active: 1,
      preparing: 0,
      releasing: 0,
      uncertain: 1,
      alternatesDisabled: false,
    };
    Object.assign(resources, {code: "private-code", reason: "private-reason", endpoint: "http://private:secret@rpc"});
    m.payloadSource.getResources.mockReturnValue(resources);
    const alternate = attributes(4, 6);
    m.start(attributes(4, 5));
    m.bidder.onAlternatePayloadAttributes(alternate);
    await vi.advanceTimersByTimeAsync(2000);
    const diagnostics = () => m.logger.debug.mock.calls.filter(([message]) => message === "Alternate parent admission");
    expect(m.payloadSource.getResources.mock.calls.length).toBeGreaterThan(30);
    expect(diagnostics().filter(([, context]) => hasReason(context, "source_busy"))).toHaveLength(1);
    expect(m.logger.debug).toHaveBeenCalledWith("Alternate parent admission", {
      code: "BUILDER_ALTERNATE_ADMISSION",
      reason: "source_busy",
      localSlot: 0,
      slot: 1,
      parentBlockRoot: toRootHex(alternate.data.parentBlockRoot),
      parentBlockHash: toRootHex(alternate.data.parentBlockHash),
      primarySlot: 1,
      primaryReady: true,
      activeBuilds: 1,
      queueSize: 1,
      attempts: 0,
      capacity: 2,
      active: 1,
      preparing: 0,
      releasing: 0,
      uncertain: 1,
      alternatesDisabled: false,
    });
    resources.alternatesDisabled = true;
    await vi.advanceTimersByTimeAsync(2000);
    for (let i = 0; i < 100; i++) m.bidder.onAlternatePayloadAttributes(alternate);
    expect(diagnostics().filter(([, context]) => hasReason(context, "source_disabled"))).toHaveLength(1);
    expect(diagnostics().filter(([, context]) => hasReason(context, "received"))).toHaveLength(1);
    expect(diagnostics().length).toBeLessThanOrEqual(16);
    expect(m.payloadSource.prepare).toHaveBeenCalledTimes(1);
    expect(m.offer(alternate)).toBeNull();
    expect(m.offer(attributes(4, 5))).not.toBeNull();
  });

  it("bounds receipt diagnostics by local slot regardless of event slot or tuple churn", () => {
    const m = setup();
    const flood = () => {
      for (let i = 0; i < 200; i++) {
        const event = attributes(i, 6);
        event.data.proposalSlot = 1000 + i;
        m.bidder.onAlternatePayloadAttributes(event);
      }
    };
    const diagnostics = () => m.logger.debug.mock.calls.filter(([message]) => message === "Alternate parent admission");
    flood();
    expect(diagnostics().filter(([, context]) => hasReason(context, "received"))).toHaveLength(4);
    expect(diagnostics().filter(([, context]) => hasReason(context, "primary_context_unavailable"))).toHaveLength(1);
    expect(diagnostics().length).toBeLessThanOrEqual(16);
    vi.setSystemTime(12_000);
    flood();
    expect(diagnostics().filter(([, context]) => hasReason(context, "received"))).toHaveLength(8);
    expect(diagnostics().length).toBeLessThanOrEqual(32);
    const count = diagnostics().length;
    vi.setSystemTime(0);
    flood();
    expect(diagnostics()).toHaveLength(count);
    expect(m.payloadSource.prepare).not.toHaveBeenCalled();
  });

  it("does not alter primary or alternate construction when admission diagnostics throw", async () => {
    const m = setup();
    m.logger.debug.mockImplementation((message) => {
      if (message === "Alternate parent admission") throw new Error("logger unavailable");
    });
    m.start(attributes(4, 5));
    m.bidder.onAlternatePayloadAttributes(attributes(4, 6));
    await vi.advanceTimersByTimeAsync(1000);
    expect(m.payloadSource.prepare).toHaveBeenCalledTimes(2);
    expect(m.sessions.size).toBe(1);
    expect(m.offer(attributes(4, 5))).not.toBeNull();
    expect(m.offer(attributes(4, 6))).not.toBeNull();
  });

  it("does not expose a diagnostic clock failure when no primary exists", () => {
    const m = setup();
    vi.spyOn(m.clock, "getCurrentSlot").mockImplementation(() => {
      throw new Error("clock unavailable");
    });
    expect(() => m.bidder.onAlternatePayloadAttributes(attributes(4, 6))).not.toThrow();
    expect(m.payloadSource.prepare).not.toHaveBeenCalled();
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
