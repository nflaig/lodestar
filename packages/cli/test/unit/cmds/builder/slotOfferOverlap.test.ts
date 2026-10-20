import {afterEach, beforeEach, describe, expect, it, vi} from "vitest";
import {SecretKey} from "@chainsafe/lodestar-z/blst";
import {type EngineApiRpcReturnTypes, JsonRpcHttpClient, serializePayloadAttributes} from "@lodestar/beacon-node";
import {createBeaconConfig} from "@lodestar/config";
import {getConfig} from "@lodestar/config/test-utils";
import {DOMAIN_BUILDER_REQUEST_AUTH, ForkName} from "@lodestar/params";
import {ZERO_HASH, computeDomain, computeSigningRoot} from "@lodestar/state-transition";
import {ssz} from "@lodestar/types";
import {toHex, toRootHex} from "@lodestar/utils";
import {getBuilderApi} from "../../../../../builder/src/api/impl.js";
import {type PayloadAttributesEvent} from "../../../../../builder/src/services/bidder.js";
import {BuilderSigner} from "../../../../../builder/src/services/builderSigner.js";
import {PayloadStore} from "../../../../../builder/src/services/payloadStore.js";
import {ProposerDutiesTracker} from "../../../../../builder/src/services/proposerDutiesTracker.js";
import {ProposerPreferencesTracker} from "../../../../../builder/src/services/proposerPreferencesTracker.js";
import {getApiClientStub, mockApiResponse} from "../../../../../builder/test/unit/utils/apiStub.js";
import {createBiddingModules} from "../../../../../builder/test/unit/utils/bidding.js";
import {ClockMock} from "../../../../../builder/test/unit/utils/clock.js";
import {getMockedLogger} from "../../../../../builder/test/unit/utils/logger.js";
import {SessionPayloadSource} from "../../../../src/cmds/builder/sessionPayloadSource.js";

const slot = 1;
const slotStart = 12_000;
const parentHash = Buffer.alloc(32, 5);
const parentRoot = Buffer.alloc(32, 4);
const dependentRoot = Buffer.alloc(32, 7);
const namespace = `0x${"cd".repeat(16)}`;
const authData = new TextEncoder().encode("synthetic-slot-overlap.example");
const proposerKey = SecretKey.fromBytes(Buffer.alloc(32, 2));
const proposerPubkey = proposerKey.toPublicKey().toBytes();
type PayloadResponse = EngineApiRpcReturnTypes["engine_getPayloadV6"];
type SerializedAttributes = ReturnType<typeof serializePayloadAttributes>;

function attributes(proposalSlot: number): PayloadAttributesEvent {
  const data = ssz.gloas.SSEPayloadAttributes.defaultValue();
  data.proposalSlot = proposalSlot;
  data.proposerIndex = 3;
  data.parentBlockHash = parentHash;
  data.parentBlockRoot = parentRoot;
  data.payloadAttributes.parentBeaconBlockRoot = parentRoot;
  data.payloadAttributes.slotNumber = proposalSlot;
  data.payloadAttributes.timestamp = proposalSlot * 12;
  data.payloadAttributes.prevRandao = Buffer.alloc(32, 6);
  return {version: ForkName.gloas, data};
}

function payload(attrs: SerializedAttributes): PayloadResponse {
  if (attrs.slotNumber === undefined) throw Error("Synthetic Gloas attributes require a slot");
  return {
    executionPayload: {
      parentHash: toHex(parentHash),
      feeRecipient: attrs.suggestedFeeRecipient,
      stateRoot: toHex(Buffer.alloc(32, 2)),
      receiptsRoot: toHex(Buffer.alloc(32, 3)),
      logsBloom: toHex(Buffer.alloc(256)),
      prevRandao: attrs.prevRandao,
      blockNumber: "0x1",
      gasLimit: "0x3938700",
      gasUsed: "0x0",
      timestamp: attrs.timestamp,
      extraData: "0x",
      baseFeePerGas: "0x7",
      blockHash: toHex(Buffer.alloc(32, Number(BigInt(attrs.slotNumber)))),
      transactions: [],
      withdrawals: attrs.withdrawals,
      blobGasUsed: "0x0",
      excessBlobGas: "0x0",
      blockAccessList: "0xc0",
      slotNumber: attrs.slotNumber,
    },
    blockValue: "0x6a94d74f430000",
    blobsBundle: {blobs: [], proofs: [], commitments: []},
    executionRequests: [],
    shouldOverrideBuilder: false,
  };
}

describe("current-slot HTTP wait overlapping next-slot construction", () => {
  let controller: AbortController;
  let pendingBuilds: Promise<void>[];

  beforeEach(() => {
    vi.useFakeTimers({now: 0, toFake: ["Date", "performance", "setTimeout", "clearTimeout"]});
    controller = new AbortController();
    pendingBuilds = [];
  });

  afterEach(async () => {
    controller.abort();
    await vi.advanceTimersByTimeAsync(0);
    await Promise.all(pendingBuilds);
    vi.restoreAllMocks();
    vi.useRealTimers();
  });

  it.each([false, true])(
    "returns the current behavior before +2520 ms with next-slot attributes=%s",
    async (startNextSlot) => {
      const rpc = new JsonRpcHttpClient(["http://127.0.0.1:1"]);
      const sessions = new Map<string, PayloadResponse>();
      const calls: {method: string; id?: string; offsetMs: number}[] = [];
      let reservation = 0;
      const fetch = vi.spyOn(rpc, "fetch").mockImplementation(async ({method, params}) => {
        if (!Array.isArray(params)) throw Error("Unexpected synthetic RPC parameters");
        const id = typeof params[0] === "string" ? params[0] : undefined;
        calls.push({method, id, offsetMs: Date.now() - slotStart});
        switch (method) {
          case "zbuilder_reserveBeginV1":
            return {reservationId: `begin-${"ab".repeat(16)}-${++reservation}`, beginReservationVersion: 1};
          case "zbuilder_beginReservedV1": {
            if (!id || typeof params[2] !== "object" || params[2] === null) throw Error("Missing synthetic build");
            sessions.set(id, payload(params[2] as SerializedAttributes));
            return {
              sessionId: id,
              beginReservationVersion: 1,
              orderGenerationVersion: 1,
              alternateSessionVersion: 0,
              retainedOfferVersion: 1,
              primaryHandoffVersion: 1,
            };
          }
          case "zbuilder_getPayloadV2": {
            const current = id && sessions.get(id);
            if (!current) throw Error("Reading a released synthetic worker");
            return {
              generation: "0x0",
              namespace,
              leaseMs: 250,
              ready: true,
              payload: params[1] === current.executionPayload.blockHash ? null : current,
            };
          }
          case "zbuilder_cancelBeginV1":
            if (!id || !sessions.delete(id)) throw Error("Releasing an unowned synthetic worker");
            return {beginReservationVersion: 1, released: true};
          default:
            throw Error(`Unexpected synthetic constructor method: ${method}`);
        }
      });
      const source = new SessionPayloadSource("synthetic", () => rpc, {
        boundedAlternates: true,
        beginReservations: true,
        retainedAlternateOffers: true,
        primaryHandoff: true,
      });
      const config = createBeaconConfig(getConfig(ForkName.gloas), Buffer.alloc(32));
      const clock = new ClockMock();
      clock.msToSlot = (target) => target * 12_000 - Date.now();
      clock.getCurrentSlot = () => Math.floor(Date.now() / 12_000);
      const api = getApiClientStub();
      api.validator.getProposerDutiesV2.mockResolvedValue(
        mockApiResponse({
          data: [slot, slot + 1].map((target) => ({slot: target, validatorIndex: 3, pubkey: proposerPubkey})),
          meta: {executionOptimistic: false, dependentRoot: toRootHex(dependentRoot)},
        })
      );
      api.beacon.publishExecutionPayloadBid.mockResolvedValue(mockApiResponse({}));
      const preferences = new ProposerPreferencesTracker();
      for (const target of [slot, slot + 1]) {
        const preference = ssz.gloas.SignedProposerPreferences.defaultValue();
        preference.message.proposalSlot = target;
        preference.message.dependentRoot = dependentRoot;
        preference.message.targetGasLimit = 60_000_000n;
        preferences.onProposerPreferences(preference);
      }
      const logger = getMockedLogger();
      const payloadStore = new PayloadStore();
      const builderKey = SecretKey.fromBytes(Buffer.alloc(32, 1));
      const retainCommitment = vi.fn().mockResolvedValue(undefined);
      const {bidder, bidStore} = createBiddingModules({
        api,
        config,
        clock,
        logger,
        payloadSource: source,
        payloadStore,
        proposerPreferencesTracker: preferences,
        builderSigner: new BuilderSigner(config, {secretKey: builderKey, publicKey: builderKey.toPublicKey()}),
        signal: controller.signal,
        retainCommitment,
        updateOptions: {
          updateIntervalMs: 100,
          updateCutoffBps: 2100,
          deadlineBps: 9250,
          alternateBranches: true,
          primaryHandoff: true,
        },
      });
      const builderApi = getBuilderApi(
        {
          config,
          clock,
          logger,
          api,
          bidStore,
          proposerDutiesTracker: new ProposerDutiesTracker(api, clock),
          builderIndex: 1,
          metrics: null,
          signal: controller.signal,
        },
        {authData}
      );
      pendingBuilds.push(bidder.onPayloadAttributes(attributes(slot)));
      await vi.advanceTimersByTimeAsync(slotStart + 1897);
      const signed = bidStore.get(slot, toRootHex(parentHash), toRootHex(parentRoot));
      expect(signed).not.toBeNull();
      if (!signed) throw Error("Missing synthetic current-slot bid");
      expect(bidStore.canOffer(signed)).toBe(true);
      expect(retainCommitment).toHaveBeenCalledExactlyOnceWith(signed);
      const eligibility = bidStore.getOfferEligibility(signed);
      expect(eligibility).toMatchObject({generation: "0x0", namespace, lease: {generation: "0x0", ready: true}});
      expect(eligibility?.lease.validUntilUnixMs).toBeGreaterThan(Date.now());
      expect(source.supportsPrimaryHandoff).toBe(true);
      const message = {slot, data: authData};
      const requestAuth = {
        message,
        signature: proposerKey
          .sign(
            computeSigningRoot(
              ssz.gloas.BuilderRequestAuth,
              message,
              computeDomain(DOMAIN_BUILDER_REQUEST_AUTH, config.GENESIS_FORK_VERSION, ZERO_HASH)
            )
          )
          .toBytes(),
      };
      let settled = false;
      const response = builderApi
        .getExecutionPayloadBid({
          slot,
          parentHash,
          parentRoot,
          proposerPubkey,
          requestAuth,
          dateMilliseconds: Date.now() - 149,
          timeoutMs: 598,
        })
        .then((result) => {
          settled = true;
          return result;
        });
      await vi.advanceTimersByTimeAsync(140);
      expect(Date.now()).toBe(slotStart + 2037);
      expect(settled).toBe(false);
      expect(bidStore.canOffer(signed)).toBe(true);
      if (startNextSlot) pendingBuilds.push(bidder.onPayloadAttributes(attributes(slot + 1)));
      await vi.advanceTimersByTimeAsync(0);
      expect(bidStore.canOffer(signed)).toBe(!startNextSlot);
      expect(source.getResources()).toMatchObject({active: 1, preparing: 0, releasing: 0, uncertain: 0});
      expect(sessions.size).toBe(1);
      const oldId = `begin-${"ab".repeat(16)}-1`;
      expect(sessions.has(oldId)).toBe(!startNextSlot);
      expect(calls.filter((call) => call.method === "zbuilder_cancelBeginV1")).toEqual(
        startNextSlot ? [{method: "zbuilder_cancelBeginV1", id: oldId, offsetMs: 2037}] : []
      );
      await vi.advanceTimersByTimeAsync(109);
      expect(settled).toBe(false);
      await vi.advanceTimersByTimeAsync(1);
      const result = await response;
      expect(Date.now()).toBe(slotStart + 2147);
      expect(config.getSlotComponentDurationMs(2100)).toBe(2520);
      expect(Date.now()).toBeLessThan(slotStart + config.getSlotComponentDurationMs(2100));
      expect(result.status ?? 200).toBe(startNextSlot ? 204 : 200);
      expect(result.data).toBe(startNextSlot ? undefined : signed);
      const responseLog = startNextSlot ? logger.debug : logger.info;
      expect(responseLog).toHaveBeenCalledWith(
        startNextSlot ? "No execution payload bid to serve" : "Served execution payload bid",
        expect.objectContaining({slot, bidWaitMs: 250, requestTransitMs: 149, waitCapacityExceeded: false})
      );
      expect(bidStore.has(signed)).toBe(true);
      expect(payloadStore.has(toRootHex(signed.message.blockHash))).toBe(true);
      expect(logger.error).not.toHaveBeenCalled();
      expect(fetch).toHaveBeenCalled();
      expect(calls.some((call) => call.method.includes("retainPrimary"))).toBe(false);
      expect(
        calls.filter((call) => call.method === "zbuilder_getPayloadV2" && call.id === oldId && call.offsetMs > 2037)
          .length
      ).toBe(startNextSlot ? 0 : 1);
    }
  );
});
