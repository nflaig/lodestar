import {describe, expect, it, vi} from "vitest";
import {type EngineApiRpcReturnTypes, JsonRpcHttpClient, serializePayloadAttributes} from "@lodestar/beacon-node";
import type {BuildRequest} from "@lodestar/builder";
import {ForkName} from "@lodestar/params";
import {ssz} from "@lodestar/types";
import {ErrorAborted, toRootHex} from "@lodestar/utils";
import {SessionPayloadErrorCode, SessionPayloadSource} from "../../../../src/cmds/builder/sessionPayloadSource.js";

type PayloadResponse = EngineApiRpcReturnTypes["engine_getPayloadV6"];

function buildRequest(): BuildRequest {
  return {
    fork: ForkName.gloas,
    forkchoiceState: {
      headBlockHash: toRootHex(Buffer.alloc(32, 1)),
      safeBlockHash: toRootHex(Buffer.alloc(32, 2)),
      finalizedBlockHash: toRootHex(Buffer.alloc(32, 3)),
    },
    payloadAttributes: {
      ...ssz.gloas.PayloadAttributes.defaultValue(),
      timestamp: 1_787_212_236,
      slotNumber: 12,
      targetGasLimit: 200_000_000n,
      suggestedFeeRecipient: "0x" + "11".repeat(20),
      prevRandao: Buffer.alloc(32, 4),
      parentBeaconBlockRoot: Buffer.alloc(32, 5),
      withdrawals: [{index: 1, validatorIndex: 2, address: Buffer.alloc(20, 6), amount: 3n}],
    },
  };
}

function payloadResponse(request: BuildRequest, transactions = ["0x01aa", "0x02bb"]): PayloadResponse {
  const attrs = serializePayloadAttributes(request.payloadAttributes);
  return {
    executionPayload: {
      parentHash: request.forkchoiceState.headBlockHash,
      feeRecipient: attrs.suggestedFeeRecipient,
      stateRoot: "0x" + "22".repeat(32),
      receiptsRoot: "0x" + "33".repeat(32),
      logsBloom: "0x" + "00".repeat(256),
      prevRandao: attrs.prevRandao,
      blockNumber: "0x1",
      gasLimit: "0xbebc200",
      gasUsed: "0x1234",
      timestamp: attrs.timestamp,
      extraData: "0x",
      baseFeePerGas: "0x7",
      blockHash: "0x" + "44".repeat(32),
      transactions,
      withdrawals: attrs.withdrawals,
      blobGasUsed: "0x0",
      excessBlobGas: "0x0",
      blockAccessList: "0xc0",
      slotNumber: attrs.slotNumber,
    },
    blockValue: "0x10000000000000001",
    blobsBundle: {blobs: [], proofs: [], commitments: []},
    executionRequests: [],
    shouldOverrideBuilder: false,
  };
}

describe("SessionPayloadSource", () => {
  const signal = new AbortController().signal;
  function setup() {
    const rpc = new JsonRpcHttpClient(["http://127.0.0.1:1"]);
    const fetch = vi.spyOn(rpc, "fetch");
    const signals: AbortSignal[] = [];
    const source = new SessionPayloadSource("session", (callSignal) => {
      signals.push(callSignal);
      return rpc;
    });
    return {source, fetch, signals, request: buildRequest()};
  }

  it("retrieves multiple snapshots from one handle and releases without invalidating them", async () => {
    const {source, fetch, signals, request} = setup();
    const first = payloadResponse(request, []);
    const second = payloadResponse(request);
    second.executionPayload.blockHash = "0x" + "55".repeat(32);
    fetch
      .mockResolvedValueOnce({sessionId: "session-1"})
      .mockResolvedValueOnce(first)
      .mockResolvedValueOnce(second)
      .mockResolvedValueOnce(true);
    const handle = await source.prepare(request, signal);
    const a = await source.getPayload(handle, signal);
    const b = await source.getPayload(handle, signal);
    await source.release(handle, signal);
    expect(fetch.mock.calls.map(([call]) => call.method)).toEqual([
      "zbuilder_beginV1",
      "zbuilder_getPayloadV1",
      "zbuilder_getPayloadV1",
      "zbuilder_endV1",
    ]);
    expect(fetch.mock.calls[0][0].params).toEqual([
      request.forkchoiceState.headBlockHash,
      serializePayloadAttributes(request.payloadAttributes),
    ]);
    expect(signals.every((value) => value === signal)).toBe(true);
    expect(a.executionPayload.transactions).toHaveLength(0);
    expect(b.executionPayload.transactions).toHaveLength(2);
    await expect(source.getPayload(handle, signal)).rejects.toMatchObject({
      type: {code: SessionPayloadErrorCode.UNKNOWN_HANDLE},
    });
  });

  it.each(["parentHash", "timestamp", "slotNumber", "prevRandao", "feeRecipient", "withdrawals"] as const)(
    "rejects a snapshot with changed %s",
    async (field) => {
      const {source, fetch, request} = setup();
      const output = payloadResponse(request);
      if (field === "withdrawals") output.executionPayload.withdrawals = [];
      else if (field === "parentHash" || field === "prevRandao")
        output.executionPayload[field] = "0x" + "ff".repeat(32);
      else if (field === "feeRecipient") output.executionPayload[field] = "0x" + "ff".repeat(20);
      else output.executionPayload[field] = "0x01";
      fetch.mockResolvedValueOnce({sessionId: "session-1"}).mockResolvedValueOnce(output);
      const handle = await source.prepare(request, signal);
      await expect(source.getPayload(handle, signal)).rejects.toMatchObject({
        type: {code: SessionPayloadErrorCode.RESPONSE_MISMATCH},
      });
    }
  );

  it("preserves constructor extra data and rejects more than 32 bytes", async () => {
    const {source, fetch, request} = setup();
    const output = payloadResponse(request);
    output.executionPayload.extraData = "0x" + Buffer.from("zbuilder 0123456789ab").toString("hex");
    fetch.mockResolvedValueOnce({sessionId: "session-1"}).mockResolvedValueOnce(output);
    const handle = await source.prepare(request, signal);
    const payload = await source.getPayload(handle, signal);
    expect(Buffer.from(payload.executionPayload.extraData).toString()).toBe("zbuilder 0123456789ab");
    output.executionPayload.extraData = "0x" + "aa".repeat(33);
    fetch.mockResolvedValueOnce(output);
    await expect(source.getPayload(handle, signal)).rejects.toThrow();
  });

  it("releases a begin response arriving after cancellation", async () => {
    const {source, fetch, request} = setup();
    const controller = new AbortController();
    fetch
      .mockImplementationOnce(async () => {
        controller.abort();
        return {sessionId: "late"};
      })
      .mockResolvedValueOnce(true);
    await expect(source.prepare(request, controller.signal)).rejects.toBeInstanceOf(ErrorAborted);
    expect(fetch.mock.calls[1][0]).toEqual({method: "zbuilder_endV1", params: ["late"]});
  });
});
