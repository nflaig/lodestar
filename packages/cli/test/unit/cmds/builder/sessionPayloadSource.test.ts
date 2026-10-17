import {createHash} from "node:crypto";
import {once} from "node:events";
import {createServer} from "node:http";
import {RLP} from "@ethereumjs/rlp";
import {describe, expect, it, vi} from "vitest";
import {type EngineApiRpcReturnTypes, JsonRpcHttpClient, serializePayloadAttributes} from "@lodestar/beacon-node";
import {type BuildRequest, PayloadSourceNotReady} from "@lodestar/builder";
import {ForkName} from "@lodestar/params";
import {ssz} from "@lodestar/types";
import {ErrorAborted, FetchError, toHex, toRootHex} from "@lodestar/utils";
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

  it("changes generations even when value drops and the block hash repeats", async () => {
    const {source, fetch, request} = setup();
    const now = vi.spyOn(performance, "now").mockReturnValue(100);
    try {
      fetch.mockResolvedValueOnce({sessionId: "leased", orderGenerationVersion: 1});
      const handle = await source.prepare(request, signal);
      const output = payloadResponse(request);
      fetch.mockResolvedValueOnce({generation: "0x0", ready: true, leaseMs: 250, payload: output});
      const old = await source.getPayload(handle, signal);
      expect(old.offerEligibility?.lease.validUntil).toBe(350);
      fetch.mockResolvedValueOnce({generation: "0x1", ready: false, leaseMs: 250, payload: null});
      await expect(source.getPayload(handle, signal)).rejects.toBeInstanceOf(PayloadSourceNotReady);
      expect(old.offerEligibility?.lease.ready).toBe(false);
      fetch.mockResolvedValueOnce({
        generation: "0x1",
        ready: true,
        leaseMs: 250,
        payload: {...output, blockValue: "0x1"},
      });
      const next = await source.getPayload(handle, signal);
      expect(next.executionPayloadValue).toBe(1n);
      expect(next.offerEligibility?.generation).toBe("0x1");
      expect(old.offerEligibility?.generation).toBe("0x0");
      fetch.mockResolvedValueOnce({generation: "0x1", ready: true, leaseMs: 250, payload: null});
      expect(await source.getPayload(handle, signal)).toBe(next);
      fetch.mockResolvedValueOnce({generation: "0x0", ready: true, leaseMs: 250, payload: output});
      await expect(source.getPayload(handle, signal)).rejects.toMatchObject({
        type: {code: SessionPayloadErrorCode.RESPONSE_MISMATCH, reason: "Order generation rollback"},
      });
      expect(next.offerEligibility?.lease.ready).toBe(false);
    } finally {
      now.mockRestore();
    }
  });

  it("charges delayed RPC delivery against the lease and never renews from response arrival", async () => {
    const {source, fetch, request} = setup();
    const now = vi.spyOn(performance, "now").mockReturnValue(100);
    try {
      fetch.mockResolvedValueOnce({sessionId: "delayed", orderGenerationVersion: 1});
      const handle = await source.prepare(request, signal);
      fetch.mockImplementationOnce(async () => {
        now.mockReturnValue(351);
        return {generation: "0x0", ready: true, leaseMs: 250, payload: payloadResponse(request)};
      });
      await expect(source.getPayload(handle, signal)).rejects.toBeInstanceOf(PayloadSourceNotReady);
      fetch.mockResolvedValueOnce({generation: "0x0", ready: true, leaseMs: 250, payload: null});
      const renewed = await source.getPayload(handle, signal);
      expect(renewed.offerEligibility?.lease.validUntil).toBe(601);
      expect(fetch).toHaveBeenLastCalledWith({
        method: "zbuilder_getPayloadV2",
        params: [handle.payloadId, payloadResponse(request).executionPayload.blockHash, "0x0"],
      });
      fetch.mockResolvedValueOnce(true);
      await source.release(handle, signal);
      expect(renewed.offerEligibility?.lease.ready).toBe(false);
    } finally {
      now.mockRestore();
    }
  });

  function transportError(code: string): FetchError {
    return new FetchError(
      "http://constructor",
      new TypeError("fetch failed", {cause: Object.assign(new Error("closed"), {code})})
    );
  }

  it("cannot revive an offer with a read response arriving after release", async () => {
    const {source, fetch, request} = setup();
    fetch.mockResolvedValueOnce({sessionId: "released", orderGenerationVersion: 1});
    const handle = await source.prepare(request, signal);
    const response = {generation: "0x0", ready: true, leaseMs: 250, payload: payloadResponse(request)};
    fetch.mockResolvedValueOnce(response);
    const first = await source.getPayload(handle, signal);
    let finish!: (value: typeof response) => void;
    fetch.mockImplementationOnce(
      () =>
        new Promise((resolve) => {
          finish = resolve;
        })
    );
    const late = source.getPayload(handle, signal);
    fetch.mockResolvedValueOnce(true);
    await source.release(handle, signal);
    finish(response);
    await expect(late).rejects.toMatchObject({type: {code: SessionPayloadErrorCode.UNKNOWN_HANDLE}});
    expect(first.offerEligibility?.lease.ready).toBe(false);
  });

  it("recovers a payload through the real HTTP client after the peer closes the socket", async () => {
    const request = buildRequest();
    const output = payloadResponse(request);
    const methods: string[] = [];
    let closeRead = true;
    const server = createServer(async (req, res) => {
      const chunks: Buffer[] = [];
      for await (const chunk of req) chunks.push(Buffer.from(chunk));
      const call = JSON.parse(Buffer.concat(chunks).toString()) as {id: number; method: string};
      methods.push(call.method);
      if (call.method === "zbuilder_getPayloadV1" && closeRead) {
        closeRead = false;
        req.socket.destroy();
        return;
      }
      res.setHeader("content-type", "application/json");
      res.end(
        JSON.stringify({
          jsonrpc: "2.0",
          id: call.id,
          result: call.method === "zbuilder_beginV1" ? {sessionId: "socket"} : output,
        })
      );
    });
    server.listen(0, "127.0.0.1");
    await once(server, "listening");
    try {
      const address = server.address();
      if (!address || typeof address === "string") throw new Error("Missing listener address");
      const source = new SessionPayloadSource(
        "socket",
        (signal) => new JsonRpcHttpClient([`http://127.0.0.1:${address.port}`], {signal, timeout: 1000})
      );
      const handle = await source.prepare(request, signal);
      expect((await source.getPayload(handle, signal)).executionPayloadValue).toBe(BigInt(output.blockValue));
      expect(methods).toEqual(["zbuilder_beginV1", "zbuilder_getPayloadV1", "zbuilder_getPayloadV1"]);
    } finally {
      server.closeAllConnections();
      await new Promise<void>((resolve, reject) => server.close((error) => (error ? reject(error) : resolve())));
    }
  });

  it.each(["UND_ERR_SOCKET", "ECONNRESET"])("retries one %s read with the original signal", async (code) => {
    const {source, fetch, signals, request} = setup();
    const output = payloadResponse(request);
    fetch
      .mockResolvedValueOnce({sessionId: "retry"})
      .mockRejectedValueOnce(transportError(code))
      .mockResolvedValueOnce(output);
    const handle = await source.prepare(request, signal);
    expect((await source.getPayload(handle, signal)).executionPayloadValue).toBe(BigInt(output.blockValue));
    expect(fetch).toHaveBeenCalledTimes(3);
    expect(fetch.mock.calls[2]).toEqual(fetch.mock.calls[1]);
    expect(signals.every((value) => value === signal)).toBe(true);
  });

  it("does not retry a second connection failure", async () => {
    const {source, fetch, request} = setup();
    const error = transportError("UND_ERR_SOCKET");
    fetch.mockResolvedValueOnce({sessionId: "retry"}).mockRejectedValue(error);
    const handle = await source.prepare(request, signal);
    await expect(source.getPayload(handle, signal)).rejects.toBe(error);
    expect(fetch).toHaveBeenCalledTimes(3);
  });

  it("does not restart a read after slot cancellation", async () => {
    const {source, fetch, request} = setup();
    const controller = new AbortController();
    fetch.mockResolvedValueOnce({sessionId: "retry"}).mockImplementationOnce(async () => {
      controller.abort();
      throw transportError("UND_ERR_SOCKET");
    });
    const handle = await source.prepare(request, controller.signal);
    await expect(source.getPayload(handle, controller.signal)).rejects.toBeInstanceOf(ErrorAborted);
    expect(fetch).toHaveBeenCalledTimes(2);
  });

  it.each([new Error("invalid JSON"), transportError("ECONNREFUSED"), transportError("ERR_TIMEOUT")])(
    "does not retry other failures: %s",
    async (error) => {
      const {source, fetch, request} = setup();
      fetch.mockResolvedValueOnce({sessionId: "retry"}).mockRejectedValueOnce(error);
      const handle = await source.prepare(request, signal);
      await expect(source.getPayload(handle, signal)).rejects.toBe(error);
      expect(fetch).toHaveBeenCalledTimes(2);
    }
  );

  it("does not retry session creation on a closing socket", async () => {
    const {source, fetch, request} = setup();
    const error = transportError("UND_ERR_SOCKET");
    fetch.mockRejectedValueOnce(error);
    await expect(source.prepare(request, signal)).rejects.toBe(error);
    expect(fetch).toHaveBeenCalledTimes(1);
  });

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

  function withBlobs(
    request: BuildRequest,
    count = 1
  ): PayloadResponse & {blobsBundle: NonNullable<PayloadResponse["blobsBundle"]>} {
    const output = payloadResponse(request, []);
    const commitment = Buffer.alloc(48, 7);
    const hash = createHash("sha256").update(commitment).digest();
    hash[0] = 1;
    const fields = Array.from({length: 14}, () => new Uint8Array());
    const tx = [...fields.slice(0, 10), Array.from({length: count}, () => hash), ...fields.slice(11)];
    output.executionPayload.transactions = [toHex(Buffer.concat([Buffer.from([3]), RLP.encode(tx)]))];
    output.executionPayload.blobGasUsed = "0x" + (count * 131072).toString(16);
    output.blobsBundle = {
      blobs: Array.from({length: count}, () => "0x" + "00".repeat(131072)),
      commitments: Array.from({length: count}, () => toHex(commitment)),
      proofs: Array.from({length: count * 128}, () => "0x" + "11".repeat(48)),
    };
    return {...output, blobsBundle: output.blobsBundle};
  }

  it("retains ordered blob data, including repeated hashes, after release", async () => {
    const {source, fetch, request} = setup();
    fetch.mockResolvedValueOnce({sessionId: "blobs"}).mockResolvedValueOnce(withBlobs(request, 2));
    const handle = await source.prepare(request, signal);
    const payload = await source.getPayload(handle, signal);
    fetch.mockResolvedValueOnce(true);
    await source.release(handle, signal);
    expect(payload.blobsBundle.blobs).toHaveLength(2);
    expect(payload.blobsBundle.proofs).toHaveLength(256);
    expect(payload.executionPayload.blobGasUsed).toBe(262144n);
  });

  it.each(["missingBlob", "oldProofs", "wrongCommitment", "wrongGas", "wrappedTransaction"])(
    "rejects incomplete blob reveals: %s",
    async (fault) => {
      const {source, fetch, request} = setup();
      const response = withBlobs(request);
      if (fault === "missingBlob") response.blobsBundle.blobs = [];
      if (fault === "oldProofs") response.blobsBundle.proofs = response.blobsBundle.proofs.slice(0, 1);
      if (fault === "wrongCommitment") response.blobsBundle.commitments[0] = "0x" + "08".repeat(48);
      if (fault === "wrongGas") response.executionPayload.blobGasUsed = "0x0";
      if (fault === "wrappedTransaction") response.executionPayload.transactions[0] = "0x03c0";
      fetch.mockResolvedValueOnce({sessionId: "blobs"}).mockResolvedValueOnce(response);
      const handle = await source.prepare(request, signal);
      await expect(source.getPayload(handle, signal)).rejects.toMatchObject({
        type: {code: SessionPayloadErrorCode.RESPONSE_MISMATCH},
      });
    }
  );
});
