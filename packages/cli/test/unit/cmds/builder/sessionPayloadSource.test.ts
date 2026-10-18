import {createHash} from "node:crypto";
import {once} from "node:events";
import {createServer} from "node:http";
import {RLP} from "@ethereumjs/rlp";
import {describe, expect, it, vi} from "vitest";
import {
  type EngineApiRpcReturnTypes,
  ErrorJsonRpcResponse,
  JsonRpcHttpClient,
  serializePayloadAttributes,
} from "@lodestar/beacon-node";
import {type BuildRequest, PayloadSourceNotReady} from "@lodestar/builder";
import {ForkName} from "@lodestar/params";
import {ssz} from "@lodestar/types";
import {ErrorAborted, FetchError, defer, toHex, toRootHex} from "@lodestar/utils";
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
  function setup(boundedAlternates = false) {
    const rpc = new JsonRpcHttpClient(["http://127.0.0.1:1"]);
    const fetch = vi.spyOn(rpc, "fetch");
    const signals: AbortSignal[] = [];
    const source = new SessionPayloadSource(
      "session",
      (callSignal) => {
        signals.push(callSignal);
        return rpc;
      },
      {boundedAlternates}
    );
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

  describe("bounded alternate reservations", () => {
    const errorCode = (code: SessionPayloadErrorCode) => ({type: {code}});
    const rejectedBegin = (message: string, code = -32000) =>
      new ErrorJsonRpcResponse({jsonrpc: "2.0", id: 1, error: {code, message}}, "zbuilder_beginAlternateV1");

    it("leaves ordinary sources unchanged and refuses alternate work before RPC", async () => {
      const {source, fetch, request} = setup();
      expect(source.supportsBoundedAlternates).toBe(false);
      expect(source.getResources()).toBeUndefined();
      await expect(source.prepare({...request, requireUnleased: true}, signal)).rejects.toMatchObject(
        errorCode(SessionPayloadErrorCode.ALTERNATE_UNSUPPORTED)
      );
      expect(fetch).not.toHaveBeenCalled();
    });

    it("requires an active primary and reserves each pending begin before RPC completes", async () => {
      const {source, fetch, request} = setup(true);
      await expect(source.prepare({...request, requireUnleased: true}, signal)).rejects.toMatchObject(
        errorCode(SessionPayloadErrorCode.CAPACITY)
      );
      const begin = defer<{sessionId: string}>();
      fetch.mockImplementationOnce(() => begin.promise);
      const preparing = source.prepare(request, signal);
      expect(source.getResources()).toEqual({
        capacity: 2,
        preparing: 1,
        active: 0,
        releasing: 0,
        uncertain: 0,
        alternatesDisabled: false,
      });
      await expect(source.prepare({...request, requireUnleased: true}, signal)).rejects.toMatchObject(
        errorCode(SessionPayloadErrorCode.CAPACITY)
      );
      begin.resolve({sessionId: "primary"});
      await preparing;
      const alternate = defer<{sessionId: string; orderGenerationVersion: number; alternateSessionVersion: number}>();
      fetch.mockImplementationOnce(() => alternate.promise);
      const preparingAlternate = source.prepare({...request, requireUnleased: true}, signal);
      expect(source.getResources()).toMatchObject({preparing: 1, active: 1, uncertain: 0});
      expect(fetch).toHaveBeenLastCalledWith({
        method: "zbuilder_beginAlternateV1",
        params: [request.forkchoiceState.headBlockHash, serializePayloadAttributes(request.payloadAttributes)],
      });
      await expect(source.prepare(request, signal)).rejects.toMatchObject(errorCode(SessionPayloadErrorCode.CAPACITY));
      alternate.resolve({sessionId: "alternate", orderGenerationVersion: 0, alternateSessionVersion: 1});
      await preparingAlternate;
      expect(source.getResources()).toMatchObject({preparing: 0, active: 2, uncertain: 0});
      expect(fetch).toHaveBeenCalledTimes(2);
    });

    it("does not reserve a worker if request serialization fails before RPC", async () => {
      const {source, fetch, request} = setup(true);
      Object.defineProperty(request.payloadAttributes, "prevRandao", {
        get() {
          throw new Error("attribute serialization failed");
        },
      });
      await expect(source.prepare(request, signal)).rejects.toThrow("attribute serialization failed");
      expect(fetch).not.toHaveBeenCalled();
      expect(source.getResources()).toMatchObject({preparing: 0, active: 0, releasing: 0, uncertain: 0});
    });

    it("reconciles cleanup failures that appear between primary allocation attempts", async () => {
      const {source, fetch, request} = setup(true);
      fetch
        .mockResolvedValueOnce({sessionId: "old-primary"})
        .mockResolvedValueOnce({sessionId: "alternate", orderGenerationVersion: 0, alternateSessionVersion: 1});
      await source.prepare(request, signal);
      const alternate = await source.prepare({...request, requireUnleased: true}, signal);
      const end = defer<boolean>();
      fetch.mockImplementationOnce(() => end.promise);
      const release = source.release(alternate, signal);
      const failed = expect(release).rejects.toThrow("lost end response");
      await expect(source.prepare(request, signal)).rejects.toMatchObject(errorCode(SessionPayloadErrorCode.CAPACITY));
      end.reject(new Error("lost end response"));
      await failed;
      fetch.mockResolvedValueOnce(true).mockResolvedValueOnce({sessionId: "new-primary"});
      await expect(source.prepare(request, signal)).rejects.toMatchObject(errorCode(SessionPayloadErrorCode.CAPACITY));
      await source.reconcileResources(signal);
      await expect(source.prepare(request, signal)).resolves.toMatchObject({payloadId: "new-primary"});
      expect(fetch.mock.calls.slice(-2).map(([call]) => call.method)).toEqual(["zbuilder_endV1", "zbuilder_beginV1"]);
      expect(source.getResources()).toMatchObject({preparing: 0, active: 2, releasing: 0, uncertain: 0});
    });

    it("uses the free worker for a primary without waiting on an uncertain old handle", async () => {
      const {source, fetch, request} = setup(true);
      fetch.mockResolvedValueOnce({sessionId: "old-primary"});
      const old = await source.prepare(request, signal);
      fetch.mockResolvedValueOnce(false);
      await expect(source.release(old, signal)).rejects.toMatchObject(
        errorCode(SessionPayloadErrorCode.RELEASE_UNCONFIRMED)
      );
      expect(source.getResources()).toMatchObject({active: 0, uncertain: 1});
      const end = defer<boolean>();
      fetch.mockImplementation(async (call) =>
        call.method === "zbuilder_endV1" ? end.promise : {sessionId: "new-primary"}
      );
      await expect(source.prepare(request, signal)).resolves.toMatchObject({payloadId: "new-primary"});
      expect(fetch.mock.calls.map(([call]) => call.method)).toEqual([
        "zbuilder_beginV1",
        "zbuilder_endV1",
        "zbuilder_beginV1",
        "zbuilder_endV1",
      ]);
      expect(source.getResources()).toMatchObject({active: 1, uncertain: 0, preparing: 0, releasing: 1});
      await expect(source.prepare({...request, requireUnleased: true}, signal)).rejects.toMatchObject(
        errorCode(SessionPayloadErrorCode.CAPACITY)
      );
      expect(fetch).toHaveBeenCalledTimes(4);
      end.resolve(true);
      await source.reconcileResources(signal);
      expect(source.getResources()).toMatchObject({active: 1, uncertain: 0, releasing: 0});
    });

    it("serializes concurrent reconciliation over both known handles", async () => {
      const {source, fetch, request} = setup(true);
      fetch.mockResolvedValueOnce({sessionId: "first"}).mockResolvedValueOnce({sessionId: "second"});
      const first = await source.prepare(request, signal);
      const second = await source.prepare(request, signal);
      fetch.mockResolvedValueOnce(false).mockResolvedValueOnce(false);
      await expect(source.release(first, signal)).rejects.toMatchObject(
        errorCode(SessionPayloadErrorCode.RELEASE_UNCONFIRMED)
      );
      await expect(source.release(second, signal)).rejects.toMatchObject(
        errorCode(SessionPayloadErrorCode.RELEASE_UNCONFIRMED)
      );
      const firstStarted = defer<void>();
      const firstEnded = defer<void>();
      const secondStarted = defer<void>();
      const secondEnded = defer<void>();
      const order: string[] = [];
      fetch
        .mockImplementationOnce(async () => {
          order.push("first");
          firstStarted.resolve();
          await firstEnded.promise;
          return true;
        })
        .mockImplementationOnce(async () => {
          order.push("second");
          secondStarted.resolve();
          await secondEnded.promise;
          return true;
        })
        .mockResolvedValueOnce({sessionId: "new-primary"});
      await expect(source.prepare(request, signal)).rejects.toMatchObject(errorCode(SessionPayloadErrorCode.CAPACITY));
      await firstStarted.promise;
      const duplicate = source.reconcileResources(signal);
      expect(order).toEqual(["first"]);
      expect(source.getResources()).toMatchObject({preparing: 0, releasing: 1, uncertain: 1});
      firstEnded.resolve();
      await secondStarted.promise;
      expect(order).toEqual(["first", "second"]);
      expect(source.getResources()).toMatchObject({preparing: 0, releasing: 1, uncertain: 0});
      // The first acknowledgement frees one worker even while the second
      // cleanup is still in progress. No cleanup RPC runs concurrently.
      await expect(source.prepare(request, signal)).resolves.toMatchObject({payloadId: "new-primary"});
      expect(source.getResources()).toMatchObject({active: 1, releasing: 1, uncertain: 0});
      secondEnded.resolve();
      await duplicate;
      expect(source.getResources()).toMatchObject({preparing: 0, active: 1, releasing: 0, uncertain: 0});
    });

    it("aborts a reconciliation waiter without cancelling another caller's end or beginning an orphan", async () => {
      const {source, fetch, request} = setup(true);
      fetch.mockResolvedValueOnce({sessionId: "old"}).mockResolvedValueOnce({sessionId: "second"});
      const handle = await source.prepare(request, signal);
      const second = await source.prepare(request, signal);
      fetch.mockResolvedValueOnce(false).mockResolvedValueOnce(false);
      await expect(source.release(handle, signal)).rejects.toMatchObject(
        errorCode(SessionPayloadErrorCode.RELEASE_UNCONFIRMED)
      );
      await expect(source.release(second, signal)).rejects.toMatchObject(
        errorCode(SessionPayloadErrorCode.RELEASE_UNCONFIRMED)
      );
      const started = defer<void>();
      const ended = defer<void>();
      fetch
        .mockImplementationOnce(async () => {
          started.resolve();
          await ended.promise;
          return true;
        })
        .mockResolvedValueOnce(true);
      const reconciliation = source.reconcileResources(signal);
      await started.promise;
      const controller = new AbortController();
      const waiting = source.reconcileResources(controller.signal);
      const aborted = expect(waiting).rejects.toBeInstanceOf(ErrorAborted);
      controller.abort();
      await aborted;
      expect(source.getResources()).toMatchObject({preparing: 0, releasing: 1, uncertain: 1});
      ended.resolve();
      await reconciliation;
      expect(fetch.mock.calls.map(([call]) => call.method)).toEqual([
        "zbuilder_beginV1",
        "zbuilder_beginV1",
        "zbuilder_endV1",
        "zbuilder_endV1",
        "zbuilder_endV1",
        "zbuilder_endV1",
      ]);
      expect(source.getResources()).toMatchObject({
        preparing: 0,
        active: 0,
        releasing: 0,
        uncertain: 0,
        alternatesDisabled: false,
      });
    });

    it("keeps reconciliation serialized when an aborted end delivers its acknowledgement late", async () => {
      const {source, fetch, request} = setup(true);
      fetch.mockResolvedValueOnce({sessionId: "first"}).mockResolvedValueOnce({sessionId: "second"});
      const first = await source.prepare(request, signal);
      const second = await source.prepare(request, signal);
      fetch.mockResolvedValueOnce(false).mockResolvedValueOnce(false);
      await expect(source.release(first, signal)).rejects.toMatchObject(
        errorCode(SessionPayloadErrorCode.RELEASE_UNCONFIRMED)
      );
      await expect(source.release(second, signal)).rejects.toMatchObject(
        errorCode(SessionPayloadErrorCode.RELEASE_UNCONFIRMED)
      );
      const started = defer<void>();
      const acknowledged = defer<void>();
      fetch.mockImplementationOnce(async () => {
        started.resolve();
        await acknowledged.promise;
        return true;
      });
      const controller = new AbortController();
      const cleanup = source.reconcileResources(controller.signal);
      const aborted = expect(cleanup).rejects.toBeInstanceOf(ErrorAborted);
      await started.promise;
      controller.abort();
      await aborted;
      const joining = source.reconcileResources(signal);
      const joiningFailed = expect(joining).rejects.toThrow();
      expect(fetch).toHaveBeenCalledTimes(5);
      expect(source.getResources()).toMatchObject({active: 0, releasing: 1, uncertain: 1});
      acknowledged.resolve();
      await joiningFailed;
      expect(fetch).toHaveBeenCalledTimes(5);
      expect(source.getResources()).toMatchObject({active: 0, releasing: 0, uncertain: 1});
      fetch.mockResolvedValueOnce(true);
      await source.reconcileResources(signal);
      expect(source.getResources()).toMatchObject({active: 0, releasing: 0, uncertain: 0});
    });

    it("keeps releasing and failed cleanup occupied until an exact acknowledgement", async () => {
      const {source, fetch, request} = setup(true);
      fetch
        .mockResolvedValueOnce({sessionId: "primary"})
        .mockResolvedValueOnce({sessionId: "alternate", orderGenerationVersion: 0, alternateSessionVersion: 1});
      await source.prepare(request, signal);
      const alternate = await source.prepare({...request, requireUnleased: true}, signal);
      const end = defer<boolean>();
      fetch.mockImplementationOnce(() => end.promise);
      const released = source.release(alternate, signal);
      const duplicate = source.release(alternate, signal);
      const rejected = expect(released).rejects.toThrow("lost reply");
      const rejectedDuplicate = expect(duplicate).rejects.toThrow("lost reply");
      expect(source.getResources()).toMatchObject({active: 1, releasing: 1, uncertain: 0});
      await expect(source.prepare(request, signal)).rejects.toMatchObject(errorCode(SessionPayloadErrorCode.CAPACITY));
      end.reject(new Error("lost reply"));
      await Promise.all([rejected, rejectedDuplicate]);
      expect(fetch).toHaveBeenCalledTimes(3);
      expect(source.getResources()).toMatchObject({active: 1, releasing: 0, uncertain: 1});
      fetch.mockResolvedValueOnce(true);
      await source.reconcileResources(signal);
      expect(source.getResources()).toMatchObject({active: 1, releasing: 0, uncertain: 0});
      fetch.mockResolvedValueOnce({sessionId: "new-primary"});
      await expect(source.prepare(request, signal)).resolves.toMatchObject({payloadId: "new-primary"});
    });

    it.each([false, null, {}, "true"])("does not treat end=%j as proof of release", async (reply) => {
      const {source, fetch, request} = setup(true);
      fetch.mockResolvedValueOnce({sessionId: "primary"});
      const handle = await source.prepare(request, signal);
      fetch.mockResolvedValueOnce(reply);
      await expect(source.release(handle, signal)).rejects.toMatchObject(
        errorCode(SessionPayloadErrorCode.RELEASE_UNCONFIRMED)
      );
      expect(source.getResources()).toMatchObject({active: 0, releasing: 0, uncertain: 1});
    });

    it("records a synchronous release transport failure as uncertain", async () => {
      const rpc = new JsonRpcHttpClient(["http://127.0.0.1:1"]);
      const fetch = vi.spyOn(rpc, "fetch").mockResolvedValueOnce({sessionId: "primary"});
      const factory = vi.fn(() => rpc);
      const source = new SessionPayloadSource("session", factory, {boundedAlternates: true});
      const handle = await source.prepare(buildRequest(), signal);
      factory.mockImplementationOnce(() => {
        throw new Error("transport unavailable");
      });
      await expect(source.release(handle, signal)).rejects.toThrow("transport unavailable");
      expect(source.getResources()).toMatchObject({active: 0, releasing: 0, uncertain: 1});
      fetch.mockResolvedValueOnce(true);
      await source.reconcileResources(signal);
      expect(source.getResources()).toMatchObject({uncertain: 0});
    });

    it("reconciles an absent instance-scoped handle without releasing another instance", async () => {
      const {source, fetch, request} = setup(true);
      const oldId = `native-${"ab".repeat(16)}-0-17`;
      const newId = `native-${"cd".repeat(16)}-0-17`;
      fetch.mockResolvedValueOnce({sessionId: oldId}).mockResolvedValueOnce({sessionId: newId});
      const old = await source.prepare(request, signal);
      const current = await source.prepare(request, signal);
      fetch.mockRejectedValueOnce(transportError("ECONNRESET"));
      await expect(source.release(old, signal)).rejects.toBeInstanceOf(FetchError);
      const absent = new ErrorJsonRpcResponse(
        {jsonrpc: "2.0", id: 1, error: {code: -32000, message: "UnknownSession"}},
        "zbuilder_endV1"
      );
      fetch.mockRejectedValueOnce(absent);
      await source.reconcileResources(signal);
      expect(source.getResources()).toMatchObject({active: 1, releasing: 0, uncertain: 0});
      fetch.mockResolvedValueOnce(payloadResponse(request));
      await expect(source.getPayload(current, signal)).resolves.toMatchObject({sourceId: "session"});
      expect(
        fetch.mock.calls.filter(([call]) => call.method === "zbuilder_endV1").map(([call]) => call.params)
      ).toEqual([[oldId], [oldId]]);
    });

    it.each([
      ["foreign-handle", -32000, "UnknownSession"],
      [`native-${"ab".repeat(16)}-2-17`, -32000, "UnknownSession"],
      [`native-${"ab".repeat(16)}-0-18446744073709551616`, -32000, "UnknownSession"],
      [`native-${"ab".repeat(16)}-0-17`, -32601, "UnknownSession"],
      [`native-${"ab".repeat(16)}-0-17`, -32000, "WorkerRejected"],
    ] as const)("keeps unconfirmed end error occupied for %s/%i/%s", async (payloadId, code, message) => {
      const {source, fetch, request} = setup(true);
      fetch.mockResolvedValueOnce({sessionId: payloadId});
      const handle = await source.prepare(request, signal);
      fetch.mockRejectedValueOnce(
        new ErrorJsonRpcResponse({jsonrpc: "2.0", id: 1, error: {code, message}}, "zbuilder_endV1")
      );
      await expect(source.release(handle, signal)).rejects.toBeInstanceOf(ErrorJsonRpcResponse);
      expect(source.getResources()).toMatchObject({active: 0, releasing: 0, uncertain: 1});
    });

    it("recovers after two real refused begin connections without losing capacity", async () => {
      const methods: string[] = [];
      const server = createServer(async (req, res) => {
        const chunks: Buffer[] = [];
        for await (const chunk of req) chunks.push(Buffer.from(chunk));
        const call = JSON.parse(Buffer.concat(chunks).toString()) as {id: number; method: string};
        methods.push(call.method);
        res.setHeader("content-type", "application/json");
        res.end(JSON.stringify({jsonrpc: "2.0", id: call.id, result: {sessionId: `restored-${methods.length}`}}));
      });
      server.listen(0, "127.0.0.1");
      await once(server, "listening");
      const address = server.address();
      if (!address || typeof address === "string") throw new Error("Missing listener address");
      await new Promise<void>((resolve, reject) => server.close((error) => (error ? reject(error) : resolve())));
      const source = new SessionPayloadSource(
        "socket",
        (signal) => new JsonRpcHttpClient([`http://127.0.0.1:${address.port}`], {signal, timeout: 1000}),
        {boundedAlternates: true}
      );
      for (let i = 0; i < 2; i++) {
        await expect(source.prepare(buildRequest(), signal)).rejects.toMatchObject({code: "ECONNREFUSED"});
        expect(source.getResources()).toMatchObject({preparing: 0, active: 0, uncertain: 0, alternatesDisabled: false});
      }
      server.listen(address.port, "127.0.0.1");
      await once(server, "listening");
      try {
        await source.prepare(buildRequest(), signal);
        await source.prepare(buildRequest(), signal);
        expect(source.getResources()).toMatchObject({active: 2, preparing: 0, uncertain: 0, alternatesDisabled: false});
        expect(methods).toEqual(["zbuilder_beginV1", "zbuilder_beginV1"]);
      } finally {
        server.closeAllConnections();
        await new Promise<void>((resolve, reject) => server.close((error) => (error ? reject(error) : resolve())));
      }
    });

    it("keeps a cancelled HTTP begin tracked until its late handle is physically released", async () => {
      const beginEntered = defer<void>();
      const beginReply = defer<void>();
      const endEntered = defer<void>();
      const endReply = defer<void>();
      const methods: string[] = [];
      const id = `native-${"ab".repeat(16)}-1-19`;
      const server = createServer(async (req, res) => {
        const chunks: Buffer[] = [];
        for await (const chunk of req) chunks.push(Buffer.from(chunk));
        const call = JSON.parse(Buffer.concat(chunks).toString()) as {id: number; method: string; params: unknown[]};
        methods.push(call.method);
        if (call.method === "zbuilder_beginV1") {
          beginEntered.resolve();
          await beginReply.promise;
        } else {
          expect(call).toMatchObject({method: "zbuilder_endV1", params: [id]});
          endEntered.resolve();
          await endReply.promise;
        }
        res.setHeader("content-type", "application/json");
        res.end(
          JSON.stringify({
            jsonrpc: "2.0",
            id: call.id,
            result: call.method === "zbuilder_beginV1" ? {sessionId: id} : true,
          })
        );
      });
      server.listen(0, "127.0.0.1");
      await once(server, "listening");
      const controller = new AbortController();
      const callSignals: AbortSignal[] = [];
      try {
        const address = server.address();
        if (!address || typeof address === "string") throw new Error("Missing listener address");
        const source = new SessionPayloadSource(
          "socket",
          (signal) => {
            callSignals.push(signal);
            return new JsonRpcHttpClient([`http://127.0.0.1:${address.port}`], {signal, timeout: 1000});
          },
          {boundedAlternates: true}
        );
        const prepare = source.prepare(buildRequest(), controller.signal);
        const cancelled = expect(prepare).rejects.toBeInstanceOf(ErrorAborted);
        await beginEntered.promise;
        controller.abort();
        expect(callSignals[0].aborted).toBe(false);
        expect(source.getResources()).toMatchObject({preparing: 1, active: 0, releasing: 0, uncertain: 0});
        beginReply.resolve();
        await endEntered.promise;
        expect(source.getResources()).toMatchObject({preparing: 0, active: 0, releasing: 1, uncertain: 0});
        endReply.resolve();
        await cancelled;
        expect(source.getResources()).toMatchObject({
          preparing: 0,
          active: 0,
          releasing: 0,
          uncertain: 0,
          alternatesDisabled: false,
        });
        expect(methods).toEqual(["zbuilder_beginV1", "zbuilder_endV1"]);
      } finally {
        beginReply.resolve();
        endReply.resolve();
        server.closeAllConnections();
        await new Promise<void>((resolve, reject) => server.close((error) => (error ? reject(error) : resolve())));
      }
    });

    it.each(["unsupported", "lost-response"])("tracks real HTTP alternate begin outcome: %s", async (fault) => {
      const request = buildRequest();
      const controller = new AbortController();
      const methods: string[] = [];
      const server = createServer(async (req, res) => {
        const chunks: Buffer[] = [];
        for await (const chunk of req) chunks.push(Buffer.from(chunk));
        const call = JSON.parse(Buffer.concat(chunks).toString()) as {id: number; method: string};
        methods.push(call.method);
        if (call.method === "zbuilder_beginAlternateV1" && fault === "lost-response") {
          // The constructor received a non-idempotent allocation request.
          // Losing its reply cannot prove whether allocation took place.
          controller.abort();
          return;
        }
        res.setHeader("content-type", "application/json");
        res.end(
          JSON.stringify({
            jsonrpc: "2.0",
            id: call.id,
            ...(call.method === "zbuilder_beginAlternateV1"
              ? {error: {code: -32000, message: "UnknownSession"}}
              : {result: {sessionId: "primary"}}),
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
          (signal) => new JsonRpcHttpClient([`http://127.0.0.1:${address.port}`], {signal, timeout: 1000}),
          {boundedAlternates: true}
        );
        await source.prepare(request, signal);
        await expect(source.prepare({...request, requireUnleased: true}, controller.signal)).rejects.toThrow();
        expect(source.getResources()).toMatchObject({
          active: 1,
          preparing: 0,
          uncertain: fault === "lost-response" ? 1 : 0,
          alternatesDisabled: true,
        });
        await source.reconcileResources(signal);
        expect(methods).toEqual(["zbuilder_beginV1", "zbuilder_beginAlternateV1"]);
      } finally {
        server.closeAllConnections();
        await new Promise<void>((resolve, reject) => server.close((error) => (error ? reject(error) : resolve())));
      }
    });

    it("retains an unknown begin outcome and never infers recovery from a session listing", async () => {
      const {source, fetch, request} = setup(true);
      fetch.mockResolvedValueOnce({sessionId: "primary"});
      const primary = await source.prepare(request, signal);
      fetch.mockRejectedValueOnce(new Error("connection timed out after send"));
      await expect(source.prepare({...request, requireUnleased: true}, signal)).rejects.toThrow("connection timed out");
      expect(source.getResources()).toMatchObject({preparing: 0, active: 1, uncertain: 1, alternatesDisabled: true});
      await source.reconcileResources(signal);
      expect(fetch).toHaveBeenCalledTimes(2);
      fetch.mockResolvedValueOnce(true);
      await source.release(primary, signal);
      expect(source.getResources()).toMatchObject({active: 0, uncertain: 1, alternatesDisabled: true});
      await expect(source.prepare({...request, requireUnleased: true}, signal)).rejects.toMatchObject(
        errorCode(SessionPayloadErrorCode.ALTERNATE_UNSUPPORTED)
      );
      fetch.mockResolvedValueOnce({sessionId: "next-primary"});
      await source.prepare(request, signal);
      await expect(source.prepare(request, signal)).rejects.toMatchObject(errorCode(SessionPayloadErrorCode.CAPACITY));
    });

    it.each([
      ["UnknownSession", -32000],
      ["AlternateLeasesUnsupported", -32000],
      ["UnknownMethod", -32000],
      ["Method not found", -32601],
    ] as const)("disables alternates on definite unsupported native response %s", async (reason, code) => {
      const {source, fetch, request} = setup(true);
      fetch.mockResolvedValueOnce({sessionId: "primary"});
      await source.prepare(request, signal);
      fetch.mockRejectedValueOnce(rejectedBegin(reason, code));
      await expect(source.prepare({...request, requireUnleased: true}, signal)).rejects.toThrow(reason);
      expect(source.getResources()).toMatchObject({active: 1, uncertain: 0, alternatesDisabled: true});
    });

    it.each(["WorkerRejected", "AlternateSessionLimit", "SessionLimit", "FileNotFound", "SyntaxError", "OutOfMemory"])(
      "allows a fresh attempt after definite native rejection %s without charging an orphan",
      async (reason) => {
        const {source, fetch, request} = setup(true);
        fetch.mockRejectedValueOnce(rejectedBegin(reason));
        await expect(source.prepare(request, signal)).rejects.toThrow(reason);
        expect(source.getResources()).toMatchObject({active: 0, uncertain: 0, alternatesDisabled: false});
        fetch.mockResolvedValueOnce({sessionId: "primary"});
        await source.prepare(request, signal);
        fetch.mockRejectedValueOnce(rejectedBegin(reason));
        await expect(source.prepare({...request, requireUnleased: true}, signal)).rejects.toThrow(reason);
        expect(source.getResources()).toMatchObject({active: 1, uncertain: 0, alternatesDisabled: false});
      }
    );

    it.each([{orderGenerationVersion: 1, alternateSessionVersion: 1}, {orderGenerationVersion: 0}, {}])(
      "cleans and disables an unconfirmed alternate capability %j",
      async (capability) => {
        const {source, fetch, request} = setup(true);
        fetch.mockResolvedValueOnce({sessionId: "primary"});
        await source.prepare(request, signal);
        fetch.mockResolvedValueOnce({sessionId: "alternate", ...capability}).mockResolvedValueOnce(true);
        await expect(source.prepare({...request, requireUnleased: true}, signal)).rejects.toMatchObject(
          errorCode(SessionPayloadErrorCode.ALTERNATE_UNSUPPORTED)
        );
        expect(fetch).toHaveBeenLastCalledWith({method: "zbuilder_endV1", params: ["alternate"]});
        expect(source.getResources()).toMatchObject({active: 1, uncertain: 0, alternatesDisabled: true});
      }
    );

    it("never ends an existing primary when an invalid begin duplicates its handle", async () => {
      const {source, fetch, request} = setup(true);
      fetch.mockResolvedValue({sessionId: "primary"});
      await source.prepare(request, signal);
      await expect(source.prepare({...request, requireUnleased: true}, signal)).rejects.toMatchObject(
        errorCode(SessionPayloadErrorCode.INVALID_SESSION)
      );
      expect(source.getResources()).toMatchObject({active: 1, uncertain: 1, alternatesDisabled: true});
      expect(fetch.mock.calls.map(([call]) => call.method)).toEqual(["zbuilder_beginV1", "zbuilder_beginAlternateV1"]);
    });

    it("retains late-cancelled known handles when their cleanup fails", async () => {
      const {source, fetch, request} = setup(true);
      const controller = new AbortController();
      fetch
        .mockImplementationOnce(async () => {
          controller.abort();
          return {sessionId: "late"};
        })
        .mockRejectedValueOnce(new Error("end lost"));
      await expect(source.prepare(request, controller.signal)).rejects.toBeInstanceOf(ErrorAborted);
      expect(source.getResources()).toMatchObject({preparing: 0, active: 0, uncertain: 1});
      fetch.mockResolvedValueOnce(true);
      await source.reconcileResources(signal);
      expect(source.getResources()).toMatchObject({uncertain: 0});
    });

    it("does not invalidate a valid lease for a foreign release handle", async () => {
      const {source, fetch, request} = setup(true);
      fetch.mockResolvedValueOnce({sessionId: "primary", orderGenerationVersion: 1});
      const handle = await source.prepare(request, signal);
      fetch.mockResolvedValueOnce({generation: "0x0", ready: true, leaseMs: 250, payload: payloadResponse(request)});
      const payload = await source.getPayload(handle, signal);
      await expect(source.release({...handle, sourceId: "other"}, signal)).rejects.toMatchObject(
        errorCode(SessionPayloadErrorCode.UNKNOWN_HANDLE)
      );
      expect(payload.offerEligibility?.lease.ready).toBe(true);
      expect(fetch).toHaveBeenCalledTimes(2);
    });

    it("cannot revive a snapshot when its read finishes during unconfirmed cleanup", async () => {
      const {source, fetch, request} = setup(true);
      fetch.mockResolvedValueOnce({sessionId: "primary"});
      const handle = await source.prepare(request, signal);
      const read = defer<PayloadResponse>();
      fetch.mockImplementationOnce(() => read.promise).mockRejectedValueOnce(new Error("end lost"));
      const payload = source.getPayload(handle, signal);
      const rejected = expect(payload).rejects.toMatchObject(errorCode(SessionPayloadErrorCode.UNKNOWN_HANDLE));
      await expect(source.release(handle, signal)).rejects.toThrow("end lost");
      read.resolve(payloadResponse(request));
      await rejected;
      expect(source.getResources()).toMatchObject({active: 0, uncertain: 1});
    });
  });
});
