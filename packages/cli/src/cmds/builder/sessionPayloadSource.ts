import {createHash} from "node:crypto";
import {RLP} from "@ethereumjs/rlp";
import {
  type EngineApiRpcReturnTypes,
  type JsonRpcHttpClient,
  parseExecutionPayload,
  serializePayloadAttributes,
} from "@lodestar/beacon-node";
import type {BuildHandle, BuildRequest, BuiltPayload, PayloadSource} from "@lodestar/builder";
import {CELLS_PER_EXT_BLOB, FIELD_ELEMENTS_PER_BLOB, ForkName} from "@lodestar/params";
import {ssz} from "@lodestar/types";
import {ErrorAborted, LodestarError, isFetchError, toHex} from "@lodestar/utils";

type Rpc = Pick<JsonRpcHttpClient, "fetch">;
type PayloadResponse = EngineApiRpcReturnTypes["engine_getPayloadV6"];

export enum SessionPayloadErrorCode {
  INVALID_SESSION = "SESSION_PAYLOAD_INVALID_SESSION",
  UNKNOWN_HANDLE = "SESSION_PAYLOAD_UNKNOWN_HANDLE",
  RESPONSE_MISMATCH = "SESSION_PAYLOAD_RESPONSE_MISMATCH",
  UNSUPPORTED_FORK = "SESSION_PAYLOAD_UNSUPPORTED_FORK",
}

export class SessionPayloadError extends LodestarError<{code: SessionPayloadErrorCode; reason: string}> {}

/** Private constructor RPC; the constructor validates seals before publishing a ready snapshot. */
export class SessionPayloadSource implements PayloadSource {
  readonly supportsUpdates = true;
  private readonly requests = new Map<string, {request: BuildRequest; latest?: BuiltPayload}>();

  constructor(
    readonly id: string,
    private readonly rpc: (signal: AbortSignal) => Rpc
  ) {}

  async prepare(request: BuildRequest, signal: AbortSignal): Promise<BuildHandle> {
    if (signal.aborted) throw new ErrorAborted();
    if (request.fork !== ForkName.gloas) {
      throw new SessionPayloadError({code: SessionPayloadErrorCode.UNSUPPORTED_FORK, reason: request.fork});
    }
    const call = {
      method: "zbuilder_beginV1",
      params: [request.forkchoiceState.headBlockHash, serializePayloadAttributes(request.payloadAttributes)],
    };
    const result = await this.rpc(signal).fetch<{sessionId: string}, typeof call.params>(call);
    if (typeof result?.sessionId !== "string" || result.sessionId.length === 0 || result.sessionId.length > 128) {
      throw new SessionPayloadError({
        code: SessionPayloadErrorCode.INVALID_SESSION,
        reason: "Invalid constructor handle",
      });
    }
    const handle = {sourceId: this.id, fork: request.fork, payloadId: result.sessionId};
    if (signal.aborted) {
      await this.release(handle, AbortSignal.timeout(1000)).catch(() => {});
      throw new ErrorAborted();
    }
    this.requests.set(result.sessionId, {request});
    return handle;
  }

  async getPayload(handle: BuildHandle, signal: AbortSignal): Promise<BuiltPayload> {
    if (signal.aborted) throw new ErrorAborted();
    const entry = this.requests.get(handle.payloadId);
    if (handle.sourceId !== this.id || !entry || entry.request.fork !== handle.fork) {
      throw new SessionPayloadError({code: SessionPayloadErrorCode.UNKNOWN_HANDLE, reason: handle.payloadId});
    }
    const call = {
      method: "zbuilder_getPayloadV1",
      params: entry.latest ? [handle.payloadId, toHex(entry.latest.executionPayload.blockHash)] : [handle.payloadId],
    };
    let response: PayloadResponse | null;
    try {
      response = await this.rpc(signal).fetch<PayloadResponse | null>(call);
    } catch (error) {
      if (signal.aborted) throw new ErrorAborted();
      if (!isFetchError(error) || (error.code !== "UND_ERR_SOCKET" && error.code !== "ECONNRESET")) throw error;
      // Only this idempotent read may retry a pooled socket closing. Preserve
      // the slot's cancellation deadline and never replay begin/append calls.
      response = await this.rpc(signal).fetch<PayloadResponse | null>(call);
    }
    if (signal.aborted) throw new ErrorAborted();
    if (response === null) {
      if (entry.latest) return entry.latest;
      throw new SessionPayloadError({code: SessionPayloadErrorCode.RESPONSE_MISMATCH, reason: "No initial payload"});
    }
    entry.latest = validateSessionPayload(this.id, entry.request, response);
    return entry.latest;
  }

  async release(handle: BuildHandle, signal: AbortSignal): Promise<void> {
    this.requests.delete(handle.payloadId);
    await this.rpc(signal).fetch({method: "zbuilder_endV1", params: [handle.payloadId]});
  }
}

function validateSessionPayload(sourceId: string, request: BuildRequest, response: PayloadResponse): BuiltPayload {
  const fail = (reason: string): never => {
    throw new SessionPayloadError({code: SessionPayloadErrorCode.RESPONSE_MISMATCH, reason});
  };
  const parsed = parseExecutionPayload(ForkName.gloas, response);
  const executionPayload = parsed.executionPayload as BuiltPayload["executionPayload"];
  const {blobsBundle, executionRequests, executionPayloadValue} = parsed;
  if (
    !blobsBundle ||
    !executionRequests ||
    !("builderDeposits" in executionRequests) ||
    !("builderExits" in executionRequests)
  ) {
    return fail("Missing Gloas payload data");
  }
  const attrs = request.payloadAttributes;
  if (
    toHex(executionPayload.parentHash).toLowerCase() !== request.forkchoiceState.headBlockHash.toLowerCase() ||
    executionPayload.timestamp !== attrs.timestamp ||
    executionPayload.slotNumber !== attrs.slotNumber ||
    toHex(executionPayload.prevRandao) !== toHex(attrs.prevRandao) ||
    toHex(executionPayload.feeRecipient).toLowerCase() !== attrs.suggestedFeeRecipient.toLowerCase() ||
    !ssz.capella.Withdrawals.equals(executionPayload.withdrawals, attrs.withdrawals) ||
    executionPayload.extraData.length > 32
  )
    return fail("Payload differs from the session parent or attributes");
  if (executionPayloadValue < 0n) return fail("Negative payload value");
  // Authentication belongs to the constructor. Check the complete ordered
  // reveal data before this adapter can offer anything for signing. Repeated
  // versioned hashes still require repeated entries in the payload bundle.
  const hashes: Uint8Array[] = [];
  try {
    for (const transaction of executionPayload.transactions) {
      if (transaction[0] !== 3) continue;
      const fields = RLP.decode(transaction.subarray(1));
      if (!Array.isArray(fields) || fields.length !== 14 || !Array.isArray(fields[10]) || !fields[10].length)
        return fail("Invalid blob transaction");
      for (const hash of fields[10]) {
        if (!(hash instanceof Uint8Array) || hash.length !== 32 || hash[0] !== 1)
          return fail("Invalid blob versioned hash");
        hashes.push(hash);
      }
    }
  } catch {
    return fail("Invalid blob transaction encoding");
  }
  if (
    hashes.length > 64 ||
    executionPayload.blobGasUsed !== BigInt(hashes.length * 131072) ||
    blobsBundle.blobs.length !== hashes.length ||
    blobsBundle.commitments.length !== hashes.length ||
    blobsBundle.proofs.length !== hashes.length * CELLS_PER_EXT_BLOB ||
    blobsBundle.blobs.some((blob) => blob.length !== FIELD_ELEMENTS_PER_BLOB * 32) ||
    blobsBundle.commitments.some((commitment) => commitment.length !== 48) ||
    blobsBundle.proofs.some((proof) => proof.length !== 48)
  )
    return fail("Incomplete blob reveal data");
  for (const [i, commitment] of blobsBundle.commitments.entries()) {
    const hash = createHash("sha256").update(commitment).digest();
    hash[0] = 1;
    if (!hash.equals(hashes[i])) return fail("Blob commitment order differs from transactions");
  }
  return {sourceId, fork: request.fork, executionPayload, executionRequests, blobsBundle, executionPayloadValue};
}
