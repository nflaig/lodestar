import {createHash} from "node:crypto";
import {RLP} from "@ethereumjs/rlp";
import {
  type EngineApiRpcReturnTypes,
  ErrorJsonRpcResponse,
  type JsonRpcHttpClient,
  parseExecutionPayload,
  serializePayloadAttributes,
} from "@lodestar/beacon-node";
import {
  type BuildHandle,
  type BuildRequest,
  type BuiltPayload,
  type OfferLease,
  type PayloadSource,
  PayloadSourceNotReady,
  type PayloadSourceResources,
} from "@lodestar/builder";
import {CELLS_PER_EXT_BLOB, FIELD_ELEMENTS_PER_BLOB, ForkName} from "@lodestar/params";
import {ssz} from "@lodestar/types";
import {ErrorAborted, LodestarError, type Logger, isFetchError, sleep, toHex, withTimeout} from "@lodestar/utils";

type Rpc = Pick<JsonRpcHttpClient, "fetch">;
type PayloadResponse = EngineApiRpcReturnTypes["engine_getPayloadV6"];
type LeasedResponse = {generation: string; leaseMs: number; ready: boolean; payload: PayloadResponse | null};
// Native operations allow up to 15s; retain the reply through bounded failure
// cleanup too. The orchestrator's build deadline still detaches independently.
const BEGIN_RPC_TIMEOUT_MS = 20_000;

export enum SessionPayloadErrorCode {
  INVALID_SESSION = "SESSION_PAYLOAD_INVALID_SESSION",
  UNKNOWN_HANDLE = "SESSION_PAYLOAD_UNKNOWN_HANDLE",
  RESPONSE_MISMATCH = "SESSION_PAYLOAD_RESPONSE_MISMATCH",
  UNSUPPORTED_FORK = "SESSION_PAYLOAD_UNSUPPORTED_FORK",
  ALTERNATE_UNSUPPORTED = "SESSION_PAYLOAD_ALTERNATE_UNSUPPORTED",
  CAPACITY = "SESSION_PAYLOAD_CAPACITY",
  RELEASE_UNCONFIRMED = "SESSION_PAYLOAD_RELEASE_UNCONFIRMED",
}

export class SessionPayloadError extends LodestarError<{code: SessionPayloadErrorCode; reason: string}> {}

type SessionEntry = {
  request: BuildRequest;
  latest?: BuiltPayload;
  lease?: OfferLease;
  state: "active" | "releasing" | "uncertain";
  release?: Promise<void>;
  reserved?: boolean;
};

type AlternateDisabledReason =
  | "ambiguous_begin"
  | "invalid_begin_handle"
  | "unsupported_alternate_method"
  | "unconfirmed_alternate_capability";

/** Private constructor RPC; the constructor validates seals before publishing a ready snapshot. */
export class SessionPayloadSource implements PayloadSource {
  readonly supportsUpdates = true;
  private readonly requests = new Map<string, SessionEntry>();
  private preparing = 0;
  private orphaned = 0;
  private alternatesDisabled = false;
  private reconciliation: Promise<void> | undefined;

  constructor(
    readonly id: string,
    private readonly rpc: (signal: AbortSignal) => Rpc,
    private readonly options: {
      boundedAlternates?: boolean;
      beginReservations?: boolean;
      logger?: {warn: Logger["warn"]};
    } = {}
  ) {}

  get supportsBoundedAlternates(): boolean {
    return this.options.boundedAlternates === true;
  }

  private get tracksSessions(): boolean {
    return this.supportsBoundedAlternates || this.options.beginReservations === true;
  }

  getResources(): PayloadSourceResources | undefined {
    if (!this.tracksSessions) return undefined;
    const result: PayloadSourceResources = {
      capacity: 2,
      preparing: this.preparing,
      active: 0,
      releasing: 0,
      uncertain: this.orphaned,
      alternatesDisabled: this.alternatesDisabled,
    };
    for (const entry of this.requests.values()) result[entry.state]++;
    return result;
  }

  private warnAlternatesDisabled(
    request: BuildRequest,
    signal: AbortSignal,
    reason: AlternateDisabledReason,
    method = request.requireUnleased ? "zbuilder_beginAlternateV1" : "zbuilder_beginV1"
  ): void {
    try {
      const logger = this.options.logger;
      if (!logger) return;
      const resources = this.getResources();
      logger.warn("Constructor alternate admission disabled", {
        code: "SESSION_PAYLOAD_ALTERNATES_DISABLED",
        reason,
        method,
        slot: request.payloadAttributes.slotNumber,
        parentBlockHash: request.forkchoiceState.headBlockHash,
        parentBlockRoot: toHex(request.payloadAttributes.parentBeaconBlockRoot),
        callerAborted: signal.aborted,
        orphaned: this.orphaned,
        capacity: resources?.capacity,
        active: resources?.active,
        preparing: resources?.preparing,
        releasing: resources?.releasing,
        uncertain: resources?.uncertain,
        alternatesDisabled: resources?.alternatesDisabled,
      });
    } catch {
      // Diagnostic failures must not change admission or session ownership.
    }
  }

  async reconcileResources(signal: AbortSignal): Promise<void> {
    if (!this.tracksSessions) return;
    let pending = this.reconciliation;
    if (pending === undefined) {
      const cleanupSignal = AbortSignal.any([signal, AbortSignal.timeout(6000)]);
      pending = (async () => {
        for (const [payloadId, entry] of this.requests) {
          cleanupSignal.throwIfAborted();
          if (entry.state === "uncertain") {
            await this.release({sourceId: this.id, fork: entry.request.fork, payloadId}, cleanupSignal);
          }
        }
      })().finally(() => {
        this.reconciliation = undefined;
      });
      this.reconciliation = pending;
    }
    await withTimeout(() => pending, 6000, signal);
  }

  async prepare(request: BuildRequest, signal: AbortSignal): Promise<BuildHandle> {
    if (signal.aborted) throw new ErrorAborted();
    if (request.fork !== ForkName.gloas) {
      throw new SessionPayloadError({code: SessionPayloadErrorCode.UNSUPPORTED_FORK, reason: request.fork});
    }
    const bounded = this.tracksSessions;
    const alternate = request.requireUnleased === true;
    if (alternate && (!this.supportsBoundedAlternates || this.alternatesDisabled))
      throw new SessionPayloadError({
        code: SessionPayloadErrorCode.ALTERNATE_UNSUPPORTED,
        reason: "Alternates unavailable",
      });
    const call = {
      method: alternate ? "zbuilder_beginAlternateV1" : "zbuilder_beginV1",
      params: [request.forkchoiceState.headBlockHash, serializePayloadAttributes(request.payloadAttributes)],
    };
    const before = this.getResources();
    // Cleanup owns its reservation independently of a short primary deadline.
    // A primary may use a free worker without waiting for every old end reply.
    if (!alternate && before && before.uncertain > this.orphaned && this.reconciliation === undefined) {
      void this.reconcileResources(AbortSignal.timeout(6000)).catch(() => {});
    }
    if (signal.aborted) throw new ErrorAborted();
    const resources = this.getResources();
    if (
      resources &&
      (resources.preparing + resources.active + resources.releasing + resources.uncertain >= resources.capacity ||
        (alternate &&
          (resources.active !== 1 ||
            resources.preparing !== 0 ||
            resources.releasing !== 0 ||
            resources.uncertain !== 0)))
    )
      throw new SessionPayloadError({
        code: SessionPayloadErrorCode.CAPACITY,
        reason: "Constructor resources occupied",
      });
    if (this.options.beginReservations) return this.prepareReserved(request, signal);
    const beginRpc = this.rpc(bounded ? AbortSignal.timeout(BEGIN_RPC_TIMEOUT_MS) : signal);
    if (bounded) this.preparing++;
    let result: {sessionId: string; orderGenerationVersion?: number; alternateSessionVersion?: number};
    let disabledReason: AlternateDisabledReason | undefined;
    try {
      // Cancelling the build must not discard a late allocation handle. Keep
      // one bounded request alive, then end any handle the caller no longer owns.
      result = bounded
        ? await beginRpc.fetch<typeof result, typeof call.params>(call, {timeout: BEGIN_RPC_TIMEOUT_MS})
        : await beginRpc.fetch<typeof result, typeof call.params>(call);
    } catch (error) {
      if (bounded) {
        const rejection = error instanceof ErrorJsonRpcResponse ? error.response.error : undefined;
        // Native begin dispatch cleans its allocation before returning an RPC
        // error. Refused connections never send the non-idempotent request.
        const definite = rejection !== undefined || (isFetchError(error) && error.code === "ECONNREFUSED");
        if (!definite) {
          this.orphaned++;
          this.alternatesDisabled = true;
          disabledReason = "ambiguous_begin";
        } else if (
          alternate &&
          rejection &&
          (rejection.code === -32601 ||
            ["UnknownSession", "UnknownMethod", "MethodNotFound", "AlternateLeasesUnsupported"].includes(
              rejection.message
            ))
        ) {
          this.alternatesDisabled = true;
          disabledReason = "unsupported_alternate_method";
        }
      }
      throw error;
    } finally {
      if (bounded) this.preparing--;
      if (disabledReason !== undefined) this.warnAlternatesDisabled(request, signal, disabledReason);
    }
    if (
      typeof result?.sessionId !== "string" ||
      result.sessionId.length === 0 ||
      result.sessionId.length > 128 ||
      (bounded && this.requests.has(result.sessionId))
    ) {
      if (bounded) {
        this.orphaned++;
        this.alternatesDisabled = true;
        this.warnAlternatesDisabled(request, signal, "invalid_begin_handle");
      }
      throw new SessionPayloadError({
        code: SessionPayloadErrorCode.INVALID_SESSION,
        reason: "Invalid constructor handle",
      });
    }
    const handle = {sourceId: this.id, fork: request.fork, payloadId: result.sessionId};
    const entry: SessionEntry = {
      request,
      state: "active",
      lease:
        result.orderGenerationVersion === 1
          ? {generation: "", validUntil: 0, validUntilUnixMs: 0, ready: false}
          : undefined,
    };
    this.requests.set(result.sessionId, entry);
    if (signal.aborted) {
      await this.release(handle, AbortSignal.timeout(bounded ? 6000 : 1000)).catch(() => {});
      throw new ErrorAborted();
    }
    if (alternate && (result.orderGenerationVersion !== 0 || result.alternateSessionVersion !== 1)) {
      this.alternatesDisabled = true;
      this.warnAlternatesDisabled(request, signal, "unconfirmed_alternate_capability");
      await this.release(handle, AbortSignal.timeout(6000)).catch(() => {});
      throw new SessionPayloadError({
        code: SessionPayloadErrorCode.ALTERNATE_UNSUPPORTED,
        reason: "Constructor did not confirm an unleased alternate session",
      });
    }
    if (
      result.orderGenerationVersion !== undefined &&
      result.orderGenerationVersion !== 0 &&
      result.orderGenerationVersion !== 1
    ) {
      await this.release(handle, AbortSignal.timeout(bounded ? 6000 : 1000)).catch(() => {});
      throw new SessionPayloadError({
        code: SessionPayloadErrorCode.RESPONSE_MISMATCH,
        reason: "Unsupported order generation protocol",
      });
    }
    return handle;
  }

  /** Reserve an allocation identity before sending the non-idempotent begin.
   * A lost reserve reply cannot allocate a worker. Once known, the ticket stays
   * counted until the constructor proves neither queued nor attached work can
   * survive. A timeout or a changed server instance is not that proof.
   */
  private async prepareReserved(request: BuildRequest, signal: AbortSignal): Promise<BuildHandle> {
    this.preparing++;
    let ticket: string | undefined;
    let method = "zbuilder_reserveBeginV1";
    let disabledReason: AlternateDisabledReason | undefined;
    try {
      const reservation = await this.rpc(signal).fetch<{reservationId: string; beginReservationVersion: number}>(
        {method: "zbuilder_reserveBeginV1", params: [request.requireUnleased === true]},
        {timeout: 1000}
      );
      if (
        reservation?.beginReservationVersion !== 1 ||
        typeof reservation.reservationId !== "string" ||
        !/^begin-[0-9a-f]{32}-[1-9][0-9]{0,19}$/.test(reservation.reservationId) ||
        BigInt(reservation.reservationId.slice(39)) > 0xffffffffffffffffn ||
        this.requests.has(reservation.reservationId)
      )
        throw new SessionPayloadError({
          code: SessionPayloadErrorCode.INVALID_SESSION,
          reason: "Invalid constructor reservation",
        });
      ticket = reservation.reservationId;
      if (signal.aborted) throw new ErrorAborted();
      method = "zbuilder_beginReservedV1";
      const result = await this.rpc(AbortSignal.timeout(BEGIN_RPC_TIMEOUT_MS)).fetch<{
        sessionId: string;
        beginReservationVersion: number;
        orderGenerationVersion: number;
        alternateSessionVersion: number;
      }>(
        {
          method: "zbuilder_beginReservedV1",
          params: [
            ticket,
            request.forkchoiceState.headBlockHash,
            serializePayloadAttributes(request.payloadAttributes),
          ],
        },
        {timeout: BEGIN_RPC_TIMEOUT_MS}
      );
      if (
        result?.sessionId !== ticket ||
        result.beginReservationVersion !== 1 ||
        (result.orderGenerationVersion !== 0 && result.orderGenerationVersion !== 1) ||
        (!request.requireUnleased && result.alternateSessionVersion !== 0) ||
        (request.requireUnleased && (result.orderGenerationVersion !== 0 || result.alternateSessionVersion !== 1))
      ) {
        if (request.requireUnleased) disabledReason = "unconfirmed_alternate_capability";
        throw new SessionPayloadError({
          code: SessionPayloadErrorCode.INVALID_SESSION,
          reason: "Constructor did not confirm reserved session capabilities",
        });
      }
      this.requests.set(ticket, {
        request,
        state: "active",
        reserved: true,
        lease:
          result.orderGenerationVersion === 1
            ? {generation: "", validUntil: 0, validUntilUnixMs: 0, ready: false}
            : undefined,
      });
      if (signal.aborted) throw new ErrorAborted();
      return {sourceId: this.id, fork: request.fork, payloadId: ticket};
    } catch (error) {
      if (request.requireUnleased && error instanceof ErrorJsonRpcResponse) {
        const rejection = error.response.error;
        if (
          rejection.code === -32601 ||
          ["MethodNotSupported", "UnknownMethod", "MethodNotFound", "AlternateLeasesUnsupported"].includes(
            rejection.message
          )
        )
          disabledReason = "unsupported_alternate_method";
      }
      if (disabledReason !== undefined) {
        this.alternatesDisabled = true;
        this.warnAlternatesDisabled(request, signal, disabledReason, method);
      }
      if (ticket !== undefined) {
        const entry: SessionEntry = this.requests.get(ticket) ?? {request, state: "uncertain", reserved: true};
        entry.state = "uncertain";
        this.requests.set(ticket, entry);
        try {
          this.options.logger?.warn("Constructor begin awaiting confirmed cancellation", {
            code: "SESSION_PAYLOAD_BEGIN_UNCERTAIN",
            slot: request.payloadAttributes.slotNumber,
            sessionId: ticket,
            callerAborted: signal.aborted,
          });
        } catch {
          // Diagnostic failures cannot change ticket ownership.
        }
      }
      throw error;
    } finally {
      this.preparing--;
      if (ticket !== undefined && this.requests.get(ticket)?.state === "uncertain") {
        void this.reconcileResources(AbortSignal.timeout(6000)).catch(() => {});
      }
    }
  }

  async getPayload(handle: BuildHandle, signal: AbortSignal): Promise<BuiltPayload> {
    if (signal.aborted) throw new ErrorAborted();
    const entry = this.requests.get(handle.payloadId);
    if (handle.sourceId !== this.id || !entry || entry.state !== "active" || entry.request.fork !== handle.fork) {
      throw new SessionPayloadError({code: SessionPayloadErrorCode.UNKNOWN_HANDLE, reason: handle.payloadId});
    }
    const started = performance.now();
    const startedUnix = Date.now();
    const call = {
      method: entry.lease ? "zbuilder_getPayloadV2" : "zbuilder_getPayloadV1",
      params: entry.latest
        ? [
            handle.payloadId,
            toHex(entry.latest.executionPayload.blockHash),
            ...(entry.lease ? [entry.lease.generation] : []),
          ]
        : [handle.payloadId],
    };
    let received: PayloadResponse | LeasedResponse | null;
    try {
      received = await this.rpc(signal).fetch<PayloadResponse | LeasedResponse | null>(call);
    } catch (error) {
      if (signal.aborted) throw new ErrorAborted();
      if (!isFetchError(error) || (error.code !== "UND_ERR_SOCKET" && error.code !== "ECONNRESET")) {
        if (entry.lease) entry.lease.ready = false;
        throw error;
      }
      // Only this idempotent read may retry a pooled socket closing. Preserve
      // the slot's cancellation deadline and never replay begin/append calls.
      try {
        received = await this.rpc(signal).fetch<PayloadResponse | LeasedResponse | null>(call);
      } catch (retryError) {
        if (entry.lease) entry.lease.ready = false;
        throw retryError;
      }
    }
    if (signal.aborted) throw new ErrorAborted();
    if (this.requests.get(handle.payloadId) !== entry || entry.state !== "active")
      throw new SessionPayloadError({code: SessionPayloadErrorCode.UNKNOWN_HANDLE, reason: handle.payloadId});
    let response: PayloadResponse | null;
    if (entry.lease) {
      entry.lease.ready = false;
      const result = received as LeasedResponse | null;
      if (
        !result ||
        typeof result.generation !== "string" ||
        !/^0x(?:0|[1-9a-f][0-9a-f]{0,15})$/.test(result.generation) ||
        result.leaseMs !== 250 ||
        typeof result.ready !== "boolean" ||
        !("payload" in result)
      )
        throw new SessionPayloadError({code: SessionPayloadErrorCode.RESPONSE_MISMATCH, reason: "Invalid offer lease"});
      const previous = entry.lease.generation;
      if (previous && BigInt(result.generation) < BigInt(previous))
        throw new SessionPayloadError({
          code: SessionPayloadErrorCode.RESPONSE_MISMATCH,
          reason: "Order generation rollback",
        });
      entry.lease.generation = result.generation;
      entry.lease.validUntil = started + result.leaseMs;
      // Linux's monotonic clock can stop during suspend. Require the wall-clock
      // allowance too; ordinary clock corrections can only shorten a live lease.
      entry.lease.validUntilUnixMs = startedUnix + result.leaseMs;
      if (previous !== result.generation) entry.latest = undefined;
      if (!result.ready) throw new PayloadSourceNotReady();
      response = result.payload;
      if (response === null && !entry.latest)
        throw new SessionPayloadError({
          code: SessionPayloadErrorCode.RESPONSE_MISMATCH,
          reason: "Missing initial generation payload",
        });
    } else response = received as PayloadResponse | null;
    try {
      if (response !== null) {
        entry.latest = validateSessionPayload(this.id, entry.request, response);
        if (entry.lease) entry.latest.offerEligibility = {generation: entry.lease.generation, lease: entry.lease};
      }
      if (!entry.latest)
        throw new SessionPayloadError({code: SessionPayloadErrorCode.RESPONSE_MISMATCH, reason: "No initial payload"});
      if (entry.lease) {
        // A large reply may consume its whole lease. Cache its verified bytes,
        // but require a fresh, small conditional read before making an offer.
        entry.lease.ready = performance.now() < entry.lease.validUntil && Date.now() < entry.lease.validUntilUnixMs;
        if (!entry.lease.ready) throw new PayloadSourceNotReady();
      }
      return entry.latest;
    } catch (error) {
      if (entry.lease) entry.lease.ready = false;
      throw error;
    }
  }

  async release(handle: BuildHandle, signal: AbortSignal): Promise<void> {
    const entry = this.requests.get(handle.payloadId);
    if (!this.tracksSessions) {
      if (entry?.lease) entry.lease.ready = false;
      this.requests.delete(handle.payloadId);
      await this.rpc(signal).fetch({method: "zbuilder_endV1", params: [handle.payloadId]});
      return;
    }
    if (handle.sourceId !== this.id || !entry || entry.request.fork !== handle.fork)
      throw new SessionPayloadError({code: SessionPayloadErrorCode.UNKNOWN_HANDLE, reason: handle.payloadId});
    if (entry.lease) entry.lease.ready = false;
    if (entry.release) return entry.release;
    entry.state = "releasing";
    entry.release = Promise.resolve()
      .then(async () => {
        if (!entry.reserved) return this.rpc(signal).fetch({method: "zbuilder_endV1", params: [handle.payloadId]});
        const cleanup = AbortSignal.any([signal, AbortSignal.timeout(6000)]);
        while (true) {
          cleanup.throwIfAborted();
          const result = await this.rpc(cleanup).fetch<{released: boolean; beginReservationVersion: number}>({
            method: "zbuilder_cancelBeginV1",
            params: [handle.payloadId],
          });
          if (result?.beginReservationVersion !== 1 || typeof result.released !== "boolean")
            throw new SessionPayloadError({
              code: SessionPayloadErrorCode.RELEASE_UNCONFIRMED,
              reason: handle.payloadId,
            });
          if (result.released) return true;
          await sleep(100, cleanup);
        }
      })
      .then((result) => {
        if (result !== true)
          throw new SessionPayloadError({code: SessionPayloadErrorCode.RELEASE_UNCONFIRMED, reason: handle.payloadId});
        this.requests.delete(handle.payloadId);
      })
      .catch((error: unknown) => {
        if (
          !entry.reserved &&
          error instanceof ErrorJsonRpcResponse &&
          error.response.error.code === -32000 &&
          error.response.error.message === "UnknownSession" &&
          /^native-[0-9a-f]{32}-[01]-(?:0|[1-9][0-9]{0,19})$/.test(handle.payloadId) &&
          BigInt(handle.payloadId.slice(handle.payloadId.lastIndexOf("-") + 1)) <= 0xffffffffffffffffn
        ) {
          this.requests.delete(handle.payloadId);
          return;
        }
        entry.state = "uncertain";
        throw error;
      })
      .finally(() => {
        entry.release = undefined;
      });
    return entry.release;
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
