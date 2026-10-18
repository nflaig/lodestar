import {afterEach, beforeEach, describe, expect, it, vi} from "vitest";
import {type BuildHandle, type BuildRequest, type BuiltPayload, PayloadSourceNotReady} from "@lodestar/builder";
import {ForkName} from "@lodestar/params";
import {ssz} from "@lodestar/types";
import {defer} from "@lodestar/utils";
import {PayloadOrchestrator} from "../../../../../builder/src/services/payloadOrchestrator.js";
import {
  ForkTransitionPayloadErrorCode,
  ForkTransitionPayloadSource,
} from "../../../../src/cmds/builder/forkTransitionPayloadSource.js";

// Match the error class from the source orchestrator rather than mixing it with the compiled package export.
vi.mock("@lodestar/builder", async (importOriginal) => ({
  ...(await importOriginal<typeof import("@lodestar/builder")>()),
  PayloadSourceNotReady: (await import("../../../../../builder/src/services/offerEligibility.js"))
    .PayloadSourceNotReady,
}));

const forkSlot = 32;
const signal = new AbortController().signal;

function request(slotNumber = forkSlot): BuildRequest {
  return {
    fork: ForkName.gloas,
    forkchoiceState: {
      headBlockHash: `0x${"11".repeat(32)}`,
      safeBlockHash: `0x${"22".repeat(32)}`,
      finalizedBlockHash: `0x${"33".repeat(32)}`,
    },
    payloadAttributes: {...ssz.gloas.PayloadAttributes.defaultValue(), slotNumber, timestamp: slotNumber * 12},
  };
}

function stub(id: string) {
  return {
    id,
    supportsUpdates: true,
    supportsBoundedAlternates: true,
    prepare: vi.fn(
      async (req: BuildRequest, _signal: AbortSignal): Promise<BuildHandle> => ({
        sourceId: id,
        fork: req.fork,
        payloadId: "0x01",
      })
    ),
    getPayload: vi.fn(
      async (handle: BuildHandle, _signal: AbortSignal): Promise<BuiltPayload> => ({
        sourceId: id,
        fork: handle.fork,
        executionPayload: ssz.gloas.ExecutionPayload.defaultValue(),
        executionRequests: ssz.gloas.ExecutionRequests.defaultValue(),
        blobsBundle: ssz.gloas.BlobsBundle.defaultValue(),
        executionPayloadValue: 1n,
      })
    ),
    release: vi.fn(async (_handle: BuildHandle, _signal: AbortSignal): Promise<void> => {}),
    reconcileResources: vi.fn(async (_signal: AbortSignal): Promise<void> => {}),
    getResources: vi.fn(() => ({
      capacity: 2 as const,
      preparing: 0,
      active: 1,
      releasing: 0,
      uncertain: 1,
      alternatesDisabled: false,
    })),
  };
}

describe("ForkTransitionPayloadSource", () => {
  beforeEach(() => vi.useFakeTimers({now: (forkSlot - 1) * 12_000}));
  afterEach(() => vi.useRealTimers());

  it("builds only on the Engine through the cutoff, snapshots at the deadline, and keeps the retained payload", async () => {
    const sessions = stub("sessions");
    const engine = stub("engine");
    const source = new ForkTransitionPayloadSource(sessions, engine, forkSlot, 900);
    const handle = await source.prepare(request(), signal);
    expect(engine.prepare).toHaveBeenCalledExactlyOnceWith(request(), signal);
    expect(sessions.prepare).not.toHaveBeenCalled();
    await expect(source.getPayload(handle, signal)).rejects.toBeInstanceOf(PayloadSourceNotReady);
    expect(engine.getPayload).not.toHaveBeenCalled();

    vi.setSystemTime(forkSlot * 12_000 - 900);
    const snapshot = await source.getPayload(handle, signal);
    vi.setSystemTime((forkSlot + 1) * 12_000);
    const nextHandle = await source.prepare(request(forkSlot + 1), signal);
    expect(sessions.prepare).toHaveBeenCalledOnce();
    expect(await source.getPayload(handle, signal)).toBe(snapshot);
    expect(engine.getPayload).toHaveBeenCalledOnce();
    await source.release(handle, signal);
    expect(engine.release).toHaveBeenCalledExactlyOnceWith(handle, signal);
    expect(snapshot.executionPayloadValue).toBe(1n);
    await expect(source.getPayload(handle, signal)).rejects.toMatchObject({
      type: {code: ForkTransitionPayloadErrorCode.UNKNOWN_HANDLE},
    });
    await source.getPayload(nextHandle, signal);
    await source.getPayload(nextHandle, signal);
    expect(sessions.getPayload).toHaveBeenCalledTimes(2);
    await source.release(nextHandle, signal);
    expect(sessions.release).toHaveBeenCalledExactlyOnceWith(nextHandle, signal);
  });

  it("does not fall back to the Engine after any post-cutoff constructor error", async () => {
    const sessions = stub("sessions");
    const engine = stub("engine");
    const error = new Error("ambiguous native begin");
    sessions.prepare.mockRejectedValue(error);
    const source = new ForkTransitionPayloadSource(sessions, engine, forkSlot, 900);
    await expect(source.prepare(request(forkSlot + 1), signal)).rejects.toBe(error);
    expect(engine.prepare).not.toHaveBeenCalled();
    expect(engine.getPayload).not.toHaveBeenCalled();
  });

  it("rejects alternate Engine work before allocation and delegates native resource ownership", async () => {
    const sessions = stub("sessions");
    const engine = stub("engine");
    const source = new ForkTransitionPayloadSource(sessions, engine, forkSlot, 900);
    await expect(source.prepare({...request(), requireUnleased: true}, signal)).rejects.toMatchObject({
      type: {code: ForkTransitionPayloadErrorCode.ALTERNATE_UNSUPPORTED},
    });
    expect(engine.prepare).not.toHaveBeenCalled();
    expect(source.getResources()).toEqual(sessions.getResources());
    expect(source.supportsUpdates).toBe(true);
    expect(source.supportsBoundedAlternates).toBe(true);
    await source.reconcileResources(signal);
    expect(sessions.reconcileResources).toHaveBeenCalledExactlyOnceWith(signal);
    await source.prepare({...request(forkSlot + 1), requireUnleased: true}, signal);
    expect(sessions.prepare).toHaveBeenCalledOnce();
  });

  it("rejects foreign handles even when payload ids collide", async () => {
    const source = new ForkTransitionPayloadSource(stub("sessions"), stub("engine"), forkSlot, 900);
    const handle = await source.prepare(request(), signal);
    vi.setSystemTime(forkSlot * 12_000);
    await expect(source.getPayload({...handle}, signal)).rejects.toMatchObject({
      type: {code: ForkTransitionPayloadErrorCode.UNKNOWN_HANDLE},
    });
    await expect(source.release({...handle}, signal)).rejects.toMatchObject({
      type: {code: ForkTransitionPayloadErrorCode.UNKNOWN_HANDLE},
    });
  });

  it("rejects cancellation before allocation and retains a late handle for cleanup", async () => {
    const sessions = stub("sessions");
    const engine = stub("engine");
    const source = new ForkTransitionPayloadSource(sessions, engine, forkSlot, 900);
    const cancelled = AbortSignal.abort();
    await expect(source.prepare(request(), cancelled)).rejects.toBe(cancelled.reason);
    expect(engine.prepare).not.toHaveBeenCalled();
    const pending = defer<BuildHandle>();
    engine.prepare.mockReturnValue(pending.promise);
    const controller = new AbortController();
    const preparing = source.prepare(request(), controller.signal);
    controller.abort();
    const handle: BuildHandle = {sourceId: engine.id, fork: ForkName.gloas, payloadId: "late"};
    pending.resolve(handle);
    expect(await preparing).toBe(handle);
    await source.release(handle, signal);
    expect(engine.release).toHaveBeenCalledExactlyOnceWith(handle, signal);
  });

  it("keeps Engine construction running until its deadline through the actual update orchestrator", async () => {
    const sessions = stub("sessions");
    const engine = stub("engine");
    const source = new ForkTransitionPayloadSource(sessions, engine, forkSlot, 900);
    const orchestrator = new PayloadOrchestrator(source, {getPayloadTimeout: 1000}, signal);
    const at = forkSlot * 12_000 - 900;
    const onPayload = vi.fn(async (_payload: BuiltPayload) => {});
    const updating = orchestrator.runUpdates(
      {id: "fork", request: request(), getPayloadAt: at},
      {intervalMs: 25, until: at + 100},
      onPayload
    );
    await vi.advanceTimersByTimeAsync(at - Date.now() - 1);
    expect(engine.prepare).toHaveBeenCalledOnce();
    expect(engine.getPayload).not.toHaveBeenCalled();
    expect(onPayload).not.toHaveBeenCalled();
    await vi.advanceTimersByTimeAsync(101);
    await updating;
    expect(engine.getPayload).toHaveBeenCalledOnce();
    expect(onPayload).toHaveBeenCalledOnce();
    expect(engine.release).toHaveBeenCalledOnce();
    expect(sessions.prepare).not.toHaveBeenCalled();
  });
});
