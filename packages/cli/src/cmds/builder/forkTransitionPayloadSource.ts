import type {BuildHandle, BuildRequest, BuiltPayload, PayloadSource, PayloadSourceResources} from "@lodestar/builder";
import {PayloadSourceNotReady} from "@lodestar/builder";
import {LodestarError} from "@lodestar/utils";

export enum ForkTransitionPayloadErrorCode {
  UNKNOWN_HANDLE = "FORK_TRANSITION_PAYLOAD_UNKNOWN_HANDLE",
  ALTERNATE_UNSUPPORTED = "FORK_TRANSITION_PAYLOAD_ALTERNATE_UNSUPPORTED",
}

export class ForkTransitionPayloadError extends LodestarError<{code: ForkTransitionPayloadErrorCode}> {}

/** Select one constructor per build; retained handles keep their original source across the slot boundary. */
export class ForkTransitionPayloadSource implements PayloadSource {
  private readonly owners = new WeakMap<
    BuildHandle,
    {source: PayloadSource; getPayloadAt?: number; snapshot?: Promise<BuiltPayload>}
  >();

  constructor(
    private readonly sessions: PayloadSource,
    private readonly engine: PayloadSource,
    private readonly engineUntilSlot: number,
    private readonly engineSnapshotLeadMs: number
  ) {}

  get id(): string {
    return this.sessions.id;
  }

  get supportsUpdates(): boolean {
    return this.sessions.supportsUpdates === true;
  }

  get supportsBoundedAlternates(): boolean {
    return this.sessions.supportsBoundedAlternates === true;
  }

  getResources(): PayloadSourceResources | undefined {
    return this.sessions.getResources?.();
  }

  async reconcileResources(signal: AbortSignal): Promise<void> {
    await this.sessions.reconcileResources?.(signal);
  }

  async prepare(request: BuildRequest, signal: AbortSignal): Promise<BuildHandle> {
    signal.throwIfAborted();
    const source = request.payloadAttributes.slotNumber <= this.engineUntilSlot ? this.engine : this.sessions;
    if (source === this.engine && request.requireUnleased) {
      throw new ForkTransitionPayloadError({code: ForkTransitionPayloadErrorCode.ALTERNATE_UNSUPPORTED});
    }
    const handle = await source.prepare(request, signal);
    this.owners.set(handle, {
      source,
      getPayloadAt:
        source === this.engine ? request.payloadAttributes.timestamp * 1000 - this.engineSnapshotLeadMs : undefined,
    });
    return handle;
  }

  async getPayload(handle: BuildHandle, signal: AbortSignal): Promise<BuiltPayload> {
    signal.throwIfAborted();
    const entry = this.owner(handle);
    if (entry.getPayloadAt !== undefined) {
      if (Date.now() < entry.getPayloadAt) throw new PayloadSourceNotReady("Waiting for scheduled Engine snapshot");
      // Engine retrieval can stop construction. Retrieve once at the configured deadline, then retain it.
      entry.snapshot ??= entry.source.getPayload(handle, signal);
      return entry.snapshot;
    }
    return entry.source.getPayload(handle, signal);
  }

  async release(handle: BuildHandle, signal: AbortSignal): Promise<void> {
    await this.owner(handle).source.release?.(handle, signal);
    this.owners.delete(handle);
  }

  private owner(handle: BuildHandle) {
    const entry = this.owners.get(handle);
    if (!entry) throw new ForkTransitionPayloadError({code: ForkTransitionPayloadErrorCode.UNKNOWN_HANDLE});
    return entry;
  }
}
