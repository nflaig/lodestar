import {ApiClient, routes} from "@lodestar/api";
import {ChainForkConfig} from "@lodestar/config";
import {ForkName, MIN_DEPOSIT_AMOUNT, isForkPostGloas} from "@lodestar/params";
import {IClock, computeEpochAtSlot} from "@lodestar/state-transition";
import {BLSPubkey, BuilderIndex, BuilderStatus, ExecutionAddress, RootHex, gloas} from "@lodestar/types";
import {
  Logger,
  TimeoutError,
  isErrorAborted,
  prettyGweiToEth,
  sleep,
  toHex,
  toRootHex,
  withTimeout,
} from "@lodestar/utils";
import {BidResult, Metrics} from "../metrics.js";
import {getAlternateBuilderBidLimit} from "./alternateBuilderEligibility.js";
import {BidLedger, BidLedgerError, BidLedgerErrorCode} from "./bidLedger.js";
import {BidPolicy} from "./bidPolicy.js";
import {BidPublisher} from "./bidPublisher.js";
import {BidStore} from "./bidStore.js";
import {MAX_BID_REVISIONS_PER_SLOT} from "./commitmentLimits.js";
import {createExecutionPayloadBid} from "./executionPayloadBid.js";
import {GossipStage, getMinGossipBidValue, validateGossipStages} from "./gossipStages.js";
import {isOfferEligible} from "./offerEligibility.js";
import {type PayloadBuildJob, PayloadOrchestrator} from "./payloadOrchestrator.js";
import type {BuiltPayload, PayloadSourceResources} from "./payloadSource.js";
import {PayloadStore, PayloadStoreCapacityError} from "./payloadStore.js";
import type {ProposerDutiesTracker} from "./proposerDutiesTracker.js";
import {ProposerPreferencesTracker} from "./proposerPreferencesTracker.js";

const GWEI_TO_WEI = 1_000_000_000n;
const MAX_ALTERNATE_ATTEMPTS_PER_SLOT = 4;
const MAX_ALTERNATE_DIAGNOSTICS_PER_SLOT = 16;
const MAX_ALTERNATE_RECEIVED_DIAGNOSTICS_PER_SLOT = 4;
const ALTERNATE_RETENTION_BUDGET_MS = 150;
const ALTERNATE_JOURNAL_RECHECK_MS = 10;
const ALTERNATE_JOURNAL_RECHECK_HEADROOM_MS = 100;
const ALTERNATE_REFRESH_PRIMARY_MARGIN_MS = 100;
const STAGED_GOSSIP_REFRESH_MARGIN_MS = 10;
const STAGED_GOSSIP_REFRESH_MAX_MS = 25;

type AlternateDiagnosticReason =
  | "received"
  | "primary_context_unavailable"
  | "past_slot"
  | "duplicate_or_queue_full"
  | "alternate_in_progress"
  | "primary_not_ready"
  | "stopping"
  | "primary_preferences_unavailable"
  | "source_resources_unavailable"
  | "source_disabled"
  | "attempt_limit"
  | "build_window_closed"
  | "source_busy"
  | "journal_busy"
  | "gossip_headroom"
  | "queued_context_changed"
  | "preferences_unavailable"
  | "builder_ineligible"
  | "refresh_capacity";

type AlternateKind = "initial" | "retry" | "refresh";
type QueuedAlternate = {
  event: AlternatePayloadAttributesEvent;
  kind: AlternateKind;
  notBefore: number;
  retryFor?: PrimaryBuild;
};

type PrimaryBuild = {
  id: string;
  slot: number;
  data: gloas.SSEPayloadAttributes;
  controller: AbortController;
  ready: boolean;
  dependentRoot?: RootHex;
  feeRecipient?: ExecutionAddress;
};

type AlternateBuild = {
  kind: AlternateKind;
  event: AlternatePayloadAttributesEvent;
  primary: PrimaryBuild;
  controller: AbortController;
  signal: AbortSignal;
  deadline: number;
  getPayloadAt: number;
  retentionStarted: boolean;
  journalRecheckStarted: boolean;
  retainedOffer: boolean;
  preemptedBy?: PrimaryBuild;
};

export type PayloadAttributesEvent = routes.events.EventData[routes.events.EventType.payloadAttributes];
export type AlternatePayloadAttributesEvent = routes.events.AlternatePayloadAttributesEvent;

export type BidderOptions = {
  dryRun?: boolean;
  /** Poll successive ready snapshots when the source supports updates. Zero disables updates. */
  updateIntervalMs?: number;
  /** Stop updates this far into the proposal slot, in basis points. */
  updateCutoffBps?: number;
  /** Limit new offers across all parents without reducing recovery capacity for existing commitments. */
  maxRevisionsPerSlot?: number;
  /** Build one bounded API/stream snapshot for each supported alternate parent tuple. */
  alternateBranches?: boolean;
  alternateBuildDurationMs?: number;
  /** Target one refresh this many ms before the slot, after primary gossip plus 100 ms. Zero disables it. */
  alternateRefreshBeforeSlotMs?: number;
  /** Point within the slot before the proposal slot at which the payload is retrieved and bid on, in basis points */
  deadlineBps: number;
  /** Do not bid while the builder balance is below this value */
  minOperatingBalanceGwei: number;
};

export type BidderModules = {
  config: ChainForkConfig;
  logger: Logger;
  clock: IClock;
  api: ApiClient;
  orchestrator: PayloadOrchestrator;
  payloadStore: PayloadStore;
  ledger: BidLedger;
  policy: BidPolicy;
  bidPublisher: BidPublisher;
  bidStore: BidStore;
  proposerPreferencesTracker: ProposerPreferencesTracker;
  proposerDutiesTracker?: ProposerDutiesTracker;
  /** Alternates may only enter an idle durable writer; primary offers retain its full capacity. */
  canRetainAlternate?: () => boolean;
  getBuilderStatus: () => {status: BuilderStatus | undefined; balance: number | undefined};
  builderIndex: BuilderIndex;
  builderPubkey?: BLSPubkey;
  executionFeeRecipient: ExecutionAddress;
  metrics: Metrics | null;
  signal: AbortSignal;
  gossipStages?: GossipStage[];
};

/**
 * Starts construction on payload attributes. Optional sessions improve API offers
 * while each parent tuple retains one gossip publication allowance.
 */
export class Bidder {
  private readonly activeBuilds = new Map<string, {slot: number; controller: AbortController}>();
  private primary: PrimaryBuild | undefined;
  private readonly alternateQueue = new Map<string, QueuedAlternate>();
  private readonly alternateAttempts = new Map<number, {id: string; kind: AlternateKind}[]>();
  private alternateBuild: AlternateBuild | undefined;
  private alternateTask: Promise<void> | undefined;
  private alternateWake: ReturnType<typeof setTimeout> | undefined;
  private alternateDiagnosticSlot = -Infinity;
  private readonly alternateDiagnostics = new Set<string>();
  private alternateReceivedDiagnostics = 0;

  constructor(
    private readonly modules: BidderModules,
    private readonly opts: BidderOptions
  ) {
    const stages = modules.gossipStages ?? [];
    if (
      stages.length &&
      (!(opts.updateIntervalMs && opts.updateIntervalMs > 0) || !modules.orchestrator.supportsUpdates)
    )
      throw Error("GOSSIP_STAGES_REQUIRE_PAYLOAD_UPDATES");
    validateGossipStages(
      {builderIndex: modules.builderIndex, deadlineBps: opts.deadlineBps},
      stages,
      opts.updateCutoffBps ?? 1000
    );
    const limit = opts.maxRevisionsPerSlot ?? MAX_BID_REVISIONS_PER_SLOT;
    if (!Number.isSafeInteger(limit) || limit < 1 || limit > MAX_BID_REVISIONS_PER_SLOT) {
      throw new RangeError(`maxRevisionsPerSlot must be between 1 and ${MAX_BID_REVISIONS_PER_SLOT}`);
    }
    const duration = opts.alternateBuildDurationMs ?? 500;
    if (!Number.isSafeInteger(duration) || duration < 250 || duration > 1000)
      throw new RangeError("alternateBuildDurationMs must be between 250 and 1000");
    const refresh = opts.alternateRefreshBeforeSlotMs ?? 0;
    if (!Number.isSafeInteger(refresh) || (refresh !== 0 && (refresh < 500 || refresh > 5000)))
      throw new RangeError("alternateRefreshBeforeSlotMs must be zero or between 500 and 5000");
    if (refresh > 0 && !opts.alternateBranches) throw Error("ALTERNATE_REFRESH_REQUIRES_ALTERNATE_BRANCHES");
    if (
      opts.alternateBranches &&
      (!(opts.updateIntervalMs && opts.updateIntervalMs > 0) ||
        !modules.orchestrator.supportsUpdates ||
        !modules.orchestrator.supportsBoundedAlternates)
    )
      throw Error("ALTERNATE_BRANCHES_REQUIRE_BOUNDED_SESSION_UPDATES");
  }

  async onPayloadAttributes(event: PayloadAttributesEvent): Promise<void> {
    if (event.context !== undefined) {
      if (event.context.role === "alternate") this.onAlternatePayloadAttributes(event);
      return;
    }
    const {clock, config, ledger, logger, metrics, orchestrator, proposerPreferencesTracker} = this.modules;
    if (!isForkPostGloas(event.version)) {
      return;
    }
    const data = event.data as gloas.SSEPayloadAttributes;
    const slot = data.proposalSlot;
    const parentBlockRoot = toRootHex(data.parentBlockRoot);
    const parentBlockHash = toRootHex(data.parentBlockHash);
    const logCtx = {slot, parentBlockRoot, parentBlockHash};

    if (slot <= clock.getCurrentSlot()) {
      logger.debug("Ignoring payload attributes for past slot", logCtx);
      return;
    }

    // The beacon node emits attributes before the slot for the parent variant it would build on. With
    // `--chain.emitPayloadAttributesEarly` it also emits on block and payload import, so one slot may see
    // the empty tuple, then the full one, then a repeat. Builds are keyed by tuple to absorb that.
    const buildId = `${slot}:${parentBlockRoot}:${parentBlockHash}`;
    const updates = (this.opts.updateIntervalMs ?? 0) > 0 && orchestrator.supportsUpdates;
    if (this.activeBuilds.has(buildId) || (!updates && ledger.hasSubmitted(slot, parentBlockHash, parentBlockRoot))) {
      return;
    }
    // A delayed event must not displace a newer primary or its reserved native worker.
    if (this.primary && this.primary.slot > slot) return;
    const controller = new AbortController();
    const primary: PrimaryBuild = {id: buildId, slot, data, controller, ready: false};
    const alternate = this.alternateBuild;
    if (
      alternate &&
      !alternate.signal.aborted &&
      Date.now() < alternate.deadline &&
      alternate.primary === this.primary &&
      alternate.event.data.proposalSlot === slot &&
      alternate.event.data.proposerIndex === data.proposerIndex
    )
      alternate.preemptedBy = primary;
    for (const [id, active] of this.activeBuilds) {
      if (id !== buildId && active.slot <= slot) active.controller.abort();
    }
    alternate?.controller.abort();
    clearTimeout(this.alternateWake);
    this.alternateWake = undefined;
    for (const [id, queued] of this.alternateQueue) {
      if (
        queued.retryFor ||
        id === buildId ||
        queued.event.data.proposalSlot !== slot ||
        queued.event.data.proposerIndex !== data.proposerIndex
      )
        this.alternateQueue.delete(id);
    }
    for (const attemptedSlot of this.alternateAttempts.keys()) {
      if (attemptedSlot < slot) this.alternateAttempts.delete(attemptedSlot);
    }
    const active = {slot, controller};
    this.activeBuilds.set(buildId, active);
    this.primary = primary;
    const signal = AbortSignal.any([controller.signal, this.modules.signal]);

    try {
      // Heze bids require inclusion list bits which are not tracked yet
      if (event.version !== ForkName.gloas) {
        logger.warn("Bidding is not supported for fork", {...logCtx, fork: event.version});
        return;
      }

      this.modules.proposerDutiesTracker?.invalidate(computeEpochAtSlot(slot));
      const dependentRoot = await this.getDependentRoot(data, signal);
      signal.throwIfAborted();
      const proposerPreferences = proposerPreferencesTracker.get(slot, dependentRoot);
      if (proposerPreferences === null) {
        logger.debug("No proposer preferences known for slot, not bidding", {...logCtx, dependentRoot});
        metrics?.bids.total.inc({result: BidResult.noProposerPreferences});
        return;
      }
      const {feeRecipient, targetGasLimit} = proposerPreferences.message;
      primary.dependentRoot = dependentRoot;
      primary.feeRecipient = feeRecipient;
      signal.throwIfAborted();

      const msToDeadline =
        clock.msToSlot(slot) - config.SLOT_DURATION_MS + config.getSlotComponentDurationMs(this.opts.deadlineBps);
      logger.verbose("Preparing payload build", {...logCtx, msToDeadline, targetGasLimit});

      const job: PayloadBuildJob = {
        id: buildId,
        request: {
          fork: event.version,
          forkchoiceState: {
            headBlockHash: parentBlockHash,
            safeBlockHash: toRootHex(data.safeBlockHash),
            finalizedBlockHash: toRootHex(data.finalizedBlockHash),
          },
          payloadAttributes: {
            ...data.payloadAttributes,
            suggestedFeeRecipient: toHex(this.modules.executionFeeRecipient),
            targetGasLimit,
          },
        },
        getPayloadAt: Date.now() + msToDeadline,
      };
      if (updates) {
        await this.runUpdates(job, data, feeRecipient, signal, primary);
      } else {
        const payload = await orchestrator.run(job, signal);
        await this.bidOnPayload(payload, data, feeRecipient, signal, false);
      }
    } catch (e) {
      if (!isErrorAborted(e) && !(signal.aborted && e === signal.reason)) {
        metrics?.bids.total.inc({result: BidResult.error});
        logger.error("Failed to bid", logCtx, e as Error);
      }
    } finally {
      controller.abort();
      if (this.activeBuilds.get(buildId) === active) this.activeBuilds.delete(buildId);
      if (this.primary === primary) {
        this.primary = undefined;
        this.alternateBuild?.controller.abort();
        this.alternateQueue.clear();
        clearTimeout(this.alternateWake);
        this.alternateWake = undefined;
      }
    }
  }

  onAlternatePayloadAttributes(event: AlternatePayloadAttributesEvent): void {
    if (!this.opts.alternateBranches || event.version !== ForkName.gloas) return;
    const data = event.data as gloas.SSEPayloadAttributes;
    this.logAlternateDiagnostic("received", data);
    const primary = this.primary;
    if (
      !primary ||
      primary.controller.signal.aborted ||
      data.proposalSlot !== primary.slot ||
      data.proposerIndex !== primary.data.proposerIndex
    ) {
      this.logAlternateDiagnostic("primary_context_unavailable", data);
      return;
    }
    if (data.proposalSlot <= this.modules.clock.getCurrentSlot()) {
      this.logAlternateDiagnostic("past_slot", data);
      return;
    }
    const id = `${data.proposalSlot}:${toRootHex(data.parentBlockRoot)}:${toRootHex(data.parentBlockHash)}`;
    const refresh = (this.opts.alternateRefreshBeforeSlotMs ?? 0) > 0;
    const active = this.alternateBuild;
    if (
      refresh &&
      active &&
      active.event.data.proposalSlot === data.proposalSlot &&
      toRootHex(active.event.data.parentBlockRoot) === toRootHex(data.parentBlockRoot) &&
      toRootHex(active.event.data.parentBlockHash) === toRootHex(data.parentBlockHash)
    ) {
      const previousLimit = this.getAlternateLimit(active.event, active.primary);
      active.event = event;
      const nextLimit = this.getAlternateLimit(event, active.primary);
      if (nextLimit === null || (active.retentionStarted && previousLimit !== null && nextLimit < previousLimit)) {
        active.controller.abort();
      }
      return;
    }
    const attempts = this.alternateAttempts.get(data.proposalSlot) ?? [];
    const queued = this.alternateQueue.get(id);
    if (refresh && queued) {
      queued.event = event;
      this.tryAlternate();
      return;
    }
    const retained = this.hasRetainedAlternate(event);
    const kind: AlternateKind = refresh && retained ? "refresh" : "initial";
    if (
      id === primary.id ||
      attempts.some((attempt) => attempt.id === id && (kind !== "refresh" || attempt.kind === "refresh")) ||
      this.alternateQueue.has(id) ||
      (!refresh &&
        this.modules.bidStore.get(data.proposalSlot, toRootHex(data.parentBlockHash), toRootHex(data.parentBlockRoot)))
    ) {
      this.logAlternateDiagnostic("duplicate_or_queue_full", data);
      return;
    }
    // A new parent must not lose its first snapshot to a queued refresh.
    if (refresh && kind === "initial" && this.alternateQueue.size >= MAX_ALTERNATE_ATTEMPTS_PER_SLOT) {
      for (const [queuedId, entry] of this.alternateQueue) {
        if (entry.kind === "refresh") {
          this.alternateQueue.delete(queuedId);
          break;
        }
      }
    }
    if (this.alternateQueue.size >= MAX_ALTERNATE_ATTEMPTS_PER_SLOT) {
      this.logAlternateDiagnostic("duplicate_or_queue_full", data);
      return;
    }
    this.alternateQueue.set(id, {
      event,
      kind,
      notBefore: kind === "refresh" ? this.alternateRefreshAt(data.proposalSlot) : 0,
    });
    this.tryAlternate();
  }

  private alternateRefreshAt(slot: number): number {
    const {clock, config} = this.modules;
    const slotStart = Date.now() + clock.msToSlot(slot);
    return Math.max(
      slotStart - (this.opts.alternateRefreshBeforeSlotMs ?? 0),
      slotStart -
        config.SLOT_DURATION_MS +
        config.getSlotComponentDurationMs(this.opts.deadlineBps) +
        ALTERNATE_REFRESH_PRIMARY_MARGIN_MS
    );
  }

  private logAlternateDiagnostic(
    reason: AlternateDiagnosticReason,
    data?: gloas.SSEPayloadAttributes,
    resources?: PayloadSourceResources
  ): void {
    try {
      const localSlot = this.modules.clock.getCurrentSlot();
      if (localSlot > this.alternateDiagnosticSlot) {
        this.alternateDiagnosticSlot = localSlot;
        this.alternateDiagnostics.clear();
        this.alternateReceivedDiagnostics = 0;
      }
      if (this.alternateDiagnostics.size >= MAX_ALTERNATE_DIAGNOSTICS_PER_SLOT) return;
      if (reason === "received" && this.alternateReceivedDiagnostics >= MAX_ALTERNATE_RECEIVED_DIAGNOSTICS_PER_SLOT)
        return;
      const key =
        reason === "received" && data
          ? `${reason}:${data.proposalSlot}:${toRootHex(data.parentBlockRoot)}:${toRootHex(data.parentBlockHash)}`
          : reason;
      if (this.alternateDiagnostics.has(key)) return;
      this.alternateDiagnostics.add(key);
      if (reason === "received") this.alternateReceivedDiagnostics++;
      const snapshot = resources ?? this.modules.orchestrator.getResources();
      this.modules.logger.debug("Alternate parent admission", {
        code: "BUILDER_ALTERNATE_ADMISSION",
        reason,
        localSlot,
        slot: data?.proposalSlot ?? this.primary?.slot,
        parentBlockRoot: data && toRootHex(data.parentBlockRoot),
        parentBlockHash: data && toRootHex(data.parentBlockHash),
        primarySlot: this.primary?.slot,
        primaryReady: this.primary?.ready ?? false,
        activeBuilds: this.activeBuilds.size,
        queueSize: this.alternateQueue.size,
        attempts: this.primary ? (this.alternateAttempts.get(this.primary.slot)?.length ?? 0) : 0,
        capacity: snapshot?.capacity,
        active: snapshot?.active,
        preparing: snapshot?.preparing,
        releasing: snapshot?.releasing,
        uncertain: snapshot?.uncertain,
        alternatesDisabled: snapshot?.alternatesDisabled,
      });
    } catch {
      // Diagnostic failures must not change admission or session ownership.
    }
  }

  /** One short-lived alternate owns the spare worker, including its confirmed cleanup. */
  private tryAlternate(): void {
    clearTimeout(this.alternateWake);
    this.alternateWake = undefined;
    const {clock, config, orchestrator} = this.modules;
    const primary = this.primary;
    const resources = orchestrator.getResources();
    const duration = this.opts.alternateBuildDurationMs ?? 500;
    if (
      !this.opts.alternateBranches ||
      this.alternateTask ||
      this.alternateQueue.size === 0 ||
      !primary?.ready ||
      primary.controller.signal.aborted ||
      this.modules.signal.aborted ||
      primary.feeRecipient === undefined ||
      primary.dependentRoot === undefined ||
      !resources ||
      resources.alternatesDisabled ||
      (this.alternateAttempts.get(primary.slot)?.length ?? 0) >= MAX_ALTERNATE_ATTEMPTS_PER_SLOT ||
      clock.msToSlot(primary.slot) <= duration + 50
    ) {
      if (this.opts.alternateBranches && this.alternateQueue.size > 0) {
        const reason: AlternateDiagnosticReason = this.alternateTask
          ? "alternate_in_progress"
          : !primary?.ready
            ? "primary_not_ready"
            : primary.controller.signal.aborted || this.modules.signal.aborted
              ? "stopping"
              : primary.feeRecipient === undefined || primary.dependentRoot === undefined
                ? "primary_preferences_unavailable"
                : !resources
                  ? "source_resources_unavailable"
                  : resources.alternatesDisabled
                    ? "source_disabled"
                    : (this.alternateAttempts.get(primary.slot)?.length ?? 0) >= MAX_ALTERNATE_ATTEMPTS_PER_SLOT
                      ? "attempt_limit"
                      : "build_window_closed";
        this.logAlternateDiagnostic(reason, this.alternateQueue.values().next().value?.event.data, resources);
      }
      return;
    }
    const retry = (): void => {
      this.alternateWake = setTimeout(() => this.tryAlternate(), 50);
    };
    if (
      this.activeBuilds.size !== 1 ||
      resources.active !== 1 ||
      resources.preparing + resources.releasing + resources.uncertain !== 0 ||
      this.modules.canRetainAlternate?.() !== true
    ) {
      this.logAlternateDiagnostic(
        this.activeBuilds.size !== 1 ||
          resources.active !== 1 ||
          resources.preparing + resources.releasing + resources.uncertain !== 0
          ? "source_busy"
          : "journal_busy",
        this.alternateQueue.values().next().value?.event.data,
        resources
      );
      retry();
      return;
    }
    // Leave the writer and event loop free around scheduled primary/staged gossip.
    const slotStart = Date.now() + clock.msToSlot(primary.slot);
    if (
      [this.opts.deadlineBps, ...(this.modules.gossipStages ?? []).map((stage) => stage.deadlineBps)].some((bps) => {
        const remaining = slotStart - config.SLOT_DURATION_MS + config.getSlotComponentDurationMs(bps) - Date.now();
        return remaining >= -50 && remaining <= duration + 50;
      })
    ) {
      this.logAlternateDiagnostic("gossip_headroom", this.alternateQueue.values().next().value?.event.data, resources);
      retry();
      return;
    }
    for (const [id, queued] of this.alternateQueue) {
      const {event, retryFor, kind} = queued;
      if (
        id === primary.id ||
        event.data.proposalSlot !== primary.slot ||
        event.data.proposerIndex !== primary.data.proposerIndex ||
        toRootHex(event.context.dependentRoot) !== primary.dependentRoot ||
        (retryFor !== undefined && retryFor !== primary) ||
        (kind !== "refresh" && this.hasRetainedAlternate(event))
      ) {
        this.logAlternateDiagnostic("queued_context_changed", event.data, resources);
        this.alternateQueue.delete(id);
      }
    }
    const entries = [...this.alternateQueue];
    const next =
      entries.find(([, queued]) => queued.kind !== "refresh" && queued.notBefore <= Date.now()) ??
      entries.find(([, queued]) => queued.notBefore <= Date.now());
    if (!next) {
      const wakeAt = Math.min(...entries.map(([, queued]) => queued.notBefore));
      if (Number.isFinite(wakeAt)) this.alternateWake = setTimeout(() => this.tryAlternate(), wakeAt - Date.now());
      return;
    }
    const [id, {event, kind}] = next;
    const attempts = this.alternateAttempts.get(primary.slot) ?? [];
    if (attempts.length >= MAX_ALTERNATE_ATTEMPTS_PER_SLOT) return;
    if (
      kind === "refresh" &&
      (attempts.length + entries.filter(([, queued]) => queued.kind !== "refresh").length >=
        MAX_ALTERNATE_ATTEMPTS_PER_SLOT ||
        this.modules.ledger.getRevisionCount(primary.slot) >=
          Math.floor((this.opts.maxRevisionsPerSlot ?? MAX_BID_REVISIONS_PER_SLOT) / 2) ||
        !this.modules.payloadStore.hasCapacityForAlternate(primary.slot))
    ) {
      this.logAlternateDiagnostic("refresh_capacity", event.data, resources);
      return;
    }
    attempts.push({id, kind});
    this.alternateAttempts.set(primary.slot, attempts);
    this.alternateQueue.delete(id);
    const controller = new AbortController();
    const signal = AbortSignal.any([controller.signal, primary.controller.signal, this.modules.signal]);
    const deadline = Date.now() + duration;
    const alternate: AlternateBuild = {
      kind,
      event,
      primary,
      controller,
      signal,
      deadline,
      getPayloadAt: deadline - Math.min(ALTERNATE_RETENTION_BUDGET_MS, Math.floor(duration / 2)),
      retentionStarted: false,
      journalRecheckStarted: false,
      retainedOffer: false,
    };
    this.alternateBuild = alternate;
    const timer = setTimeout(() => controller.abort(), duration);
    let preempted = false;
    this.alternateTask = this.buildAlternate(`${id}:${attempts.length}`, alternate)
      .catch((error: unknown) => {
        const aborted = signal.aborted && (isErrorAborted(error) || error === signal.reason);
        preempted = alternate.preemptedBy !== undefined && aborted;
        if (
          aborted &&
          !preempted &&
          !primary.controller.signal.aborted &&
          !this.modules.signal.aborted &&
          !alternate.retentionStarted &&
          Date.now() >= deadline
        )
          this.modules.logger.debug("Skipping alternate parent bid", {
            slot: event.data.proposalSlot,
            parentBlockRoot: toRootHex(event.data.parentBlockRoot),
            parentBlockHash: toRootHex(event.data.parentBlockHash),
            reason: "deadline_before_retention",
          });
        if (!signal.aborted && !isErrorAborted(error))
          this.modules.logger.debug(
            "Skipping alternate parent bid",
            {slot: event.data.proposalSlot, id},
            error as Error
          );
      })
      .finally(() => {
        clearTimeout(timer);
        controller.abort();
        this.alternateBuild = undefined;
        this.alternateTask = undefined;
        const replacement = alternate.preemptedBy;
        if (
          preempted &&
          replacement &&
          id !== replacement.id &&
          this.primary === replacement &&
          !replacement.controller.signal.aborted &&
          !this.modules.signal.aborted &&
          !alternate.retentionStarted &&
          !alternate.journalRecheckStarted &&
          kind !== "refresh" &&
          attempts.filter((attempt) => attempt.id === id).length === 1 &&
          attempts.length < MAX_ALTERNATE_ATTEMPTS_PER_SLOT &&
          clock.getCurrentSlot() < primary.slot &&
          this.alternateQueue.size < MAX_ALTERNATE_ATTEMPTS_PER_SLOT &&
          !this.hasRetainedAlternate(event)
        ) {
          this.alternateQueue.set(id, {event: alternate.event, kind: "retry", notBefore: 0, retryFor: replacement});
          this.modules.logger.debug("Retrying preempted alternate parent bid", {
            slot: primary.slot,
            parentBlockRoot: toRootHex(event.data.parentBlockRoot),
            parentBlockHash: toRootHex(event.data.parentBlockHash),
          });
        }
        if (
          kind !== "refresh" &&
          alternate.retainedOffer &&
          (this.opts.alternateRefreshBeforeSlotMs ?? 0) > 0 &&
          alternate.getPayloadAt < this.alternateRefreshAt(primary.slot) &&
          this.primary?.slot === primary.slot &&
          !this.modules.signal.aborted &&
          this.alternateQueue.size < MAX_ALTERNATE_ATTEMPTS_PER_SLOT &&
          !this.alternateQueue.has(id)
        ) {
          this.alternateQueue.set(id, {
            event: alternate.event,
            kind: "refresh",
            notBefore: this.alternateRefreshAt(primary.slot),
          });
        }
        this.tryAlternate();
      });
  }

  private hasRetainedAlternate(event: AlternatePayloadAttributesEvent): boolean {
    const parentBlockRoot = toRootHex(event.data.parentBlockRoot);
    const parentBlockHash = toRootHex(event.data.parentBlockHash);
    return this.modules.ledger
      .getBidsForSlot(event.data.proposalSlot)
      .some((bid) => bid.parentBlockRoot === parentBlockRoot && bid.parentBlockHash === parentBlockHash);
  }

  private async buildAlternate(id: string, alternate: AlternateBuild): Promise<void> {
    const {primary, event, deadline, getPayloadAt, signal} = alternate;
    const {data} = event;
    const dependentRoot = toRootHex(event.context.dependentRoot);
    signal.throwIfAborted();
    if (this.primary !== primary || dependentRoot !== primary.dependentRoot) {
      this.logAlternateDiagnostic("queued_context_changed", data);
      return;
    }
    const preferences = this.modules.proposerPreferencesTracker.get(data.proposalSlot, dependentRoot);
    if (!preferences) {
      this.logAlternateDiagnostic("preferences_unavailable", data);
      return;
    }
    if (this.getAlternateLimit(event, primary) === null) {
      this.logAlternateDiagnostic("builder_ineligible", data);
      return;
    }
    await this.modules.orchestrator.runSnapshot(
      {
        id: `alt:${id}`,
        request: {
          fork: ForkName.gloas,
          requireUnleased: true,
          forkchoiceState: {
            headBlockHash: toRootHex(data.parentBlockHash),
            safeBlockHash: toRootHex(data.safeBlockHash),
            finalizedBlockHash: toRootHex(data.finalizedBlockHash),
          },
          payloadAttributes: {
            ...data.payloadAttributes,
            suggestedFeeRecipient: toHex(this.modules.executionFeeRecipient),
            targetGasLimit: preferences.message.targetGasLimit,
          },
        },
        getPayloadAt,
      },
      async (payload) => {
        signal.throwIfAborted();
        if (this.primary !== primary || payload.offerEligibility) return;
        if (this.modules.canRetainAlternate?.() !== true) {
          alternate.journalRecheckStarted = true;
          const started = performance.now();
          let outcome = "headroom";
          try {
            if (deadline - Date.now() >= ALTERNATE_JOURNAL_RECHECK_MS + ALTERNATE_JOURNAL_RECHECK_HEADROOM_MS) {
              await sleep(ALTERNATE_JOURNAL_RECHECK_MS, signal);
              signal.throwIfAborted();
              const resources = this.modules.orchestrator.getResources();
              if (this.primary !== primary || dependentRoot !== primary.dependentRoot) outcome = "context_changed";
              else if (!primary.ready) outcome = "primary_not_ready";
              else if (!resources) outcome = "source_resources_unavailable";
              else if (resources.alternatesDisabled) outcome = "source_disabled";
              else if (resources.uncertain > 0) outcome = "source_uncertain";
              else if (deadline - Date.now() < ALTERNATE_JOURNAL_RECHECK_HEADROOM_MS) outcome = "headroom";
              else outcome = this.modules.canRetainAlternate?.() === true ? "idle" : "busy";
            }
          } catch (error) {
            outcome = signal.aborted ? "aborted" : "error";
            throw error;
          } finally {
            try {
              this.modules.logger.debug("Alternate parent journal recheck", {
                code: "BUILDER_ALTERNATE_JOURNAL_RECHECK",
                slot: data.proposalSlot,
                parentBlockRoot: toRootHex(data.parentBlockRoot),
                parentBlockHash: toRootHex(data.parentBlockHash),
                outcome,
                elapsedMs: performance.now() - started,
                remainingMs: deadline - Date.now(),
              });
            } catch {
              // Diagnostic failures must not change admission or session ownership.
            }
          }
          if (outcome !== "idle") {
            this.modules.logger.debug("Skipping alternate parent bid", {
              slot: data.proposalSlot,
              parentBlockRoot: toRootHex(data.parentBlockRoot),
              parentBlockHash: toRootHex(data.parentBlockHash),
              reason: outcome === "busy" ? "journal_busy" : `journal_recheck_${outcome}`,
            });
            return;
          }
        }
        signal.throwIfAborted();
        if (this.primary !== primary || dependentRoot !== primary.dependentRoot) return;
        const coverableGwei = this.getAlternateLimit(alternate.event, primary);
        if (coverableGwei === null) return;
        if (this.modules.proposerPreferencesTracker.get(data.proposalSlot, dependentRoot) !== preferences) return;
        const previous = this.modules.bidStore.get(
          data.proposalSlot,
          toRootHex(data.parentBlockHash),
          toRootHex(data.parentBlockRoot)
        );
        const signed = await this.bidOnPayload(
          payload,
          data,
          preferences.message.feeRecipient,
          signal,
          true,
          this.modules,
          -1,
          {
            coverableGwei,
            deadline,
            onRetentionAttempted: () => {
              alternate.retentionStarted = true;
            },
          }
        );
        alternate.retainedOffer = signed !== null && signed !== previous && this.modules.bidStore.canOffer(signed);
        if (alternate.kind === "refresh") {
          try {
            this.modules.logger.debug("Alternate parent refresh result", {
              code: "BUILDER_ALTERNATE_REFRESH",
              slot: data.proposalSlot,
              parentBlockRoot: toRootHex(data.parentBlockRoot),
              parentBlockHash: toRootHex(data.parentBlockHash),
              improved: alternate.retainedOffer,
              previousValueGwei: previous?.message.value,
              valueGwei: signed?.message.value,
              secFromSlot: this.modules.clock.secFromSlot(data.proposalSlot),
            });
          } catch {
            // Diagnostic failures cannot change a retained offer.
          }
        }
      },
      signal
    );
  }

  private getAlternateLimit(event: AlternatePayloadAttributesEvent, primary: PrimaryBuild): number | null {
    if (!this.modules.builderPubkey || primary.dependentRoot === undefined) return null;
    return getAlternateBuilderBidLimit(event, {
      slot: primary.slot,
      parentBlockRoot: toRootHex(event.data.parentBlockRoot),
      parentBlockHash: toRootHex(event.data.parentBlockHash),
      dependentRoot: primary.dependentRoot,
      proposerIndex: primary.data.proposerIndex,
      builderIndex: this.modules.builderIndex,
      builderPubkey: this.modules.builderPubkey,
      unsettledGwei: this.modules.ledger.getUnsettledValueGwei(computeEpochAtSlot(primary.slot)),
    });
  }

  private async runUpdates(
    job: PayloadBuildJob,
    data: gloas.SSEPayloadAttributes,
    feeRecipient: ExecutionAddress,
    signal: AbortSignal,
    primary: PrimaryBuild
  ): Promise<void> {
    const {clock, config, orchestrator, ledger, logger, metrics} = this.modules;
    const slot = data.proposalSlot;
    const parentBlockHash = toRootHex(data.parentBlockHash);
    const parentBlockRoot = toRootHex(data.parentBlockRoot);
    const until =
      Date.now() + clock.msToSlot(slot) + config.getSlotComponentDurationMs(this.opts.updateCutoffBps ?? 1000);
    let best: gloas.SignedExecutionPayloadBid | null = null;
    let publication: Promise<void> | undefined;
    let publicationPending = false;
    let highestAttemptedValue = Math.max(
      -1,
      ...[ledger, ...(this.modules.gossipStages ?? []).map((stage) => stage.ledger)]
        .flatMap((item) => item.getBidsForSlot(slot))
        .filter((bid) => bid.parentBlockHash === parentBlockHash && bid.parentBlockRoot === parentBlockRoot)
        .map((bid) => bid.valueGwei)
    );
    const stages = this.modules.gossipStages ?? [];
    const slotStart = Date.now() + clock.msToSlot(slot);
    const stageTime = (deadlineBps: number): number =>
      slotStart - config.SLOT_DURATION_MS + config.getSlotComponentDurationMs(deadlineBps);
    const primaryUntil = stages.length ? stageTime(stages[0].deadlineBps) : until;
    const gossipBest = (): void => {
      if (
        !best ||
        !this.modules.bidStore.canOffer(best) ||
        publication ||
        Date.now() < job.getPayloadAt ||
        Date.now() >= primaryUntil ||
        signal.aborted ||
        this.opts.dryRun ||
        ledger.hasSubmitted(slot, parentBlockHash, parentBlockRoot)
      )
        return;
      // Reserve synchronously in gossip(), before the transport can become uncertain.
      const signed = best;
      highestAttemptedValue = Math.max(highestAttemptedValue, signed.message.value);
      publicationPending = true;
      publication = this.modules.bidPublisher
        .gossip(signed, signal, this.modules.bidStore.getOfferEligibility(signed))
        .then(() => {
          metrics?.bids.total.inc({result: BidResult.published});
          logger.info("Gossiped execution payload bid", {
            slot,
            parentBlockHash,
            parentBlockRoot,
            blockHash: toRootHex(signed.message.blockHash),
            value: prettyGweiToEth(signed.message.value),
            builderIndex: signed.message.builderIndex,
          });
        })
        .catch((error: unknown) => {
          if (!isErrorAborted(error))
            logger.error(
              "Failed to gossip execution payload bid",
              {slot, parentBlockHash, parentBlockRoot},
              error as Error
            );
        })
        .finally(() => {
          publicationPending = false;
        });
    };
    const timerController = new AbortController();
    const timer = sleep(job.getPayloadAt - Date.now(), AbortSignal.any([signal, timerController.signal]))
      .then(gossipBest)
      .catch((error: unknown) => {
        if (!isErrorAborted(error)) throw error;
      });
    // A stage gets one attempt at its deadline. Missed windows never catch up in a burst.
    const stagesStartedAt = Date.now();
    const stagedPublications = stages.map((stage, index) =>
      sleep(stageTime(stage.deadlineBps) - Date.now(), AbortSignal.any([signal, timerController.signal]))
        .then(async () => {
          const stageStarted = performance.now();
          const timerLatenessMs = Date.now() - stageTime(stage.deadlineBps);
          const expires = index + 1 < stages.length ? stageTime(stages[index + 1].deadlineBps) : until;
          const logSkipped = (
            reason: string,
            preparation?: {
              initialPrepareMs: number;
              refreshAttempted: boolean;
              refreshTimedOut: boolean;
              refreshMs: number;
            }
          ): void =>
            logger.info("Skipped staged execution payload bid", {
              code: "BUILDER_STAGED_GOSSIP_SKIPPED",
              slot,
              parentBlockHash,
              parentBlockRoot,
              builderIndex: stage.builderIndex,
              reason,
              phase: preparation ? "after_preparation" : "before_preparation",
              refreshAttempted: false,
              ...preparation,
            });
          if (
            signal.aborted ||
            this.opts.dryRun ||
            stagesStartedAt >= stageTime(stage.deadlineBps) ||
            Date.now() >= expires ||
            !best ||
            !this.modules.bidStore.canOffer(best)
          )
            return;
          if (publicationPending) {
            logSkipped("publication_pending");
            return;
          }
          if (stage.ledger.hasSubmitted(slot, parentBlockHash, parentBlockRoot)) return;
          const initialBest = best;
          const stored = this.modules.payloadStore.get(toRootHex(initialBest.message.blockHash));
          if (!stored) return;
          publicationPending = true;
          try {
            const minimum = highestAttemptedValue < 0 ? 0 : getMinGossipBidValue(highestAttemptedValue);
            let signed = await this.bidOnPayload(
              {
                ...stored.payload,
                fork: ForkName.gloas,
                // The same immutable hash can be re-admitted in a new order
                // generation. Its durable snapshot may carry an older lease.
                offerEligibility: this.modules.bidStore.getOfferEligibility(initialBest),
              },
              data,
              feeRecipient,
              signal,
              true,
              stage,
              minimum - 1
            );
            const initialPrepareMs = performance.now() - stageStarted;
            // Persistence may finish after a parent switch, shutdown, or this stage's window.
            if (!signed || signal.aborted || Date.now() >= expires || signed.message.value <= highestAttemptedValue) {
              logSkipped(
                !signed
                  ? "no_prepared_bid"
                  : signal.aborted
                    ? "aborted"
                    : Date.now() >= expires
                      ? "stage_expired"
                      : "value_not_increased",
                {initialPrepareMs, refreshAttempted: false, refreshTimedOut: false, refreshMs: 0}
              );
              return;
            }
            let refreshAttempted = false;
            let refreshMs = 0;
            let refreshTimedOut = false;
            const refreshBudgetMs = Math.min(
              STAGED_GOSSIP_REFRESH_MAX_MS,
              expires - Date.now() - STAGED_GOSSIP_REFRESH_MARGIN_MS
            );
            const refreshBest = best;
            // The first signature stays revealable. Recheck once before the first gossip,
            // allowing another observed retention duration plus a scheduling margin.
            if (
              refreshBest.message.value > initialBest.message.value &&
              this.modules.bidStore.canOffer(refreshBest) &&
              refreshBudgetMs > 0 &&
              refreshBudgetMs >= initialPrepareMs
            ) {
              const refreshedPayload = this.modules.payloadStore.get(toRootHex(refreshBest.message.blockHash));
              if (refreshedPayload) {
                refreshAttempted = true;
                const refreshStarted = performance.now();
                try {
                  const minimumRefreshValue = signed.message.value;
                  const refreshed = await withTimeout(
                    (refreshSignal = signal) =>
                      this.bidOnPayload(
                        {
                          ...refreshedPayload.payload,
                          fork: ForkName.gloas,
                          offerEligibility: this.modules.bidStore.getOfferEligibility(refreshBest),
                        },
                        data,
                        feeRecipient,
                        refreshSignal,
                        true,
                        stage,
                        minimumRefreshValue
                      ),
                    refreshBudgetMs,
                    signal
                  );
                  if (refreshed) signed = refreshed;
                } catch (error) {
                  // A failed acknowledgement may still leave a durable new signature.
                  // Do not retry it; only the earlier confirmed commitment can be offered.
                  refreshTimedOut = error instanceof TimeoutError;
                  if (!refreshTimedOut && !isErrorAborted(error) && !signal.aborted)
                    logger.warn(
                      "Failed to refresh staged execution payload bid",
                      {slot, parentBlockHash, parentBlockRoot, builderIndex: stage.builderIndex},
                      error as Error
                    );
                } finally {
                  refreshMs = performance.now() - refreshStarted;
                }
              }
            }
            const skipped = signal.aborted
              ? "aborted"
              : Date.now() >= expires
                ? "stage_expired"
                : signed.message.value <= highestAttemptedValue
                  ? "value_not_increased"
                  : !stage.bidStore.canOffer(signed)
                    ? "offer_ineligible"
                    : null;
            if (skipped) {
              logSkipped(skipped, {
                refreshAttempted,
                refreshTimedOut,
                initialPrepareMs,
                refreshMs,
              });
              return;
            }
            highestAttemptedValue = signed.message.value;
            const preparedAt = performance.now();
            await stage.bidPublisher.gossip(signed, signal, stage.bidStore.getOfferEligibility(signed));
            const publishedAt = performance.now();
            metrics?.bids.total.inc({result: BidResult.published});
            logger.info("Gossiped execution payload bid", {
              slot,
              parentBlockHash,
              parentBlockRoot,
              blockHash: toRootHex(signed.message.blockHash),
              builderIndex: stage.builderIndex,
              value: prettyGweiToEth(signed.message.value),
              timerLatenessMs,
              initialPrepareMs,
              refreshAttempted,
              refreshTimedOut,
              refreshMs,
              prepareMs: preparedAt - stageStarted,
              publishMs: publishedAt - preparedAt,
            });
          } finally {
            publicationPending = false;
          }
        })
        .catch((error: unknown) => {
          if (!isErrorAborted(error) && !signal.aborted)
            logger.error(
              "Failed to gossip execution payload bid",
              {slot, parentBlockHash, parentBlockRoot, builderIndex: stage.builderIndex},
              error as Error
            );
        })
    );
    try {
      await orchestrator.runUpdates(
        job,
        {intervalMs: this.opts.updateIntervalMs ?? 100, until},
        async (payload) => {
          const signed = await this.bidOnPayload(payload, data, feeRecipient, signal, true);
          if (signed) best = signed;
          gossipBest();
          primary.ready = signed !== null && payload.offerEligibility === undefined;
          if (this.primary === primary) this.tryAlternate();
        },
        signal
      );
    } catch (error) {
      if (
        !best ||
        !(
          error instanceof PayloadStoreCapacityError ||
          (error instanceof Error && error.message === "COMMITMENT_WORKER_CAPACITY") ||
          (error instanceof BidLedgerError && error.type.code === BidLedgerErrorCode.REVISION_LIMIT)
        )
      )
        throw error;
      logger.debug("Stopped API updates at commitment capacity", {
        slot,
        parentBlockHash,
        parentBlockRoot,
        reason:
          error instanceof PayloadStoreCapacityError || error instanceof BidLedgerError
            ? error.type.code
            : error.message,
        slotBidRevisions: ledger.getRevisionCount(slot),
      });
      // Exhausting update capacity must not cancel the already retained bid's scheduled gossip.
      await timer;
      await Promise.all(stagedPublications);
      await publication;
    } finally {
      timerController.abort();
      await timer;
      await Promise.all(stagedPublications);
      await publication;
    }
  }

  private async bidOnPayload(
    payload: BuiltPayload,
    data: gloas.SSEPayloadAttributes,
    feeRecipient: ExecutionAddress,
    signal: AbortSignal,
    apiRevision: boolean,
    identity: Pick<BidderModules, "ledger" | "bidPublisher" | "bidStore" | "getBuilderStatus" | "builderIndex"> = this
      .modules,
    minimumValue = -1,
    alternateAdmission?: {coverableGwei: number; deadline: number; onRetentionAttempted: () => void}
  ): Promise<gloas.SignedExecutionPayloadBid | null> {
    const {clock, logger, metrics} = this.modules;
    const {ledger, bidPublisher, bidStore, builderIndex} = identity;
    const alternate = alternateAdmission !== undefined;
    const additional = identity !== this.modules;
    const slot = data.proposalSlot;
    const parentBlockRoot = toRootHex(data.parentBlockRoot);
    const parentBlockHash = toRootHex(data.parentBlockHash);
    const logCtx = {slot, parentBlockRoot, parentBlockHash};
    signal.throwIfAborted();
    if (alternateAdmission && Date.now() >= alternateAdmission.deadline) {
      logger.debug("Skipping alternate parent bid", {...logCtx, reason: "deadline_before_retention"});
      return null;
    }
    if (!isOfferEligible(payload.offerEligibility)) return null;
    if (this.opts.dryRun) {
      metrics?.bids.total.inc({result: BidResult.dryRun});
      logger.info("Built payload without bidding", {
        ...logCtx,
        blockHash: toRootHex(payload.executionPayload.blockHash),
        transactions: payload.executionPayload.transactions.length,
      });
      return null;
    }

    const {status, balance} = identity.getBuilderStatus();
    if (status !== "active" || balance === undefined) {
      logger.warn("Builder is not active, not bidding", {...logCtx, status});
      metrics?.bids.total.inc({result: BidResult.inactive});
      return null;
    }
    if (balance < this.opts.minOperatingBalanceGwei) {
      logger.warn("Builder balance below operating minimum, not bidding", {
        ...logCtx,
        balance: prettyGweiToEth(balance),
        minOperatingBalance: prettyGweiToEth(this.opts.minOperatingBalanceGwei),
      });
      metrics?.bids.total.inc({result: BidResult.lowBalance});
      return null;
    }

    const unsettledGwei = ledger.getUnsettledValueGwei(computeEpochAtSlot(slot));
    const coverableGwei = Math.min(
      Math.max(balance - MIN_DEPOSIT_AMOUNT - unsettledGwei, 0),
      alternateAdmission?.coverableGwei ?? Number.POSITIVE_INFINITY
    );
    const payloadValueGwei = Number(payload.executionPayloadValue / GWEI_TO_WEI);
    const value = this.modules.policy.computeValue({payloadValueGwei, coverableGwei});
    if (value === null) {
      logger.info("Bid policy declined to bid", {...logCtx, payloadValueGwei, coverableGwei, unsettledGwei});
      metrics?.bids.total.inc({result: BidResult.policyDeclined});
      return null;
    }

    if (value <= minimumValue) return null;
    const previous = bidStore.get(slot, parentBlockHash, parentBlockRoot);
    if (apiRevision && previous && value <= previous.message.value) return previous;
    signal.throwIfAborted();
    const {executionPayload} = payload;
    const blockHash = toRootHex(executionPayload.blockHash);
    if (alternate && this.modules.canRetainAlternate?.() !== true) {
      logger.debug("Skipping alternate parent bid", {...logCtx, reason: "journal_busy"});
      return null;
    }
    const revisionLimit = Math.floor(
      (this.opts.maxRevisionsPerSlot ?? MAX_BID_REVISIONS_PER_SLOT) / (alternate ? 2 : 1)
    );
    if (revisionLimit === 0) return null;
    ledger.assertCanRecordRevision({slot, parentBlockHash, parentBlockRoot}, revisionLimit);
    this.modules.payloadStore.add(
      {slot, parentBlockRoot: data.parentBlockRoot, blockHash, payload},
      alternate ? "alternate" : "offer"
    );

    const bid = createExecutionPayloadBid({
      slot,
      parentBlockRoot: data.parentBlockRoot,
      prevRandao: data.payloadAttributes.prevRandao,
      builderIndex,
      feeRecipient,
      value,
      payload: {...payload, fork: ForkName.gloas},
    });
    signal.throwIfAborted();
    if (alternateAdmission && Date.now() >= alternateAdmission.deadline) {
      logger.debug("Skipping alternate parent bid", {...logCtx, reason: "deadline_before_retention"});
      return null;
    }
    alternateAdmission?.onRetentionAttempted();
    const signedBid = await bidPublisher.prepare(bid, revisionLimit);
    // Retain before exposure, including when transport completion is uncertain
    // or arrives after cancellation. Reveal ownership does not expire with offers.
    const deadlineExpired = alternateAdmission !== undefined && Date.now() >= alternateAdmission.deadline;
    const alternateExpired = alternateAdmission && (signal.aborted || deadlineExpired);
    bidStore.add(signedBid, alternateExpired ? "recovery" : payload.offerEligibility);
    if (alternateExpired) {
      logger.debug("Skipping alternate parent bid", {
        ...logCtx,
        reason: deadlineExpired ? "deadline_after_retention" : "aborted_after_retention",
      });
      return null;
    }
    if (apiRevision) signal.throwIfAborted();
    // Disk persistence can outlive a cancellation, lease, parent or shutdown.
    // The signature remains retained even when it is no longer a new offer.
    if (!isOfferEligible(payload.offerEligibility)) return null;
    if (!apiRevision) await bidPublisher.gossip(signedBid, signal, payload.offerEligibility);

    metrics?.bids.total.inc({result: apiRevision ? BidResult.prepared : BidResult.published});
    logger.info(
      additional
        ? "Prepared staged execution payload bid"
        : apiRevision
          ? "Prepared API execution payload bid"
          : "Published execution payload bid",
      {
        ...logCtx,
        blockHash,
        builderIndex,
        value: prettyGweiToEth(value),
        payloadValue: prettyGweiToEth(payloadValueGwei),
        feeRecipient: toHex(feeRecipient),
        gasLimit: executionPayload.gasLimit,
        transactions: executionPayload.transactions.length,
        blobs: bid.blobKzgCommitments.length,
        slotBidRevisions: ledger.getRevisionCount(slot),
        retainedPayloads: this.modules.payloadStore.size,
        retainedPayloadBytes: this.modules.payloadStore.bytes,
        alternate,
        secFromSlot: clock.secFromSlot(slot - 1),
      }
    );
    return signedBid;
  }

  /** A reorg can change the dependent root within an epoch. Active builds already coalesce repeated attributes. */
  private async getDependentRoot(data: gloas.SSEPayloadAttributes, signal: AbortSignal): Promise<RootHex> {
    const epoch = computeEpochAtSlot(data.proposalSlot);
    const response = await this.modules.api.validator.getProposerDutiesV2({epoch}, {signal});
    signal.throwIfAborted();
    const dependentRoot = response.meta().dependentRoot;
    const duties = response.value();
    if (
      this.opts.alternateBranches &&
      duties.find((duty) => duty.slot === data.proposalSlot)?.validatorIndex !== data.proposerIndex
    )
      throw Error("ALTERNATE_PRIMARY_PROPOSER_DUTY_MISMATCH");
    this.modules.proposerDutiesTracker?.update(epoch, dependentRoot, duties);
    return dependentRoot;
  }
}
