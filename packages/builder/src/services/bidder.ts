import {ApiClient, routes} from "@lodestar/api";
import {ChainForkConfig} from "@lodestar/config";
import {ForkName, MIN_DEPOSIT_AMOUNT, isForkPostGloas} from "@lodestar/params";
import {IClock, computeEpochAtSlot} from "@lodestar/state-transition";
import {BuilderIndex, BuilderStatus, Epoch, ExecutionAddress, RootHex, gloas} from "@lodestar/types";
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
import {BidLedger, BidLedgerError, BidLedgerErrorCode} from "./bidLedger.js";
import {BidPolicy} from "./bidPolicy.js";
import {BidPublisher} from "./bidPublisher.js";
import {BidStore} from "./bidStore.js";
import {MAX_BID_REVISIONS_PER_SLOT} from "./commitmentLimits.js";
import {createExecutionPayloadBid} from "./executionPayloadBid.js";
import {GossipStage, getMinGossipBidValue, validateGossipStages} from "./gossipStages.js";
import {isOfferEligible} from "./offerEligibility.js";
import {type PayloadBuildJob, PayloadOrchestrator} from "./payloadOrchestrator.js";
import type {BuiltPayload} from "./payloadSource.js";
import {PayloadStore, PayloadStoreCapacityError} from "./payloadStore.js";
import {ProposerPreferencesTracker} from "./proposerPreferencesTracker.js";

const GWEI_TO_WEI = 1_000_000_000n;
const STAGED_GOSSIP_REFRESH_MARGIN_MS = 10;
const STAGED_GOSSIP_REFRESH_MAX_MS = 25;
const STAGED_GOSSIP_DEFERRED_PREPARE_MS = 25;

export type PayloadAttributesEvent = routes.events.EventData[routes.events.EventType.payloadAttributes];

export type BidderOptions = {
  dryRun?: boolean;
  /** Poll successive ready snapshots when the source supports updates. Zero disables updates. */
  updateIntervalMs?: number;
  /** Stop updates this far into the proposal slot, in basis points. */
  updateCutoffBps?: number;
  /** Limit new offers across all parents without reducing recovery capacity for existing commitments. */
  maxRevisionsPerSlot?: number;
  /** Let an unused stage wait for a higher durable API bid within its original window. */
  deferUnusedGossipStages?: boolean;
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
  getBuilderStatus: () => {status: BuilderStatus | undefined; balance: number | undefined};
  builderIndex: BuilderIndex;
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
  }

  async onPayloadAttributes(event: PayloadAttributesEvent): Promise<void> {
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
    // TODO: bid on both variants instead of cancelling the empty build once the full tuple arrives
    const buildId = `${slot}:${parentBlockRoot}:${parentBlockHash}`;
    const updates = (this.opts.updateIntervalMs ?? 0) > 0 && orchestrator.supportsUpdates;
    if (this.activeBuilds.has(buildId) || (!updates && ledger.hasSubmitted(slot, parentBlockHash, parentBlockRoot))) {
      return;
    }
    for (const [id, active] of this.activeBuilds) {
      if (id !== buildId && active.slot <= slot) active.controller.abort();
    }
    const controller = new AbortController();
    const active = {slot, controller};
    this.activeBuilds.set(buildId, active);
    const signal = AbortSignal.any([controller.signal, this.modules.signal]);

    try {
      // Heze bids require inclusion list bits which are not tracked yet
      if (event.version !== ForkName.gloas) {
        logger.warn("Bidding is not supported for fork", {...logCtx, fork: event.version});
        return;
      }

      const dependentRoot = await this.getDependentRoot(computeEpochAtSlot(slot), signal);
      signal.throwIfAborted();
      const proposerPreferences = proposerPreferencesTracker.get(slot, dependentRoot);
      if (proposerPreferences === null) {
        logger.debug("No proposer preferences known for slot, not bidding", {...logCtx, dependentRoot});
        metrics?.bids.total.inc({result: BidResult.noProposerPreferences});
        return;
      }
      const {feeRecipient, targetGasLimit} = proposerPreferences.message;

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
        await this.runUpdates(job, data, feeRecipient, signal);
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
    }
  }

  private async runUpdates(
    job: PayloadBuildJob,
    data: gloas.SSEPayloadAttributes,
    feeRecipient: ExecutionAddress,
    signal: AbortSignal
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
    const stagedSignal = AbortSignal.any([signal, timerController.signal]);
    const valueWaiters = this.opts.deferUnusedGossipStages ? new Set<() => void>() : undefined;
    const waitForValue = (expires: number): Promise<boolean> =>
      new Promise((resolve) => {
        const latestStart = expires - STAGED_GOSSIP_DEFERRED_PREPARE_MS - STAGED_GOSSIP_REFRESH_MARGIN_MS;
        const finish = (ready: boolean): void => {
          clearTimeout(timeout);
          stagedSignal.removeEventListener("abort", cancel);
          valueWaiters?.delete(check);
          resolve(ready);
        };
        const cancel = (): void => finish(false);
        const check = (): void => {
          if (stagedSignal.aborted || Date.now() >= latestStart) {
            finish(false);
            return;
          }
          const minimum = highestAttemptedValue < 0 ? 0 : getMinGossipBidValue(highestAttemptedValue);
          if (!publicationPending && best && this.modules.bidStore.canOffer(best) && best.message.value >= minimum)
            finish(true);
        };
        const timeout = setTimeout(cancel, Math.max(0, latestStart - Date.now()));
        stagedSignal.addEventListener("abort", cancel, {once: true});
        valueWaiters?.add(check);
        check();
      });
    // Only an unsigned, below-floor stage may wait inside its existing window.
    const stagesStartedAt = Date.now();
    const stagedPublications = stages.map((stage, index) =>
      sleep(stageTime(stage.deadlineBps) - Date.now(), stagedSignal)
        .then(async () => {
          let stageStarted = performance.now();
          let deferredMs = 0;
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
              deferredMs,
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
          let initialBest = best;
          let stored = this.modules.payloadStore.get(toRootHex(initialBest.message.blockHash));
          if (!stored) {
            logSkipped("payload_unavailable");
            return;
          }
          publicationPending = true;
          let ownsPublication = true;
          try {
            let minimum = highestAttemptedValue < 0 ? 0 : getMinGossipBidValue(highestAttemptedValue);
            let belowFloorValue: number | undefined;
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
              minimum - 1,
              valueWaiters
                ? (value) => {
                    belowFloorValue = value;
                  }
                : undefined
            );
            if (!signed && belowFloorValue !== undefined && valueWaiters) {
              if (stage.bidPublisher.hasPendingRevisions(slot)) {
                logSkipped("pending_revision");
                return;
              }
              if (
                stage.ledger
                  .getBidsForSlot(slot)
                  .some((bid) => bid.parentBlockHash === parentBlockHash && bid.parentBlockRoot === parentBlockRoot)
              ) {
                logSkipped("previous_signed_revision");
                return;
              }
              publicationPending = false;
              ownsPublication = false;
              const deferredAt = performance.now();
              logger.info("Deferred staged execution payload bid", {
                code: "BUILDER_STAGED_GOSSIP_DEFERRED",
                slot,
                parentBlockHash,
                parentBlockRoot,
                builderIndex: stage.builderIndex,
                computedValueGwei: belowFloorValue,
                minimumValueGwei: minimum,
              });
              const ready = await waitForValue(expires);
              deferredMs = performance.now() - deferredAt;
              if (
                !ready ||
                signal.aborted ||
                publicationPending ||
                stage.bidPublisher.hasPendingRevisions(slot) ||
                stage.ledger.hasSubmitted(slot, parentBlockHash, parentBlockRoot) ||
                !best ||
                !this.modules.bidStore.canOffer(best)
              ) {
                logSkipped(
                  signal.aborted
                    ? "aborted"
                    : !ready
                      ? "deferred_window_closed"
                      : publicationPending
                        ? "publication_pending"
                        : stage.bidPublisher.hasPendingRevisions(slot)
                          ? "pending_revision"
                          : stage.ledger.hasSubmitted(slot, parentBlockHash, parentBlockRoot)
                            ? "already_submitted"
                            : "offer_ineligible"
                );
                return;
              }
              initialBest = best;
              stored = this.modules.payloadStore.get(toRootHex(initialBest.message.blockHash));
              if (!stored) {
                logSkipped("deferred_payload_unavailable");
                return;
              }
              const budget = Math.min(
                STAGED_GOSSIP_DEFERRED_PREPARE_MS,
                expires - Date.now() - STAGED_GOSSIP_REFRESH_MARGIN_MS
              );
              if (budget <= 0) {
                logSkipped("deferred_budget_exhausted");
                return;
              }
              minimum = highestAttemptedValue < 0 ? 0 : getMinGossipBidValue(highestAttemptedValue);
              publicationPending = true;
              ownsPublication = true;
              stageStarted = performance.now();
              const deferredPayload = stored.payload;
              try {
                signed = await withTimeout(
                  (prepareSignal = signal) =>
                    this.bidOnPayload(
                      {
                        ...deferredPayload,
                        fork: ForkName.gloas,
                        offerEligibility: this.modules.bidStore.getOfferEligibility(initialBest),
                      },
                      data,
                      feeRecipient,
                      prepareSignal,
                      true,
                      stage,
                      minimum - 1
                    ),
                  budget,
                  signal
                );
              } catch (error) {
                if (error instanceof TimeoutError) {
                  logSkipped("deferred_prepare_timeout", {
                    initialPrepareMs: performance.now() - stageStarted,
                    refreshAttempted: false,
                    refreshTimedOut: false,
                    refreshMs: 0,
                  });
                  return;
                }
                throw error;
              }
            }
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
              deferredMs,
              initialPrepareMs,
              refreshAttempted,
              refreshTimedOut,
              refreshMs,
              prepareMs: preparedAt - stageStarted,
              publishMs: publishedAt - preparedAt,
            });
          } finally {
            if (ownsPublication) publicationPending = false;
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
          if (valueWaiters) for (const check of valueWaiters) check();
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
    onBelowMinimumValue?: (value: number) => void
  ): Promise<gloas.SignedExecutionPayloadBid | null> {
    const {clock, logger, metrics} = this.modules;
    const {ledger, bidPublisher, bidStore, builderIndex} = identity;
    const additional = identity !== this.modules;
    const slot = data.proposalSlot;
    const parentBlockRoot = toRootHex(data.parentBlockRoot);
    const parentBlockHash = toRootHex(data.parentBlockHash);
    const logCtx = {slot, parentBlockRoot, parentBlockHash};
    signal.throwIfAborted();
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
    const coverableGwei = Math.max(balance - MIN_DEPOSIT_AMOUNT - unsettledGwei, 0);
    const payloadValueGwei = Number(payload.executionPayloadValue / GWEI_TO_WEI);
    const value = this.modules.policy.computeValue({payloadValueGwei, coverableGwei});
    if (value === null) {
      logger.info("Bid policy declined to bid", {...logCtx, payloadValueGwei, coverableGwei, unsettledGwei});
      metrics?.bids.total.inc({result: BidResult.policyDeclined});
      return null;
    }

    if (value <= minimumValue) {
      onBelowMinimumValue?.(value);
      return null;
    }
    const previous = bidStore.get(slot, parentBlockHash, parentBlockRoot);
    if (apiRevision && previous && value <= previous.message.value) return previous;
    signal.throwIfAborted();
    const {executionPayload} = payload;
    const blockHash = toRootHex(executionPayload.blockHash);
    ledger.assertCanRecordRevision({slot, parentBlockHash, parentBlockRoot}, this.opts.maxRevisionsPerSlot);
    this.modules.payloadStore.add({slot, parentBlockRoot: data.parentBlockRoot, blockHash, payload});

    const bid = createExecutionPayloadBid({
      slot,
      parentBlockRoot: data.parentBlockRoot,
      prevRandao: data.payloadAttributes.prevRandao,
      builderIndex,
      feeRecipient,
      value,
      payload: {...payload, fork: ForkName.gloas},
    });
    const signedBid = await bidPublisher.prepare(bid);
    // Retain before exposure, including when transport completion is uncertain
    // or arrives after cancellation. Reveal ownership does not expire with offers.
    bidStore.add(signedBid, payload.offerEligibility);
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
        secFromSlot: clock.secFromSlot(slot - 1),
      }
    );
    return signedBid;
  }

  /** A reorg can change the dependent root within an epoch. Active builds already coalesce repeated attributes. */
  private async getDependentRoot(epoch: Epoch, signal: AbortSignal): Promise<RootHex> {
    return (await this.modules.api.validator.getProposerDutiesV2({epoch}, {signal})).meta().dependentRoot;
  }
}
