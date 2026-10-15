import {ApiClient, routes} from "@lodestar/api";
import {ChainForkConfig} from "@lodestar/config";
import {ForkName, MIN_DEPOSIT_AMOUNT, isForkPostGloas} from "@lodestar/params";
import {IClock, computeEpochAtSlot} from "@lodestar/state-transition";
import {BuilderIndex, BuilderStatus, Epoch, ExecutionAddress, RootHex, gloas} from "@lodestar/types";
import {Logger, isErrorAborted, prettyGweiToEth, sleep, toHex, toRootHex} from "@lodestar/utils";
import {BidResult, Metrics} from "../metrics.js";
import {BidLedger, BidLedgerError, BidLedgerErrorCode} from "./bidLedger.js";
import {BidPolicy} from "./bidPolicy.js";
import {BidPublisher} from "./bidPublisher.js";
import {BidStore} from "./bidStore.js";
import {MAX_BID_REVISIONS_PER_SLOT} from "./commitmentLimits.js";
import {createExecutionPayloadBid} from "./executionPayloadBid.js";
import {type PayloadBuildJob, PayloadOrchestrator} from "./payloadOrchestrator.js";
import type {BuiltPayload} from "./payloadSource.js";
import {PayloadStore, PayloadStoreCapacityError} from "./payloadStore.js";
import {ProposerPreferencesTracker} from "./proposerPreferencesTracker.js";

const GWEI_TO_WEI = 1_000_000_000n;

export type PayloadAttributesEvent = routes.events.EventData[routes.events.EventType.payloadAttributes];

export type BidderOptions = {
  dryRun?: boolean;
  /** Poll successive ready snapshots when the source supports updates. Zero disables updates. */
  updateIntervalMs?: number;
  /** Stop updates this far into the proposal slot, in basis points. */
  updateCutoffBps?: number;
  /** Limit new offers across all parents without reducing recovery capacity for existing commitments. */
  maxRevisionsPerSlot?: number;
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
      if (!isErrorAborted(e)) {
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
    const gossipBest = (): void => {
      if (
        !best ||
        publication ||
        Date.now() < job.getPayloadAt ||
        signal.aborted ||
        this.opts.dryRun ||
        ledger.hasSubmitted(slot, parentBlockHash, parentBlockRoot)
      )
        return;
      // Reserve synchronously in gossip(), before the transport can become uncertain.
      const signed = best;
      publication = this.modules.bidPublisher
        .gossip(signed, signal)
        .then(() => {
          metrics?.bids.total.inc({result: BidResult.published});
          logger.info("Gossiped execution payload bid", {
            slot,
            parentBlockHash,
            parentBlockRoot,
            blockHash: toRootHex(signed.message.blockHash),
            value: prettyGweiToEth(signed.message.value),
          });
        })
        .catch((error: unknown) => {
          if (!isErrorAborted(error))
            logger.error(
              "Failed to gossip execution payload bid",
              {slot, parentBlockHash, parentBlockRoot},
              error as Error
            );
        });
    };
    const timerController = new AbortController();
    const timer = sleep(job.getPayloadAt - Date.now(), AbortSignal.any([signal, timerController.signal]))
      .then(gossipBest)
      .catch((error: unknown) => {
        if (!isErrorAborted(error)) throw error;
      });
    try {
      await orchestrator.runUpdates(
        job,
        {intervalMs: this.opts.updateIntervalMs ?? 100, until},
        async (payload) => {
          const signed = await this.bidOnPayload(payload, data, feeRecipient, signal, true);
          if (signed) best = signed;
          gossipBest();
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
      await publication;
    } finally {
      timerController.abort();
      await timer;
      await publication;
    }
  }

  private async bidOnPayload(
    payload: BuiltPayload,
    data: gloas.SSEPayloadAttributes,
    feeRecipient: ExecutionAddress,
    signal: AbortSignal,
    apiRevision: boolean
  ): Promise<gloas.SignedExecutionPayloadBid | null> {
    const {clock, ledger, logger, metrics} = this.modules;
    const slot = data.proposalSlot;
    const parentBlockRoot = toRootHex(data.parentBlockRoot);
    const parentBlockHash = toRootHex(data.parentBlockHash);
    const logCtx = {slot, parentBlockRoot, parentBlockHash};
    signal.throwIfAborted();
    if (this.opts.dryRun) {
      metrics?.bids.total.inc({result: BidResult.dryRun});
      logger.info("Built payload without bidding", {
        ...logCtx,
        blockHash: toRootHex(payload.executionPayload.blockHash),
        transactions: payload.executionPayload.transactions.length,
      });
      return null;
    }

    const {status, balance} = this.modules.getBuilderStatus();
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

    const previous = this.modules.bidStore.get(slot, parentBlockHash, parentBlockRoot);
    if (apiRevision && previous && value <= previous.message.value) return null;
    signal.throwIfAborted();
    const {executionPayload} = payload;
    const blockHash = toRootHex(executionPayload.blockHash);
    ledger.assertCanRecordRevision({slot, parentBlockHash, parentBlockRoot}, this.opts.maxRevisionsPerSlot);
    this.modules.payloadStore.add({slot, parentBlockRoot: data.parentBlockRoot, blockHash, payload});

    const bid = createExecutionPayloadBid({
      slot,
      parentBlockRoot: data.parentBlockRoot,
      prevRandao: data.payloadAttributes.prevRandao,
      builderIndex: this.modules.builderIndex,
      feeRecipient,
      value,
      payload: {...payload, fork: ForkName.gloas},
    });
    const signedBid = apiRevision
      ? await this.modules.bidPublisher.prepare(bid)
      : await this.modules.bidPublisher.publish(bid, signal);
    signal.throwIfAborted();
    this.modules.bidStore.add(signedBid);

    metrics?.bids.total.inc({result: apiRevision ? BidResult.prepared : BidResult.published});
    logger.info(apiRevision ? "Prepared API execution payload bid" : "Published execution payload bid", {
      ...logCtx,
      blockHash,
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
    });
    return signedBid;
  }

  /** A reorg can change the dependent root within an epoch. Active builds already coalesce repeated attributes. */
  private async getDependentRoot(epoch: Epoch, signal: AbortSignal): Promise<RootHex> {
    return (await this.modules.api.validator.getProposerDutiesV2({epoch}, {signal})).meta().dependentRoot;
  }
}
