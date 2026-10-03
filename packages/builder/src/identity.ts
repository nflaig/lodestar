import {pubkeyCache} from "@chainsafe/lodestar-z/pubkeys";
import {ApiClient, ApiError, HttpStatusCode, WireFormat, routes} from "@lodestar/api";
import {BeaconConfig} from "@lodestar/config";
import {MAX_PENDING_DEPOSITS_PER_EPOCH, PAYLOAD_BUILDER_VERSION} from "@lodestar/params";
import {
  IClock,
  computeStartSlotAtEpoch,
  createBeaconStateView,
  getValidatorCountFromStateBytes,
  isStatePostGloas,
} from "@lodestar/state-transition";
import {BuilderStatus, Slot, getBuilderStatus as getStatusOfBuilder} from "@lodestar/types";
import {ErrorAborted, Logger, TimeoutError, isErrorAborted, isFetchError, sleep, toHex} from "@lodestar/utils";

/**
 * Point within the last slot before the Gloas fork at which the builder registry of the fork is
 * computed, in basis points. Late enough for the block of that slot to be the head and early
 * enough to bid for the first Gloas slot.
 */
const FORK_REGISTRY_LOOKUP_BPS = 5000;

export async function resolveBuilderIdentity(
  api: ApiClient,
  logger: Logger,
  id: routes.beacon.BuilderId,
  signal: AbortSignal,
  clock: IClock,
  config: BeaconConfig
): Promise<routes.beacon.BuilderResponse> {
  const builderEntry = await waitForBuilder(api, logger, id, signal, clock, config);

  if (builderEntry.builder.version !== PAYLOAD_BUILDER_VERSION) {
    throw Error(`Builder version mismatch: got ${builderEntry.builder.version}, expected ${PAYLOAD_BUILDER_VERSION}`);
  }

  logger.info("Builder identity resolved", {
    index: builderEntry.index,
    status: builderEntry.status,
    balanceGwei: builderEntry.builder.balance,
    executionAddress: toHex(builderEntry.builder.executionAddress),
    slot: clock.getCurrentSlot(),
  });

  return builderEntry;
}

export async function getBuilderStatus(
  api: ApiClient,
  logger: Logger,
  id: routes.beacon.BuilderId
): Promise<{status: BuilderStatus; balance: number} | null> {
  try {
    const builderEntry = await fetchBuilder(api, logger, id);
    if (builderEntry) {
      return {
        status: builderEntry.status,
        balance: builderEntry.builder.balance,
      };
    }
    logger.warn("Builder status not available in beacon node");
    return null;
  } catch (e) {
    logger.warn("Couldn't fetch the builder", {}, e as Error);
    return null;
  }
}

async function waitForBuilder(
  api: ApiClient,
  logger: Logger,
  id: routes.beacon.BuilderId,
  signal: AbortSignal,
  clock: IClock,
  config: BeaconConfig
): Promise<routes.beacon.BuilderResponse> {
  const gloasForkEpoch = config.GLOAS_FORK_EPOCH;
  while (!signal.aborted) {
    const currentEpoch = clock.getCurrentEpoch();
    if (currentEpoch < gloasForkEpoch) {
      const currentSlot = clock.getCurrentSlot();
      const forkSlot = computeStartSlotAtEpoch(gloasForkEpoch);
      if (currentSlot < forkSlot - 1) {
        // Builders only exist post-gloas, the beacon node returns an error for pre-gloas state
        logger.info("Waiting for Gloas fork before resolving builder identity", {
          gloasForkEpoch,
          currentEpoch,
          slot: currentSlot,
        });
        await sleep(clock.msToSlot(Math.min(computeStartSlotAtEpoch(currentEpoch + 1), forkSlot - 1)), signal);
        continue;
      }

      // Builders deposited before the fork are onboarded by the fork upgrade, the beacon node only knows
      // them once the first Gloas block is the head. Compute the registry of the fork from the head state
      // of the last slot before it, to be able to bid for the first Gloas slot.
      await sleep(config.getSlotComponentDurationMs(FORK_REGISTRY_LOOKUP_BPS) - clock.msFromSlot(currentSlot), signal);
      let builder: routes.beacon.BuilderResponse | null = null;
      try {
        builder = await fetchBuilderAtFork(api, config, forkSlot, id);
      } catch (e) {
        if (isErrorAborted(e)) throw e;
        logger.warn("Unable to compute the builder registry of the Gloas fork", {forkSlot}, e as Error);
      }
      if (builder?.status === "active") {
        return builder;
      }
      logger.info("Builder is not active at the Gloas fork", {id, status: builder?.status ?? "unknown", forkSlot});
      await sleep(clock.msToSlot(forkSlot), signal);
      continue;
    }

    let builder: routes.beacon.BuilderResponse | null = null;
    try {
      builder = await fetchBuilder(api, logger, id);
    } catch (e) {
      // At the fork boundary getStateBuilders("head") can still 400: it serves the head block's
      // post-state, which stays pre-gloas until a gloas-epoch block is head. Keep polling on the transient.
      if (e instanceof ApiError && e.status === HttpStatusCode.BAD_REQUEST) {
        logger.info("Waiting for Gloas state to be available at head", {
          gloasForkEpoch,
          currentEpoch,
          slot: clock.getCurrentSlot(),
        });
        await sleep(msToNextSlotPoll(clock), signal);
        continue;
      }
      throw e;
    }

    if (builder?.status === "active") {
      return builder;
    }
    if (builder?.status === "exited") {
      throw Error(`Builder exited: id=${id}`);
    }
    if (builder?.status === "pending") {
      logger.info("Waiting for builder deposit to be finalized", {id, slot: clock.getCurrentSlot()});
      await sleep(msToNextEpochPoll(clock), signal);
    } else {
      logger.info("Waiting for builder to be known to the beacon node", {id, slot: clock.getCurrentSlot()});
      await sleep(msToNextSlotPoll(clock), signal);
    }
  }
  throw new ErrorAborted("waitForBuilder");
}

async function fetchBuilder(
  api: ApiClient,
  logger: Logger,
  id: routes.beacon.BuilderId
): Promise<routes.beacon.BuilderResponse | null> {
  try {
    const builderRes = await api.beacon.getStateBuilders({
      stateId: "head",
      builderIds: [id],
    });

    const builders = builderRes.value();

    if (builders.length === 0) {
      return null;
    }

    const builder = builders[0];

    if (typeof id === "number") {
      if (id !== builder.index) {
        throw Error(`Index mismatch: got=${builder.index} expected=${id}`);
      }
    } else if (id !== toHex(builder.builder.pubkey)) {
      throw Error(`Pubkey mismatch: got=${toHex(builder.builder.pubkey)} expected=${id}`);
    }

    return builder;
  } catch (e) {
    if (e instanceof TimeoutError || (isFetchError(e) && e.type !== "input")) {
      logger.warn("Failed to fetch builder", {message: e.message});
      return null;
    }
    throw e;
  }
}

async function fetchBuilderAtFork(
  api: ApiClient,
  config: BeaconConfig,
  forkSlot: Slot,
  id: routes.beacon.BuilderId
): Promise<routes.beacon.BuilderResponse | null> {
  const stateBytes = (await api.debug.getStateV2({stateId: "head"}, {responseWireFormat: WireFormat.ssz})).ssz();
  const validatorCount = getValidatorCountFromStateBytes(config, stateBytes);
  if (validatorCount === null) {
    throw Error("Cannot read validator count from head state");
  }
  // The view syncs pubkeys during construction, capacity must be reserved first
  pubkeyCache.ensureCapacity(validatorCount + MAX_PENDING_DEPOSITS_PER_EPOCH);
  const forkState = createBeaconStateView({useNative: false, config, stateBytes}).processSlots(forkSlot);
  if (!isStatePostGloas(forkState)) {
    throw Error(`Expected gloas state at fork slot, got fork=${forkState.forkName}`);
  }

  const finalizedEpoch = forkState.finalizedCheckpoint.epoch;
  const buildersLength = forkState.getBuildersLength();
  for (let index = 0; index < buildersLength; index++) {
    const builder = forkState.getBuilder(index);
    if (typeof id === "number" ? id === index : id === toHex(builder.pubkey)) {
      return {index, status: getStatusOfBuilder(builder, finalizedEpoch), builder};
    }
  }
  return null;
}

function msToNextEpochPoll(clock: IClock): number {
  return clock.msToSlot(computeStartSlotAtEpoch(clock.getCurrentEpoch() + 1));
}

function msToNextSlotPoll(clock: IClock): number {
  return clock.msToSlot(clock.getCurrentSlot() + 1);
}
