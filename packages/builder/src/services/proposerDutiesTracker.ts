import {ApiClient, routes} from "@lodestar/api";
import {IClock, computeEpochAtSlot} from "@lodestar/state-transition";
import {Epoch, Slot} from "@lodestar/types";

type EpochProposers = {
  fetchedAtEpoch: Epoch;
  bySlot: Map<Slot, routes.validator.ProposerDuty>;
};

/** Resolves the proposer of a slot from the proposer duties of the beacon node */
export class ProposerDutiesTracker {
  private readonly proposersByEpoch = new Map<Epoch, EpochProposers>();

  constructor(
    private readonly api: ApiClient,
    private readonly clock: IClock
  ) {}

  /** Returns null for slots outside of the current and next epoch */
  async getProposer(slot: Slot): Promise<routes.validator.ProposerDuty | null> {
    const epoch = computeEpochAtSlot(slot);
    const currentEpoch = this.clock.getCurrentEpoch();
    if (epoch < currentEpoch || epoch > currentEpoch + 1) {
      return null;
    }

    let proposers = this.proposersByEpoch.get(epoch);
    // Duties fetched one epoch ahead are fetched again once their epoch started
    if (proposers === undefined || proposers.fetchedAtEpoch < currentEpoch) {
      const duties = (await this.api.validator.getProposerDutiesV2({epoch})).value();
      proposers = {fetchedAtEpoch: currentEpoch, bySlot: new Map(duties.map((duty) => [duty.slot, duty]))};
      this.proposersByEpoch.set(epoch, proposers);
      for (const cachedEpoch of this.proposersByEpoch.keys()) {
        if (cachedEpoch < currentEpoch) {
          this.proposersByEpoch.delete(cachedEpoch);
        }
      }
    }
    return proposers.bySlot.get(slot) ?? null;
  }
}
