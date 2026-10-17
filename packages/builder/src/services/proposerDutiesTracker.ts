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
  private readonly pending = new Map<Epoch, {fetchedAtEpoch: Epoch; result: Promise<EpochProposers>}>();

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
      let pending = this.pending.get(epoch);
      if (!pending || pending.fetchedAtEpoch < currentEpoch) {
        const result = this.api.validator.getProposerDutiesV2({epoch}).then((response) => ({
          fetchedAtEpoch: currentEpoch,
          bySlot: new Map(response.value().map((duty) => [duty.slot, duty])),
        }));
        pending = {fetchedAtEpoch: currentEpoch, result};
        this.pending.set(epoch, pending);
      }
      try {
        proposers = await pending.result;
        const cached = this.proposersByEpoch.get(epoch);
        if (!cached || cached.fetchedAtEpoch <= proposers.fetchedAtEpoch) {
          this.proposersByEpoch.set(epoch, proposers);
        } else {
          proposers = cached;
        }
        for (const cachedEpoch of this.proposersByEpoch.keys()) {
          if (cachedEpoch < this.clock.getCurrentEpoch()) this.proposersByEpoch.delete(cachedEpoch);
        }
      } finally {
        if (this.pending.get(epoch) === pending) this.pending.delete(epoch);
      }
    }
    return proposers.bySlot.get(slot) ?? null;
  }
}
