import {beforeEach, describe, expect, it} from "vitest";
import {SLOTS_PER_EPOCH} from "@lodestar/params";
import {ProposerDutiesTracker} from "../../../src/services/proposerDutiesTracker.js";
import {getApiClientStub, mockApiResponse} from "../utils/apiStub.js";
import {ClockMock} from "../utils/clock.js";

describe("ProposerDutiesTracker", () => {
  let api: ReturnType<typeof getApiClientStub>;
  let clock: ClockMock;
  let tracker: ProposerDutiesTracker;

  beforeEach(() => {
    api = getApiClientStub();
    api.validator.getProposerDutiesV2.mockImplementation(async ({epoch}) =>
      mockApiResponse({
        data: Array.from({length: SLOTS_PER_EPOCH}, (_, i) => ({
          slot: epoch * SLOTS_PER_EPOCH + i,
          validatorIndex: epoch * SLOTS_PER_EPOCH + i,
          pubkey: Buffer.alloc(48, i),
        })),
        meta: {executionOptimistic: false, dependentRoot: "0x00"},
      })
    );
    clock = new ClockMock();
    tracker = new ProposerDutiesTracker(api, clock);
  });

  it("returns the proposer of a slot in the current and next epoch", async () => {
    expect((await tracker.getProposer(3))?.validatorIndex).toBe(3);
    expect((await tracker.getProposer(SLOTS_PER_EPOCH + 1))?.validatorIndex).toBe(SLOTS_PER_EPOCH + 1);
  });

  it("returns null for slots outside of the current and next epoch", async () => {
    clock.currentSlot = SLOTS_PER_EPOCH;

    expect(await tracker.getProposer(SLOTS_PER_EPOCH - 1)).toBeNull();
    expect(await tracker.getProposer(3 * SLOTS_PER_EPOCH)).toBeNull();
    expect(api.validator.getProposerDutiesV2).not.toHaveBeenCalled();
  });

  it("fetches the duties of an epoch once", async () => {
    await tracker.getProposer(1);
    await tracker.getProposer(2);

    expect(api.validator.getProposerDutiesV2).toHaveBeenCalledExactlyOnceWith({epoch: 0});
  });

  it("fetches duties again that were fetched one epoch ahead", async () => {
    await tracker.getProposer(SLOTS_PER_EPOCH);
    clock.currentSlot = SLOTS_PER_EPOCH;
    await tracker.getProposer(SLOTS_PER_EPOCH);
    await tracker.getProposer(SLOTS_PER_EPOCH + 1);

    expect(api.validator.getProposerDutiesV2).toHaveBeenCalledTimes(2);
    expect(api.validator.getProposerDutiesV2).toHaveBeenLastCalledWith({epoch: 1});
  });
});
