import {SLOTS_PER_EPOCH} from "@lodestar/params";

export const MAX_BID_REVISIONS_PER_SLOT = 256;
export const PAYLOAD_RETENTION_SLOTS = 2;
export const BID_HISTORY_SLOTS = 3 * SLOTS_PER_EPOCH;

// Previous slots, the current and next slots, plus headroom for delayed pruning or failed publications.
export const MAX_RETAINED_PAYLOADS = (PAYLOAD_RETENTION_SLOTS + 3) * MAX_BID_REVISIONS_PER_SLOT;
// Budget one bid and one reveal per revision, plus payload files and identity.
export const MAX_COMMITMENT_JOURNAL_FILES = Math.max(
  20_000,
  1 + MAX_RETAINED_PAYLOADS + 2 * (BID_HISTORY_SLOTS + 2) * MAX_BID_REVISIONS_PER_SLOT
);
