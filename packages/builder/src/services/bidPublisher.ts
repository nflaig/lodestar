import type {ApiClient} from "@lodestar/api";
import type {ChainForkConfig} from "@lodestar/config";
import {isForkPostGloas} from "@lodestar/params";
import type {BuilderIndex, RootHex, Slot, gloas, heze} from "@lodestar/types";
import {sszTypesFor} from "@lodestar/types";
import {LodestarError, toRootHex} from "@lodestar/utils";
import type {BidIdentity, BidLedger} from "./bidLedger.js";
import type {BuilderSigner} from "./builderSigner.js";

export type BidPublisherModules = {
  api: ApiClient;
  config: ChainForkConfig;
  signer: BuilderSigner;
  ledger: BidLedger;
  builderIndex: BuilderIndex;
  hasPayload: (identity: BidIdentity) => boolean;
  /** Must finish durable retention before the signature can be exposed. */
  retainCommitment?: (bid: gloas.SignedExecutionPayloadBid) => void;
};

export enum BidPublisherErrorCode {
  BUILDER_INDEX_MISMATCH = "BID_PUBLISHER_ERROR_BUILDER_INDEX_MISMATCH",
  PAYLOAD_NOT_RETAINED = "BID_PUBLISHER_ERROR_PAYLOAD_NOT_RETAINED",
  PRE_GLOAS_BID = "BID_PUBLISHER_ERROR_PRE_GLOAS_BID",
  BID_NOT_PREPARED = "BID_PUBLISHER_ERROR_BID_NOT_PREPARED",
}

export type BidPublisherErrorType =
  | {
      code: BidPublisherErrorCode.PRE_GLOAS_BID | BidPublisherErrorCode.BID_NOT_PREPARED;
      slot: Slot;
    }
  | {
      code: BidPublisherErrorCode.BUILDER_INDEX_MISMATCH;
      builderIndex: BuilderIndex;
      bidBuilderIndex: BuilderIndex;
    }
  | {
      code: BidPublisherErrorCode.PAYLOAD_NOT_RETAINED;
      slot: Slot;
      parentBlockHash: RootHex;
      parentBlockRoot: RootHex;
      blockHash: RootHex;
    };

export class BidPublisherError extends LodestarError<BidPublisherErrorType> {}

/** Signs and submits a bid only if its payload is retained locally. */
export class BidPublisher {
  constructor(private readonly modules: BidPublisherModules) {}

  publish(bid: heze.ExecutionPayloadBid, signal: AbortSignal): Promise<heze.SignedExecutionPayloadBid>;
  publish(bid: gloas.ExecutionPayloadBid, signal: AbortSignal): Promise<gloas.SignedExecutionPayloadBid>;
  async publish(bid: gloas.ExecutionPayloadBid, signal: AbortSignal): Promise<gloas.SignedExecutionPayloadBid> {
    signal.throwIfAborted();
    this.modules.ledger.assertCanRecordBid({
      slot: bid.slot,
      parentBlockHash: toRootHex(bid.parentBlockHash),
      parentBlockRoot: toRootHex(bid.parentBlockRoot),
    });
    const signed = this.prepare(bid);
    await this.gossip(signed, signal);
    return signed;
  }

  /** Sign a retained payload and journal the commitment before the API can expose it. */
  prepare(bid: gloas.ExecutionPayloadBid): gloas.SignedExecutionPayloadBid {
    const {config, builderIndex, hasPayload, ledger, signer} = this.modules;
    const fork = config.getForkName(bid.slot);
    if (!isForkPostGloas(fork)) {
      throw new BidPublisherError({code: BidPublisherErrorCode.PRE_GLOAS_BID, slot: bid.slot});
    }
    if (bid.builderIndex !== builderIndex) {
      throw new BidPublisherError(
        {
          code: BidPublisherErrorCode.BUILDER_INDEX_MISMATCH,
          builderIndex,
          bidBuilderIndex: bid.builderIndex,
        },
        `Bid builder index does not match local builder index builderIndex=${builderIndex} bidBuilderIndex=${bid.builderIndex}`
      );
    }

    const identity: BidIdentity = {
      slot: bid.slot,
      parentBlockHash: toRootHex(bid.parentBlockHash),
      parentBlockRoot: toRootHex(bid.parentBlockRoot),
      blockHash: toRootHex(bid.blockHash),
    };
    if (!hasPayload(identity)) {
      throw new BidPublisherError(
        {code: BidPublisherErrorCode.PAYLOAD_NOT_RETAINED, ...identity},
        `Bid payload is not retained slot=${identity.slot} parentBlockHash=${identity.parentBlockHash} parentBlockRoot=${identity.parentBlockRoot} blockHash=${identity.blockHash}`
      );
    }

    const signedExecutionPayloadBid = signer.signExecutionPayloadBid(bid);
    const signedBidRoot = toRootHex(
      sszTypesFor(fork, "SignedExecutionPayloadBid").hashTreeRoot(signedExecutionPayloadBid)
    );
    ledger.recordRevision({...identity, valueGwei: bid.value, signedBidRoot});
    this.modules.retainCommitment?.(signedExecutionPayloadBid);
    return signedExecutionPayloadBid;
  }

  async gossip(signedExecutionPayloadBid: gloas.SignedExecutionPayloadBid, signal: AbortSignal): Promise<void> {
    signal.throwIfAborted();
    const {api, ledger, config, hasPayload, builderIndex} = this.modules;
    const bid = signedExecutionPayloadBid.message;
    const fork = config.getForkName(bid.slot);
    if (!isForkPostGloas(fork))
      throw new BidPublisherError({code: BidPublisherErrorCode.PRE_GLOAS_BID, slot: bid.slot});
    const identity = {
      slot: bid.slot,
      parentBlockHash: toRootHex(bid.parentBlockHash),
      parentBlockRoot: toRootHex(bid.parentBlockRoot),
      blockHash: toRootHex(bid.blockHash),
    };
    const signedBidRoot = toRootHex(
      sszTypesFor(fork, "SignedExecutionPayloadBid").hashTreeRoot(signedExecutionPayloadBid)
    );
    if (
      bid.builderIndex !== builderIndex ||
      !ledger.getBidsForSlot(bid.slot).some((record) => record.signedBidRoot === signedBidRoot)
    ) {
      throw new BidPublisherError({code: BidPublisherErrorCode.BID_NOT_PREPARED, slot: bid.slot});
    }
    if (!hasPayload(identity))
      throw new BidPublisherError({code: BidPublisherErrorCode.PAYLOAD_NOT_RETAINED, ...identity});
    ledger.recordBid({
      ...identity,
      valueGwei: bid.value,
      signedBidRoot,
    });

    const response = await api.beacon.publishExecutionPayloadBid({signedExecutionPayloadBid}, {signal});
    response.assertOk();
  }
}
