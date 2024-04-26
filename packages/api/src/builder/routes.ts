/* eslint-disable @typescript-eslint/naming-convention */
import {fromHexString, toHexString} from "@chainsafe/ssz";
import {ssz, allForks, bellatrix, Slot, Root, BLSPubkey} from "@lodestar/types";
import {ForkName, isForkExecution, isForkBlobs, ForkSeq} from "@lodestar/params";
import {ChainForkConfig} from "@lodestar/config";

import {Endpoint, RouteDefinitions, Schema} from "../utils/index.js";
import {
  ArrayOf,
  EmptyArgs,
  EmptyGetRequestCodec,
  EmptyMeta,
  EmptyRequest,
  EmptyResponseCodec,
  EmptyResponseData,
  JsonOnlyReq,
  VersionCodec,
  VersionMeta,
  WithVersion,
} from "../utils/codecs.js";
import {toForkName} from "../utils/serdes.js";
import {fromRequestHeaders} from "../utils/headers.js";

// See /packages/api/src/routes/index.ts for reasoning and instructions to add new routes

// Relays might not return any data if there are no bids from builders or min-bid threshold was not reached.
// In this case, we receive a success response (204) which is not handled as an error. The generic response
// handler already checks the status code and will not attempt to parse the body, but it will return no value.
// It is important that this type indicates that there might be no value to ensure it is properly handled downstream.
type MaybeSignedBuilderBid = allForks.SignedBuilderBid | undefined;

const RegistrationsType = ArrayOf(ssz.bellatrix.SignedValidatorRegistrationV1);

export type Endpoints = {
  status: Endpoint<
    //
    "GET",
    EmptyArgs,
    EmptyRequest,
    EmptyResponseData,
    EmptyMeta
  >;

  registerValidator: Endpoint<
    "POST",
    {registrations: bellatrix.SignedValidatorRegistrationV1[]},
    {body: unknown},
    EmptyResponseData,
    EmptyMeta
  >;

  getHeader: Endpoint<
    "GET",
    {
      slot: Slot;
      parentHash: Root;
      proposerPubkey: BLSPubkey;
    },
    {params: {slot: Slot; parent_hash: string; pubkey: string}},
    MaybeSignedBuilderBid,
    VersionMeta
  >;

  submitBlindedBlock: Endpoint<
    "POST",
    {signedBlindedBlock: allForks.SignedBlindedBeaconBlock},
    {body: unknown; headers: {"Eth-Consensus-Version": string}},
    allForks.ExecutionPayload | allForks.ExecutionPayloadAndBlobsBundle,
    VersionMeta
  >;
};

/**
 * Define javascript values for each route
 */
export function getDefinitions(config: ChainForkConfig): RouteDefinitions<Endpoints> {
  return {
    status: {
      url: "/eth/v1/builder/status",
      method: "GET",
      req: EmptyGetRequestCodec,
      resp: EmptyResponseCodec,
    },
    registerValidator: {
      url: "/eth/v1/builder/validators",
      method: "POST",
      req: JsonOnlyReq({
        writeReqJson: ({registrations}) => ({body: RegistrationsType.toJson(registrations)}),
        parseReqJson: ({body}) => ({registrations: RegistrationsType.fromJson(body)}),
        schema: {body: Schema.ObjectArray},
      }),
      resp: EmptyResponseCodec,
    },
    getHeader: {
      url: "/eth/v1/builder/header/{slot}/{parent_hash}/{pubkey}",
      method: "GET",
      req: {
        writeReq: ({slot, parentHash, proposerPubkey: proposerPubKey}) => ({
          params: {slot, parent_hash: toHexString(parentHash), pubkey: toHexString(proposerPubKey)},
        }),
        parseReq: ({params}) => ({
          slot: params.slot,
          parentHash: fromHexString(params.parent_hash),
          proposerPubkey: fromHexString(params.pubkey),
        }),
        schema: {
          params: {slot: Schema.UintRequired, parent_hash: Schema.StringRequired, pubkey: Schema.StringRequired},
        },
      },
      resp: {
        data: WithVersion<MaybeSignedBuilderBid, VersionMeta>((fork: ForkName) => {
          if (!isForkExecution(fork)) throw new Error("TODO"); // TODO

          return ssz.allForksExecution[fork].SignedBuilderBid;
        }),
        meta: VersionCodec,
      },
    },
    submitBlindedBlock: {
      url: "/eth/v1/builder/blinded_blocks",
      method: "POST",
      req: {
        writeReqJson: (args) => {
          const slot = args.signedBlindedBlock.message.slot;
          return {
            body: config.getBlindedForkTypes(slot).SignedBeaconBlock.toJson(args.signedBlindedBlock),
            headers: {
              "Eth-Consensus-Version": config.getForkName(slot),
            },
          };
        },
        parseReqJson: ({body, headers}) => {
          const forkName = toForkName(fromRequestHeaders(headers, "Eth-Consensus-Version"));
          if (!isForkExecution(forkName)) throw new Error("TODO"); // TODO
          return {
            signedBlindedBlock: ssz[forkName].SignedBlindedBeaconBlock.fromJson(body),
          };
        },
        writeReqSsz: (args) => {
          const slot = args.signedBlindedBlock.message.slot;
          return {
            body: config.getBlindedForkTypes(slot).SignedBeaconBlock.serialize(args.signedBlindedBlock),
            headers: {
              "Eth-Consensus-Version": config.getForkName(slot),
            },
          };
        },
        parseReqSsz: ({body, headers}) => {
          const forkName = toForkName(fromRequestHeaders(headers, "Eth-Consensus-Version"));
          const forkSeq = config.forks[forkName].seq;
          if (forkSeq < ForkSeq.bellatrix) throw new Error("TODO"); // TODO
          return {
            signedBlindedBlock: ssz[forkName as "bellatrix"].SignedBlindedBeaconBlock.deserialize(body),
          };
        },
        schema: {
          body: Schema.Object,
          headers: {"Eth-Consensus-Version": Schema.StringRequired},
        },
      },
      resp: {
        data: WithVersion<allForks.ExecutionPayload | allForks.ExecutionPayloadAndBlobsBundle, {version: ForkName}>(
          (fork: ForkName) =>
            isForkBlobs(fork)
              ? ssz.allForksBlobs[fork].ExecutionPayloadAndBlobsBundle
              : isForkExecution(fork)
                ? ssz.allForksExecution[fork].ExecutionPayload
                : ssz.bellatrix.ExecutionPayload
        ),
        meta: VersionCodec,
      },
    },
  };
}
