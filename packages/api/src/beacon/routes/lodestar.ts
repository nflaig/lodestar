import {Epoch, RootHex, Slot} from "@lodestar/types";
import {ApiClientResponse} from "../../interfaces.js";
import {HttpStatusCode} from "../../utils/client/httpStatusCode.js";
import {
  jsonType,
  ReqEmpty,
  reqEmpty,
  ReturnTypes,
  ReqSerializers,
  RoutesData,
  sameType,
  Schema,
} from "../../utils/index.js";
import {FilterGetPeers, NodePeer, PeerDirection, PeerState} from "./node.js";

// See /packages/api/src/routes/index.ts for reasoning and instructions to add new routes

export type SyncChainDebugState = {
  targetRoot: string | null;
  targetSlot: number | null;
  syncType: string;
  status: string;
  startEpoch: number;
  peers: number;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  batches: any[];
};

export type GossipQueueItem = {
  topic: unknown;
  propagationSource: string;
  data: Uint8Array;
  addedTimeMs: number;
  seenTimestampSec: number;
};

export type PeerScoreStat = {
  peerId: string;
  lodestarScore: number;
  gossipScore: number;
  ignoreNegativeGossipScore: boolean;
  score: number;
  lastUpdate: number;
};

export type GossipPeerScoreStat = {
  peerId: string;
  // + Other un-typed options
};

export type RegenQueueItem = {
  key: string;
  args: unknown;
  addedTimeMs: number;
};

export type BlockProcessorQueueItem = {
  blockSlots: Slot[];
  jobOpts: Record<string, string | number | boolean | undefined>;
  addedTimeMs: number;
};

export type StateCacheItem = {
  slot: Slot;
  root: RootHex;
  /** Total number of reads */
  reads: number;
  /** Unix timestamp (ms) of the last read */
  lastRead: number;
  checkpointState: boolean;
};

export type LodestarNodePeer = NodePeer & {
  agentVersion: string;
};

export type LodestarThreadType = "main" | "network" | "discv5";

export type Api = {
  /** Trigger to write a heapdump of either main/network/discv5 thread to disk at `dirpath`. May take > 1min */
  writeHeapdump(
    thread?: LodestarThreadType,
    dirpath?: string
  ): Promise<ApiClientResponse<{[HttpStatusCode.OK]: {data: {filepath: string}}}>>;
  /** Trigger to write profile of either main/network/discv5 thread to disk */
  writeProfile(
    thread?: LodestarThreadType,
    duration?: number,
    dirpath?: string
  ): Promise<ApiClientResponse<{[HttpStatusCode.OK]: {data: {filepath: string}}}>>;
  /** TODO: description */
  getLatestWeakSubjectivityCheckpointEpoch(): Promise<ApiClientResponse<{[HttpStatusCode.OK]: {data: Epoch}}>>;
  /** TODO: description */
  getSyncChainsDebugState(): Promise<ApiClientResponse<{[HttpStatusCode.OK]: {data: SyncChainDebugState[]}}>>;
  /** Dump all items in a gossip queue, by gossipType */
  getGossipQueueItems(gossipType: string): Promise<ApiClientResponse<{[HttpStatusCode.OK]: {data: unknown[]}}>>;
  /** Dump all items in the regen queue */
  getRegenQueueItems(): Promise<ApiClientResponse<{[HttpStatusCode.OK]: {data: RegenQueueItem[]}}>>;
  /** Dump all items in the block processor queue */
  getBlockProcessorQueueItems(): Promise<ApiClientResponse<{[HttpStatusCode.OK]: {data: BlockProcessorQueueItem[]}}>>;
  /** Dump a summary of the states in the block state cache and checkpoint state cache */
  getStateCacheItems(): Promise<ApiClientResponse<{[HttpStatusCode.OK]: {data: StateCacheItem[]}}>>;
  /** Dump peer gossip stats by peer */
  getGossipPeerScoreStats(): Promise<ApiClientResponse<{[HttpStatusCode.OK]: {data: GossipPeerScoreStat[]}}>>;
  /** Dump lodestar score stats by peer */
  getLodestarPeerScoreStats(): Promise<ApiClientResponse<{[HttpStatusCode.OK]: {data: PeerScoreStat[]}}>>;
  /** Run GC with `global.gc()` */
  runGC(): Promise<ApiClientResponse<{[HttpStatusCode.OK]: void}>>;
  /** Drop all states in the state cache */
  dropStateCache(): Promise<ApiClientResponse<{[HttpStatusCode.OK]: void}>>;

  /** Connect to peer at this multiaddress */
  connectPeer(peerId: string, multiaddrStrs: string[]): Promise<ApiClientResponse<{[HttpStatusCode.OK]: void}>>;
  /** Disconnect peer */
  disconnectPeer(peerId: string): Promise<ApiClientResponse<{[HttpStatusCode.OK]: void}>>;
  /** Same to node api with new fields */
  getPeers(
    filters?: FilterGetPeers
  ): Promise<ApiClientResponse<{[HttpStatusCode.OK]: {data: LodestarNodePeer[]; meta: {count: number}}}>>;

  /** Dump Discv5 Kad values */
  discv5GetKadValues(): Promise<ApiClientResponse<{[HttpStatusCode.OK]: {data: string[]}}>>;

  /**
   * Dump level-db entry keys for a given Bucket declared in code, or for all buckets.
   * @param bucket must be the string name of a bucket entry: `allForks_blockArchive`
   */
  dumpDbBucketKeys(bucket: string): Promise<ApiClientResponse<{[HttpStatusCode.OK]: {data: string[]}}>>;

  /** Return all entries in the StateArchive index with bucket index_stateArchiveRootIndex */
  dumpDbStateIndex(): Promise<ApiClientResponse<{[HttpStatusCode.OK]: {data: {root: RootHex; slot: Slot}[]}}>>;
};

/**
 * Define javascript values for each route
 */
export const routesData: RoutesData<Api> = {
  writeHeapdump: {url: "/eth/v1/lodestar/write_heapdump", method: "POST"},
  writeProfile: {url: "/eth/v1/lodestar/write_profile", method: "POST"},
  getLatestWeakSubjectivityCheckpointEpoch: {url: "/eth/v1/lodestar/ws_epoch", method: "GET"},
  getSyncChainsDebugState: {url: "/eth/v1/lodestar/sync_chains_debug_state", method: "GET"},
  getGossipQueueItems: {url: "/eth/v1/lodestar/gossip_queue_items/:gossipType", method: "GET"},
  getRegenQueueItems: {url: "/eth/v1/lodestar/regen_queue_items", method: "GET"},
  getBlockProcessorQueueItems: {url: "/eth/v1/lodestar/block_processor_queue_items", method: "GET"},
  getStateCacheItems: {url: "/eth/v1/lodestar/state_cache_items", method: "GET"},
  getGossipPeerScoreStats: {url: "/eth/v1/lodestar/gossip_peer_score_stats", method: "GET"},
  getLodestarPeerScoreStats: {url: "/eth/v1/lodestar/lodestar_peer_score_stats", method: "GET"},
  runGC: {url: "/eth/v1/lodestar/gc", method: "POST"},
  dropStateCache: {url: "/eth/v1/lodestar/drop_state_cache", method: "POST"},
  connectPeer: {url: "/eth/v1/lodestar/connect_peer", method: "POST"},
  disconnectPeer: {url: "/eth/v1/lodestar/disconnect_peer", method: "POST"},
  getPeers: {url: "/eth/v1/lodestar/peers", method: "GET"},
  discv5GetKadValues: {url: "/eth/v1/debug/discv5_kad_values", method: "GET"},
  dumpDbBucketKeys: {url: "/eth/v1/debug/dump_db_bucket_keys/:bucket", method: "GET"},
  dumpDbStateIndex: {url: "/eth/v1/debug/dump_db_state_index", method: "GET"},
};

export type ReqTypes = {
  writeHeapdump: {query: {thread?: LodestarThreadType; dirpath?: string}};
  writeProfile: {query: {thread?: LodestarThreadType; duration?: number; dirpath?: string}};
  getLatestWeakSubjectivityCheckpointEpoch: ReqEmpty;
  getSyncChainsDebugState: ReqEmpty;
  getGossipQueueItems: {params: {gossipType: string}};
  getRegenQueueItems: ReqEmpty;
  getBlockProcessorQueueItems: ReqEmpty;
  getStateCacheItems: ReqEmpty;
  getGossipPeerScoreStats: ReqEmpty;
  getLodestarPeerScoreStats: ReqEmpty;
  runGC: ReqEmpty;
  dropStateCache: ReqEmpty;
  connectPeer: {query: {peerId: string; multiaddr: string[]}};
  disconnectPeer: {query: {peerId: string}};
  getPeers: {query: {state?: PeerState[]; direction?: PeerDirection[]}};
  discv5GetKadValues: ReqEmpty;
  dumpDbBucketKeys: {params: {bucket: string}};
  dumpDbStateIndex: ReqEmpty;
};

export function getReqSerializers(): ReqSerializers<Api, ReqTypes> {
  return {
    writeHeapdump: {
      writeReq: (thread, dirpath) => ({query: {thread, dirpath}}),
      parseReq: ({query}) => [query.thread, query.dirpath],
      schema: {query: {dirpath: Schema.String}},
    },
    writeProfile: {
      writeReq: (thread, duration, dirpath) => ({query: {thread, duration, dirpath}}),
      parseReq: ({query}) => [query.thread, query.duration, query.dirpath],
      schema: {query: {dirpath: Schema.String}},
    },
    getLatestWeakSubjectivityCheckpointEpoch: reqEmpty,
    getSyncChainsDebugState: reqEmpty,
    getGossipQueueItems: {
      writeReq: (gossipType) => ({params: {gossipType}}),
      parseReq: ({params}) => [params.gossipType],
      schema: {params: {gossipType: Schema.StringRequired}},
    },
    getRegenQueueItems: reqEmpty,
    getBlockProcessorQueueItems: reqEmpty,
    getStateCacheItems: reqEmpty,
    getGossipPeerScoreStats: reqEmpty,
    getLodestarPeerScoreStats: reqEmpty,
    runGC: reqEmpty,
    dropStateCache: reqEmpty,
    connectPeer: {
      writeReq: (peerId, multiaddr) => ({query: {peerId, multiaddr}}),
      parseReq: ({query}) => [query.peerId, query.multiaddr],
      schema: {query: {peerId: Schema.StringRequired, multiaddr: Schema.StringArray}},
    },
    disconnectPeer: {
      writeReq: (peerId) => ({query: {peerId}}),
      parseReq: ({query}) => [query.peerId],
      schema: {query: {peerId: Schema.StringRequired}},
    },
    getPeers: {
      writeReq: (filters) => ({query: filters || {}}),
      parseReq: ({query}) => [query],
      schema: {query: {state: Schema.StringArray, direction: Schema.StringArray}},
    },
    discv5GetKadValues: reqEmpty,
    dumpDbBucketKeys: {
      writeReq: (bucket) => ({params: {bucket}}),
      parseReq: ({params}) => [params.bucket],
      schema: {params: {bucket: Schema.String}},
    },
    dumpDbStateIndex: reqEmpty,
  };
}

export function getReturnTypes(): ReturnTypes<Api> {
  return {
    writeHeapdump: sameType(),
    writeProfile: sameType(),
    getLatestWeakSubjectivityCheckpointEpoch: sameType(),
    getSyncChainsDebugState: jsonType("snake"),
    getGossipQueueItems: jsonType("snake"),
    getRegenQueueItems: jsonType("snake"),
    getBlockProcessorQueueItems: jsonType("snake"),
    getStateCacheItems: jsonType("snake"),
    getGossipPeerScoreStats: jsonType("snake"),
    getLodestarPeerScoreStats: jsonType("snake"),
    getPeers: jsonType("snake"),
    discv5GetKadValues: jsonType("snake"),
    dumpDbBucketKeys: sameType(),
    dumpDbStateIndex: sameType(),
  };
}
