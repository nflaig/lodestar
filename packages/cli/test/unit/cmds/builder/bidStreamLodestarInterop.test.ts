import {once} from "node:events";
import {readFileSync} from "node:fs";
import {ClientRequest} from "node:http";
import {Agent} from "node:https";
import {Socket, createConnection} from "node:net";
import {Server as TlsServer, createSecureContext, createServer} from "node:tls";
import {Mock, afterEach, describe, expect, it, vi} from "vitest";
import {SecretKey} from "@chainsafe/lodestar-z/blst";
import {routes} from "@lodestar/api";
import {createBeaconConfig} from "@lodestar/config";
import {getConfig} from "@lodestar/config/test-utils";
import {DOMAIN_BUILDER_REQUEST_AUTH, ForkName} from "@lodestar/params";
import {
  ZERO_HASH,
  computeDomain,
  computeSigningRoot,
  createSingleSignatureSetFromComponents,
  getExecutionPayloadBidSigningRoot,
  verifySignatureSet,
} from "@lodestar/state-transition";
import {gloas, ssz} from "@lodestar/types";
import {toHex} from "@lodestar/utils";
import {BlsSingleThreadVerifier} from "../../../../../beacon-node/src/chain/bls/singleThread.js";
import {BuilderApiClient, BuilderApiClientOpts} from "../../../../../beacon-node/src/execution/builder/apiClient.js";
import {getExecutionPayloadBidStream} from "../../../../../beacon-node/src/execution/builder/bidStream.js";
import {IClock} from "../../../../../beacon-node/src/util/clock.js";
import {getMockedLogger} from "../../../../../beacon-node/test/mocks/loggerMock.js";
import {BuilderBidStreamApi, getBuilderBidStreamApi} from "../../../../../builder/src/api/bidStream.js";
import {BuilderApi, BuilderApiModules, getBuilderApi} from "../../../../../builder/src/api/impl.js";
import {BidStore} from "../../../../../builder/src/services/bidStore.js";
import {OfferLease} from "../../../../../builder/src/services/offerEligibility.js";
import {ProposerDutiesTracker} from "../../../../../builder/src/services/proposerDutiesTracker.js";
import {getApiClientStub, mockApiResponse} from "../../../../../builder/test/unit/utils/apiStub.js";
import {ClockMock} from "../../../../../builder/test/unit/utils/clock.js";
import {BuilderRestApiServer} from "../../../../src/cmds/builder/apiServer.js";
import {testLogger} from "../../../utils.js";

const slot = 255;
const config = createBeaconConfig(getConfig(ForkName.gloas), Buffer.alloc(32, 1));
const proposerKey = SecretKey.fromBytes(Buffer.alloc(32, 2));
const proposerPubkey = proposerKey.toPublicKey().toBytes();
const builderKey = SecretKey.fromBytes(Buffer.alloc(32, 3));
const parentHash = Buffer.alloc(32, 3);
const parentRoot = Buffer.alloc(32, 4);
const authData = new TextEncoder().encode("127.0.0.1");

class InteropServer extends BuilderRestApiServer {
  get url(): string {
    return `http://127.0.0.1:${this.server.addresses()[0].port}`;
  }
}

// Both peers are the production Lodestar implementations; all keys and endpoints are local fixtures.
describe.each([false, true])("native Lodestar stream interop, adaptive timing %s", (adaptiveTiming) => {
  let server: InteropServer | undefined;
  let controller: AbortController;
  let bidStore: BidStore;
  let lease: OfferLease;
  let httpBid: Mock<BuilderApi["getExecutionPayloadBid"]>;
  let streamOpen: Mock<BuilderBidStreamApi["open"]>;
  let onStreamOpen: (() => void) | undefined;
  const bls = new BlsSingleThreadVerifier({metrics: null});
  const tlsServers: TlsServer[] = [];
  const tlsSockets = new Set<Socket>();
  const agents: Agent[] = [];
  const handshakeTimers = new Set<NodeJS.Timeout>();

  afterEach(async () => {
    controller?.abort();
    for (const agent of agents.splice(0)) agent.destroy();
    for (const socket of tlsSockets) socket.destroy();
    tlsSockets.clear();
    for (const timer of handshakeTimers) clearTimeout(timer);
    handshakeTimers.clear();
    await Promise.all(
      tlsServers.splice(0).map((server) => new Promise<void>((resolve) => server.close(() => resolve())))
    );
    vi.restoreAllMocks();
    await server?.close();
    server = undefined;
    onStreamOpen = undefined;
  });

  async function start(streamEnabled = true): Promise<void> {
    const api = getApiClientStub();
    api.validator.getProposerDutiesV2.mockResolvedValue(
      mockApiResponse({
        data: [{slot, validatorIndex: 7, pubkey: proposerPubkey}],
        meta: {executionOptimistic: false, dependentRoot: toHex(ZERO_HASH)},
      })
    );
    const clock = new ClockMock();
    clock.currentSlot = slot;
    bidStore = new BidStore();
    lease = {
      generation: "first",
      validUntil: performance.now() + 10_000,
      validUntilUnixMs: Date.now() + 10_000,
      ready: true,
    };
    addBid(10);
    controller = new AbortController();
    const modules: BuilderApiModules = {
      api,
      clock,
      config,
      logger: testLogger(),
      bidStore,
      proposerDutiesTracker: new ProposerDutiesTracker(api, clock),
      builderIndex: 0,
      metrics: null,
      signal: controller.signal,
    };
    const builderApi = getBuilderApi(modules, {authData});
    const streamApi = getBuilderBidStreamApi(modules, {authData});
    httpBid = vi.fn(builderApi.getExecutionPayloadBid);
    streamOpen = vi.fn(async (request) => {
      onStreamOpen?.();
      return streamApi.open(request);
    });
    server = new InteropServer(
      {
        address: "127.0.0.1",
        port: 0,
        bidStream: streamEnabled,
        bidStreamAdaptiveTiming: streamEnabled && adaptiveTiming,
      },
      {
        config,
        logger: modules.logger,
        metrics: null,
        api: {...builderApi, getExecutionPayloadBid: httpBid},
        getBidStreamApi: () => ({open: streamOpen, signal: controller.signal}),
      }
    );
    await server.listen();
  }

  function createClient(opts: BuilderApiClientOpts = {}) {
    const started = performance.now();
    const msFromSlot = vi.fn(() => performance.now() - started);
    const clock = {msFromSlot} as unknown as IClock;
    const client = new BuilderApiClient(
      {bidStream: true, bidDeadline: 300, ...opts},
      config,
      clock,
      bls,
      null,
      getMockedLogger(),
      controller.signal
    );
    return {client, msFromSlot};
  }

  function collect(client: BuilderApiClient, valid = true) {
    if (!server) throw Error("Server is not started");
    const requestEntry: routes.validator.BuilderEntry = {
      url: new TextEncoder().encode(server.url),
      auth: signedAuth(valid),
      builderPubkeys: [builderKey.toPublicKey().toBytes()],
      maxExecutionPayment: 0n,
      minBid: 0n,
      builderBoostFactor: 100n,
    };
    return client.getExecutionPayloadBids([requestEntry], slot, parentHash, parentRoot, proposerPubkey);
  }

  function addBid(value: number): gloas.SignedExecutionPayloadBid {
    const signedBid = bid(value);
    bidStore.add(signedBid, {generation: lease.generation, lease});
    return signedBid;
  }

  async function delayedTls(delayMs: number, onHandshake?: () => void): Promise<{url: string; agent: Agent}> {
    if (!server) throw Error("Server is not started");
    const target = new URL(server.url);
    const fixture = "../../../../../beacon-node/test/unit/execution/builder/fixtures/";
    const cert = readFileSync(new URL(`${fixture}bid-stream-localhost-cert.pem`, import.meta.url));
    const key = readFileSync(new URL(`${fixture}bid-stream-localhost-key.pem`, import.meta.url));
    const context = createSecureContext({cert, key});
    const tls = createServer({
      cert,
      key,
      SNICallback(_name, callback) {
        onHandshake?.();
        const timer = setTimeout(() => {
          handshakeTimers.delete(timer);
          callback(null, context);
        }, delayMs);
        handshakeTimers.add(timer);
      },
    });
    tls.on("connection", (socket) => {
      tlsSockets.add(socket);
      socket.on("error", () => {});
    });
    tls.on("secureConnection", (socket) => {
      tlsSockets.add(socket);
      const upstream = createConnection({host: target.hostname, port: Number(target.port)});
      tlsSockets.add(upstream);
      upstream.on("error", () => socket.destroy());
      socket.on("error", () => upstream.destroy());
      socket.on("close", () => upstream.destroy());
      upstream.on("close", () => socket.destroy());
      socket.pipe(upstream).pipe(socket);
    });
    tls.listen(0, "127.0.0.1");
    await once(tls, "listening");
    tlsServers.push(tls);
    const address = tls.address();
    if (!address || typeof address === "string") throw Error("Expected TCP listener");
    const agent = new Agent({ca: cert, servername: "localhost", keepAlive: true});
    agents.push(agent);
    return {url: `https://127.0.0.1:${address.port}`, agent};
  }

  function streamRequest(url: string, agent: Agent, timeoutMs = 600, requestTimeoutMs = timeoutMs + 250) {
    return {
      url,
      agent,
      slot,
      parentHash,
      parentRoot,
      proposerPubkey,
      requestAuth: signedAuth(),
      startedAt: performance.now(),
      timeoutMs,
      requestTimeoutMs,
      msFromSlot: () => 0,
    };
  }

  it("serves an already retained bid after cold TLS setup without renewing the original request budget", async () => {
    await start();
    const {url, agent} = await delayedTls(400);
    const request = streamRequest(url, agent);
    const startedAt = Date.now();
    const end = vi.spyOn(ClientRequest.prototype, "end");
    const result = await getExecutionPayloadBidStream(request);

    expect(result).toMatchObject({upgraded: true, opened: true, updates: 1});
    if (!result.bid) throw Error("Expected the retained bid");
    assertSignedBid(result.bid.signedBid, bid(10));
    expect(streamOpen).toHaveBeenCalledOnce();
    const sent = streamOpen.mock.calls[0][0];
    expect(sent.dateMilliseconds - startedAt).toBeGreaterThanOrEqual(350);
    expect(sent.timeoutMs).toBeGreaterThan(0);
    expect(sent.timeoutMs).toBeLessThan(250);
    // Refreshing the timestamp must not grant another complete 600ms after connection setup.
    expect(sent.dateMilliseconds + sent.timeoutMs).toBeLessThanOrEqual(startedAt + 605);
    expect(end).toHaveBeenCalledOnce();
    expect(httpBid).not.toHaveBeenCalled();
  });

  it("sends no upgrade request when TLS setup has consumed the nominal response budget", async () => {
    await start();
    const {url, agent} = await delayedTls(200);
    const end = vi.spyOn(ClientRequest.prototype, "end");
    const result = await getExecutionPayloadBidStream(streamRequest(url, agent, 100, 350));
    expect(result).toMatchObject({upgraded: false, opened: false, reason: "deadline"});
    expect(streamOpen).not.toHaveBeenCalled();
    expect(end).not.toHaveBeenCalled();
  });

  it.each(["abort", "deadline"])("stops during cold TLS setup on %s without sending a request", async (reason) => {
    await start();
    const abort = new AbortController();
    const {url, agent} = await delayedTls(400, reason === "abort" ? () => abort.abort() : undefined);
    const end = vi.spyOn(ClientRequest.prototype, "end");
    const began = performance.now();
    const result = await getExecutionPayloadBidStream({
      ...streamRequest(url, agent, 600, reason === "deadline" ? 80 : 850),
      signal: abort.signal,
    });
    expect(result.upgraded).toBe(false);
    expect([reason === "abort" ? "aborted" : "deadline", "error"]).toContain(result.reason);
    expect(performance.now() - began).toBeLessThan(350);
    expect(streamOpen).not.toHaveBeenCalled();
    expect(end).not.toHaveBeenCalled();
  });

  it("streams increasing revisions and then a lower eligible replacement without losing prior signed bids", async () => {
    await start();
    const {client, msFromSlot} = createClient({bidDeadline: 500});
    const pending = collect(client);
    // The first two clock reads schedule this single round; subsequent reads timestamp actual frames.
    const beforeFrames = msFromSlot.mock.calls.length;
    await vi.waitFor(() => expect(msFromSlot.mock.calls.length).toBeGreaterThan(beforeFrames), {interval: 5});
    const initialFrames = msFromSlot.mock.calls.length;
    const higher = addBid(30);
    await vi.waitFor(() => expect(msFromSlot.mock.calls.length).toBeGreaterThan(initialFrames), {interval: 5});
    lease.generation = "replacement";
    const lower = addBid(20);

    const bids = await pending;

    expect(bids).toHaveLength(1);
    assertSignedBid(bids[0].signedBid, lower);
    expect(streamOpen).toHaveBeenCalledOnce();
    expect(httpBid).not.toHaveBeenCalled();
    expect(bidStore.has(bid(10))).toBe(true);
    expect(bidStore.has(higher)).toBe(true);
    expect(bidStore.canOffer(higher)).toBe(false);
    expect(bidStore.canOffer(lower)).toBe(true);
  });

  it("returns a higher signed revision on a repeated get-bids call", async () => {
    await start();
    const {client, msFromSlot} = createClient();
    for (const value of [20, 40]) {
      const pending = collect(client);
      const beforeFrames = msFromSlot.mock.calls.length;
      await vi.waitFor(() => expect(msFromSlot.mock.calls.length).toBeGreaterThan(beforeFrames), {interval: 5});
      const expected = addBid(value);

      const bids = await pending;

      expect(bids).toHaveLength(1);
      assertSignedBid(bids[0].signedBid, expected);
    }
    expect(streamOpen).toHaveBeenCalledTimes(2);
    expect(httpBid).not.toHaveBeenCalled();
  });

  it("uses consecutive configured rounds and accepts a higher bid in the next round", async () => {
    await start();
    let opens = 0;
    onStreamOpen = () => {
      if (++opens === 2) addBid(40);
    };
    const {client} = createClient({bidDeadline: 150, bidRequests: 2});

    const bids = await collect(client);

    expect(bids).toHaveLength(2);
    assertSignedBid(bids[0].signedBid, bid(10));
    assertSignedBid(bids[1].signedBid, bid(40));
    expect(streamOpen).toHaveBeenCalledTimes(2);
    expect(httpBid).not.toHaveBeenCalled();
  });

  it("falls back to the real HTTP route when the server stream option is disabled", async () => {
    await start(false);
    const {client} = createClient();

    const bids = await collect(client);

    expect(bids).toHaveLength(1);
    assertSignedBid(bids[0].signedBid, bid(10));
    expect(streamOpen).not.toHaveBeenCalled();
    expect(httpBid).toHaveBeenCalledOnce();
  });

  it("rejects invalid request authentication locally before opening a connection", async () => {
    await start();
    const {client} = createClient();

    const bids = await collect(client, false);

    expect(bids).toEqual([]);
    expect(streamOpen).not.toHaveBeenCalled();
    expect(httpBid).not.toHaveBeenCalled();
  });

  it("rejects invalid request authentication at both real routes even after the client was registered", async () => {
    await start();
    const {client} = createClient();
    expect(await collect(client)).toHaveLength(1);

    const bids = await collect(client, false);

    expect(bids).toEqual([]);
    expect(streamOpen).toHaveBeenCalledTimes(2);
    expect(httpBid).toHaveBeenCalledOnce();
    expect(toHex(streamOpen.mock.calls[1][0].requestAuth.signature)).toBe(toHex(new Uint8Array(96)));
  });
});

function bid(value: number): gloas.SignedExecutionPayloadBid {
  const result = ssz.gloas.SignedExecutionPayloadBid.defaultValue();
  Object.assign(result.message, {
    slot,
    parentBlockHash: parentHash,
    parentBlockRoot: parentRoot,
    blockHash: Buffer.alloc(32, value),
    value,
  });
  result.signature = builderKey.sign(getExecutionPayloadBidSigningRoot(config, result.message)).toBytes();
  return result;
}

function signedAuth(valid = true): gloas.SignedBuilderRequestAuth {
  const message = {slot, data: authData};
  const domain = computeDomain(DOMAIN_BUILDER_REQUEST_AUTH, config.GENESIS_FORK_VERSION, ZERO_HASH);
  const signature = proposerKey.sign(computeSigningRoot(ssz.gloas.BuilderRequestAuth, message, domain)).toBytes();
  if (!valid) signature.fill(0);
  return {message, signature};
}

function assertSignedBid(actual: gloas.SignedExecutionPayloadBid, expected: gloas.SignedExecutionPayloadBid): void {
  expect(toHex(ssz.gloas.SignedExecutionPayloadBid.hashTreeRoot(actual))).toBe(
    toHex(ssz.gloas.SignedExecutionPayloadBid.hashTreeRoot(expected))
  );
  expect(
    verifySignatureSet(
      createSingleSignatureSetFromComponents(
        builderKey.toPublicKey().toBytes(),
        getExecutionPayloadBidSigningRoot(config, actual.message),
        actual.signature
      )
    )
  ).toBe(true);
}
