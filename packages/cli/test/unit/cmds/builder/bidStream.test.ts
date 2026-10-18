import {getEventListeners} from "node:events";
import {Socket, createConnection} from "node:net";
import {afterEach, beforeEach, describe, expect, it, vi} from "vitest";
import {WebSocket} from "ws";
import {ApiError} from "@lodestar/api/server";
import {BuilderBidStreamApi, RetainedBidStream} from "@lodestar/builder";
import {createChainForkConfig} from "@lodestar/config";
import {config} from "@lodestar/config/default";
import {ForkName} from "@lodestar/params";
import {gloas, ssz} from "@lodestar/types";
import {toHex} from "@lodestar/utils";
import {BuilderRestApiServer} from "../../../../src/cmds/builder/apiServer.js";
import {testLogger} from "../../../utils.js";

const root = toHex(Buffer.alloc(32, 1));
const parent = toHex(Buffer.alloc(32, 2));
const proposer = toHex(Buffer.alloc(48, 3));
const path = `/eth/v1/builder/execution_payload_bid_stream/5/${parent}/${root}/${proposer}`;
const auth = ssz.gloas.SignedBuilderRequestAuth.defaultValue();
auth.message.slot = 5;
auth.message.data = new TextEncoder().encode("builder.example.com");

class TestServer extends BuilderRestApiServer {
  url(): string {
    return `ws://127.0.0.1:${this.server.addresses()[0].port}`;
  }
  holdRequests(): void {
    this.server.addHook("onRequest", async () => new Promise<void>(() => {}));
  }
  upgradeListeners(): number {
    return this.server.server.listenerCount("upgrade");
  }
  inject: BuilderRestApiServer["server"]["inject"] = (...args: Parameters<BuilderRestApiServer["server"]["inject"]>) =>
    this.server.inject(...args);
}

describe("builder bid stream", () => {
  let server: TestServer;
  let api: BuilderBidStreamApi;
  let current: gloas.SignedExecutionPayloadBid | null;
  let allowed: boolean;
  let duration: number;
  let controller: AbortController;
  let listeners: Set<() => void>;
  const sockets: WebSocket[] = [];
  const rawSockets: Socket[] = [];

  beforeEach(() => {
    current = bid(10);
    allowed = true;
    duration = 2000;
    controller = new AbortController();
    listeners = new Set();
    api = {
      signal: controller.signal,
      open: vi.fn(
        async (): Promise<RetainedBidStream> => ({
          deadline: performance.now() + duration,
          signal: controller.signal,
          current: () => (allowed ? current : null),
          canOffer: () => allowed && !controller.signal.aborted,
          subscribe: (notify) => {
            listeners.add(notify);
            return () => listeners.delete(notify);
          },
        })
      ),
    };
  });

  afterEach(async () => {
    vi.restoreAllMocks();
    for (const socket of sockets.splice(0)) socket.terminate();
    for (const socket of rawSockets.splice(0)) socket.destroy();
    await server?.close();
    expect(listeners.size).toBe(0);
  });

  async function start(enabled = true, ready = true, holdRouting = false): Promise<void> {
    server = new TestServer(
      {address: "127.0.0.1", port: 0, bidStream: enabled},
      {
        config: holdRouting ? createChainForkConfig({...config, SLOT_DURATION_MS: 1000}) : config,
        logger: testLogger(),
        metrics: null,
        getBidStreamApi: () => (ready ? api : null),
        api: {
          status: async () => undefined,
          getExecutionPayloadBid: async () => ({data: current ?? undefined, meta: {version: ForkName.gloas}}),
          submitBuilderPreferences: vi.fn(),
          submitSignedBeaconBlock: vi.fn(),
        },
      }
    );
    if (holdRouting) server.holdRequests();
    await server.listen();
  }

  function headers(): Record<string, string> {
    return {
      "Eth-Consensus-Version": "gloas",
      "Date-Milliseconds": String(Date.now()),
      "X-Timeout-Ms": "2000",
      "X-Request-Auth": Buffer.from(ssz.gloas.SignedBuilderRequestAuth.serialize(auth)).toString("base64"),
    };
  }

  async function connect(overrides: Record<string, string | undefined> = {}, requestPath = path) {
    const socket = new WebSocket(server.url() + requestPath, {headers: {...headers(), ...overrides}});
    sockets.push(socket);
    const messages: Buffer[] = [];
    socket.on("message", (data, binary) => {
      expect(binary).toBe(true);
      messages.push(Buffer.from(data as Buffer));
    });
    const closed = new Promise<number>((resolve) => socket.once("close", (code) => resolve(code)));
    await new Promise<void>((resolve, reject) => {
      socket.once("open", resolve);
      socket.once("error", reject);
    });
    socket.on("error", () => {});
    return {socket, messages, closed};
  }

  async function rawUpgrade(requestPath: string, overrides: Record<string, string> = {}) {
    const url = new URL(server.url());
    const socket = createConnection({host: url.hostname, port: Number(url.port)});
    rawSockets.push(socket);
    let closed = false;
    let response = "";
    socket.on("close", () => {
      closed = true;
    });
    socket.on("error", () => {});
    socket.on("data", (data: Buffer) => {
      response += data.toString();
    });
    await new Promise<void>((resolve) => socket.once("connect", resolve));
    const requestHeaders = {
      Host: url.host,
      Connection: "Upgrade",
      Upgrade: "websocket",
      "Sec-WebSocket-Version": "13",
      "Sec-WebSocket-Key": Buffer.alloc(16, 1).toString("base64"),
      ...headers(),
      ...overrides,
    };
    socket.write(
      `GET ${requestPath} HTTP/1.1\r\n${Object.entries(requestHeaders)
        .map(([name, value]) => `${name}: ${value}`)
        .join("\r\n")}\r\n\r\n`
    );
    return {socket, isClosed: () => closed, response: () => response};
  }

  function bid(value: number): gloas.SignedExecutionPayloadBid {
    const result = ssz.gloas.SignedExecutionPayloadBid.defaultValue();
    result.message.slot = 5;
    result.message.value = value;
    result.message.blockHash = Buffer.alloc(32, value);
    return result;
  }

  function publish(value: number): void {
    current = bid(value);
    for (const notify of listeners) notify();
  }

  function decode(frame: Buffer): gloas.SignedExecutionPayloadBid {
    expect([...frame.subarray(0, 2)]).toEqual([1, 7]);
    return ssz.gloas.SignedExecutionPayloadBid.deserialize(frame.subarray(2));
  }

  it("is opt-in and leaves ordinary HTTP routes available", async () => {
    await start(false);
    await expect(connect()).rejects.toThrow("404");
    expect((await server.inject({method: "GET", url: "/eth/v1/builder/status"})).statusCode).toBe(200);
    expect(api.open).not.toHaveBeenCalled();
  });

  it.each(["/eth/v1/builder/status", "/not-a-builder-route"])(
    "closes raw upgrades on nonstream route %s",
    async (requestPath) => {
      await start();
      const client = await rawUpgrade(requestPath);
      await vi.waitFor(() => expect(client.isClosed()).toBe(true));
      expect(client.response()).not.toContain("101 Switching Protocols");
      expect(api.open).not.toHaveBeenCalled();
    }
  );

  it("closes raw upgrades with an undecodable route parameter", async () => {
    await start();
    const client = await rawUpgrade(path.replace("/5/", "/%FF/"));
    await vi.waitFor(() => expect(client.isClosed()).toBe(true));
    expect(api.open).not.toHaveBeenCalled();
  });

  it("enforces a raw upgrade deadline even before request hooks reach authentication", async () => {
    await start(true, true, true);
    const client = await rawUpgrade(path, {"X-Timeout-Ms": "24000"});
    await vi.waitFor(() => expect(client.isClosed()).toBe(true), {timeout: 3000});
    expect(api.open).not.toHaveBeenCalled();
  });

  it("releases authenticated capacity when ws rejects an invalid subprotocol", async () => {
    await start();
    for (let i = 0; i < 4; i++) {
      const invalid = await rawUpgrade(path, {"Sec-WebSocket-Protocol": ",,"});
      await vi.waitFor(() => expect(invalid.isClosed()).toBe(true));
      expect(invalid.response()).toContain("400");
    }
    const valid = await connect();
    await vi.waitFor(() => expect(valid.messages).toHaveLength(1));
  });

  it.each<Record<string, string>>([
    {"Sec-WebSocket-Version": "12"},
    {"Sec-WebSocket-Key": "invalid"},
    {Upgrade: "other"},
  ])("rejects malformed handshake fields before duty lookup", async (overrides) => {
    await start();
    const client = await rawUpgrade(path, overrides);
    await vi.waitFor(() => expect(client.isClosed()).toBe(true));
    expect(client.response()).toContain("400");
    expect(api.open).not.toHaveBeenCalled();
  });

  it("sends the current signed bid immediately and later revisions in the Gloas wire format", async () => {
    await start();
    const client = await connect();
    await vi.waitFor(() => expect(client.messages).toHaveLength(1));
    expect(ssz.gloas.SignedExecutionPayloadBid.toJson(decode(client.messages[0]))).toEqual(
      ssz.gloas.SignedExecutionPayloadBid.toJson(bid(10))
    );
    expect(api.open).toHaveBeenCalledWith(expect.objectContaining({slot: 5}));
    publish(20);
    await vi.waitFor(() => expect(client.messages).toHaveLength(2));
    expect(decode(client.messages[1]).message.value).toBe(20);
    expect((await server.inject({method: "GET", url: "/eth/v1/builder/status"})).statusCode).toBe(200);
    const ordinary = await server.inject({
      method: "POST",
      url: path.replace("_stream", ""),
      headers: headers(),
      payload: ssz.gloas.SignedBuilderRequestAuth.toJson(auth) as object,
    });
    expect(ordinary.statusCode).toBe(200);
    expect(ordinary.headers["cache-control"]).toBe("no-store");
  });

  it("waits for the first bid without inventing an empty bid", async () => {
    current = null;
    await start();
    const client = await connect();
    expect(client.messages).toHaveLength(0);
    publish(30);
    await vi.waitFor(() => expect(client.messages).toHaveLength(1));
    expect(decode(client.messages[0]).message.value).toBe(30);
  });

  it("observes lease-only eligibility renewal without a newly added bid", async () => {
    allowed = false;
    await start();
    const client = await connect();
    expect(client.messages).toHaveLength(0);
    allowed = true;
    await vi.waitFor(() => expect(client.messages).toHaveLength(1));
    expect(decode(client.messages[0]).message.value).toBe(10);
  });

  it("coalesces notifications and does not resend an identical frame", async () => {
    await start();
    const client = await connect();
    await vi.waitFor(() => expect(client.messages).toHaveLength(1));
    for (let value = 11; value <= 100; value++) publish(value);
    await vi.waitFor(() => expect(client.messages).toHaveLength(2));
    expect(decode(client.messages[1]).message.value).toBe(100);
    publish(100);
    await new Promise((resolve) => setImmediate(resolve));
    expect(client.messages).toHaveLength(2);
  });

  it("keeps at most one write in flight while a slow client falls behind", async () => {
    await start();
    const client = await connect();
    await vi.waitFor(() => expect(client.messages).toHaveLength(1));
    const original = WebSocket.prototype.send;
    const flush: Array<() => void> = [];
    const writes = vi.spyOn(WebSocket.prototype, "send").mockImplementation(function (
      this: WebSocket,
      data,
      options,
      callback
    ) {
      flush.push(() => original.call(this, data, options, callback));
    });
    publish(20);
    await vi.waitFor(() => expect(flush).toHaveLength(1));
    for (let value = 21; value <= 40; value++) publish(value);
    await new Promise((resolve) => setImmediate(resolve));
    expect(flush).toHaveLength(1);
    writes.mockRestore();
    flush[0]();
    await vi.waitFor(() => expect(client.messages).toHaveLength(3));
    expect(client.messages.map((frame) => decode(frame).message.value)).toEqual([10, 20, 40]);
  });

  it("checks eligibility after serialization and permits a lower replacement generation", async () => {
    await start();
    const client = await connect();
    await vi.waitFor(() => expect(client.messages).toHaveLength(1));
    const serialize = ssz.gloas.SignedExecutionPayloadBid.serialize.bind(ssz.gloas.SignedExecutionPayloadBid);
    const spy = vi.spyOn(ssz.gloas.SignedExecutionPayloadBid, "serialize").mockImplementation((value) => {
      allowed = false;
      return serialize(value);
    });
    publish(20);
    await new Promise((resolve) => setImmediate(resolve));
    expect(client.messages).toHaveLength(1);
    spy.mockRestore();
    allowed = true;
    publish(5);
    await vi.waitFor(() => expect(client.messages).toHaveLength(2));
    expect(decode(client.messages[1]).message.value).toBe(5);
  });

  it("closes at the response deadline and frees the subscription", async () => {
    duration = 80;
    await start();
    const client = await connect();
    expect(await client.closed).toBe(1000);
    expect(listeners.size).toBe(0);
    expect(client.messages).toHaveLength(1);
  });

  it("closes a no-bid stream with no data frame", async () => {
    duration = 80;
    current = null;
    await start();
    const client = await connect();
    expect(await client.closed).toBe(1000);
    expect(client.messages).toHaveLength(0);
  });

  it("returns no content when authentication consumes the available time", async () => {
    duration = -1;
    await start();
    await expect(connect()).rejects.toThrow("204");
    expect(listeners.size).toBe(0);
  });

  it("does not offer bids after builder shutdown", async () => {
    await start();
    const client = await connect();
    await vi.waitFor(() => expect(client.messages).toHaveLength(1));
    controller.abort();
    publish(20);
    expect(await client.closed).toBe(1001);
    expect(client.messages).toHaveLength(1);
  });

  it("releases limits on disconnect, including a connection with no bid", async () => {
    current = null;
    await start();
    const first = await connect();
    const second = await connect();
    const extra = [await connect(), await connect()];
    await expect(connect()).rejects.toThrow("429");
    first.socket.close();
    await first.closed;
    const third = await connect();
    expect(listeners.size).toBe(4);
    for (const client of extra) client.socket.close();
    second.socket.close();
    third.socket.close();
  });

  it("bounds the total number of open streams across distinct proposer identities", async () => {
    await start();
    await Promise.all(
      Array.from({length: 64}, (_, i) => connect({}, path.replace(proposer, toHex(Buffer.alloc(48, i)))))
    );
    await expect(connect({}, path.replace(proposer, toHex(Buffer.alloc(48, 90))))).rejects.toThrow();
    expect(api.open).toHaveBeenCalledTimes(64);
    expect(getEventListeners(controller.signal, "abort")).toHaveLength(1);
  });

  it("keeps stalled authentication counted until its underlying work settles", async () => {
    const finish: Array<(stream: RetainedBidStream) => void> = [];
    api.open = vi.fn(() => new Promise<RetainedBidStream>((resolve) => finish.push(resolve)));
    await start();
    const failures = Array.from({length: 64}, () => connect({"X-Timeout-Ms": "30"}));
    await Promise.all(failures.map((result) => expect(result).rejects.toThrow()));
    await expect(connect()).rejects.toThrow("429");
    const stream: RetainedBidStream = {
      deadline: 0,
      current: () => null,
      canOffer: () => false,
      subscribe: () => () => {},
    };
    for (const resolve of finish) resolve(stream);
    await new Promise((resolve) => setImmediate(resolve));
    api.open = vi.fn(async () => stream);
    await expect(connect()).rejects.toThrow("204");
  });

  it("does not reserve a proposer's authenticated quota for unfinished invalid requests", async () => {
    const open = api.open;
    const rejectAuth: Array<(error: Error) => void> = [];
    api.open = vi.fn((request) =>
      request.requestAuth.signature[0] === 42
        ? new Promise<RetainedBidStream>((_resolve, reject) => rejectAuth.push(reject))
        : open(request)
    );
    const invalidAuth = {...auth, signature: Buffer.from(auth.signature)};
    invalidAuth.signature[0] = 42;
    await start();
    const failed = Array.from({length: 4}, () =>
      expect(
        connect({
          "X-Request-Auth": Buffer.from(ssz.gloas.SignedBuilderRequestAuth.serialize(invalidAuth)).toString("base64"),
        })
      ).rejects.toThrow("401")
    );
    await vi.waitFor(() => expect(rejectAuth).toHaveLength(4));
    const valid = await connect();
    await vi.waitFor(() => expect(valid.messages).toHaveLength(1));
    for (const reject of rejectAuth) reject(new ApiError(401, "invalid signature"));
    await Promise.all(failed);
  });

  it("closes pending authentication on builder abort without freeing unfinished work", async () => {
    let finish: ((stream: RetainedBidStream) => void) | undefined;
    api.open = vi.fn(
      () =>
        new Promise<RetainedBidStream>((resolve) => {
          finish = resolve;
        })
    );
    await start();
    const failure = expect(connect()).rejects.toThrow();
    await vi.waitFor(() => expect(api.open).toHaveBeenCalledOnce());
    controller.abort();
    await failure;
    finish?.({deadline: 0, current: () => null, canOffer: () => false, subscribe: () => () => {}});
    await new Promise((resolve) => setImmediate(resolve));
    expect(getEventListeners(controller.signal, "abort")).toHaveLength(0);
  });

  it("closes pending authentication and removes its upgrade listener during server shutdown", async () => {
    let finish: ((stream: RetainedBidStream) => void) | undefined;
    api.open = vi.fn(
      () =>
        new Promise<RetainedBidStream>((resolve) => {
          finish = resolve;
        })
    );
    await start();
    const failure = expect(connect({"X-Timeout-Ms": "24000"})).rejects.toThrow();
    await vi.waitFor(() => expect(api.open).toHaveBeenCalledOnce());
    const started = performance.now();
    await server.close();
    await failure;
    expect(performance.now() - started).toBeLessThan(2000);
    expect(server.upgradeListeners()).toBe(0);
    finish?.({deadline: 0, current: () => null, canOffer: () => false, subscribe: () => () => {}});
    await new Promise((resolve) => setImmediate(resolve));
    expect(getEventListeners(controller.signal, "abort")).toHaveLength(0);
  });

  it("coalesces adjacent client pings without closing a valid stream", async () => {
    await start();
    const client = await rawUpgrade(path);
    await vi.waitFor(() => expect(client.response()).toContain("101 Switching Protocols"));
    const pongs = vi.spyOn(WebSocket.prototype, "pong");
    const frame = (value: number): Buffer => Buffer.from([0x89, 0x81, 1, 2, 3, 4, value ^ 1]);
    client.socket.write(Buffer.concat([frame(8), frame(9)]));
    await vi.waitFor(() => expect(pongs).toHaveBeenCalledTimes(2));
    expect(pongs.mock.calls[1][0]).toEqual(Buffer.from([9]));
    expect(client.isClosed()).toBe(false);
    expect(listeners.size).toBe(1);
  });

  it("bounds pong writes from a client flooding pings without reading", async () => {
    await start();
    const client = await connect();
    await vi.waitFor(() => expect(client.messages).toHaveLength(1));
    const pongs = vi.spyOn(WebSocket.prototype, "pong");
    const closes = vi.spyOn(WebSocket.prototype, "close");
    client.socket.pause();
    for (let i = 0; i < 100; i++) client.socket.ping(Buffer.alloc(125, i));
    await vi.waitFor(() => expect(listeners.size).toBe(0));
    expect(pongs.mock.calls.filter((args) => args[1] === false).length).toBeLessThanOrEqual(32);
    expect(closes).toHaveBeenCalledWith(1008);
    client.socket.resume();
    await client.closed;
  });

  it("answers WebSocket pings and accepts automatic pong replies", async () => {
    await start();
    const client = await connect();
    const pong = new Promise<Buffer>((resolve) => client.socket.once("pong", resolve));
    client.socket.ping(Buffer.from("test"));
    expect(await pong).toEqual(Buffer.from("test"));
  });

  it("rejects client data messages on a server-only stream", async () => {
    await start();
    const client = await connect();
    client.socket.send("untrusted");
    expect(await client.closed).toBe(1008);
  });

  it("rejects a plain GET and a not-yet-initialized builder", async () => {
    await start(true, false);
    expect((await server.inject({method: "GET", url: path})).statusCode).toBe(426);
    await expect(connect()).rejects.toThrow("503");
    expect(api.open).not.toHaveBeenCalled();
  });

  it.each<Record<string, string>>([
    {"Eth-Consensus-Version": "fulu"},
    {"Date-Milliseconds": "-1"},
    {"Date-Milliseconds": "9007199254740992"},
    {"X-Timeout-Ms": "1.5"},
    {"X-Request-Auth": ""},
    {"X-Request-Auth": "!!!!"},
    {"X-Request-Auth": "AAAA"},
    {"X-Request-Auth": "A".repeat(5616)},
  ])("rejects malformed upgrade headers before authentication", async (invalid) => {
    await start();
    await expect(connect(invalid)).rejects.toThrow("400");
    expect(api.open).not.toHaveBeenCalled();
  });

  it.each(["-1", "9007199254740992", "1.5"])("rejects invalid slot %s", async (slot) => {
    await start();
    await expect(connect({}, path.replace("/5/", `/${slot}/`))).rejects.toThrow("400");
    expect(api.open).not.toHaveBeenCalled();
  });

  it.each([400, 401, 503])("preserves authentication status %i before upgrading", async (status) => {
    api.open = vi.fn(async () => {
      throw new ApiError(status, "rejected");
    });
    await start();
    await expect(connect()).rejects.toThrow(String(status));
    await expect(connect()).rejects.toThrow(String(status));
    await expect(connect()).rejects.toThrow(String(status));
  });
});
