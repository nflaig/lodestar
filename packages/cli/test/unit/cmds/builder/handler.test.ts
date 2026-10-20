import {describe, expect, it, vi} from "vitest";
import {SecretKey} from "@chainsafe/lodestar-z/blst";
import {Builder} from "@lodestar/builder";
import {chainConfig} from "@lodestar/config/default";
import {LogLevel} from "@lodestar/utils";
import {BuilderRestApiServer} from "../../../../src/cmds/builder/apiServer.js";
import {builderHandler, parseProposerPreferencesUrls} from "../../../../src/cmds/builder/handler.js";
import * as keyLoader from "../../../../src/cmds/builder/loadKeypair.js";
import {IBuilderCliArgs, builderOptions} from "../../../../src/cmds/builder/options.js";
import {GlobalArgs} from "../../../../src/options/index.js";
import * as cliUtils from "../../../../src/util/index.js";
import {testFilesDir} from "../../../utils.js";

describe("cmds / builder / args handler", () => {
  const ZERO_ADDRESS = "0x" + "0".repeat(40);
  const VALID_FEE_RECIPIENT = "0x" + "1".repeat(40);

  async function runBuilderHandler(
    args: Partial<IBuilderCliArgs & GlobalArgs> & Record<string, unknown>
  ): Promise<void> {
    return builderHandler({
      logLevel: LogLevel.info,
      logFileLevel: LogLevel.debug,
      dataDir: testFilesDir,
      executionFeeRecipient: VALID_FEE_RECIPIENT,
      "bidding.updateIntervalMs": 0,
      "bidding.updateCutoffBps": 1000,
      "bidding.maxRevisionsPerSlot": 256,
      beaconNodeUrl: "http://127.0.0.1:9596",
      "proposerPreferences.timeout": 1000,
      ...args,
    } as unknown as IBuilderCliArgs & GlobalArgs);
  }

  it("Should reject unscheduled Gloas", async () => {
    await expect(runBuilderHandler({})).rejects.toThrow("Gloas must be scheduled via GLOAS_FORK_EPOCH");
  });

  it("keeps adaptive stream timing disabled by default", () => {
    expect(builderOptions["builderApi.bidStreamAdaptiveTiming"].default).toBe(false);
  });

  it.each([
    {"builderApi.httpReturnReserveCapMs": -1},
    {"builderApi.httpReturnReserveCapMs": NaN},
    {"builderApi.httpResponseMarginMs": 0},
    {"builderApi.httpResponseMarginMs": 1001},
  ])("rejects invalid HTTP timing before loading keys: %s", async (args) => {
    await expect(runBuilderHandler({"params.GLOAS_FORK_EPOCH": "0", builderApi: true, ...args})).rejects.toThrow(
      "must be an integer between"
    );
  });

  it("requires the API when setting HTTP timing", async () => {
    await expect(
      runBuilderHandler({"params.GLOAS_FORK_EPOCH": "0", "builderApi.httpReturnReserveCapMs": 60})
    ).rejects.toThrow("HTTP bid response timing options require --builderApi");
  });

  it("forwards HTTP timing to the authenticated builder API", async () => {
    const secretKey = SecretKey.fromBytes(Buffer.alloc(32, 1));
    const load = vi.spyOn(keyLoader, "loadBuilderKeypair").mockResolvedValue({
      secretKey,
      publicKey: secretKey.toPublicKey(),
    });
    const shutdown = vi.spyOn(cliUtils, "onGracefulShutdown").mockImplementation(() => {});
    const listen = vi.spyOn(BuilderRestApiServer.prototype, "listen").mockResolvedValue();
    const init = vi.spyOn(Builder, "init").mockRejectedValue(Error("BUILDER_INIT_CAPTURED"));
    try {
      await expect(
        runBuilderHandler({
          "params.GLOAS_FORK_EPOCH": "0",
          builderApi: true,
          "builderApi.authData": "0x1234",
          "builderApi.httpReturnReserveCapMs": 60,
          "builderApi.httpResponseMarginMs": 50,
          "execution.urls": ["http://127.0.0.1:8551"],
        })
      ).rejects.toThrow("BUILDER_INIT_CAPTURED");
      expect(init).toHaveBeenCalledExactlyOnceWith(
        expect.objectContaining({
          builderApi: {authData: new Uint8Array([0x12, 0x34]), httpReturnReserveCapMs: 60, httpResponseMarginMs: 50},
        })
      );
    } finally {
      load.mockRestore();
      shutdown.mockRestore();
      listen.mockRestore();
      init.mockRestore();
    }
  });

  it.each([
    {executionFeeRecipient: ZERO_ADDRESS},
    {builderApi: true, "builderApi.bidStream": false},
    {builderApi: false, "builderApi.bidStream": true},
  ])("rejects adaptive timing without both API and streaming before loading keys", async (args) => {
    await expect(
      runBuilderHandler({"params.GLOAS_FORK_EPOCH": "0", "builderApi.bidStreamAdaptiveTiming": true, ...args})
    ).rejects.toThrow("--builderApi.bidStreamAdaptiveTiming requires --builderApi and --builderApi.bidStream");
  });

  it("accepts adaptive timing with API and streaming before unrelated validation", async () => {
    await expect(
      runBuilderHandler({
        "params.GLOAS_FORK_EPOCH": "0",
        builderApi: true,
        "builderApi.bidStream": true,
        "builderApi.bidStreamAdaptiveTiming": true,
        executionFeeRecipient: ZERO_ADDRESS,
      })
    ).rejects.toThrow("Cannot put zero address as an executionFeeRecipient");
  });

  it("Should reject zero executionFeeRecipient", async () => {
    // Rejecting with the fee recipient error rather than the Gloas error also proves the
    // fork guard read the epoch merged in from CLI args, not from the default config
    await expect(
      runBuilderHandler({
        "params.GLOAS_FORK_EPOCH": String(chainConfig.FULU_FORK_EPOCH + 1),
        executionFeeRecipient: ZERO_ADDRESS,
      })
    ).rejects.toThrow("Cannot put zero address as an executionFeeRecipient");
  });

  it.each([
    [{"payload.testingUrl": "http://127.0.0.1:8545"}, "--payload.transactionsFile is required"],
    [{"payload.transactionsFile": "transactions.json"}, "requires --payload.testingUrl"],
    [
      {
        "payload.transactionsFile": "transactions.json",
        "payload.testingUrl": "http://127.0.0.1:8545",
        "payload.dryRun": true,
      },
      "requires --payload.outputDir",
    ],
  ])("rejects incomplete controlled payload options before loading keys", async (args, message) => {
    await expect(runBuilderHandler({"params.GLOAS_FORK_EPOCH": "0", ...args})).rejects.toThrow(message);
  });

  it.each([0, -1, 257, 1.5, NaN, Infinity])("rejects invalid revision limit %s before loading keys", async (limit) => {
    await expect(
      runBuilderHandler({"params.GLOAS_FORK_EPOCH": "0", "bidding.maxRevisionsPerSlot": limit})
    ).rejects.toThrow("--bidding.maxRevisionsPerSlot must be between 1 and 256");
  });

  it("keeps retained alternate authorization disabled by default", () => {
    expect(builderOptions["payload.alternateOrderGenerations"].default).toBe(false);
  });

  it.each([
    {},
    {"payload.constructorUrl": "http://127.0.0.1:8547"},
    {"payload.constructorUrl": "http://127.0.0.1:8547", "bidding.alternateBranches": true},
  ])("requires constructor, alternate branches and updates for retained permissions %j", async (args) => {
    await expect(
      runBuilderHandler({"params.GLOAS_FORK_EPOCH": "0", "payload.alternateOrderGenerations": true, ...args})
    ).rejects.toThrow("--payload.alternateOrderGenerations requires");
  });

  it("keeps primary handoff opt-in and requires retained alternate permissions", async () => {
    expect(builderOptions["bidding.primaryHandoff"].default).toBe(false);
    await expect(runBuilderHandler({"params.GLOAS_FORK_EPOCH": "0", "bidding.primaryHandoff": true})).rejects.toThrow(
      "--bidding.primaryHandoff requires --bidding.alternateBranches and --payload.alternateOrderGenerations"
    );
  });

  it("requires a constructor for recoverable begins", async () => {
    await expect(
      runBuilderHandler({"params.GLOAS_FORK_EPOCH": "0", "payload.beginReservations": true})
    ).rejects.toThrow("--payload.beginReservations requires --payload.constructorUrl");
  });

  it("rejects alternate branches without a session source and updates", async () => {
    await expect(
      runBuilderHandler({"params.GLOAS_FORK_EPOCH": "0", "bidding.alternateBranches": true})
    ).rejects.toThrow("Alternate branches require --payload.constructorUrl and payload updates");
    await expect(
      runBuilderHandler({
        "params.GLOAS_FORK_EPOCH": "0",
        "bidding.alternateBranches": true,
        "payload.constructorUrl": "http://127.0.0.1:8547",
      })
    ).rejects.toThrow("Alternate branches require --payload.constructorUrl and payload updates");
  });

  it.each([0, 249, 1001, 500.5, NaN, Infinity])("rejects unbounded alternate duration %s", async (duration) => {
    await expect(
      runBuilderHandler({"params.GLOAS_FORK_EPOCH": "0", "bidding.alternateBuildDurationMs": duration})
    ).rejects.toThrow("--bidding.alternateBuildDurationMs must be between 250 and 1000");
  });

  it.each([-1, 499, 5001, 750.5, NaN, Infinity])("rejects invalid alternate refresh offset %s", async (lead) => {
    await expect(
      runBuilderHandler({"params.GLOAS_FORK_EPOCH": "0", "bidding.alternateRefreshBeforeSlotMs": lead})
    ).rejects.toThrow("--bidding.alternateRefreshBeforeSlotMs must be zero or between 500 and 5000");
  });

  it("requires alternate branches for a refresh before loading keys", async () => {
    await expect(
      runBuilderHandler({"params.GLOAS_FORK_EPOCH": "0", "bidding.alternateRefreshBeforeSlotMs": 750})
    ).rejects.toThrow("--bidding.alternateRefreshBeforeSlotMs requires --bidding.alternateBranches");
  });

  it.each([false, true])("forwards retained alternate opt-in %s to the session source", async (enabled) => {
    const secretKey = SecretKey.fromBytes(Buffer.alloc(32, 1));
    const load = vi
      .spyOn(keyLoader, "loadBuilderKeypair")
      .mockResolvedValue({secretKey, publicKey: secretKey.toPublicKey()});
    const shutdown = vi.spyOn(cliUtils, "onGracefulShutdown").mockImplementation(() => {});
    const init = vi.spyOn(Builder, "init").mockRejectedValue(Error("BUILDER_INIT_CAPTURED"));
    try {
      await expect(
        runBuilderHandler({
          "params.GLOAS_FORK_EPOCH": "0",
          "payload.constructorUrl": "http://127.0.0.1:8547",
          "bidding.updateIntervalMs": 100,
          "bidding.alternateBranches": true,
          "payload.alternateOrderGenerations": enabled,
        })
      ).rejects.toThrow("BUILDER_INIT_CAPTURED");
      const source = init.mock.calls[0][0].payloadSource;
      // The flag alone cannot enable the protocol; the native handshake is still mandatory.
      expect(source.supportsRetainedOffers).toBe(false);
      expect(source).toMatchObject({options: {retainedAlternateOffers: enabled}});
    } finally {
      init.mockRestore();
      shutdown.mockRestore();
      load.mockRestore();
    }
  });

  it.each([undefined, 800])("forwards alternate refresh %s to Builder initialization", async (lead) => {
    const secretKey = SecretKey.fromBytes(Buffer.alloc(32, 1));
    const load = vi.spyOn(keyLoader, "loadBuilderKeypair").mockResolvedValue({
      secretKey,
      publicKey: secretKey.toPublicKey(),
    });
    const shutdown = vi.spyOn(cliUtils, "onGracefulShutdown").mockImplementation(() => {});
    const init = vi.spyOn(Builder, "init").mockRejectedValue(Error("BUILDER_INIT_CAPTURED"));
    try {
      await expect(
        runBuilderHandler({
          "params.GLOAS_FORK_EPOCH": "0",
          "payload.constructorUrl": "http://127.0.0.1:8547",
          "bidding.updateIntervalMs": 100,
          "bidding.alternateBranches": true,
          "bidding.alternateRefreshBeforeSlotMs": lead,
        })
      ).rejects.toThrow("BUILDER_INIT_CAPTURED");
      expect(init).toHaveBeenCalledExactlyOnceWith(
        expect.objectContaining({bidding: expect.objectContaining({alternateRefreshBeforeSlotMs: lead ?? 0})})
      );
    } finally {
      load.mockRestore();
      shutdown.mockRestore();
      init.mockRestore();
    }
  });

  it.each([
    [
      {"proposerPreferences.urls": ["http://a:9596", "http://b:9596", "http://c:9596", "http://d:9596"]},
      "accepts at most 3 urls",
    ],
    [{"proposerPreferences.urls": ["127.0.0.1:9596"]}, "urls[0] is not a valid http(s) url"],
    [{"proposerPreferences.urls": ["http://127.0.0.1:9596"]}, "urls[0] duplicates --beaconNodeUrl"],
    [{"proposerPreferences.urls": ["http://a:9596", "http://a:9596/"]}, "urls[1] duplicates"],
    [{"proposerPreferences.urls": ["http://a:9596"], "proposerPreferences.timeout": 50}, "at least 100 ms"],
  ])("rejects invalid proposer preferences options before loading keys", async (args, message) => {
    await expect(runBuilderHandler({"params.GLOAS_FORK_EPOCH": "0", ...args})).rejects.toThrow(message);
  });

  it("parses comma separated proposer preferences urls without echoing credentials", () => {
    const coerce = builderOptions["proposerPreferences.urls"].coerce as (urls: string[]) => string[];
    const urls = coerce(["http://user:pass@a:9596,http://b:9596", "http://c:9596"]);
    expect(urls).toEqual(["http://user:pass@a:9596", "http://b:9596", "http://c:9596"]);
    expect(parseProposerPreferencesUrls(urls, "http://127.0.0.1:9596")).toBe(urls);
    expect(() => parseProposerPreferencesUrls(["http://user:pass@a:9596", "http://user:pass@a:9596/"], "nope")).toThrow(
      /^--proposerPreferences.urls\[1\] duplicates --beaconNodeUrl or an earlier url$/
    );
  });
});
