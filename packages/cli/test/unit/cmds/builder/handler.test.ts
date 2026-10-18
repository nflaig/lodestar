import {describe, expect, it} from "vitest";
import {chainConfig} from "@lodestar/config/default";
import {LogLevel} from "@lodestar/utils";
import {builderHandler, parseProposerPreferencesUrls} from "../../../../src/cmds/builder/handler.js";
import {IBuilderCliArgs, builderOptions} from "../../../../src/cmds/builder/options.js";
import {GlobalArgs} from "../../../../src/options/index.js";
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
