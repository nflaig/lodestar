import fs from "node:fs";
import path from "node:path";
import {getClient} from "@lodestar/api";
import {
  RegistryMetricCreator,
  collectNodeJSMetrics,
  getHttpMetricsServer,
  initializeExecutionEngine,
} from "@lodestar/beacon-node";
import {Builder, EnginePayloadResult, EnginePayloadSource, getMetrics} from "@lodestar/builder";
import {getNodeLogger} from "@lodestar/logger/node";
import {fromHex, toPrintableUrl} from "@lodestar/utils";
import {getBeaconConfigFromArgs} from "../../config/beaconParams.js";
import {GlobalArgs} from "../../options/index.js";
import {getGlobalPaths} from "../../paths/global.js";
import {
  YargsError,
  cleanOldLogFiles,
  extractJwtHexSecret,
  onGracefulShutdown,
  parseFeeRecipient,
  parseLoggerArgs,
} from "../../util/index.js";
import {getVersionData} from "../../util/version.js";
import {BuilderRestApiServer} from "./apiServer.js";
import {loadBuilderKeypair} from "./loadKeypair.js";
import {IBuilderCliArgs, builderMetricsDefaultOptions} from "./options.js";

const ZERO_ADDRESS = "0x" + "0".repeat(40);

export async function builderHandler(args: IBuilderCliArgs & GlobalArgs): Promise<void> {
  const {config, network} = getBeaconConfigFromArgs(args);

  if (config.GLOAS_FORK_EPOCH === Infinity) {
    throw Error(`Gloas must be scheduled via GLOAS_FORK_EPOCH for network=${network}`);
  }

  const globalPaths = getGlobalPaths(args, network);
  const defaultLogFilepath = path.join(globalPaths.dataDir, "builder.log");
  const logger = getNodeLogger(parseLoggerArgs(args, {defaultLogFilepath}, config));

  try {
    cleanOldLogFiles(args, {defaultLogFilepath});
  } catch (e) {
    logger.debug("Not able to delete log files", {}, e as Error);
  }

  const {version, commit} = getVersionData();
  logger.info("Lodestar", {network, version, commit});

  const executionFeeRecipient = parseFeeRecipient(args.executionFeeRecipient);

  if (executionFeeRecipient === ZERO_ADDRESS) {
    throw Error("Cannot put zero address as an executionFeeRecipient");
  }

  const keypair = await loadBuilderKeypair(logger, args.keystore, args.keystorePassword, args.builderPubkey);

  const onGracefulShutdownCbs: (() => Promise<void> | void)[] = [];
  onGracefulShutdown(async () => {
    for (const cb of onGracefulShutdownCbs) await cb();
  }, logger.info.bind(logger));

  const abortController = new AbortController();
  onGracefulShutdownCbs.push(async () => abortController.abort());

  const register = args.metrics ? new RegistryMetricCreator() : null;
  const metrics = register && getMetrics(register, {version, commit, network});

  if (metrics) {
    const closeMetrics = collectNodeJSMetrics(register);
    onGracefulShutdownCbs.push(() => closeMetrics());

    const port = args["metrics.port"] ?? builderMetricsDefaultOptions.port;
    const address = args["metrics.address"] ?? builderMetricsDefaultOptions.address;
    const metricsServer = await getHttpMetricsServer({port, address}, {register, logger});

    onGracefulShutdownCbs.push(() => metricsServer.close());
  }

  const api = getClient(
    {urls: [args.beaconNodeUrl], globalInit: {signal: abortController.signal, timeoutMs: args.requestTimeout}},
    {config, logger, metrics: metrics?.restApiClient}
  );

  logger.info("Beacon node", {beaconNode: toPrintableUrl(args.beaconNodeUrl), timeoutMs: args.requestTimeout});

  // Payload ids are local to the execution client that issued them, fallback urls can't be used
  if (args["execution.urls"].length !== 1) {
    throw Error("Exactly one execution client url is required");
  }
  const executionUrl = args["execution.urls"][0];
  const engine = initializeExecutionEngine(
    {
      mode: "http",
      urls: [executionUrl],
      timeout: args["execution.timeout"],
      retries: args["execution.retries"],
      retryDelay: args["execution.retryDelay"],
      jwtSecretHex: args.jwtSecret ? extractJwtHexSecret(fs.readFileSync(args.jwtSecret, "utf-8").trim()) : undefined,
      jwtId: args.jwtId,
      version,
      commit,
    },
    {signal: abortController.signal, logger}
  );
  const payloadSource = new EnginePayloadSource(toPrintableUrl(executionUrl), {
    notifyForkchoiceUpdate: (fork, headBlockHash, safeBlockHash, finalizedBlockHash, payloadAttributes) =>
      engine.notifyForkchoiceUpdate(fork, headBlockHash, safeBlockHash, finalizedBlockHash, payloadAttributes),
    getPayload: async (fork, payloadId) => (await engine.getPayload(fork, payloadId)) as EnginePayloadResult,
  });

  const builder = await Builder.init({
    keypair,
    logger,
    config,
    abortController,
    api,
    executionFeeRecipient: fromHex(executionFeeRecipient),
    metrics,
    payloadSource,
    bidding: {
      shareBps: args["bidding.shareBps"],
      fixedCostGwei: args["bidding.fixedCostGwei"],
      subsidyGwei: args["bidding.subsidyGwei"],
      minValueGwei: args["bidding.minValueGwei"],
      maxValueGwei: args["bidding.maxValueGwei"],
      deadlineBps: args["bidding.deadlineBps"],
      getPayloadTimeout: args["bidding.getPayloadTimeout"],
      minOperatingBalanceGwei: args["bidding.minOperatingBalanceGwei"],
    },
    builderApi: args.builderApi ? {authData: getBuilderApiAuthData(args)} : undefined,
    reveal: {
      cutoffBps: args["reveal.cutoffBps"],
      adversarialWithholdExecutionPayload: args["adversarial.withhold.executionPayload"],
      adversarialDelayExecutionPayload: args["adversarial.delay.executionPayload"],
      adversarialDelayExecutionPayloadBps: args["adversarial.delay.executionPayloadBps"],
    },
  });

  onGracefulShutdownCbs.push(() => builder.close());

  if (builder.builderApi !== null) {
    const builderApiServer = new BuilderRestApiServer(
      {address: args["builderApi.address"], port: args["builderApi.port"]},
      {config, logger, api: builder.builderApi, metrics: null}
    );
    onGracefulShutdownCbs.push(() => builderApiServer.close());
    await builderApiServer.listen();
  }
}

function getBuilderApiAuthData(args: IBuilderCliArgs): Uint8Array {
  if (args["builderApi.authData"] !== undefined) {
    const authData = fromHex(args["builderApi.authData"]);
    if (authData.length === 0) {
      throw new YargsError("--builderApi.authData must not be empty");
    }
    return authData;
  }
  if (args["builderApi.publicUrl"] !== undefined) {
    // Same derivation as the default auth data of the validator client
    return new TextEncoder().encode(new URL(args["builderApi.publicUrl"]).hostname);
  }
  throw new YargsError("--builderApi requires either --builderApi.publicUrl or --builderApi.authData");
}
