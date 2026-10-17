import {Endpoint} from "@lodestar/api";
import {BuilderApiMethods, getRoutes} from "@lodestar/api/builder/server";
import {ApiError, FastifyRoute, canSendResponse} from "@lodestar/api/server";
import {RestApiServer, RestApiServerModules, RestApiServerOpts} from "@lodestar/beacon-node";
import {BuilderApi} from "@lodestar/builder";
import {ChainForkConfig} from "@lodestar/config";

export const builderRestApiServerOptsDefault: RestApiServerOpts = {
  address: "127.0.0.1",
  port: 18550,
  // Signed beacon blocks are the largest request body
  bodyLimit: 10 * 1024 * 1024, // 10MB
  stacktraces: false,
};

const USER_AGENT_HEADER = "user-agent";
const CF_CONNECTING_IP_HEADER = "cf-connecting-ip";
const FORWARDED_FOR_HEADER = "x-forwarded-for";
const DATE_MILLISECONDS_HEADER = "date-milliseconds";
const TIMEOUT_MS_HEADER = "x-timeout-ms";
// Set by a Cloudflare transform rule from http.request.timestamp.sec and .msec
const EDGE_RECEIVED_SEC_HEADER = "x-edge-received-sec";
const EDGE_RECEIVED_MSEC_HEADER = "x-edge-received-msec";
/** Upper bound for the leg from a proposer to the Cloudflare edge that receives its request, the closest one to it */
const EDGE_ALLOWANCE_MS = 25;

export type BuilderRestApiServerModules = RestApiServerModules & {
  config: ChainForkConfig;
  api: BuilderApi;
};

/**
 * Builder API that can be served before the builder is initialized. Until then the builder is reported
 * as healthy, has no bids and does not accept requests it would have to validate.
 */
export function getBuilderApiBeforeInit(config: ChainForkConfig, getBuilderApi: () => BuilderApi | null): BuilderApi {
  return {
    async status(args, context) {
      return getBuilderApi()?.status(args, context);
    },

    async getExecutionPayloadBid(args, context) {
      return (
        getBuilderApi()?.getExecutionPayloadBid(args, context) ?? {
          data: undefined,
          meta: {version: config.getForkName(args.slot)},
          status: 204,
        }
      );
    },

    async submitSignedBeaconBlock(args, context) {
      const builderApi = getBuilderApi();
      if (builderApi === null) {
        throw new ApiError(503, "Builder is not ready");
      }
      return builderApi.submitSignedBeaconBlock(args, context);
    },

    async submitBuilderPreferences(args, context) {
      const builderApi = getBuilderApi();
      if (builderApi === null) {
        throw new ApiError(503, "Builder is not ready");
      }
      return builderApi.submitBuilderPreferences(args, context);
    },
  };
}

export class BuilderRestApiServer extends RestApiServer {
  constructor(optsArg: Partial<RestApiServerOpts>, modules: BuilderRestApiServerModules) {
    const opts = {
      ...builderRestApiServerOptsDefault,
      // optsArg is a Partial type, any of its properties can be undefined
      ...Object.fromEntries(Object.entries(optsArg).filter(([_, v]) => v != null)),
    };

    super(opts, modules);

    // Bid eligibility is short-lived. Intermediaries must never replay an
    // earlier response after the constructor has fenced its order generation.
    this.server.addHook("onRequest", async (_req, res) => {
      res.header("Cache-Control", "no-store");
    });
    this.server.addHook("onSend", (_req, res, payload, done) => {
      if (!canSendResponse(res)) {
        res.code(204);
        res.removeHeader("Content-Length");
        done(null, null);
      } else done(null, payload);
    });

    // The bid wait budget counts from Date-Milliseconds, the proposer's own clock, which can be off by hundreds of
    // milliseconds. When Cloudflare received the request, its timestamp minus the edge allowance replaces it.
    const clientDateMs = new WeakMap<object, number>();
    this.server.addHook("onRequest", async (req) => {
      const edgeMs = edgeReceivedMs(req.headers);
      const dateMs = Number(firstHeaderValue(req.headers[DATE_MILLISECONDS_HEADER]));
      if (edgeMs === undefined || !Number.isFinite(dateMs)) {
        return;
      }
      clientDateMs.set(req, dateMs);
      req.headers[DATE_MILLISECONDS_HEADER] = String(edgeMs - EDGE_ALLOWANCE_MS);
    });

    // Only the routes implemented by the builder are registered, routes of earlier forks are not served
    const routes = getRoutes(modules.config, modules.api as BuilderApiMethods);
    for (const operationId of Object.keys(modules.api) as (keyof BuilderApi)[]) {
      this.server.route(routes[operationId] as FastifyRoute<Endpoint>);
    }

    // Requests of proposers are logged with the details of the caller, the builder itself only sees their content
    this.server.addHook("onResponse", async (req, res) => {
      const {operationId} = (req.routeOptions.schema ?? {}) as {operationId?: string};
      // Requests to unknown routes are logged by the not found handler
      if (operationId === undefined) {
        return;
      }

      const {slot, proposer_pubkey: proposer} = (req.params ?? {}) as {slot?: number; proposer_pubkey?: string};
      const dateMs = Number(req.headers[DATE_MILLISECONDS_HEADER]);
      const clientMs = clientDateMs.get(req);
      // Routes without path params or timing headers only log the fields they have
      const logCtx = removeUndefined({
        operationId,
        status: res.statusCode,
        slot,
        proposer,
        userAgent: req.headers[USER_AGENT_HEADER],
        // The proxies in front of the builder API pass on the address of the caller
        ip: firstHeaderValue(req.headers[CF_CONNECTING_IP_HEADER] ?? req.headers[FORWARDED_FOR_HEADER]) ?? req.ip,
        timeoutMs: firstHeaderValue(req.headers[TIMEOUT_MS_HEADER]),
        // Time the request took to arrive, from the Cloudflare edge time if there is one, else the caller's
        transitMs: Number.isFinite(dateMs) ? Math.round(Date.now() - res.elapsedTime - dateMs) : undefined,
        clientTransitMs: clientMs !== undefined ? Math.round(Date.now() - res.elapsedTime - clientMs) : undefined,
        durationMs: Math.round(res.elapsedTime),
      });

      if (operationId === "getExecutionPayloadBid" || operationId === "submitSignedBeaconBlock") {
        this.logger.info("Builder API request", logCtx);
      } else {
        this.logger.debug("Builder API request", logCtx);
      }
    });
  }

  /** Unlike the other REST API servers, the builder API is meant to be reachable by untrusted proposers */
  async listen(): Promise<void> {
    try {
      await this.server.listen({port: this.opts.port, host: this.opts.address});
      const {address, port} = this.server.addresses()[0];
      this.logger.info("Started builder API server", {address: `http://${address}:${port}`});
    } catch (e) {
      this.logger.error("Error starting builder API server", this.opts, e as Error);
      throw e;
    }
  }
}

function edgeReceivedMs(headers: Record<string, string | string[] | undefined>): number | undefined {
  const sec = Number(firstHeaderValue(headers[EDGE_RECEIVED_SEC_HEADER]));
  const msec = Number(firstHeaderValue(headers[EDGE_RECEIVED_MSEC_HEADER]));
  if (!Number.isInteger(sec) || sec <= 0 || !Number.isInteger(msec) || msec < 0 || msec > 999) {
    return undefined;
  }
  return sec * 1000 + msec;
}

function firstHeaderValue(value: string | string[] | undefined): string | undefined {
  const first = Array.isArray(value) ? value[0] : value;
  return first?.split(",")[0].trim();
}

function removeUndefined<T extends Record<string, unknown>>(obj: T): Partial<T> {
  return Object.fromEntries(Object.entries(obj).filter(([_, value]) => value !== undefined)) as Partial<T>;
}
