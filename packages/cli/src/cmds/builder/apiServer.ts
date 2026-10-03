import {Endpoint} from "@lodestar/api";
import {BuilderApiMethods, getRoutes} from "@lodestar/api/builder/server";
import {FastifyRoute} from "@lodestar/api/server";
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

export type BuilderRestApiServerModules = RestApiServerModules & {
  config: ChainForkConfig;
  api: BuilderApi;
};

export class BuilderRestApiServer extends RestApiServer {
  constructor(optsArg: Partial<RestApiServerOpts>, modules: BuilderRestApiServerModules) {
    const opts = {
      ...builderRestApiServerOptsDefault,
      // optsArg is a Partial type, any of its properties can be undefined
      ...Object.fromEntries(Object.entries(optsArg).filter(([_, v]) => v != null)),
    };

    super(opts, modules);

    // Only the routes implemented by the builder are registered, routes of earlier forks are not served
    const routes = getRoutes(modules.config, modules.api as BuilderApiMethods);
    for (const operationId of Object.keys(modules.api) as (keyof BuilderApi)[]) {
      this.server.route(routes[operationId] as FastifyRoute<Endpoint>);
    }
  }
}
