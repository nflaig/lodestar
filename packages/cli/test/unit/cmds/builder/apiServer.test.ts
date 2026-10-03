import {afterEach, describe, expect, it, vi} from "vitest";
import {ApiError} from "@lodestar/api/server";
import {BuilderApi} from "@lodestar/builder";
import {config} from "@lodestar/config/default";
import {ssz} from "@lodestar/types";
import {toHex} from "@lodestar/utils";
import {BuilderRestApiServer} from "../../../../src/cmds/builder/apiServer.js";
import {testLogger} from "../../../utils.js";

class TestBuilderRestApiServer extends BuilderRestApiServer {
  inject: BuilderRestApiServer["server"]["inject"] = (...args: Parameters<BuilderRestApiServer["server"]["inject"]>) =>
    this.server.inject(...args);
}

describe("cmds / builder / api server", () => {
  let server: TestBuilderRestApiServer | undefined;

  afterEach(async () => {
    await server?.close();
  });

  function getServer(api: Partial<BuilderApi> = {}): TestBuilderRestApiServer {
    server = new TestBuilderRestApiServer(
      {},
      {
        config,
        logger: testLogger(),
        metrics: null,
        api: {
          status: vi.fn(),
          getExecutionPayloadBid: vi.fn(),
          submitSignedBeaconBlock: vi.fn(),
          submitBuilderPreferences: vi.fn(),
          ...api,
        },
      }
    );
    return server;
  }

  it("serves the builder status", async () => {
    const res = await getServer().inject({method: "GET", url: "/eth/v1/builder/status"});

    expect(res.statusCode).toBe(200);
  });

  it("does not serve routes of earlier forks", async () => {
    const res = await getServer().inject({method: "POST", url: "/eth/v1/builder/validators", payload: []});

    expect(res.statusCode).toBe(404);
  });

  it("returns the status code of a rejected request", async () => {
    const submitBuilderPreferences = vi.fn().mockRejectedValue(new ApiError(401, "signature verification failed"));

    const res = await getServer({submitBuilderPreferences}).inject({
      method: "POST",
      url: `/eth/v1/builder/builder_preferences/${toHex(Buffer.alloc(48, 1))}`,
      headers: {"Eth-Consensus-Version": "gloas"},
      payload: ssz.gloas.BuilderPreferencesRequest.toJson(ssz.gloas.BuilderPreferencesRequest.defaultValue()) as object,
    });

    expect(res.statusCode).toBe(401);
    expect(res.json()).toEqual({code: 401, message: "signature verification failed"});
    expect(submitBuilderPreferences).toHaveBeenCalledOnce();
  });
});
