import {once} from "node:events";
import {type IncomingMessage, type ServerResponse, createServer} from "node:http";
import type {AddressInfo} from "node:net";
import {describe, expect, it} from "vitest";
import {createConstructorStatusReader} from "../../../../src/cmds/builder/constructorStatus.js";

async function fixture(handle: (request: IncomingMessage, response: ServerResponse) => void) {
  const server = createServer(handle);
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  return {
    url: `http://127.0.0.1:${(server.address() as AddressInfo).port}`,
    close: async () => {
      server.closeAllConnections();
      server.close();
      await once(server, "close");
    },
  };
}

describe("private constructor status", () => {
  it("reads the configured origin without caching or redirecting", async () => {
    const requests: {path?: string; cache?: string}[] = [];
    const owner = await fixture((request, response) => {
      requests.push({path: request.url, cache: request.headers["cache-control"]});
      response.end(JSON.stringify({ok: true, constructorInstance: "ab".repeat(16), beginRecoveryEnabled: true}));
    });
    try {
      const read = createConstructorStatusReader(`${owner.url}/rpc`);
      await expect(read(new AbortController().signal)).resolves.toMatchObject({constructorInstance: "ab".repeat(16)});
      expect(requests).toEqual([{path: "/healthz", cache: "no-cache"}]);
    } finally {
      await owner.close();
    }
  });

  it.each(["length", "chunked", "redirect", "status", "json"])("refuses an unusable %s response", async (kind) => {
    const owner = await fixture((_request, response) => {
      if (kind === "redirect") {
        response.writeHead(302, {location: "/another-owner"});
        response.end();
      } else if (kind === "status") {
        response.writeHead(503);
        response.end("{}");
      } else if (kind === "json") response.end("invalid JSON");
      else {
        if (kind === "length") response.setHeader("content-length", 20_000);
        response.write("x".repeat(12_000));
        response.end("x".repeat(8_000));
      }
    });
    try {
      await expect(createConstructorStatusReader(owner.url)(new AbortController().signal)).rejects.toThrow();
    } finally {
      await owner.close();
    }
  });

  it("bounds a response that never finishes and honors caller cancellation", async () => {
    const owner = await fixture((_request, response) => {
      response.writeHead(200);
      response.write("{");
    });
    try {
      const read = createConstructorStatusReader(owner.url);
      await expect(read(new AbortController().signal)).rejects.toThrow();
      const cancelled = new AbortController();
      cancelled.abort();
      await expect(read(cancelled.signal)).rejects.toThrow();
    } finally {
      await owner.close();
    }
  });
});
