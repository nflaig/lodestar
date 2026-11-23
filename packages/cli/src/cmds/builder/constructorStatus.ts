import {fetch} from "@lodestar/utils";

/** Private constructor identity proof, used only after an explicit reservation
 * mismatch. It must come from the configured single-owner endpoint, without a
 * redirect or a cached response. This never reads or revokes retained payloads. */
export const CONSTRUCTOR_STATUS_TIMEOUT_MS = 1000;
const MAX_STATUS_BYTES = 16 * 1024;

export type ConstructorStatusReader = (signal: AbortSignal) => Promise<unknown>;

export function createConstructorStatusReader(constructorUrl: string): ConstructorStatusReader {
  const url = new URL("/healthz", constructorUrl);
  return async (signal) => {
    const response = await fetch(url, {
      signal: AbortSignal.any([signal, AbortSignal.timeout(CONSTRUCTOR_STATUS_TIMEOUT_MS)]),
      redirect: "error",
      cache: "no-store",
      headers: {accept: "application/json", "cache-control": "no-cache"},
    });
    if (!response.body) throw Error("CONSTRUCTOR_STATUS_BODY_MISSING");
    const reader = response.body.getReader();
    try {
      if (!response.ok) throw Error("CONSTRUCTOR_STATUS_HTTP_ERROR");
      const contentLength = response.headers.get("content-length");
      if (contentLength !== null && (!/^\d+$/.test(contentLength) || Number(contentLength) > MAX_STATUS_BYTES))
        throw Error("CONSTRUCTOR_STATUS_TOO_LARGE");
      const bytes = new Uint8Array(MAX_STATUS_BYTES);
      let length = 0;
      while (true) {
        const chunk = await reader.read();
        if (chunk.done) break;
        if (chunk.value.byteLength > bytes.length - length) throw Error("CONSTRUCTOR_STATUS_TOO_LARGE");
        bytes.set(chunk.value, length);
        length += chunk.value.byteLength;
      }
      return JSON.parse(new TextDecoder("utf-8", {fatal: true}).decode(bytes.subarray(0, length))) as unknown;
    } finally {
      await reader.cancel().catch(() => {});
      reader.releaseLock();
    }
  };
}
