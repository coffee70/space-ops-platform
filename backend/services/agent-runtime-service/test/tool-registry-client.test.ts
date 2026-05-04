import assert from "node:assert/strict";
import test from "node:test";

import { HttpToolRegistryClient } from "../src/clients/tool-registry.js";

test("tool registry client forwards trace headers", async () => {
  const client = new HttpToolRegistryClient({
    port: 8080,
    databaseUrl: "postgres://example",
    controlPlaneUrl: "http://localhost:8100",
    openAiApiKey: null,
    openAiBaseUrl: null,
    modelId: "gpt-4o-mini",
    maxSteps: 3,
    requestTimeoutMs: 1000,
    scriptedMode: null,
    allowMissingKeyFallback: false,
  });

  const fetchCalls: Array<{ url: string; init?: RequestInit }> = [];
  const originalFetch = globalThis.fetch;
  globalThis.fetch = (async (input: string | URL | Request, init?: RequestInit) => {
    fetchCalls.push({
      url: typeof input === "string" ? input : input instanceof URL ? input.toString() : input.url,
      init,
    });
    return new Response(JSON.stringify([]), {
      status: 200,
      headers: { "content-type": "application/json" },
    });
  }) as typeof fetch;

  try {
    await client.listTools({
      conversation_id: "conversation-1",
      agent_run_id: "agent-run-1",
      request_id: "request-1",
      tool_call_id: null,
    });
  } finally {
    globalThis.fetch = originalFetch;
  }

  assert.equal(fetchCalls.length, 1);
  assert.equal(
    fetchCalls[0].url,
    "http://localhost:8100/internal/runtime-services/tool-registry-service/definitions?include_full_metadata=true&enabled=true",
  );
  const headers = new Headers(fetchCalls[0].init?.headers);
  assert.equal(headers.get("content-type"), "application/json");
  assert.equal(headers.get("x-conversation-id"), "conversation-1");
  assert.equal(headers.get("x-agent-run-id"), "agent-run-1");
  assert.equal(headers.get("x-request-id"), "request-1");
});
