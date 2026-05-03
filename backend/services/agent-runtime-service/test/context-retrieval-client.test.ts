import assert from "node:assert/strict";
import test from "node:test";

import { HttpContextRetrievalClient } from "../src/clients/context-retrieval.js";

test("context retrieval client posts to /packet with trace headers and retrieval plan", async () => {
  const client = new HttpContextRetrievalClient({
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
    return new Response(
      JSON.stringify({
        conversation_id: "conversation-1",
        agent_run_id: "agent-run-1",
        request_id: "request-1",
        context_packet_id: "ctx-1",
        document_chunk_count: 0,
        code_chunk_count: 0,
        platform_metadata_bytes: 0,
        tool_definition_count: 0,
        truncated: false,
        truncation_reasons: [],
        data: {},
        raw_events: [],
      }),
      {
        status: 200,
        headers: { "content-type": "application/json" },
      },
    );
  }) as typeof fetch;

  try {
    await client.resolve({
      trace: {
        conversation_id: "conversation-1",
        agent_run_id: "agent-run-1",
        request_id: "request-1",
        tool_call_id: null,
      },
      message: "hello",
      mission_id: "m1",
      vehicle_id: "v1",
      execution_mode: "read_only",
      retrieval_plan: {
        documents: true,
        code: false,
        platform: false,
        tools: true,
        summary: "test",
      },
    });
  } finally {
    globalThis.fetch = originalFetch;
  }

  assert.equal(fetchCalls.length, 1);
  assert.equal(fetchCalls[0].url, "http://localhost:8100/internal/runtime-services/context-retrieval-service/packet");
  const headers = new Headers(fetchCalls[0].init?.headers);
  assert.equal(headers.get("content-type"), "application/json");
  assert.equal(headers.get("x-conversation-id"), "conversation-1");
  assert.equal(headers.get("x-agent-run-id"), "agent-run-1");
  assert.equal(headers.get("x-request-id"), "request-1");

  const body = JSON.parse(String(fetchCalls[0].init?.body ?? "{}")) as Record<string, unknown>;
  assert.deepEqual(body.retrieval_instructions, {
    documents: true,
    code: false,
    platform: false,
    tools: true,
  });
});
