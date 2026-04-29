import assert from "node:assert/strict";
import test from "node:test";

import { HttpToolExecutionClient } from "../src/clients/tool-execution.js";

test("tool execution client posts through tool-execution-service only", async () => {
  const client = new HttpToolExecutionClient({
    port: 8080,
    databaseUrl: "postgres://example",
    controlPlaneUrl: "http://localhost:8100",
    openAiApiKey: null,
    openAiBaseUrl: null,
    modelId: "gpt-4o-mini",
    maxSteps: 3,
    requestTimeoutMs: 1000,
  });

  const calls: Array<{ url: string; init?: RequestInit }> = [];
  const originalFetch = globalThis.fetch;
  globalThis.fetch = (async (input: string | URL | Request, init?: RequestInit) => {
    calls.push({
      url: typeof input === "string" ? input : input instanceof URL ? input.toString() : input.url,
      init,
    });
    return new Response(
      JSON.stringify({
        conversation_id: "conversation-1",
        agent_run_id: "agent-run-1",
        request_id: "request-1",
        tool_call_id: "tool-call-1",
        status: "completed",
        output: { ok: true },
        raw_events: [],
      }),
      {
        status: 200,
        headers: { "content-type": "application/json" },
      },
    );
  }) as typeof fetch;

  try {
    await client.execute({
      trace: {
        conversation_id: "conversation-1",
        agent_run_id: "agent-run-1",
        request_id: "request-1",
        tool_call_id: "tool-call-1",
      },
      tool_name: "get_platform_service",
      input: { service_slug: "agent-runtime-service" },
      execution_mode: "read_only",
    });
  } finally {
    globalThis.fetch = originalFetch;
  }

  assert.equal(calls.length, 1);
  assert.equal(calls[0]?.url, "http://localhost:8100/internal/runtime-services/tool-execution-service/execute");
  const requestBody = JSON.parse(String(calls[0]?.init?.body ?? "{}")) as Record<string, unknown>;
  assert.deepEqual(requestBody.input, { service_slug: "agent-runtime-service" });
  assert.equal(requestBody.confirmation_token, null);
});

test("tool execution client forwards top-level confirmation token", async () => {
  const client = new HttpToolExecutionClient({
    port: 8080,
    databaseUrl: "postgres://example",
    controlPlaneUrl: "http://localhost:8100",
    openAiApiKey: null,
    openAiBaseUrl: null,
    modelId: "gpt-4o-mini",
    maxSteps: 3,
    requestTimeoutMs: 1000,
  });

  const calls: Array<{ url: string; init?: RequestInit }> = [];
  const originalFetch = globalThis.fetch;
  globalThis.fetch = (async (input: string | URL | Request, init?: RequestInit) => {
    calls.push({
      url: typeof input === "string" ? input : input instanceof URL ? input.toString() : input.url,
      init,
    });
    return new Response(
      JSON.stringify({
        conversation_id: "conversation-1",
        agent_run_id: "agent-run-1",
        request_id: "request-1",
        tool_call_id: "tool-call-1",
        status: "completed",
        output: { ok: true },
        raw_events: [],
      }),
      {
        status: 200,
        headers: { "content-type": "application/json" },
      },
    );
  }) as typeof fetch;

  try {
    await client.execute({
      trace: {
        conversation_id: "conversation-1",
        agent_run_id: "agent-run-1",
        request_id: "request-1",
        tool_call_id: "tool-call-1",
      },
      tool_name: "create_working_branch",
      input: {},
      execution_mode: "execute",
      confirmation_token: "confirmed",
    });
  } finally {
    globalThis.fetch = originalFetch;
  }

  assert.equal(calls.length, 1);
  const requestBody = JSON.parse(String(calls[0]?.init?.body ?? "{}")) as Record<string, unknown>;
  assert.equal(requestBody.confirmation_token, "confirmed");
  assert.deepEqual(requestBody.input, {});
});
