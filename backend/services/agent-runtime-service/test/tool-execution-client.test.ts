import assert from "node:assert/strict";
import test from "node:test";

import { HttpToolExecutionClient } from "../src/clients/tool-execution.js";
import { baseRuntimeConfig } from "./helpers.js";

test("tool execution client posts through tool-execution-service only", async () => {
  const client = new HttpToolExecutionClient(baseRuntimeConfig({ allowMissingKeyFallback: false }));

  const calls: Array<{ url: string; init?: RequestInit }> = [];
  const originalFetch = globalThis.fetch;
  globalThis.fetch = (async (input: string | URL | Request, init?: RequestInit) => {
    calls.push({
      url: typeof input === "string" ? input : input instanceof URL ? input.toString() : input.url,
      init,
    });
    return new Response(
      JSON.stringify({
        conversation_id: "11111111-1111-4111-8111-111111111111",
        agent_run_id: "22222222-2222-4222-8222-222222222222",
        request_id: "33333333-3333-4333-8333-333333333333",
        tool_call_id: "44444444-4444-4444-8444-444444444444",
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
    const response = await client.execute({
      trace: {
        conversation_id: "11111111-1111-4111-8111-111111111111",
        agent_run_id: "22222222-2222-4222-8222-222222222222",
        request_id: "33333333-3333-4333-8333-333333333333",
        tool_call_id: "44444444-4444-4444-8444-444444444444",
      },
      tool_name: "get_platform_service",
      input: { service_slug: "agent-runtime-service" },
      execution_mode: "read_only",
    });
    assert.deepEqual(response.output, { ok: true });
  } finally {
    globalThis.fetch = originalFetch;
  }

  assert.equal(calls.length, 1);
  assert.equal(calls[0]?.url, "http://localhost:8100/internal/runtime-services/tool-execution-service/execute");
  const requestBody = JSON.parse(String(calls[0]?.init?.body ?? "{}")) as Record<string, unknown>;
  assert.deepEqual(requestBody.input, { service_slug: "agent-runtime-service" });
  assert.equal(requestBody.confirmation_token, null);
  assert.equal(requestBody.permission_request_id, null);
});

test("tool execution client forwards top-level confirmation token", async () => {
  const client = new HttpToolExecutionClient(baseRuntimeConfig({ allowMissingKeyFallback: false }));

  const calls: Array<{ url: string; init?: RequestInit }> = [];
  const originalFetch = globalThis.fetch;
  globalThis.fetch = (async (input: string | URL | Request, init?: RequestInit) => {
    calls.push({
      url: typeof input === "string" ? input : input instanceof URL ? input.toString() : input.url,
      init,
    });
    return new Response(
      JSON.stringify({
        conversation_id: "11111111-1111-4111-8111-111111111111",
        agent_run_id: "22222222-2222-4222-8222-222222222222",
        request_id: "33333333-3333-4333-8333-333333333333",
        tool_call_id: "44444444-4444-4444-8444-444444444444",
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
        conversation_id: "11111111-1111-4111-8111-111111111111",
        agent_run_id: "22222222-2222-4222-8222-222222222222",
        request_id: "33333333-3333-4333-8333-333333333333",
        tool_call_id: "44444444-4444-4444-8444-444444444444",
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

test("tool execution client forwards permission request id for approved permissioned tools", async () => {
  const client = new HttpToolExecutionClient(baseRuntimeConfig({ allowMissingKeyFallback: false }));

  const calls: Array<{ url: string; init?: RequestInit }> = [];
  const originalFetch = globalThis.fetch;
  globalThis.fetch = (async (input: string | URL | Request, init?: RequestInit) => {
    calls.push({
      url: typeof input === "string" ? input : input instanceof URL ? input.toString() : input.url,
      init,
    });
    return new Response(
      JSON.stringify({
        conversation_id: "11111111-1111-4111-8111-111111111111",
        agent_run_id: "22222222-2222-4222-8222-222222222222",
        request_id: "33333333-3333-4333-8333-333333333333",
        tool_call_id: "44444444-4444-4444-8444-444444444444",
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
        conversation_id: "11111111-1111-4111-8111-111111111111",
        agent_run_id: "22222222-2222-4222-8222-222222222222",
        request_id: "33333333-3333-4333-8333-333333333333",
        tool_call_id: "44444444-4444-4444-8444-444444444444",
      },
      tool_name: "deploy_preview_change",
      input: {},
      execution_mode: "execute",
      permission_request_id: "55555555-5555-4555-8555-555555555555",
    });
  } finally {
    globalThis.fetch = originalFetch;
  }

  assert.equal(calls.length, 1);
  const requestBody = JSON.parse(String(calls[0]?.init?.body ?? "{}")) as Record<string, unknown>;
  assert.equal(requestBody.permission_request_id, "55555555-5555-4555-8555-555555555555");
  assert.equal(requestBody.confirmation_token, null);
});

test("tool execution client rejects malformed response payloads", async () => {
  const client = new HttpToolExecutionClient(baseRuntimeConfig({ allowMissingKeyFallback: false }));

  const originalFetch = globalThis.fetch;
  globalThis.fetch = (async () =>
    new Response(
      JSON.stringify({
        conversation_id: "11111111-1111-4111-8111-111111111111",
        agent_run_id: "22222222-2222-4222-8222-222222222222",
        request_id: "33333333-3333-4333-8333-333333333333",
        status: "completed",
        output: { ok: true },
        raw_events: [],
      }),
      {
        status: 200,
        headers: { "content-type": "application/json" },
      },
    )) as typeof fetch;

  try {
    await assert.rejects(
      () =>
        client.execute({
          trace: {
            conversation_id: "11111111-1111-4111-8111-111111111111",
            agent_run_id: "22222222-2222-4222-8222-222222222222",
            request_id: "33333333-3333-4333-8333-333333333333",
            tool_call_id: "44444444-4444-4444-8444-444444444444",
          },
          tool_name: "get_platform_service",
          input: { service_slug: "agent-runtime-service" },
          execution_mode: "read_only",
        }),
      /tool_call_id/,
    );
  } finally {
    globalThis.fetch = originalFetch;
  }
});
