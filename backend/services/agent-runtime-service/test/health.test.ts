import assert from "node:assert/strict";
import test from "node:test";

import { createApp } from "../src/server.js";
import { FakeContextClient, FakeToolExecutionClient, FakeToolRegistryClient, MemoryConversationStore, createStaticModelRunner } from "./helpers.js";

test("health endpoint returns ok", async () => {
  const app = createApp({
    config: {
      port: 8080,
      databaseUrl: "postgres://example",
      controlPlaneUrl: "http://localhost:8100",
      openAiApiKey: null,
      openAiBaseUrl: null,
      modelId: "gpt-4o-mini",
      maxSteps: 3,
      requestTimeoutMs: 1000,
      scriptedMode: null,
      allowMissingKeyFallback: true,
    },
    store: new MemoryConversationStore(),
    contextClient: new FakeContextClient(),
    toolRegistryClient: new FakeToolRegistryClient([]),
    toolExecutionClient: new FakeToolExecutionClient({
      conversation_id: null,
      agent_run_id: "run",
      request_id: "req",
      tool_call_id: "tool",
      status: "completed",
      output: {},
      raw_events: [],
    }),
    modelRunner: createStaticModelRunner([]),
  });

  const response = await app.request("/health");
  assert.equal(response.status, 200);
  assert.deepEqual(await response.json(), { status: "ok" });
});
