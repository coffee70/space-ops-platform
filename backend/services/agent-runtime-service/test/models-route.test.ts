import assert from "node:assert/strict";
import test from "node:test";

import { createApp } from "../src/server.js";
import { baseRuntimeConfig, FakeContextClient, FakeToolExecutionClient, FakeToolRegistryClient, MemoryConversationStore } from "./helpers.js";

test("GET /models returns stack catalog metadata", async () => {
  const app = createApp({
    config: baseRuntimeConfig(),
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
  });

  const response = await app.request("/models");
  assert.equal(response.status, 200);
  const body = (await response.json()) as { default_model_id: string; models: unknown[]; metadata: { registrySource: string } };
  assert.equal(body.default_model_id, "openai-gpt-5-5");
  assert.ok(Array.isArray(body.models) && body.models.length > 0);
  assert.equal(body.metadata.registrySource, "config");
});
