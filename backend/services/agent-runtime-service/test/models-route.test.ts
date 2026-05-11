import assert from "node:assert/strict";
import test from "node:test";

import { createApp } from "../src/server.js";
import {
  baseRuntimeConfig,
  FakeContextClient,
  FakeModelCatalog,
  FakeToolExecutionClient,
  FakeToolRegistryClient,
  MemoryConversationStore,
  modelOption,
} from "./helpers.js";

function createTestApp(modelCatalog = new FakeModelCatalog()) {
  return createApp({
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
    modelCatalog,
  });
}

test("GET /models returns model-registry catalog payload", async () => {
  const app = createTestApp(
    new FakeModelCatalog({
      default_model_id: "m1",
      models: [modelOption({ id: "m1", providerModelId: "gpt-5.1" })],
      metadata: {
        registrySource: "config",
        metadataResolvers: ["test"],
        cached: true,
        updatedAt: new Date(0).toISOString(),
      },
    }),
  );

  const response = await app.request("/models");
  assert.equal(response.status, 200);
  const body = (await response.json()) as { default_model_id: string; models: unknown[]; metadata: { registrySource: string } };
  assert.equal(body.default_model_id, "m1");
  assert.equal(body.models.length, 1);
  assert.equal(body.metadata.registrySource, "config");
});

test("GET /models/:modelId returns matching model or 404", async () => {
  const app = createTestApp(
    new FakeModelCatalog({
      default_model_id: "m1",
      models: [
        modelOption({ id: "m1", providerModelId: "gpt-5.1" }),
        modelOption({ id: "m2", providerModelId: "gpt-5.1-mini", isDefault: false }),
      ],
      metadata: {
        registrySource: "config",
        metadataResolvers: ["test"],
        cached: true,
        updatedAt: new Date(0).toISOString(),
      },
    }),
  );

  const hit = await app.request("/models/m2");
  assert.equal(hit.status, 200);
  const body = (await hit.json()) as { id: string };
  assert.equal(body.id, "m2");

  const miss = await app.request("/models/missing");
  assert.equal(miss.status, 404);
});
