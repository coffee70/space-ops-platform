import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import test from "node:test";

import { createApp } from "../src/server.js";
import { baseRuntimeConfig, FakeContextClient, FakeToolExecutionClient, FakeToolRegistryClient, MemoryConversationStore } from "./helpers.js";

const HERE = path.dirname(fileURLToPath(import.meta.url));
const MODELS_EXAMPLE_YAML = readFileSync(path.join(HERE, "..", "config", "models.local.yaml.example"), "utf8");

const MIN_VALID = `version: 1
defaults:
  chatModel: m1
  codingModel: m1
  fastModel: m1
  restrictedModel: m1
providers:
  p1:
    type: openai
    displayName: OpenAI
    apiKeyEnv: OPENAI_API_KEY
models:
  - id: m1
    providerRef: p1
    providerModelId: gpt-4o-mini
    enabled: true
    defaultFor: [chat, coding, fast]
`;

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

test("POST /models/validate-config accepts bundled models.local.yaml.example", async () => {
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

  const response = await app.request("/models/validate-config", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ content: MODELS_EXAMPLE_YAML }),
  });
  assert.equal(response.status, 200);
  const body = (await response.json()) as { valid: boolean; parsed: { model_count: number } | null; errors: unknown[] };
  assert.equal(body.valid, true);
  assert.ok(body.parsed && body.parsed.model_count > 0);
  assert.ok(Array.isArray(body.errors) && body.errors.length === 0);
});

test("POST /models/validate-config rejects invalid YAML", async () => {
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

  const response = await app.request("/models/validate-config", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ content: "foo: [\n" }),
  });
  assert.equal(response.status, 200);
  const body = (await response.json()) as { valid: boolean; parsed: null; errors: { loc: string[] }[] };
  assert.equal(body.valid, false);
  assert.equal(body.parsed, null);
  assert.ok(body.errors.length >= 1);
});

test("POST /models/validate-config rejects unknown providerRef", async () => {
  const bad = MIN_VALID.replace("providerRef: p1", "providerRef: missing");
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

  const response = await app.request("/models/validate-config", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ content: bad }),
  });
  assert.equal(response.status, 200);
  const body = (await response.json()) as { valid: boolean; errors: { type: string }[] };
  assert.equal(body.valid, false);
  assert.ok(body.errors.some((e) => e.type.startsWith("semantic.") || e.type.startsWith("zod.")));
});

test("POST /models/validate-config survives invalid JSON body", async () => {
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

  const response = await app.request("/models/validate-config", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: "{not-json",
  });
  assert.equal(response.status, 200);
  const body = (await response.json()) as { valid: boolean; errors: { type: string }[] };
  assert.equal(body.valid, false);
  assert.ok(body.errors.some((e) => e.type === "json_parse"));
});
