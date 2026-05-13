import assert from "node:assert/strict";
import test from "node:test";

import { ModelSelectionError } from "../src/ai/model-errors.js";
import { createApp } from "../src/server.js";
import {
  baseRuntimeConfig,
  contextResolvedEvent,
  FakeContextClient,
  FakeModelCatalog,
  FakeToolExecutionClient,
  FakeToolRegistryClient,
  MemoryConversationStore,
  modelOption,
  parseNdjson,
} from "./helpers.js";

test("chat rejects disabled registry model with run.failed", async () => {
  const store = new MemoryConversationStore();
  const conversation = await store.createConversation({
    title: "AI Engineer Session",
    execution_mode: "read_only",
    initial_message: { role: "user", content: "Start AI Engineer session." },
  });

  const app = createApp({
    config: baseRuntimeConfig({
      openAiApiKey: "test-key",
      allowMissingKeyFallback: false,
    }),
    store,
    contextClient: new FakeContextClient([contextResolvedEvent()]),
    toolRegistryClient: new FakeToolRegistryClient([]),
    toolExecutionClient: new FakeToolExecutionClient({
      conversation_id: conversation.id,
      agent_run_id: "ignored",
      request_id: "ignored",
      tool_call_id: "ignored",
      status: "completed",
      output: {},
      raw_events: [],
    }),
    modelRunner: {
      async *stream(input) {
        void input.model;
        yield { type: "text-delta", textDelta: "should not run" };
      },
    },
    modelCatalog: new FakeModelCatalog(undefined, () => {
      throw new ModelSelectionError("model_disabled", "Model is disabled: anthropic-claude-sonnet-4-6");
    }),
  });

  const response = await app.request("/chat", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      conversation_id: conversation.id,
      execution_mode: "read_only",
      model_id: "anthropic-claude-sonnet-4-6",
      messages: [{ role: "user", content: "Try disabled model." }],
    }),
  });

  assert.equal(response.status, 200);
  const chunks = parseNdjson(await response.text());
  const types = chunks.filter((c) => c.kind === "event").map((c) => (c as { event: { event_type: string } }).event.event_type);
  assert.ok(types.includes("run.failed"));
});

test("selected OpenAI model id is passed to model runner", async () => {
  const store = new MemoryConversationStore();
  const conversation = await store.createConversation({
    title: "AI Engineer Session",
    execution_mode: "read_only",
    initial_message: { role: "user", content: "Start AI Engineer session." },
  });

  let observedProviderModelId: string | null = null;

  const app = createApp({
    config: baseRuntimeConfig({
      openAiApiKey: "test-key",
      maxSteps: 3,
      allowMissingKeyFallback: false,
    }),
    store,
    contextClient: new FakeContextClient([contextResolvedEvent()]),
    toolRegistryClient: new FakeToolRegistryClient([]),
    toolExecutionClient: new FakeToolExecutionClient({
      conversation_id: conversation.id,
      agent_run_id: "ignored",
      request_id: "ignored",
      tool_call_id: "ignored",
      status: "completed",
      output: {},
      raw_events: [],
    }),
    modelRunner: {
      async *stream(input) {
        observedProviderModelId = input.model.providerModelId;
        yield { type: "text-delta", textDelta: "ok" };
        yield { type: "finish", finishReason: "stop" };
      },
    },
    modelCatalog: new FakeModelCatalog(
      {
        default_model_id: "openai-gpt-5-1-mini",
        models: [modelOption({ id: "openai-gpt-5-1-mini", providerModelId: "gpt-5.1-mini" })],
        metadata: {
          registrySource: "config",
          metadataResolvers: ["test"],
          cached: true,
          updatedAt: new Date(0).toISOString(),
        },
      },
      () => ({
        option: modelOption({ id: "openai-gpt-5-1-mini", providerModelId: "gpt-5.1-mini" }),
        runtime: {
          id: "openai-gpt-5-1-mini",
          providerType: "openai",
          providerModelId: "gpt-5.1-mini",
          apiKey: "test-key",
          baseUrl: null,
        },
      }),
    ),
  });

  const response = await app.request("/chat", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      conversation_id: conversation.id,
      execution_mode: "read_only",
      model_id: "openai-gpt-5-1-mini",
      messages: [{ role: "user", content: "Hello" }],
    }),
  });

  assert.equal(response.status, 200);
  const text = await response.text();
  assert.equal(observedProviderModelId, "gpt-5.1-mini");
  assert.match(text, /model\.selected/);
});

test("chat fails cleanly when selecting google provider model", async () => {
  const store = new MemoryConversationStore();
  const conversation = await store.createConversation({
    title: "AI Engineer Session",
    execution_mode: "read_only",
    initial_message: { role: "user", content: "Start AI Engineer session." },
  });

  const app = createApp({
    config: baseRuntimeConfig({
      openAiApiKey: "test-key",
      allowMissingKeyFallback: false,
    }),
    store,
    contextClient: new FakeContextClient([contextResolvedEvent()]),
    toolRegistryClient: new FakeToolRegistryClient([]),
    toolExecutionClient: new FakeToolExecutionClient({
      conversation_id: conversation.id,
      agent_run_id: "ignored",
      request_id: "ignored",
      tool_call_id: "ignored",
      status: "completed",
      output: {},
      raw_events: [],
    }),
    modelRunner: {
      async *stream() {
        yield { type: "text-delta", textDelta: "should not run" };
      },
    },
    modelCatalog: new FakeModelCatalog(undefined, () => {
      throw new ModelSelectionError("provider_not_implemented", "Provider type google is not implemented yet.");
    }),
  });

  const response = await app.request("/chat", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      conversation_id: conversation.id,
      execution_mode: "read_only",
      model_id: "google-gemini-test",
      messages: [{ role: "user", content: "Hello" }],
    }),
  });

  assert.equal(response.status, 200);
  const text = await response.text();
  assert.match(text, /run\.failed/);
  assert.match(text, /provider_not_implemented|Provider type google/i);
});

test("assistant message metadata includes selected stack model fields", async () => {
  const store = new MemoryConversationStore();
  const conversation = await store.createConversation({
    title: "AI Engineer Session",
    execution_mode: "read_only",
    initial_message: { role: "user", content: "Start AI Engineer session." },
  });

  const app = createApp({
    config: baseRuntimeConfig({
      openAiApiKey: "test-key",
      maxSteps: 3,
      allowMissingKeyFallback: false,
    }),
    store,
    contextClient: new FakeContextClient([contextResolvedEvent()]),
    toolRegistryClient: new FakeToolRegistryClient([]),
    toolExecutionClient: new FakeToolExecutionClient({
      conversation_id: conversation.id,
      agent_run_id: "ignored",
      request_id: "ignored",
      tool_call_id: "ignored",
      status: "completed",
      output: {},
      raw_events: [],
    }),
    modelRunner: {
      async *stream(input) {
        void input.model;
        yield { type: "text-delta", textDelta: "hello" };
        yield { type: "finish", finishReason: "stop" };
      },
    },
    modelCatalog: new FakeModelCatalog(
      {
        default_model_id: "openai-gpt-5-5",
        models: [modelOption({ id: "openai-gpt-5-5", providerModelId: "gpt-5.5", name: "GPT-5.5" })],
        metadata: {
          registrySource: "config",
          metadataResolvers: ["test"],
          cached: true,
          updatedAt: new Date(0).toISOString(),
        },
      },
      () => ({
        option: modelOption({ id: "openai-gpt-5-5", providerModelId: "gpt-5.5", name: "GPT-5.5" }),
        runtime: {
          id: "openai-gpt-5-5",
          providerType: "openai",
          providerModelId: "gpt-5.5",
          apiKey: "test-key",
          baseUrl: null,
        },
      }),
    ),
  });

  const response = await app.request("/chat", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      conversation_id: conversation.id,
      execution_mode: "read_only",
      model_id: "openai-gpt-5-5",
      messages: [{ role: "user", content: "Hello" }],
    }),
  });

  assert.equal(response.status, 200);
  await response.text();

  const updated = await store.getConversation(conversation.id);
  assert.ok(updated);
  const assistants = updated.messages.filter((m) => m.role === "assistant");
  assert.ok(assistants.length >= 1);
  const meta = assistants[assistants.length - 1].metadata_json;
  assert.equal(meta.model_id, "openai-gpt-5-5");
  assert.equal(meta.provider_type, "openai");
  assert.equal(meta.provider_model_id, "gpt-5.5");
  assert.equal(meta.provider, "OpenAI");
  assert.equal(meta.data_boundary, "external_api");
});
