import assert from "node:assert/strict";
import test from "node:test";

import { createApp } from "../src/server.js";
import {
  baseRuntimeConfig,
  contextResolvedEvent,
  FakeContextClient,
  FakeToolExecutionClient,
  FakeToolRegistryClient,
  MemoryConversationStore,
  parseNdjson,
} from "./helpers.js";

test("chat rejects disabled registry model with run.failed", async () => {
  const store = new MemoryConversationStore();
  const conversation = await store.createConversation({
    title: "AI Engineer Session",
    execution_mode: "read_only",
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
