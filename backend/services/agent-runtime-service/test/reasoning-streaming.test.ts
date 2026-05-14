import assert from "node:assert/strict";
import test from "node:test";

import { createApp } from "../src/server.js";
import {
  baseRuntimeConfig,
  contextResolvedEvent,
  createStaticModelRunner,
  FakeContextClient,
  FakeModelCatalog,
  FakeToolExecutionClient,
  FakeToolRegistryClient,
  MemoryConversationStore,
  parseNdjson,
} from "./helpers.js";

test("chat orchestration streams and persists provider reasoning separately from answer text", async () => {
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
      requestTimeoutMs: 1000,
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
    modelRunner: createStaticModelRunner([
      { type: "reasoning", textDelta: "Inspecting context. " },
      { type: "reasoning", textDelta: "Planning response." },
      { type: "reasoning-part-finish" },
      { type: "text-delta", textDelta: "Here is the answer." },
      { type: "finish", finishReason: "stop" },
    ]),
    modelCatalog: new FakeModelCatalog(),
    createId: (() => {
      const ids = ["agent-run-1", "request-1"];
      return () => ids.shift() ?? crypto.randomUUID();
    })(),
  });

  const response = await app.request("/chat", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      conversation_id: conversation.id,
      execution_mode: "read_only",
      messages: [{ role: "user", content: "Explain the next step." }],
    }),
  });

  assert.equal(response.status, 200);
  const chunks = parseNdjson(await response.text());
  const events = chunks.filter((chunk) => chunk.kind === "event") as Array<{
    event: { event_type: string; payload: Record<string, unknown> };
  }>;

  assert.deepEqual(
    events.map((chunk) => chunk.event.event_type),
    [
      "run.started",
      "context.requested",
      "context.resolved",
      "model.selected",
      "message.reasoning.started",
      "message.reasoning.delta",
      "message.reasoning.delta",
      "message.reasoning.completed",
      "message.delta",
      "message.completed",
      "run.completed",
    ],
  );

  const started = events.find((chunk) => chunk.event.event_type === "message.reasoning.started")?.event;
  assert.equal(started?.payload.representation, "reasoning_summary");
  assert.equal(started?.payload.source, "provider_exposed");

  const reasoningDeltas = events.filter((chunk) => chunk.event.event_type === "message.reasoning.delta");
  assert.deepEqual(
    reasoningDeltas.map((chunk) => chunk.event.payload.text_delta),
    ["Inspecting context. ", "Planning response."],
  );

  const completed = events.find((chunk) => chunk.event.event_type === "message.reasoning.completed")?.event;
  assert.equal(completed?.payload.text_length, "Inspecting context. Planning response.".length);

  const detail = await store.getConversation(conversation.id);
  const assistant = detail?.messages.find((message) => message.role === "assistant");
  assert.equal(assistant?.content, "Here is the answer.");
  assert.deepEqual(assistant?.metadata_json.reasoning, {
    text: "Inspecting context. Planning response.",
    representation: "reasoning_summary",
    source: "provider_exposed",
    provider_type: "openai",
    provider_model_id: "gpt-5.1-mini",
    streamed: true,
  });
});

test("chat orchestration normalizes AI SDK reasoning-start/delta/end parts", async () => {
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
      requestTimeoutMs: 1000,
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
    modelRunner: createStaticModelRunner([
      { type: "reasoning-start", id: "0" },
      { type: "reasoning-delta", id: "0", text: "Inspecting context. " },
      { type: "reasoning-delta", id: "0", text: "Planning response." },
      { type: "reasoning-end", id: "0" },
      { type: "text-delta", textDelta: "Here is the answer." },
      { type: "finish", finishReason: "stop" },
    ]),
    modelCatalog: new FakeModelCatalog({
      providerType: "anthropic",
      providerModelId: "claude-sonnet-4-6",
      provider: "Anthropic",
    }),
    createId: (() => {
      const ids = ["agent-run-1", "request-1"];
      return () => ids.shift() ?? crypto.randomUUID();
    })(),
  });

  const response = await app.request("/chat", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      conversation_id: conversation.id,
      execution_mode: "read_only",
      messages: [{ role: "user", content: "Explain the next step." }],
    }),
  });

  assert.equal(response.status, 200);
  const chunks = parseNdjson(await response.text());
  const events = chunks.filter((chunk) => chunk.kind === "event") as Array<{
    event: { event_type: string; payload: Record<string, unknown> };
  }>;

  assert.deepEqual(
    events.map((chunk) => chunk.event.event_type),
    [
      "run.started",
      "context.requested",
      "context.resolved",
      "model.selected",
      "message.reasoning.started",
      "message.reasoning.delta",
      "message.reasoning.delta",
      "message.reasoning.completed",
      "message.delta",
      "message.completed",
      "run.completed",
    ],
  );

  const started = events.find((chunk) => chunk.event.event_type === "message.reasoning.started")?.event;
  assert.equal(started?.payload.representation, "thinking");
  assert.equal(started?.payload.source, "provider_exposed");

  const reasoningDeltas = events.filter((chunk) => chunk.event.event_type === "message.reasoning.delta");
  assert.deepEqual(
    reasoningDeltas.map((chunk) => chunk.event.payload.text_delta),
    ["Inspecting context. ", "Planning response."],
  );

  const completed = events.find((chunk) => chunk.event.event_type === "message.reasoning.completed")?.event;
  assert.equal(completed?.payload.text_length, "Inspecting context. Planning response.".length);

  const detail = await store.getConversation(conversation.id);
  const assistant = detail?.messages.find((message) => message.role === "assistant");
  assert.equal(assistant?.content, "Here is the answer.");
  assert.deepEqual(assistant?.metadata_json.reasoning, {
    text: "Inspecting context. Planning response.",
    representation: "thinking",
    source: "provider_exposed",
    provider_type: "anthropic",
    provider_model_id: "claude-sonnet-4-6",
    streamed: true,
  });
});
