import assert from "node:assert/strict";
import test from "node:test";

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

test("fallback path still emits runtime-owned completion lifecycle", async () => {
  const store = new MemoryConversationStore();
  const conversation = await store.createConversation({
    title: "AI Engineer Session",
    execution_mode: "read_only",
    initial_message: { role: "user", content: "Start AI Engineer session." },
  });

  const app = createApp({
    config: baseRuntimeConfig({
      maxSteps: 3,
      requestTimeoutMs: 1000,
    }),
    store,
    contextClient: new FakeContextClient([contextResolvedEvent()]),
    toolRegistryClient: new FakeToolRegistryClient([]),
    toolExecutionClient: new FakeToolExecutionClient({
      conversation_id: conversation.id,
      agent_run_id: "run",
      request_id: "req",
      tool_call_id: "tool",
      status: "completed",
      output: {},
      raw_events: [],
    }),
    modelRunner: {
      async *stream(input) {
        void input.model;
        throw new Error("model runner should not be invoked in fallback mode");
      },
    },
    modelCatalog: new FakeModelCatalog(undefined, () => {
      const option = modelOption();
      return {
        option,
        runtime: {
          id: option.id,
          providerType: option.providerType,
          providerModelId: option.providerModelId,
          apiKey: null,
          baseUrl: null,
        },
      };
    }),
  });

  const response = await app.request("/chat", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      conversation_id: conversation.id,
      execution_mode: "read_only",
      messages: [{ role: "user", content: "Run fallback mode." }],
    }),
  });

  assert.equal(response.status, 200);
  const chunks = parseNdjson(await response.text());
  const delta = chunks.find((chunk) => chunk.kind === "event" && (chunk as { event: { event_type: string } }).event.event_type === "message.delta") as {
    event: { payload: { text_delta: string } };
  };
  assert.match(delta.event.payload.text_delta, /Deterministic no-LLM runtime mode is active/);

  const eventTypes = chunks
    .filter((chunk) => chunk.kind === "event")
    .map((chunk) => (chunk as { event: { event_type: string } }).event.event_type);
  assert.deepEqual(eventTypes, ["run.started", "context.requested", "context.resolved", "message.delta", "message.completed", "run.completed"]);
});
