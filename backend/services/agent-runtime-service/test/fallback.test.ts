import assert from "node:assert/strict";
import test from "node:test";

import { createApp } from "../src/server.js";
import { contextResolvedEvent, FakeContextClient, FakeToolExecutionClient, FakeToolRegistryClient, MemoryConversationStore, parseNdjson } from "./helpers.js";

test("fallback path still emits runtime-owned completion lifecycle", async () => {
  const store = new MemoryConversationStore();
  const conversation = await store.createConversation({
    title: "AI Engineer Session",
    execution_mode: "read_only",
  });

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
    },
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
      async *stream() {
        throw new Error("model runner should not be invoked in fallback mode");
      },
    },
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
  assert.match(delta.event.payload.text_delta, /Model API key is not configured/);

  const eventTypes = chunks
    .filter((chunk) => chunk.kind === "event")
    .map((chunk) => (chunk as { event: { event_type: string } }).event.event_type);
  assert.deepEqual(eventTypes, ["run.started", "context.requested", "context.resolved", "message.delta", "message.completed", "run.completed"]);
});
