import assert from "node:assert/strict";
import test from "node:test";

import { createApp } from "../src/server.js";
import { FakeContextClient, FakeToolExecutionClient, FakeToolRegistryClient, MemoryConversationStore } from "./helpers.js";

test("chat stream delivers message deltas before completion across multiple chunks", async () => {
  const store = new MemoryConversationStore();
  const conversation = await store.createConversation({
    title: "Streaming Session",
    execution_mode: "read_only",
  });

  const app = createApp({
    config: {
      port: 8080,
      databaseUrl: "postgres://example",
      controlPlaneUrl: "http://localhost:8100",
      openAiApiKey: "test-key",
      openAiBaseUrl: null,
      modelId: "gpt-4o-mini",
      maxSteps: 3,
      requestTimeoutMs: 1000,
    },
    store,
    contextClient: new FakeContextClient([
      {
        event_type: "context.resolved",
        emitted_by: "context-retrieval-service",
        payload: { context_packet_id: "ctx-1" },
      },
    ]),
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
        yield { type: "text-delta", textDelta: "Hello" };
        await new Promise((resolve) => setTimeout(resolve, 10));
        yield { type: "text-delta", textDelta: " world" };
        await new Promise((resolve) => setTimeout(resolve, 10));
        yield { type: "finish", finishReason: "stop" };
      },
    },
  });

  const response = await app.request("/agent/chat", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      conversation_id: conversation.id,
      execution_mode: "read_only",
      messages: [{ role: "user", content: "Stream a response." }],
    }),
  });

  assert.equal(response.status, 200);
  assert.ok(response.body);

  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";
  const chunkPayloads: string[] = [];
  const eventOrder: string[] = [];
  let deltaCount = 0;

  while (true) {
    const { done, value } = await reader.read();
    if (done) {
      break;
    }
    assert.ok(value);
    chunkPayloads.push(decoder.decode(value, { stream: true }));
    buffer += chunkPayloads[chunkPayloads.length - 1];

    let newlineIndex = buffer.indexOf("\n");
    while (newlineIndex >= 0) {
      const line = buffer.slice(0, newlineIndex).trim();
      buffer = buffer.slice(newlineIndex + 1);
      if (line.length > 0) {
        const payload = JSON.parse(line) as
          | { kind: "message.delta"; delta: string }
          | { kind: "event"; event: { event_type: string } };
        if (payload.kind === "message.delta") {
          deltaCount += 1;
          eventOrder.push("message.delta");
        } else {
          eventOrder.push(payload.event.event_type);
        }
      }
      newlineIndex = buffer.indexOf("\n");
    }
  }

  buffer += decoder.decode();
  assert.equal(buffer, "");
  assert.ok(chunkPayloads.length > 1, "expected NDJSON to arrive in multiple chunks");
  assert.ok(deltaCount >= 1, "expected at least one message.delta chunk");
  const firstDeltaIndex = eventOrder.indexOf("message.delta");
  const runCompletedIndex = eventOrder.indexOf("run.completed");
  assert.notEqual(firstDeltaIndex, -1, "expected a message.delta event");
  assert.notEqual(runCompletedIndex, -1, "expected a run.completed event");
  assert.ok(firstDeltaIndex < runCompletedIndex, "expected message.delta before run.completed");
});
