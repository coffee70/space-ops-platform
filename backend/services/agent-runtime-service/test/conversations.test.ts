import assert from "node:assert/strict";
import test from "node:test";

import { createApp } from "../src/server.js";
import { FakeContextClient, FakeToolExecutionClient, FakeToolRegistryClient, MemoryConversationStore, createStaticModelRunner } from "./helpers.js";

function createTestApp(store: MemoryConversationStore) {
  return createApp({
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
}

test("conversation endpoints create, list, and fetch messages", async () => {
  const store = new MemoryConversationStore();
  const app = createTestApp(store);

  const createResponse = await app.request("/agent/conversations", {
    method: "POST",
    body: JSON.stringify({
      title: "AI Engineer Session",
      execution_mode: "read_only",
    }),
    headers: { "content-type": "application/json" },
  });

  assert.equal(createResponse.status, 200);
  const conversation = (await createResponse.json()) as { id: string; title: string };
  assert.equal(conversation.title, "AI Engineer Session");

  await store.appendMessage({
    conversationId: conversation.id,
    role: "user",
    content: "Inspect runtime service ownership.",
  });

  const listResponse = await app.request("/agent/conversations");
  assert.equal(listResponse.status, 200);
  const listed = (await listResponse.json()) as Array<{ id: string }>;
  assert.equal(listed.length, 1);
  assert.equal(listed[0].id, conversation.id);

  const getResponse = await app.request(`/agent/conversations/${conversation.id}`);
  assert.equal(getResponse.status, 200);
  const detail = (await getResponse.json()) as { messages: Array<{ content: string }> };
  assert.equal(detail.messages.length, 1);
  assert.equal(detail.messages[0].content, "Inspect runtime service ownership.");
});
