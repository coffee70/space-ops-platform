import assert from "node:assert/strict";
import test from "node:test";

import { createApp } from "../src/server.js";
import {
  FakeContextClient,
  FakeModelCatalog,
  FakeToolExecutionClient,
  FakeToolRegistryClient,
  MemoryConversationStore,
  baseRuntimeConfig,
  createStaticModelRunner,
  parseNdjson,
} from "./helpers.js";

function createTestApp(store: MemoryConversationStore) {
  return createApp({
    config: baseRuntimeConfig({ maxSteps: 3 }),
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

  const emptyCreateResponse = await app.request("/conversations", {
    method: "POST",
    body: JSON.stringify({
      title: "AI Engineer Session",
      execution_mode: "read_only",
    }),
    headers: { "content-type": "application/json" },
  });
  assert.equal(emptyCreateResponse.status, 400);

  const createResponse = await app.request("/conversations", {
    method: "POST",
    body: JSON.stringify({
      title: "AI Engineer Session",
      execution_mode: "read_only",
      initial_message: { role: "user", content: "Inspect runtime service ownership." },
    }),
    headers: { "content-type": "application/json" },
  });

  assert.equal(createResponse.status, 200);
  const conversation = (await createResponse.json()) as { id: string; title: string; messages: Array<{ content: string }> };
  assert.equal(conversation.title, "AI Engineer Session");
  assert.equal(conversation.messages.length, 1);

  const listResponse = await app.request("/conversations");
  assert.equal(listResponse.status, 200);
  const listed = (await listResponse.json()) as Array<{ id: string }>;
  assert.equal(listed.length, 1);
  assert.equal(listed[0].id, conversation.id);

  const getResponse = await app.request(`/conversations/${conversation.id}`);
  assert.equal(getResponse.status, 200);
  const detail = (await getResponse.json()) as { messages: Array<{ content: string }> };
  assert.equal(detail.messages.length, 1);
  assert.equal(detail.messages[0].content, "Inspect runtime service ownership.");

  const emptyConversationId = crypto.randomUUID();
  store.conversations.set(emptyConversationId, {
    id: emptyConversationId,
    title: "Empty",
    mission_id: null,
    vehicle_id: null,
    execution_mode: "read_only",
    created_at: new Date().toISOString(),
    updated_at: new Date().toISOString(),
    messages: [],
  });
  const filteredListResponse = await app.request("/conversations");
  const filteredList = (await filteredListResponse.json()) as Array<{ id: string }>;
  assert.equal(filteredList.some((item) => item.id === emptyConversationId), false);

  const emptyGetResponse = await app.request(`/conversations/${emptyConversationId}`);
  assert.equal(emptyGetResponse.status, 404);
});

test("chat can run against a pre-created first user message without duplicating it", async () => {
  const store = new MemoryConversationStore();
  const app = createApp({
    config: baseRuntimeConfig({
      openAiApiKey: "test-key",
      allowMissingKeyFallback: false,
      maxSteps: 3,
    }),
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
    modelRunner: createStaticModelRunner([{ type: "text-delta", textDelta: "Done." }]),
    modelCatalog: new FakeModelCatalog(),
  });

  const conversation = await store.createConversation({
    title: "AI Engineer Session",
    execution_mode: "read_only",
    initial_message: { role: "user", content: "Inspect runtime service ownership." },
  });
  const firstMessageId = conversation.messages[0].id;

  const response = await app.request("/chat", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      conversation_id: conversation.id,
      execution_mode: "read_only",
      persisted_user_message_id: firstMessageId,
      messages: [{ role: "user", content: "Inspect runtime service ownership." }],
    }),
  });

  assert.equal(response.status, 200);
  const chunks = parseNdjson(await response.text());
  assert.ok(chunks.some((chunk) => chunk.kind === "event"));

  const detail = await store.getConversation(conversation.id);
  assert.equal(detail?.messages.filter((message) => message.role === "user").length, 1);
  assert.equal(detail?.messages.length, 2);
});
