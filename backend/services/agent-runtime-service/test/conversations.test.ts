import assert from "node:assert/strict";
import test from "node:test";

import { createApp } from "../src/server.js";
import { maybeGenerateConversationTitle } from "../src/title-generation.js";
import {
  FakeContextClient,
  FakeModelCatalog,
  FakeToolExecutionClient,
  FakeToolRegistryClient,
  MemoryConversationStore,
  baseRuntimeConfig,
  createStaticModelRunner,
  modelOption,
  parseNdjson,
} from "./helpers.js";
import type { RunDependencies } from "../src/types.js";

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

function createTitleDependencies(
  store: MemoryConversationStore,
  overrides: Partial<Pick<RunDependencies, "modelRunner" | "modelCatalog">> = {},
): RunDependencies {
  const first = modelOption({ id: "first-available", isDefault: true });
  return {
    config: baseRuntimeConfig(),
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
    modelRunner: createStaticModelRunner([{ type: "text-delta", textDelta: "Generated Mission Title" }]),
    modelCatalog: new FakeModelCatalog({
      default_model_id: first.id,
      chat_title_generation: { model_id: first.id },
      models: [first],
      metadata: { registrySource: "config", metadataResolvers: ["test"], cached: true, updatedAt: new Date(0).toISOString() },
    }),
    createId: crypto.randomUUID,
    now: () => new Date(),
    ...overrides,
  };
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
      execution_mode: "read_only",
      initial_message: { role: "user", content: "Inspect runtime service ownership." },
    }),
    headers: { "content-type": "application/json" },
  });

  assert.equal(createResponse.status, 200);
  const conversation = (await createResponse.json()) as { id: string; title: string | null; title_source: string; messages: Array<{ content: string }> };
  assert.equal(conversation.title, null);
  assert.equal(conversation.title_source, "initial");
  assert.equal(conversation.messages.length, 1);

  const listResponse = await app.request("/conversations");
  assert.equal(listResponse.status, 200);
  const listed = (await listResponse.json()) as Array<{ id: string }>;
  assert.equal(listed.length, 1);
  assert.equal(listed[0].id, conversation.id);

  const getResponse = await app.request(`/conversations/${conversation.id}`);
  assert.equal(getResponse.status, 200);
  const detail = (await getResponse.json()) as { messages: Array<{ content: string }>; events: unknown[] };
  assert.equal(detail.messages.length, 1);
  assert.equal(detail.messages[0].content, "Inspect runtime service ownership.");
  assert.deepEqual(detail.events, []);

  const emptyConversationId = crypto.randomUUID();
  store.conversations.set(emptyConversationId, {
    id: emptyConversationId,
    title: "Empty",
    mission_id: null,
    vehicle_id: null,
    execution_mode: "read_only",
    selected_model_id: null,
    title_source: "initial",
    title_model_id: null,
    created_at: new Date().toISOString(),
    updated_at: new Date().toISOString(),
    messages: [],
    events: [],
  });
  const filteredListResponse = await app.request("/conversations");
  const filteredList = (await filteredListResponse.json()) as Array<{ id: string }>;
  assert.equal(filteredList.some((item) => item.id === emptyConversationId), false);

  const emptyGetResponse = await app.request(`/conversations/${emptyConversationId}`);
  assert.equal(emptyGetResponse.status, 404);
});

test("conversation creation marks explicit titles as manual", async () => {
  const store = new MemoryConversationStore();
  const app = createTestApp(store);

  const response = await app.request("/conversations", {
    method: "POST",
    body: JSON.stringify({
      title: "Operator Chosen Title",
      execution_mode: "read_only",
      initial_message: { role: "user", content: "Inspect runtime service ownership." },
    }),
    headers: { "content-type": "application/json" },
  });

  assert.equal(response.status, 200);
  const conversation = (await response.json()) as { title: string | null; title_source: string };
  assert.equal(conversation.title, "Operator Chosen Title");
  assert.equal(conversation.title_source, "manual");
});

test("conversation detail returns scoped persisted events in stable order", async () => {
  const store = new MemoryConversationStore();
  const app = createTestApp(store);
  const first = await store.createConversation({
    title: "First",
    execution_mode: "execute",
    initial_message: { role: "user", content: "Make a scripted change." },
  });
  const second = await store.createConversation({
    title: "Second",
    execution_mode: "read_only",
    initial_message: { role: "user", content: "Inspect something else." },
  });
  const runId = crypto.randomUUID();
  const requestId = crypto.randomUUID();
  await store.appendEvent({
    conversation_id: first.id,
    agent_run_id: runId,
    request_id: requestId,
    tool_call_id: null,
    sequence: 2,
    emitted_by: "agent-runtime-service",
    event_type: "run.started",
    payload: { execution_mode: "execute", model_id: "fixture", message_count: 1 },
    created_at: "2026-05-13T12:00:00.000Z",
  });
  await store.appendEvent({
    conversation_id: second.id,
    agent_run_id: crypto.randomUUID(),
    request_id: crypto.randomUUID(),
    tool_call_id: null,
    sequence: 1,
    emitted_by: "agent-runtime-service",
    event_type: "run.started",
    payload: { execution_mode: "read_only", model_id: "fixture", message_count: 1 },
    created_at: "2026-05-13T11:59:00.000Z",
  });
  await store.appendEvent({
    conversation_id: first.id,
    agent_run_id: runId,
    request_id: requestId,
    tool_call_id: null,
    sequence: 1,
    emitted_by: "agent-runtime-service",
    event_type: "change.summary",
    payload: {
      branch: "preview/example",
      base_branch: "main",
      base_commit_sha: "abc123",
      commit_sha: "def456",
      changed_files: ["src/example.ts"],
      target_application_id: "ai-engineer",
      target_unit_id: "unit-ai",
    },
    created_at: "2026-05-13T12:00:00.000Z",
  });

  const response = await app.request(`/conversations/${first.id}`);
  assert.equal(response.status, 200);
  const detail = (await response.json()) as {
    events: Array<{ conversation_id: string; event_type: string; sequence: number; payload: Record<string, unknown> }>;
  };
  assert.equal(detail.events.length, 2);
  assert.deepEqual(
    detail.events.map((event) => event.conversation_id),
    [first.id, first.id],
  );
  assert.deepEqual(
    detail.events.map((event) => event.event_type),
    ["change.summary", "run.started"],
  );
  assert.deepEqual(
    detail.events.map((event) => event.sequence),
    [1, 2],
  );
  assert.equal(detail.events[0].payload.branch, "preview/example");
});

test("conversation patch updates persisted settings and manual title", async () => {
  const store = new MemoryConversationStore();
  const app = createTestApp(store);
  const conversation = await store.createConversation({
    title: null,
    execution_mode: "read_only",
    initial_message: { role: "user", content: "Tune the model." },
  });

  const response = await app.request(`/conversations/${conversation.id}`, {
    method: "PATCH",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      title: "Model Tuning",
      execution_mode: "execute",
      selected_model_id: "openai-gpt-5-1-mini",
    }),
  });

  assert.equal(response.status, 200);
  const updated = (await response.json()) as {
    title: string;
    execution_mode: string;
    selected_model_id: string;
    title_source: string;
  };
  assert.equal(updated.title, "Model Tuning");
  assert.equal(updated.execution_mode, "execute");
  assert.equal(updated.selected_model_id, "openai-gpt-5-1-mini");
  assert.equal(updated.title_source, "manual");
});

test("title generation uses YAML configured available model from catalog", async () => {
  const store = new MemoryConversationStore();
  const conversation = await store.createConversation({
    title: null,
    execution_mode: "read_only",
    initial_message: { role: "user", content: "Inspect thermal telemetry drift." },
  });
  await store.appendMessage({
    conversationId: conversation.id,
    role: "assistant",
    content: "I found the telemetry drift.",
  });
  let resolvedModelId: string | null | undefined;
  const configured = modelOption({ id: "title-model", isDefault: false });
  const fallback = modelOption({ id: "fallback-model", isDefault: true });
  const modelCatalog = new FakeModelCatalog(
    {
      default_model_id: "fallback-model",
      chat_title_generation: { model_id: "title-model" },
      models: [fallback, configured],
      metadata: { registrySource: "config", metadataResolvers: ["test"], cached: true, updatedAt: new Date(0).toISOString() },
    },
    (modelId, _mode) => {
      resolvedModelId = modelId;
      const option = modelId === "title-model" ? configured : fallback;
      return {
        option,
        runtime: { id: option.id, providerType: option.providerType, providerModelId: option.providerModelId, apiKey: "key", baseUrl: null },
      };
    },
  );

  await maybeGenerateConversationTitle({
    conversationId: conversation.id,
    dependencies: {
      config: baseRuntimeConfig(),
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
      modelRunner: createStaticModelRunner([{ type: "text-delta", textDelta: "Thermal Drift Review" }]),
      modelCatalog,
      createId: crypto.randomUUID,
      now: () => new Date(),
    },
  });

  const updated = await store.getConversation(conversation.id);
  assert.equal(resolvedModelId, "title-model");
  assert.equal(updated?.title, "Thermal Drift Review");
  assert.equal(updated?.title_model_id, "title-model");
});

test("title generation falls back to first available model when YAML title model is unset", async () => {
  const store = new MemoryConversationStore();
  const conversation = await store.createConversation({
    title: null,
    execution_mode: "read_only",
    initial_message: { role: "user", content: "Inspect battery telemetry." },
  });
  await store.appendMessage({ conversationId: conversation.id, role: "assistant", content: "Battery telemetry is stable." });
  let resolvedModelId: string | null | undefined;
  const first = modelOption({ id: "first-available", isDefault: false });
  const second = modelOption({ id: "second-available", isDefault: true });
  const dependencies = {
    config: baseRuntimeConfig(),
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
    modelRunner: createStaticModelRunner([{ type: "text-delta", textDelta: "Battery Telemetry" }]),
    modelCatalog: new FakeModelCatalog(
      {
        default_model_id: "second-available",
        chat_title_generation: { model_id: null },
        models: [first, second],
        metadata: { registrySource: "config", metadataResolvers: ["test"], cached: true, updatedAt: new Date(0).toISOString() },
      },
      (modelId, _mode) => {
        resolvedModelId = modelId;
        return {
          option: modelId === "second-available" ? second : first,
          runtime: { id: modelId ?? first.id, providerType: first.providerType, providerModelId: first.providerModelId, apiKey: "key", baseUrl: null },
        };
      },
    ),
    createId: crypto.randomUUID,
    now: () => new Date(),
  };

  await maybeGenerateConversationTitle({ conversationId: conversation.id, dependencies });
  const generated = await store.getConversation(conversation.id);
  assert.equal(resolvedModelId, "first-available");
  assert.equal(generated?.title, "Battery Telemetry");
});

test("title generation falls back safely when YAML title model is unavailable disabled or missing", async () => {
  const cases = [
    {
      name: "unavailable",
      models: [
        modelOption({
          id: "configured-title",
          enabled: true,
          isAvailable: false,
          disabledReason: "Provider model id is unavailable.",
          isDefault: false,
        }),
      ],
    },
    {
      name: "disabled",
      models: [
        modelOption({
          id: "configured-title",
          enabled: false,
          isAvailable: false,
          disabledReason: "Model is disabled.",
          isDefault: false,
        }),
      ],
    },
    {
      name: "missing",
      models: [],
    },
  ];

  for (const currentCase of cases) {
    const store = new MemoryConversationStore();
    const conversation = await store.createConversation({
      title: null,
      execution_mode: "read_only",
      initial_message: { role: "user", content: `Inspect ${currentCase.name} propulsion telemetry.` },
    });
    await store.appendMessage({ conversationId: conversation.id, role: "assistant", content: "Propulsion telemetry looks nominal." });
    let resolvedModelId: string | null | undefined;
    const first = modelOption({ id: "first-available", isDefault: false });

    await maybeGenerateConversationTitle({
      conversationId: conversation.id,
      dependencies: {
        config: baseRuntimeConfig(),
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
        modelRunner: createStaticModelRunner([{ type: "text-delta", textDelta: "Propulsion Telemetry" }]),
        modelCatalog: new FakeModelCatalog(
          {
            default_model_id: "first-available",
            chat_title_generation: { model_id: "configured-title" },
            models: [...currentCase.models, first],
            metadata: { registrySource: "config", metadataResolvers: ["test"], cached: true, updatedAt: new Date(0).toISOString() },
          },
          (modelId, _mode) => {
            resolvedModelId = modelId;
            return {
              option: first,
              runtime: {
                id: first.id,
                providerType: first.providerType,
                providerModelId: first.providerModelId,
                apiKey: "key",
                baseUrl: null,
              },
            };
          },
        ),
        createId: crypto.randomUUID,
        now: () => new Date(),
      },
    });

    const generated = await store.getConversation(conversation.id);
    assert.equal(resolvedModelId, "first-available", currentCase.name);
    assert.equal(generated?.title, "Propulsion Telemetry", currentCase.name);
    assert.equal(generated?.title_model_id, "first-available", currentCase.name);
  }
});

test("title generation preserves manual titles", async () => {
  const store = new MemoryConversationStore();
  const conversation = await store.createConversation({
    title: null,
    execution_mode: "read_only",
    initial_message: { role: "user", content: "Inspect battery telemetry." },
  });
  await store.appendMessage({ conversationId: conversation.id, role: "assistant", content: "Battery telemetry is stable." });
  const first = modelOption({ id: "first-available", isDefault: false });
  const dependencies = {
    config: baseRuntimeConfig(),
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
    modelRunner: createStaticModelRunner([{ type: "text-delta", textDelta: "Battery Telemetry" }]),
    modelCatalog: new FakeModelCatalog({
      default_model_id: "first-available",
      chat_title_generation: { model_id: "first-available" },
      models: [first],
      metadata: { registrySource: "config", metadataResolvers: ["test"], cached: true, updatedAt: new Date(0).toISOString() },
    }),
    createId: crypto.randomUUID,
    now: () => new Date(),
  };

  await store.updateConversation(conversation.id, { title: "Manual Battery Name", title_source: "manual" });
  await maybeGenerateConversationTitle({ conversationId: conversation.id, dependencies });
  const manual = await store.getConversation(conversation.id);
  assert.equal(manual?.title, "Manual Battery Name");
  assert.equal(manual?.title_source, "manual");
});

test("title generation creates a generated title for an initial conversation", async () => {
  const store = new MemoryConversationStore();
  const conversation = await store.createConversation({
    title: null,
    execution_mode: "read_only",
    initial_message: { role: "user", content: "Inspect battery telemetry." },
  });
  await store.appendMessage({ conversationId: conversation.id, role: "assistant", content: "Battery telemetry is stable." });

  await maybeGenerateConversationTitle({ conversationId: conversation.id, dependencies: createTitleDependencies(store) });

  const generated = await store.getConversation(conversation.id);
  assert.equal(generated?.title, "Generated Mission Title");
  assert.equal(generated?.title_source, "generated");
  assert.equal(generated?.title_model_id, "first-available");
});

test("title generation remains retryable after model failure", async () => {
  const store = new MemoryConversationStore();
  const conversation = await store.createConversation({
    title: null,
    execution_mode: "read_only",
    initial_message: { role: "user", content: "Inspect propulsion telemetry." },
  });
  await store.appendMessage({ conversationId: conversation.id, role: "assistant", content: "Propulsion telemetry is stable." });

  await maybeGenerateConversationTitle({
    conversationId: conversation.id,
    dependencies: createTitleDependencies(store, {
      modelRunner: {
        async *stream() {
          throw new Error("model unavailable");
        },
      },
    }),
  });

  const afterFailure = await store.getConversation(conversation.id);
  assert.equal(afterFailure?.title, null);
  assert.equal(afterFailure?.title_source, "initial");

  await maybeGenerateConversationTitle({ conversationId: conversation.id, dependencies: createTitleDependencies(store) });

  const afterRetry = await store.getConversation(conversation.id);
  assert.equal(afterRetry?.title, "Generated Mission Title");
  assert.equal(afterRetry?.title_source, "generated");
});

test("title generation does not overwrite a manual rename made while generation is in progress", async () => {
  const store = new MemoryConversationStore();
  const conversation = await store.createConversation({
    title: null,
    execution_mode: "read_only",
    initial_message: { role: "user", content: "Inspect avionics telemetry." },
  });
  await store.appendMessage({ conversationId: conversation.id, role: "assistant", content: "Avionics telemetry is stable." });
  let releaseGeneration!: () => void;
  const generationStarted = new Promise<void>((resolve) => {
    const releasePromise = new Promise<void>((release) => {
      releaseGeneration = release;
    });
    void releasePromise.then(resolve);
  });
  const titlePromise = maybeGenerateConversationTitle({
    conversationId: conversation.id,
    dependencies: createTitleDependencies(store, {
      modelRunner: {
        async *stream() {
          yield { type: "text-delta", textDelta: "Generated Avionics Title" };
          await generationStarted;
        },
      },
    }),
  });

  await Promise.resolve();
  await store.updateConversation(conversation.id, { title: "Manual Avionics Title", title_source: "manual" });
  releaseGeneration();
  await titlePromise;

  const manual = await store.getConversation(conversation.id);
  assert.equal(manual?.title, "Manual Avionics Title");
  assert.equal(manual?.title_source, "manual");
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
    title: null,
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
  assert.ok(
    chunks.some(
      (chunk) =>
        chunk.kind === "event" &&
        (chunk as { event: { event_type: string; payload: Record<string, unknown> } }).event.event_type ===
          "conversation.title.generated",
    ),
  );

  const detail = await store.getConversation(conversation.id);
  assert.equal(detail?.messages.filter((message) => message.role === "user").length, 1);
  assert.equal(detail?.messages.length, 2);
  assert.equal(detail?.title_source, "generated");
});
