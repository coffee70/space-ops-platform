import assert from "node:assert/strict";
import test from "node:test";

import { createApp } from "../src/server.js";
import { contextResolvedEvent, FakeContextClient, FakeToolExecutionClient, FakeToolRegistryClient, MemoryConversationStore, parseNdjson } from "./helpers.js";

test("chat orchestration emits backend-owned run, context, tool, and completion events", async () => {
  const store = new MemoryConversationStore();
  const conversation = await store.createConversation({
    title: "AI Engineer Session",
    execution_mode: "read_only",
  });
  const toolRegistry = new FakeToolRegistryClient([
    {
      name: "get_platform_service",
      description: "Get runtime service details.",
      category: "platform_discovery",
      layer_target: "layer1",
      read_write_classification: "read",
      required_execution_mode: "read_only",
      enabled: true,
      requires_confirmation: false,
      input_schema_json: {
        type: "object",
        properties: {
          service_slug: {
            type: "string",
            description: "Service slug to inspect.",
          },
        },
        required: ["service_slug"],
      },
    },
  ]);

  const toolExecution = new FakeToolExecutionClient({
    conversation_id: conversation.id,
    agent_run_id: "ignored",
    request_id: "ignored",
    tool_call_id: "tool-call-1",
    status: "completed",
    output: { service_slug: "agent-runtime-service" },
    raw_events: [
      {
        event_type: "tool.started",
        emitted_by: "tool-execution-service",
        tool_call_id: "tool-call-1",
        payload: {
          tool_name: "get_platform_service",
          category: "platform_discovery",
          read_write_classification: "read",
          input_preview: { service_slug: "agent-runtime-service" },
        },
      },
      {
        event_type: "tool.completed",
        emitted_by: "tool-execution-service",
        tool_call_id: "tool-call-1",
        payload: {
          tool_name: "get_platform_service",
          status: "completed",
          result_preview: { service_slug: "agent-runtime-service" },
          duration_ms: 12,
        },
      },
    ],
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
    contextClient: new FakeContextClient([contextResolvedEvent()]),
    toolRegistryClient: toolRegistry,
    toolExecutionClient: toolExecution,
    modelRunner: {
      async *stream(input) {
        const runtimeTool = input.tools.get_platform_service as {
          execute: (args: { service_slug: string }, options: { toolCallId: string; messages: [] }) => Promise<unknown>;
        };
        await runtimeTool.execute({ service_slug: "agent-runtime-service" }, { toolCallId: "tool-call-1", messages: [] });
        yield { type: "text-delta", textDelta: "Runtime service ownership corrected." };
        yield { type: "finish", finishReason: "stop" };
      },
    },
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
      messages: [{ role: "user", content: "Inspect the runtime service." }],
    }),
  });

  assert.equal(response.status, 200);
  const chunks = parseNdjson(await response.text());
  const events = chunks.filter((chunk) => chunk.kind === "event") as Array<{
    event: { event_type: string; agent_run_id: string; request_id: string; sequence: number; payload: Record<string, unknown> };
  }>;

  assert.deepEqual(
    events.map((event) => event.event.event_type),
    ["run.started", "context.requested", "context.resolved", "tool.started", "tool.completed", "message.delta", "message.completed", "run.completed"],
  );
  assert.deepEqual(
    events.map((event) => event.event.agent_run_id),
    ["agent-run-1", "agent-run-1", "agent-run-1", "agent-run-1", "agent-run-1", "agent-run-1", "agent-run-1", "agent-run-1"],
  );
  assert.deepEqual(
    events.map((event) => event.event.request_id),
    ["request-1", "request-1", "request-1", "request-1", "request-1", "request-1", "request-1", "request-1"],
  );
  assert.deepEqual(
    events.map((event) => event.event.sequence),
    [1, 2, 3, 4, 5, 6, 7, 8],
  );
  const startedEvents = events.filter((chunk) => chunk.event.event_type === "tool.started");
  assert.equal(startedEvents.length, 1);
  assert.equal(startedEvents[0]?.event.payload.tool_name, "get_platform_service");

  const lifecycleEvents = events.filter((chunk) => chunk.event.event_type.startsWith("tool."));
  assert.deepEqual(
    lifecycleEvents.map((chunk) => chunk.event.event_type),
    ["tool.started", "tool.completed"],
  );
  assert.ok(
    lifecycleEvents.every((chunk) => (chunk.event as { emitted_by?: string }).emitted_by === "tool-execution-service"),
  );
  assert.deepEqual(
    lifecycleEvents.map((chunk) => (chunk.event as { tool_call_id?: string | null }).tool_call_id),
    ["tool-call-1", "tool-call-1"],
  );

  const delta = events.find((chunk) => chunk.event.event_type === "message.delta")?.event;
  assert.equal(delta?.payload.text_delta, "Runtime service ownership corrected.");
  assert.equal(delta?.agent_run_id, "agent-run-1");
  assert.equal(delta?.request_id, "request-1");
  assert.equal(store.events.length, events.length);
  assert.deepEqual(toolRegistry.traces, [
    {
      conversation_id: conversation.id,
      agent_run_id: "agent-run-1",
      request_id: "request-1",
      tool_call_id: null,
    },
  ]);
  assert.equal(toolExecution.calls.length, 1);
  assert.equal(toolExecution.calls[0]?.tool_name, "get_platform_service");
});

test("invalid downstream raw events become canonical error events", async () => {
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
      messages: [{ role: "user", content: "Use invalid downstream context." }],
    }),
  });

  assert.equal(response.status, 200);
  const chunks = parseNdjson(await response.text());
  const errorEvent = chunks.find((chunk) => chunk.kind === "event" && (chunk as { event: { event_type: string } }).event.event_type === "error") as {
    event: { payload: Record<string, unknown> };
  };
  assert.equal(errorEvent.event.payload.error_code, "invalid_downstream_event");
  assert.equal(errorEvent.event.payload.source, "context-retrieval-service");
});
