import assert from "node:assert/strict";
import test from "node:test";

import { createApp } from "../src/server.js";
import type { ToolDefinition, ToolExecutionResponse } from "../src/types.js";
import { contextResolvedEvent, FakeContextClient, FakeToolExecutionClient, FakeToolRegistryClient, MemoryConversationStore, parseNdjson } from "./helpers.js";

const READ_TOOL_DEFINITIONS: ToolDefinition[] = [
  "list_available_tools",
  "list_platform_services",
  "list_platform_applications",
  "search_documents",
  "search_codebase",
  "navigate_to_application",
].map((name) => ({
  name,
  description: name,
  category: "phase3-test",
  layer_target: name === "navigate_to_application" ? "layer3" : "layer2",
  read_write_classification: "read",
  required_execution_mode: "read_only",
  enabled: true,
  requires_confirmation: false,
  input_schema_json: { type: "object", properties: {}, additionalProperties: true },
}));

function toolResponse(toolName: string, trace: { conversation_id: string; agent_run_id: string; request_id: string; tool_call_id?: string | null }): ToolExecutionResponse {
  const toolCallId = trace.tool_call_id ?? crypto.randomUUID();
  const output =
    toolName === "navigate_to_application"
      ? { action: "navigate_to_application", application_id: "ai-engineer", route_path: "/apps/ai-engineer" }
      : { ok: true, tool_name: toolName };
  const raw_events = [
    {
      event_type: "tool.started",
      emitted_by: "tool-execution-service",
      tool_call_id: toolCallId,
      payload: {
        tool_name: toolName,
        category: "phase3-test",
        read_write_classification: "read",
        input_preview: {},
      },
    },
    {
      event_type: "tool.completed",
      emitted_by: "tool-execution-service",
      tool_call_id: toolCallId,
      payload: {
        tool_name: toolName,
        status: "completed",
        result_preview: output,
        duration_ms: 1,
      },
    },
    ...(toolName === "navigate_to_application"
      ? [
          {
            event_type: "navigation.requested",
            emitted_by: "tool-execution-service",
            tool_call_id: toolCallId,
            payload: output,
          },
        ]
      : []),
  ];

  return {
    conversation_id: trace.conversation_id,
    agent_run_id: trace.agent_run_id,
    request_id: trace.request_id,
    tool_call_id: toolCallId,
    status: "completed",
    output,
    raw_events,
  };
}

test("scripted_read_tools delegates every tool through tool-execution and streams correlated events", async () => {
  const store = new MemoryConversationStore();
  const conversation = await store.createConversation({
    title: "AI Engineer Session",
    execution_mode: "read_only",
  });

  const toolRegistry = new FakeToolRegistryClient(READ_TOOL_DEFINITIONS);
  const toolExecution = new FakeToolExecutionClient((input) => toolResponse(input.tool_name, input.trace));

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
      scriptedMode: "scripted_read_tools",
      allowMissingKeyFallback: true,
    },
    store,
    contextClient: new FakeContextClient([contextResolvedEvent()]),
    toolRegistryClient: toolRegistry,
    toolExecutionClient: toolExecution,
    modelRunner: {
      async *stream() {
        throw new Error("model runner should not be invoked in scripted mode");
      },
    },
    createId: (() => {
      const ids = ["agent-run-read", "request-read"];
      return () => ids.shift() ?? crypto.randomUUID();
    })(),
  });

  const response = await app.request("/chat", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      conversation_id: conversation.id,
      execution_mode: "read_only",
      messages: [{ role: "user", content: "Run deterministic no-LLM reads." }],
    }),
  });

  assert.equal(response.status, 200);
  const chunks = parseNdjson(await response.text());
  const events = chunks
    .filter((chunk) => chunk.kind === "event")
    .map((chunk) => (chunk as { event: { event_type: string; agent_run_id: string; request_id: string; tool_call_id: string | null } }).event);

  assert.deepEqual(toolExecution.calls.map((call) => call.tool_name), [
    "list_available_tools",
    "list_platform_services",
    "list_platform_applications",
    "search_documents",
    "search_codebase",
    "navigate_to_application",
  ]);
  assert.ok(events.some((event) => event.event_type === "navigation.requested"));
  assert.ok(events.every((event) => event.agent_run_id === "agent-run-read"));
  assert.ok(events.every((event) => event.request_id === "request-read"));

  const toolLifecycle = events.filter((event) => event.event_type.startsWith("tool."));
  assert.equal(toolLifecycle.length, 12);
  const toolCallIds = new Set(toolLifecycle.map((event) => event.tool_call_id));
  assert.equal(toolCallIds.size, 6);

  const completedEvent = events.find((event) => event.event_type === "run.completed");
  assert.ok(completedEvent);
  assert.equal(store.events.at(-1)?.event_type, "run.completed");
  assert.deepEqual(toolRegistry.traces, [
    {
      conversation_id: conversation.id,
      agent_run_id: "agent-run-read",
      request_id: "request-read",
      tool_call_id: null,
    },
  ]);
});
