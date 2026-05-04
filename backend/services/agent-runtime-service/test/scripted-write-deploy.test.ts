import assert from "node:assert/strict";
import test from "node:test";

import { createApp } from "../src/server.js";
import type { ToolDefinition, ToolExecutionResponse } from "../src/types.js";
import { contextResolvedEvent, FakeContextClient, FakeToolExecutionClient, FakeToolRegistryClient, MemoryConversationStore, parseNdjson } from "./helpers.js";

const WRITE_TOOL_DEFINITIONS: ToolDefinition[] = [
  "create_working_branch",
  "scaffold_service",
  "write_source_file",
  "create_commit",
  "deploy_service_or_application",
].map((name) => ({
  name,
  description: name,
  category: "phase3-test",
  layer_target: "layer1",
  read_write_classification: "write",
  required_execution_mode: "execute",
  enabled: true,
  requires_confirmation: false,
  input_schema_json: { type: "object", properties: {}, additionalProperties: true },
}));

function toolResponse(toolName: string, trace: { conversation_id: string; agent_run_id: string; request_id: string; tool_call_id?: string | null }): ToolExecutionResponse {
  const toolCallId = trace.tool_call_id ?? crypto.randomUUID();
  return {
    conversation_id: trace.conversation_id,
    agent_run_id: trace.agent_run_id,
    request_id: trace.request_id,
    tool_call_id: toolCallId,
    status: "completed",
    output: { ok: true, tool_name: toolName },
    raw_events: [
      {
        event_type: "tool.started",
        emitted_by: "tool-execution-service",
        tool_call_id: toolCallId,
        payload: {
          tool_name: toolName,
          category: "phase3-test",
          read_write_classification: "write",
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
          result_preview: { ok: true, tool_name: toolName },
          duration_ms: 1,
        },
      },
    ],
  };
}

test("scripted_write_deploy uses execute-only tools through tool-execution in order", async () => {
  const store = new MemoryConversationStore();
  const conversation = await store.createConversation({
    title: "AI Engineer Session",
    execution_mode: "execute",
  });

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
      scriptedMode: "scripted_write_deploy",
      allowMissingKeyFallback: true,
    },
    store,
    contextClient: new FakeContextClient([contextResolvedEvent()]),
    toolRegistryClient: new FakeToolRegistryClient(WRITE_TOOL_DEFINITIONS),
    toolExecutionClient: toolExecution,
    modelRunner: {
      async *stream() {
        throw new Error("model runner should not be invoked in scripted mode");
      },
    },
  });

  const response = await app.request("/chat", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      conversation_id: conversation.id,
      execution_mode: "execute",
      messages: [{ role: "user", content: "Deploy the deterministic Phase 3 fixture." }],
    }),
  });

  assert.equal(response.status, 200);
  const chunks = parseNdjson(await response.text());
  const completed = chunks.find((chunk) => chunk.kind === "event" && (chunk as { event: { event_type: string } }).event.event_type === "run.completed");
  assert.ok(completed);
  assert.deepEqual(toolExecution.calls.map((call) => call.tool_name), [
    "create_working_branch",
    "scaffold_service",
    "write_source_file",
    "write_source_file",
    "create_commit",
    "deploy_service_or_application",
  ]);
  assert.ok(toolExecution.calls.every((call) => call.execution_mode === "execute"));
  assert.deepEqual(toolExecution.calls[0]?.input, { branch: "feature/phase3-no-llm", from_branch: "main" });
  assert.equal(toolExecution.calls[1]?.input.unit_id, "phase3-test-fixture-service");
  assert.equal(toolExecution.calls[5]?.input.unit_id, "phase3-test-fixture-service");
});

test("scripted_write_deploy fails before mutation when execution mode is read_only", async () => {
  const store = new MemoryConversationStore();
  const conversation = await store.createConversation({
    title: "AI Engineer Session",
    execution_mode: "read_only",
  });

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
      scriptedMode: "scripted_write_deploy",
      allowMissingKeyFallback: true,
    },
    store,
    contextClient: new FakeContextClient([contextResolvedEvent()]),
    toolRegistryClient: new FakeToolRegistryClient(WRITE_TOOL_DEFINITIONS),
    toolExecutionClient: toolExecution,
    modelRunner: {
      async *stream() {
        throw new Error("model runner should not be invoked in scripted mode");
      },
    },
  });

  const response = await app.request("/chat", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      conversation_id: conversation.id,
      execution_mode: "read_only",
      messages: [{ role: "user", content: "Try to deploy in read-only mode." }],
    }),
  });

  assert.equal(response.status, 200);
  const chunks = parseNdjson(await response.text());
  const eventTypes = chunks
    .filter((chunk) => chunk.kind === "event")
    .map((chunk) => (chunk as { event: { event_type: string } }).event.event_type);
  assert.deepEqual(eventTypes, ["run.started", "context.requested", "context.resolved", "run.failed"]);
  assert.equal(toolExecution.calls.length, 0);
});
