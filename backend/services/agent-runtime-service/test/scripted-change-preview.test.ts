import assert from "node:assert/strict";
import test from "node:test";

import { createApp } from "../src/server.js";
import type { ToolDefinition, ToolExecutionResponse } from "../src/types.js";
import { contextResolvedEvent, FakeContextClient, FakeToolExecutionClient, FakeToolRegistryClient, MemoryConversationStore, parseNdjson } from "./helpers.js";

const PREVIEW_TOOL_DEFINITIONS: ToolDefinition[] = [
  "create_working_branch",
  "write_source_file",
  "create_commit",
].map((name) => ({
  name,
  description: name,
  category: "change-preview-test",
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
          category: "change-preview-test",
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

test("scripted_change_preview prepares a preview branch and emits change.summary without deploying", async () => {
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
      scriptedMode: "scripted_change_preview",
      allowMissingKeyFallback: true,
    },
    store,
    contextClient: new FakeContextClient([contextResolvedEvent()]),
    toolRegistryClient: new FakeToolRegistryClient(PREVIEW_TOOL_DEFINITIONS),
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
      messages: [{ role: "user", content: "Prepare a scoped change preview." }],
    }),
  });

  assert.equal(response.status, 200);
  const chunks = parseNdjson(await response.text());
  const eventTypes = chunks
    .filter((chunk) => chunk.kind === "event")
    .map((chunk) => (chunk as { event: { event_type: string } }).event.event_type);

  assert.ok(eventTypes.includes("change.summary"));
  assert.ok(eventTypes.includes("run.completed"));
  assert.ok(!eventTypes.includes("run.failed"));

  const changeSummary = chunks.find(
    (chunk) => chunk.kind === "event" && (chunk as { event: { event_type: string } }).event.event_type === "change.summary",
  ) as { event: { payload: Record<string, unknown> } };
  assert.equal(changeSummary.event.payload.branch, "preview/derived-telemetry-preview");
  assert.equal(changeSummary.event.payload.base_branch, "main");
  assert.equal(changeSummary.event.payload.target_unit_id, "derived-telemetry-service");
  assert.equal(changeSummary.event.payload.target_application_id, "telemetry");
  assert.equal(changeSummary.event.payload.risk_level, "low");
  assert.equal(changeSummary.event.payload.validation_status, "not_run");
  assert.deepEqual(changeSummary.event.payload.changed_files, [
    "project/space-ops-platform/backend/services/derived-telemetry-service/app/main.py",
  ]);

  assert.deepEqual(
    toolExecution.calls.map((call) => call.tool_name),
    ["create_working_branch", "write_source_file", "create_commit"],
  );
  assert.ok(toolExecution.calls.every((call) => call.execution_mode === "execute"));
});
