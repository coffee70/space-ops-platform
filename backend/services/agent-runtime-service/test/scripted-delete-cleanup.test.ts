import assert from "node:assert/strict";
import test from "node:test";

import { createApp } from "../src/server.js";
import type { ToolDefinition } from "../src/types.js";
import { contextResolvedEvent, FakeContextClient, FakeToolExecutionClient, FakeToolRegistryClient, MemoryConversationStore, parseNdjson } from "./helpers.js";

const DELETE_TOOL: ToolDefinition = {
  name: "delete_managed_resources",
  description: "Delete managed resources.",
  category: "resource_delete",
  layer_target: "layer1",
  read_write_classification: "destructive_write",
  required_execution_mode: "execute",
  enabled: true,
  requires_confirmation: false,
  input_schema_json: { type: "object", properties: {}, additionalProperties: true },
};

test("scripted_delete_cleanup routes managed unit cleanup through tool-execution", async () => {
  const store = new MemoryConversationStore();
  const conversation = await store.createConversation({
    title: "AI Engineer Session",
    execution_mode: "execute",
  });

  const toolExecution = new FakeToolExecutionClient((input) => ({
    conversation_id: input.trace.conversation_id,
    agent_run_id: input.trace.agent_run_id,
    request_id: input.trace.request_id,
    tool_call_id: input.trace.tool_call_id ?? crypto.randomUUID(),
    status: "completed",
    output: { deleted: ["phase3-test-fixture-service"] },
    raw_events: [
      {
        event_type: "tool.started",
        emitted_by: "tool-execution-service",
        tool_call_id: input.trace.tool_call_id ?? null,
        payload: {
          tool_name: "delete_managed_resources",
          category: "resource_delete",
          read_write_classification: "destructive_write",
          input_preview: input.input,
        },
      },
      {
        event_type: "tool.completed",
        emitted_by: "tool-execution-service",
        tool_call_id: input.trace.tool_call_id ?? null,
        payload: {
          tool_name: "delete_managed_resources",
          status: "completed",
          result_preview: { deleted: ["phase3-test-fixture-service"] },
          duration_ms: 1,
        },
      },
    ],
  }));

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
      scriptedMode: "scripted_delete_cleanup",
      allowMissingKeyFallback: true,
    },
    store,
    contextClient: new FakeContextClient([contextResolvedEvent()]),
    toolRegistryClient: new FakeToolRegistryClient([DELETE_TOOL]),
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
      messages: [{ role: "user", content: "Clean up the deterministic fixture." }],
    }),
  });

  assert.equal(response.status, 200);
  const chunks = parseNdjson(await response.text());
  assert.ok(chunks.some((chunk) => chunk.kind === "event" && (chunk as { event: { event_type: string } }).event.event_type === "run.completed"));
  assert.equal(toolExecution.calls.length, 1);
  assert.deepEqual(toolExecution.calls[0]?.input, { mode: "managed_unit", unit_id: "phase3-test-fixture-service" });
});
