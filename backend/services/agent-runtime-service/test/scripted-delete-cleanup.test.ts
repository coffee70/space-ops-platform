import assert from "node:assert/strict";
import test from "node:test";

import { createApp } from "../src/server.js";
import type { ToolDefinition } from "../src/types.js";
import { baseRuntimeConfig, contextResolvedEvent, FakeContextClient, FakeToolExecutionClient, FakeToolRegistryClient, MemoryConversationStore, parseNdjson } from "./helpers.js";

const DELETE_TOOL: ToolDefinition = {
  name: "delete_managed_resources",
  description: "Delete managed resources.",
  category: "resource_delete",
  layer_target: "layer1",
  read_write_classification: "destructive_write",
  required_execution_mode: "execute",
  mode_policy_json: {
    read_only: "disabled",
    suggest: "requires_permission",
    execute: "requires_permission",
    governed_execute: "requires_permission",
  },
  enabled: true,
  requires_confirmation: false,
  input_schema_json: { type: "object", properties: {}, additionalProperties: true },
};

test("scripted_delete_cleanup runs in execute mode and resumes cleanup after approval", async () => {
  const store = new MemoryConversationStore();
  const conversation = await store.createConversation({
    title: "AI Engineer Session",
    execution_mode: "execute",
    initial_message: { role: "user", content: "Start AI Engineer session." },
  });

  const toolExecution = new FakeToolExecutionClient((input) => {
    const toolCallId = input.trace.tool_call_id ?? crypto.randomUUID();
    if (!input.permission_request_id) {
      return {
        conversation_id: input.trace.conversation_id,
        agent_run_id: input.trace.agent_run_id,
        request_id: input.trace.request_id,
        tool_call_id: toolCallId,
        status: "permission_required",
        output: { permission_request_id: "permission-delete-1" },
        raw_events: [
          {
            event_type: "tool.permission_required",
            emitted_by: "tool-execution-service",
            tool_call_id: toolCallId,
            payload: {
              tool_name: "delete_managed_resources",
              tool_call_id: toolCallId,
              permission_request_id: "permission-delete-1",
              execution_mode: "execute",
              prompt: { title: "Delete managed resources?", primary_action: "Approve delete" },
            },
          },
        ],
      };
    }
    return {
      conversation_id: input.trace.conversation_id,
      agent_run_id: input.trace.agent_run_id,
      request_id: input.trace.request_id,
      tool_call_id: toolCallId,
      status: "completed",
      output: { deleted: ["phase3-test-fixture-service"] },
      raw_events: [
        {
          event_type: "tool.started",
          emitted_by: "tool-execution-service",
          tool_call_id: toolCallId,
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
          tool_call_id: toolCallId,
          payload: {
            tool_name: "delete_managed_resources",
            status: "completed",
            result_preview: { deleted: ["phase3-test-fixture-service"] },
            duration_ms: 1,
          },
        },
      ],
    };
  });

  const app = createApp({
    config: baseRuntimeConfig({
      maxSteps: 3,
      requestTimeoutMs: 1000,
      scriptedMode: "scripted_delete_cleanup",
    }),
    store,
    contextClient: new FakeContextClient([contextResolvedEvent()]),
    toolRegistryClient: new FakeToolRegistryClient([DELETE_TOOL]),
    toolExecutionClient: toolExecution,
    toolPermissionClient: {
      async waitForDecision(input) {
        assert.equal(input.permissionRequestId, "permission-delete-1");
        return { status: "approved" };
      },
    },
    modelRunner: {
      async *stream(input) {
        void input.model;
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
  assert.ok(chunks.some((chunk) => chunk.kind === "event" && (chunk as { event: { event_type: string } }).event.event_type === "tool.permission_required"));
  assert.equal(toolExecution.calls.length, 2);
  assert.deepEqual(toolExecution.calls[0]?.input, { mode: "managed_unit", unit_id: "phase3-test-fixture-service" });
  assert.equal(toolExecution.calls[0]?.permission_request_id, undefined);
  assert.equal(toolExecution.calls[1]?.permission_request_id, "permission-delete-1");
});
