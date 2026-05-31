import assert from "node:assert/strict";
import test from "node:test";

import { createApp } from "../src/server.js";
import type { ToolDefinition, ToolExecutionResponse } from "../src/types.js";
import { baseRuntimeConfig, contextResolvedEvent, FakeContextClient, FakeToolExecutionClient, FakeToolRegistryClient, MemoryConversationStore, parseNdjson } from "./helpers.js";

const WRITE_TOOL_DEFINITIONS: ToolDefinition[] = [
  "create_working_branch",
  "scaffold_service",
  "write_source_file",
  "create_commit",
  "deploy_service_or_application",
  "run_deployment_validation",
].map((name) => ({
  name,
  description: name,
  category: "phase3-test",
  layer_target: "layer1",
  read_write_classification: name === "run_deployment_validation" ? "read" : "write",
  required_execution_mode: name === "run_deployment_validation" ? "read_only" : "execute",
  mode_policy_json:
    name === "run_deployment_validation"
      ? { read_only: "enabled", suggest: "enabled", execute: "enabled", governed_execute: "enabled" }
      : name === "deploy_service_or_application"
      ? { read_only: "disabled", suggest: "requires_permission", execute: "requires_permission", governed_execute: "enabled" }
      : { read_only: "disabled", suggest: "enabled", execute: "enabled", governed_execute: "enabled" },
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
    output: {
      ok: true,
      tool_name: toolName,
      ...(toolName === "deploy_service_or_application" ? { deployment_id: "dep_scripted_fixture" } : {}),
      ...(toolName === "run_deployment_validation" ? { deployment_id: "dep_scripted_fixture", validation_status: "passed" } : {}),
    },
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

test("scripted_write_deploy runs in execute mode and resumes direct deployment after approval", async () => {
  const store = new MemoryConversationStore();
  const conversation = await store.createConversation({
    title: "AI Engineer Session",
    execution_mode: "execute",
    initial_message: { role: "user", content: "Start AI Engineer session." },
  });

  const toolExecution = new FakeToolExecutionClient((input) => {
    if (input.tool_name === "deploy_service_or_application" && !input.permission_request_id) {
      const toolCallId = input.trace.tool_call_id ?? crypto.randomUUID();
      return {
        conversation_id: input.trace.conversation_id,
        agent_run_id: input.trace.agent_run_id,
        request_id: input.trace.request_id,
        tool_call_id: toolCallId,
        status: "permission_required",
        output: { permission_request_id: "permission-deploy-1" },
        raw_events: [
          {
            event_type: "tool.permission_required",
            emitted_by: "tool-execution-service",
            tool_call_id: toolCallId,
            payload: {
              tool_name: "deploy_service_or_application",
              tool_call_id: toolCallId,
              permission_request_id: "permission-deploy-1",
              execution_mode: "execute",
              prompt: { title: "Deploy managed service or application?", primary_action: "Approve deploy" },
            },
          },
        ],
      };
    }
    return toolResponse(input.tool_name, input.trace);
  });
  const app = createApp({
    config: baseRuntimeConfig({
      maxSteps: 3,
      requestTimeoutMs: 1000,
      scriptedMode: "scripted_write_deploy",
    }),
    store,
    contextClient: new FakeContextClient([contextResolvedEvent()]),
    toolRegistryClient: new FakeToolRegistryClient(WRITE_TOOL_DEFINITIONS),
    toolExecutionClient: toolExecution,
    toolPermissionClient: {
      async waitForDecision(input) {
        assert.equal(input.permissionRequestId, "permission-deploy-1");
        return {
          status: "approved",
          raw_events: [
            {
              event_type: "tool.permission_approved",
              emitted_by: "tool-execution-service",
              tool_call_id: toolExecution.calls.at(-1)?.trace.tool_call_id ?? null,
              payload: {
                tool_name: "deploy_service_or_application",
                tool_call_id: toolExecution.calls.at(-1)?.trace.tool_call_id ?? null,
                permission_request_id: "permission-deploy-1",
              },
            },
          ],
        };
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
      messages: [{ role: "user", content: "Deploy the deterministic Phase 3 fixture." }],
    }),
  });

  assert.equal(response.status, 200);
  const chunks = parseNdjson(await response.text());
  const completed = chunks.find((chunk) => chunk.kind === "event" && (chunk as { event: { event_type: string } }).event.event_type === "run.completed");
  assert.ok(completed);
  const changeSummary = chunks.find(
    (chunk) => chunk.kind === "event" && (chunk as { event: { event_type: string } }).event.event_type === "change.summary",
  ) as { event: { payload: Record<string, unknown> } } | undefined;
  assert.ok(changeSummary, "change.summary event should be emitted after deploy");
  assert.equal(changeSummary?.event.payload.branch, "feature/phase3-no-llm");
  assert.equal(changeSummary?.event.payload.target_unit_id, "phase3-test-fixture-service");
  assert.equal(changeSummary?.event.payload.affected_capability, "phase3-test-fixture");
  assert.deepEqual(changeSummary?.event.payload.changed_files, [
    "project/space-ops-platform/backend/services/phase3-test-fixture-service/requirements.txt",
    "project/space-ops-platform/backend/services/phase3-test-fixture-service/app/main.py",
  ]);
  assert.deepEqual(toolExecution.calls.map((call) => call.tool_name), [
    "create_working_branch",
    "scaffold_service",
    "write_source_file",
    "write_source_file",
    "create_commit",
    "deploy_service_or_application",
    "deploy_service_or_application",
    "run_deployment_validation",
  ]);
  assert.ok(chunks.some((chunk) => chunk.kind === "event" && (chunk as { event: { event_type: string } }).event.event_type === "tool.permission_required"));
  assert.ok(chunks.some((chunk) => chunk.kind === "event" && (chunk as { event: { event_type: string } }).event.event_type === "tool.permission_approved"));
  assert.ok(toolExecution.calls.every((call) => call.execution_mode === "execute"));
  assert.deepEqual(toolExecution.calls[0]?.input, { branch: "feature/phase3-no-llm", from_branch: "main" });
  assert.equal(toolExecution.calls[1]?.input.unit_id, "phase3-test-fixture-service");
  assert.equal(toolExecution.calls[5]?.input.unit_id, "phase3-test-fixture-service");
  assert.equal(toolExecution.calls[5]?.permission_request_id, undefined);
  assert.equal(toolExecution.calls[6]?.tool_name, "deploy_service_or_application");
  assert.equal(toolExecution.calls[6]?.permission_request_id, "permission-deploy-1");
  assert.equal(toolExecution.calls[7]?.tool_name, "run_deployment_validation");
  assert.deepEqual(toolExecution.calls[7]?.input, { deployment_id: "dep_scripted_fixture" });
  assert.equal(changeSummary?.event.payload.validation_status, "passed");
});

test("scripted_write_deploy fails before mutation when execution mode is read_only", async () => {
  const store = new MemoryConversationStore();
  const conversation = await store.createConversation({
    title: "AI Engineer Session",
    execution_mode: "read_only",
    initial_message: { role: "user", content: "Start AI Engineer session." },
  });

  const toolExecution = new FakeToolExecutionClient((input) => toolResponse(input.tool_name, input.trace));
  const app = createApp({
    config: baseRuntimeConfig({
      maxSteps: 3,
      requestTimeoutMs: 1000,
      scriptedMode: "scripted_write_deploy",
    }),
    store,
    contextClient: new FakeContextClient([contextResolvedEvent()]),
    toolRegistryClient: new FakeToolRegistryClient(WRITE_TOOL_DEFINITIONS),
    toolExecutionClient: toolExecution,
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
