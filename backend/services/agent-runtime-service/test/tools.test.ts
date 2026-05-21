import assert from "node:assert/strict";
import test from "node:test";

import { createToolSet, filterToolDefinitionsForExecutionMode, schemaToZod } from "../src/ai/tools.js";
import type { ToolDefinition } from "../src/types.js";

const CONVERSATION_ID = "11111111-1111-4111-8111-111111111111";
const AGENT_RUN_ID = "22222222-2222-4222-8222-222222222222";
const REQUEST_ID = "33333333-3333-4333-8333-333333333333";
const TOOL_CALL_ID = "44444444-4444-4444-8444-444444444444";
const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

test("schemaToZod rejects unknown fields when additionalProperties is false", () => {
  const schema = schemaToZod({
    type: "object",
    properties: {
      service_slug: { type: "string" },
    },
    required: ["service_slug"],
    additionalProperties: false,
  });
  const result = schema.safeParse({ service_slug: "agent-runtime-service", extra: "nope" });
  assert.equal(result.success, false);
});

test("schemaToZod rejects confirmation_token when tool schema is strict empty object", () => {
  const schema = schemaToZod({
    type: "object",
    properties: {},
    additionalProperties: false,
  });
  const result = schema.safeParse({ confirmation_token: "wrong-place" });
  assert.equal(result.success, false);
});

test("createToolSet excludes disabled tools from active model tools", () => {
  const definitions: ToolDefinition[] = [
    {
      name: "enabled_tool",
      description: "Enabled",
      category: "platform_discovery",
      layer_target: "layer1",
      read_write_classification: "read",
      required_execution_mode: "read_only",
      enabled: true,
      requires_confirmation: false,
      input_schema_json: { type: "object", properties: {}, additionalProperties: false },
    },
    {
      name: "disabled_tool",
      description: "Disabled",
      category: "write_future",
      layer_target: "layer1",
      read_write_classification: "write",
      required_execution_mode: "execute",
      enabled: false,
      requires_confirmation: true,
      input_schema_json: { type: "object", properties: {}, additionalProperties: false },
    },
  ];

  const tools = createToolSet({
    toolDefinitions: definitions,
    executionMode: "governed_execute",
    trace: {
      conversation_id: CONVERSATION_ID,
      agent_run_id: AGENT_RUN_ID,
      request_id: REQUEST_ID,
      tool_call_id: null,
    },
    toolExecutionClient: {
      async execute() {
        return {
          conversation_id: CONVERSATION_ID,
          agent_run_id: AGENT_RUN_ID,
          request_id: REQUEST_ID,
          tool_call_id: TOOL_CALL_ID,
          status: "completed",
          output: {},
          raw_events: [],
        };
      },
    },
    emitRawToolEvents: async () => {},
  });

  assert.ok("enabled_tool" in tools);
  assert.equal("disabled_tool" in tools, false);
});

test("createToolSet replaces provider tool-call identifiers with platform UUIDs", async () => {
  const definitions: ToolDefinition[] = [
    {
      name: "enabled_tool",
      description: "Enabled",
      category: "platform_discovery",
      layer_target: "layer1",
      read_write_classification: "read",
      required_execution_mode: "read_only",
      enabled: true,
      requires_confirmation: false,
      input_schema_json: { type: "object", properties: {}, additionalProperties: false },
    },
  ];
  const traces: Array<{ tool_call_id?: string | null }> = [];
  const tools = createToolSet({
    toolDefinitions: definitions,
    executionMode: "read_only",
    trace: {
      conversation_id: CONVERSATION_ID,
      agent_run_id: AGENT_RUN_ID,
      request_id: REQUEST_ID,
      tool_call_id: null,
    },
    toolExecutionClient: {
      async execute(input) {
        traces.push({ tool_call_id: input.trace.tool_call_id });
        return {
          conversation_id: CONVERSATION_ID,
          agent_run_id: AGENT_RUN_ID,
          request_id: REQUEST_ID,
          tool_call_id: input.trace.tool_call_id ?? TOOL_CALL_ID,
          status: "completed",
          output: {},
          raw_events: [],
        };
      },
    },
    emitRawToolEvents: async () => {},
  });

  const runtimeTool = tools.enabled_tool as {
    execute: (args: Record<string, never>, options: { toolCallId: string }) => Promise<unknown>;
  };
  await runtimeTool.execute({}, { toolCallId: "call_provider_non_uuid" });

  assert.equal(traces.length, 1);
  assert.ok(traces[0]?.tool_call_id);
  assert.notEqual(traces[0]?.tool_call_id, "call_provider_non_uuid");
  assert.match(String(traces[0]?.tool_call_id), UUID_PATTERN);
});

test("filterToolDefinitionsForExecutionMode exposes permission-required tools", () => {
  const definitions: ToolDefinition[] = [
    {
      name: "deploy_preview_change",
      description: "Deploy preview",
      category: "deployment",
      layer_target: "layer1",
      read_write_classification: "write",
      required_execution_mode: "governed_execute",
      enabled: true,
      requires_confirmation: false,
      mode_policy_json: {
        read_only: "disabled",
        suggest: "requires_permission",
        execute: "requires_permission",
        governed_execute: "enabled",
      },
      input_schema_json: { type: "object", properties: {}, additionalProperties: false },
    },
    {
      name: "delete_managed_resources",
      description: "Delete",
      category: "resource_delete",
      layer_target: "layer1",
      read_write_classification: "destructive_write",
      required_execution_mode: "governed_execute",
      enabled: true,
      requires_confirmation: false,
      mode_policy_json: {
        read_only: "disabled",
        suggest: "disabled",
        execute: "disabled",
        governed_execute: "requires_permission",
      },
      input_schema_json: { type: "object", properties: {}, additionalProperties: false },
    },
  ];

  assert.deepEqual(filterToolDefinitionsForExecutionMode(definitions, "read_only").map((tool) => tool.name), []);
  assert.deepEqual(filterToolDefinitionsForExecutionMode(definitions, "suggest").map((tool) => tool.name), [
    "deploy_preview_change",
  ]);
  assert.deepEqual(filterToolDefinitionsForExecutionMode(definitions, "governed_execute").map((tool) => tool.name), [
    "deploy_preview_change",
    "delete_managed_resources",
  ]);
});

test("permission-required tool waits, re-executes with approval token, and returns final output", async () => {
  const definition: ToolDefinition = {
    name: "deploy_preview_change",
    description: "Deploy preview",
    category: "deployment",
    layer_target: "layer1",
    read_write_classification: "write",
    required_execution_mode: "governed_execute",
    enabled: true,
    requires_confirmation: false,
    mode_policy_json: {
      read_only: "disabled",
      suggest: "requires_permission",
      execute: "requires_permission",
      governed_execute: "enabled",
    },
    input_schema_json: { type: "object", properties: {}, additionalProperties: false },
  };
  const executionCalls: Array<{ approval_token?: string | null }> = [];
  const emittedEvents: string[] = [];
  const tools = createToolSet({
    toolDefinitions: [definition],
    executionMode: "execute",
    trace: {
      conversation_id: CONVERSATION_ID,
      agent_run_id: AGENT_RUN_ID,
      request_id: REQUEST_ID,
      tool_call_id: null,
    },
    toolExecutionClient: {
      async execute(input) {
        executionCalls.push({ approval_token: input.approval_token });
        if (!input.approval_token) {
          return {
            conversation_id: CONVERSATION_ID,
            agent_run_id: AGENT_RUN_ID,
            request_id: REQUEST_ID,
            tool_call_id: input.trace.tool_call_id ?? TOOL_CALL_ID,
            status: "permission_required",
            output: {
              permission_request_id: "permission-1",
              approval_token: "approval-token",
            },
            raw_events: [{ event_type: "tool.permission_required", emitted_by: "test", payload: {}, tool_call_id: input.trace.tool_call_id }],
          };
        }
        return {
          conversation_id: CONVERSATION_ID,
          agent_run_id: AGENT_RUN_ID,
          request_id: REQUEST_ID,
          tool_call_id: input.trace.tool_call_id ?? TOOL_CALL_ID,
          status: "completed",
          output: { deployment_id: "deployment-1" },
          raw_events: [{ event_type: "tool.completed", emitted_by: "test", payload: {}, tool_call_id: input.trace.tool_call_id }],
        };
      },
    },
    toolPermissionClient: {
      async waitForDecision(input) {
        assert.equal(input.permissionRequestId, "permission-1");
        return { status: "approved" };
      },
    },
    emitRawToolEvents: async (events) => {
      emittedEvents.push(...(events ?? []).map((event) => event.event_type));
    },
  });

  const runtimeTool = tools.deploy_preview_change as {
    execute: (args: Record<string, never>, options: { toolCallId: string }) => Promise<unknown>;
  };
  const output = await runtimeTool.execute({}, { toolCallId: "call_provider_non_uuid" });

  assert.deepEqual(output, { deployment_id: "deployment-1" });
  assert.deepEqual(executionCalls.map((call) => call.approval_token ?? null), [null, "approval-token"]);
  assert.deepEqual(emittedEvents, ["tool.permission_required", "tool.completed"]);
});

test("permission-required tool returns denial result without executing approved path", async () => {
  const definition: ToolDefinition = {
    name: "delete_managed_resources",
    description: "Delete",
    category: "resource_delete",
    layer_target: "layer1",
    read_write_classification: "destructive_write",
    required_execution_mode: "governed_execute",
    enabled: true,
    requires_confirmation: false,
    mode_policy_json: {
      read_only: "disabled",
      suggest: "requires_permission",
      execute: "requires_permission",
      governed_execute: "requires_permission",
    },
    input_schema_json: { type: "object", properties: {}, additionalProperties: false },
  };
  let executionCount = 0;
  const tools = createToolSet({
    toolDefinitions: [definition],
    executionMode: "governed_execute",
    trace: {
      conversation_id: CONVERSATION_ID,
      agent_run_id: AGENT_RUN_ID,
      request_id: REQUEST_ID,
      tool_call_id: null,
    },
    toolExecutionClient: {
      async execute(input) {
        executionCount += 1;
        return {
          conversation_id: CONVERSATION_ID,
          agent_run_id: AGENT_RUN_ID,
          request_id: REQUEST_ID,
          tool_call_id: input.trace.tool_call_id ?? TOOL_CALL_ID,
          status: "permission_required",
          output: {
            permission_request_id: "permission-2",
            approval_token: "approval-token",
          },
          raw_events: [],
        };
      },
    },
    toolPermissionClient: {
      async waitForDecision() {
        return { status: "denied", reason: "user_denied" };
      },
    },
    emitRawToolEvents: async () => {},
  });

  const runtimeTool = tools.delete_managed_resources as {
    execute: (args: Record<string, never>, options: { toolCallId: string }) => Promise<unknown>;
  };
  const output = await runtimeTool.execute({}, { toolCallId: "call_provider_non_uuid" });

  assert.equal(executionCount, 1);
  assert.deepEqual(output, {
    status: "permission_denied",
    message: "The user denied this tool call. No action was taken.",
    permission_request_id: "permission-2",
    tool_name: "delete_managed_resources",
    reason: "user_denied",
  });
});
