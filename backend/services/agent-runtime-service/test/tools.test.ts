import assert from "node:assert/strict";
import test from "node:test";

import { buildSystemPrompt } from "../src/ai/prompts.js";
import { createToolSet, filterToolDefinitionsForExecutionMode, schemaToZod, validateToolDefinitionsForModel } from "../src/ai/tools.js";
import type { ContextPacketResponse, RetrievalPlan, ToolDefinition } from "../src/types.js";

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

const CALL_PLATFORM_HTTP_GET_SCHEMA = {
  type: "object",
  required: ["path"],
  additionalProperties: false,
  properties: {
    path: {
      type: "string",
      description: "Same-origin platform path beginning with /. Absolute URLs are not allowed.",
    },
    query: {
      type: "object",
      description: "Optional query parameters to append to the request.",
      additionalProperties: { type: ["string", "number", "boolean"] },
    },
    headers: {
      type: "object",
      description: "Optional non-sensitive request headers.",
      additionalProperties: { type: "string" },
    },
    timeout_ms: {
      type: "integer",
      minimum: 100,
      maximum: 30000,
      default: 10000,
    },
    expected_status: {
      oneOf: [
        { type: "integer", minimum: 100, maximum: 599 },
        {
          type: "array",
          items: { type: "integer", minimum: 100, maximum: 599 },
        },
      ],
      description: "Optional expected HTTP status or list of acceptable statuses.",
    },
    max_response_bytes: {
      type: "integer",
      minimum: 1024,
      maximum: 131072,
      default: 32768,
    },
  },
};

test("schemaToZod accepts call_platform_http_get rich JSON Schema inputs", () => {
  const schema = schemaToZod(CALL_PLATFORM_HTTP_GET_SCHEMA);

  assert.equal(schema.safeParse({ path: "/apps/overview" }).success, true);
  assert.equal(
    schema.safeParse({
      path: "/apps/overview",
      query: { source_id: "sim", limit: 10, latest: true },
    }).success,
    true,
  );
  assert.equal(
    schema.safeParse({
      path: "/apps/overview",
      headers: { "x-request-id": "abc" },
    }).success,
    true,
  );
  assert.equal(schema.safeParse({ path: "/apps/overview", expected_status: 200 }).success, true);
  assert.equal(schema.safeParse({ path: "/apps/overview", expected_status: [200, 204] }).success, true);
});

test("schemaToZod rejects invalid call_platform_http_get inputs", () => {
  const schema = schemaToZod(CALL_PLATFORM_HTTP_GET_SCHEMA);

  assert.equal(schema.safeParse({ path: "/apps/overview", extra: true }).success, false);
  assert.equal(
    schema.safeParse({
      path: "/apps/overview",
      headers: { authorization: 123 },
    }).success,
    false,
  );
  assert.equal(schema.safeParse({ path: "/apps/overview", expected_status: "200" }).success, false);
});

test("schemaToZod handles simplified get_code_index_status schema", () => {
  const schema = schemaToZod({
    type: "object",
    properties: {
      repository: { type: "string", maxLength: 256 },
      root: { type: "string", maxLength: 512 },
      branch: { type: "string", maxLength: 256 },
    },
    additionalProperties: false,
  });

  assert.equal(schema.safeParse({ repository: "space-ops-platform" }).success, true);
  assert.equal(schema.safeParse({ root: "project/space-ops-platform" }).success, true);
  assert.equal(schema.safeParse({ repository: "x", extra: true }).success, false);
  assert.equal(schema.safeParse({}).success, true);
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
    assistantMessageId: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
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

test("createToolSet skips malformed tool schema and emits diagnostic", () => {
  const diagnostics: Array<{ name: string; reason: string }> = [];
  const emittedEvents: Array<{ event_type: string; payload: Record<string, unknown> }> = [];
  const definitions: ToolDefinition[] = [
    {
      name: "valid_tool",
      description: "Valid",
      category: "platform_discovery",
      layer_target: "layer1",
      read_write_classification: "read",
      required_execution_mode: "read_only",
      enabled: true,
      requires_confirmation: false,
      input_schema_json: { type: "object", properties: {}, additionalProperties: false },
    },
    {
      name: "bad_tool",
      description: "Bad",
      category: "platform_discovery",
      layer_target: "layer1",
      read_write_classification: "read",
      required_execution_mode: "read_only",
      enabled: true,
      requires_confirmation: false,
      input_schema_json: { type: "None" } as unknown as Record<string, unknown>,
    },
  ];

  const tools = createToolSet({
    toolDefinitions: definitions,
    executionMode: "read_only",
    trace: {
      conversation_id: CONVERSATION_ID,
      agent_run_id: AGENT_RUN_ID,
      request_id: REQUEST_ID,
      tool_call_id: null,
    },
    assistantMessageId: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
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
    onToolSchemaInvalid(definition, reason) {
      diagnostics.push({ name: definition.name, reason });
    },
    emitRawToolEvents: async (events) => {
      emittedEvents.push(...(events ?? []).map((event) => ({ event_type: event.event_type, payload: event.payload })));
    },
  });

  assert.ok("valid_tool" in tools);
  assert.equal("bad_tool" in tools, false);
  assert.deepEqual(diagnostics, [{ name: "bad_tool", reason: 'tool input schema root type must be "object"' }]);
  assert.equal(emittedEvents.length, 1);
  assert.equal(emittedEvents[0]?.event_type, "tool.schema_invalid");
  assert.deepEqual(emittedEvents[0]?.payload, {
    tool_name: "bad_tool",
    reason: 'tool input schema root type must be "object"',
    action: "omitted_from_model_toolset",
  });
});

test("validated tool definitions drive both provider toolset and prompt-visible tools", () => {
  const definitions: ToolDefinition[] = [
    {
      name: "valid_tool",
      description: "Visible valid tool",
      category: "platform_discovery",
      layer_target: "layer1",
      read_write_classification: "read",
      required_execution_mode: "read_only",
      enabled: true,
      requires_confirmation: false,
      input_schema_json: { type: "object", properties: {}, additionalProperties: false },
    },
    {
      name: "bad_tool",
      description: "Should not be visible",
      category: "platform_discovery",
      layer_target: "layer1",
      read_write_classification: "read",
      required_execution_mode: "read_only",
      enabled: true,
      requires_confirmation: false,
      input_schema_json: { type: "None" } as unknown as Record<string, unknown>,
    },
  ];
  const { validToolDefinitions, skippedToolSchemaDiagnostics } = validateToolDefinitionsForModel({
    toolDefinitions: definitions,
    executionMode: "read_only",
  });
  const tools = createToolSet({
    toolDefinitions: validToolDefinitions,
    executionMode: "read_only",
    trace: {
      conversation_id: CONVERSATION_ID,
      agent_run_id: AGENT_RUN_ID,
      request_id: REQUEST_ID,
      tool_call_id: null,
    },
    assistantMessageId: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
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
  const retrievalPlan: RetrievalPlan = {
    documents: false,
    code: false,
    platform: true,
    tools: true,
    summary: "documents=false, code=false, platform=true, tools=true",
  };
  const context: ContextPacketResponse = {
    conversation_id: CONVERSATION_ID,
    agent_run_id: AGENT_RUN_ID,
    request_id: REQUEST_ID,
    context_packet_id: "ctx-1",
    document_chunk_count: 0,
    code_chunk_count: 0,
    platform_metadata_bytes: 0,
    tool_definition_count: validToolDefinitions.length,
    truncated: false,
    truncation_reasons: [],
    data: {},
    raw_events: [],
  };
  const prompt = buildSystemPrompt({
    executionMode: "read_only",
    retrievalPlan,
    context,
    tools: validToolDefinitions,
    messages: [{ role: "user", content: "inspect tools" }],
  });

  assert.deepEqual(validToolDefinitions.map((definition) => definition.name), ["valid_tool"]);
  assert.deepEqual(skippedToolSchemaDiagnostics.map((diagnostic) => diagnostic.definition.name), ["bad_tool"]);
  assert.ok("valid_tool" in tools);
  assert.equal("bad_tool" in tools, false);
  assert.match(prompt, /valid_tool: Visible valid tool/);
  assert.doesNotMatch(prompt, /bad_tool/);
  assert.doesNotMatch(prompt, /Should not be visible/);
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
    assistantMessageId: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
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
      required_execution_mode: "execute",
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
      required_execution_mode: "execute",
      enabled: true,
      requires_confirmation: false,
      mode_policy_json: {
        read_only: "disabled",
        suggest: "requires_permission",
        execute: "requires_permission",
        governed_execute: "requires_permission",
      },
      input_schema_json: { type: "object", properties: {}, additionalProperties: false },
    },
  ];

  assert.deepEqual(filterToolDefinitionsForExecutionMode(definitions, "read_only").map((tool) => tool.name), []);
  assert.deepEqual(filterToolDefinitionsForExecutionMode(definitions, "suggest").map((tool) => tool.name), [
    "deploy_preview_change",
    "delete_managed_resources",
  ]);
  assert.deepEqual(filterToolDefinitionsForExecutionMode(definitions, "execute").map((tool) => tool.name), [
    "deploy_preview_change",
    "delete_managed_resources",
  ]);
  assert.deepEqual(filterToolDefinitionsForExecutionMode(definitions, "governed_execute").map((tool) => tool.name), [
    "deploy_preview_change",
    "delete_managed_resources",
  ]);
});

test("permission-required tool waits, re-executes with permission request id, and returns final output", async () => {
  const definition: ToolDefinition = {
    name: "deploy_preview_change",
    description: "Deploy preview",
    category: "deployment",
    layer_target: "layer1",
    read_write_classification: "write",
    required_execution_mode: "execute",
    enabled: true,
    requires_confirmation: false,
    mode_policy_json: {
      read_only: "disabled",
      suggest: "requires_permission",
      execute: "requires_permission",
      governed_execute: "enabled",
    },
    input_schema_json: {
      type: "object",
      properties: {
        branch: { type: "string" },
        target_unit_id: { type: "string" },
        target_application_id: { type: "string" },
      },
      required: ["branch", "target_unit_id"],
      additionalProperties: false,
    },
  };
  const executionCalls: Array<{ permission_request_id?: string | null }> = [];
  const emittedEvents: Array<{ event_type: string; payload: Record<string, unknown> }> = [];
  const tools = createToolSet({
    toolDefinitions: [definition],
    executionMode: "execute",
    trace: {
      conversation_id: CONVERSATION_ID,
      agent_run_id: AGENT_RUN_ID,
      request_id: REQUEST_ID,
      tool_call_id: null,
    },
    assistantMessageId: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
    toolExecutionClient: {
      async execute(input) {
        executionCalls.push({ permission_request_id: input.permission_request_id });
        if (!input.permission_request_id) {
          return {
            conversation_id: CONVERSATION_ID,
            agent_run_id: AGENT_RUN_ID,
            request_id: REQUEST_ID,
            tool_call_id: input.trace.tool_call_id ?? TOOL_CALL_ID,
            status: "permission_required",
            output: {
              permission_request_id: "permission-1",
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
      emittedEvents.push(...(events ?? []).map((event) => ({ event_type: event.event_type, payload: event.payload })));
    },
  });

  const runtimeTool = tools.deploy_preview_change as {
    execute: (args: Record<string, string>, options: { toolCallId: string }) => Promise<unknown>;
  };
  const output = await runtimeTool.execute(
    {
      branch: "preview/cyan",
      target_unit_id: "mission-control-frontend-shell",
      target_application_id: "telemetry",
    },
    { toolCallId: "call_provider_non_uuid" },
  );

  assert.deepEqual(output, { deployment_id: "deployment-1" });
  assert.deepEqual(executionCalls.map((call) => call.permission_request_id ?? null), [null, "permission-1"]);
  assert.deepEqual(emittedEvents.map((event) => event.event_type), ["tool.permission_required", "deployment.requested", "tool.completed"]);
  assert.deepEqual(emittedEvents[1]?.payload, {
    tool_name: "deploy_preview_change",
    branch: "preview/cyan",
    unit_id: "mission-control-frontend-shell",
    target_application_id: "telemetry",
    status: "requested",
  });
});

test("permission-required tool returns denial result without executing approved path", async () => {
  const definition: ToolDefinition = {
    name: "delete_managed_resources",
    description: "Delete",
    category: "resource_delete",
    layer_target: "layer1",
    read_write_classification: "destructive_write",
    required_execution_mode: "execute",
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
    executionMode: "execute",
    trace: {
      conversation_id: CONVERSATION_ID,
      agent_run_id: AGENT_RUN_ID,
      request_id: REQUEST_ID,
      tool_call_id: null,
    },
    assistantMessageId: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
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
