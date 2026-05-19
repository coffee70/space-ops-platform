import assert from "node:assert/strict";
import test from "node:test";

import { createToolSet, schemaToZod } from "../src/ai/tools.js";
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
