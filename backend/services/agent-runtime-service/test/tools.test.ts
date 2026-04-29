import assert from "node:assert/strict";
import test from "node:test";

import { createToolSet, schemaToZod } from "../src/ai/tools.js";
import type { ToolDefinition } from "../src/types.js";

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
      conversation_id: "conversation-1",
      agent_run_id: "agent-run-1",
      request_id: "request-1",
      tool_call_id: null,
    },
    toolExecutionClient: {
      async execute() {
        return {
          conversation_id: "conversation-1",
          agent_run_id: "agent-run-1",
          request_id: "request-1",
          tool_call_id: "tool-call-1",
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
