import assert from "node:assert/strict";
import test from "node:test";

import { filterToolDefinitionsForExecutionMode } from "../src/ai/tools.js";
import type { ToolDefinition } from "../src/types.js";

const defs: ToolDefinition[] = [
  {
    name: "list_documents",
    description: "Docs",
    category: "documents",
    layer_target: "layer2",
    read_write_classification: "read",
    required_execution_mode: "read_only",
    enabled: true,
    requires_confirmation: false,
    input_schema_json: { type: "object", properties: {}, additionalProperties: false },
  },
  {
    name: "write_source_file",
    description: "Write",
    category: "code_write",
    layer_target: "layer1",
    read_write_classification: "write",
    required_execution_mode: "execute",
    enabled: true,
    requires_confirmation: false,
    input_schema_json: { type: "object", properties: {}, additionalProperties: false },
  },
  {
    name: "trigger_document_reingestion",
    description: "Reingest",
    category: "documents",
    layer_target: "layer2",
    read_write_classification: "write",
    required_execution_mode: "execute",
    enabled: true,
    requires_confirmation: false,
    input_schema_json: { type: "object", properties: {}, additionalProperties: false },
  },
];

test("filterToolDefinitionsForExecutionMode hides write tools until execute", () => {
  const readSorted = filterToolDefinitionsForExecutionMode(defs, "read_only").map((t) => t.name);
  assert.deepEqual(readSorted.sort(), ["list_documents"]);

  const suggest = filterToolDefinitionsForExecutionMode(defs, "suggest").map((t) => t.name).sort();
  assert.deepEqual(suggest, ["list_documents"]);

  const exec = filterToolDefinitionsForExecutionMode(defs, "execute").map((t) => t.name).sort();
  assert.deepEqual(exec, ["list_documents", "trigger_document_reingestion", "write_source_file"]);
});
