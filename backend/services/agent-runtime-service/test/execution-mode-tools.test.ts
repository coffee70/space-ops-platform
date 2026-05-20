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
    name: "create_working_branch",
    description: "Branch",
    category: "code_write",
    layer_target: "layer1",
    read_write_classification: "write",
    required_execution_mode: "execute",
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
    name: "create_commit",
    description: "Commit",
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
  {
    name: "deploy_service_or_application",
    description: "Deploy",
    category: "deployment",
    layer_target: "layer1",
    read_write_classification: "write",
    required_execution_mode: "governed_execute",
    enabled: true,
    requires_confirmation: false,
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
    input_schema_json: { type: "object", properties: {}, additionalProperties: false },
  },
];

test("filterToolDefinitionsForExecutionMode hides write tools until execute", () => {
  const readSorted = filterToolDefinitionsForExecutionMode(defs, "read_only").map((t) => t.name);
  assert.deepEqual(readSorted.sort(), ["list_documents"]);

  const suggest = filterToolDefinitionsForExecutionMode(defs, "suggest").map((t) => t.name).sort();
  assert.deepEqual(suggest, ["list_documents"]);

  const exec = filterToolDefinitionsForExecutionMode(defs, "execute").map((t) => t.name).sort();
  assert.deepEqual(exec, [
    "create_commit",
    "create_working_branch",
    "list_documents",
    "trigger_document_reingestion",
    "write_source_file",
  ]);
  assert.ok(exec.includes("create_working_branch"));
  assert.ok(exec.includes("write_source_file"));
  assert.ok(exec.includes("create_commit"));
  assert.ok(!exec.includes("deploy_service_or_application"));
  assert.ok(!exec.includes("delete_managed_resources"));

  const governed = filterToolDefinitionsForExecutionMode(defs, "governed_execute").map((t) => t.name).sort();
  assert.ok(governed.includes("deploy_service_or_application"));
  assert.ok(governed.includes("delete_managed_resources"));
});
