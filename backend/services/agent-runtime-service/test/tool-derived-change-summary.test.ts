import assert from "node:assert/strict";
import test from "node:test";

import { createApp } from "../src/server.js";
import type {
  ChangeSummaryRegistryClient,
  ChangeSummaryRegistryUnit,
} from "../src/change-summary.js";
import type {
  ModelRunner,
  ModelStreamPart,
  ToolDefinition,
  ToolExecutionResponse,
} from "../src/types.js";
import {
  baseRuntimeConfig,
  contextResolvedEvent,
  FakeContextClient,
  FakeToolExecutionClient,
  FakeToolRegistryClient,
  MemoryConversationStore,
  parseNdjson,
} from "./helpers.js";

const REAL_TOOL_DEFINITIONS: ToolDefinition[] = [
  {
    name: "create_working_branch",
    description: "Create a working branch in the managed fork.",
    category: "code-write",
    layer_target: "layer1",
    read_write_classification: "write",
    required_execution_mode: "execute",
    enabled: true,
    requires_confirmation: false,
    input_schema_json: {
      type: "object",
      properties: {
        branch: { type: "string" },
        from_branch: { type: "string" },
      },
      required: ["branch"],
      additionalProperties: false,
    },
  },
  {
    name: "write_source_file",
    description: "Write a file on a managed branch.",
    category: "code-write",
    layer_target: "layer1",
    read_write_classification: "write",
    required_execution_mode: "execute",
    enabled: true,
    requires_confirmation: false,
    input_schema_json: {
      type: "object",
      properties: {
        branch: { type: "string" },
        path: { type: "string" },
        content: { type: "string" },
      },
      required: ["branch", "path", "content"],
      additionalProperties: false,
    },
  },
  {
    name: "create_commit",
    description: "Create a commit on a managed branch.",
    category: "code-write",
    layer_target: "layer1",
    read_write_classification: "write",
    required_execution_mode: "execute",
    enabled: true,
    requires_confirmation: false,
    input_schema_json: {
      type: "object",
      properties: {
        branch: { type: "string" },
        message: { type: "string" },
      },
      required: ["branch", "message"],
      additionalProperties: false,
    },
  },
];

function envelopeFor(toolName: string): Record<string, unknown> {
  if (toolName === "create_working_branch") {
    return {
      branch: "preview/agent-real",
      commit_sha: "baseline-sha",
      changed_files: [],
      data: { created: true, base_branch: "main", base_commit_sha: "baseline-sha" },
    };
  }
  if (toolName === "write_source_file") {
    return {
      branch: "preview/agent-real",
      commit_sha: "baseline-sha",
      changed_files: [
        "project/space-ops-platform/backend/services/derived-telemetry-service/app/main.py",
      ],
      data: { written: true },
    };
  }
  return {
    branch: "preview/agent-real",
    commit_sha: "preview-sha",
    changed_files: [
      "project/space-ops-platform/backend/services/derived-telemetry-service/app/main.py",
    ],
    data: { message: "agent commit" },
  };
}

const REGISTRY_UNITS: ChangeSummaryRegistryUnit[] = [
  {
    unit_id: "derived-telemetry-service",
    source_path: "project/space-ops-platform/backend/services/derived-telemetry-service",
    service_slug: "derived-telemetry-service",
    application_id: null,
    runtime_kind: "service",
    capabilities: ["telemetry"],
    category: "telemetry",
  },
];

class StaticRegistryClient implements ChangeSummaryRegistryClient {
  async listUnits(): Promise<ChangeSummaryRegistryUnit[]> {
    return REGISTRY_UNITS;
  }
}

function modelRunnerWithTools(): ModelRunner {
  return {
    async *stream(input): AsyncIterable<ModelStreamPart> {
      void input.model;
      const { tools } = input;
      yield { type: "text-delta", textDelta: "Preparing preview..." };
      const branchTool = tools.create_working_branch;
      const writeTool = tools.write_source_file;
      const commitTool = tools.create_commit;
      assert.ok(branchTool?.execute, "branch tool must be wired");
      assert.ok(writeTool?.execute, "write tool must be wired");
      assert.ok(commitTool?.execute, "commit tool must be wired");
      await branchTool.execute!({ branch: "preview/agent-real", from_branch: "main" }, { toolCallId: "tc-1" });
      await writeTool.execute!(
        {
          branch: "preview/agent-real",
          path: "project/space-ops-platform/backend/services/derived-telemetry-service/app/main.py",
          content: "from fastapi import FastAPI\napp = FastAPI()\n",
        },
        { toolCallId: "tc-2" },
      );
      await commitTool.execute!({ branch: "preview/agent-real", message: "agent commit" }, { toolCallId: "tc-3" });
      yield { type: "text-delta", textDelta: " Ready to deploy." };
    },
  };
}

function toolExecutionResponse(
  toolName: string,
  trace: { conversation_id: string; agent_run_id: string; request_id: string; tool_call_id?: string | null },
): ToolExecutionResponse {
  const toolCallId = trace.tool_call_id ?? crypto.randomUUID();
  return {
    conversation_id: trace.conversation_id,
    agent_run_id: trace.agent_run_id,
    request_id: trace.request_id,
    tool_call_id: toolCallId,
    status: "completed",
    output: envelopeFor(toolName),
    raw_events: [
      {
        event_type: "tool.started",
        emitted_by: "tool-execution-service",
        tool_call_id: toolCallId,
        payload: { tool_name: toolName, category: "code-write", read_write_classification: "write", input_preview: {} },
      },
      {
        event_type: "tool.completed",
        emitted_by: "tool-execution-service",
        tool_call_id: toolCallId,
        payload: { tool_name: toolName, status: "completed", result_preview: envelopeFor(toolName), duration_ms: 1 },
      },
    ],
  };
}

test("real LLM-driven tool flow emits change.summary with structured base_commit_sha and target metadata", async () => {
  const store = new MemoryConversationStore();
  const conversation = await store.createConversation({
    title: "AI Engineer Session",
    execution_mode: "execute",
  });
  const toolExecution = new FakeToolExecutionClient((input) => toolExecutionResponse(input.tool_name, input.trace));
  const app = createApp({
    config: baseRuntimeConfig({
      openAiApiKey: "test-key",
      maxSteps: 8,
      requestTimeoutMs: 1000,
      allowMissingKeyFallback: false,
    }),
    store,
    contextClient: new FakeContextClient([contextResolvedEvent()]),
    toolRegistryClient: new FakeToolRegistryClient(REAL_TOOL_DEFINITIONS),
    toolExecutionClient: toolExecution,
    modelRunner: modelRunnerWithTools(),
    changeSummaryRegistryClient: new StaticRegistryClient(),
  });

  const response = await app.request("/chat", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      conversation_id: conversation.id,
      execution_mode: "execute",
      messages: [{ role: "user", content: "Tag the derived telemetry service as preview." }],
    }),
  });

  assert.equal(response.status, 200);
  const chunks = parseNdjson(await response.text());
  const changeSummary = chunks.find(
    (chunk) => chunk.kind === "event" && (chunk as { event: { event_type: string } }).event.event_type === "change.summary",
  ) as { event: { payload: Record<string, unknown> } } | undefined;
  assert.ok(changeSummary, "change.summary must be emitted from real tool flow");
  assert.equal(changeSummary.event.payload.branch, "preview/agent-real");
  assert.equal(changeSummary.event.payload.base_branch, "main");
  assert.equal(changeSummary.event.payload.base_commit_sha, "baseline-sha");
  assert.equal(changeSummary.event.payload.commit_sha, "preview-sha");
  assert.equal(changeSummary.event.payload.target_unit_id, "derived-telemetry-service");
  assert.deepEqual(changeSummary.event.payload.changed_files, [
    "project/space-ops-platform/backend/services/derived-telemetry-service/app/main.py",
  ]);
});
