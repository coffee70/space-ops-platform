import type { AgentEventStream } from "./events/stream.js";
import type { ConversationStore, ExecutionMode, RawEventFact, ToolDefinition, ToolExecutionClient, ToolExecutionResponse, TraceEnvelope } from "./types.js";

const FIXTURE_UNIT_ID = "phase3-test-fixture-service";
const FIXTURE_BRANCH = "feature/phase3-no-llm";
const FIXTURE_SOURCE_ROOT = `project/space-ops-platform/backend/services/${FIXTURE_UNIT_ID}`;

const FIXTURE_FILES: Array<{ path: string; content: string }> = [
  {
    path: `${FIXTURE_SOURCE_ROOT}/requirements.txt`,
    content: "fastapi==0.115.0\nuvicorn==0.32.0\n",
  },
  {
    path: `${FIXTURE_SOURCE_ROOT}/app/main.py`,
    content: [
      "from fastapi import FastAPI",
      "",
      'app = FastAPI(title="Phase 3 Test Fixture Service")',
      "",
      '@app.get("/health")',
      "def health():",
      '    return {"status": "ok", "service": "phase3-test-fixture-service"}',
      "",
      '@app.get("/metadata")',
      "def metadata():",
      '    return {"display_name": "Phase 3 Test Fixture Service", "mode": "deterministic"}',
      "",
    ].join("\n"),
  },
];

type ScriptedRunResult =
  | { status: "completed"; assistantText: string; toolCallCount: number }
  | { status: "failed"; toolCallCount: number };

function normalizeModeName(message: string): string | null {
  const match = message.match(/\[scripted:([a-z0-9_-]+)\]/i);
  return match ? match[1].toLowerCase() : null;
}

export function resolveScriptedMode(configuredMode: string | null, message: string): string | null {
  return configuredMode ?? normalizeModeName(message);
}

function modeRank(mode: ExecutionMode): number {
  return {
    read_only: 0,
    suggest: 1,
    execute: 2,
    governed_execute: 3,
  }[mode];
}

function getToolDefinition(definitions: ToolDefinition[], toolName: string, executionMode: ExecutionMode): ToolDefinition {
  const definition = definitions.find((candidate) => candidate.name === toolName && candidate.enabled);
  if (!definition) {
    throw new Error(`scripted tool not available: ${toolName}`);
  }
  if (modeRank(executionMode) < modeRank(definition.required_execution_mode)) {
    throw new Error(
      `scripted mode requires ${definition.required_execution_mode} for ${toolName}, received ${executionMode}`,
    );
  }
  return definition;
}

async function executeTool(input: {
  toolDefinitions: ToolDefinition[];
  toolExecutionClient: ToolExecutionClient;
  trace: TraceEnvelope;
  executionMode: ExecutionMode;
  stream: AgentEventStream;
  toolName: string;
  args: Record<string, unknown>;
}): Promise<ToolExecutionResponse> {
  getToolDefinition(input.toolDefinitions, input.toolName, input.executionMode);
  const toolCallId = crypto.randomUUID();
  const response = await input.toolExecutionClient.execute({
    trace: { ...input.trace, tool_call_id: toolCallId },
    tool_name: input.toolName,
    input: input.args,
    execution_mode: input.executionMode,
  });
  await input.stream.emitRawEvents(response.raw_events as RawEventFact[] | undefined);
  return response;
}

async function emitCompletedRun(input: {
  store: ConversationStore;
  stream: AgentEventStream;
  trace: TraceEnvelope;
  assistantText: string;
  toolCallCount: number;
  contextPacketId: string | null;
}) {
  await input.stream.emitMessageDelta(input.assistantText);
  const assistantMessage = await input.store.appendMessage({
    conversationId: input.trace.conversation_id,
    role: "assistant",
    content: input.assistantText,
    metadata: {
      agent_run_id: input.trace.agent_run_id,
      request_id: input.trace.request_id,
    },
  });
  await input.stream.emitEvent("message.completed", {
    message_id: assistantMessage.id,
    content_preview: input.assistantText.slice(0, 300),
  });
  await input.stream.emitEvent("run.completed", {
    assistant_message_id: assistantMessage.id,
    tool_call_count: input.toolCallCount,
    context_packet_id: input.contextPacketId,
  });
}

export async function runScriptedMode(input: {
  mode: string;
  stream: AgentEventStream;
  store: ConversationStore;
  trace: TraceEnvelope;
  executionMode: ExecutionMode;
  toolDefinitions: ToolDefinition[];
  toolExecutionClient: ToolExecutionClient;
  contextPacketId: string | null;
}): Promise<ScriptedRunResult> {
  let toolCallCount = 0;
  const execute = async (toolName: string, args: Record<string, unknown>) => {
    toolCallCount += 1;
    return executeTool({
      toolDefinitions: input.toolDefinitions,
      toolExecutionClient: input.toolExecutionClient,
      trace: input.trace,
      executionMode: input.executionMode,
      stream: input.stream,
      toolName,
      args,
    });
  };

  if (input.mode === "scripted_text") {
    return {
      status: "completed",
      toolCallCount,
      assistantText: "Deterministic scripted text response completed without a model provider.",
    };
  }

  if (input.mode === "scripted_read_tools") {
    await execute("list_available_tools", {});
    await execute("list_platform_services", {});
    await execute("list_platform_applications", {});
    await execute("search_documents", { query: "battery efficiency", limit: 2 });
    await execute("search_codebase", { query: "metadata endpoint", branch: "main", limit: 2 });
    await execute("navigate_to_application", { application_id: "ai-engineer", route_path: "/apps/ai-engineer" });
    return {
      status: "completed",
      toolCallCount,
      assistantText: "Deterministic scripted read workflow completed through Tool Execution.",
    };
  }

  if (input.mode === "scripted_write_deploy") {
    await execute("create_working_branch", { branch: FIXTURE_BRANCH, from_branch: "main" });
    await execute("scaffold_service", {
      template_id: "python-service",
      unit_id: FIXTURE_UNIT_ID,
      display_name: "Phase 3 Test Fixture Service",
      branch: FIXTURE_BRANCH,
      package_owner: "space-ops-platform",
      source_path: FIXTURE_SOURCE_ROOT,
      discovery: {
        service_slug: FIXTURE_UNIT_ID,
        category: "platform",
        capabilities: ["phase3-test-fixture"],
        health_endpoint: "/health",
      },
    });
    for (const file of FIXTURE_FILES) {
      await execute("write_source_file", {
        branch: FIXTURE_BRANCH,
        path: file.path,
        content: file.content,
      });
    }
    await execute("create_commit", { branch: FIXTURE_BRANCH, message: "Add deterministic Phase 3 fixture service" });
    await execute("deploy_service_or_application", { unit_id: FIXTURE_UNIT_ID, branch: FIXTURE_BRANCH });
    return {
      status: "completed",
      toolCallCount,
      assistantText: "Deterministic scripted write/deploy workflow completed through the managed fork and deployment path.",
    };
  }

  if (input.mode === "scripted_delete_cleanup") {
    await execute("delete_managed_resources", { mode: "managed_unit", unit_id: FIXTURE_UNIT_ID });
    return {
      status: "completed",
      toolCallCount,
      assistantText: "Deterministic scripted cleanup completed through delete_managed_resources.",
    };
  }

  if (input.mode === "scripted_error") {
    await input.stream.emitEvent("error", {
      error_code: "scripted_runtime_error",
      message: "Deterministic scripted runtime failure.",
      source: "agent-runtime-service",
    });
    await input.stream.emitEvent("run.failed", {
      error_code: "scripted_runtime_error",
      message: "Deterministic scripted runtime failure.",
    });
    return { status: "failed", toolCallCount };
  }

  if (input.mode === "phase3_no_llm") {
    await execute("list_available_tools", {});
    await execute("list_platform_services", {});
    await execute("list_platform_applications", {});
    await execute("search_documents", { query: "battery efficiency", limit: 2 });
    await execute("search_codebase", { query: "metadata endpoint", branch: "main", limit: 2 });
    await execute("navigate_to_application", { application_id: "ai-engineer", route_path: "/apps/ai-engineer" });
    await execute("create_working_branch", { branch: FIXTURE_BRANCH, from_branch: "main" });
    await execute("scaffold_service", {
      template_id: "python-service",
      unit_id: FIXTURE_UNIT_ID,
      display_name: "Phase 3 Test Fixture Service",
      branch: FIXTURE_BRANCH,
      package_owner: "space-ops-platform",
      source_path: FIXTURE_SOURCE_ROOT,
      discovery: {
        service_slug: FIXTURE_UNIT_ID,
        category: "platform",
        capabilities: ["phase3-test-fixture"],
        health_endpoint: "/health",
      },
    });
    for (const file of FIXTURE_FILES) {
      await execute("write_source_file", {
        branch: FIXTURE_BRANCH,
        path: file.path,
        content: file.content,
      });
    }
    await execute("create_commit", { branch: FIXTURE_BRANCH, message: "Add deterministic Phase 3 fixture service" });
    await execute("deploy_service_or_application", { unit_id: FIXTURE_UNIT_ID, branch: FIXTURE_BRANCH });
    await execute("delete_managed_resources", { mode: "managed_unit", unit_id: FIXTURE_UNIT_ID });
    return {
      status: "completed",
      toolCallCount,
      assistantText: "Deterministic Phase 3 no-LLM workflow completed across read, deploy, navigation, and cleanup paths.",
    };
  }

  throw new Error(`unsupported scripted mode: ${input.mode}`);
}

export async function completeScriptedRun(input: {
  store: ConversationStore;
  stream: AgentEventStream;
  trace: TraceEnvelope;
  result: ScriptedRunResult;
  contextPacketId: string | null;
}): Promise<void> {
  if (input.result.status !== "completed") {
    return;
  }
  await emitCompletedRun({
    store: input.store,
    stream: input.stream,
    trace: input.trace,
    assistantText: input.result.assistantText,
    toolCallCount: input.result.toolCallCount,
    contextPacketId: input.contextPacketId,
  });
}
