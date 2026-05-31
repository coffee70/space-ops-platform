import type { AgentEventStream } from "./events/stream.js";
import type { ConversationStore, ExecutionMode, RawEventFact, ToolDefinition, ToolExecutionClient, ToolExecutionResponse, ToolPermissionClient, TraceEnvelope } from "./types.js";

const FIXTURE_UNIT_ID = "phase3-test-fixture-service";
const FIXTURE_BRANCH = "feature/phase3-no-llm";
const FIXTURE_SOURCE_ROOT = `project/space-ops-platform/backend/services/${FIXTURE_UNIT_ID}`;

const PREVIEW_FIXTURE_UNIT_ID = "derived-telemetry-service";
const PREVIEW_FIXTURE_APPLICATION_ID = "telemetry";
const PREVIEW_FIXTURE_BRANCH = "preview/derived-telemetry-preview";
const PREVIEW_FIXTURE_SOURCE_ROOT = `project/space-ops-platform/backend/services/${PREVIEW_FIXTURE_UNIT_ID}`;
const PREVIEW_FIXTURE_FILES: Array<{ path: string; content: string }> = [
  {
    path: `${PREVIEW_FIXTURE_SOURCE_ROOT}/app/main.py`,
    content: [
      "from fastapi import FastAPI",
      "",
      'app = FastAPI(title="Derived Telemetry Service (preview)")',
      "",
      '@app.get("/health")',
      "def health():",
      '    return {"status": "ok", "service": "derived-telemetry-service", "variant": "preview"}',
      "",
      '@app.get("/metadata")',
      "def metadata():",
      '    return {"display_name": "Derived Telemetry Service", "mode": "preview"}',
      "",
    ].join("\n"),
  },
];

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

type ValidationStatus = "not_run" | "not_ready" | "running" | "passed" | "failed";

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
  abortSignal?: AbortSignal;
  stream: AgentEventStream;
  toolName: string;
  args: Record<string, unknown>;
  toolPermissionClient?: ToolPermissionClient;
  assistantMessageId: string;
}): Promise<ToolExecutionResponse> {
  getToolDefinition(input.toolDefinitions, input.toolName, input.executionMode);
  const toolCallId = crypto.randomUUID();
  const response = await input.toolExecutionClient.execute({
    trace: { ...input.trace, tool_call_id: toolCallId },
    assistant_message_id: input.assistantMessageId,
    tool_name: input.toolName,
    input: input.args,
    execution_mode: input.executionMode,
  });
  await input.stream.emitRawEvents(response.raw_events as RawEventFact[] | undefined);
  if (response.status !== "permission_required") {
    return response;
  }

  const output = response.output && typeof response.output === "object" ? (response.output as Record<string, unknown>) : {};
  const permissionRequestId = typeof output.permission_request_id === "string" ? output.permission_request_id : null;
  if (!permissionRequestId || !input.toolPermissionClient) {
    throw new Error(`scripted tool ${input.toolName} requires permission but no approval path is available`);
  }
  const decision = await input.toolPermissionClient.waitForDecision({
    permissionRequestId,
    abortSignal: input.abortSignal,
  });
  await input.stream.emitRawEvents(decision.raw_events);
  if (decision.status === "denied") {
    return {
      ...response,
      status: "permission_denied",
      output: {
        status: "permission_denied",
        message: "The user denied this tool call. No action was taken.",
        permission_request_id: permissionRequestId,
        tool_name: input.toolName,
        reason: decision.reason ?? "user_denied",
      },
    };
  }

  const approvedResponse = await input.toolExecutionClient.execute({
    trace: { ...input.trace, tool_call_id: toolCallId },
    assistant_message_id: input.assistantMessageId,
    tool_name: input.toolName,
    input: input.args,
    execution_mode: input.executionMode,
    permission_request_id: permissionRequestId,
  });
  await input.stream.emitRawEvents(approvedResponse.raw_events as RawEventFact[] | undefined);
  return approvedResponse;
}

function extractBaseCommitSha(output: unknown): string | undefined {
  if (typeof output !== "object" || output === null) return undefined;
  const envelope = output as Record<string, unknown>;
  const data = envelope.data;
  if (typeof data !== "object" || data === null) return undefined;
  const value = (data as Record<string, unknown>).base_commit_sha;
  return typeof value === "string" && value.length > 0 ? value : undefined;
}

function extractCommitSha(output: unknown): string | undefined {
  if (typeof output !== "object" || output === null) return undefined;
  const value = (output as Record<string, unknown>).commit_sha;
  return typeof value === "string" && value.length > 0 ? value : undefined;
}

function extractDeploymentId(output: unknown): string | undefined {
  if (typeof output !== "object" || output === null) return undefined;
  const value = (output as Record<string, unknown>).deployment_id;
  return typeof value === "string" && value.length > 0 ? value : undefined;
}

async function emitChangeSummary(input: {
  stream: AgentEventStream;
  branch: string;
  baseBranch?: string;
  baseCommitSha?: string;
  commitSha?: string;
  changedFiles: string[];
  targetUnitId?: string;
  targetApplicationId?: string;
  affectedCapability: string;
  riskLevel?: "low" | "medium" | "high";
  validationStatus?: ValidationStatus;
}): Promise<void> {
  await input.stream.emitEvent("change.summary", {
    branch: input.branch,
    base_branch: input.baseBranch ?? "main",
    base_commit_sha: input.baseCommitSha ?? null,
    commit_sha: input.commitSha ?? null,
    changed_files: input.changedFiles,
    target_unit_id: input.targetUnitId ?? null,
    target_application_id: input.targetApplicationId ?? null,
    affected_capability: input.affectedCapability,
    risk_level: input.riskLevel ?? "low",
    validation_status: input.validationStatus ?? "not_run",
  });
}

async function emitCompletedRun(input: {
  store: ConversationStore;
  stream: AgentEventStream;
  trace: TraceEnvelope;
  assistantMessageId: string;
  assistantText: string;
  toolCallCount: number;
  contextPacketId: string | null;
}) {
  await input.stream.emitMessageDelta(input.assistantText);
  const assistantMessage = await input.store.updateMessage(input.assistantMessageId, {
    content: input.assistantText,
    requestId: input.trace.request_id,
    agentRunId: input.trace.agent_run_id,
    metadata: {
      agent_run_id: input.trace.agent_run_id,
      request_id: input.trace.request_id,
    },
  });
  if (!assistantMessage) {
    throw new Error("assistant message not found");
  }
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
  abortSignal?: AbortSignal;
  toolDefinitions: ToolDefinition[];
  toolExecutionClient: ToolExecutionClient;
  toolPermissionClient?: ToolPermissionClient;
  assistantMessageId: string;
  contextPacketId: string | null;
}): Promise<ScriptedRunResult> {
  let toolCallCount = 0;
  const execute = async (toolName: string, args: Record<string, unknown>) => {
    toolCallCount += 1;
    return executeTool({
      toolDefinitions: input.toolDefinitions,
      toolExecutionClient: input.toolExecutionClient,
      trace: input.trace,
      assistantMessageId: input.assistantMessageId,
      executionMode: input.executionMode,
      stream: input.stream,
      toolName,
      args,
      abortSignal: input.abortSignal,
      toolPermissionClient: input.toolPermissionClient,
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
    const deploymentResponse = await execute("deploy_service_or_application", { unit_id: FIXTURE_UNIT_ID, branch: FIXTURE_BRANCH });
    const deploymentId = extractDeploymentId(deploymentResponse.output);
    let validationStatus: ValidationStatus = "not_run";
    if (deploymentId) {
      const validationResponse = await execute("run_deployment_validation", { deployment_id: deploymentId });
      const rawValidationStatus =
        typeof validationResponse.output === "object" && validationResponse.output !== null
          ? (validationResponse.output as Record<string, unknown>).validation_status
          : null;
      validationStatus =
        rawValidationStatus === "passed" || rawValidationStatus === "failed" || rawValidationStatus === "not_ready"
          ? rawValidationStatus
          : "not_run";
    }
    await emitChangeSummary({
      stream: input.stream,
      branch: FIXTURE_BRANCH,
      changedFiles: FIXTURE_FILES.map((file) => file.path),
      targetUnitId: FIXTURE_UNIT_ID,
      affectedCapability: "phase3-test-fixture",
      validationStatus,
    });
    return {
      status: "completed",
      toolCallCount,
      assistantText:
        validationStatus === "passed"
          ? "Deterministic scripted write/deploy workflow deployed and passed post-deploy validation."
          : "Deterministic scripted write/deploy workflow deployed, but post-deploy validation did not pass.",
    };
  }

  if (input.mode === "scripted_change_preview") {
    const branchResponse = await execute("create_working_branch", {
      branch: PREVIEW_FIXTURE_BRANCH,
      from_branch: "main",
    });
    for (const file of PREVIEW_FIXTURE_FILES) {
      await execute("write_source_file", {
        branch: PREVIEW_FIXTURE_BRANCH,
        path: file.path,
        content: file.content,
      });
    }
    const commitResponse = await execute("create_commit", {
      branch: PREVIEW_FIXTURE_BRANCH,
      message: "Preview: tag derived telemetry metadata as preview variant",
    });
    // Capture the exact baseline commit SHA from create_working_branch and the
    // resulting commit SHA from create_commit so the change summary carries
    // accurate Git metadata even on the scripted product path. The aggregator
    // path covers real flows; scripted mirrors the structure for fixtures.
    const baseCommitSha = extractBaseCommitSha(branchResponse.output);
    const commitSha = extractCommitSha(commitResponse.output);
    await emitChangeSummary({
      stream: input.stream,
      branch: PREVIEW_FIXTURE_BRANCH,
      baseBranch: "main",
      baseCommitSha,
      commitSha,
      changedFiles: PREVIEW_FIXTURE_FILES.map((file) => file.path),
      targetUnitId: PREVIEW_FIXTURE_UNIT_ID,
      targetApplicationId: PREVIEW_FIXTURE_APPLICATION_ID,
      affectedCapability: "telemetry-detail",
    });
    return {
      status: "completed",
      toolCallCount,
      assistantText:
        "I created a preview branch and scoped change. Ready to deploy when you are.",
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
    await emitChangeSummary({
      stream: input.stream,
      branch: FIXTURE_BRANCH,
      changedFiles: FIXTURE_FILES.map((file) => file.path),
      targetUnitId: FIXTURE_UNIT_ID,
      affectedCapability: "phase3-test-fixture",
    });
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
  assistantMessageId: string;
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
    assistantMessageId: input.assistantMessageId,
    assistantText: input.result.assistantText,
    toolCallCount: input.result.toolCallCount,
    contextPacketId: input.contextPacketId,
  });
}
