import { z } from "zod";

export const AGENT_EVENT_REQUIRED_PAYLOAD_FIELDS = {
  "run.started": ["execution_mode", "message_id", "user_message_preview"],
  "run.completed": ["assistant_message_id", "tool_call_count"],
  "run.failed": ["error_code", "message"],
  "run.cancelled": ["reason", "assistant_text_length", "reasoning_text_length", "tool_call_count"],
  "context.requested": ["retrieval_plan", "limits"],
  "context.resolved": ["context_packet_id", "document_chunk_count", "code_chunk_count", "platform_metadata_bytes", "tool_definition_count", "truncated"],
  "context.failed": ["error_code", "message"],
  "tool.started": ["tool_name", "category", "read_write_classification", "input_preview"],
  "tool.completed": ["tool_name", "status", "result_preview", "duration_ms"],
  "tool.failed": ["tool_name", "error_code", "message", "duration_ms"],
  "tool.permission_required": [
    "tool_name",
    "tool_call_id",
    "permission_request_id",
    "execution_mode",
    "prompt",
  ],
  "tool.permission_approved": ["tool_name", "tool_call_id", "permission_request_id"],
  "tool.permission_denied": ["tool_name", "tool_call_id", "permission_request_id", "reason"],
  "document.uploaded": ["document_id", "title", "document_type", "content_hash"],
  "document.ingestion_started": ["document_id", "chunking_strategy", "embedding_model"],
  "document.ingestion_completed": ["document_id", "chunk_count", "embedding_model", "duration_ms"],
  "document.ingestion_failed": ["document_id", "error_code", "message"],
  "code.index_started": ["repository", "branch", "commit_sha"],
  "code.index_completed": ["repository", "branch", "commit_sha", "file_count", "chunk_count", "duration_ms"],
  "code.index_failed": ["repository", "branch", "error_code", "message"],
  "navigation.requested": ["action", "application_id", "route_path"],
  "deployment.requested": ["tool_name", "branch", "unit_id"],
  "deployment.submitted": ["deployment_id", "branch", "unit_id", "status"],
  "deployment.build_started": ["deployment_id", "branch", "unit_id", "status"],
  "deployment.health_passed": ["deployment_id", "branch", "unit_id", "status"],
  "deployment.failed": ["deployment_id", "branch", "unit_id", "status", "failure_reason"],
  "deployment.timeout": ["deployment_id", "branch", "unit_id", "status", "message"],
  "preview.active": ["deployment_id", "branch", "unit_id", "status"],
  "change.summary": [
    "branch",
    "base_branch",
    "changed_files",
    "target_unit_id",
    "affected_capability",
    "risk_level",
    "validation_status",
  ],
  "message.reasoning.started": ["provider_type", "provider_model_id", "representation", "source"],
  "message.reasoning.delta": ["text_delta"],
  "message.reasoning.completed": ["text_length", "representation"],
  "message.delta": ["text_delta"],
  "message.completed": ["message_id", "content_preview"],
  "model.selected": [
    "model_id",
    "provider_type",
    "provider_model_id",
    "model_name",
    "provider",
    "data_boundary",
    "capabilities",
  ],
  error: ["error_code", "message", "source"],
} as const;

export type AgentEventType = keyof typeof AGENT_EVENT_REQUIRED_PAYLOAD_FIELDS;

const agentEventTypeSchema = z.enum(Object.keys(AGENT_EVENT_REQUIRED_PAYLOAD_FIELDS) as [AgentEventType, ...AgentEventType[]]);

const TOOL_EVENT_PREFIX = "tool.";
const SENSITIVE_KEYS = new Set(["authorization", "api_key", "token", "password", "cookie", "set-cookie", "secret"]);
const STRING_LIMIT = 2000;
const ARRAY_LIMIT = 20;
const OBJECT_KEYS_LIMIT = 50;

export function isAgentEventType(eventType: string): eventType is AgentEventType {
  return eventType in AGENT_EVENT_REQUIRED_PAYLOAD_FIELDS;
}

export function redactAndTruncate(value: unknown): unknown {
  if (Array.isArray(value)) {
    const mapped = value.slice(0, ARRAY_LIMIT).map((item) => redactAndTruncate(item));
    return value.length > ARRAY_LIMIT ? [...mapped, `...<${value.length - ARRAY_LIMIT} items truncated>`] : mapped;
  }

  if (value && typeof value === "object") {
    const entries = Object.entries(value as Record<string, unknown>);
    const output: Record<string, unknown> = {};
    for (const [key, item] of entries.slice(0, OBJECT_KEYS_LIMIT)) {
      output[key] = SENSITIVE_KEYS.has(key.toLowerCase()) ? "***REDACTED***" : redactAndTruncate(item);
    }
    if (entries.length > OBJECT_KEYS_LIMIT) {
      output.__truncated_keys = entries.length - OBJECT_KEYS_LIMIT;
    }
    return output;
  }

  if (typeof value === "string" && value.length > STRING_LIMIT) {
    return `${value.slice(0, STRING_LIMIT)}...<truncated>`;
  }

  return value;
}

export function validateAgentEventPayload(eventType: string, payload: Record<string, unknown>, toolCallId?: string | null): AgentEventType {
  const parsedEventType = agentEventTypeSchema.parse(eventType);
  const missingFields = AGENT_EVENT_REQUIRED_PAYLOAD_FIELDS[parsedEventType].filter((field) => !(field in payload));
  if (missingFields.length > 0) {
    throw new Error(`event ${eventType} missing required payload field(s): ${missingFields.join(", ")}`);
  }
  if (parsedEventType.startsWith(TOOL_EVENT_PREFIX) && !toolCallId) {
    throw new Error(`event ${eventType} requires tool_call_id`);
  }
  return parsedEventType;
}
