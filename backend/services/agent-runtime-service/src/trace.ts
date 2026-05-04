import type { TraceEnvelope } from "./types.js";

export function createTrace(input: {
  conversationId: string;
  requestId?: string | null;
  createId?: () => string;
}): TraceEnvelope {
  const createId = input.createId ?? crypto.randomUUID;

  return {
    conversation_id: input.conversationId,
    agent_run_id: createId(),
    request_id: input.requestId ?? createId(),
    tool_call_id: null,
  };
}

export function withToolTrace(trace: TraceEnvelope, toolCallId: string): TraceEnvelope {
  return {
    ...trace,
    tool_call_id: toolCallId,
  };
}

export function traceHeaders(trace: TraceEnvelope): Record<string, string> {
  return {
    "x-conversation-id": trace.conversation_id,
    "x-agent-run-id": trace.agent_run_id,
    "x-request-id": trace.request_id,
    ...(trace.tool_call_id ? { "x-tool-call-id": trace.tool_call_id } : {}),
  };
}
