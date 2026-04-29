import type { ChatInputMessage, ContextPacketResponse, ExecutionMode, RetrievalPlan, ToolDefinition } from "../types.js";

function summarizeHistory(messages: ChatInputMessage[]): string {
  const priorMessages = messages.slice(0, -1);
  if (priorMessages.length === 0) {
    return "No prior conversation history.";
  }

  return priorMessages
    .slice(-8)
    .map((message, index) => `${index + 1}. ${message.role}: ${message.content}`)
    .join("\n");
}

function summarizeTools(tools: ToolDefinition[]): string {
  if (tools.length === 0) {
    return "No tools are exposed for the current execution mode.";
  }

  return tools
    .map((tool) => `${tool.name}: ${tool.description} [${tool.read_write_classification}, ${tool.required_execution_mode}]`)
    .join("\n");
}

export function buildSystemPrompt(input: {
  executionMode: ExecutionMode;
  retrievalPlan: RetrievalPlan;
  context: ContextPacketResponse;
  tools: ToolDefinition[];
  messages: ChatInputMessage[];
}): string {
  return [
    "You are the Agent Runtime behind the AI Engineer application.",
    "You orchestrate context retrieval, model responses, and delegated tool execution.",
    "You must never execute actions directly. Every action must go through the provided tools, which call tool-execution-service.",
    `Execution mode: ${input.executionMode}.`,
    `Retrieval plan: ${input.retrievalPlan.summary}.`,
    "",
    "Conversation history:",
    summarizeHistory(input.messages),
    "",
    "Retrieved context packet:",
    JSON.stringify(input.context.data),
    "",
    "Tools exposed in this execution mode (same list the runtime registers for tool calls — write-classification tools appear only when mode is execute or governed_execute):",
    summarizeTools(input.tools),
  ].join("\n");
}
