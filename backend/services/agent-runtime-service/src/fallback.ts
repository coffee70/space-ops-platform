import type { ConversationMessageRecord } from "./types.js";
import type { AgentEventStream } from "./events/stream.js";

export async function runFallback(input: {
  stream: AgentEventStream;
  userMessage: string;
  executionMode: string;
  contextPacketId?: string | null;
  persistAssistantMessage: (content: string) => Promise<ConversationMessageRecord>;
}): Promise<ConversationMessageRecord> {
  const text = [
    "Deterministic no-LLM runtime mode is active in this local/test environment.",
    `Execution mode: ${input.executionMode}.`,
    "",
    "The request completed through the runtime-owned deterministic fallback path.",
    `User message preview: ${input.userMessage.slice(0, 300)}`,
  ].join("\n");

  await input.stream.emitMessageDelta(text);
  const assistantMessage = await input.persistAssistantMessage(text);
  await input.stream.emitEvent("message.completed", {
    message_id: assistantMessage.id,
    content_preview: text.slice(0, 300),
  });
  await input.stream.emitEvent("run.completed", {
    assistant_message_id: assistantMessage.id,
    tool_call_count: 0,
    context_packet_id: input.contextPacketId ?? null,
  });
  return assistantMessage;
}
