import type { ConversationMessageRecord } from "./types.js";
import type { AgentEventStream } from "./events/stream.js";

export async function runFallback(input: {
  stream: AgentEventStream;
  userMessage: string;
  executionMode: string;
  persistAssistantMessage: (content: string) => Promise<ConversationMessageRecord>;
}): Promise<ConversationMessageRecord> {
  const text = [
    "Model API key is not configured in this environment.",
    `Execution mode: ${input.executionMode}.`,
    "",
    "The request completed through the runtime fallback path.",
    `User message preview: ${input.userMessage.slice(0, 300)}`,
  ].join("\n");

  await input.stream.emitMessageDelta(text);
  const assistantMessage = await input.persistAssistantMessage(text);
  await input.stream.emitEvent("message.completed", {
    message_id: assistantMessage.id,
    role: "assistant",
  });
  await input.stream.emitEvent("run.completed", {
    completion_mode: "fallback",
  });
  return assistantMessage;
}
