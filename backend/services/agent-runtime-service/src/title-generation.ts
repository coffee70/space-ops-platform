import type { ChatInputMessage, ConversationDetail, RunDependencies } from "./types.js";

const TITLE_PROMPT = `Create a concise title for this AI Engineer chat.
Use 3 to 7 words.
Do not use quotes.
Do not end with punctuation.
Focus on the user's task, not generic words like "Chat".`;

function cleanTitle(value: string): string {
  return value
    .replace(/^["'`]+|["'`.!?;:]+$/g, "")
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, 80);
}

export function fallbackTitleFromConversation(conversation: ConversationDetail): string {
  const firstUser = conversation.messages.find((message) => message.role === "user")?.content ?? "Untitled chat";
  const words = firstUser
    .replace(/[^\w\s-]/g, " ")
    .split(/\s+/)
    .filter((word) => word.length > 0)
    .slice(0, 7);
  const title = cleanTitle(words.join(" "));
  return title.length > 0 ? title : "Untitled chat";
}

async function resolveTitleModelId(dependencies: RunDependencies): Promise<string | null> {
  if (dependencies.config.titleGenerationModelId) {
    return dependencies.config.titleGenerationModelId;
  }
  const catalog = await dependencies.modelCatalog.listModelsResponse();
  return catalog.models.find((model) => model.enabled && model.isAvailable)?.id ?? catalog.models[0]?.id ?? null;
}

export async function maybeGenerateConversationTitle(input: {
  dependencies: RunDependencies;
  conversationId: string;
}): Promise<void> {
  const conversation = await input.dependencies.store.getConversation(input.conversationId);
  if (!conversation || conversation.title_source === "manual" || conversation.title_source === "generated") {
    return;
  }
  const hasUser = conversation.messages.some((message) => message.role === "user");
  const hasAssistant = conversation.messages.some((message) => message.role === "assistant");
  if (!hasUser || !hasAssistant) {
    return;
  }

  let title = fallbackTitleFromConversation(conversation);
  let titleModelId: string | null = null;
  try {
    titleModelId = await resolveTitleModelId(input.dependencies);
    if (titleModelId) {
      const selection = await input.dependencies.modelCatalog.resolveForChat(titleModelId, "read_only");
      let generated = "";
      const messages = conversation.messages
        .filter((message) => message.role === "user" || message.role === "assistant")
        .slice(0, 6)
        .map((message) => ({ role: message.role, content: message.content })) as ChatInputMessage[];
      for await (const part of input.dependencies.modelRunner.stream({
        system: TITLE_PROMPT,
        messages,
        tools: {},
        maxSteps: 1,
        model: selection.runtime,
      })) {
        if (part.type === "text-delta") {
          const fields = part as Record<string, unknown>;
          generated +=
            typeof fields.text === "string"
              ? fields.text
              : typeof fields.delta === "string"
                ? fields.delta
                : typeof fields.textDelta === "string"
                  ? fields.textDelta
                  : "";
        }
      }
      title = cleanTitle(generated) || title;
      titleModelId = selection.option.id;
    }
  } catch (error) {
    console.warn("[agent-runtime] falling back to deterministic conversation title", error);
  }

  const latest = await input.dependencies.store.getConversation(input.conversationId);
  if (!latest || latest.title_source === "manual") {
    return;
  }
  await input.dependencies.store.updateConversation(input.conversationId, {
    title,
    title_source: "generated",
    title_model_id: titleModelId,
  });
}
