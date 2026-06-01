import { LanguageModelUsageSnapshotSchema, type LanguageModelUsageSnapshot } from "./model-usage.js";
import type { ChatInputMessage } from "../types.js";

export function estimateTokensFromText(text: string): number {
  return Math.ceil(text.length / 3.5);
}

export function estimateTokensFromMessages(input: { system: string; messages: ChatInputMessage[] }): number {
  return estimateTokensFromText([input.system, ...input.messages.map((message) => `${message.role}: ${message.content}`)].join("\n\n"));
}

export function estimatedUsageSnapshot(input: {
  inputTokens?: number | null;
  outputTokens?: number | null;
  source: "estimated_preflight" | "estimated_current_step";
  stepIndex?: number | null;
}): LanguageModelUsageSnapshot {
  const inputTokens = input.inputTokens ?? null;
  const outputTokens = input.outputTokens ?? null;
  const totalTokens = inputTokens !== null || outputTokens !== null ? (inputTokens ?? 0) + (outputTokens ?? 0) : null;
  return LanguageModelUsageSnapshotSchema.parse({
    input_tokens: inputTokens,
    output_tokens: outputTokens,
    total_tokens: totalTokens,
    reasoning_tokens: null,
    cached_input_tokens: null,
    raw: null,
    source: input.source,
    step_index: input.stepIndex ?? null,
    synced_after: null,
    is_actual: false,
  });
}
