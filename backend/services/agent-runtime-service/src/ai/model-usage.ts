import { z } from "zod";

export const AiSdkLanguageModelUsageSchema = z
  .object({
    inputTokens: z.number().int().nonnegative().optional(),
    outputTokens: z.number().int().nonnegative().optional(),
    totalTokens: z.number().int().nonnegative().optional(),
    reasoningTokens: z.number().int().nonnegative().optional(),
    cachedInputTokens: z.number().int().nonnegative().optional(),
    promptTokens: z.number().int().nonnegative().optional(),
    completionTokens: z.number().int().nonnegative().optional(),
    raw: z.unknown().optional(),
  })
  .passthrough();

export const LanguageModelUsageSnapshotSchema = z
  .object({
    input_tokens: z.number().int().nonnegative().nullable(),
    output_tokens: z.number().int().nonnegative().nullable(),
    total_tokens: z.number().int().nonnegative().nullable(),
    reasoning_tokens: z.number().int().nonnegative().nullable(),
    cached_input_tokens: z.number().int().nonnegative().nullable(),
    raw: z.unknown().nullable().optional(),
    source: z.enum([
      "ai_sdk_step_usage",
      "ai_sdk_total_usage",
      "provider_gateway",
      "provider_count_tokens",
      "estimated_current_step",
      "estimated_preflight",
    ]),
    step_index: z.number().int().nonnegative().nullable().optional(),
    synced_after: z.string().nullable().optional(),
    is_actual: z.boolean(),
  })
  .strict();

export type AiSdkLanguageModelUsage = z.infer<typeof AiSdkLanguageModelUsageSchema>;
export type LanguageModelUsageSnapshot = z.infer<typeof LanguageModelUsageSnapshotSchema>;

export function normalizeAiSdkUsage(
  usage: unknown,
  source: LanguageModelUsageSnapshot["source"],
  options?: { stepIndex?: number | null; syncedAfter?: string | null },
): LanguageModelUsageSnapshot {
  const parsed = AiSdkLanguageModelUsageSchema.safeParse(usage);
  if (!parsed.success) {
    return LanguageModelUsageSnapshotSchema.parse({
      input_tokens: null,
      output_tokens: null,
      total_tokens: null,
      reasoning_tokens: null,
      cached_input_tokens: null,
      raw: null,
      source,
      step_index: options?.stepIndex ?? null,
      synced_after: options?.syncedAfter ?? null,
      is_actual: false,
    });
  }

  const inputTokens = parsed.data.inputTokens ?? parsed.data.promptTokens ?? null;
  const outputTokens = parsed.data.outputTokens ?? parsed.data.completionTokens ?? null;
  const totalTokens = parsed.data.totalTokens ?? (inputTokens !== null || outputTokens !== null ? (inputTokens ?? 0) + (outputTokens ?? 0) : null);

  return LanguageModelUsageSnapshotSchema.parse({
    input_tokens: inputTokens,
    output_tokens: outputTokens,
    total_tokens: totalTokens,
    reasoning_tokens: parsed.data.reasoningTokens ?? null,
    cached_input_tokens: parsed.data.cachedInputTokens ?? null,
    raw: parsed.data.raw ?? null,
    source,
    step_index: options?.stepIndex ?? null,
    synced_after: options?.syncedAfter ?? null,
    is_actual: source === "ai_sdk_step_usage" || source === "ai_sdk_total_usage" || source === "provider_gateway",
  });
}

export function emptyUsageSnapshot(source: LanguageModelUsageSnapshot["source"]): LanguageModelUsageSnapshot {
  return LanguageModelUsageSnapshotSchema.parse({
    input_tokens: null,
    output_tokens: null,
    total_tokens: null,
    reasoning_tokens: null,
    cached_input_tokens: null,
    raw: null,
    source,
    step_index: null,
    synced_after: null,
    is_actual: false,
  });
}
