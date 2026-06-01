import { z } from "zod";

import { LanguageModelUsageSnapshotSchema, type LanguageModelUsageSnapshot } from "./model-usage.js";
import type { RuntimeBudgetConfig, RuntimeConfig } from "../types.js";
import type { ModelUsageStore } from "../storage/model-usage-store.js";

export const ModelBudgetStatusSchema = z.enum(["normal", "watch", "danger", "exhausted", "throttled", "unknown"]);
export const ModelBudgetSourceSchema = z.enum(["estimated", "provider_headers", "provider_error", "configured", "mixed"]);

export const ModelBudgetSnapshotPayloadSchema = z
  .object({
    provider_type: z.string().min(1),
    provider_model_id: z.string().min(1),
    model_id: z.string().min(1).nullable().optional(),
    source: ModelBudgetSourceSchema,
    measured_at: z.string().datetime(),
    usage: LanguageModelUsageSnapshotSchema.nullable().optional(),
    context: z.object({
      limit_tokens: z.number().int().positive().nullable(),
      used_tokens: z.number().int().nonnegative().nullable(),
      remaining_tokens: z.number().int().nonnegative().nullable(),
      percent_used: z.number().nonnegative().nullable(),
      status: ModelBudgetStatusSchema.exclude(["throttled"]),
      measurement_source: z.enum(["provider_usage", "provider_count_tokens", "estimated", "configured", "unknown"]),
    }),
    throughput: z.object({
      window_seconds: z.number().int().positive().nullable(),
      limit_tokens: z.number().int().positive().nullable(),
      used_tokens: z.number().int().nonnegative().nullable(),
      remaining_tokens: z.number().int().nonnegative().nullable(),
      percent_used: z.number().nonnegative().nullable(),
      reset_at: z.string().datetime().nullable(),
      seconds_until_reset: z.number().nonnegative().nullable(),
      status: ModelBudgetStatusSchema,
      measurement_source: z.enum(["provider_usage", "provider_headers", "provider_error", "configured_rolling_window", "estimated", "mixed", "unknown"]),
    }),
  })
  .strict();

export const ModelBudgetWarningPayloadSchema = z
  .object({
    kind: z.enum(["context", "throughput"]),
    status: z.enum(["watch", "danger", "exhausted", "throttled"]),
    message: z.string().min(1),
    percent_used: z.number().nonnegative().nullable(),
    remaining_tokens: z.number().int().nonnegative().nullable(),
    reset_at: z.string().datetime().nullable().optional(),
    provider_type: z.string().min(1),
    provider_model_id: z.string().min(1),
  })
  .strict();

export type ModelBudgetSnapshotPayload = z.infer<typeof ModelBudgetSnapshotPayloadSchema>;
export type ModelBudgetWarningPayload = z.infer<typeof ModelBudgetWarningPayloadSchema>;
export type ModelBudgetStatus = z.infer<typeof ModelBudgetStatusSchema>;

function percent(used: number | null, limit: number | null): number | null {
  if (used === null || !limit) return null;
  return Math.min(999, used / limit);
}

function remaining(used: number | null, limit: number | null): number | null {
  if (used === null || !limit) return null;
  return Math.max(0, limit - used);
}

function statusFor(value: number | null, config: RuntimeConfig): ModelBudgetStatus {
  if (value === null) return "unknown";
  if (value >= config.modelBudgetExhaustedThreshold) return "exhausted";
  if (value >= config.modelBudgetDangerThreshold) return "danger";
  if (value >= config.modelBudgetWarningThreshold) return "watch";
  return "normal";
}

function recordFromUsage(input: {
  conversationId?: string | null;
  agentRunId: string;
  requestId: string;
  providerType: string;
  providerModelId: string;
  modelId?: string | null;
  usage: LanguageModelUsageSnapshot;
  stepIndex?: number | null;
  stepType?: string | null;
}) {
  return {
    conversation_id: input.conversationId ?? null,
    agent_run_id: input.agentRunId,
    request_id: input.requestId,
    step_index: input.stepIndex ?? input.usage.step_index ?? null,
    step_type: input.stepType ?? null,
    provider_type: input.providerType,
    provider_model_id: input.providerModelId,
    model_id: input.modelId ?? null,
    input_tokens: input.usage.input_tokens,
    output_tokens: input.usage.output_tokens,
    total_tokens: input.usage.total_tokens,
    reasoning_tokens: input.usage.reasoning_tokens,
    cached_input_tokens: input.usage.cached_input_tokens,
    usage_source: input.usage.source,
    is_actual: input.usage.is_actual,
    raw_usage_json: input.usage.raw ?? null,
    synced_after: input.usage.synced_after ?? null,
  };
}

export class ModelBudgetTracker {
  constructor(
    private readonly usageStore: ModelUsageStore,
    private readonly config: RuntimeConfig,
  ) {}

  async recordEstimatedRequest(input: {
    conversationId?: string | null;
    agentRunId: string;
    providerType: string;
    providerModelId: string;
    modelId?: string | null;
    budget: RuntimeBudgetConfig | null | undefined;
    usage: LanguageModelUsageSnapshot;
    requestId: string;
    nowMs: number;
  }): Promise<ModelBudgetSnapshotPayload> {
    return this.snapshot({ ...input, usage: input.usage, source: "estimated" });
  }

  async recordActualUsage(input: {
    conversationId?: string | null;
    agentRunId: string;
    providerType: string;
    providerModelId: string;
    modelId?: string | null;
    budget: RuntimeBudgetConfig | null | undefined;
    usage: LanguageModelUsageSnapshot;
    requestId: string;
    stepType?: string | null;
    stepIndex?: number | null;
    nowMs: number;
  }): Promise<ModelBudgetSnapshotPayload> {
    const record = recordFromUsage(input);
    if (input.usage.source === "ai_sdk_total_usage") {
      await this.usageStore.upsertRunTotalUsage(record);
    } else if (input.usage.source === "ai_sdk_step_usage" || input.usage.source === "provider_gateway") {
      await this.usageStore.insertStepUsage(record);
    }
    return this.snapshot({ ...input, usage: input.usage, source: "mixed" });
  }

  async recordEstimatedCurrentStep(input: {
    conversationId?: string | null;
    agentRunId: string;
    providerType: string;
    providerModelId: string;
    modelId?: string | null;
    budget: RuntimeBudgetConfig | null | undefined;
    usage: LanguageModelUsageSnapshot;
    requestId: string;
    stepIndex?: number | null;
    nowMs: number;
  }): Promise<ModelBudgetSnapshotPayload> {
    return this.snapshot({ ...input, source: "estimated" });
  }

  async markRateLimited(input: {
    providerType: string;
    providerModelId: string;
    modelId?: string | null;
    budget: RuntimeBudgetConfig | null | undefined;
    retryAfterMs: number | null;
    nowMs: number;
  }): Promise<ModelBudgetSnapshotPayload> {
    const now = new Date(input.nowMs);
    const resetAt = input.retryAfterMs === null ? null : new Date(input.nowMs + input.retryAfterMs).toISOString();
    const windowSeconds = input.budget?.rollingWindowSeconds ?? 60;
    const limit = input.budget?.tokensPerMinute ?? null;
    return ModelBudgetSnapshotPayloadSchema.parse({
      provider_type: input.providerType,
      provider_model_id: input.providerModelId,
      model_id: input.modelId ?? null,
      source: "provider_error",
      measured_at: now.toISOString(),
      usage: null,
      context: {
        limit_tokens: input.budget?.contextWindowTokens ?? null,
        used_tokens: null,
        remaining_tokens: null,
        percent_used: null,
        status: "unknown",
        measurement_source: input.budget?.contextWindowTokens ? "configured" : "unknown",
      },
      throughput: {
        window_seconds: windowSeconds,
        limit_tokens: limit,
        used_tokens: limit,
        remaining_tokens: 0,
        percent_used: limit ? 1 : null,
        reset_at: resetAt,
        seconds_until_reset: input.retryAfterMs === null ? null : input.retryAfterMs / 1000,
        status: "throttled",
        measurement_source: "provider_error",
      },
    });
  }

  async snapshot(input: {
    conversationId?: string | null;
    agentRunId: string;
    providerType: string;
    providerModelId: string;
    modelId?: string | null;
    budget: RuntimeBudgetConfig | null | undefined;
    usage: LanguageModelUsageSnapshot | null;
    requestId?: string;
    nowMs: number;
    source: z.infer<typeof ModelBudgetSourceSchema>;
  }): Promise<ModelBudgetSnapshotPayload> {
    const now = new Date(input.nowMs);
    const budget = input.budget ?? null;
    const contextLimit = budget?.contextWindowTokens ?? null;
    const windowSeconds = budget?.rollingWindowSeconds ?? 60;
    const throughputLimit = budget?.tokensPerMinute ?? null;
    const actual = input.agentRunId ? await this.usageStore.sumActualUsageForRun({ agentRunId: input.agentRunId }) : null;
    const currentEstimate = input.usage?.source === "estimated_current_step" ? input.usage.total_tokens ?? 0 : 0;
    const contextUsed = input.usage?.source === "estimated_preflight" ? input.usage.total_tokens : actual?.total_tokens !== null ? (actual?.total_tokens ?? 0) + currentEstimate : input.usage?.total_tokens ?? null;
    const rolling = throughputLimit
      ? await this.usageStore.getRollingThroughputUsage({
          providerType: input.providerType,
          providerModelId: input.providerModelId,
          windowSeconds,
          now,
        })
      : null;
    const throughputUsed = throughputLimit ? (rolling?.totalTokens ?? 0) + currentEstimate : null;
    const throughputResetAt = throughputLimit && rolling?.oldestSampleAt ? new Date(rolling.oldestSampleAt.getTime() + windowSeconds * 1000) : null;
    const secondsUntilReset = throughputResetAt ? Math.max(0, (throughputResetAt.getTime() - now.getTime()) / 1000) : null;
    const contextPercent = percent(contextUsed, contextLimit);
    const throughputPercent = percent(throughputUsed, throughputLimit);

    return ModelBudgetSnapshotPayloadSchema.parse({
      provider_type: input.providerType,
      provider_model_id: input.providerModelId,
      model_id: input.modelId ?? null,
      source: input.source,
      measured_at: now.toISOString(),
      usage: input.usage,
      context: {
        limit_tokens: contextLimit,
        used_tokens: contextUsed,
        remaining_tokens: remaining(contextUsed, contextLimit),
        percent_used: contextPercent,
        status: statusFor(contextPercent, this.config),
        measurement_source: input.usage?.is_actual ? "provider_usage" : input.usage ? "estimated" : contextLimit ? "configured" : "unknown",
      },
      throughput: {
        window_seconds: throughputLimit ? windowSeconds : null,
        limit_tokens: throughputLimit,
        used_tokens: throughputUsed,
        remaining_tokens: remaining(throughputUsed, throughputLimit),
        percent_used: throughputPercent,
        reset_at: throughputResetAt?.toISOString() ?? null,
        seconds_until_reset: secondsUntilReset,
        status: statusFor(throughputPercent, this.config),
        measurement_source: throughputLimit ? (input.usage?.is_actual ? "provider_usage" : "configured_rolling_window") : "unknown",
      },
    });
  }
}

export function warningFromSnapshot(snapshot: ModelBudgetSnapshotPayload): ModelBudgetWarningPayload | null {
  const candidates = [
    { kind: "context" as const, value: snapshot.context },
    { kind: "throughput" as const, value: snapshot.throughput },
  ];
  const candidate = candidates.find(({ value }) => ["watch", "danger", "exhausted", "throttled"].includes(value.status));
  if (!candidate) return null;
  return ModelBudgetWarningPayloadSchema.parse({
    kind: candidate.kind,
    status: candidate.value.status,
    message:
      candidate.kind === "context"
        ? "Context budget is approaching the selected model limit."
        : candidate.value.status === "throttled"
          ? "Provider throughput is throttled."
          : "Throughput is approaching the configured provider limit.",
    percent_used: candidate.value.percent_used,
    remaining_tokens: candidate.value.remaining_tokens,
    reset_at: "reset_at" in candidate.value ? candidate.value.reset_at : null,
    provider_type: snapshot.provider_type,
    provider_model_id: snapshot.provider_model_id,
  });
}
