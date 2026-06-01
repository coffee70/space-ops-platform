import { z } from "zod";

import type { RuntimeConfig } from "./types.js";

const emptyStringToUndefined = (value: unknown): unknown => {
  if (typeof value === "string" && value.trim().length === 0) {
    return undefined;
  }
  return value;
};

const envSchema = z.object({
  PORT: z.coerce.number().int().positive().default(8080),
  DATABASE_URL: z.string().min(1),
  CONTROL_PLANE_URL: z.string().url().default("http://localhost:8100"),
  OPENAI_API_KEY: z.preprocess(emptyStringToUndefined, z.string().min(1).optional()),
  OPENAI_BASE_URL: z.preprocess(emptyStringToUndefined, z.string().url().optional()),
  AGENT_RUNTIME_MODEL: z.string().min(1).default("gpt-4o-mini"),
  AGENT_RUNTIME_MAX_STEPS: z.coerce.number().int().positive().default(5),
  AGENT_RUNTIME_REQUEST_TIMEOUT_MS: z.coerce.number().int().positive().default(30000),
  AGENT_RUNTIME_SCRIPTED_MODE: z.preprocess(emptyStringToUndefined, z.string().min(1).optional()),
  AGENT_RUNTIME_ALLOW_NO_LLM_FALLBACK: z.coerce.boolean().optional(),
  MODEL_REGISTRY_BASE_URL: z.preprocess(emptyStringToUndefined, z.string().min(1)),
  OPENROUTER_API_KEY: z.preprocess(emptyStringToUndefined, z.string().min(1).optional()),
  OPENROUTER_BASE_URL: z.preprocess(emptyStringToUndefined, z.string().url().optional()),
  AGENT_RUNTIME_MODEL_METADATA_CACHE_TTL_SECONDS: z.preprocess(emptyStringToUndefined, z.coerce.number().int().nonnegative().optional()),
  AGENT_RUNTIME_LOG_STREAM_PARTS: z.coerce.boolean().default(false),
  AGENT_RUNTIME_MODEL_RETRY_MAX_ATTEMPTS: z.coerce.number().int().positive().default(3),
  AGENT_RUNTIME_MODEL_RETRY_BASE_DELAY_MS: z.coerce.number().int().nonnegative().default(1000),
  AGENT_RUNTIME_MODEL_RETRY_MAX_DELAY_MS: z.coerce.number().int().nonnegative().default(30000),
  AGENT_RUNTIME_MODEL_RETRY_JITTER_MS: z.coerce.number().int().nonnegative().default(500),
  AGENT_RUNTIME_MODEL_BUDGET_WARNING_THRESHOLD: z.coerce.number().positive().default(0.7),
  AGENT_RUNTIME_MODEL_BUDGET_DANGER_THRESHOLD: z.coerce.number().positive().default(0.9),
  AGENT_RUNTIME_MODEL_BUDGET_EXHAUSTED_THRESHOLD: z.coerce.number().positive().default(0.98),
  NODE_ENV: z.preprocess(emptyStringToUndefined, z.string().optional()),
});

export function loadConfig(env: NodeJS.ProcessEnv = process.env): RuntimeConfig {
  const parsed = envSchema.parse(env);
  const nodeEnv = parsed.NODE_ENV;

  return {
    port: parsed.PORT,
    databaseUrl: parsed.DATABASE_URL,
    controlPlaneUrl: parsed.CONTROL_PLANE_URL,
    openAiApiKey: parsed.OPENAI_API_KEY ?? null,
    openAiBaseUrl: parsed.OPENAI_BASE_URL ?? null,
    modelId: parsed.AGENT_RUNTIME_MODEL,
    maxSteps: parsed.AGENT_RUNTIME_MAX_STEPS,
    requestTimeoutMs: parsed.AGENT_RUNTIME_REQUEST_TIMEOUT_MS,
    scriptedMode: parsed.AGENT_RUNTIME_SCRIPTED_MODE ?? null,
    allowMissingKeyFallback:
      parsed.AGENT_RUNTIME_ALLOW_NO_LLM_FALLBACK ?? ((parsed.NODE_ENV ?? "development") !== "production"),
    nodeEnv,
    modelRegistryBaseUrl: parsed.MODEL_REGISTRY_BASE_URL,
    openRouterApiKey: parsed.OPENROUTER_API_KEY ?? null,
    openRouterBaseUrl: parsed.OPENROUTER_BASE_URL ?? null,
    modelMetadataCacheTtlSeconds: parsed.AGENT_RUNTIME_MODEL_METADATA_CACHE_TTL_SECONDS ?? null,
    logModelStreamParts: parsed.AGENT_RUNTIME_LOG_STREAM_PARTS,
    modelRetryMaxAttempts: parsed.AGENT_RUNTIME_MODEL_RETRY_MAX_ATTEMPTS,
    modelRetryBaseDelayMs: parsed.AGENT_RUNTIME_MODEL_RETRY_BASE_DELAY_MS,
    modelRetryMaxDelayMs: parsed.AGENT_RUNTIME_MODEL_RETRY_MAX_DELAY_MS,
    modelRetryJitterMs: parsed.AGENT_RUNTIME_MODEL_RETRY_JITTER_MS,
    modelBudgetWarningThreshold: parsed.AGENT_RUNTIME_MODEL_BUDGET_WARNING_THRESHOLD,
    modelBudgetDangerThreshold: parsed.AGENT_RUNTIME_MODEL_BUDGET_DANGER_THRESHOLD,
    modelBudgetExhaustedThreshold: parsed.AGENT_RUNTIME_MODEL_BUDGET_EXHAUSTED_THRESHOLD,
  };
}
