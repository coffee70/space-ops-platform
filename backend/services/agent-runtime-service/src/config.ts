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
});

export function loadConfig(env: NodeJS.ProcessEnv = process.env): RuntimeConfig {
  const parsed = envSchema.parse(env);

  return {
    port: parsed.PORT,
    databaseUrl: parsed.DATABASE_URL,
    controlPlaneUrl: parsed.CONTROL_PLANE_URL,
    openAiApiKey: parsed.OPENAI_API_KEY ?? null,
    openAiBaseUrl: parsed.OPENAI_BASE_URL ?? null,
    modelId: parsed.AGENT_RUNTIME_MODEL,
    maxSteps: parsed.AGENT_RUNTIME_MAX_STEPS,
    requestTimeoutMs: parsed.AGENT_RUNTIME_REQUEST_TIMEOUT_MS,
  };
}
