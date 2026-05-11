import path from "node:path";
import { fileURLToPath } from "node:url";
import { z } from "zod";

import type { RuntimeConfig } from "./types.js";

const emptyStringToUndefined = (value: unknown): unknown => {
  if (typeof value === "string" && value.trim().length === 0) return undefined;
  return value;
};

function defaultModelsConfigPath(): string {
  const here = path.dirname(fileURLToPath(import.meta.url));
  // `src/config.ts` -> `../config/models.local.yaml`
  return path.join(here, "..", "config", "models.local.yaml");
}

const envSchema = z.object({
  PORT: z.coerce.number().int().positive().default(8080),
  MODEL_CONFIG_PATH: z.preprocess(emptyStringToUndefined, z.string().min(1).optional()),

  OPENAI_API_KEY: z.preprocess(emptyStringToUndefined, z.string().min(1).optional()),
  OPENAI_BASE_URL: z.preprocess(emptyStringToUndefined, z.string().url().optional()),

  // Optional override for default resolution when request does not specify an explicit model id.
  MODEL_ID: z.preprocess(emptyStringToUndefined, z.string().min(1).optional()),

  OPENROUTER_API_KEY: z.preprocess(emptyStringToUndefined, z.string().min(1).optional()),
  OPENROUTER_BASE_URL: z.preprocess(emptyStringToUndefined, z.string().url().optional()),

  MODEL_METADATA_CACHE_TTL_SECONDS: z.preprocess(emptyStringToUndefined, z.coerce.number().int().nonnegative().optional()),

  NODE_ENV: z.preprocess(emptyStringToUndefined, z.string().optional()),
});

export function loadConfig(env: NodeJS.ProcessEnv = process.env): RuntimeConfig {
  const parsed = envSchema.parse(env);

  return {
    port: parsed.PORT,
    openAiApiKey: parsed.OPENAI_API_KEY ?? null,
    openAiBaseUrl: parsed.OPENAI_BASE_URL ?? null,
    modelId: parsed.MODEL_ID ?? "gpt-4o-mini",
    requestTimeoutMs: 30_000,
    nodeEnv: parsed.NODE_ENV,
    modelsConfigPath: parsed.MODEL_CONFIG_PATH ?? defaultModelsConfigPath(),
    openRouterApiKey: parsed.OPENROUTER_API_KEY ?? null,
    openRouterBaseUrl: parsed.OPENROUTER_BASE_URL ?? null,
    modelMetadataCacheTtlSeconds: parsed.MODEL_METADATA_CACHE_TTL_SECONDS ?? null,
  };
}

