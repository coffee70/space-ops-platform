import { readFileSync, existsSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { parse } from "yaml";
import { z } from "zod";

import type { ExecutionMode, ModelDataBoundary, ModelProviderType, ModelRegistryEntry, ModelRegistryProvider } from "../types.js";
import type { RuntimeConfig } from "../types.js";

const executionModeSchema = z.enum(["read_only", "suggest", "execute", "governed_execute"]);

const providerTypeSchema = z.enum([
  "openai",
  "anthropic",
  "openai-compatible",
  "google",
  "azure-openai",
  "bedrock",
  "vertex",
  "vercel-gateway",
]) as z.ZodType<ModelProviderType>;

const dataBoundarySchema = z.enum(["external_api", "private_cloud", "local_airgapped", "unknown"]) as z.ZodType<ModelDataBoundary>;

const modelCapabilitySchema = z.enum(["text", "vision", "tool-use", "reasoning", "json", "file-input", "web-search", "code"]);

const metadataOverridesSchema = z
  .object({
    displayName: z.string().optional(),
    providerDisplayName: z.string().optional(),
    description: z.string().nullable().optional(),
    contextWindow: z.number().nullable().optional(),
    maxOutputTokens: z.number().nullable().optional(),
    inputModalities: z.array(z.string()).optional(),
    outputModalities: z.array(z.string()).optional(),
    supportedParameters: z.array(z.string()).optional(),
    capabilities: z.array(modelCapabilitySchema).optional(),
    pricing: z
      .object({
        inputPerMillionTokens: z.number().nullable().optional(),
        outputPerMillionTokens: z.number().nullable().optional(),
        currency: z.enum(["USD", "internal"]).nullable().optional(),
      })
      .optional(),
    qualityTier: z.enum(["standard", "advanced", "frontier", "unknown"]).optional(),
    costTier: z.enum(["$", "$$", "$$$", "$$$$", "internal", "unknown"]).optional(),
    speedTier: z.enum(["fast", "balanced", "deep", "unknown"]).optional(),
    reasoningTier: z.enum(["none", "light", "strong", "unknown"]).optional(),
    recommendedFor: z.array(z.string()).optional(),
  })
  .strict();

const modelEntrySchema = z.object({
  id: z.string().min(1),
  providerRef: z.string().min(1),
  providerModelId: z.string().min(1),
  enabled: z.boolean(),
  disabledReason: z.string().optional(),
  defaultFor: z.array(z.string()).optional(),
  governance: z
    .object({
      allowedModes: z.array(executionModeSchema).optional(),
      dataBoundary: dataBoundarySchema.optional(),
    })
    .optional(),
  metadataOverrides: metadataOverridesSchema.optional(),
});

const providerEntrySchema = z.object({
  type: providerTypeSchema,
  displayName: z.string().min(1),
  apiKeyEnv: z.string().min(1).optional(),
  baseUrl: z.string().url().optional(),
});

const registryFileSchema = z.object({
  version: z.literal(1),
  defaults: z.object({
    chatModel: z.string().min(1),
    codingModel: z.string().min(1),
    fastModel: z.string().min(1),
    restrictedModel: z.string().min(1),
  }),
  metadataResolvers: z
    .object({
      openrouter: z
        .object({
          enabled: z.boolean(),
          baseUrl: z.string().url(),
          apiKeyEnv: z.string().min(1),
          cacheTtlSeconds: z.number().int().nonnegative(),
          allowUnauthenticated: z.boolean(),
        })
        .optional(),
    })
    .optional(),
  providers: z.record(z.string(), providerEntrySchema),
  models: z.array(modelEntrySchema),
});

export type ModelRegistryConfigFile = z.infer<typeof registryFileSchema>;

export type LoadedModelRegistry = {
  defaults: ModelRegistryConfigFile["defaults"];
  metadataResolvers: ModelRegistryConfigFile["metadataResolvers"];
  providersById: Map<string, ModelRegistryProvider>;
  models: ModelRegistryEntry[];
};

function resolveRegistryConfigDir(): string {
  const here = path.dirname(fileURLToPath(import.meta.url));
  return path.join(here, "..", "..", "config");
}

/** Bundled fallbacks: optional gitignored override, then committed example. */
function bundledRegistryConfigCandidates(): string[] {
  const dir = resolveRegistryConfigDir();
  return [path.join(dir, "models.local.yaml"), path.join(dir, "models.local.yaml.example")];
}

function uniqueCandidatePaths(configuredPath: string | null | undefined): string[] {
  const seen = new Set<string>();
  const out: string[] = [];
  const push = (p: string) => {
    const normalized = path.normalize(p);
    if (seen.has(normalized)) return;
    seen.add(normalized);
    out.push(normalized);
  };
  const trimmed = configuredPath?.trim() ?? "";
  if (trimmed) push(trimmed);
  for (const bundled of bundledRegistryConfigCandidates()) {
    push(bundled);
  }
  return out;
}

export function loadModelRegistryConfig(runtime: RuntimeConfig): LoadedModelRegistry {
  const candidatePaths = uniqueCandidatePaths(runtime.modelsConfigPath);

  let rawYaml: string | null = null;
  for (const candidate of candidatePaths) {
    if (candidate && existsSync(candidate)) {
      rawYaml = readFileSync(candidate, "utf8");
      break;
    }
  }

  if (!rawYaml) {
    const isProd = runtime.nodeEnv === "production";
    const envHint = runtime.modelsConfigPath?.trim() ?? "<default bundled paths>";
    if (isProd) {
      throw new Error(`Model registry config not found (AGENT_RUNTIME_MODELS_CONFIG_PATH=${envHint}). Tried: ${candidatePaths.join(", ")}`);
    }
    throw new Error(`Model registry config not found. Tried: ${candidatePaths.join(", ")}`);
  }

  const parsedUnknown = parse(rawYaml);
  const parsed = registryFileSchema.parse(parsedUnknown);

  const providersById = new Map<string, ModelRegistryProvider>();
  for (const [id, provider] of Object.entries(parsed.providers)) {
    providersById.set(id, { id, ...provider });
  }

  const modelIds = new Set<string>();
  for (const model of parsed.models) {
    if (modelIds.has(model.id)) {
      throw new Error(`Duplicate model id in registry: ${model.id}`);
    }
    modelIds.add(model.id);
    if (!providersById.has(model.providerRef)) {
      throw new Error(`Model ${model.id} references unknown providerRef ${model.providerRef}`);
    }
  }

  for (const key of ["chatModel", "codingModel", "fastModel", "restrictedModel"] as const) {
    const modelId = parsed.defaults[key];
    if (!modelIds.has(modelId)) {
      throw new Error(`defaults.${key} (${modelId}) is not a configured model id`);
    }
  }

  const chatDefaults = parsed.models.filter((m) => m.defaultFor?.includes("chat") ?? false);
  if (chatDefaults.length !== 1) {
    throw new Error(`Expected exactly one model with defaultFor containing "chat", found ${chatDefaults.length}`);
  }
  if (chatDefaults[0].id !== parsed.defaults.chatModel) {
    throw new Error(`defaults.chatModel (${parsed.defaults.chatModel}) must match the model whose defaultFor includes "chat" (${chatDefaults[0].id})`);
  }

  const enabledModels = parsed.models.filter((m) => m.enabled);
  if (enabledModels.length === 0) {
    throw new Error("Model registry must include at least one enabled model");
  }

  const models: ModelRegistryEntry[] = parsed.models.map((m) => ({
    id: m.id,
    providerRef: m.providerRef,
    providerModelId: m.providerModelId,
    enabled: m.enabled,
    disabledReason: m.disabledReason,
    defaultFor: m.defaultFor,
    governance: m.governance as ModelRegistryEntry["governance"],
    metadataOverrides: m.metadataOverrides as ModelRegistryEntry["metadataOverrides"],
  }));

  return {
    defaults: parsed.defaults,
    metadataResolvers: parsed.metadataResolvers,
    providersById,
    models,
  };
}

export function resolveProcessEnvKey(envName: string | undefined): string | null {
  if (!envName) return null;
  const value = process.env[envName];
  if (typeof value !== "string" || value.trim().length === 0) return null;
  return value.trim();
}

export function defaultAllowedModes(entry: ModelRegistryEntry): ExecutionMode[] {
  return entry.governance?.allowedModes ?? ["read_only", "suggest", "execute"];
}

export function defaultDataBoundary(entry: ModelRegistryEntry): ModelDataBoundary {
  return entry.governance?.dataBoundary ?? "unknown";
}
