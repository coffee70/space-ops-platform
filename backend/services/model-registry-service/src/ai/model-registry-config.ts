import { readFileSync, existsSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { parseDocument } from "yaml";
import { z } from "zod";

import type {
  ExecutionMode,
  ModelDataBoundary,
  ModelProviderType,
  ModelRegistryEntry,
  ModelRegistryProvider,
  RuntimeConfig,
} from "../types.js";

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

export type ModelRegistryValidateConfigErrorItem = {
  loc: string[];
  message: string;
  type: string;
};

export type ModelRegistryValidateParsedSummary = {
  provider_count: number;
  model_count: number;
  enabled_model_count: number;
  default_model_id?: string | null;
  provider_types: string[];
  missing_api_key_envs: string[];
  warnings: string[];
};

export type ModelRegistryValidateConfigResponse = {
  valid: boolean;
  parsed: ModelRegistryValidateParsedSummary | null;
  errors: ModelRegistryValidateConfigErrorItem[];
};

const SK_LIKE_SECRET = /^sk-[a-zA-Z0-9_-]{8,}$/;

function yamlComposeErrors(loc: unknown): ModelRegistryValidateConfigErrorItem[] {
  if (
    typeof loc !== "object" ||
    loc === null ||
    !("message" in loc) ||
    typeof (loc as { message: unknown }).message !== "string"
  ) {
    return [{ loc: [], message: "YAML parse failed", type: "yaml_parse" }];
  }
  return [{ loc: [], message: (loc as { message: string }).message, type: "yaml_syntax" }];
}

function zodToErrors(error: z.ZodError): ModelRegistryValidateConfigErrorItem[] {
  return error.issues.map((issue) => ({
    loc: issue.path.map(String),
    message: issue.message,
    type: `zod.${issue.code}`,
  }));
}

function checkApiKeyEnvReferences(registry: ModelRegistryConfigFile): ModelRegistryValidateConfigErrorItem[] {
  const errs: ModelRegistryValidateConfigErrorItem[] = [];

  for (const [pid, provider] of Object.entries(registry.providers)) {
    const envName = provider.apiKeyEnv;
    if (envName?.trim()) {
      checkApiKeyEnvReferencesHelper(errs, ["providers", pid, "apiKeyEnv"], envName);
    }
  }

  const or = registry.metadataResolvers?.openrouter;
  if (or?.apiKeyEnv?.trim()) {
    checkApiKeyEnvReferencesHelper(errs, ["metadataResolvers", "openrouter", "apiKeyEnv"], or.apiKeyEnv);
  }

  return errs;
}

function checkApiKeyEnvReferencesHelper(
  errs: ModelRegistryValidateConfigErrorItem[],
  loc: string[],
  envName: string,
): void {
  const s = envName.trim();
  if (SK_LIKE_SECRET.test(s)) {
    errs.push({
      loc,
      message:
        "Do not paste provider API keys into YAML; set apiKeyEnv to an environment variable name (for example OPENAI_API_KEY)",
      type: "value_error.literal_secret",
    });
  } else if (!/^[A-Z][A-Z0-9_]*$/.test(s)) {
    errs.push({
      loc,
      message: "apiKeyEnv should look like an environment variable name (for example OPENAI_API_KEY)",
      type: "value_error.pattern",
    });
  }
}

function collectMissingApiKeyEnvNames(registry: ModelRegistryConfigFile): string[] {
  const names = new Set<string>();
  for (const p of Object.values(registry.providers)) {
    if (p.apiKeyEnv?.trim()) names.add(p.apiKeyEnv.trim());
  }
  const or = registry.metadataResolvers?.openrouter;
  if (or?.apiKeyEnv?.trim()) names.add(or.apiKeyEnv.trim());

  const missing: string[] = [];
  for (const name of [...names].sort()) {
    if (!resolveProcessEnvKey(name)) missing.push(name);
  }
  return missing;
}

function semanticErrorsForRegistry(parsed: ModelRegistryConfigFile): ModelRegistryValidateConfigErrorItem[] {
  const errors: ModelRegistryValidateConfigErrorItem[] = [];
  errors.push(...checkApiKeyEnvReferences(parsed));

  const providersById = new Map<string, ModelRegistryProvider>();
  for (const [id, provider] of Object.entries(parsed.providers)) {
    providersById.set(id, { id, ...provider });
  }

  const modelIds = new Set<string>();
  parsed.models.forEach((model, index) => {
    if (modelIds.has(model.id)) {
      errors.push({
        loc: ["models"],
        message: `Duplicate model id in registry: ${model.id}`,
        type: "semantic.duplicate_model_id",
      });
    }
    modelIds.add(model.id);
    if (!providersById.has(model.providerRef)) {
      errors.push({
        loc: ["models", String(index)],
        message: `Model ${model.id} references unknown providerRef ${model.providerRef}`,
        type: "semantic.unknown_provider_ref",
      });
    }
  });

  for (const key of ["chatModel", "codingModel", "fastModel", "restrictedModel"] as const) {
    const modelId = parsed.defaults[key];
    if (!modelIds.has(modelId)) {
      errors.push({
        loc: ["defaults", key],
        message: `defaults.${key} (${modelId}) is not a configured model id`,
        type: "semantic.defaults_model_id",
      });
    }
  }

  const chatDefaults = parsed.models.filter((m) => m.defaultFor?.includes("chat") ?? false);
  if (chatDefaults.length !== 1) {
    errors.push({
      loc: ["models"],
      message: `Expected exactly one model with defaultFor containing "chat", found ${chatDefaults.length}`,
      type: "semantic.chat_default",
    });
  } else if (chatDefaults[0].id !== parsed.defaults.chatModel) {
    errors.push({
      loc: ["defaults", "chatModel"],
      message: `defaults.chatModel (${parsed.defaults.chatModel}) must match the model whose defaultFor includes "chat" (${chatDefaults[0].id})`,
      type: "semantic.chat_default_mismatch",
    });
  }

  const enabledModels = parsed.models.filter((m) => m.enabled);
  if (enabledModels.length === 0) {
    errors.push({
      loc: ["models"],
      message: "Model registry must include at least one enabled model",
      type: "semantic.enabled_models",
    });
  }

  return errors;
}

function buildParsedSummary(parsed: ModelRegistryConfigFile): ModelRegistryValidateParsedSummary {
  const providerTypes = [...new Set(Object.values(parsed.providers).map((p) => p.type))].sort((a, b) => a.localeCompare(b));

  const enabledCt = parsed.models.filter((m) => m.enabled).length;
  const chat = parsed.defaults.chatModel?.trim();

  return {
    provider_count: Object.keys(parsed.providers).length,
    model_count: parsed.models.length,
    enabled_model_count: enabledCt,
    default_model_id: chat || null,
    provider_types: providerTypes,
    missing_api_key_envs: collectMissingApiKeyEnvNames(parsed),
    warnings: [],
  };
}

function buildLoadedRegistry(parsed: ModelRegistryConfigFile): LoadedModelRegistry {
  const providersById = new Map<string, ModelRegistryProvider>();
  for (const [id, provider] of Object.entries(parsed.providers)) {
    providersById.set(id, { id, ...provider });
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

export type CompiledRegistryOk = {
  ok: true;
  registry: LoadedModelRegistry;
  parsed: ModelRegistryConfigFile;
  summary: ModelRegistryValidateParsedSummary;
};

export type CompiledRegistryFail = {
  ok: false;
  errors: ModelRegistryValidateConfigErrorItem[];
};

/** Shared validation path for file load + HTTP POST /models/validate-config. */
export function compileRegistryYamlContent(content: string): CompiledRegistryOk | CompiledRegistryFail {
  const trimmed = typeof content === "string" ? content : "";

  try {
    const doc = parseDocument(trimmed, { uniqueKeys: true });
    if (doc.errors.length > 0) {
      return { ok: false, errors: yamlComposeErrors(doc.errors[0]) };
    }
    const parsedUnknown = doc.toJSON();
    if (parsedUnknown !== null && parsedUnknown !== undefined && typeof parsedUnknown !== "object") {
      return {
        ok: false,
        errors: [{ loc: [], message: "Top-level YAML must be an object", type: "type_error.object" }],
      };
    }

    const zodParsed = registryFileSchema.safeParse(parsedUnknown);
    if (!zodParsed.success) {
      return { ok: false, errors: zodToErrors(zodParsed.error) };
    }

    const semantic = semanticErrorsForRegistry(zodParsed.data);
    if (semantic.length > 0) {
      return { ok: false, errors: semantic };
    }

    const summary = buildParsedSummary(zodParsed.data);
    const registry = buildLoadedRegistry(zodParsed.data);

    return { ok: true, registry, parsed: zodParsed.data, summary };
  } catch (cause) {
    if (cause && typeof cause === "object" && "message" in cause && typeof (cause as { message: unknown }).message === "string") {
      const msg = (cause as { message: string }).message;
      if (msg.includes("Duplicate key")) {
        return { ok: false, errors: [{ loc: [], message: msg, type: "yaml_duplicate_key" }] };
      }
    }
    return {
      ok: false,
      errors: [{ loc: [], message: String(cause ?? "YAML parse failed"), type: "yaml_error" }],
    };
  }
}

export function validateModelRegistryConfigContent(content: string): ModelRegistryValidateConfigResponse {
  const r = compileRegistryYamlContent(content);
  if (!r.ok) {
    return { valid: false, parsed: null, errors: r.errors };
  }
  return { valid: true, parsed: r.summary, errors: [] };
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

  const compiled = compileRegistryYamlContent(rawYaml);
  if (!compiled.ok) {
    const msg =
      compiled.errors.map((e) => e.message).join("; ") || "Invalid model registry configuration";
    throw new Error(msg);
  }
  return compiled.registry;
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
