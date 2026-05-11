import type { LoadedModelRegistry } from "../model-registry-config.js";
import type { ModelCapability, ModelMetadata, ModelRegistryEntry, ModelRegistryProvider } from "../../types.js";

const OPENROUTER_FETCH_TIMEOUT_MS = 5000;

type OpenRouterModel = {
  id: string;
  name?: string;
  description?: string | null;
  context_length?: number | null;
  architecture?: {
    input_modalities?: string[];
    output_modalities?: string[];
    modality?: string | null;
    tokenizer?: string | null;
    instruct_type?: string | null;
  };
  top_provider?: {
    context_length?: number | null;
    max_completion_tokens?: number | null;
  };
  supported_parameters?: string[];
  pricing?: {
    prompt?: string | number | null;
    completion?: string | number | null;
    web_search?: string | number | null;
    internal_reasoning?: string | number | null;
    image?: string | number | null;
    request?: string | number | null;
  };
};

type CacheEntry = {
  fetchedAt: number;
  modelsById: Map<string, OpenRouterModel>;
};

/** Keyed by resolver endpoint + auth presence (never stores secrets). */
const cacheByKey = new Map<string, CacheEntry>();

/** Clears OpenRouter fetch caches after registry updates and for isolated tests. */
export function invalidateOpenRouterResolverCache(): void {
  cacheByKey.clear();
}

export const resetOpenRouterResolverCacheForTests = invalidateOpenRouterResolverCache;

export function buildOpenRouterCacheKey(baseUrl: string, hasAuth: boolean): string {
  const normalized = baseUrl.replace(/\/$/, "");
  return `${normalized}|auth=${hasAuth}|output=text`;
}

function ttlMs(registry: LoadedModelRegistry, runtimeTtlSeconds: number | null): number {
  const fromRegistry = registry.metadataResolvers?.openrouter?.cacheTtlSeconds;
  const seconds = runtimeTtlSeconds ?? fromRegistry ?? 21_600;
  return seconds * 1000;
}

export async function ensureOpenRouterCatalog(
  registry: LoadedModelRegistry,
  runtime: { openRouterApiKey: string | null; openRouterBaseUrl: string | null; modelMetadataCacheTtlSeconds: number | null },
): Promise<{ modelsById: Map<string, OpenRouterModel>; cached: boolean; updatedAt: string }> {
  const openrouter = registry.metadataResolvers?.openrouter;
  if (!openrouter?.enabled) {
    return { modelsById: new Map(), cached: true, updatedAt: new Date(0).toISOString() };
  }

  const now = Date.now();
  const ttl = ttlMs(registry, runtime.modelMetadataCacheTtlSeconds);

  const apiKey = runtime.openRouterApiKey ?? resolveProcessEnvKey(openrouter.apiKeyEnv);
  const baseUrl =
    runtime.openRouterBaseUrl?.trim() ||
    openrouter.baseUrl.trim();

  const cacheKey = buildOpenRouterCacheKey(baseUrl, Boolean(apiKey));
  const cachedEntry = cacheByKey.get(cacheKey);
  if (cachedEntry && now - cachedEntry.fetchedAt < ttl) {
    return { modelsById: cachedEntry.modelsById, cached: true, updatedAt: new Date(cachedEntry.fetchedAt).toISOString() };
  }

  const headers: Record<string, string> = {};
  if (apiKey) {
    headers.Authorization = `Bearer ${apiKey}`;
  } else if (!openrouter.allowUnauthenticated) {
    const stale = cacheByKey.get(cacheKey);
    return {
      modelsById: stale?.modelsById ?? new Map(),
      cached: Boolean(stale),
      updatedAt: new Date(stale?.fetchedAt ?? 0).toISOString(),
    };
  }

  const url = `${baseUrl.replace(/\/$/, "")}?output_modalities=text`;
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), OPENROUTER_FETCH_TIMEOUT_MS);

  try {
    const response = await fetch(url, { headers, signal: controller.signal });
    if (!response.ok) {
      const stale = cacheByKey.get(cacheKey);
      return {
        modelsById: stale?.modelsById ?? new Map(),
        cached: Boolean(stale),
        updatedAt: new Date(stale?.fetchedAt ?? 0).toISOString(),
      };
    }
    const body = (await response.json()) as { data?: OpenRouterModel[] };
    const modelsById = new Map<string, OpenRouterModel>();
    for (const model of body.data ?? []) {
      modelsById.set(model.id, model);
    }
    cacheByKey.set(cacheKey, { fetchedAt: now, modelsById });
    return { modelsById, cached: false, updatedAt: new Date(now).toISOString() };
  } catch {
    const stale = cacheByKey.get(cacheKey);
    return {
      modelsById: stale?.modelsById ?? new Map(),
      cached: Boolean(stale),
      updatedAt: new Date(stale?.fetchedAt ?? 0).toISOString(),
    };
  } finally {
    clearTimeout(timeoutId);
  }
}

function resolveProcessEnvKey(envName: string): string | null {
  const value = process.env[envName];
  if (typeof value !== "string" || value.trim().length === 0) return null;
  return value.trim();
}

function pickCapability(map: Map<string, OpenRouterModel>, candidates: string[]): OpenRouterModel | null {
  for (const id of candidates) {
    const hit = map.get(id);
    if (hit) return hit;
  }
  return null;
}

function num(v: string | number | null | undefined): number | null {
  if (v === null || v === undefined) return null;
  const n = typeof v === "number" ? v : Number(v);
  return Number.isFinite(n) ? n : null;
}

function deriveCapabilities(model: OpenRouterModel): ModelCapability[] {
  const caps = new Set<ModelCapability>();
  caps.add("text");
  const inputModalities = model.architecture?.input_modalities ?? [];
  const outputModalities = model.architecture?.output_modalities ?? [];
  const supported = model.supported_parameters ?? [];
  if (outputModalities.includes("text")) caps.add("text");
  if (inputModalities.includes("image")) caps.add("vision");
  if (supported.some((p) => p === "tools" || p === "tool_choice")) caps.add("tool-use");
  if (supported.some((p) => p.includes("reasoning") || p === "include_reasoning")) caps.add("reasoning");
  if (supported.some((p) => p.includes("response_format") || p.includes("structured_outputs"))) caps.add("json");
  if (inputModalities.includes("file")) caps.add("file-input");
  const ws = num(model.pricing?.web_search);
  if (ws !== null && ws !== 0) caps.add("web-search");
  return [...caps];
}

export function openRouterToMetadata(model: OpenRouterModel, providerDisplayName: string): ModelMetadata {
  const contextWindow =
    num(model.context_length as number | string | null | undefined) ??
    num(model.top_provider?.context_length as number | string | null | undefined);
  const maxOutputTokens = num(model.top_provider?.max_completion_tokens as number | string | null | undefined);

  const promptPrice = num(model.pricing?.prompt);
  const completionPrice = num(model.pricing?.completion);

  return {
    displayName: model.name ?? model.id.split("/").pop() ?? model.id,
    providerDisplayName,
    description: model.description ?? null,
    contextWindow,
    maxOutputTokens,
    inputModalities: model.architecture?.input_modalities ?? [],
    outputModalities: model.architecture?.output_modalities ?? [],
    supportedParameters: model.supported_parameters ?? [],
    capabilities: deriveCapabilities(model),
    pricing: {
      inputPerMillionTokens: promptPrice !== null ? promptPrice * 1_000_000 : null,
      outputPerMillionTokens: completionPrice !== null ? completionPrice * 1_000_000 : null,
      currency: "USD",
    },
    qualityTier: "unknown",
    costTier: "unknown",
    speedTier: "unknown",
    reasoningTier: "unknown",
    metadataSources: ["openrouter"],
  };
}

export function resolveOpenRouterMetadata(input: {
  provider: ModelRegistryProvider;
  entry: ModelRegistryEntry;
  modelsById: Map<string, unknown>;
}): ModelMetadata | null {
  const { provider, entry, modelsById } = input;
  const pt = provider.type;
  const pm = entry.providerModelId;

  const candidates: string[] = [];
  if (pt === "openai") {
    candidates.push(`openai/${pm}`, `${provider.id}/${pm}`, pm);
  } else if (pt === "anthropic") {
    candidates.push(`anthropic/${pm}`, `${provider.id}/${pm}`, pm);
  } else if (pt === "openai-compatible") {
    candidates.push(pm, `${provider.id}/${pm}`);
  } else {
    candidates.push(`${provider.id}/${pm}`, pm);
  }

  const or = pickCapability(modelsById as Map<string, OpenRouterModel>, candidates);
  if (!or) return null;
  return openRouterToMetadata(or, provider.displayName);
}
