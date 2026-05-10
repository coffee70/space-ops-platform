import { ModelSelectionError } from "./model-errors.js";
import {
  defaultAllowedModes,
  defaultDataBoundary,
  loadModelRegistryConfig,
  resolveProcessEnvKey,
  type LoadedModelRegistry,
} from "./model-registry-config.js";
import { fallbackMetadataForEntry } from "./metadata/fallback-metadata.js";
import { ensureOpenRouterCatalog, resolveOpenRouterMetadata } from "./metadata/openrouter-resolver.js";
import type {
  AiEngineerModelOption,
  ExecutionMode,
  ListAiEngineerModelsResponse,
  ModelCatalogPort,
  ModelMetadata,
  ModelProviderType,
  ModelRegistryEntry,
  ModelRegistryProvider,
  ResolvedChatModel,
  ResolvedRuntimeModel,
  RuntimeConfig,
} from "../types.js";

function mergeMetadata(base: ModelMetadata, overlay: Partial<ModelMetadata>): ModelMetadata {
  return {
    ...base,
    ...overlay,
    pricing: {
      ...base.pricing,
      ...overlay.pricing,
    },
    inputModalities: overlay.inputModalities ?? base.inputModalities,
    outputModalities: overlay.outputModalities ?? base.outputModalities,
    supportedParameters: overlay.supportedParameters ?? base.supportedParameters,
    capabilities: overlay.capabilities ?? base.capabilities,
    metadataSources: [...new Set([...base.metadataSources, ...(overlay.metadataSources ?? [])])],
  };
}

function providerSortKey(provider: ModelRegistryProvider): number {
  const name = provider.displayName.toLowerCase();
  if (name.includes("openai") && !name.includes("compatible")) return 0;
  if (name.includes("anthropic")) return 1;
  if (name.includes("bare") || name.includes("local")) return 2;
  return 3;
}

function qualityRank(tier: ModelMetadata["qualityTier"]): number {
  const order: ModelMetadata["qualityTier"][] = ["frontier", "advanced", "standard", "unknown"];
  return order.indexOf(tier);
}

function extractGpt5Minor(pm: string): number | null {
  const m = pm.match(/^gpt-5\.(\d+)/);
  if (!m) return null;
  return Number.parseInt(m[1], 10);
}

export function sortModelsForPicker(models: AiEngineerModelOption[]): AiEngineerModelOption[] {
  return [...models].sort((x, y) => {
    if (x.enabled !== y.enabled) return x.enabled ? -1 : 1;
    if (x.isDefault !== y.isDefault) return x.isDefault ? -1 : 1;
    const xDemo = x.recommendedFor.includes("demo-safe") ? 1 : 0;
    const yDemo = y.recommendedFor.includes("demo-safe") ? 1 : 0;
    if (xDemo !== yDemo) return yDemo - xDemo;

    const px = providerSortKey({ id: x.providerRef, type: x.providerType, displayName: x.provider } as ModelRegistryProvider);
    const py = providerSortKey({ id: y.providerRef, type: y.providerType, displayName: y.provider } as ModelRegistryProvider);
    if (px !== py) return px - py;

    const mx = extractGpt5Minor(x.providerModelId);
    const my = extractGpt5Minor(y.providerModelId);
    if (mx !== null && my !== null && mx !== my) return my - mx;

    const qx = qualityRank(x.qualityTier);
    const qy = qualityRank(y.qualityTier);
    if (qx !== qy) return qx - qy;

    return x.name.localeCompare(y.name);
  });
}

export class ModelCatalogService implements ModelCatalogPort {
  private readonly registry: LoadedModelRegistry;
  private readonly runtime: RuntimeConfig;
  private cache:
    | {
        payload: ListAiEngineerModelsResponse;
        byId: Map<string, AiEngineerModelOption>;
      }
    | undefined;

  constructor(runtime: RuntimeConfig, registry?: LoadedModelRegistry) {
    this.runtime = runtime;
    this.registry = registry ?? loadModelRegistryConfig(runtime);
  }

  async listModelsResponse(): Promise<ListAiEngineerModelsResponse> {
    const built = await this.buildEnriched();
    this.cache = built;
    return built.payload;
  }

  async resolveForChat(modelId: string | null | undefined, executionMode: ExecutionMode): Promise<ResolvedChatModel> {
    const built = this.cache ?? (await this.buildEnriched());
    const byId = built.byId;

    const requested = typeof modelId === "string" && modelId.trim().length > 0 ? modelId.trim() : null;

    let resolvedId: string | null = null;

    if (requested) {
      resolvedId = requested;
    } else {
      const defaultChat = this.registry.defaults.chatModel;
      if (defaultChat && byId.get(defaultChat)?.enabled) {
        resolvedId = defaultChat;
      } else if (this.runtime.modelId && byId.get(this.runtime.modelId)?.enabled) {
        resolvedId = this.runtime.modelId;
      } else {
        resolvedId = [...byId.values()].find((m) => m.enabled)?.id ?? null;
      }
    }

    if (!resolvedId) {
      throw new ModelSelectionError("unknown_model", "No model could be resolved for this chat request.");
    }

    const option = byId.get(resolvedId);
    if (!option) {
      throw new ModelSelectionError("unknown_model", `Unknown model: ${resolvedId}`);
    }

    if (!option.enabled || !option.isAvailable) {
      throw new ModelSelectionError("model_disabled", option.disabledReason ?? `Model is disabled: ${resolvedId}`);
    }

    if (!option.governance.allowedModes.includes(executionMode)) {
      throw new ModelSelectionError(
        "model_not_allowed_for_mode",
        `Model ${resolvedId} is not allowed in ${executionMode}`,
      );
    }

    const entry = this.registry.models.find((m) => m.id === resolvedId);
    const provider = entry ? this.registry.providersById.get(entry.providerRef) : undefined;
    if (!entry || !provider) {
      throw new ModelSelectionError("unknown_model", `Unknown model: ${resolvedId}`);
    }

    const unsupported: ModelProviderType[] = ["google", "azure-openai", "bedrock", "vertex", "vercel-gateway"];
    if (unsupported.includes(provider.type)) {
      throw new ModelSelectionError("provider_not_implemented", `Provider type ${provider.type} is not implemented yet.`);
    }

    const runtimeModel = this.resolveProviderRuntime(entry, provider);

    return { option, runtime: runtimeModel };
  }

  private resolveProviderRuntime(entry: ModelRegistryEntry, provider: ModelRegistryProvider): ResolvedRuntimeModel {
    let apiKey = provider.apiKeyEnv ? resolveProcessEnvKey(provider.apiKeyEnv) : null;
    if (!apiKey && provider.type === "openai") {
      apiKey = this.runtime.openAiApiKey;
    }

    let baseUrl: string | null = provider.baseUrl ?? null;
    if (provider.type === "openai") {
      baseUrl = this.runtime.openAiBaseUrl ?? baseUrl;
    }

    return {
      id: entry.id,
      providerType: provider.type,
      providerModelId: entry.providerModelId,
      apiKey,
      baseUrl,
    };
  }

  private async buildEnriched(): Promise<{ payload: ListAiEngineerModelsResponse; byId: Map<string, AiEngineerModelOption> }> {
    const { modelsById, cached, updatedAt } = await ensureOpenRouterCatalog(this.registry, this.runtime);

    const options: AiEngineerModelOption[] = [];
    for (const entry of this.registry.models) {
      const provider = this.registry.providersById.get(entry.providerRef);
      if (!provider) continue;
      const option = this.enrichEntry(entry, provider, modelsById);
      options.push(option);
    }

    const sorted = sortModelsForPicker(options);
    const byId = new Map(sorted.map((item) => [item.id, item]));

    const metadataResolverLabels = ["fallback", "config-overrides"];
    if (this.registry.metadataResolvers?.openrouter?.enabled) {
      metadataResolverLabels.unshift("openrouter");
    }

    const payload: ListAiEngineerModelsResponse = {
      default_model_id: this.registry.defaults.chatModel,
      models: sorted,
      metadata: {
        registrySource: "config",
        metadataResolvers: metadataResolverLabels,
        cached,
        updatedAt,
      },
    };

    return { payload, byId };
  }

  private enrichEntry(
    entry: ModelRegistryEntry,
    provider: ModelRegistryProvider,
    modelsById: Map<string, unknown>,
  ): AiEngineerModelOption {
    const fallback = fallbackMetadataForEntry({ entry, provider });
    const orPartial =
      this.registry.metadataResolvers?.openrouter?.enabled
        ? resolveOpenRouterMetadata({ provider, entry, modelsById })
        : null;

    let merged = orPartial ? mergeMetadata(fallback, orPartial) : fallback;

    const overrides = entry.metadataOverrides;
    let recommendedFor: string[] = overrides?.recommendedFor ?? [];

    if (overrides) {
      const { recommendedFor: _r, ...metaParts } = overrides as Record<string, unknown>;
      merged = mergeMetadata(merged, metaParts as Partial<ModelMetadata>);
      if (Array.isArray(_r)) recommendedFor = _r as string[];
    }

    const allowedModes = defaultAllowedModes(entry);
    const dataBoundary = defaultDataBoundary(entry);

    const isDefault = entry.id === this.registry.defaults.chatModel;

    return {
      id: entry.id,
      providerRef: provider.id,
      providerType: provider.type,
      providerModelId: entry.providerModelId,
      name: merged.displayName,
      provider: merged.providerDisplayName,
      description: merged.description,
      enabled: entry.enabled,
      isAvailable: entry.enabled,
      disabledReason: entry.enabled ? null : entry.disabledReason ?? "Model is disabled",
      isDefault,
      defaultFor: entry.defaultFor ?? [],
      governance: {
        allowedModes,
        dataBoundary,
      },
      contextWindow: merged.contextWindow,
      maxOutputTokens: merged.maxOutputTokens,
      inputModalities: merged.inputModalities,
      outputModalities: merged.outputModalities,
      supportedParameters: merged.supportedParameters,
      capabilities: merged.capabilities,
      pricing: merged.pricing,
      qualityTier: merged.qualityTier,
      costTier: merged.costTier,
      speedTier: merged.speedTier,
      reasoningTier: merged.reasoningTier,
      recommendedFor,
      metadataSources: merged.metadataSources,
    };
  }
}
