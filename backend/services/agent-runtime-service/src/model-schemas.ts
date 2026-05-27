import { z } from "zod";

export const ExecutionModeSchema = z.enum(["read_only", "suggest", "execute", "governed_execute"]);
const ModelProviderTypeSchema = z.enum([
  "openai",
  "anthropic",
  "openai-compatible",
  "google",
  "azure-openai",
  "bedrock",
  "vertex",
  "vercel-gateway",
]);
const ModelDataBoundarySchema = z.enum(["external_api", "private_cloud", "local_airgapped", "unknown"]);
const ModelCapabilitySchema = z.enum(["text", "vision", "tool-use", "reasoning", "json", "file-input", "web-search", "code"]);

export const AiEngineerModelOptionSchema = z
  .object({
    id: z.string(),
    providerRef: z.string(),
    providerType: ModelProviderTypeSchema,
    providerModelId: z.string(),
    name: z.string(),
    provider: z.string(),
    description: z.string().nullable(),
    enabled: z.boolean(),
    isAvailable: z.boolean(),
    disabledReason: z.string().nullable(),
    isDefault: z.boolean(),
    defaultFor: z.array(z.string()),
    governance: z
      .object({
        allowedModes: z.array(ExecutionModeSchema),
        dataBoundary: ModelDataBoundarySchema,
      })
      .passthrough(),
    contextWindow: z.number().nullable(),
    maxOutputTokens: z.number().nullable(),
    inputModalities: z.array(z.string()),
    outputModalities: z.array(z.string()),
    supportedParameters: z.array(z.string()),
    capabilities: z.array(ModelCapabilitySchema),
    pricing: z
      .object({
        inputPerMillionTokens: z.number().nullable(),
        outputPerMillionTokens: z.number().nullable(),
        currency: z.enum(["USD", "internal"]).nullable(),
      })
      .passthrough(),
    qualityTier: z.enum(["standard", "advanced", "frontier", "unknown"]),
    costTier: z.enum(["$", "$$", "$$$", "$$$$", "internal", "unknown"]),
    speedTier: z.enum(["fast", "balanced", "deep", "unknown"]),
    reasoningTier: z.enum(["none", "light", "strong", "unknown"]),
    recommendedFor: z.array(z.string()),
    metadataSources: z.array(z.string()),
  })
  .passthrough();

export const ListAiEngineerModelsResponseSchema = z
  .object({
    default_model_id: z.string(),
    chat_title_generation: z
      .object({
        model_id: z.string().nullable(),
      })
      .optional(),
    models: z.array(AiEngineerModelOptionSchema),
    metadata: z
      .object({
        registrySource: z.literal("config"),
        metadataResolvers: z.array(z.string()),
        cached: z.boolean(),
        updatedAt: z.string(),
      })
      .passthrough(),
  })
  .passthrough();

export const RuntimeReasoningConfigSchema = z
  .object({
    enabled: z.boolean(),
    representation: z.enum(["reasoning", "reasoning_summary", "thinking"]),
    source: z.literal("provider_exposed"),
    providerOptions: z.record(z.unknown()),
  })
  .passthrough();

export const ResolvedRuntimeModelSchema = z
  .object({
    id: z.string(),
    providerType: ModelProviderTypeSchema,
    providerModelId: z.string(),
    apiKey: z.string().nullable(),
    baseUrl: z.string().nullable(),
    reasoning: RuntimeReasoningConfigSchema.nullable().optional(),
  })
  .passthrough();

export const ResolvedChatModelSchema = z
  .object({
    option: AiEngineerModelOptionSchema,
    runtime: ResolvedRuntimeModelSchema,
  })
  .passthrough();
