export type ExecutionMode = "read_only" | "suggest" | "execute" | "governed_execute";

export type ModelProviderType =
  | "openai"
  | "anthropic"
  | "openai-compatible"
  | "google"
  | "azure-openai"
  | "bedrock"
  | "vertex"
  | "vercel-gateway";

export type ModelDataBoundary = "external_api" | "private_cloud" | "local_airgapped" | "unknown";

export type ModelCapability =
  | "text"
  | "vision"
  | "tool-use"
  | "reasoning"
  | "json"
  | "file-input"
  | "web-search"
  | "code";

export type ReasoningStreamRepresentation = "reasoning" | "reasoning_summary" | "thinking";

export type RuntimeReasoningConfig = {
  enabled: boolean;
  representation: ReasoningStreamRepresentation;
  source: "provider_exposed";
  providerOptions: Record<string, unknown>;
};

export type ModelRegistryReasoningConfig = {
  enabled: boolean;
  representation: ReasoningStreamRepresentation;
  providerOptions?: Record<string, unknown>;
};

export type ModelRegistryProvider = {
  id: string;
  type: ModelProviderType;
  displayName: string;
  apiKeyEnv?: string;
  baseUrl?: string;
};

export type ModelMetadata = {
  displayName: string;
  providerDisplayName: string;
  description: string | null;
  contextWindow: number | null;
  maxOutputTokens: number | null;
  inputModalities: string[];
  outputModalities: string[];
  supportedParameters: string[];
  capabilities: ModelCapability[];
  pricing: {
    inputPerMillionTokens: number | null;
    outputPerMillionTokens: number | null;
    currency: "USD" | "internal";
  };
  qualityTier: "standard" | "advanced" | "frontier" | "unknown";
  costTier: "$" | "$$" | "$$$" | "$$$$" | "internal" | "unknown";
  speedTier: "fast" | "balanced" | "deep" | "unknown";
  reasoningTier: "none" | "light" | "strong" | "unknown";
  metadataSources: string[];
};

export type ModelRegistryEntry = {
  id: string;
  providerRef: string;
  providerModelId: string;
  enabled: boolean;
  disabledReason?: string;
  defaultFor?: string[];
  governance?: {
    allowedModes?: ExecutionMode[];
    dataBoundary?: ModelDataBoundary;
  };
  reasoning?: ModelRegistryReasoningConfig;
  metadataOverrides?: Partial<ModelMetadata> & {
    recommendedFor?: string[];
  };
};

export type AiEngineerModelOption = {
  id: string;
  providerRef: string;
  providerType: ModelProviderType;
  providerModelId: string;
  name: string;
  provider: string;
  description: string | null;
  enabled: boolean;
  isAvailable: boolean;
  disabledReason: string | null;
  isDefault: boolean;
  defaultFor: string[];
  governance: {
    allowedModes: ExecutionMode[];
    dataBoundary: ModelDataBoundary;
  };
  contextWindow: number | null;
  maxOutputTokens: number | null;
  inputModalities: string[];
  outputModalities: string[];
  supportedParameters: string[];
  capabilities: ModelCapability[];
  pricing: ModelMetadata["pricing"];
  qualityTier: ModelMetadata["qualityTier"];
  costTier: ModelMetadata["costTier"];
  speedTier: ModelMetadata["speedTier"];
  reasoningTier: ModelMetadata["reasoningTier"];
  recommendedFor: string[];
  metadataSources: string[];
};

export type ResolvedRuntimeModel = {
  id: string;
  providerType: ModelProviderType;
  providerModelId: string;
  apiKey: string | null;
  baseUrl: string | null;
  reasoning?: RuntimeReasoningConfig | null;
};

export type ResolvedChatModel = {
  option: AiEngineerModelOption;
  runtime: ResolvedRuntimeModel;
};

export type ListAiEngineerModelsResponse = {
  default_model_id: string;
  models: AiEngineerModelOption[];
  metadata: {
    registrySource: "config";
    metadataResolvers: string[];
    cached: boolean;
    updatedAt: string;
  };
};

export interface ModelCatalogPort {
  listModelsResponse(): Promise<ListAiEngineerModelsResponse>;
  resolveForChat(modelId: string | null | undefined, executionMode: ExecutionMode): Promise<ResolvedChatModel>;
}

export interface RuntimeConfig {
  port: number;
  openAiApiKey: string | null;
  openAiBaseUrl: string | null;
  // Used for "default resolution" when request does not specify an explicit model id.
  modelId: string;
  requestTimeoutMs: number;
  nodeEnv: string | undefined;
  modelsConfigPath: string | null;
  openRouterApiKey: string | null;
  openRouterBaseUrl: string | null;
  modelMetadataCacheTtlSeconds: number | null;
}

