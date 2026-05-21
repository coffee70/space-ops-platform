import type { ToolSet } from "ai";

export type ExecutionMode = "read_only" | "suggest" | "execute" | "governed_execute";
export type ToolModePolicy = "disabled" | "requires_permission" | "enabled";
export type ToolPermissionStatus = "pending" | "approved" | "denied" | "executing" | "executed" | "failed" | "expired";

export interface ClientContext {
  current_application_id?: string;
  current_route?: string;
}

export interface ChatInputMessage {
  role: "user" | "assistant";
  content: string;
}

export interface ChatRequestBody {
  conversation_id: string;
  execution_mode?: ExecutionMode;
  model_id?: string | null;
  mission_id?: string | null;
  vehicle_id?: string | null;
  persisted_user_message_id?: string | null;
  messages: ChatInputMessage[];
  client_context?: ClientContext;
}

export interface ConversationInitialMessageBody {
  role: "user";
  content: string;
  metadata?: Record<string, unknown>;
}

export interface ConversationCreateBody {
  title?: string | null;
  mission_id?: string | null;
  vehicle_id?: string | null;
  execution_mode?: ExecutionMode;
  initial_message: ConversationInitialMessageBody;
}

export interface ConversationRecord {
  id: string;
  title: string | null;
  mission_id: string | null;
  vehicle_id: string | null;
  execution_mode: ExecutionMode;
  created_at: string;
  updated_at: string;
}

export interface ConversationMessageRecord {
  id: string;
  conversation_id: string;
  role: "user" | "assistant";
  content: string;
  metadata_json: Record<string, unknown>;
  created_at: string;
}

export interface ConversationDetail extends ConversationRecord {
  messages: ConversationMessageRecord[];
  events: PersistedEvent[];
}

export interface TraceEnvelope {
  conversation_id: string;
  agent_run_id: string;
  request_id: string;
  tool_call_id?: string | null;
}

export interface RetrievalPlan {
  documents: boolean;
  code: boolean;
  platform: boolean;
  tools: boolean;
  summary: string;
}

export interface RawEventFact {
  event_type: string;
  emitted_by: string;
  payload: Record<string, unknown>;
  tool_call_id?: string | null;
  created_at?: string;
}

export interface PersistedEvent {
  id: string;
  event_type: string;
  conversation_id: string | null;
  agent_run_id: string;
  request_id: string;
  tool_call_id: string | null;
  sequence: number;
  emitted_by: string;
  payload: Record<string, unknown>;
  created_at: string;
}

export interface EventChunk {
  kind: "event";
  event: PersistedEvent;
}

export type StreamChunk = EventChunk;

export interface ContextPacketResponse {
  conversation_id: string | null;
  agent_run_id: string;
  request_id: string;
  context_packet_id: string;
  document_chunk_count: number;
  code_chunk_count: number;
  platform_metadata_bytes: number;
  tool_definition_count: number;
  truncated: boolean;
  truncation_reasons: string[];
  failed_sources?: Array<{ service: string; failure_type: string }>;
  data: Record<string, unknown>;
  raw_events?: RawEventFact[];
}

export interface ToolDefinition {
  name: string;
  description: string;
  category: string;
  layer_target: string;
  read_write_classification: string;
  required_execution_mode: ExecutionMode;
  enabled: boolean;
  requires_confirmation: boolean;
  mode_policy_json?: Partial<Record<ExecutionMode, ToolModePolicy>>;
  permission_prompt_json?: Record<string, unknown>;
  input_schema_json: Record<string, unknown>;
  output_schema_json?: Record<string, unknown>;
  audit_policy_json?: Record<string, unknown>;
  redaction_policy_json?: Record<string, unknown>;
  backing_service?: string | null;
  backing_api?: string | null;
}

export interface ToolExecutionResponse {
  conversation_id: string | null;
  agent_run_id: string;
  request_id: string;
  tool_call_id: string;
  status: "completed" | "failed" | "confirmation_required" | "permission_required" | "permission_denied";
  output: unknown;
  raw_events?: RawEventFact[];
}

export interface ToolPermissionStatusResponse {
  permission_request_id: string;
  tool_call_id: string;
  status: ToolPermissionStatus;
  response_json?: Record<string, unknown> | null;
  raw_events?: RawEventFact[];
}

export interface ConversationStore {
  listConversations(): Promise<ConversationRecord[]>;
  createConversation(input: ConversationCreateBody): Promise<ConversationDetail>;
  getConversation(conversationId: string): Promise<ConversationDetail | null>;
  appendMessage(input: {
    conversationId: string;
    role: "user" | "assistant";
    content: string;
    metadata?: Record<string, unknown>;
  }): Promise<ConversationMessageRecord>;
  appendEvent(input: Omit<PersistedEvent, "id" | "created_at"> & { created_at?: string }): Promise<PersistedEvent>;
}

export interface ContextRetrievalClient {
  resolve(input: {
    trace: TraceEnvelope;
    message: string;
    mission_id?: string | null;
    vehicle_id?: string | null;
    execution_mode: ExecutionMode;
    retrieval_plan: RetrievalPlan;
  }): Promise<ContextPacketResponse>;
}

export interface ToolRegistryClient {
  listTools(trace: TraceEnvelope): Promise<ToolDefinition[]>;
}

export interface ToolExecutionClient {
  execute(input: {
    trace: TraceEnvelope;
    tool_name: string;
    input: Record<string, unknown>;
    execution_mode: ExecutionMode;
    confirmation_token?: string | null;
    permission_request_id?: string | null;
  }): Promise<ToolExecutionResponse>;
}

export interface ToolPermissionClient {
  waitForDecision(input: {
    permissionRequestId: string;
    abortSignal?: AbortSignal;
  }): Promise<{ status: "approved" | "denied"; reason?: string | null; raw_events?: RawEventFact[] }>;
}

export interface ModelStreamTextDelta {
  type: "text-delta";
  delta?: string;
  text?: string;
  textDelta?: string;
}

export interface ModelStreamToolCall {
  type: "tool-call";
  toolCallId: string;
  toolName: string;
}

export interface ModelStreamToolResult {
  type: "tool-result";
  toolCallId: string;
  toolName: string;
}

export interface ModelStreamStepFinish {
  type: "step-finish";
  finishReason: string;
  messageId: string;
}

export interface ModelStreamFinish {
  type: "finish";
  finishReason: string;
}

export interface ModelStreamAbort {
  type: "abort";
}

export interface ModelStreamReasoningDelta {
  type: "reasoning";
  delta?: string;
  text?: string;
  textDelta?: string;
  providerMetadata?: unknown;
}

export interface ModelStreamReasoningFinish {
  type: "reasoning-part-finish";
  providerMetadata?: unknown;
}

export type ModelStreamPart =
  | ModelStreamTextDelta
  | ModelStreamToolCall
  | ModelStreamToolResult
  | ModelStreamStepFinish
  | ModelStreamFinish
  | ModelStreamAbort
  | ModelStreamReasoningDelta
  | ModelStreamReasoningFinish
  | { type: "error"; error: unknown }
  | { type: string; [key: string]: unknown };

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
    currency: "USD" | "internal" | null;
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

export interface ModelRunner {
  stream(input: {
    system: string;
    messages: ChatInputMessage[];
    tools: ToolSet;
    maxSteps: number;
    model: ResolvedRuntimeModel;
    abortSignal?: AbortSignal;
  }): AsyncIterable<ModelStreamPart>;
}

export interface RuntimeConfig {
  port: number;
  databaseUrl: string;
  controlPlaneUrl: string;
  openAiApiKey: string | null;
  openAiBaseUrl: string | null;
  modelId: string;
  maxSteps: number;
  requestTimeoutMs: number;
  scriptedMode: string | null;
  allowMissingKeyFallback: boolean;
  nodeEnv: string | undefined;
  modelRegistryBaseUrl: string;
  openRouterApiKey: string | null;
  openRouterBaseUrl: string | null;
  modelMetadataCacheTtlSeconds: number | null;
  logModelStreamParts: boolean;
}

export interface RunDependencies {
  store: ConversationStore;
  contextClient: ContextRetrievalClient;
  toolRegistryClient: ToolRegistryClient;
  toolExecutionClient: ToolExecutionClient;
  toolPermissionClient: ToolPermissionClient;
  modelRunner: ModelRunner;
  modelCatalog: ModelCatalogPort;
  config: RuntimeConfig;
  now: () => Date;
  createId: () => string;
  /**
   * Optional override for the change-summary registry client. The default
   * implementation talks to the control-plane over HTTP. Tests inject a fake.
   */
  changeSummaryRegistryClient?: import("./change-summary.js").ChangeSummaryRegistryClient;
}
