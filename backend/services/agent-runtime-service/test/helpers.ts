import type {
  ContextPacketResponse,
  ContextRetrievalClient,
  ConversationCreateBody,
  ConversationDetail,
  ConversationMessageRecord,
  ConversationRecord,
  ConversationUpdateBody,
  ConversationStore,
  ExecutionMode,
  ModelRunner,
  ModelCatalogPort,
  AiEngineerModelOption,
  ListAiEngineerModelsResponse,
  ResolvedChatModel,
  ModelStreamPart,
  PersistedEvent,
  RawEventFact,
  RetrievalPlan,
  RuntimeConfig,
  ToolDefinition,
  ToolExecutionClient,
  ToolExecutionResponse,
  ToolRegistryClient,
  TraceEnvelope,
} from "../src/types.js";

export function baseRuntimeConfig(overrides: Partial<RuntimeConfig> = {}): RuntimeConfig {
  return {
    port: 8080,
    databaseUrl: "postgres://example",
    controlPlaneUrl: "http://localhost:8100",
    openAiApiKey: null,
    openAiBaseUrl: null,
    modelId: "gpt-4o-mini",
    titleGenerationModelId: null,
    maxSteps: 5,
    requestTimeoutMs: 1000,
    scriptedMode: null,
    allowMissingKeyFallback: true,
    nodeEnv: "test",
    modelRegistryBaseUrl: "http://model-registry-service:8080",
    openRouterApiKey: null,
    openRouterBaseUrl: null,
    modelMetadataCacheTtlSeconds: null,
    logModelStreamParts: false,
    ...overrides,
  };
}

export function modelOption(overrides: Partial<AiEngineerModelOption> = {}): AiEngineerModelOption {
  return {
    id: "openai-gpt-5-1-mini",
    providerRef: "openai-main",
    providerType: "openai",
    providerModelId: "gpt-5.1-mini",
    name: "GPT-5.1 Mini",
    provider: "OpenAI",
    description: null,
    enabled: true,
    isAvailable: true,
    disabledReason: null,
    isDefault: true,
    defaultFor: ["chat", "fast"],
    governance: {
      allowedModes: ["read_only", "suggest", "execute"],
      dataBoundary: "external_api",
    },
    contextWindow: null,
    maxOutputTokens: null,
    inputModalities: ["text"],
    outputModalities: ["text"],
    supportedParameters: [],
    capabilities: ["text", "tool-use"],
    pricing: {
      inputPerMillionTokens: null,
      outputPerMillionTokens: null,
      currency: "USD",
    },
    qualityTier: "advanced",
    costTier: "$",
    speedTier: "fast",
    reasoningTier: "light",
    recommendedFor: ["fast-chat"],
    metadataSources: ["test"],
    ...overrides,
  };
}

export class FakeModelCatalog implements ModelCatalogPort {
  constructor(
    private readonly response: ListAiEngineerModelsResponse = {
      default_model_id: "openai-gpt-5-1-mini",
      models: [modelOption()],
      metadata: {
        registrySource: "config",
        metadataResolvers: ["test"],
        cached: true,
        updatedAt: new Date(0).toISOString(),
      },
    },
    private readonly resolver: (modelId: string | null | undefined, executionMode: ExecutionMode) => Promise<ResolvedChatModel> | ResolvedChatModel = (
      modelId,
      _executionMode,
    ) => {
      const option = this.response.models.find((candidate) => candidate.id === (modelId ?? this.response.default_model_id));
      if (!option) throw new Error(`unknown model: ${modelId ?? ""}`);
      return {
        option,
        runtime: {
          id: option.id,
          providerType: option.providerType,
          providerModelId: option.providerModelId,
          apiKey: "test-key",
          baseUrl: null,
        },
      };
    },
  ) {}

  async listModelsResponse(): Promise<ListAiEngineerModelsResponse> {
    return this.response;
  }

  async resolveForChat(modelId: string | null | undefined, executionMode: ExecutionMode): Promise<ResolvedChatModel> {
    return this.resolver(modelId, executionMode);
  }
}

export class MemoryConversationStore implements ConversationStore {
  conversations = new Map<string, ConversationDetail>();
  events: PersistedEvent[] = [];

  async listConversations(): Promise<ConversationRecord[]> {
    return [...this.conversations.values()]
      .filter((conversation) => conversation.messages.length > 0)
      .map(({ messages: _messages, events: _events, ...conversation }) => conversation);
  }

  async createConversation(input: ConversationCreateBody): Promise<ConversationDetail> {
    const initialContent = input.initial_message.content.trim();
    if (initialContent.length === 0) {
      throw new Error("initial user message is required");
    }
    const now = new Date().toISOString();
    const conversation: ConversationDetail = {
      id: crypto.randomUUID(),
      title: input.title ?? null,
      mission_id: input.mission_id ?? null,
      vehicle_id: input.vehicle_id ?? null,
      execution_mode: input.execution_mode ?? "read_only",
      selected_model_id: input.selected_model_id ?? null,
      title_source: input.title ? "manual" : "initial",
      title_model_id: null,
      created_at: now,
      updated_at: now,
      events: [],
      messages: [
        {
          id: crypto.randomUUID(),
          conversation_id: "",
          role: "user",
          content: initialContent,
          request_id: null,
          agent_run_id: null,
          sequence: null,
          metadata_json: input.initial_message.metadata ?? {},
          tool_permission_requests: [],
          created_at: now,
        },
      ],
    };
    conversation.messages[0].conversation_id = conversation.id;
    this.conversations.set(conversation.id, conversation);
    return structuredClone(conversation);
  }

  async getConversation(conversationId: string): Promise<ConversationDetail | null> {
    const conversation = this.conversations.get(conversationId);
    if (!conversation || conversation.messages.length === 0) return null;
    const events = this.events
      .filter((event) => event.conversation_id === conversationId)
      .sort((left, right) => left.created_at.localeCompare(right.created_at) || left.sequence - right.sequence);
    return structuredClone({
      ...conversation,
      events,
    });
  }

  async updateConversation(conversationId: string, input: ConversationUpdateBody): Promise<ConversationDetail | null> {
    const conversation = this.conversations.get(conversationId);
    if (!conversation) return null;
    if ("title" in input) conversation.title = input.title ?? null;
    if ("execution_mode" in input && input.execution_mode) conversation.execution_mode = input.execution_mode;
    if ("selected_model_id" in input) conversation.selected_model_id = input.selected_model_id ?? null;
    if ("title_source" in input) conversation.title_source = input.title_source ?? null;
    if ("title_model_id" in input) conversation.title_model_id = input.title_model_id ?? null;
    conversation.updated_at = new Date().toISOString();
    return this.getConversation(conversationId);
  }

  async appendMessage(input: {
    conversationId: string;
    role: "user" | "assistant" | "tool";
    content: string;
    requestId?: string | null;
    agentRunId?: string | null;
    sequence?: number | null;
    metadata?: Record<string, unknown>;
  }): Promise<ConversationMessageRecord> {
    const conversation = this.conversations.get(input.conversationId);
    if (!conversation) {
      throw new Error("conversation not found");
    }
    const record: ConversationMessageRecord = {
      id: crypto.randomUUID(),
      conversation_id: input.conversationId,
      role: input.role,
      content: input.content,
      request_id: input.requestId ?? null,
      agent_run_id: input.agentRunId ?? null,
      sequence: input.sequence ?? null,
      metadata_json: input.metadata ?? {},
      tool_permission_requests: [],
      created_at: new Date().toISOString(),
    };
    conversation.messages.push(record);
    conversation.updated_at = new Date().toISOString();
    return structuredClone(record);
  }

  async updateMessage(
    messageId: string,
    input: {
      content?: string;
      requestId?: string | null;
      agentRunId?: string | null;
      sequence?: number | null;
      metadata?: Record<string, unknown>;
    },
  ): Promise<ConversationMessageRecord | null> {
    for (const conversation of this.conversations.values()) {
      const message = conversation.messages.find((candidate) => candidate.id === messageId);
      if (!message) continue;
      if ("content" in input) message.content = input.content ?? "";
      if ("requestId" in input) message.request_id = input.requestId ?? null;
      if ("agentRunId" in input) message.agent_run_id = input.agentRunId ?? null;
      if ("sequence" in input) message.sequence = input.sequence ?? null;
      if ("metadata" in input) message.metadata_json = input.metadata ?? {};
      conversation.updated_at = new Date().toISOString();
      return structuredClone(message);
    }
    return null;
  }

  async deleteMessage(messageId: string): Promise<void> {
    for (const conversation of this.conversations.values()) {
      const index = conversation.messages.findIndex((candidate) => candidate.id === messageId);
      if (index < 0) continue;
      conversation.messages.splice(index, 1);
      conversation.updated_at = new Date().toISOString();
      return;
    }
  }

  async appendEvent(input: Omit<PersistedEvent, "id" | "created_at"> & { created_at?: string }): Promise<PersistedEvent> {
    const record: PersistedEvent = {
      id: crypto.randomUUID(),
      created_at: input.created_at ?? new Date().toISOString(),
      ...input,
    };
    this.events.push(record);
    return structuredClone(record);
  }
}

export class FakeContextClient implements ContextRetrievalClient {
  calls: Array<{
    message: string;
    mission_id?: string | null;
    vehicle_id?: string | null;
    execution_mode: ExecutionMode;
    retrieval_plan: RetrievalPlan;
  }> = [];

  constructor(private readonly rawEvents: RawEventFact[] = []) {}

  async resolve(input: {
    trace: TraceEnvelope;
    message: string;
    mission_id?: string | null;
    vehicle_id?: string | null;
    execution_mode: ExecutionMode;
    retrieval_plan: RetrievalPlan;
  }): Promise<ContextPacketResponse> {
    this.calls.push({
      message: input.message,
      mission_id: input.mission_id,
      vehicle_id: input.vehicle_id,
      execution_mode: input.execution_mode,
      retrieval_plan: input.retrieval_plan,
    });

    return {
      conversation_id: input.trace.conversation_id,
      agent_run_id: input.trace.agent_run_id,
      request_id: input.trace.request_id,
      context_packet_id: "ctx-1",
      document_chunk_count: 0,
      code_chunk_count: 0,
      platform_metadata_bytes: 0,
      tool_definition_count: 0,
      truncated: false,
      truncation_reasons: [],
      data: {
        mission_documents: [],
        code_context: [],
        platform_context: {},
        runtime_context: {},
        tool_context: [],
      },
      raw_events: this.rawEvents,
    };
  }
}

export function contextResolvedEvent(contextPacketId = "ctx-1"): RawEventFact {
  return {
    event_type: "context.resolved",
    emitted_by: "context-retrieval-service",
    payload: {
      context_packet_id: contextPacketId,
      document_chunk_count: 0,
      code_chunk_count: 0,
      platform_metadata_bytes: 0,
      tool_definition_count: 0,
      truncated: false,
    },
  };
}

export class FakeToolRegistryClient implements ToolRegistryClient {
  traces: TraceEnvelope[] = [];

  constructor(private readonly definitions: ToolDefinition[]) {}

  async listTools(trace: TraceEnvelope): Promise<ToolDefinition[]> {
    this.traces.push(trace);
    return this.definitions;
  }
}

export class FakeToolExecutionClient implements ToolExecutionClient {
  calls: Array<{
    tool_name: string;
    assistant_message_id: string;
    input: Record<string, unknown>;
    trace: TraceEnvelope;
    execution_mode: ExecutionMode;
    permission_request_id?: string | null;
  }> = [];

  constructor(
    private readonly response:
      | ToolExecutionResponse
      | ((input: {
          trace: TraceEnvelope;
          assistant_message_id: string;
          tool_name: string;
          input: Record<string, unknown>;
          execution_mode: ExecutionMode;
          confirmation_token?: string | null;
          permission_request_id?: string | null;
        }) => Promise<ToolExecutionResponse> | ToolExecutionResponse),
  ) {}

  async execute(input: {
    trace: TraceEnvelope;
    assistant_message_id: string;
    tool_name: string;
    input: Record<string, unknown>;
    execution_mode: ExecutionMode;
    confirmation_token?: string | null;
    permission_request_id?: string | null;
  }): Promise<ToolExecutionResponse> {
    this.calls.push({
      tool_name: input.tool_name,
      assistant_message_id: input.assistant_message_id,
      input: input.input,
      trace: input.trace,
      execution_mode: input.execution_mode,
      permission_request_id: input.permission_request_id,
    });
    return typeof this.response === "function" ? await this.response(input) : this.response;
  }
}

export function parseNdjson(body: string): Array<Record<string, unknown>> {
  return body
    .trim()
    .split("\n")
    .filter((line) => line.length > 0)
    .map((line) => JSON.parse(line) as Record<string, unknown>);
}

export function createStaticModelRunner(parts: Array<ModelStreamPart>): ModelRunner {
  return {
    async *stream(input) {
      void input.model;
      for (const part of parts) {
        yield part;
      }
    },
  };
}
