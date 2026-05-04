import type {
  ContextPacketResponse,
  ContextRetrievalClient,
  ConversationCreateBody,
  ConversationDetail,
  ConversationMessageRecord,
  ConversationRecord,
  ConversationStore,
  ExecutionMode,
  ModelRunner,
  ModelStreamPart,
  PersistedEvent,
  RawEventFact,
  ToolDefinition,
  ToolExecutionClient,
  ToolExecutionResponse,
  ToolRegistryClient,
  TraceEnvelope,
} from "../src/types.js";

export class MemoryConversationStore implements ConversationStore {
  conversations = new Map<string, ConversationDetail>();
  events: PersistedEvent[] = [];

  async listConversations(): Promise<ConversationRecord[]> {
    return [...this.conversations.values()].map(({ messages: _messages, ...conversation }) => conversation);
  }

  async createConversation(input: ConversationCreateBody): Promise<ConversationRecord> {
    const now = new Date().toISOString();
    const conversation: ConversationDetail = {
      id: crypto.randomUUID(),
      title: input.title ?? null,
      mission_id: input.mission_id ?? null,
      vehicle_id: input.vehicle_id ?? null,
      execution_mode: input.execution_mode ?? "read_only",
      created_at: now,
      updated_at: now,
      messages: [],
    };
    this.conversations.set(conversation.id, conversation);
    const { messages: _messages, ...record } = conversation;
    return record;
  }

  async getConversation(conversationId: string): Promise<ConversationDetail | null> {
    const conversation = this.conversations.get(conversationId);
    return conversation ? structuredClone(conversation) : null;
  }

  async appendMessage(input: {
    conversationId: string;
    role: "user" | "assistant";
    content: string;
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
      metadata_json: input.metadata ?? {},
      created_at: new Date().toISOString(),
    };
    conversation.messages.push(record);
    conversation.updated_at = new Date().toISOString();
    return structuredClone(record);
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
  constructor(private readonly rawEvents: RawEventFact[] = []) {}

  async resolve(input: {
    trace: TraceEnvelope;
    message: string;
    mission_id?: string | null;
    vehicle_id?: string | null;
    execution_mode: ExecutionMode;
  }): Promise<ContextPacketResponse> {
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
  calls: Array<{ tool_name: string; input: Record<string, unknown>; trace: TraceEnvelope; execution_mode: ExecutionMode }> = [];

  constructor(
    private readonly response:
      | ToolExecutionResponse
      | ((input: {
          trace: TraceEnvelope;
          tool_name: string;
          input: Record<string, unknown>;
          execution_mode: ExecutionMode;
          message_id?: string | null;
          confirmation_token?: string | null;
        }) => Promise<ToolExecutionResponse> | ToolExecutionResponse),
  ) {}

  async execute(input: {
    trace: TraceEnvelope;
    tool_name: string;
    input: Record<string, unknown>;
    execution_mode: ExecutionMode;
    message_id?: string | null;
    confirmation_token?: string | null;
  }): Promise<ToolExecutionResponse> {
    this.calls.push({
      tool_name: input.tool_name,
      input: input.input,
      trace: input.trace,
      execution_mode: input.execution_mode,
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
    async *stream() {
      for (const part of parts) {
        yield part;
      }
    },
  };
}
