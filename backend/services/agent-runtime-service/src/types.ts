export type ExecutionMode = "read_only" | "suggest" | "execute" | "governed_execute";

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
  mission_id?: string | null;
  vehicle_id?: string | null;
  messages: ChatInputMessage[];
  client_context?: ClientContext;
}

export interface ConversationCreateBody {
  title?: string | null;
  mission_id?: string | null;
  vehicle_id?: string | null;
  execution_mode?: ExecutionMode;
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
  conversation_id: string;
  agent_run_id: string;
  request_id: string;
  tool_call_id?: string | null;
  sequence: number;
  emitted_by: string;
  payload: Record<string, unknown>;
  created_at: string;
}

export interface MessageDeltaChunk {
  kind: "message.delta";
  conversation_id: string;
  agent_run_id: string;
  request_id: string;
  message_id: string | null;
  sequence: number;
  delta: string;
  created_at: string;
}

export interface EventChunk {
  kind: "event";
  event: PersistedEvent;
}

export type StreamChunk = EventChunk | MessageDeltaChunk;

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
  status: "completed" | "failed" | "confirmation_required";
  output: Record<string, unknown>;
  raw_events?: RawEventFact[];
}

export interface ConversationStore {
  listConversations(): Promise<ConversationRecord[]>;
  createConversation(input: ConversationCreateBody): Promise<ConversationRecord>;
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
  listTools(): Promise<ToolDefinition[]>;
}

export interface ToolExecutionClient {
  execute(input: {
    trace: TraceEnvelope;
    tool_name: string;
    input: Record<string, unknown>;
    execution_mode: ExecutionMode;
    message_id?: string | null;
  }): Promise<ToolExecutionResponse>;
}

export interface ModelStreamTextDelta {
  type: "text-delta";
  textDelta: string;
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

export type ModelStreamPart =
  | ModelStreamTextDelta
  | ModelStreamToolCall
  | ModelStreamToolResult
  | ModelStreamStepFinish
  | ModelStreamFinish
  | { type: string; [key: string]: unknown };

export interface ModelRunner {
  stream(input: {
    system: string;
    messages: ChatInputMessage[];
    tools: ToolSet;
    maxSteps: number;
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
}

export interface RunDependencies {
  store: ConversationStore;
  contextClient: ContextRetrievalClient;
  toolRegistryClient: ToolRegistryClient;
  toolExecutionClient: ToolExecutionClient;
  modelRunner: ModelRunner;
  config: RuntimeConfig;
  now: () => Date;
  createId: () => string;
}
import type { ToolSet } from "ai";
