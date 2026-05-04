import { Pool } from "pg";

import type {
  ConversationCreateBody,
  ConversationDetail,
  ConversationMessageRecord,
  ConversationRecord,
  ConversationStore,
  PersistedEvent,
} from "../types.js";

function mapConversation(row: Record<string, unknown>): ConversationRecord {
  return {
    id: String(row.id),
    title: (row.title as string | null) ?? null,
    mission_id: (row.mission_id as string | null) ?? null,
    vehicle_id: (row.vehicle_id as string | null) ?? null,
    execution_mode: String(row.execution_mode) as ConversationRecord["execution_mode"],
    created_at: new Date(String(row.created_at)).toISOString(),
    updated_at: new Date(String(row.updated_at)).toISOString(),
  };
}

function mapMessage(row: Record<string, unknown>): ConversationMessageRecord {
  return {
    id: String(row.id),
    conversation_id: String(row.conversation_id),
    role: String(row.role) as ConversationMessageRecord["role"],
    content: String(row.content),
    metadata_json: (row.metadata_json as Record<string, unknown>) ?? {},
    created_at: new Date(String(row.created_at)).toISOString(),
  };
}

export class PgConversationStore implements ConversationStore {
  readonly #pool: Pool;

  constructor(connectionString: string) {
    this.#pool = new Pool({ connectionString });
  }

  async listConversations(): Promise<ConversationRecord[]> {
    const result = await this.#pool.query(
      "SELECT id::text, title, mission_id, vehicle_id, execution_mode, created_at, updated_at FROM ai_conversations ORDER BY created_at DESC LIMIT 100",
    );
    return result.rows.map((row) => mapConversation(row as Record<string, unknown>));
  }

  async createConversation(input: ConversationCreateBody): Promise<ConversationRecord> {
    const id = crypto.randomUUID();
    const result = await this.#pool.query(
      `INSERT INTO ai_conversations (id, title, created_by, mission_id, vehicle_id, execution_mode, created_at, updated_at)
       VALUES ($1::uuid, $2, NULL, $3, $4, $5, now(), now())
       RETURNING id::text, title, mission_id, vehicle_id, execution_mode, created_at, updated_at`,
      [id, input.title ?? null, input.mission_id ?? null, input.vehicle_id ?? null, input.execution_mode ?? "read_only"],
    );
    return mapConversation(result.rows[0]);
  }

  async getConversation(conversationId: string): Promise<ConversationDetail | null> {
    const conversationResult = await this.#pool.query(
      "SELECT id::text, title, mission_id, vehicle_id, execution_mode, created_at, updated_at FROM ai_conversations WHERE id = $1::uuid",
      [conversationId],
    );

    if (conversationResult.rowCount === 0) {
      return null;
    }

    const messagesResult = await this.#pool.query(
      `SELECT id::text, conversation_id::text, role, content, metadata_json, created_at
       FROM ai_conversation_messages
       WHERE conversation_id = $1::uuid
       ORDER BY created_at ASC`,
      [conversationId],
    );

    return {
      ...mapConversation(conversationResult.rows[0]),
      messages: messagesResult.rows.map((row) => mapMessage(row as Record<string, unknown>)),
    };
  }

  async appendMessage(input: {
    conversationId: string;
    role: "user" | "assistant";
    content: string;
    metadata?: Record<string, unknown>;
  }): Promise<ConversationMessageRecord> {
    const id = crypto.randomUUID();
    const result = await this.#pool.query(
      `INSERT INTO ai_conversation_messages (id, conversation_id, role, content, metadata_json, created_at)
       VALUES ($1::uuid, $2::uuid, $3, $4, $5::jsonb, now())
       RETURNING id::text, conversation_id::text, role, content, metadata_json, created_at`,
      [id, input.conversationId, input.role, input.content, JSON.stringify(input.metadata ?? {})],
    );
    await this.#pool.query("UPDATE ai_conversations SET updated_at = now() WHERE id = $1::uuid", [input.conversationId]);
    return mapMessage(result.rows[0]);
  }

  async appendEvent(input: Omit<PersistedEvent, "id" | "created_at"> & { created_at?: string }): Promise<PersistedEvent> {
    const id = crypto.randomUUID();
    const createdAt = input.created_at ?? new Date().toISOString();
    const result = await this.#pool.query(
      `INSERT INTO ai_agent_events (
         id, conversation_id, agent_run_id, request_id, tool_call_id, sequence, emitted_by, event_type, payload_json, created_at
       ) VALUES (
         $1::uuid, $2::uuid, $3::uuid, $4::uuid, $5::uuid, $6, $7, $8, $9::jsonb, $10::timestamptz
       )
       RETURNING id::text, conversation_id::text, agent_run_id::text, request_id::text, tool_call_id::text, sequence, emitted_by, event_type, payload_json, created_at`,
      [
        id,
        input.conversation_id,
        input.agent_run_id,
        input.request_id,
        input.tool_call_id ?? null,
        input.sequence,
        input.emitted_by,
        input.event_type,
        JSON.stringify(input.payload),
        createdAt,
      ],
    );

    const row = result.rows[0];
    return {
      id: String(row.id),
      conversation_id: String(row.conversation_id),
      agent_run_id: String(row.agent_run_id),
      request_id: String(row.request_id),
      tool_call_id: (row.tool_call_id as string | null) ?? null,
      sequence: Number(row.sequence),
      emitted_by: String(row.emitted_by),
      event_type: String(row.event_type),
      payload: (row.payload_json as Record<string, unknown>) ?? {},
      created_at: new Date(String(row.created_at)).toISOString(),
    };
  }
}
