import { Pool } from "pg";

import type {
  ConversationCreateBody,
  ConversationDetail,
  ConversationMessageRecord,
  ConversationRecord,
  ConversationUpdateBody,
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
    selected_model_id: (row.selected_model_id as string | null) ?? null,
    title_source: (row.title_source as ConversationRecord["title_source"]) ?? null,
    title_model_id: (row.title_model_id as string | null) ?? null,
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
    request_id: (row.request_id as string | null) ?? null,
    agent_run_id: (row.agent_run_id as string | null) ?? null,
    sequence: row.sequence === null || row.sequence === undefined ? null : Number(row.sequence),
    metadata_json: (row.metadata_json as Record<string, unknown>) ?? {},
    tool_permission_requests: [],
    created_at: new Date(String(row.created_at)).toISOString(),
  };
}

function mapEvent(row: Record<string, unknown>): PersistedEvent {
  return {
    id: String(row.id),
    conversation_id: (row.conversation_id as string | null) ?? null,
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

export class PgConversationStore implements ConversationStore {
  readonly #pool: Pool;

  constructor(connectionString: string) {
    this.#pool = new Pool({ connectionString });
  }

  async listConversations(): Promise<ConversationRecord[]> {
    const result = await this.#pool.query(
      `SELECT c.id::text, c.title, c.mission_id, c.vehicle_id, c.execution_mode, c.selected_model_id, c.title_source, c.title_model_id, c.created_at, c.updated_at
       FROM ai_conversations c
       WHERE EXISTS (
         SELECT 1 FROM ai_conversation_messages m WHERE m.conversation_id = c.id
       )
       ORDER BY c.updated_at DESC
       LIMIT 100`,
    );
    return result.rows.map((row) => mapConversation(row as Record<string, unknown>));
  }

  async createConversation(input: ConversationCreateBody): Promise<ConversationDetail> {
    const initialContent = input.initial_message.content.trim();
    if (initialContent.length === 0) {
      throw new Error("initial user message is required");
    }
    const id = crypto.randomUUID();
    const messageId = crypto.randomUUID();
    const client = await this.#pool.connect();
    try {
      await client.query("BEGIN");
      const conversationResult = await client.query(
        `INSERT INTO ai_conversations (id, title, created_by, mission_id, vehicle_id, execution_mode, selected_model_id, title_source, created_at, updated_at)
         VALUES ($1::uuid, $2, NULL, $3, $4, $5, $6, $7, now(), now())
         RETURNING id::text, title, mission_id, vehicle_id, execution_mode, selected_model_id, title_source, title_model_id, created_at, updated_at`,
        [
          id,
          input.title ?? null,
          input.mission_id ?? null,
          input.vehicle_id ?? null,
          input.execution_mode ?? "read_only",
          input.selected_model_id ?? null,
          input.title ? "manual" : "initial",
        ],
      );
      const messageResult = await client.query(
        `INSERT INTO ai_conversation_messages (id, conversation_id, role, content, request_id, agent_run_id, sequence, metadata_json, created_at)
         VALUES ($1::uuid, $2::uuid, 'user', $3, NULL, NULL, NULL, $4::jsonb, now())
         RETURNING id::text, conversation_id::text, role, content, request_id::text, agent_run_id::text, sequence, metadata_json, created_at`,
        [messageId, id, initialContent, JSON.stringify(input.initial_message.metadata ?? {})],
      );
      await client.query("COMMIT");
      return {
        ...mapConversation(conversationResult.rows[0]),
        messages: [mapMessage(messageResult.rows[0])],
        events: [],
      };
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    } finally {
      client.release();
    }
  }

  async getConversation(conversationId: string): Promise<ConversationDetail | null> {
    const conversationResult = await this.#pool.query(
      `SELECT c.id::text, c.title, c.mission_id, c.vehicle_id, c.execution_mode, c.selected_model_id, c.title_source, c.title_model_id, c.created_at, c.updated_at
       FROM ai_conversations c
       WHERE c.id = $1::uuid
         AND EXISTS (
           SELECT 1 FROM ai_conversation_messages m WHERE m.conversation_id = c.id
         )`,
      [conversationId],
    );

    if (conversationResult.rowCount === 0) {
      return null;
    }

    const messagesResult = await this.#pool.query(
      `SELECT id::text, conversation_id::text, role, content, request_id::text, agent_run_id::text, sequence, metadata_json, created_at
       FROM ai_conversation_messages
       WHERE conversation_id = $1::uuid
       ORDER BY created_at ASC`,
      [conversationId],
    );

    const permissionsResult = await this.#pool.query(
      `SELECT
         id::text AS permission_request_id,
         assistant_message_id::text,
         tool_call_id::text,
         tool_name,
         status,
         prompt_json,
         response_json,
         created_at
       FROM ai_tool_permission_requests
       WHERE conversation_id = $1::uuid
         AND assistant_message_id IS NOT NULL
       ORDER BY created_at ASC`,
      [conversationId],
    );
    const permissionsByMessage = new Map<string, NonNullable<ConversationMessageRecord["tool_permission_requests"]>>();
    for (const row of permissionsResult.rows as Array<Record<string, unknown>>) {
      const assistantMessageId = String(row.assistant_message_id);
      const existing = permissionsByMessage.get(assistantMessageId) ?? [];
      existing.push({
        permission_request_id: String(row.permission_request_id),
        tool_call_id: String(row.tool_call_id),
        tool_name: String(row.tool_name),
        status: String(row.status) as NonNullable<ConversationMessageRecord["tool_permission_requests"]>[number]["status"],
        prompt: (row.prompt_json as Record<string, unknown>) ?? {},
        response: (row.response_json as Record<string, unknown> | null) ?? null,
      });
      permissionsByMessage.set(assistantMessageId, existing);
    }

    const eventsResult = await this.#pool.query(
      `SELECT id::text, conversation_id::text, agent_run_id::text, request_id::text, tool_call_id::text, sequence, emitted_by, event_type, payload_json, created_at
       FROM ai_agent_events
       WHERE conversation_id = $1::uuid
       ORDER BY created_at ASC, sequence ASC`,
      [conversationId],
    );

    return {
      ...mapConversation(conversationResult.rows[0]),
      messages: messagesResult.rows.map((row) => {
        const message = mapMessage(row as Record<string, unknown>);
        return {
          ...message,
          tool_permission_requests: permissionsByMessage.get(message.id) ?? [],
        };
      }),
      events: eventsResult.rows.map((row) => mapEvent(row as Record<string, unknown>)),
    };
  }

  async updateConversation(conversationId: string, input: ConversationUpdateBody): Promise<ConversationDetail | null> {
    const assignments: string[] = [];
    const values: unknown[] = [];
    const add = (sql: string, value: unknown) => {
      values.push(value);
      assignments.push(`${sql} = $${values.length}`);
    };

    if ("title" in input) add("title", input.title ?? null);
    if ("execution_mode" in input) add("execution_mode", input.execution_mode);
    if ("selected_model_id" in input) add("selected_model_id", input.selected_model_id ?? null);
    if ("title_source" in input) add("title_source", input.title_source ?? null);
    if ("title_model_id" in input) add("title_model_id", input.title_model_id ?? null);

    if (assignments.length > 0) {
      values.push(conversationId);
      await this.#pool.query(
        `UPDATE ai_conversations
         SET ${assignments.join(", ")}, updated_at = now()
         WHERE id = $${values.length}::uuid`,
        values,
      );
    }

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
    const id = crypto.randomUUID();
    const result = await this.#pool.query(
      `INSERT INTO ai_conversation_messages (id, conversation_id, role, content, request_id, agent_run_id, sequence, metadata_json, created_at)
       VALUES ($1::uuid, $2::uuid, $3, $4, $5::uuid, $6::uuid, $7, $8::jsonb, now())
       RETURNING id::text, conversation_id::text, role, content, request_id::text, agent_run_id::text, sequence, metadata_json, created_at`,
      [
        id,
        input.conversationId,
        input.role,
        input.content,
        input.requestId ?? null,
        input.agentRunId ?? null,
        input.sequence ?? null,
        JSON.stringify(input.metadata ?? {}),
      ],
    );
    await this.#pool.query("UPDATE ai_conversations SET updated_at = now() WHERE id = $1::uuid", [input.conversationId]);
    return mapMessage(result.rows[0]);
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
    const assignments: string[] = [];
    const values: unknown[] = [];
    const add = (sql: string, value: unknown) => {
      values.push(value);
      assignments.push(`${sql} = $${values.length}`);
    };
    if ("content" in input) add("content", input.content ?? "");
    if ("requestId" in input) add("request_id", input.requestId ?? null);
    if ("agentRunId" in input) add("agent_run_id", input.agentRunId ?? null);
    if ("sequence" in input) add("sequence", input.sequence ?? null);
    if ("metadata" in input) add("metadata_json", JSON.stringify(input.metadata ?? {}));
    if (assignments.length === 0) {
      const existing = await this.#pool.query(
        `SELECT id::text, conversation_id::text, role, content, request_id::text, agent_run_id::text, sequence, metadata_json, created_at
         FROM ai_conversation_messages
         WHERE id = $1::uuid`,
        [messageId],
      );
      return existing.rowCount ? mapMessage(existing.rows[0]) : null;
    }
    values.push(messageId);
    const result = await this.#pool.query(
      `UPDATE ai_conversation_messages
       SET ${assignments.join(", ")}
       WHERE id = $${values.length}::uuid
       RETURNING id::text, conversation_id::text, role, content, request_id::text, agent_run_id::text, sequence, metadata_json, created_at`,
      values,
    );
    if (result.rowCount === 0) return null;
    await this.#pool.query("UPDATE ai_conversations SET updated_at = now() WHERE id = $1::uuid", [result.rows[0].conversation_id]);
    return mapMessage(result.rows[0]);
  }

  async deleteMessage(messageId: string): Promise<void> {
    await this.#pool.query("DELETE FROM ai_conversation_messages WHERE id = $1::uuid", [messageId]);
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

    return mapEvent(result.rows[0] as Record<string, unknown>);
  }
}
