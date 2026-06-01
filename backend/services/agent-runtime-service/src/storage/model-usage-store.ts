import { Pool } from "pg";
import { z } from "zod";

import { LanguageModelUsageSnapshotSchema, type LanguageModelUsageSnapshot } from "../ai/model-usage.js";

export const PersistedModelUsageRecordSchema = z
  .object({
    conversation_id: z.string().uuid().nullable().optional(),
    agent_run_id: z.string().uuid(),
    request_id: z.string().nullable().optional(),
    step_index: z.number().int().nonnegative().nullable().optional(),
    step_type: z.string().nullable().optional(),
    provider_type: z.string().min(1),
    provider_model_id: z.string().min(1),
    model_id: z.string().min(1).nullable().optional(),
    input_tokens: z.number().int().nonnegative().nullable(),
    output_tokens: z.number().int().nonnegative().nullable(),
    total_tokens: z.number().int().nonnegative().nullable(),
    reasoning_tokens: z.number().int().nonnegative().nullable(),
    cached_input_tokens: z.number().int().nonnegative().nullable(),
    usage_source: z.enum([
      "ai_sdk_step_usage",
      "ai_sdk_total_usage",
      "provider_gateway",
      "provider_count_tokens",
      "estimated_current_step",
      "estimated_preflight",
    ]),
    is_actual: z.boolean(),
    raw_usage_json: z.unknown().nullable().optional(),
    synced_after: z.string().nullable().optional(),
  })
  .strict();

export type PersistedModelUsageRecord = z.infer<typeof PersistedModelUsageRecordSchema>;

export interface ModelUsageStore {
  insertStepUsage(record: PersistedModelUsageRecord): Promise<void>;
  upsertRunTotalUsage(record: PersistedModelUsageRecord): Promise<void>;
  insertStepUsageEstimate(record: PersistedModelUsageRecord): Promise<void>;
  sumActualUsageForRun(input: { agentRunId: string }): Promise<LanguageModelUsageSnapshot>;
  getRollingThroughputUsage(input: {
    providerType: string;
    providerModelId: string;
    windowSeconds: number;
    now?: Date;
  }): Promise<{ totalTokens: number; windowStartedAt: Date; oldestSampleAt: Date | null }>;
}

function usageRecordFromRow(
  row: Record<string, unknown>,
  source: PersistedModelUsageRecord["usage_source"] = "ai_sdk_step_usage",
  syncedAfter = "run_sum",
): LanguageModelUsageSnapshot {
  return LanguageModelUsageSnapshotSchema.parse({
    input_tokens: row.input_tokens === null ? null : Number(row.input_tokens ?? 0),
    output_tokens: row.output_tokens === null ? null : Number(row.output_tokens ?? 0),
    total_tokens: row.total_tokens === null ? null : Number(row.total_tokens ?? 0),
    reasoning_tokens: row.reasoning_tokens === null ? null : Number(row.reasoning_tokens ?? 0),
    cached_input_tokens: row.cached_input_tokens === null ? null : Number(row.cached_input_tokens ?? 0),
    raw: null,
    source,
    step_index: null,
    synced_after: syncedAfter,
    is_actual: true,
  });
}

export class PostgresModelUsageStore implements ModelUsageStore {
  readonly #pool: Pool;

  constructor(connectionString: string) {
    this.#pool = new Pool({ connectionString });
  }

  async insertStepUsage(record: PersistedModelUsageRecord): Promise<void> {
    const parsed = PersistedModelUsageRecordSchema.parse(record);
    if (parsed.step_index === null || parsed.step_index === undefined) return;
    await this.#pool.query(
      `INSERT INTO agent_model_step_usage (
         conversation_id, agent_run_id, request_id, step_index, step_type, provider_type, provider_model_id, model_id,
         input_tokens, output_tokens, total_tokens, reasoning_tokens, cached_input_tokens, usage_source, is_actual,
         raw_usage_json, synced_after, created_at
       ) VALUES (
         $1::uuid, $2::uuid, $3, $4, $5, $6, $7, $8,
         $9, $10, $11, $12, $13, $14, $15, $16::jsonb, $17, now()
       )
       ON CONFLICT (agent_run_id, step_index, usage_source) DO UPDATE SET
         input_tokens = EXCLUDED.input_tokens,
         output_tokens = EXCLUDED.output_tokens,
         total_tokens = EXCLUDED.total_tokens,
         reasoning_tokens = EXCLUDED.reasoning_tokens,
         cached_input_tokens = EXCLUDED.cached_input_tokens,
         raw_usage_json = EXCLUDED.raw_usage_json,
         synced_after = EXCLUDED.synced_after`,
      [
        parsed.conversation_id ?? null,
        parsed.agent_run_id,
        parsed.request_id ?? null,
        parsed.step_index,
        parsed.step_type ?? null,
        parsed.provider_type,
        parsed.provider_model_id,
        parsed.model_id ?? null,
        parsed.input_tokens,
        parsed.output_tokens,
        parsed.total_tokens,
        parsed.reasoning_tokens,
        parsed.cached_input_tokens,
        parsed.usage_source,
        parsed.is_actual,
        JSON.stringify(parsed.raw_usage_json ?? null),
        parsed.synced_after ?? null,
      ],
    );
  }

  async upsertRunTotalUsage(record: PersistedModelUsageRecord): Promise<void> {
    const parsed = PersistedModelUsageRecordSchema.parse(record);
    await this.#pool.query(
      `INSERT INTO agent_run_model_usage (
         agent_run_id, conversation_id, provider_type, provider_model_id, model_id,
         input_tokens, output_tokens, total_tokens, reasoning_tokens, cached_input_tokens, usage_source,
         is_actual, raw_usage_json, created_at, updated_at
       ) VALUES (
         $1::uuid, $2::uuid, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13::jsonb, now(), now()
       )
       ON CONFLICT (agent_run_id) DO UPDATE SET
         input_tokens = EXCLUDED.input_tokens,
         output_tokens = EXCLUDED.output_tokens,
         total_tokens = EXCLUDED.total_tokens,
         reasoning_tokens = EXCLUDED.reasoning_tokens,
         cached_input_tokens = EXCLUDED.cached_input_tokens,
         usage_source = EXCLUDED.usage_source,
         is_actual = EXCLUDED.is_actual,
         raw_usage_json = EXCLUDED.raw_usage_json,
         updated_at = now()`,
      [
        parsed.agent_run_id,
        parsed.conversation_id ?? null,
        parsed.provider_type,
        parsed.provider_model_id,
        parsed.model_id ?? null,
        parsed.input_tokens,
        parsed.output_tokens,
        parsed.total_tokens,
        parsed.reasoning_tokens,
        parsed.cached_input_tokens,
        parsed.usage_source,
        parsed.is_actual,
        JSON.stringify(parsed.raw_usage_json ?? null),
      ],
    );
  }

  async insertStepUsageEstimate(record: PersistedModelUsageRecord): Promise<void> {
    const parsed = PersistedModelUsageRecordSchema.parse(record);
    await this.#pool.query(
      `INSERT INTO agent_model_step_usage_estimate (
         conversation_id, agent_run_id, request_id, step_index, provider_type, provider_model_id,
         estimated_output_tokens, estimated_total_tokens, estimate_source, created_at
       ) VALUES ($1::uuid, $2::uuid, $3, $4, $5, $6, $7, $8, $9, now())`,
      [
        parsed.conversation_id ?? null,
        parsed.agent_run_id,
        parsed.request_id ?? null,
        parsed.step_index ?? 0,
        parsed.provider_type,
        parsed.provider_model_id,
        parsed.output_tokens,
        parsed.total_tokens,
        parsed.usage_source,
      ],
    );
  }

  async sumActualUsageForRun(input: { agentRunId: string }): Promise<LanguageModelUsageSnapshot> {
    const totalResult = await this.#pool.query(
      `SELECT input_tokens, output_tokens, total_tokens, reasoning_tokens, cached_input_tokens, usage_source
       FROM agent_run_model_usage
       WHERE agent_run_id = $1::uuid AND is_actual = TRUE
       LIMIT 1`,
      [input.agentRunId],
    );
    if (totalResult.rows[0]) {
      const row = totalResult.rows[0] as Record<string, unknown>;
      const source = (row.usage_source as PersistedModelUsageRecord["usage_source"] | undefined) ?? "ai_sdk_total_usage";
      return usageRecordFromRow(row, source, "run_total");
    }

    const result = await this.#pool.query(
      `SELECT
         NULLIF(COALESCE(SUM(input_tokens), 0), 0)::integer AS input_tokens,
         NULLIF(COALESCE(SUM(output_tokens), 0), 0)::integer AS output_tokens,
         NULLIF(COALESCE(SUM(total_tokens), 0), 0)::integer AS total_tokens,
         NULLIF(COALESCE(SUM(reasoning_tokens), 0), 0)::integer AS reasoning_tokens,
         NULLIF(COALESCE(SUM(cached_input_tokens), 0), 0)::integer AS cached_input_tokens
       FROM agent_model_step_usage
       WHERE agent_run_id = $1::uuid AND is_actual = TRUE`,
      [input.agentRunId],
    );
    return usageRecordFromRow((result.rows[0] ?? {}) as Record<string, unknown>);
  }

  async getRollingThroughputUsage(input: {
    providerType: string;
    providerModelId: string;
    windowSeconds: number;
    now?: Date;
  }): Promise<{ totalTokens: number; windowStartedAt: Date; oldestSampleAt: Date | null }> {
    const now = input.now ?? new Date();
    const windowStartedAt = new Date(now.getTime() - input.windowSeconds * 1000);
    const result = await this.#pool.query(
      `SELECT
         COALESCE(SUM(total_tokens), 0)::integer AS total_tokens,
         MIN(created_at) AS oldest_sample_at
       FROM agent_model_step_usage
       WHERE provider_type = $1
         AND provider_model_id = $2
         AND is_actual = TRUE
         AND created_at >= $3::timestamptz`,
      [input.providerType, input.providerModelId, windowStartedAt.toISOString()],
    );
    const oldest = result.rows[0]?.oldest_sample_at;
    return {
      totalTokens: Number(result.rows[0]?.total_tokens ?? 0),
      windowStartedAt,
      oldestSampleAt: oldest instanceof Date ? oldest : oldest ? new Date(String(oldest)) : null,
    };
  }
}

export class EphemeralModelUsageStore implements ModelUsageStore {
  readonly #records: Array<{ record: PersistedModelUsageRecord; createdAt: Date }> = [];
  readonly #totals = new Map<string, PersistedModelUsageRecord>();
  readonly #estimates: PersistedModelUsageRecord[] = [];
  readonly #now: () => Date;

  constructor(now: () => Date = () => new Date()) {
    this.#now = now;
  }

  async insertStepUsage(record: PersistedModelUsageRecord): Promise<void> {
    this.#records.push({ record: PersistedModelUsageRecordSchema.parse(record), createdAt: this.#now() });
  }

  async upsertRunTotalUsage(record: PersistedModelUsageRecord): Promise<void> {
    const parsed = PersistedModelUsageRecordSchema.parse(record);
    this.#totals.set(parsed.agent_run_id, parsed);
  }

  async insertStepUsageEstimate(record: PersistedModelUsageRecord): Promise<void> {
    this.#estimates.push(PersistedModelUsageRecordSchema.parse(record));
  }

  async sumActualUsageForRun(input: { agentRunId: string }): Promise<LanguageModelUsageSnapshot> {
    const total = this.#totals.get(input.agentRunId);
    if (total?.is_actual) {
      return LanguageModelUsageSnapshotSchema.parse({
        input_tokens: total.input_tokens,
        output_tokens: total.output_tokens,
        total_tokens: total.total_tokens,
        reasoning_tokens: total.reasoning_tokens,
        cached_input_tokens: total.cached_input_tokens,
        raw: null,
        source: total.usage_source,
        step_index: null,
        synced_after: "run_total",
        is_actual: true,
      });
    }

    const records = this.#records.map((entry) => entry.record).filter((record) => record.agent_run_id === input.agentRunId && record.is_actual);
    const sum = (key: "input_tokens" | "output_tokens" | "total_tokens" | "reasoning_tokens" | "cached_input_tokens") =>
      records.some((record) => record[key] !== null) ? records.reduce((total, record) => total + (record[key] ?? 0), 0) : null;
    return LanguageModelUsageSnapshotSchema.parse({
      input_tokens: sum("input_tokens"),
      output_tokens: sum("output_tokens"),
      total_tokens: sum("total_tokens"),
      reasoning_tokens: sum("reasoning_tokens"),
      cached_input_tokens: sum("cached_input_tokens"),
      raw: null,
      source: "ai_sdk_step_usage",
      step_index: null,
      synced_after: "run_sum",
      is_actual: true,
    });
  }

  async getRollingThroughputUsage(input: {
    providerType: string;
    providerModelId: string;
    windowSeconds: number;
    now?: Date;
  }): Promise<{ totalTokens: number; windowStartedAt: Date; oldestSampleAt: Date | null }> {
    const now = input.now ?? new Date();
    const windowStartedAt = new Date(now.getTime() - input.windowSeconds * 1000);
    const records = this.#records.filter(
      ({ record, createdAt }) =>
        record.provider_type === input.providerType &&
        record.provider_model_id === input.providerModelId &&
        record.is_actual &&
        createdAt.getTime() >= windowStartedAt.getTime(),
    );
    const totalTokens = records.reduce((total, { record }) => total + (record.total_tokens ?? 0), 0);
    const oldestSampleAt = records.reduce<Date | null>((oldest, entry) => {
      if (!oldest || entry.createdAt.getTime() < oldest.getTime()) return entry.createdAt;
      return oldest;
    }, null);
    return { totalTokens, windowStartedAt, oldestSampleAt };
  }
}
