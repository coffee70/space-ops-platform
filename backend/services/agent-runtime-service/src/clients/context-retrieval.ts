import { traceHeaders } from "../trace.js";
import type { ContextPacketResponse, ContextRetrievalClient, RetrievalPlan, RuntimeConfig, TraceEnvelope } from "../types.js";
import { z } from "zod";

function serviceUrl(config: RuntimeConfig, serviceSlug: string, path: string): string {
  return `${config.controlPlaneUrl.replace(/\/$/, "")}/internal/runtime-services/${serviceSlug}/${path.replace(/^\//, "")}`;
}

const RawEventFactSchema = z
  .object({
    event_type: z.string(),
    emitted_by: z.string(),
    payload: z.record(z.unknown()),
    tool_call_id: z.string().nullable().optional(),
    created_at: z.string().optional(),
  })
  .passthrough();

const ContextPacketResponseSchema: z.ZodType<ContextPacketResponse, z.ZodTypeDef, unknown> = z
  .object({
    conversation_id: z.string().nullable(),
    agent_run_id: z.string(),
    request_id: z.string(),
    context_packet_id: z.string(),
    document_chunk_count: z.number(),
    code_chunk_count: z.number(),
    platform_metadata_bytes: z.number(),
    tool_definition_count: z.number(),
    truncated: z.boolean(),
    truncation_reasons: z.array(z.string()),
    failed_sources: z.array(z.object({ service: z.string(), failure_type: z.string() }).passthrough()).optional(),
    data: z.record(z.unknown()),
    raw_events: z.array(RawEventFactSchema).optional(),
  })
  .passthrough();

export class HttpContextRetrievalClient implements ContextRetrievalClient {
  readonly #config: RuntimeConfig;

  constructor(config: RuntimeConfig) {
    this.#config = config;
  }

  async resolve(input: {
    trace: TraceEnvelope;
    message: string;
    mission_id?: string | null;
    vehicle_id?: string | null;
    execution_mode: string;
    retrieval_plan: RetrievalPlan;
  }): Promise<ContextPacketResponse> {
    const response = await fetch(serviceUrl(this.#config, "context-retrieval-service", "packet"), {
      method: "POST",
      headers: {
        "content-type": "application/json",
        ...traceHeaders(input.trace),
      },
      body: JSON.stringify({
        conversation_id: input.trace.conversation_id,
        agent_run_id: input.trace.agent_run_id,
        request_id: input.trace.request_id,
        message: input.message,
        mission_id: input.mission_id ?? null,
        vehicle_id: input.vehicle_id ?? null,
        execution_mode: input.execution_mode,
        retrieval_instructions: {
          documents: input.retrieval_plan.documents,
          code: input.retrieval_plan.code,
          platform: input.retrieval_plan.platform,
          tools: input.retrieval_plan.tools,
        },
      }),
      signal: AbortSignal.timeout(this.#config.requestTimeoutMs),
    });

    if (!response.ok) {
      const detail = await response.text();
      throw new Error(detail || "Context retrieval failed");
    }

    return ContextPacketResponseSchema.parse(await response.json());
  }
}
