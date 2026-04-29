import { traceHeaders } from "../trace.js";
import type { ContextPacketResponse, ContextRetrievalClient, RetrievalPlan, RuntimeConfig, TraceEnvelope } from "../types.js";

function serviceUrl(config: RuntimeConfig, serviceSlug: string, path: string): string {
  return `${config.controlPlaneUrl.replace(/\/$/, "")}/internal/runtime-services/${serviceSlug}/${path.replace(/^\//, "")}`;
}

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
    const response = await fetch(serviceUrl(this.#config, "context-retrieval-service", "context/packet"), {
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

    return (await response.json()) as ContextPacketResponse;
  }
}
