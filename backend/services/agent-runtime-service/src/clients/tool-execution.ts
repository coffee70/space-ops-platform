import { traceHeaders } from "../trace.js";
import type { RuntimeConfig, ToolExecutionClient, ToolExecutionResponse, TraceEnvelope } from "../types.js";
import { z } from "zod";

function serviceUrl(config: RuntimeConfig, serviceSlug: string, path: string): string {
  return `${config.controlPlaneUrl.replace(/\/$/, "")}/internal/runtime-services/${serviceSlug}/${path.replace(/^\//, "")}`;
}

export class HttpToolExecutionClient implements ToolExecutionClient {
  readonly #config: RuntimeConfig;

  constructor(config: RuntimeConfig) {
    this.#config = config;
  }

  async execute(input: {
    trace: TraceEnvelope;
    tool_name: string;
    input: Record<string, unknown>;
    execution_mode: string;
    message_id?: string | null;
    confirmation_token?: string | null;
  }): Promise<ToolExecutionResponse> {
    const response = await fetch(serviceUrl(this.#config, "tool-execution-service", "execute"), {
      method: "POST",
      headers: {
        "content-type": "application/json",
        ...traceHeaders(input.trace),
      },
      body: JSON.stringify({
        conversation_id: input.trace.conversation_id,
        agent_run_id: input.trace.agent_run_id,
        request_id: input.trace.request_id,
        tool_call_id: input.trace.tool_call_id,
        tool_name: input.tool_name,
        input: input.input,
        execution_mode: input.execution_mode,
        message_id: input.message_id ?? null,
        confirmation_token: input.confirmation_token ?? null,
      }),
      signal: AbortSignal.timeout(this.#config.requestTimeoutMs),
    });

    if (!response.ok) {
      const detail = await response.text();
      throw new Error(detail || "Tool execution failed");
    }

    const payload = await response.json();
    const parsed = z
      .object({
        conversation_id: z.string().nullable(),
        agent_run_id: z.string(),
        request_id: z.string(),
        tool_call_id: z.string(),
        status: z.enum(["completed", "failed", "confirmation_required"]),
        output: z.record(z.unknown()).default({}),
        raw_events: z.array(z.record(z.unknown())).optional(),
      })
      .parse(payload);
    return parsed as ToolExecutionResponse;
  }
}
