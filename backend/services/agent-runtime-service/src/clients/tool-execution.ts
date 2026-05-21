import { traceHeaders } from "../trace.js";
import type { RuntimeConfig, ToolExecutionClient, ToolExecutionResponse, TraceEnvelope } from "../types.js";
import { z } from "zod";

function serviceUrl(config: RuntimeConfig, serviceSlug: string, path: string): string {
  return `${config.controlPlaneUrl.replace(/\/$/, "")}/internal/runtime-services/${serviceSlug}/${path.replace(/^\//, "")}`;
}

export function runtimeServiceUrl(config: RuntimeConfig, serviceSlug: string, path: string): string {
  return serviceUrl(config, serviceSlug, path);
}

const ToolExecutionResponseSchema = z
  .object({
    conversation_id: z.string().nullable(),
    agent_run_id: z.string(),
    request_id: z.string(),
    tool_call_id: z.string(),
    status: z.enum(["completed", "failed", "confirmation_required", "permission_required", "permission_denied"]),
    output: z.unknown(),
    raw_events: z
      .array(
        z
          .object({
            event_type: z.string(),
            emitted_by: z.string(),
            payload: z.record(z.unknown()),
            tool_call_id: z.string().nullable().optional(),
            created_at: z.string().optional(),
          })
          .passthrough(),
      )
      .optional(),
  })
  .passthrough()
  .superRefine((value, ctx) => {
    if (!Object.hasOwn(value, "output")) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        path: ["output"],
        message: "Required",
      });
    }
  });

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
    confirmation_token?: string | null;
    approval_token?: string | null;
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
        confirmation_token: input.confirmation_token ?? null,
        approval_token: input.approval_token ?? input.confirmation_token ?? null,
      }),
      signal: AbortSignal.timeout(this.#config.requestTimeoutMs),
    });

    if (!response.ok) {
      const detail = await response.text();
      throw new Error(detail || "Tool execution failed");
    }

    const parsed = ToolExecutionResponseSchema.parse(await response.json());
    return {
      conversation_id: parsed.conversation_id,
      agent_run_id: parsed.agent_run_id,
      request_id: parsed.request_id,
      tool_call_id: parsed.tool_call_id,
      status: parsed.status,
      output: parsed.output,
      raw_events: parsed.raw_events,
    };
  }
}
