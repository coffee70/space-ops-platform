import { traceHeaders } from "../trace.js";
import type { RuntimeConfig, ToolDefinition, ToolRegistryClient, TraceEnvelope } from "../types.js";
import { z } from "zod";

function serviceUrl(config: RuntimeConfig, serviceSlug: string, path: string): string {
  return `${config.controlPlaneUrl.replace(/\/$/, "")}/internal/runtime-services/${serviceSlug}/${path.replace(/^\//, "")}`;
}

const ToolDefinitionSchema: z.ZodType<ToolDefinition, z.ZodTypeDef, unknown> = z
  .object({
    name: z.string().min(1),
    description: z.string(),
    category: z.string(),
    layer_target: z.string(),
    read_write_classification: z.string(),
    required_execution_mode: z.enum(["read_only", "suggest", "execute", "governed_execute"]),
    enabled: z.boolean(),
    requires_confirmation: z.boolean(),
    input_schema_json: z.record(z.unknown()),
    output_schema_json: z.record(z.unknown()).optional(),
    audit_policy_json: z.record(z.unknown()).optional(),
    redaction_policy_json: z.record(z.unknown()).optional(),
    backing_service: z.string().nullable().optional(),
    backing_api: z.string().nullable().optional(),
  })
  .passthrough();

export class HttpToolRegistryClient implements ToolRegistryClient {
  readonly #config: RuntimeConfig;

  constructor(config: RuntimeConfig) {
    this.#config = config;
  }

  async listTools(trace: TraceEnvelope): Promise<ToolDefinition[]> {
    const response = await fetch(serviceUrl(this.#config, "tool-registry-service", "definitions?include_full_metadata=true&enabled=true"), {
      headers: {
        "Content-Type": "application/json",
        ...traceHeaders(trace),
      },
      signal: AbortSignal.timeout(this.#config.requestTimeoutMs),
    });

    if (!response.ok) {
      const detail = await response.text();
      throw new Error(detail || "Tool registry lookup failed");
    }

    return z.array(ToolDefinitionSchema).parse(await response.json());
  }
}
