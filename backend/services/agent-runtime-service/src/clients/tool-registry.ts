import { traceHeaders } from "../trace.js";
import type { RuntimeConfig, ToolDefinition, ToolRegistryClient, TraceEnvelope } from "../types.js";

function serviceUrl(config: RuntimeConfig, serviceSlug: string, path: string): string {
  return `${config.controlPlaneUrl.replace(/\/$/, "")}/internal/runtime-services/${serviceSlug}/${path.replace(/^\//, "")}`;
}

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

    return (await response.json()) as ToolDefinition[];
  }
}
