import type { ExecutionMode, ListAiEngineerModelsResponse, ModelCatalogPort, ResolvedChatModel, RuntimeConfig } from "../types.js";

function normalizeBaseUrl(baseUrl: string): string {
  return baseUrl.trim().replace(/\/$/, "");
}

async function jsonRequest<T>(url: string, init: RequestInit, timeoutMs: number): Promise<T> {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch(url, { ...init, signal: controller.signal });
    if (!response.ok) {
      const text = await response.text().catch(() => "");
      throw new Error(`HTTP ${response.status} ${text}`.trim());
    }
    return (await response.json()) as T;
  } finally {
    clearTimeout(timeout);
  }
}

export class HttpModelRegistryClient implements ModelCatalogPort {
  private readonly baseUrl: string;
  private readonly timeoutMs: number;

  constructor(config: RuntimeConfig) {
    if (!config.modelRegistryBaseUrl) {
      throw new Error("MODEL_REGISTRY_BASE_URL is not configured");
    }
    this.baseUrl = normalizeBaseUrl(config.modelRegistryBaseUrl);
    this.timeoutMs = config.requestTimeoutMs;
  }

  async listModelsResponse(): Promise<ListAiEngineerModelsResponse> {
    return jsonRequest<ListAiEngineerModelsResponse>(
      `${this.baseUrl}/models`,
      { method: "GET" },
      this.timeoutMs,
    );
  }

  async resolveForChat(modelId: string | null | undefined, executionMode: ExecutionMode): Promise<ResolvedChatModel> {
    return jsonRequest<ResolvedChatModel>(
      `${this.baseUrl}/models/resolve-chat`,
      {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ model_id: modelId ?? null, execution_mode: executionMode }),
      },
      this.timeoutMs,
    );
  }
}

