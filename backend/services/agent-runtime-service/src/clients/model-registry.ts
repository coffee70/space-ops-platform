import type { ExecutionMode, ListAiEngineerModelsResponse, ModelCatalogPort, ResolvedChatModel, RuntimeConfig } from "../types.js";
import { ModelSelectionError, type ModelSelectionErrorCode } from "../ai/model-errors.js";
import { ListAiEngineerModelsResponseSchema, ResolvedChatModelSchema } from "../model-schemas.js";
import { z } from "zod";

function normalizeBaseUrl(baseUrl: string): string {
  return baseUrl.trim().replace(/\/$/, "");
}

const ModelRegistryErrorResponseSchema = z.object({
  detail: z
    .union([
      z.string(),
      z.object({
        message: z.unknown().optional(),
        code: z.unknown().optional(),
      }),
    ])
    .optional(),
});

async function jsonRequest<T>(url: string, init: RequestInit, timeoutMs: number, schema: z.ZodSchema<T>): Promise<T> {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch(url, { ...init, signal: controller.signal });
    if (!response.ok) {
      let message = `HTTP ${response.status}`;
      let code: ModelSelectionErrorCode = "unknown_model";
      const text = await response.text().catch(() => "");
      try {
        const parsed = ModelRegistryErrorResponseSchema.safeParse(JSON.parse(text));
        if (parsed.success) {
          const body = parsed.data;
          if (typeof body.detail === "string") {
            message = body.detail;
          } else if (body.detail && typeof body.detail.message === "string") {
            message = body.detail.message;
            if (typeof body.detail.code === "string") code = body.detail.code as ModelSelectionErrorCode;
          }
        }
      } catch {
        if (text.trim().length > 0) message = `${message} ${text}`.trim();
      }
      throw new ModelSelectionError(code, message);
    }
    return schema.parse(await response.json());
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
      ListAiEngineerModelsResponseSchema,
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
      ResolvedChatModelSchema,
    );
  }
}
