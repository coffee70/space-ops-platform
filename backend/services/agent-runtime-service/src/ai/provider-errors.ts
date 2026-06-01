import { z } from "zod";

export const ModelProviderErrorCategorySchema = z.enum([
  "rate_limited",
  "quota_exceeded",
  "context_length_exceeded",
  "auth_failed",
  "model_unavailable",
  "provider_overloaded",
  "network_transient",
  "cancelled",
  "unknown",
]);

export const NormalizedModelProviderErrorSchema = z
  .object({
    category: ModelProviderErrorCategorySchema,
    retryable: z.boolean(),
    retry_after_ms: z.number().int().nonnegative().nullable(),
    provider_type: z.string().min(1),
    provider_model_id: z.string().min(1),
    provider_error_type: z.string().nullable(),
    provider_error_code: z.string().nullable(),
    http_status: z.number().int().positive().nullable(),
    message: z.string().min(1),
    raw_summary: z.record(z.unknown()).optional(),
  })
  .strict();

export type ModelProviderErrorCategory = z.infer<typeof ModelProviderErrorCategorySchema>;
export type NormalizedModelProviderError = z.infer<typeof NormalizedModelProviderErrorSchema>;

export class ModelProviderRuntimeError extends Error {
  readonly normalized: NormalizedModelProviderError;

  constructor(normalized: NormalizedModelProviderError) {
    const parsed = NormalizedModelProviderErrorSchema.parse(normalized);
    super(parsed.message);
    this.name = "ModelProviderRuntimeError";
    this.normalized = parsed;
  }
}

function objectValue(value: unknown): Record<string, unknown> | null {
  return value && typeof value === "object" && !Array.isArray(value) ? (value as Record<string, unknown>) : null;
}

function stringField(source: Record<string, unknown> | null, names: string[]): string | null {
  if (!source) return null;
  for (const name of names) {
    const value = source[name];
    if (typeof value === "string" && value.trim().length > 0) return value.trim();
  }
  return null;
}

function numberField(source: Record<string, unknown> | null, names: string[]): number | null {
  if (!source) return null;
  for (const name of names) {
    const value = source[name];
    if (typeof value === "number" && Number.isFinite(value) && value > 0) return Math.trunc(value);
    if (typeof value === "string" && /^\d+$/.test(value.trim())) return Number.parseInt(value.trim(), 10);
  }
  return null;
}

function headerValue(headers: unknown, name: string): string | null {
  if (!headers) return null;
  if (typeof (headers as { get?: unknown }).get === "function") {
    const value = (headers as { get(name: string): unknown }).get(name);
    return typeof value === "string" ? value : null;
  }
  const raw = objectValue(headers);
  if (!raw) return null;
  const wanted = name.toLowerCase();
  for (const [key, value] of Object.entries(raw)) {
    if (key.toLowerCase() === wanted) {
      return Array.isArray(value) ? String(value[0] ?? "") : typeof value === "string" ? value : String(value);
    }
  }
  return null;
}

function retryAfterMsFromHeader(value: string | null): number | null {
  if (!value) return null;
  const trimmed = value.trim();
  if (/^\d+(\.\d+)?$/.test(trimmed)) return Math.max(0, Math.round(Number.parseFloat(trimmed) * 1000));
  const dateMs = Date.parse(trimmed);
  if (Number.isFinite(dateMs)) return Math.max(0, dateMs - Date.now());
  return null;
}

export function retryAfterMsFromMessage(message: string): number | null {
  const lower = message.toLowerCase();
  const seconds = lower.match(/(?:try again in|retry after)\s+(\d+(?:\.\d+)?)\s*s(?:ec(?:ond)?s?)?/);
  if (seconds) return Math.round(Number.parseFloat(seconds[1] ?? "0") * 1000);
  const minutesSeconds = lower.match(/(?:try again in|retry after)\s+(\d+)m\s*(\d+(?:\.\d+)?)s/);
  if (minutesSeconds) {
    return Math.round((Number.parseInt(minutesSeconds[1] ?? "0", 10) * 60 + Number.parseFloat(minutesSeconds[2] ?? "0")) * 1000);
  }
  const plainSeconds = lower.match(/retry after\s+(\d+(?:\.\d+)?)\s+seconds?/);
  if (plainSeconds) return Math.round(Number.parseFloat(plainSeconds[1] ?? "0") * 1000);
  return null;
}

function classify(input: {
  status: number | null;
  code: string | null;
  type: string | null;
  message: string;
  name: string | null;
}): ModelProviderErrorCategory {
  const haystack = [input.code, input.type, input.message, input.name].filter(Boolean).join(" ").toLowerCase();
  if (input.name === "AbortError" || haystack.includes("aborterror") || haystack.includes("aborted")) return "cancelled";
  if (haystack.includes("quota") || haystack.includes("insufficient_quota") || haystack.includes("billing")) return "quota_exceeded";
  if (input.status === 429 || haystack.includes("rate_limit") || haystack.includes("rate limit") || haystack.includes("tpm") || haystack.includes("tokens per min") || haystack.includes("requests per minute")) return "rate_limited";
  if (haystack.includes("context_length") || haystack.includes("maximum context") || haystack.includes("input too long")) return "context_length_exceeded";
  if (input.status === 401 || input.status === 403 || haystack.includes("invalid_api_key") || haystack.includes("authentication") || haystack.includes("unauthorized")) return "auth_failed";
  if (input.status === 404 || haystack.includes("model not found") || haystack.includes("model unavailable")) return "model_unavailable";
  if (input.status === 503 || input.status === 529 || haystack.includes("overloaded") || haystack.includes("temporarily unavailable")) return "provider_overloaded";
  if (haystack.includes("econnreset") || haystack.includes("etimedout") || haystack.includes("timeout") || haystack.includes("network")) return "network_transient";
  return "unknown";
}

function retryableFor(category: ModelProviderErrorCategory, retryAfterMs: number | null): boolean {
  if (category === "rate_limited") return retryAfterMs !== null || true;
  return category === "provider_overloaded" || category === "network_transient";
}

export function normalizeModelProviderError(input: {
  error: unknown;
  providerType: string;
  providerModelId: string;
}): NormalizedModelProviderError {
  const root = objectValue(input.error);
  const nested = objectValue(root?.error);
  const response = objectValue(root?.response);
  const headers = root?.headers ?? response?.headers ?? nested?.headers;
  const message =
    stringField(nested, ["message"]) ??
    stringField(root, ["message", "statusText"]) ??
    (typeof input.error === "string" && input.error.trim().length > 0 ? input.error.trim() : "Model provider request failed.");
  const providerErrorCode = stringField(nested, ["code"]) ?? stringField(root, ["code"]);
  const providerErrorType = stringField(nested, ["type"]) ?? stringField(root, ["type", "name"]);
  const httpStatus = numberField(root, ["status", "statusCode"]) ?? numberField(response, ["status", "statusCode"]);
  const retryAfterMs = retryAfterMsFromHeader(headerValue(headers, "retry-after")) ?? retryAfterMsFromMessage(message);
  const category = classify({
    status: httpStatus,
    code: providerErrorCode,
    type: providerErrorType,
    message,
    name: stringField(root, ["name"]),
  });

  return NormalizedModelProviderErrorSchema.parse({
    category,
    retryable: retryableFor(category, retryAfterMs),
    retry_after_ms: retryAfterMs,
    provider_type: input.providerType,
    provider_model_id: input.providerModelId,
    provider_error_type: providerErrorType,
    provider_error_code: providerErrorCode,
    http_status: httpStatus,
    message,
    raw_summary: {
      name: stringField(root, ["name"]),
      keys: root ? Object.keys(root).sort() : [],
      nested_keys: nested ? Object.keys(nested).sort() : [],
    },
  });
}

export function providerErrorPayload(error: NormalizedModelProviderError): Record<string, unknown> {
  return {
    provider_type: error.provider_type,
    provider_model_id: error.provider_model_id,
    category: error.category,
    retryable: error.retryable,
    retry_after_ms: error.retry_after_ms,
    provider_error_type: error.provider_error_type,
    provider_error_code: error.provider_error_code,
    http_status: error.http_status,
    message: error.message,
  };
}
