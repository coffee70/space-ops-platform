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

export const RetryableModelProviderCategorySchema = z.enum([
  "rate_limited",
  "provider_overloaded",
  "network_transient",
]);

export const ModelRetryScheduledPayloadSchema = z
  .object({
    provider_type: z.string().min(1),
    provider_model_id: z.string().min(1),
    category: RetryableModelProviderCategorySchema,
    attempt: z.number().int().positive(),
    max_attempts: z.number().int().positive(),
    retry_after_ms: z.number().int().nonnegative(),
    retry_at: z.string().datetime(),
    safe_to_retry: z.boolean(),
  })
  .strict();

export const ModelRetryingPayloadSchema = z
  .object({
    provider_type: z.string().min(1),
    provider_model_id: z.string().min(1),
    attempt: z.number().int().positive(),
    max_attempts: z.number().int().positive(),
  })
  .strict();

export const ModelProviderErrorPayloadSchema = z
  .object({
    provider_type: z.string().min(1),
    provider_model_id: z.string().min(1),
    category: ModelProviderErrorCategorySchema,
    retryable: z.boolean(),
    retry_after_ms: z.number().int().nonnegative().nullable(),
    provider_error_type: z.string().nullable().optional(),
    provider_error_code: z.string().nullable().optional(),
    http_status: z.number().int().positive().nullable().optional(),
    message: z.string().min(1),
  })
  .strict();

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
export type ModelRetryScheduledPayload = z.infer<typeof ModelRetryScheduledPayloadSchema>;
export type ModelRetryingPayload = z.infer<typeof ModelRetryingPayloadSchema>;
export type ModelProviderErrorPayload = z.infer<typeof ModelProviderErrorPayloadSchema>;
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

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value && typeof value === "object");
}

function recordAt(value: unknown, key: string): Record<string, unknown> | null {
  if (!isRecord(value)) {
    return null;
  }
  const nested = value[key];
  return isRecord(nested) ? nested : null;
}

function stringAt(value: unknown, key: string): string | null {
  if (!isRecord(value)) {
    return null;
  }
  const item = value[key];
  return typeof item === "string" && item.trim().length > 0 ? item : null;
}

function numberAt(value: unknown, key: string): number | null {
  if (!isRecord(value)) {
    return null;
  }
  const item = value[key];
  if (typeof item === "number" && Number.isFinite(item) && item > 0) {
    return Math.trunc(item);
  }
  if (typeof item === "string" && /^\d+$/.test(item)) {
    return Number.parseInt(item, 10);
  }
  return null;
}

function headerValue(headers: unknown, name: string): string | null {
  if (!headers) {
    return null;
  }
  const normalizedName = name.toLowerCase();
  if (typeof (headers as { get?: unknown }).get === "function") {
    const value = (headers as { get: (key: string) => unknown }).get(name);
    return typeof value === "string" ? value : null;
  }
  if (isRecord(headers)) {
    for (const [key, value] of Object.entries(headers)) {
      if (key.toLowerCase() === normalizedName) {
        if (Array.isArray(value)) {
          const first = value.find((item) => typeof item === "string");
          return typeof first === "string" ? first : null;
        }
        return typeof value === "string" || typeof value === "number" ? String(value) : null;
      }
    }
  }
  return null;
}

function parseRetryAfter(value: string | null): number | null {
  if (!value) {
    return null;
  }
  const trimmed = value.trim();
  const seconds = Number.parseFloat(trimmed);
  if (Number.isFinite(seconds)) {
    return Math.max(0, Math.round(seconds * 1000));
  }
  const dateMs = Date.parse(trimmed);
  if (!Number.isNaN(dateMs)) {
    return Math.max(0, dateMs - Date.now());
  }
  return null;
}

function parseDurationMsFromMessage(message: string): number | null {
  const secondsMatch = message.match(/(?:try again in|retry after)\s+(\d+(?:\.\d+)?)\s*(?:s|sec|second|seconds)\b/i);
  if (secondsMatch) {
    return Math.round(Number.parseFloat(secondsMatch[1]) * 1000);
  }
  const minuteSecondMatch = message.match(/(?:try again in|retry after)\s+(\d+)\s*m\s*(\d+(?:\.\d+)?)\s*s/i);
  if (minuteSecondMatch) {
    return Math.round((Number.parseInt(minuteSecondMatch[1], 10) * 60 + Number.parseFloat(minuteSecondMatch[2])) * 1000);
  }
  const minuteMatch = message.match(/(?:try again in|retry after)\s+(\d+(?:\.\d+)?)\s*(?:m|min|minute|minutes)\b/i);
  if (minuteMatch) {
    return Math.round(Number.parseFloat(minuteMatch[1]) * 60_000);
  }
  return null;
}

function firstString(...values: Array<string | null>): string | null {
  return values.find((value): value is string => Boolean(value)) ?? null;
}

function classify(input: {
  httpStatus: number | null;
  code: string | null;
  type: string | null;
  message: string;
  errorName: string | null;
}): ModelProviderErrorCategory {
  const codeType = `${input.code ?? ""} ${input.type ?? ""}`.toLowerCase();
  const message = input.message.toLowerCase();
  const combined = `${codeType} ${message}`;

  if (input.errorName === "AbortError" || combined.includes("aborterror") || combined.includes("aborted")) {
    return "cancelled";
  }
  if (combined.includes("quota") || combined.includes("insufficient_quota") || combined.includes("billing")) {
    return "quota_exceeded";
  }
  if (input.httpStatus === 429 || codeType.includes("rate_limit") || /\btpm\b/.test(message) || message.includes("tokens per min") || message.includes("requests per minute")) {
    return "rate_limited";
  }
  if (combined.includes("context_length") || combined.includes("maximum context") || combined.includes("input too long")) {
    return "context_length_exceeded";
  }
  if (input.httpStatus === 401 || input.httpStatus === 403 || combined.includes("invalid_api_key") || combined.includes("authentication") || combined.includes("auth")) {
    return "auth_failed";
  }
  if ((input.httpStatus === 404 && (combined.includes("model") || combined.includes("not found"))) || combined.includes("model unavailable")) {
    return "model_unavailable";
  }
  if (input.httpStatus === 503 || input.httpStatus === 529 || combined.includes("overloaded") || combined.includes("temporarily unavailable")) {
    return "provider_overloaded";
  }
  if (combined.includes("econnreset") || combined.includes("etimedout") || combined.includes("timeout") || combined.includes("network")) {
    return "network_transient";
  }
  return "unknown";
}

function retryableFor(category: ModelProviderErrorCategory): boolean {
  if (category === "rate_limited") {
    return true;
  }
  return category === "provider_overloaded" || category === "network_transient";
}

export function normalizeModelProviderError(input: {
  error: unknown;
  providerType: string;
  providerModelId: string;
}): NormalizedModelProviderError {
  const error = input.error;
  const nestedError = recordAt(error, "error");
  const response = recordAt(error, "response");
  const headers = isRecord(error) ? error.headers : null;
  const responseHeaders = response?.headers ?? null;
  const code = firstString(stringAt(error, "code"), stringAt(nestedError, "code"));
  const type = firstString(stringAt(error, "type"), stringAt(nestedError, "type"));
  const message =
    firstString(stringAt(error, "message"), stringAt(nestedError, "message")) ??
    (typeof error === "string" && error.trim().length > 0 ? error : "Model provider request failed");
  const httpStatus = numberAt(error, "status") ?? numberAt(error, "statusCode") ?? numberAt(response, "status");
  const retryAfterMs = parseRetryAfter(headerValue(headers, "retry-after") ?? headerValue(responseHeaders, "retry-after")) ?? parseDurationMsFromMessage(message);
  const errorName = error instanceof Error ? error.name : stringAt(error, "name");
  const category = classify({ httpStatus, code, type, message, errorName });

  return NormalizedModelProviderErrorSchema.parse({
    category,
    retryable: retryableFor(category),
    retry_after_ms: retryAfterMs,
    provider_type: input.providerType,
    provider_model_id: input.providerModelId,
    provider_error_type: type,
    provider_error_code: code,
    http_status: httpStatus,
    message,
    raw_summary: {
      name: errorName,
      code,
      type,
      status: httpStatus,
    },
  });
}
