import { stepCountIs, streamText } from "ai";
import { createAnthropic } from "@ai-sdk/anthropic";
import { createOpenAI } from "@ai-sdk/openai";
import { createOpenAICompatible } from "@ai-sdk/openai-compatible";

import type { ModelRunner, ModelStreamPart, ResolvedRuntimeModel, RuntimeConfig } from "../types.js";
import {
  ModelProviderErrorPayloadSchema,
  ModelProviderRuntimeError,
  ModelRetryingPayloadSchema,
  ModelRetryScheduledPayloadSchema,
  type NormalizedModelProviderError,
  normalizeModelProviderError,
} from "./provider-errors.js";

const DEFAULT_OPENAI_BASE_URL = "https://api.openai.com/v1";
type StreamTextProviderOptions = NonNullable<Parameters<typeof streamText>[0]["providerOptions"]>;
type StreamTextFn = typeof streamText;

function createLanguageModel(model: ResolvedRuntimeModel) {
  if (model.providerType === "openai") {
    if (!model.apiKey) {
      throw new Error(`Model ${model.id} is missing required API key for provider openai`);
    }
    const options: Parameters<typeof createOpenAI>[0] = {
      apiKey: model.apiKey,
      baseURL: model.baseUrl ?? DEFAULT_OPENAI_BASE_URL,
    };
    return createOpenAI(options)(model.providerModelId);
  }

  if (model.providerType === "anthropic") {
    if (!model.apiKey) {
      throw new Error(`Model ${model.id} is missing required API key for provider anthropic`);
    }
    return createAnthropic({
      apiKey: model.apiKey,
    })(model.providerModelId);
  }

  if (model.providerType === "openai-compatible") {
    if (!model.baseUrl) {
      throw new Error(`Model ${model.id} is missing required baseUrl for OpenAI-compatible provider`);
    }
    return createOpenAICompatible({
      name: model.id,
      apiKey: model.apiKey ?? "local",
      baseURL: model.baseUrl,
    })(model.providerModelId);
  }

  throw new Error(`Unsupported provider type: ${model.providerType}`);
}

export function providerOptionsForModel(model: ResolvedRuntimeModel): StreamTextProviderOptions | undefined {
  const reasoning = model.reasoning;
  if (!reasoning?.enabled) {
    return undefined;
  }

  if (Object.keys(reasoning.providerOptions).length > 0) {
    return reasoning.providerOptions as StreamTextProviderOptions;
  }

  // For OpenAI models, reasoning.enabled=true means the registry has opted this
  // model into visible reasoning summaries. The registry, not model-id naming,
  // is the source of truth for whether this provider option should be added.
  if (model.providerType === "openai") {
    return {
      openai: {
        reasoningSummary: "auto",
      },
    };
  }

  return undefined;
}

function summarizeStreamPart(part: ModelStreamPart): Record<string, unknown> {
  const fields = part as Record<string, unknown>;
  return {
    type: part.type,
    id: typeof fields.id === "string" ? fields.id : undefined,
    text: typeof fields.text === "string" ? { length: fields.text.length, preview: fields.text.slice(0, 80) } : undefined,
    delta: typeof fields.delta === "string" ? { length: fields.delta.length, preview: fields.delta.slice(0, 80) } : undefined,
    textDelta:
      typeof fields.textDelta === "string" ? { length: fields.textDelta.length, preview: fields.textDelta.slice(0, 80) } : undefined,
    toolCallId: typeof fields.toolCallId === "string" ? fields.toolCallId : undefined,
    toolName: typeof fields.toolName === "string" ? fields.toolName : undefined,
    finishReason: typeof fields.finishReason === "string" ? fields.finishReason : undefined,
    keys: Object.keys(part).sort(),
  };
}

function getTextDeltaPreview(part: ModelStreamPart): { length: number; preview: string } | null {
  if (part.type !== "text-delta") {
    return null;
  }
  const fields = part as Record<string, unknown>;
  const text =
    typeof fields.text === "string"
      ? fields.text
      : typeof fields.delta === "string"
        ? fields.delta
        : typeof fields.textDelta === "string"
          ? fields.textDelta
          : null;
  if (text === null) {
    return null;
  }
  return { length: text.length, preview: text.slice(0, 80) };
}

function extractStreamPartError(part: ModelStreamPart): unknown {
  if (part.type !== "error") {
    return part;
  }
  const fields = part as Record<string, unknown>;
  return fields.error ?? new Error("Model stream failed");
}

function isUnsafeToRetryPart(part: ModelStreamPart): boolean {
  return part.type === "text-delta" || part.type === "reasoning" || part.type === "reasoning-delta" || part.type === "tool-call" || part.type === "tool-result";
}

function providerErrorPayload(normalized: NormalizedModelProviderError): Record<string, unknown> {
  return ModelProviderErrorPayloadSchema.parse({
    provider_type: normalized.provider_type,
    provider_model_id: normalized.provider_model_id,
    category: normalized.category,
    retryable: normalized.retryable,
    retry_after_ms: normalized.retry_after_ms,
    provider_error_type: normalized.provider_error_type,
    provider_error_code: normalized.provider_error_code,
    http_status: normalized.http_status,
    message: normalized.message,
  });
}

function computeRetryDelayMs(normalized: NormalizedModelProviderError, attempt: number, config: RuntimeConfig): number {
  if (normalized.retry_after_ms !== null) {
    return Math.min(normalized.retry_after_ms, config.modelRetryMaxDelayMs);
  }
  const exponential = config.modelRetryBaseDelayMs * 2 ** Math.max(0, attempt - 1);
  const jitter = config.modelRetryJitterMs > 0 ? Math.floor(Math.random() * (config.modelRetryJitterMs + 1)) : 0;
  return Math.min(exponential + jitter, config.modelRetryMaxDelayMs);
}

async function sleepWithAbort(delayMs: number, abortSignal?: AbortSignal): Promise<void> {
  if (delayMs <= 0) {
    if (abortSignal?.aborted) {
      throw abortError();
    }
    return;
  }
  await new Promise<void>((resolve, reject) => {
    if (abortSignal?.aborted) {
      reject(abortError());
      return;
    }
    const timeout = setTimeout(resolve, delayMs);
    const onAbort = () => {
      clearTimeout(timeout);
      reject(abortError());
    };
    abortSignal?.addEventListener("abort", onAbort, { once: true });
  });
}

function abortError(): Error {
  const error = new Error("Model stream aborted.");
  error.name = "AbortError";
  return error;
}

export function createModelRunner(config: RuntimeConfig, input?: { streamText?: StreamTextFn }): ModelRunner {
  const streamTextFn = input?.streamText ?? streamText;
  return {
    async *stream(input): AsyncIterable<ModelStreamPart> {
      const model = input.model;

      const languageModel = createLanguageModel(model);

      for (let attempt = 1; attempt <= config.modelRetryMaxAttempts; attempt += 1) {
        let safeToRetry = true;

        try {
          const result = streamTextFn({
            model: languageModel,
            system: input.system,
            messages: input.messages,
            tools: input.tools,
            stopWhen: stepCountIs(input.maxSteps),
            providerOptions: providerOptionsForModel(model),
            abortSignal: input.abortSignal,
          });

          for await (const rawPart of result.fullStream) {
            const part = rawPart as ModelStreamPart;
            if (part.type === "error") {
              throw extractStreamPartError(part);
            }
            if (isUnsafeToRetryPart(part)) {
              safeToRetry = false;
            }
            if (config.logModelStreamParts) {
              const textDelta = getTextDeltaPreview(part);
              if (textDelta) {
                console.debug(
                  "[agent-runtime] model text-delta",
                  JSON.stringify({
                    timestamp: new Date().toISOString(),
                    providerType: model.providerType,
                    providerModelId: model.providerModelId,
                    deltaLength: textDelta.length,
                    preview: textDelta.preview,
                  }),
                );
              }
              console.debug(
                "[agent-runtime] model fullStream part",
                JSON.stringify({
                  timestamp: new Date().toISOString(),
                  providerType: model.providerType,
                  providerModelId: model.providerModelId,
                  part: summarizeStreamPart(part),
                }),
              );
            }
            yield part;
          }
          return;
        } catch (error) {
          const normalized = normalizeModelProviderError({
            error,
            providerType: model.providerType,
            providerModelId: model.providerModelId,
          });

          await input.onRuntimeEvent?.("model.provider_error", providerErrorPayload(normalized));

          if (normalized.category === "cancelled") {
            throw error;
          }

          if (!normalized.retryable || !safeToRetry || attempt >= config.modelRetryMaxAttempts) {
            throw new ModelProviderRuntimeError(normalized);
          }

          const retryAfterMs = computeRetryDelayMs(normalized, attempt, config);
          await input.onRuntimeEvent?.("model.retry_scheduled", ModelRetryScheduledPayloadSchema.parse({
            provider_type: normalized.provider_type,
            provider_model_id: normalized.provider_model_id,
            category: normalized.category,
            attempt,
            max_attempts: config.modelRetryMaxAttempts,
            retry_after_ms: retryAfterMs,
            retry_at: new Date(Date.now() + retryAfterMs).toISOString(),
            safe_to_retry: safeToRetry,
          }));

          await sleepWithAbort(retryAfterMs, input.abortSignal);
          await input.onRuntimeEvent?.("model.retrying", ModelRetryingPayloadSchema.parse({
            provider_type: normalized.provider_type,
            provider_model_id: normalized.provider_model_id,
            attempt: attempt + 1,
            max_attempts: config.modelRetryMaxAttempts,
          }));
        }
      }

      throw new Error("Model stream failed after retry attempts were exhausted.");
    },
  };
}
