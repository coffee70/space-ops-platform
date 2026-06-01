import { stepCountIs, streamText } from "ai";
import { createAnthropic } from "@ai-sdk/anthropic";
import { createOpenAI } from "@ai-sdk/openai";
import { createOpenAICompatible } from "@ai-sdk/openai-compatible";

import { normalizeAiSdkUsage } from "./model-usage.js";
import { ModelProviderRuntimeError, normalizeModelProviderError, providerErrorPayload } from "./provider-errors.js";
import type { ModelRunner, ModelStreamPart, ResolvedRuntimeModel, RuntimeConfig } from "../types.js";

const DEFAULT_OPENAI_BASE_URL = "https://api.openai.com/v1";
type StreamTextProviderOptions = NonNullable<Parameters<typeof streamText>[0]["providerOptions"]>;

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

function errorFromStreamPart(part: ModelStreamPart): unknown {
  const fields = part as Record<string, unknown>;
  return fields.error ?? part;
}

function unsafeToRetryPart(part: ModelStreamPart): boolean {
  return (
    part.type === "text-delta" ||
    part.type === "reasoning" ||
    part.type === "reasoning-delta" ||
    part.type === "tool-call" ||
    part.type === "tool-result"
  );
}

function retryDelayMs(errorRetryAfterMs: number | null, attempt: number, config: RuntimeConfig): number {
  if (errorRetryAfterMs !== null) return Math.min(errorRetryAfterMs, config.modelRetryMaxDelayMs);
  const exponential = config.modelRetryBaseDelayMs * 2 ** Math.max(0, attempt - 1);
  const jitter = config.modelRetryJitterMs > 0 ? Math.floor(Math.random() * config.modelRetryJitterMs) : 0;
  return Math.min(config.modelRetryMaxDelayMs, exponential + jitter);
}

function sleep(ms: number, signal?: AbortSignal): Promise<void> {
  if (ms <= 0) return Promise.resolve();
  if (signal?.aborted) return Promise.reject(new DOMException("Retry sleep aborted.", "AbortError"));
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(resolve, ms);
    signal?.addEventListener(
      "abort",
      () => {
        clearTimeout(timeout);
        reject(new DOMException("Retry sleep aborted.", "AbortError"));
      },
      { once: true },
    );
  });
}

export function createModelRunner(config: RuntimeConfig): ModelRunner {
  return {
    async *stream(input): AsyncIterable<ModelStreamPart> {
      const model = input.model;

      const languageModel = createLanguageModel(model);

      for (let attempt = 1; attempt <= config.modelRetryMaxAttempts; attempt += 1) {
        let safeToRetry = true;
        let stepIndex = 0;
        try {
          const result = streamText({
            model: languageModel,
            system: input.system,
            messages: input.messages,
            tools: input.tools,
            stopWhen: stepCountIs(input.maxSteps),
            providerOptions: providerOptionsForModel(model),
            abortSignal: input.abortSignal,
            onStepFinish: async (step) => {
              const stepFields = step as unknown as Record<string, unknown>;
              const usage = normalizeAiSdkUsage((step as { usage?: unknown }).usage, "ai_sdk_step_usage", {
                stepIndex,
                syncedAfter: `step_${stepIndex}`,
              });
              await input.onRuntimeEvent?.("model.usage.step", {
                provider_type: model.providerType,
                provider_model_id: model.providerModelId,
                model_id: model.id,
                step_type: typeof stepFields.stepType === "string" ? stepFields.stepType : null,
                step_index: stepIndex,
                usage,
              });
              stepIndex += 1;
            },
          });

          for await (const rawPart of result.fullStream) {
            const part = rawPart as ModelStreamPart;
            if (unsafeToRetryPart(part)) {
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
            if (part.type === "error") {
              throw errorFromStreamPart(part);
            }
            yield part;
          }

          const totalUsage = normalizeAiSdkUsage(await result.totalUsage, "ai_sdk_total_usage", { syncedAfter: "run_finish" });
          await input.onRuntimeEvent?.("model.usage.total", {
            provider_type: model.providerType,
            provider_model_id: model.providerModelId,
            model_id: model.id,
            usage: totalUsage,
          });
          return;
        } catch (error) {
          const normalized = normalizeModelProviderError({
            error,
            providerType: model.providerType,
            providerModelId: model.providerModelId,
          });
          await input.onRuntimeEvent?.("model.provider_error", providerErrorPayload(normalized));
          const canRetry = normalized.retryable && safeToRetry && attempt < config.modelRetryMaxAttempts;
          if (!canRetry) {
            throw new ModelProviderRuntimeError(normalized);
          }
          const delayMs = retryDelayMs(normalized.retry_after_ms, attempt, config);
          const retryAt = new Date(Date.now() + delayMs).toISOString();
          await input.onRuntimeEvent?.("model.retry_scheduled", {
            provider_type: model.providerType,
            provider_model_id: model.providerModelId,
            category: normalized.category,
            attempt,
            max_attempts: config.modelRetryMaxAttempts,
            retry_after_ms: delayMs,
            retry_at: retryAt,
            safe_to_retry: safeToRetry,
          });
          await sleep(delayMs, input.abortSignal);
          await input.onRuntimeEvent?.("model.retrying", {
            provider_type: model.providerType,
            provider_model_id: model.providerModelId,
            attempt: attempt + 1,
            max_attempts: config.modelRetryMaxAttempts,
          });
          if (config.logModelStreamParts) {
            console.debug(
              "[agent-runtime] retrying model stream",
              JSON.stringify({
                timestamp: new Date().toISOString(),
                providerType: model.providerType,
                providerModelId: model.providerModelId,
                attempt: attempt + 1,
              }),
            );
          }
        }
      }

      throw new ModelProviderRuntimeError(
        normalizeModelProviderError({
          error: new Error("Model provider retry attempts exhausted."),
          providerType: model.providerType,
          providerModelId: model.providerModelId,
        }),
      );
    },
  };
}
