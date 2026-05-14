import { stepCountIs, streamText } from "ai";
import { createAnthropic } from "@ai-sdk/anthropic";
import { createOpenAI } from "@ai-sdk/openai";
import { createOpenAICompatible } from "@ai-sdk/openai-compatible";

import type { ModelRunner, ModelStreamPart, ResolvedRuntimeModel, RuntimeConfig } from "../types.js";

const DEFAULT_OPENAI_BASE_URL = "https://api.openai.com/v1";

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

function providerOptionsForModel(model: ResolvedRuntimeModel): Record<string, Record<string, unknown>> | undefined {
  const reasoning = model.reasoning;
  if (reasoning?.enabled && Object.keys(reasoning.providerOptions).length > 0) {
    return reasoning.providerOptions as Record<string, Record<string, unknown>>;
  }

  // OpenAI reasoning models can already use/bill internal reasoning tokens.
  // Requesting a reasoning summary is a visibility knob; it intentionally does
  // not set reasoningEffort, so it does not ask OpenAI to reason harder.
  if (model.providerType === "openai" && model.providerModelId.startsWith("gpt-5")) {
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

export function createModelRunner(config: RuntimeConfig): ModelRunner {
  return {
    async *stream(input): AsyncIterable<ModelStreamPart> {
      const model = input.model;

      const languageModel = createLanguageModel(model);

      const result = streamText({
        model: languageModel,
        system: input.system,
        messages: input.messages,
        tools: input.tools,
        stopWhen: stepCountIs(input.maxSteps),
        providerOptions: providerOptionsForModel(model),
      });

      for await (const rawPart of result.fullStream) {
        const part = rawPart as ModelStreamPart;
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
    },
  };
}
