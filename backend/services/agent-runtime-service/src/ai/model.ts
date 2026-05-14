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
      });

      for await (const rawPart of result.fullStream) {
        const part = rawPart as ModelStreamPart;
        if (config.logModelStreamParts) {
          console.debug(
            "[agent-runtime] model fullStream part",
            JSON.stringify({
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
