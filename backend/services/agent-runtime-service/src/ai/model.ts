import { stepCountIs, streamText } from "ai";
import { createAnthropic } from "@ai-sdk/anthropic";
import { createOpenAI } from "@ai-sdk/openai";
import { createOpenAICompatible } from "@ai-sdk/openai-compatible";

import type { ModelRunner, ModelStreamPart, ResolvedRuntimeModel, RuntimeConfig } from "../types.js";

function createLanguageModel(model: ResolvedRuntimeModel) {
  if (model.providerType === "openai") {
    if (!model.apiKey) {
      throw new Error(`Model ${model.id} is missing required API key for provider openai`);
    }
    return createOpenAI({
      apiKey: model.apiKey,
      baseURL: model.baseUrl ?? undefined,
    })(model.providerModelId);
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

export function createModelRunner(_config: RuntimeConfig): ModelRunner {
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

      for await (const part of result.fullStream) {
        yield part as ModelStreamPart;
      }
    },
  };
}
