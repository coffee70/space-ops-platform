import { stepCountIs, streamText } from "ai";
import { createOpenAI } from "@ai-sdk/openai";

import type { ModelRunner, ModelStreamPart, RuntimeConfig } from "../types.js";

export function createModelRunner(config: RuntimeConfig): ModelRunner {
  return {
    async *stream(input): AsyncIterable<ModelStreamPart> {
      if (!config.openAiApiKey) {
        return;
      }

      const openai = createOpenAI({
        apiKey: config.openAiApiKey,
        baseURL: config.openAiBaseUrl ?? undefined,
      });

      const result = streamText({
        model: openai(config.modelId),
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
