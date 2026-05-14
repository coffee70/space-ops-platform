import assert from "node:assert/strict";
import test from "node:test";

import { providerOptionsForModel } from "../src/ai/model.js";
import type { ResolvedRuntimeModel } from "../src/types.js";

function openAiGpt5Model(overrides: Partial<ResolvedRuntimeModel> = {}): ResolvedRuntimeModel {
  return {
    id: "openai-gpt-5-5",
    providerType: "openai",
    providerModelId: "gpt-5.5",
    apiKey: "test-key",
    baseUrl: null,
    ...overrides,
  };
}

test("OpenAI GPT-5 models default to auto reasoning summaries when reasoning is not explicitly configured", () => {
  assert.deepEqual(providerOptionsForModel(openAiGpt5Model()), {
    openai: {
      reasoningSummary: "auto",
    },
  });
});

test("explicit reasoning.enabled=false suppresses OpenAI GPT-5 auto reasoning summaries", () => {
  assert.equal(
    providerOptionsForModel(
      openAiGpt5Model({
        reasoning: {
          enabled: false,
          representation: "reasoning_summary",
          source: "provider_exposed",
          providerOptions: {},
        },
      }),
    ),
    undefined,
  );
});

test("explicit reasoning provider options take precedence over OpenAI GPT-5 auto reasoning summaries", () => {
  assert.deepEqual(
    providerOptionsForModel(
      openAiGpt5Model({
        reasoning: {
          enabled: true,
          representation: "reasoning_summary",
          source: "provider_exposed",
          providerOptions: {
            openai: {
              reasoningSummary: "detailed",
            },
          },
        },
      }),
    ),
    {
      openai: {
        reasoningSummary: "detailed",
      },
    },
  );
});
