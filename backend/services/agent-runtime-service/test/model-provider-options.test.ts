import assert from "node:assert/strict";
import test from "node:test";

import { providerOptionsForModel } from "../src/ai/model.js";
import type { ResolvedRuntimeModel } from "../src/types.js";

function openAiModel(providerModelId: string, overrides: Partial<ResolvedRuntimeModel> = {}): ResolvedRuntimeModel {
  return {
    id: `openai-${providerModelId}`,
    providerType: "openai",
    providerModelId,
    apiKey: "test-key",
    baseUrl: null,
    ...overrides,
  };
}

test("OpenAI models do not request reasoning summaries without explicit reasoning enablement", () => {
  assert.equal(providerOptionsForModel(openAiModel("gpt-5.5")), undefined);
  assert.equal(providerOptionsForModel(openAiModel("o3")), undefined);
  assert.equal(providerOptionsForModel(openAiModel("gpt-6")), undefined);
});

test("explicit OpenAI reasoning enablement defaults to auto reasoning summaries regardless of model id", () => {
  for (const providerModelId of ["gpt-5.5", "o3", "gpt-6"]) {
    assert.deepEqual(
      providerOptionsForModel(
        openAiModel(providerModelId, {
          reasoning: {
            enabled: true,
            representation: "reasoning_summary",
            source: "provider_exposed",
            providerOptions: {},
          },
        }),
      ),
      {
        openai: {
          reasoningSummary: "auto",
        },
      },
      `expected reasoning summaries to default on for ${providerModelId}`,
    );
  }
});

test("explicit reasoning.enabled=false suppresses OpenAI reasoning summaries", () => {
  assert.equal(
    providerOptionsForModel(
      openAiModel("gpt-5.5", {
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

test("explicit OpenAI reasoning provider options take precedence over the auto summary default", () => {
  assert.deepEqual(
    providerOptionsForModel(
      openAiModel("gpt-6", {
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

test("non-OpenAI reasoning models still require explicit provider options", () => {
  assert.equal(
    providerOptionsForModel({
      id: "anthropic-sonnet-4-6",
      providerType: "anthropic",
      providerModelId: "claude-sonnet-4-6",
      apiKey: "test-key",
      baseUrl: null,
      reasoning: {
        enabled: true,
        representation: "thinking",
        source: "provider_exposed",
        providerOptions: {},
      },
    }),
    undefined,
  );
});
