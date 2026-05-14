import assert from "node:assert/strict";
import test from "node:test";

import { fallbackMetadataForEntry } from "../src/ai/metadata/fallback-metadata.js";
import { resolveOpenRouterMetadata } from "../src/ai/metadata/openrouter-resolver.js";
import type { ModelRegistryEntry, ModelRegistryProvider } from "../src/types.js";

const anthropicProvider: ModelRegistryProvider = {
  id: "anthropic-main",
  type: "anthropic",
  displayName: "Anthropic",
};

const openAiProvider: ModelRegistryProvider = {
  id: "openai-main",
  type: "openai",
  displayName: "OpenAI",
};

function entry(providerModelId: string, providerRef = "anthropic-main"): ModelRegistryEntry {
  return {
    id: providerModelId,
    providerRef,
    providerModelId,
    enabled: true,
  };
}

test("fallback Anthropic metadata formats hyphenated model versions with periods", () => {
  assert.equal(
    fallbackMetadataForEntry({ provider: anthropicProvider, entry: entry("claude-sonnet-4-6") }).displayName,
    "Claude Sonnet 4.6",
  );
  assert.equal(
    fallbackMetadataForEntry({ provider: anthropicProvider, entry: entry("claude-opus-4-1") }).displayName,
    "Claude Opus 4.1",
  );
});

test("OpenRouter metadata strips provider prefix from OpenAI display names", () => {
  const metadata = resolveOpenRouterMetadata({
    provider: openAiProvider,
    entry: entry("gpt-5.5-mini", "openai-main"),
    modelsById: new Map([
      [
        "openai/gpt-5.5-mini",
        {
          id: "openai/gpt-5.5-mini",
          name: "OpenAI: GPT-5.5 Mini",
          architecture: {
            input_modalities: ["text"],
            output_modalities: ["text"],
          },
          supported_parameters: ["tools"],
          pricing: {},
        },
      ],
    ]),
  });

  assert.equal(metadata?.displayName, "GPT-5.5 Mini");
  assert.equal(metadata?.providerDisplayName, "OpenAI");
});
