import assert from "node:assert/strict";
import test from "node:test";

import { fallbackMetadataForEntry } from "../src/ai/metadata/fallback-metadata.js";
import type { ModelRegistryEntry, ModelRegistryProvider } from "../src/types.js";

const openaiProvider: ModelRegistryProvider = {
  id: "openai-main",
  type: "openai",
  displayName: "OpenAI",
  apiKeyEnv: "OPENAI_API_KEY",
};

function entry(providerModelId: string): ModelRegistryEntry {
  return {
    id: "test-model",
    providerRef: "openai-main",
    providerModelId,
    enabled: true,
    defaultFor: [],
  };
}

test("GPT-5 family tier suffixes map correctly for mini/nano/pro/codex-max", () => {
  const mini = fallbackMetadataForEntry({ entry: entry("gpt-5.5-mini"), provider: openaiProvider });
  assert.equal(mini.qualityTier, "advanced");
  assert.equal(mini.reasoningTier, "light");
  assert.equal(mini.speedTier, "fast");
  assert.equal(mini.costTier, "$$");

  const nano = fallbackMetadataForEntry({ entry: entry("gpt-5.5-nano"), provider: openaiProvider });
  assert.equal(nano.qualityTier, "standard");
  assert.equal(nano.reasoningTier, "light");
  assert.equal(nano.speedTier, "fast");
  assert.equal(nano.costTier, "$");

  const pro = fallbackMetadataForEntry({ entry: entry("gpt-5.5-pro"), provider: openaiProvider });
  assert.equal(pro.speedTier, "deep");
  assert.equal(pro.qualityTier, "frontier");

  const codexMax = fallbackMetadataForEntry({ entry: entry("gpt-5.5-codex-max"), provider: openaiProvider });
  assert.equal(codexMax.speedTier, "deep");

  const codexBase = fallbackMetadataForEntry({ entry: entry("gpt-5.5-codex"), provider: openaiProvider });
  assert.equal(codexBase.speedTier, "balanced");
  assert.equal(codexBase.qualityTier, "frontier");
  assert.equal(codexBase.reasoningTier, "strong");
  assert.equal(codexBase.costTier, "$$$");
});
