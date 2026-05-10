import assert from "node:assert/strict";
import path from "node:path";
import { fileURLToPath } from "node:url";
import test from "node:test";

import { loadModelRegistryConfig } from "../src/ai/model-registry-config.js";
import { baseRuntimeConfig } from "./helpers.js";

const HERE = path.dirname(fileURLToPath(import.meta.url));
const INVALID_METADATA_YAML = path.join(HERE, "fixtures", "models-invalid-metadata.yaml");
const GOOGLE_PROVIDER_YAML = path.join(HERE, "fixtures", "models-google.yaml");

test("stack model registry YAML loads and validates", () => {
  const registry = loadModelRegistryConfig(baseRuntimeConfig());
  assert.equal(registry.defaults.chatModel, "openai-gpt-5-5");
  assert.ok(registry.models.some((m) => m.id === "openai-gpt-5-1" && m.enabled));
  assert.ok(registry.models.some((m) => m.id === "anthropic-claude-sonnet-4-6" && !m.enabled));
  assert.ok(registry.models.some((m) => m.id === "baremetal-qwen-coder-32b" && !m.enabled));
});

test("GPT-5.1 through GPT-5.5 base variants are present", () => {
  const registry = loadModelRegistryConfig(baseRuntimeConfig());
  for (let v = 1; v <= 5; v++) {
    assert.ok(registry.models.some((m) => m.providerModelId === `gpt-5.${v}`));
    assert.ok(registry.models.some((m) => m.providerModelId === `gpt-5.${v}-mini`));
  }
});

test("google provider type is accepted by schema", () => {
  const registry = loadModelRegistryConfig(baseRuntimeConfig({ modelsConfigPath: GOOGLE_PROVIDER_YAML }));
  assert.equal(registry.providersById.get("google-main")?.type, "google");
});

test("unknown metadataOverrides keys fail validation", () => {
  assert.throws(() => loadModelRegistryConfig(baseRuntimeConfig({ modelsConfigPath: INVALID_METADATA_YAML })));
});
