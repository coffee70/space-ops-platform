import assert from "node:assert/strict";
import path from "node:path";
import { fileURLToPath } from "node:url";
import test from "node:test";

import { loadModelRegistryConfig } from "../src/ai/model-registry-config.js";
import {
  buildOpenRouterCacheKey,
  ensureOpenRouterCatalog,
  resetOpenRouterResolverCacheForTests,
} from "../src/ai/metadata/openrouter-resolver.js";
import { baseRuntimeConfig } from "./helpers.js";

const HERE = path.dirname(fileURLToPath(import.meta.url));
const OPENROUTER_REGISTRY = path.join(HERE, "fixtures", "models-openrouter-fetch.yaml");

test("buildOpenRouterCacheKey distinguishes baseUrl and auth presence", () => {
  assert.notEqual(
    buildOpenRouterCacheKey("https://a.com/api/v1/models", false),
    buildOpenRouterCacheKey("https://b.com/api/v1/models", false),
  );
  assert.notEqual(
    buildOpenRouterCacheKey("https://a.com/api/v1/models", false),
    buildOpenRouterCacheKey("https://a.com/api/v1/models", true),
  );
});

test("ensureOpenRouterCatalog returns empty map on fetch failure without throwing", async () => {
  resetOpenRouterResolverCacheForTests();
  const originalFetch = globalThis.fetch;
  globalThis.fetch = (async () => {
    throw new Error("network unreachable");
  }) as typeof fetch;

  try {
    const registry = loadModelRegistryConfig(baseRuntimeConfig({ modelsConfigPath: OPENROUTER_REGISTRY }));
    const result = await ensureOpenRouterCatalog(registry, {
      openRouterApiKey: null,
      openRouterBaseUrl: null,
      modelMetadataCacheTtlSeconds: 86_400,
    });
    assert.ok(result.modelsById instanceof Map);
    assert.equal(result.modelsById.size, 0);
  } finally {
    globalThis.fetch = originalFetch;
    resetOpenRouterResolverCacheForTests();
  }
});

test("ensureOpenRouterCatalog caches independently per baseUrl", async () => {
  resetOpenRouterResolverCacheForTests();
  let fetchCount = 0;
  const originalFetch = globalThis.fetch;
  globalThis.fetch = (async () => {
    fetchCount += 1;
    return new Response(JSON.stringify({ data: [{ id: "openai/gpt-4", name: "GPT-4" }] }), {
      status: 200,
      headers: { "content-type": "application/json" },
    });
  }) as typeof fetch;

  try {
    const registry = loadModelRegistryConfig(baseRuntimeConfig({ modelsConfigPath: OPENROUTER_REGISTRY }));
    await ensureOpenRouterCatalog(registry, {
      openRouterApiKey: null,
      openRouterBaseUrl: "https://first.example/api/v1/models",
      modelMetadataCacheTtlSeconds: 86_400,
    });
    await ensureOpenRouterCatalog(registry, {
      openRouterApiKey: null,
      openRouterBaseUrl: "https://second.example/api/v1/models",
      modelMetadataCacheTtlSeconds: 86_400,
    });
    assert.equal(fetchCount, 2);

    await ensureOpenRouterCatalog(registry, {
      openRouterApiKey: null,
      openRouterBaseUrl: "https://first.example/api/v1/models",
      modelMetadataCacheTtlSeconds: 86_400,
    });
    assert.equal(fetchCount, 2);
  } finally {
    globalThis.fetch = originalFetch;
    resetOpenRouterResolverCacheForTests();
  }
});
