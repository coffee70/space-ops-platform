import assert from "node:assert/strict";
import test from "node:test";

import { ModelSelectionError } from "../src/ai/model-errors.js";
import { HttpModelRegistryClient } from "../src/clients/model-registry.js";
import { baseRuntimeConfig } from "./helpers.js";

async function captureResolveError(body: unknown): Promise<ModelSelectionError> {
  const originalFetch = globalThis.fetch;
  globalThis.fetch = (async () =>
    new Response(JSON.stringify(body), {
      status: 400,
      headers: { "content-type": "application/json" },
    })) as typeof fetch;

  try {
    const client = new HttpModelRegistryClient(
      baseRuntimeConfig({
        modelRegistryBaseUrl: "http://model-registry-service:8080",
      }),
    );
    await client.resolveForChat("disabled-model", "read_only");
  } catch (error) {
    assert.ok(error instanceof ModelSelectionError);
    return error;
  } finally {
    globalThis.fetch = originalFetch;
  }

  throw new Error("Expected model registry client to throw");
}

test("model registry client preserves valid model selection error codes", async () => {
  const error = await captureResolveError({
    detail: {
      message: "Model is disabled",
      code: "model_disabled",
    },
  });

  assert.equal(error.code, "model_disabled");
  assert.equal(error.message, "Model is disabled");
});

test("model registry client rejects invalid error code strings", async () => {
  const error = await captureResolveError({
    detail: {
      message: "Invalid model",
      code: "totally_invalid_code",
    },
  });

  assert.equal(error.code, "unknown_model");
  assert.equal(error.message, "Invalid model");
});
