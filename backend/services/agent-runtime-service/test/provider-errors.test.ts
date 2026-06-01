import { strict as assert } from "node:assert";
import test from "node:test";

import { normalizeModelProviderError } from "../src/ai/provider-errors.js";

test("normalizes OpenAI-style TPM rate limit errors with retry delay", () => {
  const error = normalizeModelProviderError({
    providerType: "openai",
    providerModelId: "gpt-5.5",
    error: {
      status: 429,
      error: {
        code: "rate_limit_exceeded",
        type: "tokens",
        message: "TPM exceeded. Please try again in 8.173s.",
      },
    },
  });

  assert.equal(error.category, "rate_limited");
  assert.equal(error.retryable, true);
  assert.equal(error.retry_after_ms, 8173);
  assert.equal(error.provider_error_code, "rate_limit_exceeded");
});

test("normalizes gateway Retry-After headers", () => {
  const error = normalizeModelProviderError({
    providerType: "openai-compatible",
    providerModelId: "gateway-model",
    error: {
      statusCode: 429,
      message: "rate limit",
      headers: { "retry-after": "10" },
    },
  });

  assert.equal(error.category, "rate_limited");
  assert.equal(error.retry_after_ms, 10000);
});

test("classifies context, auth, overload, and network errors", () => {
  assert.equal(
    normalizeModelProviderError({
      providerType: "anthropic",
      providerModelId: "claude",
      error: { message: "maximum context length exceeded" },
    }).category,
    "context_length_exceeded",
  );
  assert.equal(
    normalizeModelProviderError({
      providerType: "openai",
      providerModelId: "gpt",
      error: { status: 401, message: "invalid_api_key" },
    }).category,
    "auth_failed",
  );
  assert.equal(
    normalizeModelProviderError({
      providerType: "anthropic",
      providerModelId: "claude",
      error: { status: 529, message: "overloaded" },
    }).retryable,
    true,
  );
  assert.equal(
    normalizeModelProviderError({
      providerType: "openai-compatible",
      providerModelId: "local",
      error: { code: "ETIMEDOUT", message: "network timeout" },
    }).category,
    "network_transient",
  );
});
