import assert from "node:assert/strict";
import test from "node:test";

import { normalizeModelProviderError } from "../src/ai/provider-errors.js";

function normalize(error: unknown) {
  return normalizeModelProviderError({
    error,
    providerType: "openai",
    providerModelId: "gpt-5.5",
  });
}

test("normalizes OpenAI-style TPM rate limit with retry delay", () => {
  const normalized = normalize({
    code: "rate_limit_exceeded",
    type: "tokens",
    message: "Rate limit reached for TPM. Please try again in 8.173s.",
  });

  assert.equal(normalized.category, "rate_limited");
  assert.equal(normalized.retryable, true);
  assert.equal(normalized.retry_after_ms, 8173);
  assert.equal(normalized.provider_error_code, "rate_limit_exceeded");
});

test("normalizes Anthropic-style 429 rate limit", () => {
  const normalized = normalize({
    status: 429,
    error: {
      type: "rate_limit_error",
      message: "Your account has hit a rate limit.",
    },
  });

  assert.equal(normalized.category, "rate_limited");
  assert.equal(normalized.retryable, true);
});

test("normalizes OpenAI-compatible Retry-After header", () => {
  const normalized = normalize({
    statusCode: 429,
    headers: { "Retry-After": "10" },
    error: { message: "Gateway rate limit" },
  });

  assert.equal(normalized.category, "rate_limited");
  assert.equal(normalized.retry_after_ms, 10_000);
});

test("normalizes context length, auth, overload, timeout, quota, and cancellation categories", () => {
  assert.equal(normalize({ message: "maximum context length exceeded: input too long" }).category, "context_length_exceeded");
  assert.equal(normalize({ status: 401, code: "invalid_api_key", message: "bad key" }).category, "auth_failed");
  assert.equal(normalize({ status: 503, message: "provider overloaded" }).category, "provider_overloaded");
  assert.equal(normalize({ code: "ETIMEDOUT", message: "network timeout" }).category, "network_transient");
  assert.equal(normalize({ code: "insufficient_quota", message: "check billing details" }).category, "quota_exceeded");

  const abort = new Error("aborted");
  abort.name = "AbortError";
  assert.equal(normalize(abort).category, "cancelled");
});

test("parses compact minute and second retry delay from provider message", () => {
  const normalized = normalize({
    status: 429,
    message: "Please try again in 1m2s.",
  });

  assert.equal(normalized.category, "rate_limited");
  assert.equal(normalized.retry_after_ms, 62_000);
});

