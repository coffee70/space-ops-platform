import assert from "node:assert/strict";
import test from "node:test";

import { createModelRunner } from "../src/ai/model.js";
import { baseRuntimeConfig } from "./helpers.js";

test("model runner throws when OpenAI API key is missing", async () => {
  const runner = createModelRunner(baseRuntimeConfig());
  const iterator = runner.stream({
    system: "s",
    messages: [{ role: "user", content: "hi" }],
    tools: {},
    maxSteps: 1,
    model: {
      id: "m1",
      providerType: "openai",
      providerModelId: "gpt-4o-mini",
      apiKey: null,
      baseUrl: null,
    },
  });
  await assert.rejects(
    async () => {
      for await (const _ of iterator) {
        /* consume */
      }
    },
    (err: unknown) => err instanceof Error && /missing required API key.*openai/i.test(err.message),
  );
});

test("model runner throws when Anthropic API key is missing", async () => {
  const runner = createModelRunner(baseRuntimeConfig());
  const iterator = runner.stream({
    system: "s",
    messages: [{ role: "user", content: "hi" }],
    tools: {},
    maxSteps: 1,
    model: {
      id: "m1",
      providerType: "anthropic",
      providerModelId: "claude-3-5-sonnet-latest",
      apiKey: null,
      baseUrl: null,
    },
  });
  await assert.rejects(
    async () => {
      for await (const _ of iterator) {
        /* consume */
      }
    },
    (err: unknown) => err instanceof Error && /missing required API key.*anthropic/i.test(err.message),
  );
});

test("model runner throws when OpenAI-compatible baseUrl is missing", async () => {
  const runner = createModelRunner(baseRuntimeConfig());
  const iterator = runner.stream({
    system: "s",
    messages: [{ role: "user", content: "hi" }],
    tools: {},
    maxSteps: 1,
    model: {
      id: "m1",
      providerType: "openai-compatible",
      providerModelId: "local-model",
      apiKey: "local",
      baseUrl: null,
    },
  });
  await assert.rejects(
    async () => {
      for await (const _ of iterator) {
        /* consume */
      }
    },
    (err: unknown) => err instanceof Error && /missing required baseUrl/i.test(err.message),
  );
});
