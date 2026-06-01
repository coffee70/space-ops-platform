import assert from "node:assert/strict";
import test from "node:test";

import { createModelRunner } from "../src/ai/model.js";
import { ModelProviderRuntimeError } from "../src/ai/provider-errors.js";
import type { ModelStreamPart, ResolvedRuntimeModel } from "../src/types.js";
import { baseRuntimeConfig } from "./helpers.js";

const model: ResolvedRuntimeModel = {
  id: "test-model",
  providerType: "openai",
  providerModelId: "gpt-5.5",
  apiKey: "test-key",
  baseUrl: null,
};

function rateLimitError() {
  return {
    status: 429,
    code: "rate_limit_exceeded",
    message: "TPM limit reached. Please try again in 0s.",
  };
}

async function* streamParts(parts: ModelStreamPart[]): AsyncIterable<ModelStreamPart> {
  for (const part of parts) {
    yield part;
  }
}

async function collect(runner: ReturnType<typeof createModelRunner>, events: Array<{ eventType: string; payload: Record<string, unknown> }>) {
  const parts: ModelStreamPart[] = [];
  for await (const part of runner.stream({
    system: "system",
    messages: [{ role: "user", content: "hello" }],
    tools: {},
    maxSteps: 2,
    model,
    onRuntimeEvent: async (eventType, payload) => {
      events.push({ eventType, payload });
    },
  })) {
    parts.push(part);
  }
  return parts;
}

test("retries safe provider errors before any output", async () => {
  let attempts = 0;
  const runner = createModelRunner(baseRuntimeConfig({ modelRetryMaxAttempts: 2 }), {
    streamText: (() => {
      attempts += 1;
      if (attempts === 1) {
        throw rateLimitError();
      }
      return { fullStream: streamParts([{ type: "text-delta", text: "recovered" }]) };
    }) as never,
  });
  const events: Array<{ eventType: string; payload: Record<string, unknown> }> = [];

  const parts = await collect(runner, events);

  assert.equal(attempts, 2);
  assert.equal(parts[0]?.type, "text-delta");
  assert.deepEqual(events.map((event) => event.eventType), ["model.provider_error", "model.retry_scheduled", "model.retrying"]);
});

test("does not retry after emitted text, reasoning, or tool state", async () => {
  for (const unsafePart of [
    { type: "text-delta", text: "partial" },
    { type: "reasoning", textDelta: "thinking" },
    { type: "tool-call", toolCallId: "tool-1", toolName: "inspect" },
  ] satisfies ModelStreamPart[]) {
    let attempts = 0;
    const runner = createModelRunner(baseRuntimeConfig({ modelRetryMaxAttempts: 2 }), {
      streamText: (() => {
        attempts += 1;
        return {
          fullStream: streamParts([unsafePart, { type: "error", error: rateLimitError() }]),
        };
      }) as never,
    });
    const events: Array<{ eventType: string; payload: Record<string, unknown> }> = [];

    await assert.rejects(() => collect(runner, events), ModelProviderRuntimeError);
    assert.equal(attempts, 1);
    assert.equal(events.some((event) => event.eventType === "model.retry_scheduled"), false);
  }
});

test("throws normalized provider error when retries are exhausted", async () => {
  const runner = createModelRunner(baseRuntimeConfig({ modelRetryMaxAttempts: 2 }), {
    streamText: (() => {
      throw rateLimitError();
    }) as never,
  });
  const events: Array<{ eventType: string; payload: Record<string, unknown> }> = [];

  await assert.rejects(
    () => collect(runner, events),
    (error: unknown) => error instanceof ModelProviderRuntimeError && error.normalized.category === "rate_limited",
  );
  assert.equal(events.filter((event) => event.eventType === "model.provider_error").length, 2);
});

test("abort during retry sleep is surfaced as cancellation", async () => {
  const controller = new AbortController();
  const runner = createModelRunner(baseRuntimeConfig({ modelRetryMaxAttempts: 2, modelRetryBaseDelayMs: 50, modelRetryMaxDelayMs: 50 }), {
    streamText: (() => {
      throw { status: 503, message: "provider overloaded" };
    }) as never,
  });

  await assert.rejects(
    async () => {
      for await (const _part of runner.stream({
        system: "system",
        messages: [{ role: "user", content: "hello" }],
        tools: {},
        maxSteps: 2,
        model,
        abortSignal: controller.signal,
        onRuntimeEvent: async (eventType) => {
          if (eventType === "model.retry_scheduled") {
            controller.abort();
          }
        },
      })) {
        /* consume */
      }
    },
    (error: unknown) => error instanceof Error && error.name === "AbortError",
  );
});

