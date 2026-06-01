import assert from "node:assert/strict";
import test from "node:test";

import { ModelBudgetTracker } from "../src/ai/model-budget.js";
import type { LanguageModelUsageSnapshot } from "../src/ai/model-usage.js";
import { EphemeralModelUsageStore } from "../src/storage/model-usage-store.js";
import type { RuntimeBudgetConfig } from "../src/types.js";
import { baseRuntimeConfig } from "./helpers.js";

const budget: RuntimeBudgetConfig = {
  contextWindowTokens: 1000,
  maxOutputTokens: 250,
  tokensPerMinute: 1000,
  requestsPerMinute: null,
  rollingWindowSeconds: 60,
};

const baseInput = {
  agentRunId: "11111111-1111-4111-8111-111111111111",
  providerType: "openai",
  providerModelId: "gpt-4o-mini",
  modelId: "m1",
  requestId: "req-1",
  budget,
};

function usage(totalTokens: number, source: LanguageModelUsageSnapshot["source"], stepIndex: number | null): LanguageModelUsageSnapshot {
  return {
    input_tokens: totalTokens,
    output_tokens: 0,
    total_tokens: totalTokens,
    reasoning_tokens: null,
    cached_input_tokens: null,
    raw: null,
    source,
    step_index: stepIndex,
    synced_after: null,
    is_actual: source === "ai_sdk_step_usage" || source === "ai_sdk_total_usage",
  };
}

test("model budget snapshot includes step usage mid-run", async () => {
  const store = new EphemeralModelUsageStore();
  const tracker = new ModelBudgetTracker(store, baseRuntimeConfig());

  const snapshot = await tracker.recordActualUsage({
    ...baseInput,
    usage: usage(100, "ai_sdk_step_usage", 0),
    stepIndex: 0,
    nowMs: Date.UTC(2026, 0, 1, 0, 0, 0),
  });

  assert.equal(snapshot.context.used_tokens, 100);
  assert.equal(snapshot.throughput.used_tokens, 100);
});

test("model budget final total supersedes step-summed context usage without double-counting throughput", async () => {
  const store = new EphemeralModelUsageStore();
  const tracker = new ModelBudgetTracker(store, baseRuntimeConfig());
  const nowMs = Date.UTC(2026, 0, 1, 0, 0, 0);

  await tracker.recordActualUsage({ ...baseInput, usage: usage(100, "ai_sdk_step_usage", 0), stepIndex: 0, nowMs });
  await tracker.recordActualUsage({ ...baseInput, requestId: "req-2", usage: usage(200, "ai_sdk_step_usage", 1), stepIndex: 1, nowMs });
  const snapshot = await tracker.recordActualUsage({ ...baseInput, requestId: "req-final", usage: usage(250, "ai_sdk_total_usage", null), nowMs });

  assert.equal(snapshot.context.used_tokens, 250);
  assert.equal(snapshot.throughput.used_tokens, 300);
});

test("model budget falls back to step-summed usage when final total is missing", async () => {
  const store = new EphemeralModelUsageStore();
  const tracker = new ModelBudgetTracker(store, baseRuntimeConfig());
  const nowMs = Date.UTC(2026, 0, 1, 0, 0, 0);

  await tracker.recordActualUsage({ ...baseInput, usage: usage(100, "ai_sdk_step_usage", 0), stepIndex: 0, nowMs });
  const snapshot = await tracker.recordActualUsage({ ...baseInput, requestId: "req-2", usage: usage(200, "ai_sdk_step_usage", 1), stepIndex: 1, nowMs });

  assert.equal(snapshot.context.used_tokens, 300);
  assert.equal(snapshot.throughput.used_tokens, 300);
});

test("ephemeral usage store applies rolling throughput window", async () => {
  let nowMs = Date.UTC(2026, 0, 1, 0, 0, 0);
  const store = new EphemeralModelUsageStore(() => new Date(nowMs));
  const tracker = new ModelBudgetTracker(store, baseRuntimeConfig());

  await tracker.recordActualUsage({ ...baseInput, usage: usage(100, "ai_sdk_step_usage", 0), stepIndex: 0, nowMs });
  nowMs += 30_000;
  await tracker.recordActualUsage({ ...baseInput, requestId: "req-2", usage: usage(200, "ai_sdk_step_usage", 1), stepIndex: 1, nowMs });
  nowMs += 40_000;

  const snapshot = await tracker.snapshot({
    ...baseInput,
    usage: null,
    source: "mixed",
    nowMs,
  });

  assert.equal(snapshot.context.used_tokens, 300);
  assert.equal(snapshot.throughput.used_tokens, 200);
  assert.equal(snapshot.throughput.seconds_until_reset, 20);
  assert.equal(snapshot.throughput.reset_at, new Date(Date.UTC(2026, 0, 1, 0, 1, 30)).toISOString());
});
