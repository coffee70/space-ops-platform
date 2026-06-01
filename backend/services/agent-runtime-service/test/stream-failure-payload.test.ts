import assert from "node:assert/strict";
import { webcrypto } from "node:crypto";
import test from "node:test";

import { ModelProviderRuntimeError } from "../src/ai/provider-errors.js";
import { AgentEventStream } from "../src/events/stream.js";
import { RunSequencer } from "../src/events/sequencer.js";
import { MemoryConversationStore } from "./helpers.js";

if (!globalThis.crypto) {
  Object.defineProperty(globalThis, "crypto", {
    value: webcrypto,
  });
}

function streamWithStore(store = new MemoryConversationStore()) {
  return {
    store,
    stream: new AgentEventStream({
      store,
      trace: {
        conversation_id: "conversation-1",
        agent_run_id: "run-1",
        request_id: "request-1",
      },
      sequencer: new RunSequencer(),
      now: () => new Date("2026-06-01T00:00:00.000Z"),
    }),
  };
}

test("run.failed is enriched for normalized provider rate limits", async () => {
  const { store, stream } = streamWithStore();
  const drain = stream.response.text();

  await stream.fail(
    new ModelProviderRuntimeError({
      category: "rate_limited",
      retryable: true,
      retry_after_ms: 8173,
      provider_type: "openai",
      provider_model_id: "gpt-5.5",
      provider_error_type: "tokens",
      provider_error_code: "rate_limit_exceeded",
      http_status: 429,
      message: "Please try again in 8.173s.",
    }),
    {
      context_packet_id: "ctx-1",
      tool_call_count: 12,
      assistant_text_length: 0,
      reasoning_text_length: 0,
    },
  );
  await drain;

  const failed = store.events.find((event) => event.event_type === "run.failed");
  assert.equal(failed?.payload.error_code, "model_provider_rate_limited");
  assert.equal(failed?.payload.category, "rate_limited");
  assert.equal(failed?.payload.retryable, true);
  assert.equal(failed?.payload.retry_after_ms, 8173);
  assert.equal(failed?.payload.provider_type, "openai");
  assert.equal(failed?.payload.tool_call_count, 12);
});

test("run.failed keeps generic runtime failures generic", async () => {
  const { store, stream } = streamWithStore();
  const drain = stream.response.text();

  await stream.fail(new Error("plain failure"));
  await drain;

  const failed = store.events.find((event) => event.event_type === "run.failed");
  assert.equal(failed?.payload.error_code, "agent_runtime_failed");
  assert.equal(failed?.payload.message, "plain failure");
});
