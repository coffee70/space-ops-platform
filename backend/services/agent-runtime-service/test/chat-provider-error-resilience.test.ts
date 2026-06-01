import assert from "node:assert/strict";
import test from "node:test";

import { ModelProviderRuntimeError } from "../src/ai/provider-errors.js";
import { createApp } from "../src/server.js";
import type { ToolDefinition } from "../src/types.js";
import {
  baseRuntimeConfig,
  contextResolvedEvent,
  FakeContextClient,
  FakeModelCatalog,
  FakeToolExecutionClient,
  FakeToolRegistryClient,
  MemoryConversationStore,
  parseNdjson,
} from "./helpers.js";

const inspectTool: ToolDefinition = {
  name: "inspect_status",
  description: "Inspect status",
  category: "read",
  layer_target: "platform",
  read_write_classification: "read",
  required_execution_mode: "read_only",
  enabled: true,
  requires_confirmation: false,
  input_schema_json: {
    type: "object",
    properties: {
      target: { type: "string" },
    },
    required: ["target"],
    additionalProperties: false,
  },
};

function rateLimitRuntimeError() {
  return new ModelProviderRuntimeError({
    category: "rate_limited",
    retryable: true,
    retry_after_ms: 8173,
    provider_type: "openai",
    provider_model_id: "gpt-5.5",
    provider_error_type: "tokens",
    provider_error_code: "rate_limit_exceeded",
    http_status: 429,
    message: "TPM limit. Please try again in 8.173s.",
  });
}

test("chat preserves assistant metadata and enriched failure after completed tool calls", async () => {
  const store = new MemoryConversationStore();
  const conversation = await store.createConversation({
    title: "Provider failure",
    execution_mode: "read_only",
    initial_message: { role: "user", content: "Start session." },
  });

  const app = createApp({
    config: baseRuntimeConfig({ allowMissingKeyFallback: false }),
    store,
    contextClient: new FakeContextClient([contextResolvedEvent()]),
    toolRegistryClient: new FakeToolRegistryClient([inspectTool]),
    toolExecutionClient: new FakeToolExecutionClient((input) => ({
      conversation_id: input.trace.conversation_id,
      agent_run_id: input.trace.agent_run_id,
      request_id: input.trace.request_id,
      tool_call_id: input.trace.tool_call_id ?? "tool-1",
      status: "completed",
      output: { ok: true },
      raw_events: [
        {
          event_type: "tool.started",
          emitted_by: "tool-execution-service",
          tool_call_id: input.trace.tool_call_id,
          payload: {
            tool_name: input.tool_name,
            category: "read",
            read_write_classification: "read",
            input_preview: { target: "vehicle" },
          },
        },
        {
          event_type: "tool.completed",
          emitted_by: "tool-execution-service",
          tool_call_id: input.trace.tool_call_id,
          payload: {
            tool_name: input.tool_name,
            status: "completed",
            result_preview: { ok: true },
            duration_ms: 1,
          },
        },
      ],
    })),
    modelRunner: {
      async *stream(input) {
        await (input.tools.inspect_status as { execute: (args: unknown, options: unknown) => Promise<unknown> }).execute(
          { target: "vehicle" },
          { toolCallId: "tool-1" },
        );
        throw rateLimitRuntimeError();
      },
    },
    modelCatalog: new FakeModelCatalog(),
  });

  const response = await app.request("/chat", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      conversation_id: conversation.id,
      execution_mode: "read_only",
      messages: [{ role: "user", content: "Inspect and continue." }],
    }),
  });

  assert.equal(response.status, 200);
  const chunks = parseNdjson(await response.text()) as Array<{ kind: "event"; event: { event_type: string; payload: Record<string, unknown> } }>;
  const failed = chunks.find((chunk) => chunk.event.event_type === "run.failed")?.event.payload;

  assert.equal(failed?.error_code, "model_provider_rate_limited");
  assert.equal(failed?.tool_call_count, 1);
  assert.equal(failed?.context_packet_id, "ctx-1");

  const updatedConversation = await store.getConversation(conversation.id);
  const assistant = updatedConversation?.messages.findLast((message) => message.role === "assistant");
  assert.equal(assistant?.metadata_json.completion_status, "interrupted_provider_retryable");
  assert.equal(assistant?.metadata_json.tool_call_count, 1);
  assert.equal(assistant?.metadata_json.can_continue, true);
});

