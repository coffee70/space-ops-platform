import assert from "node:assert/strict";
import test from "node:test";

import { createApp } from "../src/server.js";
import { FakeContextClient, FakeToolExecutionClient, FakeToolRegistryClient, MemoryConversationStore, parseNdjson } from "./helpers.js";

test("chat orchestration emits backend-owned run, context, tool, and completion events", async () => {
  const store = new MemoryConversationStore();
  const conversation = await store.createConversation({
    title: "AI Engineer Session",
    execution_mode: "read_only",
  });
  const toolRegistry = new FakeToolRegistryClient([
    {
      name: "get_runtime_service",
      description: "Get runtime service details.",
      category: "layer1_runtime",
      layer_target: "layer1",
      read_write_classification: "read",
      required_execution_mode: "read_only",
      enabled: true,
      requires_confirmation: false,
      input_schema_json: {
        type: "object",
        properties: {
          service_slug: {
            type: "string",
            description: "Service slug to inspect.",
          },
        },
        required: ["service_slug"],
      },
    },
  ]);

  const toolExecution = new FakeToolExecutionClient({
    conversation_id: conversation.id,
    agent_run_id: "ignored",
    request_id: "ignored",
    tool_call_id: "tool-call-1",
    status: "completed",
    output: { service_slug: "agent-runtime-service" },
    raw_events: [
      {
        event_type: "tool.completed",
        emitted_by: "tool-execution-service",
        tool_call_id: "tool-call-1",
        payload: {
          tool_name: "get_runtime_service",
          status: "completed",
        },
      },
    ],
  });

  const app = createApp({
    config: {
      port: 8080,
      databaseUrl: "postgres://example",
      controlPlaneUrl: "http://localhost:8100",
      openAiApiKey: "test-key",
      openAiBaseUrl: null,
      modelId: "gpt-4o-mini",
      maxSteps: 3,
      requestTimeoutMs: 1000,
    },
    store,
    contextClient: new FakeContextClient([
      {
        event_type: "context.resolved",
        emitted_by: "context-retrieval-service",
        payload: { context_packet_id: "ctx-1" },
      },
    ]),
    toolRegistryClient: toolRegistry,
    toolExecutionClient: toolExecution,
    modelRunner: {
      async *stream(input) {
        const runtimeTool = input.tools.get_runtime_service as {
          execute: (args: { service_slug: string }, options: { toolCallId: string; messages: [] }) => Promise<unknown>;
        };
        await runtimeTool.execute({ service_slug: "agent-runtime-service" }, { toolCallId: "tool-call-1", messages: [] });
        yield { type: "text-delta", textDelta: "Runtime service ownership corrected." };
        yield { type: "finish", finishReason: "stop" };
      },
    },
    createId: (() => {
      const ids = ["agent-run-1", "request-1"];
      return () => ids.shift() ?? crypto.randomUUID();
    })(),
  });

  const response = await app.request("/agent/chat", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      conversation_id: conversation.id,
      execution_mode: "read_only",
      messages: [{ role: "user", content: "Inspect the runtime service." }],
    }),
  });

  assert.equal(response.status, 200);
  const chunks = parseNdjson(await response.text());
  const events = chunks.filter((chunk) => chunk.kind === "event") as Array<{
    event: { event_type: string; agent_run_id: string; request_id: string; sequence: number };
  }>;

  assert.deepEqual(
    events.map((event) => event.event.event_type),
    ["run.started", "context.requested", "context.resolved", "tool.started", "tool.completed", "message.completed", "run.completed"],
  );
  assert.deepEqual(
    events.map((event) => event.event.agent_run_id),
    ["agent-run-1", "agent-run-1", "agent-run-1", "agent-run-1", "agent-run-1", "agent-run-1", "agent-run-1"],
  );
  assert.deepEqual(
    events.map((event) => event.event.request_id),
    ["request-1", "request-1", "request-1", "request-1", "request-1", "request-1", "request-1"],
  );
  assert.deepEqual(
    events.map((event) => event.event.sequence),
    [1, 2, 3, 4, 5, 7, 8],
  );

  const delta = chunks.find((chunk) => chunk.kind === "message.delta") as { delta: string; agent_run_id: string; request_id: string };
  assert.equal(delta.delta, "Runtime service ownership corrected.");
  assert.equal(delta.agent_run_id, "agent-run-1");
  assert.equal(delta.request_id, "request-1");
  assert.deepEqual(toolRegistry.traces, [
    {
      conversation_id: conversation.id,
      agent_run_id: "agent-run-1",
      request_id: "request-1",
      tool_call_id: null,
    },
  ]);
  assert.equal(toolExecution.calls.length, 1);
});
