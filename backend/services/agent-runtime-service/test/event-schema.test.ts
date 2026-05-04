import assert from "node:assert/strict";
import test from "node:test";

import { AGENT_EVENT_REQUIRED_PAYLOAD_FIELDS, validateAgentEventPayload } from "../src/events/schema.js";

function validPayload(eventType: keyof typeof AGENT_EVENT_REQUIRED_PAYLOAD_FIELDS): Record<string, unknown> {
  return Object.fromEntries(AGENT_EVENT_REQUIRED_PAYLOAD_FIELDS[eventType].map((field) => [field, `${field}-value`]));
}

test("every fixed event type validates required payload fields", () => {
  for (const eventType of Object.keys(AGENT_EVENT_REQUIRED_PAYLOAD_FIELDS) as Array<keyof typeof AGENT_EVENT_REQUIRED_PAYLOAD_FIELDS>) {
    assert.equal(validateAgentEventPayload(eventType, validPayload(eventType), eventType.startsWith("tool.") ? "tool-call-1" : null), eventType);
  }
});

test("missing required payload fields and unsupported event types are rejected", () => {
  assert.throws(() => validateAgentEventPayload("run.started", { execution_mode: "read_only" }), /missing required payload/);
  assert.throws(() => validateAgentEventPayload("run.unknown", {}), /Invalid enum value|invalid/i);
});

test("tool lifecycle events require tool_call_id", () => {
  assert.throws(() => validateAgentEventPayload("tool.completed", validPayload("tool.completed"), null), /requires tool_call_id/);
});
