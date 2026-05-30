import assert from "node:assert/strict";
import test from "node:test";

import { AGENT_EVENT_REQUIRED_PAYLOAD_FIELDS, validateAgentEventPayload } from "../src/events/schema.js";

function validPayload(eventType: keyof typeof AGENT_EVENT_REQUIRED_PAYLOAD_FIELDS): Record<string, unknown> {
  return Object.fromEntries(AGENT_EVENT_REQUIRED_PAYLOAD_FIELDS[eventType].map((field) => [field, `${field}-value`]));
}

const TOOL_CALL_EVENT_TYPES = new Set([
  "tool.started",
  "tool.completed",
  "tool.failed",
  "tool.permission_required",
  "tool.permission_approved",
  "tool.permission_denied",
]);

test("every fixed event type validates required payload fields", () => {
  for (const eventType of Object.keys(AGENT_EVENT_REQUIRED_PAYLOAD_FIELDS) as Array<keyof typeof AGENT_EVENT_REQUIRED_PAYLOAD_FIELDS>) {
    assert.equal(validateAgentEventPayload(eventType, validPayload(eventType), TOOL_CALL_EVENT_TYPES.has(eventType) ? "tool-call-1" : null), eventType);
  }
});

test("missing required payload fields and unsupported event types are rejected", () => {
  assert.throws(() => validateAgentEventPayload("run.started", { execution_mode: "read_only" }), /missing required payload/);
  assert.throws(() => validateAgentEventPayload("run.unknown", {}), /Invalid enum value|invalid/i);
});

test("tool lifecycle events require tool_call_id", () => {
  assert.throws(() => validateAgentEventPayload("tool.completed", validPayload("tool.completed"), null), /requires tool_call_id/);
});

test("tool schema diagnostics do not require a tool_call_id", () => {
  assert.equal(
    validateAgentEventPayload(
      "tool.schema_invalid",
      {
        tool_name: "bad_tool",
        reason: 'tool input schema root type must be "object"',
        action: "omitted_from_model_toolset",
      },
      null,
    ),
    "tool.schema_invalid",
  );
});

test("permission tool events validate required payloads and event-level tool_call_id", () => {
  assert.equal(
    validateAgentEventPayload(
      "tool.permission_required",
      {
        tool_name: "deploy_preview_change",
        tool_call_id: "tool-call-1",
        permission_request_id: "permission-1",
        execution_mode: "execute",
        prompt: { title: "Deploy preview changes?" },
      },
      "tool-call-1",
    ),
    "tool.permission_required",
  );
  assert.equal(
    validateAgentEventPayload(
      "tool.permission_approved",
      {
        tool_name: "deploy_preview_change",
        tool_call_id: "tool-call-1",
        permission_request_id: "permission-1",
      },
      "tool-call-1",
    ),
    "tool.permission_approved",
  );
  assert.equal(
    validateAgentEventPayload(
      "tool.permission_denied",
      {
        tool_name: "deploy_preview_change",
        tool_call_id: "tool-call-1",
        permission_request_id: "permission-1",
        reason: "user_denied",
      },
      "tool-call-1",
    ),
    "tool.permission_denied",
  );
});

test("permission tool events still reject missing fields, missing tool_call_id, and unknown types", () => {
  assert.throws(
    () =>
      validateAgentEventPayload(
        "tool.permission_required",
        {
          tool_name: "deploy_preview_change",
          tool_call_id: "tool-call-1",
          execution_mode: "execute",
          prompt: { title: "Deploy preview changes?" },
        },
        "tool-call-1",
      ),
    /missing required payload.*permission_request_id/,
  );
  assert.throws(
    () => validateAgentEventPayload("tool.permission_denied", validPayload("tool.permission_denied"), null),
    /requires tool_call_id/,
  );
  assert.throws(() => validateAgentEventPayload("tool.permission_expired", {}, "tool-call-1"), /Invalid enum value|invalid/i);
});
