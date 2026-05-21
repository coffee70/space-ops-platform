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

test("permission tool events validate required payloads and event-level tool_call_id", () => {
  assert.equal(
    validateAgentEventPayload(
      "tool.permission_required",
      {
        tool_name: "deploy_preview_change",
        tool_call_id: "tool-call-1",
        permission_request_id: "permission-1",
        approval_token: "approval-token",
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
          permission_request_id: "permission-1",
          execution_mode: "execute",
          prompt: { title: "Deploy preview changes?" },
        },
        "tool-call-1",
      ),
    /missing required payload.*approval_token/,
  );
  assert.throws(
    () => validateAgentEventPayload("tool.permission_denied", validPayload("tool.permission_denied"), null),
    /requires tool_call_id/,
  );
  assert.throws(() => validateAgentEventPayload("tool.permission_expired", {}, "tool-call-1"), /Invalid enum value|invalid/i);
});
