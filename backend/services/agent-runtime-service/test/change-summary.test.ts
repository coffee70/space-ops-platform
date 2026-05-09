import assert from "node:assert/strict";
import test from "node:test";

import { ChangeSummaryAggregator, resolveTargetUnit } from "../src/change-summary.js";
import type { ChangeSummaryRegistryClient, ChangeSummaryRegistryUnit } from "../src/change-summary.js";
import type { AgentEventStream } from "../src/events/stream.js";
import type { RawEventFact } from "../src/types.js";

interface RecordedEvent {
  type: string;
  payload: Record<string, unknown>;
}

function fakeStream(): { stream: AgentEventStream; events: RecordedEvent[] } {
  const events: RecordedEvent[] = [];
  const stream = {
    async emitEvent(type: string, payload: Record<string, unknown>) {
      events.push({ type, payload });
    },
    async emitRawEvents(_events: RawEventFact[] | undefined) {},
  } as unknown as AgentEventStream;
  return { stream, events };
}

function fakeRegistryClient(units: ChangeSummaryRegistryUnit[]): ChangeSummaryRegistryClient {
  return {
    async listUnits() {
      return units;
    },
  };
}

const DERIVED_TELEMETRY_UNIT: ChangeSummaryRegistryUnit = {
  unit_id: "derived-telemetry-service",
  source_path: "project/space-ops-platform/backend/services/derived-telemetry-service",
  service_slug: "derived-telemetry-service",
  application_id: null,
  runtime_kind: "service",
  capabilities: ["telemetry", "derived"],
  category: "telemetry",
};

const TELEMETRY_APP_UNIT: ChangeSummaryRegistryUnit = {
  unit_id: "telemetry",
  source_path: "project/space-ops-apps/mission-control-ui/src/applications/telemetry",
  service_slug: null,
  application_id: "telemetry",
  runtime_kind: "frontend_application",
  capabilities: ["telemetry-detail"],
  category: "frontend",
};

test("aggregator emits change.summary after create_working_branch + write + create_commit", async () => {
  const { stream, events } = fakeStream();
  const aggregator = new ChangeSummaryAggregator({
    stream,
    registryClient: fakeRegistryClient([DERIVED_TELEMETRY_UNIT]),
  });

  await aggregator.observeToolCompletion(
    "create_working_branch",
    { branch: "preview/x", from_branch: "main" },
    {
      branch: "preview/x",
      commit_sha: "baseline-sha",
      changed_files: [],
      data: { created: true, base_branch: "main", base_commit_sha: "baseline-sha" },
    },
  );
  await aggregator.observeToolCompletion(
    "write_source_file",
    {
      branch: "preview/x",
      path: "project/space-ops-platform/backend/services/derived-telemetry-service/app/main.py",
    },
    {
      branch: "preview/x",
      commit_sha: "baseline-sha",
      changed_files: ["project/space-ops-platform/backend/services/derived-telemetry-service/app/main.py"],
    },
  );
  await aggregator.observeToolCompletion(
    "create_commit",
    { branch: "preview/x", message: "Add preview" },
    {
      branch: "preview/x",
      commit_sha: "preview-sha",
      changed_files: ["project/space-ops-platform/backend/services/derived-telemetry-service/app/main.py"],
    },
  );

  assert.equal(events.length, 1);
  const event = events[0];
  assert.equal(event.type, "change.summary");
  assert.equal(event.payload.branch, "preview/x");
  assert.equal(event.payload.base_branch, "main");
  assert.equal(event.payload.base_commit_sha, "baseline-sha");
  assert.equal(event.payload.commit_sha, "preview-sha");
  assert.equal(event.payload.target_unit_id, "derived-telemetry-service");
  assert.equal(event.payload.target_application_id, null);
  assert.equal(event.payload.risk_level, "low");
  assert.equal(event.payload.validation_status, "not_run");
  assert.deepEqual(event.payload.changed_files, [
    "project/space-ops-platform/backend/services/derived-telemetry-service/app/main.py",
  ]);
});

test("aggregator does not double-emit for the same commit", async () => {
  const { stream, events } = fakeStream();
  const aggregator = new ChangeSummaryAggregator({
    stream,
    registryClient: fakeRegistryClient([DERIVED_TELEMETRY_UNIT]),
  });
  await aggregator.observeToolCompletion(
    "create_working_branch",
    { branch: "preview/y", from_branch: "main" },
    { branch: "preview/y", commit_sha: "b", data: { base_branch: "main", base_commit_sha: "b" } },
  );
  await aggregator.observeToolCompletion(
    "write_source_file",
    {
      branch: "preview/y",
      path: "project/space-ops-platform/backend/services/derived-telemetry-service/app/main.py",
    },
    {
      branch: "preview/y",
      commit_sha: "b",
      changed_files: ["project/space-ops-platform/backend/services/derived-telemetry-service/app/main.py"],
    },
  );
  await aggregator.observeToolCompletion(
    "create_commit",
    { branch: "preview/y", message: "Initial preview" },
    {
      branch: "preview/y",
      commit_sha: "c",
      changed_files: ["project/space-ops-platform/backend/services/derived-telemetry-service/app/main.py"],
    },
  );
  // Re-emitting the same commit (no new write/commit) must not produce a second event.
  await aggregator.observeToolCompletion(
    "create_commit",
    { branch: "preview/y", message: "Same commit retry" },
    {
      branch: "preview/y",
      commit_sha: "c",
      changed_files: ["project/space-ops-platform/backend/services/derived-telemetry-service/app/main.py"],
    },
  );
  assert.equal(events.length, 1);
});

test("aggregator emits a fresh summary for a follow-up commit on the same branch", async () => {
  const { stream, events } = fakeStream();
  const aggregator = new ChangeSummaryAggregator({
    stream,
    registryClient: fakeRegistryClient([DERIVED_TELEMETRY_UNIT]),
  });
  await aggregator.observeToolCompletion(
    "create_working_branch",
    { branch: "preview/z", from_branch: "main" },
    { branch: "preview/z", commit_sha: "b", data: { base_branch: "main", base_commit_sha: "b" } },
  );
  await aggregator.observeToolCompletion(
    "write_source_file",
    {
      branch: "preview/z",
      path: "project/space-ops-platform/backend/services/derived-telemetry-service/app/main.py",
    },
    {
      branch: "preview/z",
      commit_sha: "b",
      changed_files: ["project/space-ops-platform/backend/services/derived-telemetry-service/app/main.py"],
    },
  );
  await aggregator.observeToolCompletion(
    "create_commit",
    { branch: "preview/z", message: "First commit" },
    { branch: "preview/z", commit_sha: "c1", changed_files: [] },
  );
  await aggregator.observeToolCompletion(
    "write_source_file",
    {
      branch: "preview/z",
      path: "project/space-ops-platform/backend/services/derived-telemetry-service/app/handlers.py",
    },
    {
      branch: "preview/z",
      commit_sha: "c1",
      changed_files: ["project/space-ops-platform/backend/services/derived-telemetry-service/app/handlers.py"],
    },
  );
  await aggregator.observeToolCompletion(
    "create_commit",
    { branch: "preview/z", message: "Second commit" },
    { branch: "preview/z", commit_sha: "c2", changed_files: [] },
  );

  assert.equal(events.length, 2);
  assert.equal(events[1].payload.commit_sha, "c2");
  assert.deepEqual(events[1].payload.changed_files, [
    "project/space-ops-platform/backend/services/derived-telemetry-service/app/handlers.py",
    "project/space-ops-platform/backend/services/derived-telemetry-service/app/main.py",
  ]);
});

test("aggregator suppresses change.summary when commit lands on base branch", async () => {
  const { stream, events } = fakeStream();
  const aggregator = new ChangeSummaryAggregator({
    stream,
    registryClient: fakeRegistryClient([DERIVED_TELEMETRY_UNIT]),
    logger: { warn: () => {} },
  });
  // Branch == from_branch is the unsafe shape: agent committed straight to main.
  await aggregator.observeToolCompletion(
    "create_working_branch",
    { branch: "main", from_branch: "main" },
    { branch: "main", commit_sha: "abc", data: { base_branch: "main", base_commit_sha: "abc" } },
  );
  await aggregator.observeToolCompletion(
    "write_source_file",
    { branch: "main", path: "project/space-ops-platform/backend/services/derived-telemetry-service/app/main.py" },
    { branch: "main", commit_sha: "abc", changed_files: ["project/space-ops-platform/backend/services/derived-telemetry-service/app/main.py"] },
  );
  await aggregator.observeToolCompletion(
    "create_commit",
    { branch: "main", message: "no preview" },
    { branch: "main", commit_sha: "def", changed_files: [] },
  );
  assert.equal(events.length, 0);
});

test("aggregator emits null target when no unit matches", async () => {
  const { stream, events } = fakeStream();
  const aggregator = new ChangeSummaryAggregator({
    stream,
    registryClient: fakeRegistryClient([DERIVED_TELEMETRY_UNIT]),
  });
  await aggregator.observeToolCompletion(
    "create_working_branch",
    { branch: "preview/random", from_branch: "main" },
    { branch: "preview/random", commit_sha: "b", data: { base_branch: "main", base_commit_sha: "b" } },
  );
  await aggregator.observeToolCompletion(
    "write_source_file",
    { branch: "preview/random", path: "docs/random/notes.md" },
    { branch: "preview/random", commit_sha: "b", changed_files: ["docs/random/notes.md"] },
  );
  await aggregator.observeToolCompletion(
    "create_commit",
    { branch: "preview/random", message: "Update docs" },
    { branch: "preview/random", commit_sha: "c", changed_files: [] },
  );
  assert.equal(events.length, 1);
  assert.equal(events[0].payload.target_unit_id, null);
  assert.equal(events[0].payload.target_application_id, null);
  assert.equal(events[0].payload.affected_capability, "platform-change");
});

test("aggregator emits null target when changes span multiple units (ambiguous)", async () => {
  const { stream, events } = fakeStream();
  const aggregator = new ChangeSummaryAggregator({
    stream,
    registryClient: fakeRegistryClient([DERIVED_TELEMETRY_UNIT, TELEMETRY_APP_UNIT]),
  });
  await aggregator.observeToolCompletion(
    "create_working_branch",
    { branch: "preview/multi", from_branch: "main" },
    { branch: "preview/multi", commit_sha: "b", data: { base_branch: "main", base_commit_sha: "b" } },
  );
  await aggregator.observeToolCompletion(
    "write_source_file",
    {
      branch: "preview/multi",
      path: "project/space-ops-platform/backend/services/derived-telemetry-service/app/main.py",
    },
    { branch: "preview/multi", commit_sha: "b", changed_files: ["project/space-ops-platform/backend/services/derived-telemetry-service/app/main.py"] },
  );
  await aggregator.observeToolCompletion(
    "write_source_file",
    {
      branch: "preview/multi",
      path: "project/space-ops-apps/mission-control-ui/src/applications/telemetry/index.ts",
    },
    { branch: "preview/multi", commit_sha: "b", changed_files: ["project/space-ops-apps/mission-control-ui/src/applications/telemetry/index.ts"] },
  );
  await aggregator.observeToolCompletion(
    "create_commit",
    { branch: "preview/multi", message: "Cross-unit change" },
    { branch: "preview/multi", commit_sha: "c", changed_files: [] },
  );
  assert.equal(events.length, 1);
  assert.equal(events[0].payload.target_unit_id, null);
  assert.equal(events[0].payload.target_application_id, null);
});

test("resolveTargetUnit picks the longest matching source_path", () => {
  const result = resolveTargetUnit(
    [DERIVED_TELEMETRY_UNIT, TELEMETRY_APP_UNIT],
    ["project/space-ops-apps/mission-control-ui/src/applications/telemetry/page.tsx"],
  );
  assert.equal(result.ambiguous, false);
  assert.equal(result.unit?.unit_id, "telemetry");
});
