import assert from "node:assert/strict";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { existsSync, mkdirSync, readFileSync, writeFileSync, rmSync } from "node:fs";

import { loadConfig } from "../src/config.js";
import { compileRegistryYamlContent } from "../src/ai/model-registry-config.js";
import { createApp, ensureModelRegistryConfigFile } from "../src/server.js";

const REASONING_VALID = `version: 1
defaults:
  chatModel: anthropic-thinking
  codingModel: anthropic-thinking
  fastModel: anthropic-thinking
  restrictedModel: anthropic-thinking
providers:
  anthropic-main:
    type: anthropic
    displayName: Anthropic
    apiKeyEnv: ANTHROPIC_API_KEY
models:
  - id: anthropic-thinking
    providerRef: anthropic-main
    providerModelId: claude-sonnet-4-6
    enabled: true
    defaultFor: [chat, coding, fast]
    reasoning:
      enabled: true
      representation: thinking
      providerOptions:
        anthropic:
          thinking:
            type: adaptive
`;

const MIN_VALID = `version: 1
defaults:
  chatModel: m1
  codingModel: m1
  fastModel: m1
  restrictedModel: m1
chat_title_generation:
  model_id: m1
providers:
  p1:
    type: openai
    displayName: OpenAI
    apiKeyEnv: OPENAI_API_KEY
models:
  - id: m1
    providerRef: p1
    providerModelId: gpt-4o-mini
    enabled: true
    defaultFor: [chat, coding, fast]
`;

function writeTmpYaml(content: string): { dir: string; filePath: string } {
  const dir = path.join(os.tmpdir(), `model-registry-${Date.now()}-${Math.random().toString(16).slice(2)}`);
  mkdirSync(dir);
  const filePath = path.join(dir, "models.local.yaml");
  writeFileSync(filePath, content, { encoding: "utf-8" });
  return { dir, filePath };
}

test("GET /health returns ok", async () => {
  const { filePath, dir } = writeTmpYaml(MIN_VALID);
  try {
    const config = loadConfig({ MODEL_CONFIG_PATH: filePath });
    const app = createApp({ config });
    const response = await app.request("/health");
    assert.equal(response.status, 200);
    assert.deepEqual(await response.json(), { status: "ok" });
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test("GET /model-config returns content + parsed summary", async () => {
  const { filePath, dir } = writeTmpYaml(MIN_VALID);
  try {
    const config = loadConfig({ MODEL_CONFIG_PATH: filePath });
    const app = createApp({ config });
    const response = await app.request("/model-config");
    assert.equal(response.status, 200);
    const body = (await response.json()) as { parsed?: { model_count: number } | null; validation_errors: unknown[] };
    assert.equal(body.validation_errors.length, 0);
    assert.equal(body.parsed?.model_count, 1);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test("startup seeds missing model registry YAML from bundled example", () => {
  const dir = path.join(os.tmpdir(), `model-registry-seed-${Date.now()}`);
  mkdirSync(dir);
  try {
    const target = path.join(dir, "models.local.yaml");
    const example = path.resolve("config/models.local.yaml.example");

    ensureModelRegistryConfigFile(target, example);

    assert.equal(existsSync(target), true);
    assert.equal(readFileSync(target, "utf-8"), readFileSync(example, "utf-8"));
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test("startup does not overwrite existing model registry YAML", () => {
  const { filePath, dir } = writeTmpYaml("custom: true\n");
  try {
    ensureModelRegistryConfigFile(filePath, path.resolve("config/models.local.yaml.example"));

    assert.equal(readFileSync(filePath, "utf-8"), "custom: true\n");
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test("GET /model-config works after startup seed", async () => {
  const dir = path.join(os.tmpdir(), `model-registry-seeded-get-${Date.now()}`);
  mkdirSync(dir);
  try {
    const filePath = path.join(dir, "models.local.yaml");
    const config = loadConfig({ MODEL_CONFIG_PATH: filePath });
    const app = createApp({ config });
    const response = await app.request("/model-config");
    assert.equal(response.status, 200);
    const body = (await response.json()) as { content: string; validation_errors: unknown[] };
    assert.ok(body.content.includes("version: 1"));
    assert.equal(body.validation_errors.length, 0);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test("POST /model-config/validate accepts valid content", async () => {
  const dir = path.join(os.tmpdir(), `model-registry-validate-${Date.now()}`);
  mkdirSync(dir);
  try {
    const filePath = path.join(dir, "models.local.yaml");
    writeFileSync(filePath, MIN_VALID, { encoding: "utf-8" });
    const config = loadConfig({ MODEL_CONFIG_PATH: filePath });
    const app = createApp({ config });

    const response = await app.request("/model-config/validate", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ content: MIN_VALID }),
    });
    assert.equal(response.status, 200);
    const body = (await response.json()) as { valid: boolean };
    assert.equal(body.valid, true);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test("model registry parses chat title generation config", () => {
  const compiled = compileRegistryYamlContent(MIN_VALID);
  assert.equal(compiled.ok, true);
  if (!compiled.ok) return;

  assert.equal(compiled.registry.chatTitleGeneration.modelId, "m1");
  assert.equal(compiled.parsed.chat_title_generation?.model_id, "m1");
});

test("POST /model-config/validate accepts reasoning config", async () => {
  const dir = path.join(os.tmpdir(), `model-registry-reasoning-validate-${Date.now()}`);
  mkdirSync(dir);
  try {
    const filePath = path.join(dir, "models.local.yaml");
    writeFileSync(filePath, REASONING_VALID, { encoding: "utf-8" });
    const config = loadConfig({ MODEL_CONFIG_PATH: filePath });
    const app = createApp({ config });

    const response = await app.request("/model-config/validate", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ content: REASONING_VALID }),
    });
    assert.equal(response.status, 200);
    const body = (await response.json()) as { valid: boolean };
    assert.equal(body.valid, true);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test("PUT /model-config rejects invalid content and does not save", async () => {
  const before = MIN_VALID.replace("\n", "\r\n");
  const { filePath, dir } = writeTmpYaml(before);
  try {
    const config = loadConfig({ MODEL_CONFIG_PATH: filePath });
    const app = createApp({ config });

    const response = await app.request("/model-config", {
      method: "PUT",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ content: "foo: [\n" }),
    });
    assert.equal(response.status, 400);
    const raw = await response.text();
    assert.ok(raw.length > 0);

    const after = await import("node:fs/promises").then((fs) => fs.readFile(filePath, "utf-8"));
    assert.equal(after, before, "invalid PUT must not overwrite the YAML file");
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test("PUT /model-config saves valid content and normalizes line endings", async () => {
  const { filePath, dir } = writeTmpYaml(MIN_VALID);
  try {
    const config = loadConfig({ MODEL_CONFIG_PATH: filePath });
    const app = createApp({ config });

    const payload = MIN_VALID.replace("\n", "\r\n");
    const response = await app.request("/model-config", {
      method: "PUT",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ content: payload }),
    });
    assert.equal(response.status, 200);
    const body = (await response.json()) as { saved: boolean };
    assert.equal(body.saved, true);

    const fs = await import("node:fs/promises");
    const after = await fs.readFile(filePath, "utf-8");
    assert.ok(!after.includes("\r\n"));
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test("GET /models returns list payload", async () => {
  const { filePath, dir } = writeTmpYaml(MIN_VALID);
  try {
    const config = loadConfig({ MODEL_CONFIG_PATH: filePath });
    const app = createApp({ config });
    const response = await app.request("/models");
    assert.equal(response.status, 200);
    const body = (await response.json()) as {
      default_model_id: string;
      chat_title_generation: { model_id: string | null };
      models: unknown[];
    };
    assert.equal(body.default_model_id, "m1");
    assert.deepEqual(body.chat_title_generation, { model_id: "m1" });
    assert.ok(Array.isArray(body.models) && body.models.length === 1);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test("POST /models/resolve-chat resolves default chat model", async () => {
  const { filePath, dir } = writeTmpYaml(MIN_VALID);
  try {
    const config = loadConfig({ MODEL_CONFIG_PATH: filePath });
    const app = createApp({ config });
    const response = await app.request("/models/resolve-chat", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ model_id: null, execution_mode: "read_only" }),
    });
    assert.equal(response.status, 200);
    const body = (await response.json()) as { option: { id: string }; runtime: { providerModelId: string } };
    assert.equal(body.option.id, "m1");
    assert.equal(body.runtime.providerModelId, "gpt-4o-mini");
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test("POST /models/resolve-chat preserves reasoning config in runtime model", async () => {
  const { filePath, dir } = writeTmpYaml(REASONING_VALID);
  try {
    const config = loadConfig({ MODEL_CONFIG_PATH: filePath });
    const app = createApp({ config });
    const response = await app.request("/models/resolve-chat", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ model_id: "anthropic-thinking", execution_mode: "read_only" }),
    });
    assert.equal(response.status, 200);
    const body = (await response.json()) as {
      option: { id: string };
      runtime: {
        providerType: string;
        providerModelId: string;
        reasoning?: {
          enabled: boolean;
          representation: string;
          source: string;
          providerOptions: Record<string, unknown>;
        } | null;
      };
    };
    assert.equal(body.option.id, "anthropic-thinking");
    assert.equal(body.runtime.providerType, "anthropic");
    assert.equal(body.runtime.providerModelId, "claude-sonnet-4-6");
    assert.deepEqual(body.runtime.reasoning, {
      enabled: true,
      representation: "thinking",
      source: "provider_exposed",
      providerOptions: {
        anthropic: {
          thinking: {
            type: "adaptive",
          },
        },
      },
    });
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test("PUT /model-config invalidates catalog for subsequent /models and /models/resolve-chat", async () => {
  const { filePath, dir } = writeTmpYaml(MIN_VALID);
  try {
    const config = loadConfig({ MODEL_CONFIG_PATH: filePath });
    const app = createApp({ config });

    const before = await app.request("/models");
    assert.equal(before.status, 200);
    assert.equal(((await before.json()) as { default_model_id: string }).default_model_id, "m1");

    const updated = MIN_VALID
      .replaceAll("m1", "m2")
      .replace("providerModelId: gpt-4o-mini", "providerModelId: gpt-5.1-mini");

    const save = await app.request("/model-config", {
      method: "PUT",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ content: updated }),
    });
    assert.equal(save.status, 200);

    const after = await app.request("/models");
    assert.equal(after.status, 200);
    const afterBody = (await after.json()) as { default_model_id: string; models: Array<{ id: string; providerModelId: string }> };
    assert.equal(afterBody.default_model_id, "m2");
    assert.equal(afterBody.models[0].id, "m2");
    assert.equal(afterBody.models[0].providerModelId, "gpt-5.1-mini");

    const resolved = await app.request("/models/resolve-chat", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ execution_mode: "read_only" }),
    });
    assert.equal(resolved.status, 200);
    const resolvedBody = (await resolved.json()) as { option: { id: string }; runtime: { providerModelId: string } };
    assert.equal(resolvedBody.option.id, "m2");
    assert.equal(resolvedBody.runtime.providerModelId, "gpt-5.1-mini");
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});
