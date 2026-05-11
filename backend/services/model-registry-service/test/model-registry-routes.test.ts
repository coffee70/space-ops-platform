import assert from "node:assert/strict";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { mkdirSync, writeFileSync, rmSync } from "node:fs";

import { loadConfig } from "../src/config.js";
import { createApp } from "../src/server.js";

const MIN_VALID = `version: 1
defaults:
  chatModel: m1
  codingModel: m1
  fastModel: m1
  restrictedModel: m1
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

test("GET /model-config returns 404 when YAML is missing", async () => {
  const dir = path.join(os.tmpdir(), `model-registry-missing-${Date.now()}`);
  mkdirSync(dir);
  try {
    const missing = path.join(dir, "models.local.yaml");
    const config = loadConfig({ MODEL_CONFIG_PATH: missing });
    const app = createApp({ config });
    const response = await app.request("/model-config");
    assert.equal(response.status, 404);
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
    const body = (await response.json()) as { default_model_id: string; models: unknown[] };
    assert.equal(body.default_model_id, "m1");
    assert.ok(Array.isArray(body.models) && body.models.length === 1);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

