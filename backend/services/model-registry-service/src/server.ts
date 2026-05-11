import { serve } from "@hono/node-server";
import { Hono } from "hono";
import type { ExecutionMode, ListAiEngineerModelsResponse, RuntimeConfig, ResolvedChatModel } from "./types.js";

import { readFileSync, writeFileSync, existsSync } from "node:fs";

import { loadConfig } from "./config.js";
import { ModelCatalogService } from "./ai/model-catalog.js";
import { ModelSelectionError } from "./ai/model-errors.js";
import { invalidateOpenRouterResolverCache } from "./ai/metadata/openrouter-resolver.js";
import { validateModelRegistryConfigContent } from "./ai/model-registry-config.js";

function normalizeLineEndings(content: string): string {
  return content.replace("\r\n", "\n").replace("\r", "\n");
}

function getRequiredConfigPath(config: RuntimeConfig): string {
  if (!config.modelsConfigPath) {
    // Should not happen in real deployments (kernel injects MODEL_CONFIG_PATH).
    throw new Error("MODEL_CONFIG_PATH is not configured");
  }
  return config.modelsConfigPath;
}

function createModelCatalog(config: RuntimeConfig): ModelCatalogService {
  // ModelCatalogService owns model selection + enrichment (openrouter metadata, etc).
  return new ModelCatalogService(config);
}

export function createApp(overrides?: { config?: RuntimeConfig }) {
  const config = overrides?.config ?? loadConfig();
  const configPath = getRequiredConfigPath(config);

  let catalog: ModelCatalogService | null = null;
  const getCatalog = (): ModelCatalogService => {
    if (!catalog) catalog = createModelCatalog(config);
    return catalog;
  };

  const app = new Hono();

  app.get("/health", (c) => c.json({ status: "ok" }));

  app.get("/model-config", (c) => {
    if (!existsSync(configPath)) {
      return c.json(
        { detail: { message: `Model registry YAML not found at ${configPath}` } },
        404,
      );
    }
    const raw = readFileSync(configPath, "utf-8");
    const validation = validateModelRegistryConfigContent(raw);
    return c.json({
      path: configPath,
      content: raw,
      format: "yaml",
      parsed: validation.valid ? validation.parsed : null,
      validation_errors: validation.errors,
    });
  });

  app.post("/model-config/validate", async (c) => {
    const body = await c.req.json<{ content?: unknown }>();
    const content = typeof body?.content === "string" ? body.content : "";
    return c.json(validateModelRegistryConfigContent(content));
  });

  app.put("/model-config", async (c) => {
    const body = await c.req.json<{ content?: unknown }>();
    const content = typeof body?.content === "string" ? body.content : "";
    const validation = validateModelRegistryConfigContent(content);
    if (!validation.valid || !validation.parsed) {
      return c.json(
        {
          detail: {
            message: "Model registry validation failed",
            errors: validation.errors,
          },
        },
        400,
      );
    }

    const normalized = normalizeLineEndings(content);
    writeFileSync(configPath, normalized, "utf-8");
    // Invalidate in-memory catalog so the next /models call uses the updated file.
    catalog = null;
    invalidateOpenRouterResolverCache();

    return c.json({
      path: configPath,
      parsed: validation.parsed,
      saved: true,
    });
  });

  app.get("/models", async (c) => {
    try {
      const payload: ListAiEngineerModelsResponse = await getCatalog().listModelsResponse();
      return c.json(payload);
    } catch (err) {
      return c.json({ detail: { message: `Model registry unavailable: ${String(err)}` } }, 500);
    }
  });

  app.post("/models/resolve-chat", async (c) => {
    const body = await c.req.json<{ model_id?: unknown; execution_mode?: unknown }>();
    const modelId = typeof body?.model_id === "string" ? body.model_id : null;
    const executionMode = body?.execution_mode as ExecutionMode | undefined;
    if (!executionMode) {
      return c.json({ detail: { message: "execution_mode is required" } }, 400);
    }

    try {
      const resolved: ResolvedChatModel = await getCatalog().resolveForChat(modelId, executionMode);
      return c.json(resolved);
    } catch (err) {
      if (err instanceof ModelSelectionError) {
        return c.json({ detail: { message: err.message, code: err.code } }, 400);
      }
      return c.json({ detail: { message: String(err) } }, 400);
    }
  });

  return app;
}

if (import.meta.url === `file://${process.argv[1]}`) {
  const config = loadConfig();
  const app = createApp({ config });
  serve({ fetch: app.fetch, port: config.port });
}
