import type { Hono } from "hono";

import { validateModelRegistryConfigContent } from "../ai/model-registry-config.js";
import type { RunDependencies } from "../types.js";

export function registerModelRoutes(app: Hono, dependencies: RunDependencies): void {
  app.post("/models/validate-config", async (c) => {
    let content = "";
    try {
      const body = await c.req.json<{ content?: unknown }>();
      if (body && typeof body.content === "string") content = body.content;
    } catch {
      return c.json({
        valid: false,
        parsed: null,
        errors: [{ loc: [], message: "Request body must be JSON with a string content field", type: "json_parse" }],
      });
    }
    return c.json(validateModelRegistryConfigContent(content));
  });

  app.get("/models", async (c) => {
    const payload = await dependencies.modelCatalog.listModelsResponse();
    return c.json(payload);
  });

  app.get("/models/:modelId", async (c) => {
    const payload = await dependencies.modelCatalog.listModelsResponse();
    const modelId = c.req.param("modelId");
    const hit = payload.models.find((m) => m.id === modelId);
    if (!hit) {
      return c.json({ detail: "model not found" }, 404);
    }
    return c.json(hit);
  });
}
