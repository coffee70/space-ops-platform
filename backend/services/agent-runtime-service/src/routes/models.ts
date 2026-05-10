import type { Hono } from "hono";

import type { RunDependencies } from "../types.js";

export function registerModelRoutes(app: Hono, dependencies: RunDependencies): void {
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
