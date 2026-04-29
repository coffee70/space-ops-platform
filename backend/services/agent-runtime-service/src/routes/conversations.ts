import { z } from "zod";
import type { Hono } from "hono";

import type { RunDependencies } from "../types.js";

const createConversationSchema = z.object({
  title: z.string().trim().min(1).max(255).optional().nullable(),
  mission_id: z.string().trim().min(1).optional().nullable(),
  vehicle_id: z.string().trim().min(1).optional().nullable(),
  execution_mode: z.enum(["read_only", "suggest", "execute", "governed_execute"]).default("read_only"),
});

export function registerConversationRoutes(app: Hono, dependencies: RunDependencies): void {
  app.get("/conversations", async (c) => {
    const conversations = await dependencies.store.listConversations();
    return c.json(conversations);
  });

  app.post("/conversations", async (c) => {
    const payload = createConversationSchema.parse(await c.req.json());
    const conversation = await dependencies.store.createConversation(payload);
    return c.json(conversation);
  });

  app.get("/conversations/:conversationId", async (c) => {
    const conversation = await dependencies.store.getConversation(c.req.param("conversationId"));
    if (!conversation) {
      return c.json({ detail: "conversation not found" }, 404);
    }
    return c.json(conversation);
  });
}
