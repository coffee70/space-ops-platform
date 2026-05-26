import { z } from "zod";
import type { Hono } from "hono";

import type { RunDependencies } from "../types.js";

const createConversationSchema = z.object({
  title: z.string().trim().min(1).max(255).optional().nullable(),
  mission_id: z.string().trim().min(1).optional().nullable(),
  vehicle_id: z.string().trim().min(1).optional().nullable(),
  execution_mode: z.enum(["read_only", "suggest", "execute", "governed_execute"]).default("read_only"),
  selected_model_id: z.string().trim().min(1).optional().nullable(),
  initial_message: z.object({
    role: z.literal("user"),
    content: z.string().trim().min(1),
    metadata: z.record(z.string(), z.unknown()).optional(),
  }),
});

const updateConversationSchema = z
  .object({
    title: z.string().trim().min(1).max(255).optional().nullable(),
    execution_mode: z.enum(["read_only", "suggest", "execute", "governed_execute"]).optional(),
    selected_model_id: z.string().trim().min(1).optional().nullable(),
  })
  .refine((value) => Object.keys(value).length > 0, { message: "at least one field is required" });

export function registerConversationRoutes(app: Hono, dependencies: RunDependencies): void {
  app.get("/conversations", async (c) => {
    const conversations = await dependencies.store.listConversations();
    return c.json(conversations);
  });

  app.post("/conversations", async (c) => {
    let body: unknown;
    try {
      body = await c.req.json();
    } catch {
      body = {};
    }
    const payloadResult = createConversationSchema.safeParse(body);
    if (!payloadResult.success) {
      return c.json(
        {
          detail: "initial user message is required",
          issues: payloadResult.error.issues,
        },
        400,
      );
    }
    const payload = payloadResult.data;
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

  app.patch("/conversations/:conversationId", async (c) => {
    let body: unknown;
    try {
      body = await c.req.json();
    } catch {
      body = {};
    }
    const payloadResult = updateConversationSchema.safeParse(body);
    if (!payloadResult.success) {
      return c.json({ detail: "invalid conversation update payload", issues: payloadResult.error.issues }, 400);
    }
    const payload = payloadResult.data;
    const conversation = await dependencies.store.updateConversation(c.req.param("conversationId"), {
      ...payload,
      title_source: "title" in payload ? "manual" : undefined,
    });
    if (!conversation) {
      return c.json({ detail: "conversation not found" }, 404);
    }
    return c.json(conversation);
  });
}
