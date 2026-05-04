import { serve } from "@hono/node-server";
import { Hono } from "hono";

import { createModelRunner } from "./ai/model.js";
import { HttpContextRetrievalClient } from "./clients/context-retrieval.js";
import { HttpToolExecutionClient } from "./clients/tool-execution.js";
import { HttpToolRegistryClient } from "./clients/tool-registry.js";
import { loadConfig } from "./config.js";
import { PgConversationStore } from "./db/conversations.js";
import { registerChatRoutes } from "./routes/chat.js";
import { registerConversationRoutes } from "./routes/conversations.js";
import type { RunDependencies } from "./types.js";

export function createApp(overrides?: Partial<RunDependencies>): Hono {
  const config = overrides?.config ?? loadConfig();

  const dependencies: RunDependencies = {
    config,
    store: overrides?.store ?? new PgConversationStore(config.databaseUrl),
    contextClient: overrides?.contextClient ?? new HttpContextRetrievalClient(config),
    toolRegistryClient: overrides?.toolRegistryClient ?? new HttpToolRegistryClient(config),
    toolExecutionClient: overrides?.toolExecutionClient ?? new HttpToolExecutionClient(config),
    modelRunner: overrides?.modelRunner ?? createModelRunner(config),
    now: overrides?.now ?? (() => new Date()),
    createId: overrides?.createId ?? (() => crypto.randomUUID()),
  };

  const app = new Hono();

  app.get("/health", (c) => c.json({ status: "ok" }));

  registerConversationRoutes(app, dependencies);
  registerChatRoutes(app, dependencies);

  return app;
}

if (import.meta.url === `file://${process.argv[1]}`) {
  const config = loadConfig();
  const app = createApp({ config });
  serve({ fetch: app.fetch, port: config.port });
}
