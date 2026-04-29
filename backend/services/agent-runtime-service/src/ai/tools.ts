import { tool, type ToolSet } from "ai";
import { z, type ZodTypeAny } from "zod";

import { withToolTrace } from "../trace.js";
import type { ExecutionMode, RawEventFact, ToolDefinition, ToolExecutionClient, TraceEnvelope } from "../types.js";

function canUseTool(requiredMode: ExecutionMode, executionMode: ExecutionMode): boolean {
  const rank: Record<ExecutionMode, number> = {
    read_only: 0,
    suggest: 1,
    execute: 2,
    governed_execute: 3,
  };

  return rank[executionMode] >= rank[requiredMode];
}

function schemaToZod(schema: unknown): ZodTypeAny {
  if (!schema || typeof schema !== "object") {
    return z.object({}).passthrough();
  }

  const typedSchema = schema as { type?: string; properties?: Record<string, unknown>; required?: string[] };
  if (typedSchema.type !== "object" || !typedSchema.properties) {
    return z.object({}).passthrough();
  }

  const required = new Set(typedSchema.required ?? []);
  const shape: Record<string, ZodTypeAny> = {};

  for (const [key, value] of Object.entries(typedSchema.properties)) {
    const property = value as { type?: string; description?: string };
    let field: ZodTypeAny;
    switch (property.type) {
      case "number":
      case "integer":
        field = z.number();
        break;
      case "boolean":
        field = z.boolean();
        break;
      case "array":
        field = z.array(z.any());
        break;
      case "object":
        field = z.object({}).passthrough();
        break;
      default:
        field = z.string();
        break;
    }

    if (property.description) {
      field = field.describe(property.description);
    }
    if (!required.has(key)) {
      field = field.optional();
    }
    shape[key] = field;
  }

  return z.object(shape).passthrough();
}

export function createToolSet(input: {
  toolDefinitions: ToolDefinition[];
  toolExecutionClient: ToolExecutionClient;
  trace: TraceEnvelope;
  executionMode: ExecutionMode;
  onToolCallRequested?: (definition: ToolDefinition, toolCallId: string, args: Record<string, unknown>) => void | Promise<void>;
  emitRawToolEvents: (events: RawEventFact[] | undefined) => Promise<void>;
}): ToolSet {
  const toolEntries = input.toolDefinitions
    .filter((definition) => definition.enabled && canUseTool(definition.required_execution_mode, input.executionMode))
    .map((definition) => [
      definition.name,
      tool({
        description: definition.description,
        inputSchema: schemaToZod(definition.input_schema_json),
        execute: async (args: unknown, options: { toolCallId?: string }) => {
          const normalizedArgs = typeof args === "object" && args !== null ? (args as Record<string, unknown>) : {};
          const toolCallId = options.toolCallId ?? crypto.randomUUID();

          await input.onToolCallRequested?.(definition, toolCallId, normalizedArgs);
          const response = await input.toolExecutionClient.execute({
            trace: withToolTrace(input.trace, toolCallId),
            tool_name: definition.name,
            input: normalizedArgs,
            execution_mode: input.executionMode,
          });
          await input.emitRawToolEvents(response.raw_events);
          return response.output;
        },
      }),
    ]);

  return Object.fromEntries(toolEntries);
}
