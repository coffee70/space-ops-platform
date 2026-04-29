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

/** Matches `createToolSet` exposure so prompts list the same callable tools as the SDK tool surface. */
export function filterToolDefinitionsForExecutionMode(
  definitions: ToolDefinition[],
  executionMode: ExecutionMode,
): ToolDefinition[] {
  return definitions.filter(
    (definition) => definition.enabled && canUseTool(definition.required_execution_mode, executionMode),
  );
}

type JsonSchema = {
  type?: string;
  properties?: Record<string, JsonSchema>;
  required?: string[];
  additionalProperties?: boolean;
  items?: JsonSchema;
  enum?: Array<string | number | boolean>;
  description?: string;
  minLength?: number;
  maxLength?: number;
  minimum?: number;
  maximum?: number;
};

function applyCommonConstraints(schema: JsonSchema, field: ZodTypeAny): ZodTypeAny {
  let constrained = field;
  if (schema.description) constrained = constrained.describe(schema.description);
  return constrained;
}

function schemaNodeToZod(schema: JsonSchema): ZodTypeAny {
  switch (schema.type) {
    case "string": {
      let value = z.string();
      if (typeof schema.minLength === "number") value = value.min(schema.minLength);
      if (typeof schema.maxLength === "number") value = value.max(schema.maxLength);
      if (Array.isArray(schema.enum) && schema.enum.length > 0 && schema.enum.every((item) => typeof item === "string")) {
        const options = schema.enum as [string, ...string[]];
        return applyCommonConstraints(schema, z.enum(options));
      }
      return applyCommonConstraints(schema, value);
    }
    case "integer": {
      let value = z.number().int();
      if (typeof schema.minimum === "number") value = value.min(schema.minimum);
      if (typeof schema.maximum === "number") value = value.max(schema.maximum);
      return applyCommonConstraints(schema, value);
    }
    case "number": {
      let value = z.number();
      if (typeof schema.minimum === "number") value = value.min(schema.minimum);
      if (typeof schema.maximum === "number") value = value.max(schema.maximum);
      return applyCommonConstraints(schema, value);
    }
    case "boolean":
      return applyCommonConstraints(schema, z.boolean());
    case "array":
      return applyCommonConstraints(schema, z.array(schema.items ? schemaNodeToZod(schema.items) : z.unknown()));
    case "object": {
      const properties = schema.properties ?? {};
      const required = new Set(schema.required ?? []);
      const shape: Record<string, ZodTypeAny> = {};
      for (const [key, propSchema] of Object.entries(properties)) {
        const prop = schemaNodeToZod(propSchema);
        shape[key] = required.has(key) ? prop : prop.optional();
      }
      const objectSchema = z.object(shape);
      return applyCommonConstraints(schema, schema.additionalProperties === false ? objectSchema.strict() : objectSchema);
    }
    default:
      return z.unknown();
  }
}

export function schemaToZod(schema: unknown): ZodTypeAny {
  if (!schema || typeof schema !== "object") {
    return z.object({}).strict();
  }
  const typedSchema = schema as JsonSchema;
  if (typedSchema.type !== "object") {
    return z.object({}).strict();
  }
  return schemaNodeToZod(typedSchema);
}

export function createToolSet(input: {
  toolDefinitions: ToolDefinition[];
  toolExecutionClient: ToolExecutionClient;
  trace: TraceEnvelope;
  executionMode: ExecutionMode;
  onToolCallRequested?: (definition: ToolDefinition, toolCallId: string, args: Record<string, unknown>) => void | Promise<void>;
  emitRawToolEvents: (events: RawEventFact[] | undefined) => Promise<void>;
}): ToolSet {
  const toolEntries = filterToolDefinitionsForExecutionMode(input.toolDefinitions, input.executionMode)
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
