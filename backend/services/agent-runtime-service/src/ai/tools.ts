import { randomUUID } from "node:crypto";

import { tool, type ToolSet } from "ai";
import { z, type ZodTypeAny } from "zod";

import { withToolTrace } from "../trace.js";
import type { ExecutionMode, RawEventFact, ToolDefinition, ToolExecutionClient, ToolModePolicy, ToolPermissionClient, TraceEnvelope } from "../types.js";

function canUseTool(requiredMode: ExecutionMode, executionMode: ExecutionMode): boolean {
  const rank: Record<ExecutionMode, number> = {
    read_only: 0,
    suggest: 1,
    execute: 2,
    governed_execute: 3,
  };

  return rank[executionMode] >= rank[requiredMode];
}

export function policyForMode(definition: ToolDefinition, executionMode: ExecutionMode): ToolModePolicy {
  const policy = definition.mode_policy_json?.[executionMode];
  if (policy === "disabled" || policy === "requires_permission" || policy === "enabled") {
    return policy;
  }
  return canUseTool(definition.required_execution_mode, executionMode) ? "enabled" : "disabled";
}

/** Matches `createToolSet` exposure so prompts list the same callable tools as the SDK tool surface. */
export function filterToolDefinitionsForExecutionMode(
  definitions: ToolDefinition[],
  executionMode: ExecutionMode,
): ToolDefinition[] {
  return definitions.filter((definition) => definition.enabled && policyForMode(definition, executionMode) !== "disabled");
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

const JsonSchemaSchema: z.ZodType<JsonSchema> = z.lazy(() =>
  z
    .object({
      type: z.string().optional(),
      properties: z.record(JsonSchemaSchema).optional(),
      required: z.array(z.string()).optional(),
      additionalProperties: z.boolean().optional(),
      items: JsonSchemaSchema.optional(),
      enum: z.array(z.union([z.string(), z.number(), z.boolean()])).optional(),
      description: z.string().optional(),
      minLength: z.number().optional(),
      maxLength: z.number().optional(),
      minimum: z.number().optional(),
      maximum: z.number().optional(),
    })
    .passthrough(),
);

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
  const parsed = JsonSchemaSchema.safeParse(schema);
  if (!parsed.success || parsed.data.type !== "object") {
    return z.object({}).strict();
  }
  return schemaNodeToZod(parsed.data);
}

export function createToolSet(input: {
  toolDefinitions: ToolDefinition[];
  toolExecutionClient: ToolExecutionClient;
  toolPermissionClient?: ToolPermissionClient;
  trace: TraceEnvelope;
  executionMode: ExecutionMode;
  abortSignal?: AbortSignal;
  onToolCallRequested?: (definition: ToolDefinition, toolCallId: string, args: Record<string, unknown>) => void | Promise<void>;
  onToolCallCompleted?: (
    definition: ToolDefinition,
    toolCallId: string,
    args: Record<string, unknown>,
    output: unknown,
    status: "completed" | "failed" | "confirmation_required",
  ) => void | Promise<void>;
  emitRawToolEvents: (events: RawEventFact[] | undefined) => Promise<void>;
}): ToolSet {
  const toolEntries = filterToolDefinitionsForExecutionMode(input.toolDefinitions, input.executionMode)
    .map((definition) => {
      const inputSchema = schemaToZod(definition.input_schema_json);
      return [
        definition.name,
        tool({
        description: definition.description,
        inputSchema,
        execute: async (args: unknown, _options: { toolCallId?: string }) => {
          const parsedArgs = inputSchema.safeParse(args);
          const normalizedArgs =
            parsedArgs.success && typeof parsedArgs.data === "object" && parsedArgs.data !== null
              ? (parsedArgs.data as Record<string, unknown>)
              : {};
          const toolCallId = randomUUID();

          await input.onToolCallRequested?.(definition, toolCallId, normalizedArgs);
          const response = await input.toolExecutionClient.execute({
            trace: withToolTrace(input.trace, toolCallId),
            tool_name: definition.name,
            input: normalizedArgs,
            execution_mode: input.executionMode,
          });
          await input.emitRawToolEvents(response.raw_events);
          if (response.status === "permission_required") {
            const output = response.output && typeof response.output === "object" ? (response.output as Record<string, unknown>) : {};
            const permissionRequestId =
              typeof output.permission_request_id === "string" ? output.permission_request_id : null;
            const approvalToken = typeof output.approval_token === "string" ? output.approval_token : null;
            if (!permissionRequestId || !approvalToken || !input.toolPermissionClient) {
              return response.output;
            }

            const decision = await input.toolPermissionClient.waitForDecision({
              permissionRequestId,
              abortSignal: input.abortSignal,
            });
            await input.emitRawToolEvents(decision.raw_events);
            if (decision.status === "denied") {
              return {
                status: "permission_denied",
                message: "The user denied this tool call. No action was taken.",
                permission_request_id: permissionRequestId,
                tool_name: definition.name,
                reason: decision.reason ?? "user_denied",
              };
            }

            const approvedResponse = await input.toolExecutionClient.execute({
              trace: withToolTrace(input.trace, toolCallId),
              tool_name: definition.name,
              input: normalizedArgs,
              execution_mode: input.executionMode,
              approval_token: approvalToken,
            });
            await input.emitRawToolEvents(approvedResponse.raw_events);
            if (approvedResponse.status === "completed") {
              await input.onToolCallCompleted?.(definition, toolCallId, normalizedArgs, approvedResponse.output, approvedResponse.status);
            }
            return approvedResponse.output;
          }
          if (response.status === "completed") {
            await input.onToolCallCompleted?.(definition, toolCallId, normalizedArgs, response.output, response.status);
          }
          return response.output;
        },
      }),
      ];
    });

  return Object.fromEntries(toolEntries);
}
