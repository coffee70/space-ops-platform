import type { Hono } from "hono";
import { z } from "zod";

import { buildSystemPrompt } from "../ai/prompts.js";
import { createToolSet } from "../ai/tools.js";
import { AgentEventStream } from "../events/stream.js";
import { RunSequencer } from "../events/sequencer.js";
import { runFallback } from "../fallback.js";
import { createTrace } from "../trace.js";
import type { ChatInputMessage, ContextPacketResponse, ExecutionMode, RawEventFact, RunDependencies } from "../types.js";

const chatRequestSchema = z.object({
  conversation_id: z.string().uuid(),
  execution_mode: z.enum(["read_only", "suggest", "execute", "governed_execute"]).optional(),
  mission_id: z.string().trim().min(1).optional().nullable(),
  vehicle_id: z.string().trim().min(1).optional().nullable(),
  client_context: z
    .object({
      current_application_id: z.string().trim().min(1).optional(),
      current_route: z.string().trim().min(1).optional(),
    })
    .optional(),
  messages: z
    .array(
      z.object({
        role: z.enum(["user", "assistant"]),
        content: z.string(),
      }),
    )
    .min(1),
});

function buildRetrievalPlan(message: string): {
  documents: boolean;
  code: boolean;
  platform: boolean;
  tools: boolean;
  summary: string;
} {
  const normalized = message.toLowerCase();
  const code = /(code|service|route|component|file|repo|runtime|deploy|deployment)/.test(normalized);
  const documents = /(document|mission|vehicle|telemetry|analysis|plan|design)/.test(normalized) || !code;

  return {
    documents,
    code,
    platform: false,
    tools: true,
    summary: `documents=${documents}, code=${code}, platform=false, tools=true`,
  };
}

function emptyContext(trace: { conversation_id: string; agent_run_id: string; request_id: string }, event: RawEventFact): ContextPacketResponse {
  return {
    conversation_id: trace.conversation_id,
    agent_run_id: trace.agent_run_id,
    request_id: trace.request_id,
    context_packet_id: crypto.randomUUID(),
    document_chunk_count: 0,
    code_chunk_count: 0,
    platform_metadata_bytes: 0,
    tool_definition_count: 0,
    truncated: false,
    truncation_reasons: [],
    data: {
      mission_documents: [],
      code_context: [],
      platform_context: {},
      runtime_context: {},
      tool_context: [],
    },
    raw_events: [event],
  };
}

export function registerChatRoutes(app: Hono, dependencies: RunDependencies): void {
  app.post("/agent/chat", async (c) => {
    const payload = chatRequestSchema.parse(await c.req.json());
    const conversation = await dependencies.store.getConversation(payload.conversation_id);
    if (!conversation) {
      return c.json({ detail: "conversation not found" }, 404);
    }

    const latestMessage = payload.messages[payload.messages.length - 1];
    if (latestMessage.role !== "user" || latestMessage.content.trim().length === 0) {
      return c.json({ detail: "latest user message is required" }, 400);
    }

    const executionMode = payload.execution_mode ?? conversation.execution_mode;
    const trace = createTrace({
      conversationId: payload.conversation_id,
      createId: dependencies.createId,
    });
    const stream = new AgentEventStream({
      store: dependencies.store,
      trace,
      sequencer: new RunSequencer(),
      now: dependencies.now,
    });

    void orchestrateChat({
      dependencies,
      stream,
      conversation,
      executionMode,
      missionId: payload.mission_id ?? conversation.mission_id,
      vehicleId: payload.vehicle_id ?? conversation.vehicle_id,
      latestUserMessage: latestMessage.content.trim(),
      requestMessages: payload.messages,
      trace,
    });

    return stream.response;
  });
}

async function orchestrateChat(input: {
  dependencies: RunDependencies;
  stream: AgentEventStream;
  conversation: { id: string; mission_id: string | null; vehicle_id: string | null; messages: ChatInputMessage[] };
  executionMode: ExecutionMode;
  missionId: string | null;
  vehicleId: string | null;
  latestUserMessage: string;
  requestMessages: ChatInputMessage[];
  trace: { conversation_id: string; agent_run_id: string; request_id: string };
}): Promise<void> {
  const { dependencies, stream } = input;

  try {
    await dependencies.store.appendMessage({
      conversationId: input.trace.conversation_id,
      role: "user",
      content: input.latestUserMessage,
      metadata: {
        request_id: input.trace.request_id,
        agent_run_id: input.trace.agent_run_id,
      },
    });

    await stream.emitEvent("run.started", {
      execution_mode: input.executionMode,
    });

    const retrievalPlan = buildRetrievalPlan(input.latestUserMessage);
    await stream.emitEvent("context.requested", {
      retrieval_plan: retrievalPlan.summary,
    });

    const context = await resolveContext({
      dependencies,
      trace: input.trace,
      latestUserMessage: input.latestUserMessage,
      missionId: input.missionId,
      vehicleId: input.vehicleId,
      executionMode: input.executionMode,
      retrievalPlan,
    });
    await stream.emitRawEvents(context.raw_events);

    const toolDefinitions = await dependencies.toolRegistryClient.listTools();
    const tools = createToolSet({
      toolDefinitions,
      toolExecutionClient: dependencies.toolExecutionClient,
      trace: input.trace,
      executionMode: input.executionMode,
      emitToolStarted: async (toolName, toolCallId, args) => {
        await stream.emitEvent(
          "tool.started",
          {
            tool_name: toolName,
            input_preview: args,
          },
          { toolCallId, emittedBy: "agent-runtime-service" },
        );
      },
      emitRawToolEvents: async (events) => {
        await stream.emitRawEvents(events as RawEventFact[] | undefined);
      },
    });

    const modelMessages = [...input.conversation.messages, { role: "user", content: input.latestUserMessage }] as ChatInputMessage[];
    const systemPrompt = buildSystemPrompt({
      executionMode: input.executionMode,
      retrievalPlan,
      context,
      tools: toolDefinitions,
      messages: modelMessages,
    });

    if (!dependencies.config.openAiApiKey) {
      await runFallback({
        stream,
        userMessage: input.latestUserMessage,
        executionMode: input.executionMode,
        persistAssistantMessage: async (content) =>
          dependencies.store.appendMessage({
            conversationId: input.trace.conversation_id,
            role: "assistant",
            content,
            metadata: {
              agent_run_id: input.trace.agent_run_id,
              request_id: input.trace.request_id,
            },
          }),
      });
      await stream.close();
      return;
    }

    let assistantText = "";
    for await (const part of dependencies.modelRunner.stream({
      system: systemPrompt,
      messages: modelMessages,
      tools,
      maxSteps: dependencies.config.maxSteps,
    })) {
      if (part.type === "text-delta" && typeof part.textDelta === "string" && part.textDelta.length > 0) {
        assistantText += part.textDelta;
        await stream.emitMessageDelta(part.textDelta);
      }
    }

    const finalAssistantText = assistantText.trim().length > 0 ? assistantText : "No response.";
    if (assistantText.trim().length === 0) {
      await stream.emitMessageDelta(finalAssistantText);
    }
    const assistantMessage = await dependencies.store.appendMessage({
      conversationId: input.trace.conversation_id,
      role: "assistant",
      content: finalAssistantText,
      metadata: {
        agent_run_id: input.trace.agent_run_id,
        request_id: input.trace.request_id,
      },
    });
    await stream.emitEvent("message.completed", {
      message_id: assistantMessage.id,
      role: "assistant",
    });
    await stream.emitEvent("run.completed", {
      completion_mode: "model",
    });
    await stream.close();
  } catch (error) {
    await stream.fail(error);
  }
}

async function resolveContext(input: {
  dependencies: RunDependencies;
  trace: { conversation_id: string; agent_run_id: string; request_id: string };
  latestUserMessage: string;
  missionId: string | null;
  vehicleId: string | null;
  executionMode: ExecutionMode;
  retrievalPlan: ReturnType<typeof buildRetrievalPlan>;
}): Promise<ContextPacketResponse> {
  try {
    return await input.dependencies.contextClient.resolve({
      trace: input.trace,
      message: input.latestUserMessage,
      mission_id: input.missionId,
      vehicle_id: input.vehicleId,
      execution_mode: input.executionMode,
      retrieval_plan: input.retrievalPlan,
    });
  } catch (error) {
    return emptyContext(input.trace, {
      event_type: "context.failed",
      emitted_by: "context-retrieval-service",
      payload: {
        error_code: "context_resolution_failed",
        message: error instanceof Error ? error.message : "Context retrieval failed",
      },
    });
  }
}
