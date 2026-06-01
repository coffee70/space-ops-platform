import type { Hono } from "hono";
import { z } from "zod";

import { buildSystemPrompt } from "../ai/prompts.js";
import { ModelSelectionError } from "../ai/model-errors.js";
import { ModelBudgetTracker, warningFromSnapshot } from "../ai/model-budget.js";
import { LanguageModelUsageSnapshotSchema } from "../ai/model-usage.js";
import { ModelProviderRuntimeError, normalizeModelProviderError } from "../ai/provider-errors.js";
import { estimatedUsageSnapshot, estimateTokensFromMessages } from "../ai/token-estimation.js";
import { createToolSet, toolSchemaInvalidDiagnosticEvent, validateToolDefinitionsForModel } from "../ai/tools.js";
import { AgentEventStream } from "../events/stream.js";
import { RunSequencer } from "../events/sequencer.js";
import { runFallback } from "../fallback.js";
import { completeScriptedRun, resolveScriptedMode, runScriptedMode } from "../scripted.js";
import { maybeGenerateConversationTitle } from "../title-generation.js";
import { createTrace } from "../trace.js";
import type {
  ChatInputMessage,
  ContextPacketResponse,
  ConversationDetail,
  ExecutionMode,
  RawEventFact,
  ReasoningStreamRepresentation,
  ResolvedChatModel,
  RunDependencies,
} from "../types.js";

const chatRequestSchema = z.object({
  conversation_id: z.string().uuid(),
  execution_mode: z.enum(["read_only", "suggest", "execute", "governed_execute"]).optional(),
  mission_id: z.string().trim().min(1).optional().nullable(),
  vehicle_id: z.string().trim().min(1).optional().nullable(),
  model_id: z.string().trim().min(1).optional().nullable(),
  persisted_user_message_id: z.string().uuid().optional().nullable(),
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

const CONTEXT_LIMITS = {
  document_chunks: 6,
  code_chunks: 6,
  platform_metadata_bytes: 6000,
  tool_definitions: 20,
};

const CODE_INTENT_PATTERN =
  /\b(code|codebase|source|service|route|component|file|repo|repository|runtime|deploy|deployment|index|indexed|search|implementation|endpoint)\b|code_index_not_ready|search_codebase/;
const DOCUMENT_INTENT_PATTERN = /\b(document|mission|vehicle|telemetry|analysis|plan|design)\b/;
const RETRY_INTENT_PATTERN =
  /\b(try again|retry|rerun|run it again|again|reattempt|re-attempt|hit the index|hit index|hit the search|search again|recheck|check again)\b/;

function hasCodeIntent(message: string): boolean {
  return CODE_INTENT_PATTERN.test(message.toLowerCase());
}

function hasRetryIntent(message: string): boolean {
  return RETRY_INTENT_PATTERN.test(message.toLowerCase());
}

function rawEventsIncludeToolActivity(events: RawEventFact[] | undefined): boolean {
  return (events ?? []).some((event) => event.event_type.startsWith("tool."));
}

function textDeltaWithToolBoundary(assistantText: string, textDelta: string, pendingToolTextBoundary: boolean): string {
  if (!pendingToolTextBoundary || assistantText.trim().length === 0) {
    return textDelta;
  }
  return `${assistantText.endsWith("\n\n") ? "" : "\n\n"}${textDelta}`;
}

function recentConversationHasCodeIntent(messages: ChatInputMessage[]): boolean {
  return messages.slice(-8).some((message) => hasCodeIntent(message.content));
}

async function maybeGenerateAndEmitConversationTitle(input: {
  dependencies: RunDependencies;
  conversationId: string;
  stream: AgentEventStream;
}): Promise<void> {
  const conversation = await maybeGenerateConversationTitle({
    dependencies: input.dependencies,
    conversationId: input.conversationId,
  });
  if (!conversation || conversation.title_source !== "generated" || !conversation.title) {
    return;
  }
  await input.stream.emitEvent("conversation.title.generated", {
    conversation_id: conversation.id,
    title: conversation.title,
    title_source: "generated",
    title_model_id: conversation.title_model_id,
  });
}

function buildRetrievalPlan(message: string, recentMessages: ChatInputMessage[] = []): {
  documents: boolean;
  code: boolean;
  platform: boolean;
  tools: boolean;
  summary: string;
} {
  const normalized = message.toLowerCase();
  const code = hasCodeIntent(normalized) || (hasRetryIntent(normalized) && recentConversationHasCodeIntent(recentMessages));
  const documents = DOCUMENT_INTENT_PATTERN.test(normalized) || !code;

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

function contentPreview(content: string): string {
  return content.length > 300 ? `${content.slice(0, 300)}...<truncated>` : content;
}

function stringField(fields: Record<string, unknown>, names: string[]): string | null {
  for (const name of names) {
    const value = fields[name];
    if (typeof value === "string") {
      return value;
    }
  }
  return null;
}

function getTextDelta(part: { type: string }): string | null {
  if (part.type !== "text-delta") {
    return null;
  }
  return stringField(part as Record<string, unknown>, ["text", "delta", "textDelta"]);
}

function isReasoningStart(part: { type: string }): boolean {
  return part.type === "reasoning-start";
}

function getReasoningDelta(part: { type: string }): string | null {
  if (part.type !== "reasoning" && part.type !== "reasoning-delta") {
    return null;
  }
  return stringField(part as Record<string, unknown>, ["textDelta", "delta", "text"]);
}

function isReasoningEnd(part: { type: string }): boolean {
  return part.type === "reasoning-part-finish" || part.type === "reasoning-end";
}

function reasoningRepresentationForProvider(providerType: string): ReasoningStreamRepresentation {
  if (providerType === "openai") {
    return "reasoning_summary";
  }
  if (providerType === "anthropic") {
    return "thinking";
  }
  return "reasoning";
}

function stepLimitReachedMessage(maxSteps: number): string {
  return `I reached the configured agent work limit before I could produce a final response. This run used the maximum of ${maxSteps} agent steps. Increase PLATFORM_AGENT_RUNTIME_MAX_STEPS if this workflow should be allowed to continue longer.`;
}

type ChatCancellationReason = "user_requested_stop" | "model_stream_aborted";

class ChatRunCancelledError extends Error {
  readonly reason: ChatCancellationReason;

  constructor(reason: ChatCancellationReason) {
    super(reason === "user_requested_stop" ? "Chat run cancelled by the user." : "Model stream aborted.");
    this.name = "ChatRunCancelledError";
    this.reason = reason;
  }
}

function cancellationReasonForSignal(signal?: AbortSignal): ChatCancellationReason {
  return signal?.aborted ? "user_requested_stop" : "model_stream_aborted";
}

function throwIfRunCancelled(signal?: AbortSignal): void {
  if (signal?.aborted) {
    throw new ChatRunCancelledError("user_requested_stop");
  }
}

function isRunCancelled(error: unknown, signal?: AbortSignal): boolean {
  return Boolean(
    signal?.aborted ||
      error instanceof ChatRunCancelledError ||
      (error instanceof Error && error.name === "AbortError"),
  );
}

export function registerChatRoutes(app: Hono, dependencies: RunDependencies): void {
  app.post("/chat", async (c) => {
    const parsedBody = await c.req.json();
    const payloadResult = chatRequestSchema.safeParse(parsedBody);
    if (!payloadResult.success) {
      return c.json(
        {
          detail: "invalid chat request payload",
          issues: payloadResult.error.issues,
        },
        400,
      );
    }
    const payload = payloadResult.data;
    const conversation = await dependencies.store.getConversation(payload.conversation_id);
    if (!conversation) {
      return c.json({ detail: "conversation not found" }, 404);
    }

    const latestMessage = payload.messages[payload.messages.length - 1];
    if (latestMessage.role !== "user" || latestMessage.content.trim().length === 0) {
      return c.json({ detail: "latest user message is required" }, 400);
    }

    const executionMode = payload.execution_mode ?? conversation.execution_mode ?? "read_only";
    const trace = createTrace({
      conversationId: payload.conversation_id,
      createId: dependencies.createId,
    });
    const stream = new AgentEventStream({
      store: dependencies.store,
      trace,
      sequencer: new RunSequencer(),
      now: dependencies.now,
      logStreamWrites: dependencies.config.logModelStreamParts,
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
      modelId: payload.model_id ?? conversation.selected_model_id,
      persistedUserMessageId: payload.persisted_user_message_id,
      abortSignal: c.req.raw.signal,
    }).catch((error) => {
      console.error("[agent-runtime] unhandled chat orchestration error", error);
      void stream.fail(error).catch((streamError) => {
        console.error("[agent-runtime] failed to report chat orchestration error", streamError);
      });
    });

    return stream.response;
  });
}

async function orchestrateChat(input: {
  dependencies: RunDependencies;
  stream: AgentEventStream;
  conversation: ConversationDetail;
  executionMode: ExecutionMode;
  missionId: string | null;
  vehicleId: string | null;
  latestUserMessage: string;
  requestMessages: ChatInputMessage[];
  trace: { conversation_id: string; agent_run_id: string; request_id: string };
  modelId: string | null | undefined;
  persistedUserMessageId?: string | null;
  abortSignal?: AbortSignal;
}): Promise<void> {
  const { dependencies, stream } = input;

  let toolCallCount = 0;
  let assistantText = "";
  let pendingToolTextBoundary = false;
  let reasoningText = "";
  let reasoningStarted = false;
  let reasoningCompleted = false;
  let contextPacketId: string | null = null;
  let selection: ResolvedChatModel | null = null;
  let observedStepCount = 0;
  let latestStepFinishReason: string | null = null;
  let finalFinishReason: string | null = null;
  let assistantMessageId: string | null = null;

  try {
    const persistedUserMessage = input.persistedUserMessageId
      ? input.conversation.messages.find((message) => message.id === input.persistedUserMessageId)
      : null;
    if (input.persistedUserMessageId) {
      if (!persistedUserMessage || persistedUserMessage.role !== "user" || persistedUserMessage.content.trim() !== input.latestUserMessage) {
        await stream.fail(new Error("persisted user message does not match chat request"));
        return;
      }
    }

    const userMessage =
      persistedUserMessage ??
      (await dependencies.store.appendMessage({
        conversationId: input.trace.conversation_id,
        role: "user",
        content: input.latestUserMessage,
        requestId: input.trace.request_id,
        agentRunId: input.trace.agent_run_id,
        metadata: {
          request_id: input.trace.request_id,
          agent_run_id: input.trace.agent_run_id,
        },
      }));

    const assistantMessage = await dependencies.store.appendMessage({
      conversationId: input.trace.conversation_id,
      role: "assistant",
      content: "",
      requestId: input.trace.request_id,
      agentRunId: input.trace.agent_run_id,
      metadata: {
        request_id: input.trace.request_id,
        agent_run_id: input.trace.agent_run_id,
        completion_status: "streaming",
      },
    });
    assistantMessageId = assistantMessage.id;
    const activeAssistantMessageId = assistantMessage.id;

    await stream.emitEvent("run.started", {
      execution_mode: input.executionMode,
      message_id: userMessage.id,
      user_message_preview: contentPreview(input.latestUserMessage),
    });

    const retrievalPlan = buildRetrievalPlan(input.latestUserMessage, input.conversation.messages as ChatInputMessage[]);
    await stream.emitEvent("context.requested", {
      retrieval_plan: retrievalPlan,
      limits: CONTEXT_LIMITS,
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
    contextPacketId = context.context_packet_id;

    const toolDefinitions = await dependencies.toolRegistryClient.listTools(input.trace);
    const { validToolDefinitions, skippedToolSchemaDiagnostics } = validateToolDefinitionsForModel({
      toolDefinitions,
      executionMode: input.executionMode,
    });
    await stream.emitRawEvents(skippedToolSchemaDiagnostics.map(toolSchemaInvalidDiagnosticEvent));
    const tools = createToolSet({
      toolDefinitions: validToolDefinitions,
      toolExecutionClient: dependencies.toolExecutionClient,
      toolPermissionClient: dependencies.toolPermissionClient,
      trace: input.trace,
      assistantMessageId: activeAssistantMessageId,
      executionMode: input.executionMode,
      abortSignal: input.abortSignal,
      onToolCallRequested: () => {
        toolCallCount += 1;
      },
      emitRawToolEvents: async (events) => {
        if (rawEventsIncludeToolActivity(events) && assistantText.trim().length > 0) {
          pendingToolTextBoundary = true;
        }
        await stream.emitRawEvents(events as RawEventFact[] | undefined);
      },
    });

    const modelMessages = (
      persistedUserMessage ? input.conversation.messages : [...input.conversation.messages, { role: "user", content: input.latestUserMessage }]
    ) as ChatInputMessage[];
    const systemPrompt = buildSystemPrompt({
      executionMode: input.executionMode,
      retrievalPlan,
      context,
      tools: validToolDefinitions,
      messages: modelMessages,
    });

    throwIfRunCancelled(input.abortSignal);
    const scriptedMode = resolveScriptedMode(dependencies.config.scriptedMode, input.latestUserMessage);
    if (scriptedMode) {
      if (!dependencies.config.allowMissingKeyFallback) {
        throw new Error("Deterministic scripted mode is disabled in this environment.");
      }
      const result = await runScriptedMode({
        mode: scriptedMode,
        stream,
        store: dependencies.store,
        trace: input.trace,
        executionMode: input.executionMode,
        abortSignal: input.abortSignal,
        toolDefinitions: validToolDefinitions,
        toolExecutionClient: dependencies.toolExecutionClient,
        toolPermissionClient: dependencies.toolPermissionClient,
        assistantMessageId: activeAssistantMessageId,
        contextPacketId: context.context_packet_id,
      });
      await completeScriptedRun({
        store: dependencies.store,
        stream,
        trace: input.trace,
        assistantMessageId: activeAssistantMessageId,
        result,
        contextPacketId: context.context_packet_id,
      });
      await maybeGenerateAndEmitConversationTitle({ dependencies, conversationId: input.trace.conversation_id, stream });
      await stream.close();
      return;
    }

    try {
      selection = await dependencies.modelCatalog.resolveForChat(input.modelId, input.executionMode);
    } catch (error) {
      if (error instanceof ModelSelectionError) {
        await stream.fail(error);
        return;
      }
      throw error;
    }

    if (!selection) {
      throw new Error("Model selection did not resolve.");
    }

    const requiresStrictApiKey =
      selection.runtime.providerType === "openai" || selection.runtime.providerType === "anthropic";

    if (requiresStrictApiKey && !selection.runtime.apiKey) {
      if (!dependencies.config.allowMissingKeyFallback) {
        await stream.fail(
          new Error(
            `Model API key is not configured for provider ${selection.option.provider} and deterministic no-LLM mode is not enabled.`,
          ),
        );
        return;
      }
      await runFallback({
        stream,
        userMessage: input.latestUserMessage,
        executionMode: input.executionMode,
        contextPacketId: context.context_packet_id,
        persistAssistantMessage: async (content) =>
          dependencies.store.updateMessage(activeAssistantMessageId, {
            content,
            requestId: input.trace.request_id,
            agentRunId: input.trace.agent_run_id,
            metadata: {
              agent_run_id: input.trace.agent_run_id,
              request_id: input.trace.request_id,
            },
          }).then((message) => {
            if (!message) throw new Error("assistant message not found");
            return message;
          }),
      });
      await maybeGenerateAndEmitConversationTitle({ dependencies, conversationId: input.trace.conversation_id, stream });
      await stream.close();
      return;
    }

    await stream.emitEvent("model.selected", {
      model_id: selection.option.id,
      provider_type: selection.option.providerType,
      provider_model_id: selection.option.providerModelId,
      model_name: selection.option.name,
      provider: selection.option.provider,
      data_boundary: selection.option.governance.dataBoundary,
      capabilities: selection.option.capabilities,
    });

    const selectedRuntime = selection.runtime;
    const budgetTelemetryEnabled = Boolean(
      selectedRuntime.budget?.contextWindowTokens || selectedRuntime.budget?.tokensPerMinute || selectedRuntime.budget?.maxOutputTokens,
    );
    const budgetTracker = budgetTelemetryEnabled ? new ModelBudgetTracker(dependencies.modelUsageStore, dependencies.config) : null;
    const emitBudgetSnapshot = async (snapshot: Awaited<ReturnType<ModelBudgetTracker["snapshot"]>>) => {
      await stream.emitEvent("model.budget.snapshot", snapshot);
      const warning = warningFromSnapshot(snapshot);
      if (warning) {
        await stream.emitEvent("model.budget.warning", warning);
      }
    };
    const preflightUsage = estimatedUsageSnapshot({
      inputTokens: estimateTokensFromMessages({ system: systemPrompt, messages: modelMessages }),
      source: "estimated_preflight",
    });
    if (budgetTracker) {
      await emitBudgetSnapshot(
        await budgetTracker.recordEstimatedRequest({
          conversationId: input.trace.conversation_id,
          agentRunId: input.trace.agent_run_id,
          providerType: selectedRuntime.providerType,
          providerModelId: selectedRuntime.providerModelId,
          modelId: selectedRuntime.id,
          budget: selectedRuntime.budget,
          usage: preflightUsage,
          requestId: input.trace.request_id,
          nowMs: dependencies.now().getTime(),
        }),
      );
    }
    const reasoningRepresentation = reasoningRepresentationForProvider(selectedRuntime.providerType);

    const emitReasoningStarted = async () => {
      if (reasoningStarted) {
        return;
      }
      reasoningStarted = true;
      await stream.emitEvent("message.reasoning.started", {
        provider_type: selectedRuntime.providerType,
        provider_model_id: selectedRuntime.providerModelId,
        representation: reasoningRepresentation,
        source: "provider_exposed",
      });
    };

    throwIfRunCancelled(input.abortSignal);
    for await (const part of dependencies.modelRunner.stream({
      system: systemPrompt,
      messages: modelMessages,
      tools,
      maxSteps: dependencies.config.maxSteps,
      model: selection.runtime,
      abortSignal: input.abortSignal,
      trace: input.trace,
      onRuntimeEvent: async (eventType, eventPayload) => {
        await stream.emitEvent(eventType, eventPayload);
        if (budgetTracker && (eventType === "model.usage.step" || eventType === "model.usage.total")) {
          const parsedUsage = LanguageModelUsageSnapshotSchema.safeParse(eventPayload.usage);
          if (parsedUsage.success) {
            await emitBudgetSnapshot(
              await budgetTracker.recordActualUsage({
                conversationId: input.trace.conversation_id,
                agentRunId: input.trace.agent_run_id,
                providerType: selectedRuntime.providerType,
                providerModelId: selectedRuntime.providerModelId,
                modelId: selectedRuntime.id,
                budget: selectedRuntime.budget,
                usage: parsedUsage.data,
                requestId: input.trace.request_id,
                stepType: typeof eventPayload.step_type === "string" ? eventPayload.step_type : null,
                stepIndex: typeof eventPayload.step_index === "number" ? eventPayload.step_index : parsedUsage.data.step_index ?? null,
                nowMs: dependencies.now().getTime(),
              }),
            );
          }
        }
        if (budgetTracker && eventType === "model.provider_error") {
          const category = eventPayload.category;
          if (category === "rate_limited") {
            await emitBudgetSnapshot(
              await budgetTracker.markRateLimited({
                providerType: selectedRuntime.providerType,
                providerModelId: selectedRuntime.providerModelId,
                modelId: selectedRuntime.id,
                budget: selectedRuntime.budget,
                retryAfterMs: typeof eventPayload.retry_after_ms === "number" ? eventPayload.retry_after_ms : null,
                nowMs: dependencies.now().getTime(),
              }),
            );
          }
        }
      },
    })) {
      if (part.type === "abort") {
        throw new ChatRunCancelledError(cancellationReasonForSignal(input.abortSignal));
      }

      throwIfRunCancelled(input.abortSignal);

      if (part.type === "error") {
        const fields = part as Record<string, unknown>;
        const error = fields.error;
        throw new ModelProviderRuntimeError(
          normalizeModelProviderError({
            error,
            providerType: selectedRuntime.providerType,
            providerModelId: selectedRuntime.providerModelId,
          }),
        );
      }

      if (part.type === "step-finish") {
        observedStepCount += 1;
        latestStepFinishReason = typeof part.finishReason === "string" ? part.finishReason : null;
        continue;
      }

      if (part.type === "finish") {
        finalFinishReason = typeof part.finishReason === "string" ? part.finishReason : null;
        continue;
      }

      if (isReasoningStart(part)) {
        await emitReasoningStarted();
        continue;
      }

      const reasoningDelta = getReasoningDelta(part);
      if (reasoningDelta && reasoningDelta.length > 0) {
        await emitReasoningStarted();
        reasoningText += reasoningDelta;
        await stream.emitEvent("message.reasoning.delta", {
          text_delta: reasoningDelta,
        });
        continue;
      }

      if (isReasoningEnd(part) && reasoningStarted && !reasoningCompleted) {
        reasoningCompleted = true;
        await stream.emitEvent("message.reasoning.completed", {
          text_length: reasoningText.length,
          representation: reasoningRepresentation,
        });
        continue;
      }

      const textDelta = getTextDelta(part);
      if (textDelta && textDelta.length > 0) {
        const segmentedTextDelta = textDeltaWithToolBoundary(assistantText, textDelta, pendingToolTextBoundary);
        assistantText += segmentedTextDelta;
        pendingToolTextBoundary = false;
        await stream.emitMessageDelta(segmentedTextDelta);
      }
    }

    if (reasoningStarted && !reasoningCompleted) {
      await stream.emitEvent("message.reasoning.completed", {
        text_length: reasoningText.length,
        representation: reasoningRepresentation,
      });
    }

    const usedStepLimitFallback = assistantText.trim().length === 0 && observedStepCount >= dependencies.config.maxSteps;
    const finalAssistantText =
      assistantText.trim().length > 0
        ? assistantText
        : usedStepLimitFallback
          ? stepLimitReachedMessage(dependencies.config.maxSteps)
          : "No response.";
    if (assistantText.trim().length === 0) {
      await stream.emitMessageDelta(finalAssistantText);
    }
    const assistantMetadata: Record<string, unknown> = {
      agent_run_id: input.trace.agent_run_id,
      request_id: input.trace.request_id,
      model_id: selection.option.id,
      provider_type: selection.option.providerType,
      provider_model_id: selection.option.providerModelId,
      provider: selection.option.provider,
      data_boundary: selection.option.governance.dataBoundary,
    };
    if (usedStepLimitFallback) {
      assistantMetadata.completion_status = "step_limit_reached";
      assistantMetadata.max_steps = dependencies.config.maxSteps;
      assistantMetadata.observed_step_count = observedStepCount;
      assistantMetadata.tool_call_count = toolCallCount;
      if (latestStepFinishReason) {
        assistantMetadata.latest_step_finish_reason = latestStepFinishReason;
      }
      if (finalFinishReason) {
        assistantMetadata.final_finish_reason = finalFinishReason;
      }
    }
    if (reasoningText.trim().length > 0) {
      assistantMetadata.reasoning = {
        text: reasoningText,
        representation: reasoningRepresentation,
        source: "provider_exposed",
        provider_type: selection.runtime.providerType,
        provider_model_id: selection.runtime.providerModelId,
        streamed: true,
      };
    }

    const completedAssistantMessage = await dependencies.store.updateMessage(activeAssistantMessageId, {
      content: finalAssistantText,
      requestId: input.trace.request_id,
      agentRunId: input.trace.agent_run_id,
      metadata: assistantMetadata,
    });
    if (!completedAssistantMessage) {
      throw new Error("assistant message not found");
    }
    await stream.emitEvent("message.completed", {
      message_id: completedAssistantMessage.id,
      content_preview: contentPreview(finalAssistantText),
    });
    await stream.emitEvent("run.completed", {
      assistant_message_id: completedAssistantMessage.id,
      tool_call_count: toolCallCount,
      context_packet_id: context.context_packet_id,
      ...(usedStepLimitFallback
        ? {
            completion_status: "step_limit_reached",
            max_steps: dependencies.config.maxSteps,
            observed_step_count: observedStepCount,
            latest_step_finish_reason: latestStepFinishReason,
            final_finish_reason: finalFinishReason,
          }
        : {}),
    });
    await maybeGenerateAndEmitConversationTitle({ dependencies, conversationId: input.trace.conversation_id, stream });
    await stream.close();
  } catch (error) {
    if (isRunCancelled(error, input.abortSignal)) {
      const assistantMetadata: Record<string, unknown> = {
        agent_run_id: input.trace.agent_run_id,
        request_id: input.trace.request_id,
        completion_status: "cancelled",
      };

      if (selection) {
        assistantMetadata.model_id = selection.option.id;
        assistantMetadata.provider_type = selection.option.providerType;
        assistantMetadata.provider_model_id = selection.option.providerModelId;
        assistantMetadata.provider = selection.option.provider;
        assistantMetadata.data_boundary = selection.option.governance.dataBoundary;
      }

      if (reasoningText.trim().length > 0 && selection) {
        assistantMetadata.reasoning = {
          text: reasoningText,
          representation: reasoningRepresentationForProvider(selection.runtime.providerType),
          source: "provider_exposed",
          provider_type: selection.runtime.providerType,
          provider_model_id: selection.runtime.providerModelId,
          streamed: true,
          completion_status: "cancelled",
        };
      }

      let partialAssistantMessageId: string | null = null;
      if (assistantText.trim().length > 0 || reasoningText.trim().length > 0) {
        const partialAssistantMessage = assistantMessageId
          ? await dependencies.store.updateMessage(assistantMessageId, {
              content: assistantText,
              requestId: input.trace.request_id,
              agentRunId: input.trace.agent_run_id,
              metadata: assistantMetadata,
            })
          : null;
        partialAssistantMessageId = partialAssistantMessage?.id ?? null;
      }

      const payload: Record<string, unknown> = {
        reason: error instanceof ChatRunCancelledError ? error.reason : cancellationReasonForSignal(input.abortSignal),
        assistant_text_length: assistantText.length,
        reasoning_text_length: reasoningText.length,
        tool_call_count: toolCallCount,
      };

      if (partialAssistantMessageId) {
        payload.partial_assistant_message_id = partialAssistantMessageId;
      }
      if (contextPacketId) {
        payload.context_packet_id = contextPacketId;
      }

      await stream.emitEvent("run.cancelled", payload);
      await stream.close();
      return;
    }

    if (error instanceof ModelProviderRuntimeError && assistantMessageId) {
      const normalized = error.normalized;
      const hasUsefulPartialState = assistantText.trim().length > 0 || reasoningText.trim().length > 0 || toolCallCount > 0 || normalized.retryable;
      if (hasUsefulPartialState) {
        const providerMetadata: Record<string, unknown> = {
          agent_run_id: input.trace.agent_run_id,
          request_id: input.trace.request_id,
          completion_status: normalized.retryable ? "interrupted_provider_retryable" : "interrupted_provider_failed",
          failure_category: normalized.category,
          retryable: normalized.retryable,
          can_continue: normalized.retryable,
          provider_type: normalized.provider_type,
          provider_model_id: normalized.provider_model_id,
          context_packet_id: contextPacketId,
          tool_call_count: toolCallCount,
        };
        if (selection) {
          providerMetadata.model_id = selection.option.id;
          providerMetadata.provider = selection.option.provider;
          providerMetadata.data_boundary = selection.option.governance.dataBoundary;
        }
        if (reasoningText.trim().length > 0 && selection) {
          providerMetadata.reasoning = {
            text: reasoningText,
            representation: reasoningRepresentationForProvider(selection.runtime.providerType),
            source: "provider_exposed",
            provider_type: selection.runtime.providerType,
            provider_model_id: selection.runtime.providerModelId,
            streamed: true,
            completion_status: "interrupted",
          };
        }
        await dependencies.store.updateMessage(assistantMessageId, {
          content: assistantText,
          requestId: input.trace.request_id,
          agentRunId: input.trace.agent_run_id,
          metadata: providerMetadata,
        });
      } else {
        await dependencies.store.deleteMessage(assistantMessageId);
      }
    } else if (assistantMessageId && assistantText.trim().length === 0 && reasoningText.trim().length === 0 && toolCallCount === 0) {
      await dependencies.store.deleteMessage(assistantMessageId);
    }
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
