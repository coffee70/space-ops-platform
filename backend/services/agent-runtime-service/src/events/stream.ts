import { RunSequencer } from "./sequencer.js";
import { redactAndTruncate, validateAgentEventPayload } from "./schema.js";
import type { ConversationStore, PersistedEvent, RawEventFact, StreamChunk, TraceEnvelope } from "../types.js";
import { ModelProviderRuntimeError, type ModelProviderErrorCategory } from "../ai/provider-errors.js";

const encoder = new TextEncoder();

const modelProviderErrorCodeByCategory: Record<ModelProviderErrorCategory, string> = {
  rate_limited: "model_provider_rate_limited",
  quota_exceeded: "model_provider_quota_exceeded",
  context_length_exceeded: "model_context_length_exceeded",
  auth_failed: "model_provider_auth_failed",
  model_unavailable: "model_provider_unavailable",
  provider_overloaded: "model_provider_overloaded",
  network_transient: "model_provider_network_transient",
  cancelled: "model_stream_cancelled",
  unknown: "model_provider_failed",
};

function defaultProviderFailureMessage(category: ModelProviderErrorCategory): string {
  if (category === "rate_limited") {
    return "The selected model hit a provider throughput limit. Completed tool actions were preserved. You can continue after the provider window clears.";
  }
  if (category === "provider_overloaded") {
    return "The selected model provider is temporarily overloaded. Completed tool actions were preserved. You can continue once the provider recovers.";
  }
  if (category === "network_transient") {
    return "The model connection was interrupted by a transient network/provider issue. Completed tool actions were preserved. You can continue the conversation.";
  }
  if (category === "context_length_exceeded") {
    return "The selected model could not continue because the request exceeded its context limit. Completed tool actions were preserved, but the next message may need a smaller scope or summarized context.";
  }
  return "Model provider request failed.";
}

function runFailedPayloadForError(error: unknown, context?: Record<string, unknown>): Record<string, unknown> {
  if (error instanceof ModelProviderRuntimeError) {
    const normalized = error.normalized;
    return {
      error_code: modelProviderErrorCodeByCategory[normalized.category],
      category: normalized.category,
      retryable: normalized.retryable,
      can_continue: normalized.retryable || normalized.category === "context_length_exceeded",
      retry_after_ms: normalized.retry_after_ms,
      provider_type: normalized.provider_type,
      provider_model_id: normalized.provider_model_id,
      provider_error_type: normalized.provider_error_type,
      provider_error_code: normalized.provider_error_code,
      http_status: normalized.http_status,
      message: defaultProviderFailureMessage(normalized.category),
      ...context,
    };
  }

  const message = error instanceof Error ? error.message : "Agent runtime failed";
  return {
    error_code: "agent_runtime_failed",
    message,
    ...context,
  };
}

export class AgentEventStream {
  readonly response: Response;
  readonly #writer: WritableStreamDefaultWriter<Uint8Array>;
  readonly #store: ConversationStore;
  readonly #trace: TraceEnvelope;
  readonly #sequencer: RunSequencer;
  readonly #now: () => Date;
  readonly #logStreamWrites: boolean;
  #transportClosed = false;

  constructor(input: {
    store: ConversationStore;
    trace: TraceEnvelope;
    sequencer: RunSequencer;
    now: () => Date;
    logStreamWrites?: boolean;
  }) {
    const stream = new TransformStream<Uint8Array, Uint8Array>();
    this.#writer = stream.writable.getWriter();
    this.#store = input.store;
    this.#trace = input.trace;
    this.#sequencer = input.sequencer;
    this.#now = input.now;
    this.#logStreamWrites = input.logStreamWrites ?? false;

    const init: ResponseInit = {
      headers: {
        "content-type": "application/x-ndjson; charset=utf-8",
        "cache-control": "no-store",
        "x-agent-run-id": this.#trace.agent_run_id,
        "x-request-id": this.#trace.request_id,
        "x-conversation-id": this.#trace.conversation_id,
      },
    };

    this.response = new Response(stream.readable, init);
  }

  async emitEvent(eventType: string, payload: Record<string, unknown>, input?: { emittedBy?: string; toolCallId?: string | null }): Promise<PersistedEvent> {
    const toolCallId = input?.toolCallId ?? null;
    validateAgentEventPayload(eventType, payload, toolCallId);
    const safePayload = redactAndTruncate(payload) as Record<string, unknown>;
    const sequence = this.#sequencer.next();
    const persistedEvent = await this.#store.appendEvent({
      conversation_id: this.#trace.conversation_id,
      agent_run_id: this.#trace.agent_run_id,
      request_id: this.#trace.request_id,
      tool_call_id: toolCallId,
      sequence,
      emitted_by: input?.emittedBy ?? "agent-runtime-service",
      event_type: eventType,
      payload: safePayload,
      created_at: this.#now().toISOString(),
    });

    await this.#write({
      kind: "event",
      event: persistedEvent,
    });

    return persistedEvent;
  }

  async emitRawEvents(events: RawEventFact[] | undefined): Promise<void> {
    for (const event of events ?? []) {
      try {
        await this.emitEvent(event.event_type, event.payload, {
          emittedBy: event.emitted_by,
          toolCallId: event.tool_call_id ?? null,
        });
      } catch (error) {
        await this.emitEvent("error", {
          error_code: "invalid_downstream_event",
          message: error instanceof Error ? error.message : "Invalid downstream event",
          source: event.emitted_by || "downstream-service",
        });
      }
    }
  }

  async emitMessageDelta(delta: string): Promise<PersistedEvent> {
    return this.emitEvent("message.delta", {
      text_delta: delta,
    });
  }

  async close(): Promise<void> {
    if (this.#transportClosed) {
      return;
    }
    this.#transportClosed = true;
    try {
      await this.#writer.close();
    } catch {
      // The downstream client may intentionally disconnect during cancellation.
    }
  }

  async fail(error: unknown, context?: Record<string, unknown>): Promise<void> {
    await this.emitEvent("run.failed", runFailedPayloadForError(error, context));
    await this.close();
  }

  async #write(chunk: StreamChunk): Promise<void> {
    if (this.#transportClosed) {
      return;
    }

    if (this.#logStreamWrites && chunk.kind === "event" && chunk.event.event_type === "message.delta") {
      const delta = chunk.event.payload.text_delta;
      console.debug(
        "[agent-runtime] ndjson write message.delta",
        JSON.stringify({
          timestamp: new Date().toISOString(),
          sequence: chunk.event.sequence,
          deltaLength: typeof delta === "string" ? delta.length : 0,
          preview: typeof delta === "string" ? delta.slice(0, 80) : "",
        }),
      );
    }
    try {
      await this.#writer.write(encoder.encode(`${JSON.stringify(chunk)}\n`));
    } catch {
      this.#transportClosed = true;
    }
  }
}
