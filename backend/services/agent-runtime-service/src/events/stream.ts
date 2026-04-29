import { RunSequencer } from "./sequencer.js";
import type { ConversationStore, PersistedEvent, RawEventFact, StreamChunk, TraceEnvelope } from "../types.js";

const encoder = new TextEncoder();

export class AgentEventStream {
  readonly response: Response;
  readonly #writer: WritableStreamDefaultWriter<Uint8Array>;
  readonly #store: ConversationStore;
  readonly #trace: TraceEnvelope;
  readonly #sequencer: RunSequencer;
  readonly #now: () => Date;

  constructor(input: {
    store: ConversationStore;
    trace: TraceEnvelope;
    sequencer: RunSequencer;
    now: () => Date;
  }) {
    const stream = new TransformStream<Uint8Array, Uint8Array>();
    this.#writer = stream.writable.getWriter();
    this.#store = input.store;
    this.#trace = input.trace;
    this.#sequencer = input.sequencer;
    this.#now = input.now;

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
    const sequence = this.#sequencer.next();
    const persistedEvent = await this.#store.appendEvent({
      conversation_id: this.#trace.conversation_id,
      agent_run_id: this.#trace.agent_run_id,
      request_id: this.#trace.request_id,
      tool_call_id: input?.toolCallId ?? null,
      sequence,
      emitted_by: input?.emittedBy ?? "agent-runtime-service",
      event_type: eventType,
      payload,
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
      await this.emitEvent(event.event_type, event.payload, {
        emittedBy: event.emitted_by,
        toolCallId: event.tool_call_id ?? null,
      });
    }
  }

  async emitMessageDelta(delta: string, messageId: string | null = null): Promise<void> {
    await this.#write({
      kind: "message.delta",
      conversation_id: this.#trace.conversation_id,
      agent_run_id: this.#trace.agent_run_id,
      request_id: this.#trace.request_id,
      message_id: messageId,
      sequence: this.#sequencer.next(),
      delta,
      created_at: this.#now().toISOString(),
    });
  }

  async close(): Promise<void> {
    await this.#writer.close();
  }

  async fail(error: unknown): Promise<void> {
    const message = error instanceof Error ? error.message : "Agent runtime failed";
    await this.emitEvent("run.failed", {
      error_code: "agent_runtime_failed",
      message,
    });
    await this.close();
  }

  async #write(chunk: StreamChunk): Promise<void> {
    await this.#writer.write(encoder.encode(`${JSON.stringify(chunk)}\n`));
  }
}
