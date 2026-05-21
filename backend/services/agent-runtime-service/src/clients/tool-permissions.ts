import { z } from "zod";

import { runtimeServiceUrl } from "./tool-execution.js";
import type { RuntimeConfig, ToolPermissionClient, ToolPermissionStatusResponse } from "../types.js";

const ToolPermissionStatusResponseSchema = z
  .object({
    permission_request_id: z.string(),
    tool_call_id: z.string(),
    status: z.enum(["pending", "approved", "denied", "executing", "executed", "failed", "expired"]),
    response_json: z.record(z.unknown()).nullable().optional(),
    raw_events: z
      .array(
        z
          .object({
            event_type: z.string(),
            emitted_by: z.string(),
            payload: z.record(z.unknown()),
            tool_call_id: z.string().nullable().optional(),
            created_at: z.string().optional(),
          })
          .passthrough(),
      )
      .optional(),
  })
  .passthrough();

function sleep(ms: number, signal?: AbortSignal): Promise<void> {
  if (signal?.aborted) {
    return Promise.reject(new DOMException("Permission wait aborted", "AbortError"));
  }
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(resolve, ms);
    signal?.addEventListener(
      "abort",
      () => {
        clearTimeout(timeout);
        reject(new DOMException("Permission wait aborted", "AbortError"));
      },
      { once: true },
    );
  });
}

export class HttpToolPermissionClient implements ToolPermissionClient {
  readonly #config: RuntimeConfig;

  constructor(config: RuntimeConfig) {
    this.#config = config;
  }

  async #getStatus(permissionRequestId: string, signal?: AbortSignal): Promise<ToolPermissionStatusResponse> {
    const response = await fetch(
      runtimeServiceUrl(this.#config, "tool-execution-service", `permissions/${permissionRequestId}`),
      { signal },
    );
    if (!response.ok) {
      const detail = await response.text();
      throw new Error(detail || "Failed to load tool permission status");
    }
    return ToolPermissionStatusResponseSchema.parse(await response.json());
  }

  async waitForDecision(input: {
    permissionRequestId: string;
    abortSignal?: AbortSignal;
  }): Promise<{ status: "approved" | "denied"; reason?: string | null; raw_events?: ToolPermissionStatusResponse["raw_events"] }> {
    while (true) {
      const status = await this.#getStatus(input.permissionRequestId, input.abortSignal);
      if (status.status === "approved" || status.status === "executing" || status.status === "executed") {
        return { status: "approved" };
      }
      if (status.status === "denied") {
        const reason = typeof status.response_json?.reason === "string" ? status.response_json.reason : "user_denied";
        return { status: "denied", reason, raw_events: status.raw_events };
      }
      if (status.status === "failed" || status.status === "expired") {
        return { status: "denied", reason: status.status, raw_events: status.raw_events };
      }
      await sleep(350, input.abortSignal);
    }
  }
}
