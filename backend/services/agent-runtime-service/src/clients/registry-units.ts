import type { ChangeSummaryRegistryClient, ChangeSummaryRegistryUnit } from "../change-summary.js";
import type { RuntimeConfig } from "../types.js";
import { z } from "zod";

const RegistryUnitWireSchema = z
  .object({
    unit_id: z.string().optional(),
    unitId: z.string().optional(),
    source_path: z.string().optional(),
    sourcePath: z.string().optional(),
    service_slug: z.string().nullable().optional(),
    serviceSlug: z.string().nullable().optional(),
    application_id: z.string().nullable().optional(),
    applicationId: z.string().nullable().optional(),
    runtime_kind: z.string().nullable().optional(),
    runtimeKind: z.string().nullable().optional(),
    category: z.string().nullable().optional(),
    capabilities: z.array(z.string()).optional().default([]),
  })
  .passthrough();

const RegistryUnitsWireSchema = z.array(RegistryUnitWireSchema);

function pickString(value: unknown): string | undefined {
  return typeof value === "string" && value.length > 0 ? value : undefined;
}

/**
 * Resolves managed units from the control-plane registry so the change-summary
 * aggregator can map a set of changed file paths back to a target unit /
 * application without any service-specific hardcoding.
 *
 * The control plane only returns metadata that is safe to expose to the chat
 * UI (no runtime topology), but it does return `source_path`, `service_slug`,
 * and `application_id` which is everything we need for matching.
 */
export class HttpChangeSummaryRegistryClient implements ChangeSummaryRegistryClient {
  readonly #config: RuntimeConfig;

  constructor(config: RuntimeConfig) {
    this.#config = config;
  }

  async listUnits(): Promise<ChangeSummaryRegistryUnit[]> {
    const url = `${this.#config.controlPlaneUrl.replace(/\/$/, "")}/registry/units`;
    const response = await fetch(url, {
      headers: { "Content-Type": "application/json" },
      signal: AbortSignal.timeout(this.#config.requestTimeoutMs),
    });
    if (!response.ok) {
      throw new Error(`registry units lookup failed: ${response.status}`);
    }
    const parsed = RegistryUnitsWireSchema.safeParse(await response.json());
    if (!parsed.success) {
      throw new Error("registry units lookup returned invalid payload");
    }
    const payload = parsed.data;
    const units: ChangeSummaryRegistryUnit[] = [];
    for (const entry of payload) {
      const unitId = pickString(entry.unit_id) ?? pickString(entry.unitId);
      if (!unitId) continue;
      const sourcePath = pickString(entry.source_path) ?? pickString(entry.sourcePath);
      if (!sourcePath) continue;
      const runtimeKind = pickString(entry.runtime_kind) ?? pickString(entry.runtimeKind);
      units.push({
        unit_id: unitId,
        source_path: sourcePath,
        service_slug: pickString(entry.service_slug) ?? pickString(entry.serviceSlug) ?? null,
        application_id: pickString(entry.application_id) ?? pickString(entry.applicationId) ?? null,
        runtime_kind: runtimeKind ?? null,
        capabilities: entry.capabilities,
        category: pickString(entry.category) ?? null,
      });
    }
    return units;
  }
}
