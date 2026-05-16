/**
 * Change-summary aggregator.
 *
 * Watches tool-call results for the canonical "preview branch + write + commit"
 * sequence and emits a structured `change.summary` event so the UI can render
 * a deploy/revert chat message without parsing assistant text.
 *
 * The aggregator is intentionally generic: it has no knowledge of any specific
 * managed unit or capability. Target unit and application are resolved by
 * matching changed file paths against the registry's catalog (managed unit
 * source paths). If the target cannot be inferred unambiguously, the summary
 * is still emitted with `target_unit_id: null` and the UI must keep deploy
 * disabled until the target is provided.
 */

import type { AgentEventStream } from "./events/stream.js";
import { z } from "zod";

export interface ChangeSummaryRegistryUnit {
  unit_id: string;
  source_path: string;
  service_slug?: string | null;
  application_id?: string | null;
  runtime_kind?: string | null;
  capabilities?: string[];
  category?: string | null;
}

export interface ChangeSummaryRegistryClient {
  /** Fetches units once per chat run; the aggregator caches the result. */
  listUnits(): Promise<ChangeSummaryRegistryUnit[]>;
}

interface BranchContext {
  branch: string;
  baseBranch: string;
  baseCommitSha: string | null;
  changedFiles: Set<string>;
  /** Most recent commit SHA observed on this branch via `create_commit`. */
  lastCommitSha: string | null;
  emittedCommitSha: string | null;
}

const ToolEnvelopeSchema = z
  .object({
    branch: z.string().min(1).optional(),
    commit_sha: z.string().min(1).optional(),
    changed_files: z.array(z.string()).optional(),
    data: z.record(z.unknown()).optional(),
  })
  .passthrough();

const CreateWorkingBranchArgsSchema = z
  .object({
    branch: z.string().min(1).optional(),
    from_branch: z.string().min(1).optional(),
  })
  .passthrough();

const WriteSourceFileArgsSchema = z
  .object({
    branch: z.string().min(1).optional(),
    path: z.string().min(1).optional(),
  })
  .passthrough();

const CreateCommitArgsSchema = z
  .object({
    branch: z.string().min(1).optional(),
  })
  .passthrough();

const CreateBranchEnvelopeDataSchema = z
  .object({
    base_branch: z.string().min(1).optional(),
    base_commit_sha: z.string().min(1).optional(),
    branch_existed_before: z.boolean().optional(),
  })
  .passthrough();

function readEnvelope(output: unknown): z.infer<typeof ToolEnvelopeSchema> {
  const parsed = ToolEnvelopeSchema.safeParse(output);
  return parsed.success ? parsed.data : {};
}

function isManagedFilePath(path: string): boolean {
  if (path.length === 0) return false;
  if (path.includes("..")) return false;
  return true;
}

/**
 * Resolve the target unit/application by matching changed file paths against
 * the longest unit `source_path` prefix. Returns `null` if no unique match.
 */
export function resolveTargetUnit(
  units: ChangeSummaryRegistryUnit[],
  changedFiles: string[],
): { unit: ChangeSummaryRegistryUnit | null; ambiguous: boolean } {
  const matchingUnits = new Map<string, { unit: ChangeSummaryRegistryUnit; matchLength: number }>();
  for (const file of changedFiles) {
    if (!isManagedFilePath(file)) continue;
    let bestUnit: ChangeSummaryRegistryUnit | null = null;
    let bestLength = -1;
    for (const unit of units) {
      const sourcePath = unit.source_path?.trim();
      if (!sourcePath) continue;
      const normalized = sourcePath.replace(/\/$/, "");
      const startsWithSourcePath =
        file === normalized || file.startsWith(`${normalized}/`) || file.startsWith(`project/${normalized}/`);
      if (startsWithSourcePath && normalized.length > bestLength) {
        bestUnit = unit;
        bestLength = normalized.length;
      }
    }
    if (bestUnit) {
      matchingUnits.set(bestUnit.unit_id, { unit: bestUnit, matchLength: bestLength });
    }
  }
  if (matchingUnits.size === 0) {
    return { unit: null, ambiguous: false };
  }
  if (matchingUnits.size > 1) {
    return { unit: null, ambiguous: true };
  }
  const [{ unit }] = [...matchingUnits.values()];
  return { unit, ambiguous: false };
}

function pickAffectedCapability(unit: ChangeSummaryRegistryUnit | null): string {
  if (!unit) return "platform-change";
  if (unit.capabilities && unit.capabilities.length > 0) return unit.capabilities[0];
  if (unit.service_slug) return unit.service_slug;
  if (unit.application_id) return unit.application_id;
  return unit.unit_id;
}

export interface ChangeSummaryAggregatorOptions {
  stream: AgentEventStream;
  registryClient: ChangeSummaryRegistryClient;
  logger?: { warn: (message: string, error?: unknown) => void };
}

export class ChangeSummaryAggregator {
  private readonly stream: AgentEventStream;
  private readonly registryClient: ChangeSummaryRegistryClient;
  private readonly logger?: ChangeSummaryAggregatorOptions["logger"];
  private readonly branchContexts = new Map<string, BranchContext>();
  private cachedUnits: ChangeSummaryRegistryUnit[] | null = null;
  private cachedUnitsPromise: Promise<ChangeSummaryRegistryUnit[]> | null = null;

  constructor(options: ChangeSummaryAggregatorOptions) {
    this.stream = options.stream;
    this.registryClient = options.registryClient;
    this.logger = options.logger;
  }

  /**
   * Called by the chat orchestration layer after every successful tool call.
   * The aggregator only acts on the three change-producing tools and ignores
   * anything else.
   */
  async observeToolCompletion(toolName: string, args: Record<string, unknown>, output: unknown): Promise<void> {
    if (toolName === "create_working_branch") {
      this.observeCreateBranch(args, output);
      return;
    }
    if (toolName === "write_source_file") {
      this.observeWriteFile(args, output);
      return;
    }
    if (toolName === "create_commit") {
      await this.observeCreateCommit(args, output);
    }
  }

  private getOrCreateContext(branch: string): BranchContext {
    let context = this.branchContexts.get(branch);
    if (!context) {
      context = {
        branch,
        baseBranch: "main",
        baseCommitSha: null,
        changedFiles: new Set(),
        lastCommitSha: null,
        emittedCommitSha: null,
      };
      this.branchContexts.set(branch, context);
    }
    return context;
  }

  private observeCreateBranch(args: Record<string, unknown>, output: unknown): void {
    const argsParsed = CreateWorkingBranchArgsSchema.safeParse(args);
    const branch = argsParsed.success ? argsParsed.data.branch : undefined;
    const fromBranchArg = argsParsed.success ? argsParsed.data.from_branch : undefined;
    const envelope = readEnvelope(output);
    const branchName = envelope.branch ?? branch;
    if (!branchName) return;
    const context = this.getOrCreateContext(branchName);
    if (envelope.data) {
      const dataParsed = CreateBranchEnvelopeDataSchema.safeParse(envelope.data);
      if (dataParsed.success) {
        const { base_branch, base_commit_sha, branch_existed_before } = dataParsed.data;
        if (base_branch) context.baseBranch = base_branch;
        if (base_commit_sha) context.baseCommitSha = base_commit_sha;
        if (branch_existed_before && envelope.commit_sha) {
          // If we attached to an existing branch, treat the current head as the
          // baseline so revert restores something sensible.
          context.baseCommitSha = context.baseCommitSha ?? envelope.commit_sha;
        }
      }
    }
    if (!context.baseCommitSha && envelope.commit_sha) {
      context.baseCommitSha = envelope.commit_sha;
    }
    if (!context.baseBranch && fromBranchArg) {
      context.baseBranch = fromBranchArg;
    }
  }

  private observeWriteFile(args: Record<string, unknown>, output: unknown): void {
    const argsParsed = WriteSourceFileArgsSchema.safeParse(args);
    const branch = argsParsed.success ? argsParsed.data.branch : undefined;
    const path = argsParsed.success ? argsParsed.data.path : undefined;
    const envelope = readEnvelope(output);
    const branchName = envelope.branch ?? branch;
    if (!branchName) return;
    const context = this.getOrCreateContext(branchName);
    if (path) context.changedFiles.add(path);
    for (const file of envelope.changed_files ?? []) {
      context.changedFiles.add(file);
    }
  }

  private async observeCreateCommit(args: Record<string, unknown>, output: unknown): Promise<void> {
    const argsParsed = CreateCommitArgsSchema.safeParse(args);
    const branch = argsParsed.success ? argsParsed.data.branch : undefined;
    const envelope = readEnvelope(output);
    const branchName = envelope.branch ?? branch;
    if (!branchName) return;
    const context = this.getOrCreateContext(branchName);
    for (const file of envelope.changed_files ?? []) {
      context.changedFiles.add(file);
    }
    if (envelope.commit_sha) {
      context.lastCommitSha = envelope.commit_sha;
    }
    // Only emit when we have something to deploy and we have not already
    // emitted for this commit (LLM-driven flows can emit multiple commits in
    // sequence; each new commit produces a fresh `change.summary`).
    if (!context.lastCommitSha) return;
    if (context.lastCommitSha === context.emittedCommitSha) return;
    if (context.changedFiles.size === 0) return;
    if (context.branch === context.baseBranch) {
      // Refuse to emit a "preview deploy" card for changes committed directly
      // to main: the product invariant is that previews live on dedicated
      // branches.
      this.logger?.warn(
        `change.summary suppressed: commit landed on base branch ${context.branch}; previews must use a dedicated branch.`,
      );
      context.emittedCommitSha = context.lastCommitSha;
      return;
    }
    await this.emitChangeSummary(context);
    context.emittedCommitSha = context.lastCommitSha;
  }

  private async loadUnits(): Promise<ChangeSummaryRegistryUnit[]> {
    if (this.cachedUnits) return this.cachedUnits;
    if (!this.cachedUnitsPromise) {
      this.cachedUnitsPromise = this.registryClient
        .listUnits()
        .then((units) => {
          this.cachedUnits = units;
          return units;
        })
        .catch((error) => {
          this.logger?.warn("change.summary: registry unit lookup failed", error);
          this.cachedUnits = [];
          return [];
        });
    }
    return this.cachedUnitsPromise;
  }

  private async emitChangeSummary(context: BranchContext): Promise<void> {
    const changedFiles = [...context.changedFiles].sort();
    const units = await this.loadUnits();
    const { unit } = resolveTargetUnit(units, changedFiles);
    await this.stream.emitEvent("change.summary", {
      branch: context.branch,
      base_branch: context.baseBranch,
      base_commit_sha: context.baseCommitSha,
      commit_sha: context.lastCommitSha,
      changed_files: changedFiles,
      target_unit_id: unit?.unit_id ?? null,
      target_application_id: unit?.application_id ?? null,
      affected_capability: pickAffectedCapability(unit),
      risk_level: "low",
      validation_status: "not_run",
    });
  }
}
