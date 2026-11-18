import { randomUUID } from "node:crypto";
import path from "node:path";
import { withStoreContext } from "../persistence/execution.js";
import { bindRecorder, recorderFromEnv, sessionFromEnv } from "../persistence/context.js";
import {
	readStateJsonWithLock as readStateJson,
	cleanupApprovalIndexByStateKey,
} from "../state/store.js";
import { loadWorkflowFile } from "./load.js";
import { dryRunWorkflow } from "./dry_run.js";
import { resolveWorkflowArgs } from "./args.js";
import { pipelineDefinition, snapshotDefinition } from "./definition.js";
import { ExecutionStore } from "../engine/store.js";
import { Machine, matchesEvent } from "../engine/machine.js";
import type { RunContext } from "../engine/context.js";
import { ExecutionConflictError, type Definition, type ExecutionState } from "../engine/state.js";
import type { WorkflowRunResult } from "./types.js";
import { visibleTo, callerId } from "../persistence/waits.js";

export { loadWorkflowFile } from "./load.js";
export { resolveWorkflowArgs } from "./args.js";
export { WorkflowResumeArgumentError, ResumeRefusedError } from "../engine/gates.js";
import { ResumeRefusedError } from "../engine/gates.js";
export type {
	WorkflowFile,
	ParallelBranch,
	ParallelConfig,
	WorkflowStep,
	WorkflowApproval,
	WorkflowInputRequest,
	WorkflowStepResult,
	WorkflowRunResult,
} from "./types.js";

/** A pause saved by an older Lobster. `lobster upgrade` converts it in place. */
export class UpgradeRequiredError extends Error {
	readonly code = "LOBSTER_UPGRADE_REQUIRED";
	constructor() {
		super(
			"This was paused by an older version of Lobster. Run `lobster upgrade`, then resume it again with the same token or approval ID.",
		);
		this.name = "UpgradeRequiredError";
	}
}

export type WorkflowResumePayload = {
	protocolVersion: 1;
	v: 1;
	kind: "workflow-file";
	stateKey?: string;
	filePath?: string;
};

export type WorkflowRunOptions = {
	filePath?: string;
	args?: Record<string, unknown>;
	ctx: RunContext;
	resume?: WorkflowResumePayload;
	approved?: boolean;
	response?: unknown;
	cancel?: boolean;
	/** Host execution lease; not a workflow setting. */
	leaseMs?: number;
	/** Run this definition instead of loading `filePath` (a standalone pipeline). */
	definition?: Definition;
	/** The kind recorded for a new run: "workflow" (default), "pipeline" or "sdk". */
	kind?: string;
	/**
	 * Set by the Lobster SDK, whose stages are closures only its host has: the run's pipeline
	 * must name exactly these stages. Runs of kind "sdk" are refused without it.
	 */
	sdk?: { pipeline: string };
};

const SDK_RUN_ELSEWHERE =
	"This run was started by the Lobster SDK: resume it with Lobster.resume on an instance with the same stages";

/** SDK runs need their host's stages; everything else must not claim to be one. */
function assertRunnableHere(kind: string, sdk: WorkflowRunOptions["sdk"]) {
	if (kind === "sdk" && !sdk) throw new ResumeRefusedError(SDK_RUN_ELSEWHERE);
	if (kind !== "sdk" && sdk)
		throw new ResumeRefusedError("This run was not started by the Lobster SDK");
}

/**
 * Run a standalone pipeline (`lobster '...'`, `runToolRequest({ pipeline })`) as a one-step
 * workflow, so it pauses, resumes and recovers exactly like workflow files do.
 */
export async function runPipelineText(options: {
	pipeline: string;
	input?: unknown[];
	ctx: RunContext;
	leaseMs?: number;
}): Promise<WorkflowRunResult & { runId?: string }> {
	return runWorkflowFile({
		definition: pipelineDefinition(options.pipeline, options.input),
		kind: "pipeline",
		ctx: options.ctx,
		leaseMs: options.leaseMs,
	});
}

export async function runWorkflowFile(
	options: WorkflowRunOptions,
): Promise<WorkflowRunResult & { runId?: string }> {
	if (options.ctx.dryRun) {
		const workflow = await loadWorkflowFile(options.filePath!);
		return dryRunWorkflow({
			steps: workflow.steps,
			resolvedArgs: resolveWorkflowArgs(workflow.args, options.args),
			results: {},
			startIndex: 0,
			ctx: options.ctx,
		});
	}
	return withStoreContext({ ...options.ctx, trackRuns: true }, async (ctx) => {
		const session = sessionFromEnv(ctx.env)!;
		let recorder = recorderFromEnv(ctx.env);
		let runId: string;
		// Load and validate first: a workflow that cannot start leaves no run behind.
		let fresh: Pick<ExecutionState, "definition" | "args"> | undefined;
		if (!options.resume) {
			if (!options.filePath && !options.definition) throw new Error("Workflow file path required");
			const definition =
				options.definition ?? (await snapshotDefinition(options.filePath!, options.args ?? {}));
			fresh = { definition, args: resolveWorkflowArgs(definition.workflow.args, options.args) };
		}
		if (options.resume) {
			if (!options.resume.stateKey)
				throw new Error(
					"This resume token is from a Lobster version that kept workflow state inside the token; it cannot be resumed",
				);
			const state = (await readStateJson({
				env: ctx.env,
				key: options.resume.stateKey,
				signal: ctx.signal,
			})) as { machineVersion?: number; runId?: string; type?: string } | null;
			// A token an older Lobster already used left this marker behind.
			if (!state || state.type === "lobster.consumed-resume-state.v1")
				throw new ResumeRefusedError();
			if (state.machineVersion !== 1 || !state.runId) throw new UpgradeRequiredError();
			runId = state.runId;
			ctx._onResumeStateResolved?.(options.resume.stateKey);
		} else {
			recorder ??= await session.createRun(options.kind ?? "workflow", ctx.runMetadata);
			runId = recorder.runId;
		}
		const run = await session.getRunState(runId);
		if (!run || !visibleTo(run, callerId(ctx.runMetadata)))
			throw new Error("Workflow run not found");
		assertRunnableHere(run.kind, options.sdk);
		recorder ??= await session.recorderForState(options.resume!.stateKey!);
		const bound = { ...ctx, env: recorder ? bindRecorder(ctx.env, recorder) : ctx.env };
		const store = new ExecutionStore(session, runId, options.leaseMs);
		try {
			let state = await store.claim();
			if (!state) {
				if (!fresh) throw new UpgradeRequiredError();
				state = {
					version: 1,
					revision: 0,
					...fresh,
					cwd: path.resolve(ctx.cwd ?? process.cwd()),
					activations: {},
					status: "running",
					startedAt: Date.now(),
				};
			}
			if (state.version !== 1) throw new Error("Unsupported workflow execution version");
			if (options.sdk) assertSdkPipeline(state, options.sdk);
			const machine = new Machine(state, store, bound);
			if (!state.revision) await machine.save();
			if (options.resume) {
				await machine.answer(options.resume.stateKey!, options);
			}
			return { ...(await machine.run()), runId };
		} catch (error) {
			throw withRunId(error, runId);
		} finally {
			await store.close();
		}
	});
}

/**
 * Continue a saved run: after a process restart, a lapsed timer or a crash. A failed run is
 * retried from the step that failed. No conversation is required.
 */
export async function advanceWorkflowRun(options: {
	runId: string;
	ctx: RunContext;
	leaseMs?: number;
	/** Set by the Lobster SDK for its own runs (see `WorkflowRunOptions.sdk`). */
	sdk?: { pipeline: string };
}): Promise<WorkflowRunResult & { runId: string }> {
	return withStoreContext({ ...options.ctx, trackRuns: true }, async (ctx) => {
		const session = sessionFromEnv(ctx.env)!;
		const run = await session.getRunState(options.runId);
		if (!run || !visibleTo(run, callerId(ctx.runMetadata)))
			throw new Error("Workflow run not found");
		if (!run.execution)
			throw new Error(
				"Run has no restartable workflow checkpoint; migration or inspection required",
			);
		assertRunnableHere(run.kind, options.sdk);
		const store = new ExecutionStore(session, run.id, options.leaseMs);
		try {
			const state = await store.claim();
			if (state!.version !== 1) throw new Error("Unsupported workflow execution version");
			if (options.sdk) assertSdkPipeline(state!, options.sdk);
			const { ExecutionRecorder } = await import("../persistence/session.js");
			const recorder = new ExecutionRecorder(session, run.id, "", run.historyEnabled !== false);
			const machine = new Machine(state!, store, { ...ctx, env: bindRecorder(ctx.env, recorder) });
			if (state!.status === "failed") {
				machine.retryFailed();
				await machine.save();
			}
			return { ...(await machine.run()), runId: run.id };
		} catch (error) {
			throw withRunId(error, run.id);
		} finally {
			await store.close();
		}
	});
}

export type DueRunOutcome = {
	runId: string;
	status: WorkflowRunResult["status"] | "blocked" | "failed" | "busy";
	waitingFor?: WorkflowRunResult["waitingFor"];
	error?: string;
};

/** Advance one run, reporting the outcome instead of throwing (used for batches). */
async function advanceReporting(runId: string, ctx: RunContext, leaseMs?: number) {
	try {
		const result = await advanceWorkflowRun({ runId, ctx, leaseMs });
		return {
			runId,
			status: result.status,
			...(result.waitingFor ? { waitingFor: result.waitingFor } : {}),
		} satisfies DueRunOutcome;
	} catch (error: any) {
		// Another process holds the run's lease: it is being advanced right now.
		if (error instanceof ExecutionConflictError) return { runId, status: "busy" } as DueRunOutcome;
		const session = sessionFromEnv(ctx.env);
		const run = session ? await session.getRunState(runId) : null;
		return {
			runId,
			status: run?.execution?.status === "blocked" ? "blocked" : "failed",
			error: error?.message ?? String(error),
		} as DueRunOutcome;
	}
}

/**
 * Advance every run whose time has come, then return: a crashed run whose lease lapsed, a
 * timer that is due, a question or event wait past its deadline. Lobster never loops on its
 * own; a host, cron job or person calls this. The usual rules apply: a step that may have
 * acted re-runs only when repeatable, everything else becomes blocked for a person.
 */
export async function advanceDueRuns(options: {
	ctx: RunContext;
	limit?: number;
	/** How many runs to advance at once (default 1). Each is a separate run with its own lease. */
	concurrency?: number;
	dryRun?: boolean;
	leaseMs?: number;
}): Promise<{ due: string[]; advanced: DueRunOutcome[] }> {
	const concurrency = options.concurrency ?? 1;
	if (!Number.isSafeInteger(concurrency) || concurrency < 1)
		throw new Error("concurrency must be a positive integer");
	return withStoreContext({ ...options.ctx, trackRuns: true }, async (ctx) => {
		const session = sessionFromEnv(ctx.env)!;
		const due = await session.dueRuns({
			limit: options.limit ?? 100,
			callerId: callerId(ctx.runMetadata),
		});
		if (options.dryRun) return { due, advanced: [] };
		// Runs share nothing but the store, whose writes are serialized: a slow run no longer
		// delays the others beyond the number of runs advanced at once.
		const advanced: DueRunOutcome[] = Array.from({ length: due.length });
		let next = 0;
		const worker = async () => {
			while (next < due.length) {
				const index = next++;
				advanced[index] = await advanceReporting(due[index], ctx, options.leaseMs);
			}
		};
		await Promise.all(Array.from({ length: Math.min(concurrency, due.length) }, worker));
		return { due, advanced };
	});
}

/**
 * Deliver an external event: store it in the inbox (a repeated `id` is ignored) and advance
 * the waiting runs whose `wait:` step it matches. Runs that reach their `wait:` later still
 * find it, since they match events received since they started.
 */
export async function signalEvent(options: {
	name: string;
	data?: unknown;
	id?: string;
	ctx: RunContext;
	leaseMs?: number;
}): Promise<{ event: { id: string; name: string; inserted: boolean }; advanced: DueRunOutcome[] }> {
	if (!/^[A-Za-z0-9_.:/-]+$/.test(options.name))
		throw new Error("Event names use letters, digits and . _ : / -");
	return withStoreContext({ ...options.ctx, trackRuns: true }, async (ctx) => {
		const session = sessionFromEnv(ctx.env)!;
		const id = options.id ?? randomUUID();
		const owner = callerId(ctx.runMetadata);
		// Scoped to the sender: its events reach only its own runs (or any run, for an unscoped
		// trusted caller), and its delivery IDs cannot collide with another caller's.
		const { inserted } = await session.appendEvent({
			id,
			name: options.name,
			data: options.data ?? null,
			at: Date.now(),
			owner: owner ?? "",
		});
		const waiting: string[] = [];
		for (let after = ""; ;) {
			const page = await session.listRuns({
				status: "waiting",
				after,
				limit: 100,
				callerId: owner,
			});
			for (const summary of page.runs) {
				const run = await session.getRunState(summary.id);
				const listens = Object.values(run?.execution?.activations ?? {}).some(
					(frame) =>
						frame.phase === "waiting" &&
						frame.wait?.kind === "event" &&
						frame.wait.event === options.name &&
						matchesEvent(frame.wait.match, options.data ?? null),
				);
				if (listens) waiting.push(summary.id);
			}
			if (!page.nextCursor) break;
			after = page.nextCursor;
		}
		const advanced: DueRunOutcome[] = [];
		for (const runId of waiting) {
			// A run that is busy is usually just saving its wait; it looks at the inbox again
			// after saving, but wait for it so this delivery reports what happened.
			let outcome = await advanceReporting(runId, ctx, options.leaseMs);
			for (let delay = 25; outcome.status === "busy" && delay <= 1600; delay *= 2) {
				await new Promise((resolve) => setTimeout(resolve, delay));
				outcome = await advanceReporting(runId, ctx, options.leaseMs);
			}
			advanced.push(outcome);
		}
		return { event: { id, name: options.name, inserted }, advanced };
	});
}

/**
 * Stop a run for good: one that is blocked, was interrupted, or waits for an answer.
 * Steps that had started are recorded as abandoned (their outcome stays unknown and
 * nothing runs again), pending questions are withdrawn, and the run is cancelled, so
 * retention can clean it up. Refuses while another process is executing the run.
 */
export async function abandonWorkflowRun(options: {
	runId: string;
	ctx: RunContext;
	leaseMs?: number;
}): Promise<{ runId: string; status: string; abandoned: string[] }> {
	return withStoreContext({ ...options.ctx, trackRuns: true }, async (ctx) => {
		const session = sessionFromEnv(ctx.env)!;
		const run = await session.getRunState(options.runId);
		if (!run || !visibleTo(run, callerId(ctx.runMetadata)))
			throw new Error("Workflow run not found");
		if (!run.execution) throw new Error("Run has no workflow execution to abandon");
		if (["completed", "failed", "cancelled"].includes(run.execution.status))
			return { runId: run.id, status: run.execution.status, abandoned: [] };
		const store = new ExecutionStore(session, run.id, options.leaseMs);
		try {
			const state = (await store.claim())!;
			// It may have finished between the first read and the claim: keep its result.
			if (["completed", "failed", "cancelled"].includes(state.status))
				return { runId: run.id, status: state.status, abandoned: [] };
			const abandoned: string[] = [];
			const waits: string[] = [];
			for (const [path, frame] of Object.entries(state.activations)) {
				if (frame.phase === "started" && path.endsWith("/activity")) abandoned.push(path);
				else if (frame.phase === "waiting" && frame.waitKey) waits.push(frame.waitKey);
				else continue;
				frame.phase = "abandoned";
			}
			state.status = "cancelled";
			state.error =
				"Abandoned" + (abandoned.length ? `; outcome unknown for ${abandoned.join(", ")}` : "");
			// A cancelled run never runs again; the error above keeps what was abandoned.
			state.activations = {};
			const current = await session.getRunState(run.id);
			const pending = waits.filter((key) => current?.waits[key]?.status === "pending");
			if (!pending.length) await store.save(state);
			for (const key of pending) await store.save(state, { consume: { key } });
			for (const key of waits)
				await cleanupApprovalIndexByStateKey({ env: ctx.env, stateKey: key }).catch(() => {});
			return { runId: run.id, status: "cancelled", abandoned };
		} finally {
			await store.close();
		}
	});
}

export function decodeWorkflowResumePayload(payload: unknown): WorkflowResumePayload | null {
	if (
		!payload ||
		typeof payload !== "object" ||
		(payload as WorkflowResumePayload).kind !== "workflow-file"
	)
		return null;
	const value = payload as WorkflowResumePayload;
	if (value.protocolVersion !== 1 || value.v !== 1) throw new Error("Unsupported token version");
	return value;
}

function stageCount(pipeline: string) {
	return pipeline ? pipeline.split(" | ").length : 0;
}

/** An SDK instance continues only a run of the same stages. */
function assertSdkPipeline(state: ExecutionState, sdk: { pipeline: string }) {
	const saved = String(state.definition.workflow.steps[0]?.pipeline ?? "");
	if (saved !== sdk.pipeline)
		throw new ResumeRefusedError(
			`This Lobster instance has ${stageCount(sdk.pipeline)} stages; the paused run has ${stageCount(saved)}`,
		);
}

/** Name the run an error stopped, so a host can inspect or retry it. */
/** The run each failure belongs to, set only here so a command cannot claim another run. */
const failedRuns = new WeakMap<object, string>();

function withRunId(error: unknown, runId: string) {
	// The innermost run keeps the error: a later, outer frame does not relabel it.
	if (error && typeof error === "object" && !failedRuns.has(error)) failedRuns.set(error, runId);
	return error;
}

/** The run a failure from `runWorkflowFile` or `advanceWorkflowRun` belongs to. */
export function runIdOf(error: unknown): string | undefined {
	return error && typeof error === "object" ? failedRuns.get(error) : undefined;
}
