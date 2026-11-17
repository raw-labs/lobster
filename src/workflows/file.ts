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
import { snapshotDefinition } from "./definition.js";
import { ExecutionStore } from "../engine/store.js";
import { Machine, matchesEvent } from "../engine/machine.js";
import type { RunContext } from "../engine/context.js";
import { ExecutionConflictError, type ExecutionState } from "../engine/state.js";
import type { WorkflowRunResult } from "./types.js";
import { visibleTo, callerId } from "../persistence/waits.js";

export { loadWorkflowFile } from "./load.js";
export { resolveWorkflowArgs } from "./args.js";
export { WorkflowResumeArgumentError } from "../engine/gates.js";
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
};

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
			if (!options.filePath) throw new Error("Workflow file path required");
			const definition = await snapshotDefinition(options.filePath, options.args ?? {});
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
			})) as { machineVersion?: number; runId?: string } | null;
			if (!state) throw new Error("Workflow resume state not found");
			if (state.machineVersion !== 1 || !state.runId) throw new UpgradeRequiredError();
			runId = state.runId;
			ctx._onResumeStateResolved?.(options.resume.stateKey);
		} else {
			recorder ??= await session.createRun("workflow", ctx.runMetadata);
			runId = recorder.runId;
		}
		const run = await session.getRunState(runId);
		if (!run || !visibleTo(run, callerId(ctx.runMetadata)))
			throw new Error("Workflow run not found");
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
			const machine = new Machine(state, store, bound);
			if (!state.revision) await machine.save();
			if (options.resume) {
				await machine.answer(options.resume.stateKey!, options);
				if (recorder) recorder.ownsExecution = true;
			}
			return { ...(await machine.run()), runId };
		} finally {
			await store.close();
		}
	});
}

/** Continue a saved workflow after process restart. No conversation is required. */
export async function advanceWorkflowRun(options: {
	runId: string;
	ctx: RunContext;
	leaseMs?: number;
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
		const store = new ExecutionStore(session, run.id, options.leaseMs);
		try {
			const state = await store.claim();
			if (state!.version !== 1) throw new Error("Unsupported workflow execution version");
			const { ExecutionRecorder } = await import("../persistence/session.js");
			const recorder = new ExecutionRecorder(session, run.id, "", run.historyEnabled !== false);
			return {
				...(await new Machine(state!, store, {
					...ctx,
					env: bindRecorder(ctx.env, recorder),
				}).run()),
				runId: run.id,
			};
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
	dryRun?: boolean;
	leaseMs?: number;
}): Promise<{ due: string[]; advanced: DueRunOutcome[] }> {
	return withStoreContext({ ...options.ctx, trackRuns: true }, async (ctx) => {
		const session = sessionFromEnv(ctx.env)!;
		const due = await session.dueRuns({
			limit: options.limit ?? 100,
			callerId: callerId(ctx.runMetadata),
		});
		if (options.dryRun) return { due, advanced: [] };
		const advanced: DueRunOutcome[] = [];
		for (const runId of due) advanced.push(await advanceReporting(runId, ctx, options.leaseMs));
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
		const { inserted } = await session.appendEvent({
			id,
			name: options.name,
			data: options.data ?? null,
			at: Date.now(),
		});
		const owner = callerId(ctx.runMetadata);
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
		for (const runId of waiting) advanced.push(await advanceReporting(runId, ctx, options.leaseMs));
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
