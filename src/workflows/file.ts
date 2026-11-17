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
import { Machine } from "../engine/machine.js";
import type { RunContext } from "../engine/context.js";
import type { ExecutionState } from "../engine/state.js";
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
