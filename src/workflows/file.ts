import path from "node:path";
import { withStoreContext } from "../persistence/execution.js";
import { bindRecorder, recorderFromEnv, sessionFromEnv } from "../persistence/context.js";
import { readStateJsonWithLock as readStateJson } from "../state/store.js";
import { loadWorkflowFile } from "./load.js";
import { dryRunWorkflow } from "./dry_run.js";
import { resolveWorkflowArgs } from "../definition/args.js";
import { snapshotDefinition } from "../definition/normalize.js";
import { ExecutionStore } from "../engine/store.js";
import { Machine } from "../engine/machine.js";
import type { RunContext } from "../engine/context.js";
import type { WorkflowRunResult } from "./types.js";
import { visibleTo, callerId } from "../persistence/waits.js";

export { loadWorkflowFile } from "./load.js";
export { resolveWorkflowArgs } from "../definition/args.js";
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
		if (options.resume) {
			if (!options.resume.stateKey)
				throw new Error("Legacy workflow state requires one-off migration");
			const state = (await readStateJson({
				env: ctx.env,
				key: options.resume.stateKey,
				signal: ctx.signal,
			})) as { machineVersion?: number; runId?: string } | null;
			if (!state) throw new Error("Workflow resume state not found");
			if (state.machineVersion !== 1 || !state.runId)
				throw new Error("Legacy workflow state requires one-off migration");
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
				if (options.resume) throw new Error("Legacy workflow state requires one-off migration");
				if (!options.filePath) throw new Error("Workflow file path required");
				const definition = await snapshotDefinition(options.filePath, options.args ?? {});
				state = {
					version: 1,
					revision: 0,
					definition,
					args: resolveWorkflowArgs(definition.workflow.args, options.args),
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
